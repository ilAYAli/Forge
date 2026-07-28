from __future__ import annotations

import copy
import re
import time
from pathlib import Path
from typing import Any

from .calibration import (
    CHUNK_CACHE_SCHEMA as ADAPTIVE_CHUNK_CACHE_SCHEMA,
    PROCESS_RATE_DIR as ADAPTIVE_PROCESS_RATE_DIR,
    PROCESS_RATE_SCHEMA as ADAPTIVE_PROCESS_RATE_SCHEMA,
    RATE_HISTORY_WEIGHT as DEFAULT_SELFPLAY_CHUNK_HISTORY_WEIGHT,
    RATE_WARMUP_FRACTION as ADAPTIVE_RATE_WARMUP_FRACTION,
    RATE_WARMUP_MAX_UNITS as ADAPTIVE_RATE_WARMUP_MAX_UNITS,
    RATE_WARMUP_MIN_UNITS as ADAPTIVE_RATE_WARMUP_MIN_UNITS,
    SELFPLAY_CHUNK_CACHE_NAME,
    SELFPLAY_CHUNK_CACHE_SCHEMA,
    adaptive_blended_throughput,
    adaptive_cached_throughput,
    adaptive_chunk_cache_path,
    adaptive_chunk_signature,
    adaptive_chunk_signature_key,
    adaptive_chunk_signature_label,
    adaptive_process_cached_throughput,
    adaptive_process_rate_path,
    adaptive_rate_warmup_units,
    adaptive_template_parameters,
    adaptive_worker_cached_throughput,
    load_adaptive_chunk_history,
    load_selfplay_chunk_history,
    record_adaptive_chunk_rate,
    record_adaptive_process_chunk_rate,
    selfplay_cached_throughput,
    selfplay_chunk_cache_path,
    selfplay_chunk_signature,
    selfplay_chunk_signature_key,
    selfplay_chunk_signature_label,
    write_adaptive_chunk_history,
    write_selfplay_chunk_history,
)
from .core import *
from .tasks import (
    task_done_path,
    task_heartbeat_path,
    task_running_path,
    task_state,
)

DEFAULT_SELFPLAY_CHUNK_TIME_SECONDS = 300
DEFAULT_SELFPLAY_COLD_START_THROUGHPUT = 15 / 300


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def selfplay_chunk_target_seconds(params: dict[str, Any]) -> int:
    value = _parse_int(params.get("chunk_time_seconds"))
    if value is None or value <= 0:
        return DEFAULT_SELFPLAY_CHUNK_TIME_SECONDS
    return int(value)


def selfplay_default_chunk_games(params: dict[str, Any]) -> int:
    target_seconds = selfplay_chunk_target_seconds(params)
    return max(1, int(round(target_seconds * DEFAULT_SELFPLAY_COLD_START_THROUGHPUT)))


def selfplay_task_games(task: dict[str, Any]) -> int:
    progress = task.get("progress")
    if isinstance(progress, dict):
        total = _parse_int(progress.get("total"))
        if total is not None and total > 0:
            return total
    command = str(task.get("command") or "")
    match = re.search(r"\b-rounds\s+(\d+)\b", command)
    if match:
        return int(match.group(1)) * 2
    return 0


def adaptive_split_config(template_data: dict[str, Any]) -> tuple[str, str, str] | None:
    task = template_data.get("task")
    split = task.get("split") if isinstance(task, dict) else None
    if not isinstance(split, dict):
        return None
    total_key = split.get("total")
    count_key = split.get("count")
    unit = split.get("unit")
    if not all(isinstance(value, str) and value for value in (total_key, count_key, unit)):
        return None
    return str(total_key), str(count_key), str(unit)


def adaptive_task_units(task: dict[str, Any], unit: str) -> int:
    progress = task.get("progress")
    if isinstance(progress, dict):
        direct = _parse_int(progress.get(unit))
        if direct is not None and direct > 0:
            return direct
        if str(progress.get("unit") or "") == unit:
            total = _parse_int(progress.get("total"))
            if total is not None and total > 0:
                return total
    return selfplay_task_games(task) if unit == "games" else 0


def adaptive_manifest_split(manifest: dict[str, Any]) -> tuple[str, str, str] | None:
    template = manifest.get("template")
    path_value = template.get("path") if isinstance(template, dict) else None
    if not isinstance(path_value, str) or not path_value:
        return ("games", "shards", "games") if str(manifest.get("kind") or "") == "selfplay" else None
    try:
        from .templates import load_template

        return adaptive_split_config(load_template(expand_path(path_value)))
    except (OSError, SystemExit):
        return None


def adaptive_live_rechunk_enabled(manifest: dict[str, Any]) -> bool:
    template = manifest.get("template")
    path_value = template.get("path") if isinstance(template, dict) else None
    if not isinstance(path_value, str) or not path_value:
        return False
    try:
        from .templates import load_template

        template_data = load_template(expand_path(path_value))
    except (OSError, SystemExit):
        return False
    chunking = template_data.get("chunking")
    return (
        isinstance(chunking, dict)
        and bool(chunking.get("live_rechunk"))
        and adaptive_split_config(template_data) is not None
    )


def adaptive_worker_slot_count(manifest_path: Path, worker: str) -> int:
    workers_path = manifest_path.parent / "workers.json"
    try:
        payload = read_json(workers_path)
    except (OSError, SystemExit):
        return 1
    records = payload.get("workers") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        return 1
    slots = 0
    for record in records:
        if not isinstance(record, dict) or str(record.get("host") or "").strip() != worker:
            continue
        value = _parse_int(record.get("slots", 1))
        if value is not None and value > 0:
            slots += value
    return max(1, slots)


def adaptive_autosized_shards(
    template_name: str,
    template_data: dict[str, Any],
    params: dict[str, Any],
    worker_payload: dict[str, Any],
    *,
    cold_units: int,
    cache_path: Path | None = None,
) -> int | None:
    split = adaptive_split_config(template_data)
    if split is None:
        return None
    total_key, _count_key, _unit = split
    total = _parse_int(params.get(total_key))
    if total is None or total <= 0:
        return None
    signature_params = {**params, "__forge_template_name": template_name}
    worker_rates: list[float] = []
    worker_names: list[str] = []
    workers = worker_payload.get("workers")
    if isinstance(workers, list):
        for worker in workers:
            name = str(
                (worker.get("name") or worker.get("host") or "")
                if isinstance(worker, dict)
                else worker
            ).strip()
            if not name:
                continue
            worker_names.append(name)
            worker_rate = selfplay_cached_throughput(
                {**signature_params, "__forge_worker": name},
                cache_path=cache_path,
            )
            if worker_rate is not None and worker_rate > 0:
                worker_rates.append(worker_rate)
    if worker_names:
        rate = min(worker_rates) if len(worker_rates) == len(worker_names) else None
    else:
        rate = selfplay_cached_throughput(signature_params, cache_path=cache_path)
    target_seconds = selfplay_chunk_target_seconds(params)
    target_units = max(1, int(round(rate * target_seconds))) if rate else max(1, int(cold_units))
    shards = max(1, (total + target_units - 1) // target_units)
    slots = 0
    workers = worker_payload.get("workers")
    if isinstance(workers, list):
        for worker in workers:
            try:
                slots += max(0, int(worker.get("slots", 1))) if isinstance(worker, dict) else 1
            except (TypeError, ValueError):
                continue
    if slots > 0:
        chunking = template_data.get("chunking")
        min_per_slot = (
            max(1, int(chunking.get("min_chunks_per_slot", 1)))
            if isinstance(chunking, dict)
            else 1
        )
        shards = max(slots * min_per_slot, ((shards + slots - 1) // slots) * slots)
    return min(total, shards)


def record_adaptive_chunk_history(
    manifest: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    split = adaptive_manifest_split(manifest)
    if split is None:
        return None
    _total_key, _count_key, unit = split
    total_units = 0
    total_elapsed = 0.0
    for task in manifest.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        done = read_json(task_done_path(manifest, task)) if task_done_path(manifest, task).exists() else None
        elapsed = _parse_float(done.get("elapsed_s")) if isinstance(done, dict) else None
        if elapsed is None or elapsed <= 0:
            continue
        units = adaptive_task_units(task, unit)
        if units <= 0:
            continue
        total_units += units
        total_elapsed += elapsed
    rate = total_units / total_elapsed if total_units > 0 and total_elapsed > 0 else None
    if rate is None or rate <= 0:
        return None

    params = adaptive_template_parameters(manifest)
    return record_adaptive_chunk_rate(params, rate=rate, cache_path=cache_path)


def record_adaptive_chunk_task_result(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    split = adaptive_manifest_split(manifest)
    if split is None:
        return None
    _total_key, _count_key, unit = split
    done_path = task_done_path(manifest, task)
    if not done_path.exists():
        return None
    done = read_json(done_path)
    rc = _parse_int(done.get("rc")) if isinstance(done, dict) else None
    if (
        not isinstance(done, dict)
        or rc != 0
        or bool(done.get("soft_cancel"))
        or bool(done.get("skipped"))
    ):
        return None
    elapsed = _parse_float(done.get("elapsed_s"))
    if elapsed is None or elapsed <= 0:
        return None
    units = adaptive_task_units(task, unit)
    if units <= 0:
        return None
    params = adaptive_template_parameters(manifest)
    rate = units / elapsed
    aggregate = record_adaptive_chunk_rate(params, rate=rate, cache_path=cache_path)
    worker_value = done.get("worker")
    if isinstance(worker_value, dict):
        worker = str(
            worker_value.get("name")
            or worker_value.get("host")
            or done.get("host")
            or ""
        ).strip()
        worker_id = str(worker_value.get("process_id") or done.get("worker_id") or "").strip()
    else:
        worker = str(worker_value or done.get("host") or "").strip()
        worker_id = str(done.get("worker_id") or "").strip()
    if worker:
        record_adaptive_chunk_rate(
            {**params, "__forge_worker": worker},
            rate=rate,
            cache_path=cache_path,
        )
    if worker and worker_id:
        record_adaptive_chunk_rate(
            {
                **params,
                "__forge_worker": worker,
                "__forge_worker_id": worker_id,
            },
            rate=rate,
            cache_path=cache_path,
        )
        record_adaptive_process_chunk_rate(
            manifest,
            worker=worker,
            worker_id=worker_id,
            rate=rate,
            units=units,
            elapsed_s=elapsed,
            started_at=done.get("started_at"),
        )
    return aggregate


def adaptive_rechunk_pending_tasks(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    lease_seconds: int,
    worker: str | None = None,
    worker_id: str | None = None,
    reassign_foreign: bool = False,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    from .templates import expand_template, load_template

    template = manifest.get("template")
    if not isinstance(template, dict):
        return None
    template_path_value = template.get("path")
    if not isinstance(template_path_value, str) or not template_path_value:
        return None
    template_path = expand_path(template_path_value)
    if not template_path.is_file():
        return None
    template_data = load_template(template_path)
    chunking = template_data.get("chunking")
    if not isinstance(chunking, dict) or not bool(chunking.get("live_rechunk")):
        return None
    split = adaptive_split_config(template_data)
    if split is None:
        return None
    total_key, count_key, unit = split
    params = template.get("parameters")
    if not isinstance(params, dict):
        return None

    worker_name = str(worker or "").strip()
    process_id = str(worker_id or "").strip()
    if reassign_foreign and not worker_name:
        return None

    def route_matches(task: dict[str, Any]) -> bool:
        preferred = str(task.get("claim_worker") or "").strip()
        if reassign_foreign:
            return bool(preferred and preferred != worker_name)
        return not preferred or preferred == worker_name

    pending = [
        task
        for task in manifest.get("tasks") or []
        if isinstance(task, dict)
        and task_state(manifest, task, lease_seconds=lease_seconds) == "pending"
        and route_matches(task)
    ]
    if not pending:
        return None

    ranged_pending = all(
        isinstance(task.get("adaptive_range"), dict)
        and str(task["adaptive_range"].get("unit") or "") == unit
        for task in pending
    )

    live_done = 0
    live_elapsed = 0.0
    live_warmup_units = 0
    now_epoch = time.time()
    if worker_name:
        for task in manifest.get("tasks") or []:
            if not isinstance(task, dict):
                continue
            running_path = task_running_path(manifest, task)
            heartbeat_path = task_heartbeat_path(manifest, task)
            if not running_path.exists() or not heartbeat_path.exists():
                continue
            running = read_json(running_path)
            heartbeat = read_json(heartbeat_path)
            if not isinstance(running, dict) or not isinstance(heartbeat, dict):
                continue
            if str(running.get("worker") or heartbeat.get("worker") or "").strip() != worker_name:
                continue
            if process_id and str(heartbeat.get("worker_id") or running.get("worker_id") or "").strip() != process_id:
                continue
            started = _parse_float(running.get("started_epoch") or heartbeat.get("claimed_epoch"))
            progress = heartbeat.get("progress_data")
            done = _parse_int(progress.get("done")) if isinstance(progress, dict) else None
            if started is None or done is None:
                continue
            elapsed = now_epoch - started
            if elapsed < 30:
                continue
            live_done += max(0, done)
            live_elapsed += elapsed
            task_total = adaptive_task_units(task, unit)
            if task_total > 0:
                live_warmup_units += adaptive_rate_warmup_units(task_total)
    live_rate = live_done / live_elapsed if live_done > 0 and live_elapsed > 0 else None
    cached_rate = adaptive_worker_cached_throughput(
        manifest,
        worker=worker_name,
        worker_id=process_id,
        cache_path=cache_path,
    )
    effective_rate = adaptive_blended_throughput(
        cached_rate,
        live_rate,
        observed_units=live_done,
        warmup_units=live_warmup_units,
    )
    if effective_rate is not None and effective_rate > 0:
        estimated_games = max(1, int(round(effective_rate * selfplay_chunk_target_seconds(params))))
    else:
        estimated_games = max(1, int(chunking.get("initial_units") or 1))
    if ranged_pending:
        pending.sort(key=lambda task: int(task["adaptive_range"]["start"]))
        contiguous = [pending[0]]
        expected = (
            int(pending[0]["adaptive_range"]["start"])
            + adaptive_task_units(pending[0], unit)
        )
        for task in pending[1:]:
            start = int(task["adaptive_range"]["start"])
            if start != expected:
                break
            contiguous.append(task)
            expected += adaptive_task_units(task, unit)
        pending = contiguous
    elif reassign_foreign:
        pending.sort(key=lambda task: int(task.get("index", 0)))
    else:
        pending.sort(key=lambda task: (
            0 if str(task.get("claim_worker") or "").strip() == worker_name else 1,
            -int(task.get("index", 0))
            if str(task.get("claim_worker") or "").strip() == worker_name
            else int(task.get("index", 0)),
        ))
    tolerance = max(2, estimated_games // 4)
    lower = max(1, estimated_games - tolerance)
    upper = estimated_games + tolerance
    current_max_games = max((adaptive_task_units(task, unit) for task in pending), default=0)
    first_units = adaptive_task_units(pending[0], unit)
    if not reassign_foreign and lower <= first_units <= upper:
        return None

    chunks_per_slot = max(1, int(chunking.get("rechunk_chunks_per_slot", 2)))
    desired_chunks = (
        1
        if process_id
        else max(
            1,
            (adaptive_worker_slot_count(manifest_path, worker_name) if worker_name else 1)
            * chunks_per_slot,
        )
    )
    desired_units = estimated_games * desired_chunks
    selected_units = 0
    best_any: tuple[tuple[float, int], int, int] | None = None
    best_batch: tuple[tuple[float, int], int, int] | None = None
    for count, task in enumerate(pending, start=1):
        selected_units += adaptive_task_units(task, unit)
        if selected_units <= 0:
            continue
        shards = max(1, int(round(selected_units / estimated_games)))
        average = selected_units / shards
        score = (abs(average - estimated_games) / estimated_games, count)
        candidate = (score, count, shards)
        if best_any is None or score < best_any[0]:
            best_any = candidate
        if shards >= desired_chunks and (best_batch is None or score < best_batch[0]):
            best_batch = candidate
        if score[0] == 0 and shards >= desired_chunks:
            break
        if count >= 32 and selected_units >= desired_units and best_batch is not None:
            break
    best = best_batch or best_any
    if best is None:
        return None
    _best_score, best_count, target_shards = best
    if best_count <= 0 or target_shards <= 0:
        return None
    selected = pending[:best_count]
    selected_units = sum(adaptive_task_units(task, unit) for task in selected)

    replan_params = {
        **params,
        total_key: selected_units,
        count_key: target_shards,
        "__forge_preserve_derived": [total_key],
    }
    split_data = template_data.get("task", {}).get("split", {})
    start_key = split_data.get("start") if isinstance(split_data, dict) else None
    if ranged_pending and isinstance(start_key, str) and start_key:
        replan_params[start_key] = int(selected[0]["adaptive_range"]["start"])
    if "seed" in params:
        replan_params["seed"] = int(params.get("seed") or 1) + min(
            int(task.get("index", 0)) for task in selected
        )
    replanned = expand_template(
        str(template_data.get("name") or manifest.get("kind") or "task"),
        template_path,
        template_data,
        replan_params,
    )
    replanned_tasks = replanned.get("tasks")
    if not isinstance(replanned_tasks, list) or not replanned_tasks:
        return None

    kept_tasks = [
        task
        for task in manifest.get("tasks") or []
        if task not in selected
    ]
    next_index = max((int(task.get("index", -1)) for task in kept_tasks), default=-1) + 1
    width = max(4, len(str(next_index + len(replanned_tasks) - 1)))
    pending_ids = {str(task.get("id") or "") for task in selected}
    replacement_tasks: list[dict[str, Any]] = []
    for offset, task in enumerate(replanned_tasks):
        if not isinstance(task, dict):
            continue
        repl = copy.deepcopy(task)
        old_id = str(repl.get("id") or "")
        new_index = next_index + offset
        prefix_match = re.match(r"^(.*?)(?:_?\d+)?$", old_id)
        prefix = (prefix_match.group(1) if prefix_match else old_id).rstrip("_") or "task"
        new_id = f"{prefix}_{new_index:0{width}d}"
        repl["id"] = new_id
        repl["index"] = new_index
        if worker_name:
            repl["claim_worker"] = worker_name
        if isinstance(repl.get("command"), str):
            repl["command"] = repl["command"].replace(old_id, new_id)
        if isinstance(repl.get("label"), str):
            repl["label"] = repl["label"].replace(old_id, new_id)
        outputs = repl.get("outputs")
        if isinstance(outputs, list):
            repl["outputs"] = [str(path).replace(old_id, new_id) for path in outputs]
        replacement_tasks.append(repl)

    if not replacement_tasks:
        return None

    updated = copy.deepcopy(manifest)
    updated["tasks"] = [*kept_tasks, *replacement_tasks]
    write_json(manifest_path, updated)
    return {
        "remaining_games": selected_units,
        "current_max_games": current_max_games,
        "target_games": estimated_games,
        "replacement_tasks": len(replacement_tasks),
        "kept_tasks": len(kept_tasks),
        "pending_tasks": len(pending_ids),
    }


# Legacy function names remain available through forge_lib.selfplay_chunks.
record_selfplay_chunk_history = record_adaptive_chunk_history
record_selfplay_chunk_task_result = record_adaptive_chunk_task_result
