from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .core import *
from .tasks import task_done_path, task_state


SELFPLAY_CHUNK_CACHE_SCHEMA = "forge.selfplay.chunk-history.v1"
SELFPLAY_CHUNK_CACHE_NAME = "selfplay-chunk-history.json"
DEFAULT_SELFPLAY_CHUNK_TIME_SECONDS = 300
DEFAULT_SELFPLAY_COLD_START_THROUGHPUT = 15 / 300
DEFAULT_SELFPLAY_CHUNK_HISTORY_WEIGHT = 4


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


def selfplay_chunk_cache_dir() -> Path:
    return expand_path("~/.cache/forge")


def selfplay_chunk_cache_path() -> Path:
    return selfplay_chunk_cache_dir() / SELFPLAY_CHUNK_CACHE_NAME


@contextmanager
def locked_selfplay_chunk_history(*, cache_path: Path | None = None):
    path = cache_path or selfplay_chunk_cache_path()
    lock_path = Path(str(path) + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def selfplay_chunk_signature(params: dict[str, Any]) -> dict[str, Any]:
    signature = {
        "kind": "selfplay",
        "template": "selfplay",
    }
    return signature


def selfplay_chunk_signature_label(signature: dict[str, Any]) -> str:
    return str(signature.get("template") or signature.get("kind") or "selfplay")


def selfplay_chunk_signature_key(signature: dict[str, Any]) -> str:
    payload = json.dumps(signature, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_selfplay_chunk_history(*, cache_path: Path | None = None) -> dict[str, Any]:
    path = cache_path or selfplay_chunk_cache_path()
    try:
        payload = read_json(path)
    except (OSError, SystemExit):
        payload = None
    if not isinstance(payload, dict) or payload.get("schema") != SELFPLAY_CHUNK_CACHE_SCHEMA:
        return {
            "schema": SELFPLAY_CHUNK_CACHE_SCHEMA,
            "records": {},
        }
    records = payload.get("records")
    if not isinstance(records, dict):
        records = {}
    return {
        "schema": SELFPLAY_CHUNK_CACHE_SCHEMA,
        "records": records,
    }


def write_selfplay_chunk_history(payload: dict[str, Any], *, cache_path: Path | None = None) -> None:
    path = cache_path or selfplay_chunk_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)


def selfplay_chunk_target_seconds(params: dict[str, Any]) -> int:
    value = _parse_int(params.get("chunk_time_seconds"))
    if value is None or value <= 0:
        return DEFAULT_SELFPLAY_CHUNK_TIME_SECONDS
    return int(value)


def selfplay_default_chunk_games(params: dict[str, Any]) -> int:
    target_seconds = selfplay_chunk_target_seconds(params)
    return max(1, int(round(target_seconds * DEFAULT_SELFPLAY_COLD_START_THROUGHPUT)))


def selfplay_observed_throughput(manifest: dict[str, Any]) -> float | None:
    total_games = 0
    total_elapsed = 0.0
    for task in manifest.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        done_path = task_done_path(manifest, task)
        done = read_json(done_path) if done_path.exists() else None
        rc = _parse_int(done.get("rc")) if isinstance(done, dict) else None
        if not isinstance(done, dict) or rc != 0:
            continue
        elapsed = _parse_float(done.get("elapsed_s"))
        if elapsed is None or elapsed <= 0:
            continue
        games = selfplay_task_games(task)
        if games <= 0:
            continue
        total_games += games
        total_elapsed += float(elapsed)
    if total_games <= 0 or total_elapsed <= 0:
        return None
    return total_games / total_elapsed


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


def selfplay_cached_throughput(params: dict[str, Any], *, cache_path: Path | None = None) -> float | None:
    signature = selfplay_chunk_signature(params)
    key = selfplay_chunk_signature_key(signature)
    history = load_selfplay_chunk_history(cache_path=cache_path)
    record = history["records"].get(key)
    if not isinstance(record, dict):
        return None
    rate = _parse_float(record.get("rate"))
    if rate is None or rate <= 0:
        return None
    return float(rate)


def selfplay_estimated_chunk_games(
    params: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> int | None:
    rate = selfplay_cached_throughput(params, cache_path=cache_path)
    target_seconds = selfplay_chunk_target_seconds(params)
    if rate is None or rate <= 0:
        return selfplay_default_chunk_games(params)
    estimated = int(round(rate * target_seconds))
    return max(1, estimated)


def _record_selfplay_chunk_rate(
    params: dict[str, Any],
    *,
    rate: float,
    samples: int = 1,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    if rate <= 0 or samples <= 0:
        return None

    signature = selfplay_chunk_signature(params)
    key = selfplay_chunk_signature_key(signature)
    with locked_selfplay_chunk_history(cache_path=cache_path):
        history = load_selfplay_chunk_history(cache_path=cache_path)
        record = history["records"].get(key)
        previous_rate = _parse_float(record.get("rate")) if isinstance(record, dict) else None
        previous_samples = int(record.get("samples") or 0) if isinstance(record, dict) else 0

        if previous_rate is not None and previous_rate > 0 and previous_samples > 0:
            weight = min(previous_samples, DEFAULT_SELFPLAY_CHUNK_HISTORY_WEIGHT)
            blended_rate = (previous_rate * weight + rate * samples) / (weight + samples)
        else:
            blended_rate = rate

        history["records"][key] = {
            "label": selfplay_chunk_signature_label(signature),
            "rate": blended_rate,
            "samples": previous_samples + samples,
            "updated_epoch": time.time(),
        }
        write_selfplay_chunk_history(history, cache_path=cache_path)
        return history["records"][key]


def selfplay_autosized_shards(
    params: dict[str, Any],
    worker_payload: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> int | None:
    games = _parse_int(params.get("games"))
    if games is None or games <= 0:
        return None
    slots = 0
    workers = worker_payload.get("workers")
    if isinstance(workers, list):
        for worker in workers:
            if isinstance(worker, dict):
                try:
                    slots += max(0, int(worker.get("slots", 1)))
                except (TypeError, ValueError):
                    continue
            else:
                slots += 1
    estimated_games = selfplay_estimated_chunk_games(params, cache_path=cache_path)
    shards = (games + estimated_games - 1) // estimated_games
    if slots > 0:
        shards = max(slots, ((shards + slots - 1) // slots) * slots)
    return max(1, shards)


def record_selfplay_chunk_history(
    manifest: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    if str(manifest.get("kind") or "") != "selfplay":
        return None
    rate = selfplay_observed_throughput(manifest)
    if rate is None or rate <= 0:
        return None

    history = load_selfplay_chunk_history(cache_path=cache_path)
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else {}
    if not isinstance(params, dict):
        params = {}
    return _record_selfplay_chunk_rate(params, rate=rate, cache_path=cache_path)


def record_selfplay_chunk_task_result(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    if str(manifest.get("kind") or "") != "selfplay":
        return None
    done_path = task_done_path(manifest, task)
    if not done_path.exists():
        return None
    done = read_json(done_path)
    rc = _parse_int(done.get("rc")) if isinstance(done, dict) else None
    if not isinstance(done, dict) or rc != 0:
        return None
    elapsed = _parse_float(done.get("elapsed_s"))
    if elapsed is None or elapsed <= 0:
        return None
    games = selfplay_task_games(task)
    if games <= 0:
        return None
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else {}
    if not isinstance(params, dict):
        params = {}
    return _record_selfplay_chunk_rate(params, rate=games / elapsed, cache_path=cache_path)


def selfplay_rechunk_pending_tasks(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    lease_seconds: int,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    from .templates import expand_template, load_template

    if str(manifest.get("kind") or "") != "selfplay":
        return None
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
    params = template.get("parameters")
    if not isinstance(params, dict):
        return None

    pending = [
        task
        for task in manifest.get("tasks") or []
        if isinstance(task, dict) and task_state(manifest, task, lease_seconds=lease_seconds) == "pending"
    ]
    if not pending:
        return None

    estimated_games = selfplay_estimated_chunk_games(params, cache_path=cache_path)
    if estimated_games is None or estimated_games <= 0:
        estimated_games = selfplay_default_chunk_games(params)
    threshold_games = estimated_games + max(2, estimated_games // 4)
    current_max_games = max((selfplay_task_games(task) for task in pending), default=0)
    if current_max_games <= threshold_games:
        return None

    remaining_games = sum(selfplay_task_games(task) for task in pending)
    if remaining_games <= 0:
        return None

    target_shards = max(1, (remaining_games + estimated_games - 1) // estimated_games)
    replanned = expand_template(
        str(template_data.get("name") or "selfplay"),
        template_path,
        template_data,
        {
            **params,
            "games": remaining_games,
            "shards": target_shards,
        },
    )
    replanned_tasks = replanned.get("tasks")
    if not isinstance(replanned_tasks, list) or not replanned_tasks:
        return None

    kept_tasks = [
        task
        for task in manifest.get("tasks") or []
        if task not in pending
    ]
    next_index = max((int(task.get("index", -1)) for task in kept_tasks), default=-1) + 1
    width = max(4, len(str(next_index + len(replanned_tasks) - 1)))
    pending_ids = {str(task.get("id") or "") for task in pending}
    replacement_tasks: list[dict[str, Any]] = []
    for offset, task in enumerate(replanned_tasks):
        if not isinstance(task, dict):
            continue
        repl = copy.deepcopy(task)
        old_id = str(repl.get("id") or "")
        new_index = next_index + offset
        new_id = f"selfplay_{new_index:0{width}d}"
        repl["id"] = new_id
        repl["index"] = new_index
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
        "remaining_games": remaining_games,
        "current_max_games": current_max_games,
        "target_games": estimated_games,
        "replacement_tasks": len(replacement_tasks),
        "kept_tasks": len(kept_tasks),
        "pending_tasks": len(pending_ids),
    }
