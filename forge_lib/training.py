from __future__ import annotations

import json
import re
import shlex
from pathlib import Path
from typing import Any

from .core import expand_path, file_record, now, write_json
from .tasks import task_outputs
from .workers import (
    worker_host,
    worker_records_from_payload,
    worker_resources,
    workers_payload_from_records,
)


TRAIN_FINALIZER = "distributed-train"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def distributed_train_superbatches(params: dict[str, Any]) -> int | None:
    """Read the local-superbatch target from the staged Bullet build file."""
    repository = str(params.get("nnue_repo") or "").strip()
    if not repository:
        return None
    try:
        build = json.loads((expand_path(repository) / "build.json").read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    value = build.get("superbatches") if isinstance(build, dict) else None
    return value if isinstance(value, int) and value > 0 else None


def distributed_train_workers_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the selected hosts with exactly one Forge worker slot each."""
    records = worker_records_from_payload(payload, label="distributed train workers")
    seen: set[str] = set()
    selected: list[dict[str, Any]] = []
    for record in records:
        host = worker_host(record)
        if not host or host in seen:
            continue
        if "gpu" not in worker_resources(record):
            raise SystemExit(
                f"distributed train requires gpu resource on {host}; "
                "set resources to [\"cpu\", \"gpu\"] in workers.json"
            )
        seen.add(host)
        item = dict(record)
        item.pop("slots", None)
        selected.append(item)
    if len(selected) < 2:
        raise SystemExit("distributed train requires at least two selected worker hosts")
    return workers_payload_from_records(selected)


def prepare_distributed_train_parameters(
    params: dict[str, Any],
    workers_payload: dict[str, Any],
) -> dict[str, Any]:
    prepared = dict(params)
    workers = worker_records_from_payload(workers_payload, label="distributed train workers")
    hosts = [worker_host(worker) for worker in workers]
    coordinator = str(params.get("coordinator") or "").strip()
    coordinator = str(params.get("coordinator") or "").strip()
    coordinator = str(prepared.get("coordinator") or "").strip()
    if coordinator not in hosts:
        raise SystemExit(
            f"distributed train coordinator {coordinator!r} is not among selected workers: "
            f"{', '.join(hosts)}"
        )
    shard_dir = expand_path(str(prepared.get("shard_dir") or ""))
    if not shard_dir.is_dir():
        raise SystemExit(f"distributed train shard directory does not exist: {shard_dir}")
    missing = [host for host in hosts if not (shard_dir / f"{host}.bullet").is_file()]
    if missing:
        raise SystemExit(
            "distributed train is missing shard(s): "
            + ", ".join(str(shard_dir / f"{host}.bullet") for host in missing)
        )
    prepared.update({"rank_count": len(hosts), "peer_count": len(hosts) - 1})
    return prepared


def attach_distributed_train_inputs(
    manifest: dict[str, Any],
    workers_payload: dict[str, Any],
) -> None:
    """Pin ranks and stage exactly one named Bullet shard on each selected host."""
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else None
    tasks = manifest.get("tasks")
    if not isinstance(params, dict) or not isinstance(tasks, list):
        raise ValueError("distributed train manifest is missing parameters or tasks")
    total_superbatches = distributed_train_superbatches(params)
    workers = worker_records_from_payload(workers_payload, label="distributed train workers")
    hosts = [worker_host(worker) for worker in workers]
    coordinator = str(params.get("coordinator") or "").strip()
    if len(tasks) != len(hosts):
        raise ValueError("distributed train must have exactly one task per selected host")
    shard_dir = expand_path(str(params.get("shard_dir") or "")).resolve()
    requirements = manifest.get("requirements")
    if not isinstance(requirements, dict):
        raise ValueError("distributed train manifest is missing requirements")
    inputs = requirements.setdefault("inputs", [])
    if not isinstance(inputs, list):
        raise ValueError("distributed train requirements.inputs must be a list")

    run_name = str(manifest.get("name") or "").strip()
    if not run_name:
        raise ValueError("distributed train manifest is missing a run name")
    barrier = f"/tmp/forge-distributed-train-{run_name}"
    # Setup completes at different times on different machines.  Do not let a
    # fast rank begin training alone: every host first publishes readiness to
    # the coordinator, then all ranks cross this gate together.
    barrier_prefix = (
        f'BARRIER={shlex.quote(barrier)}; '
        f'ssh -o BatchMode=yes {shlex.quote(coordinator)} "mkdir -p $BARRIER; touch $BARRIER/$HOST.ready"; '
        'deadline=$((SECONDS + 1800)); '
        f'while [ "$(ssh -o BatchMode=yes {shlex.quote(coordinator)} "find $BARRIER -maxdepth 1 -type f | wc -l")" -lt {len(hosts)} ]; do '
        '[ "$SECONDS" -lt "$deadline" ] || {{ echo "distributed launch barrier timed out" >&2; exit 1; }}; sleep 1; '
        'done; '
        f'if [ "$HOST" != {shlex.quote(coordinator)} ]; then sleep 2; fi; '
    ) if coordinator else ""

    for task, host in zip(tasks, hosts):
        shard = shard_dir / f"{host}.bullet"
        if not shard.is_file():
            raise ValueError(f"distributed train shard is missing: {shard}")
        record = file_record(shard)
        # Training shards are immutable and large. Cache them by content rather
        # than run name so a retry only verifies the local copy instead of
        # transferring another full shard to every worker.
        worker_path = f"~/.cache/forge/inputs/train/shards/{record['sha256']}.bullet"
        inputs.append({
            "name": f"training shard {host}",
            "source": str(shard),
            "path": worker_path,
            "hosts": [host],
            "bytes": record["bytes"],
            "sha256": record["sha256"],
        })
        task["claim_worker"] = host
        task["strict_claim_worker"] = True
        command = str(task.get("command") or "").replace(
            "{train_shard}", worker_path,
        )
        if barrier_prefix:
            task["command"] = command.replace(
                'HOST="$(hostname -s)";',
                f'HOST="$(hostname -s)"; {barrier_prefix}',
                1,
            )
        else:
            task["command"] = command
        if total_superbatches is not None:
            progress = task.get("progress")
            if isinstance(progress, dict) and not isinstance(progress.get("total"), int):
                progress["total"] = total_superbatches


def finalize_distributed_train(
    manifest_path: Path,
    manifest: dict[str, Any],
) -> dict[str, Any] | None:
    finalizer = manifest.get("finalize")
    if not isinstance(finalizer, dict) or finalizer.get("type") != TRAIN_FINALIZER:
        return None
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("distributed train has no rank tasks")
    hashes: dict[str, str] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise ValueError("distributed train has an invalid task")
        host = str(task.get("claim_worker") or "").strip()
        if not host or not bool(task.get("strict_claim_worker")):
            raise ValueError(f"{task.get('id', 'task')}: rank is not host-pinned")
        outputs = task_outputs(task, manifest)
        if len(outputs) != 1:
            raise ValueError(f"{task.get('id', 'task')}: expected one SHA-256 output")
        try:
            digest = outputs[0].read_text(encoding="utf-8").strip().lower()
        except OSError as exc:
            raise ValueError(f"{task.get('id', 'task')}: cannot read quantised hash: {exc}") from exc
        if not _SHA256_RE.fullmatch(digest):
            raise ValueError(f"{task.get('id', 'task')}: invalid quantised SHA-256")
        hashes[host] = digest
    unique = set(hashes.values())
    if len(unique) != 1:
        detail = ", ".join(f"{host}={digest}" for host, digest in sorted(hashes.items()))
        raise ValueError(f"distributed train quantised.bin hashes differ: {detail}")
    result = {
        "schema": "forge.distributed-train-summary.v1",
        "completed_at": now(),
        "quantised_sha256": unique.pop(),
        "ranks": hashes,
    }
    write_json(manifest_path.parent / "training-summary.json", result)
    return result
