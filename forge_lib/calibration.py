"""Adaptive throughput samples and per-process calibration state."""

from __future__ import annotations

import hashlib
import fcntl
import json
import math
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .core import expand_path, read_json, write_json
from .manifest import state_dir


CHUNK_CACHE_SCHEMA = "forge.selfplay.chunk-history.v1"
CHUNK_CACHE_NAME = "selfplay-chunk-history.json"
PROCESS_RATE_SCHEMA = "forge.adaptive.process-rate.v1"
PROCESS_RATE_DIR = "adaptive-process-rates"
RATE_HISTORY_WEIGHT = 4
RATE_WARMUP_FRACTION = 0.20
RATE_WARMUP_MIN_UNITS = 4
RATE_WARMUP_MAX_UNITS = 40


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _count(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def adaptive_rate_warmup_units(total: int) -> int:
    scaled = math.ceil(total * RATE_WARMUP_FRACTION)
    return min(total, max(RATE_WARMUP_MIN_UNITS, min(RATE_WARMUP_MAX_UNITS, scaled)))


def adaptive_blended_throughput(
    cached_rate: float | None,
    observed_rate: float | None,
    *,
    observed_units: int,
    warmup_units: int,
) -> float | None:
    cached = _number(cached_rate)
    observed = _number(observed_rate)
    if observed is None or observed <= 0:
        return cached if cached is not None and cached > 0 else None
    if cached is None or cached <= 0:
        return observed
    confidence = min(1.0, max(0, observed_units) / max(1, warmup_units))
    return cached * (1.0 - confidence) + observed * confidence


def blend_history_rate(
    previous_rate: float | None,
    previous_samples: int,
    sample_rate: float,
    *,
    sample_weight: int = 1,
) -> float:
    if previous_rate is None or previous_rate <= 0 or previous_samples <= 0:
        return sample_rate
    weight = min(previous_samples, RATE_HISTORY_WEIGHT)
    return (previous_rate * weight + sample_rate * sample_weight) / (weight + sample_weight)


def adaptive_chunk_cache_path() -> Path:
    return expand_path("~/.cache/forge") / CHUNK_CACHE_NAME


@contextmanager
def locked_adaptive_chunk_history(*, cache_path: Path | None = None):
    path = cache_path or adaptive_chunk_cache_path()
    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def adaptive_chunk_signature(params: dict[str, Any]) -> dict[str, Any]:
    template = str(params.get("__forge_template_name") or "selfplay")
    signature = {"kind": template, "template": template}
    for key, field in (
        ("__forge_worker", "worker"),
        ("__forge_worker_id", "worker_id"),
    ):
        value = str(params.get(key) or "").strip()
        if value:
            signature[field] = value
    return signature


def adaptive_chunk_signature_label(signature: dict[str, Any]) -> str:
    return str(signature.get("template") or signature.get("kind") or "selfplay")


def adaptive_chunk_signature_key(signature: dict[str, Any]) -> str:
    payload = json.dumps(signature, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def load_adaptive_chunk_history(*, cache_path: Path | None = None) -> dict[str, Any]:
    path = cache_path or adaptive_chunk_cache_path()
    try:
        payload = read_json(path)
    except (OSError, SystemExit):
        payload = None
    records = payload.get("records") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != CHUNK_CACHE_SCHEMA
        or not isinstance(records, dict)
    ):
        records = {}
    return {"schema": CHUNK_CACHE_SCHEMA, "records": records}


def write_adaptive_chunk_history(
    payload: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> None:
    path = cache_path or adaptive_chunk_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)


def adaptive_cached_throughput(
    params: dict[str, Any],
    *,
    cache_path: Path | None = None,
) -> float | None:
    signature = adaptive_chunk_signature(params)
    record = load_adaptive_chunk_history(cache_path=cache_path)["records"].get(
        adaptive_chunk_signature_key(signature)
    )
    rate = _number(record.get("rate")) if isinstance(record, dict) else None
    return rate if rate is not None and rate > 0 else None


def record_adaptive_chunk_rate(
    params: dict[str, Any],
    *,
    rate: float,
    samples: int = 1,
    cache_path: Path | None = None,
) -> dict[str, Any] | None:
    if rate <= 0 or samples <= 0:
        return None
    signature = adaptive_chunk_signature(params)
    key = adaptive_chunk_signature_key(signature)
    with locked_adaptive_chunk_history(cache_path=cache_path):
        history = load_adaptive_chunk_history(cache_path=cache_path)
        previous = history["records"].get(key)
        previous_rate = _number(previous.get("rate")) if isinstance(previous, dict) else None
        previous_samples = _count(previous.get("samples")) if isinstance(previous, dict) else 0
        history["records"][key] = {
            "label": adaptive_chunk_signature_label(signature),
            "rate": blend_history_rate(
                previous_rate,
                previous_samples,
                rate,
                sample_weight=samples,
            ),
            "samples": previous_samples + samples,
            "updated_epoch": time.time(),
        }
        write_adaptive_chunk_history(history, cache_path=cache_path)
    return history["records"][key]


def adaptive_template_parameters(manifest: dict[str, Any]) -> dict[str, Any]:
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else {}
    result = dict(params) if isinstance(params, dict) else {}
    name = template.get("name") if isinstance(template, dict) else None
    result["__forge_template_name"] = str(name or manifest.get("kind") or "task")
    return result


def adaptive_process_rate_path(manifest: dict[str, Any], worker_id: str) -> Path:
    digest = hashlib.sha256(worker_id.encode("utf-8")).hexdigest()[:16]
    return state_dir(manifest) / PROCESS_RATE_DIR / f"{digest}.json"


def _process_rate_payload(
    manifest: dict[str, Any],
    worker_id: str | None,
) -> dict[str, Any] | None:
    process_id = str(worker_id or "").strip()
    if not process_id:
        return None
    path = adaptive_process_rate_path(manifest, process_id)
    try:
        payload = read_json(path) if path.exists() else None
    except (OSError, SystemExit):
        return None
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != PROCESS_RATE_SCHEMA
        or str(payload.get("worker_id") or "") != process_id
    ):
        return None
    return payload


def adaptive_process_cached_throughput(
    manifest: dict[str, Any],
    worker_id: str | None,
) -> float | None:
    payload = _process_rate_payload(manifest, worker_id)
    rate = _number(payload.get("rate")) if payload is not None else None
    return rate if rate is not None and rate > 0 else None


def adaptive_worker_cached_throughput(
    manifest: dict[str, Any],
    *,
    worker: str | None,
    worker_id: str | None,
    cache_path: Path | None = None,
) -> float | None:
    worker_name = str(worker or "").strip()
    process_id = str(worker_id or "").strip()
    params = adaptive_template_parameters(manifest)
    rates = [adaptive_process_cached_throughput(manifest, process_id)]
    if worker_name and process_id:
        rates.append(adaptive_cached_throughput(
            {
                **params,
                "__forge_worker": worker_name,
                "__forge_worker_id": process_id,
            },
            cache_path=cache_path,
        ))
    if worker_name:
        rates.append(adaptive_cached_throughput(
            {**params, "__forge_worker": worker_name},
            cache_path=cache_path,
        ))
    return next((rate for rate in rates if rate is not None and rate > 0), None)


def record_adaptive_process_chunk_rate(
    manifest: dict[str, Any],
    *,
    worker: str,
    worker_id: str,
    rate: float,
    units: int,
    elapsed_s: float,
    started_at: Any = None,
) -> dict[str, Any] | None:
    if not worker_id or rate <= 0 or units <= 0 or elapsed_s <= 0:
        return None
    previous = _process_rate_payload(manifest, worker_id)
    previous_rate = _number(previous.get("rate")) if previous is not None else None
    previous_samples = _count(previous.get("samples")) if previous is not None else 0
    sample_rate = rate
    rate = blend_history_rate(previous_rate, previous_samples, sample_rate)

    template = manifest.get("template")
    template_name = (
        template.get("name")
        if isinstance(template, dict)
        else None
    ) or manifest.get("kind") or "task"
    payload = {
        "schema": PROCESS_RATE_SCHEMA,
        "template": str(template_name),
        "worker": worker,
        "worker_id": worker_id,
        "rate": rate,
        "samples": previous_samples + 1,
        "last_rate": sample_rate,
        "last_units": units,
        "last_elapsed_s": elapsed_s,
        "last_started_at": started_at,
        "updated_epoch": time.time(),
    }
    write_json(adaptive_process_rate_path(manifest, worker_id), payload)
    return payload


# The on-disk format predates generic adaptive chunking. Keep these names as a
# compatibility surface while new code uses the adaptive names above.
SELFPLAY_CHUNK_CACHE_SCHEMA = CHUNK_CACHE_SCHEMA
SELFPLAY_CHUNK_CACHE_NAME = CHUNK_CACHE_NAME
selfplay_chunk_cache_path = adaptive_chunk_cache_path
selfplay_chunk_signature = adaptive_chunk_signature
selfplay_chunk_signature_label = adaptive_chunk_signature_label
selfplay_chunk_signature_key = adaptive_chunk_signature_key
load_selfplay_chunk_history = load_adaptive_chunk_history
write_selfplay_chunk_history = write_adaptive_chunk_history
selfplay_cached_throughput = adaptive_cached_throughput
