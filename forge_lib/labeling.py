from __future__ import annotations

import hashlib
import json
import os
import shutil
import struct
import sys
import time
from pathlib import Path
from typing import Any

from .core import expand_path, now, schema_matches, sha256_file, write_json
from .tasks import task_outputs


INVENTORY_SCHEMA = "forge.lc0-inventory.v1"
STATS_SCHEMA = "enyo.label-stats.v1"
RECORD_BYTES = 32
HASH_CACHE_NAME = "lc0-inventory-hashes.json"
VERIFIED_MARKER_NAME = ".verified.json"
LC0_V6_RECORD_BYTES = 8356


def canonical_inventory_bytes(files: list[dict[str, Any]]) -> bytes:
    payload = {"schema": INVENTORY_SCHEMA, "files": files}
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def inventory_digest(files: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_inventory_bytes(files)).hexdigest()


def load_hash_cache(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    entries = payload.get("entries")
    return entries if isinstance(entries, dict) else {}


def build_inventory(source: str | Path, *, cache_home: Path | None = None) -> dict[str, Any]:
    source_path = expand_path(str(source)).resolve()
    if not source_path.is_dir():
        raise SystemExit(f"LC0 source is not a directory: {source_path}")
    cache_home = cache_home or expand_path("~/.cache/forge")
    hash_cache_path = cache_home / HASH_CACHE_NAME
    hash_cache = load_hash_cache(hash_cache_path)
    updated_cache: dict[str, Any] = dict(hash_cache)
    files: list[dict[str, Any]] = []
    source_key = str(source_path)
    print(f"Scanning LC0 source: {source_path} ...", file=sys.stderr, flush=True)
    all_paths: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(source_path):
        dirnames.sort()
        dp = Path(dirpath)
        for name in sorted(fn for fn in filenames if fn.startswith("training.")):
            all_paths.append(dp / name)
    n_total = len(all_paths)
    n_cached = sum(
        1 for p in all_paths
        if isinstance(hash_cache.get(f"{source_key}\0{p.relative_to(source_path).as_posix()}"), dict)
    )
    print(f"  found {n_total:,} files ({n_cached:,} cached)", file=sys.stderr, flush=True)
    last_report = time.monotonic()
    n_hashed = 0
    for path in all_paths:
        relative = path.relative_to(source_path).as_posix()
        stat = path.stat()
        key = f"{source_key}\0{relative}"
        cached = hash_cache.get(key)
        digest = ""
        if isinstance(cached, dict):
            if cached.get("size") == stat.st_size and cached.get("mtime_ns") == stat.st_mtime_ns:
                digest = str(cached.get("sha256") or "")
        if len(digest) != 64:
            digest = sha256_file(path)
            n_hashed += 1
            now_ = time.monotonic()
            if now_ - last_report >= 5.0:
                pct = 100 * len(files) // n_total if n_total else 0
                print(f"  hashed {n_hashed:,} new files ({pct}% done) ...", file=sys.stderr, flush=True)
                last_report = now_
        updated_cache[key] = {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "sha256": digest,
        }
        files.append({"path": relative, "size": stat.st_size, "sha256": digest})
    if n_hashed:
        print(f"  hashed {n_hashed:,} new files (done)", file=sys.stderr, flush=True)
    if not files:
        raise SystemExit(f"LC0 source contains no training.* files: {source_path}")
    cache_home.mkdir(parents=True, exist_ok=True)
    write_json(hash_cache_path, {"schema": INVENTORY_SCHEMA, "entries": updated_cache})

    digest = inventory_digest(files)
    cache_root = cache_home / "inputs" / digest
    cache_root.mkdir(parents=True, exist_ok=True)
    inventory_path = cache_root / "inventory.json"
    inventory_path.write_bytes(canonical_inventory_bytes(files) + b"\n")
    for item in files:
        relative = Path(str(item["path"]))
        target = cache_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        source_file = source_path / relative
        if target.is_symlink() and target.resolve() == source_file:
            continue
        target.unlink(missing_ok=True)
        target.symlink_to(source_file)
    if not marker_matches(cache_root, digest):
        marker = verify_inventory_tree(cache_root, digest)
    else:
        marker = json.loads((cache_root / VERIFIED_MARKER_NAME).read_text(encoding="utf-8"))
    return {
        "digest": digest,
        "files": files,
        "bytes": marker["bytes"],
        "cache_root": cache_root,
        "inventory": inventory_path,
        "marker": marker,
        "source": source_path,
    }

def inventory_file_records(path: Path) -> int:
    if path.name.endswith(".gz"):
        with path.open("rb") as handle:
            handle.seek(-4, os.SEEK_END)
            uncompressed_bytes = struct.unpack("<I", handle.read(4))[0]
    else:
        uncompressed_bytes = path.stat().st_size
    if uncompressed_bytes % LC0_V6_RECORD_BYTES:
        raise ValueError(
            f"{path}: uncompressed size {uncompressed_bytes} is not divisible "
            f"by LC0 V6 record size {LC0_V6_RECORD_BYTES}"
        )
    return uncompressed_bytes // LC0_V6_RECORD_BYTES

def split_record_limit(total: int, count: int, index: int) -> int:
    if count <= 0 or not 0 <= index < count:
        raise ValueError("invalid label task index/count")
    base, remainder = divmod(max(0, total), count)
    return base + int(index < remainder)

def build_inventory_subset(
    inventory_info: dict[str, Any],
    files: list[dict[str, Any]],
    *,
    cache_home: Path | None = None,
) -> dict[str, Any]:
    cache_home = cache_home or inventory_info.get("cache_home") or expand_path("~/.cache/forge")
    cache_home = Path(cache_home)
    source = Path(inventory_info["source"])
    digest = inventory_digest(files)
    cache_root = cache_home / "task-inputs" / digest
    cache_root.mkdir(parents=True, exist_ok=True)
    inventory_path = cache_root / "inventory.json"
    inventory_path.write_bytes(canonical_inventory_bytes(files) + b"\n")
    for item in files:
        relative = Path(str(item["path"]))
        target = cache_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        source_file = source / relative
        if target.is_symlink() and target.resolve() == source_file:
            continue
        target.unlink(missing_ok=True)
        target.symlink_to(source_file)
    marker = {
        "schema": INVENTORY_SCHEMA,
        "digest": digest,
        "files": len(files),
        "bytes": sum(int(item["size"]) for item in files),
        "verified_at": now(),
    }
    write_json(cache_root / VERIFIED_MARKER_NAME, marker)
    return {
        "digest": digest,
        "files": files,
        "bytes": marker["bytes"],
        "cache_root": cache_root,
        "inventory": inventory_path,
        "marker": marker,
        "source": source,
    }

def attach_label_task_inputs(
    manifest: dict[str, Any],
    inventory_info: dict[str, Any],
    *,
    cache_home: Path | None = None,
) -> None:
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else None
    tasks = manifest.get("tasks")
    files = inventory_info.get("files")
    if not isinstance(params, dict) or not isinstance(tasks, list) or not isinstance(files, list):
        raise ValueError("label manifest is missing parameters, tasks, or inventory files")
    task_count = len(tasks)
    if task_count <= 0:
        raise ValueError("label manifest has no tasks")
    max_records = int(params.get("max_records") or 0)
    source = Path(inventory_info["source"])
    record_counts = {
        str(item["path"]): inventory_file_records(source / str(item["path"]))
        for item in files
    }
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError("label manifest task must be an object")
        quota = split_record_limit(max_records, task_count, index)
        selected: list[dict[str, Any]] = []
        selected_records = 0
        for item in files[index::task_count]:
            selected.append(item)
            selected_records += record_counts[str(item["path"])]
            if quota > 0 and selected_records >= quota:
                break
        if not selected:
            raise ValueError(f"{task.get('id', index)}: no LC0 input files assigned")
        subset = build_inventory_subset(inventory_info, selected, cache_home=cache_home)
        worker_root = f"~/.cache/forge/task-inputs/{subset['digest']}"
        task["inputs"] = [{
            "name": "LC0 task input",
            "source": str(subset["cache_root"]),
            "path": worker_root,
            "tree": "lc0-inventory",
            "digest": subset["digest"],
            "bytes": subset["bytes"],
            "files": len(selected),
        }]
        command = str(task.get("command") or "")
        command = command.replace("{task_input}", worker_root)
        command = command.replace("{task_inventory}", worker_root + "/inventory.json")
        command = command.replace("{task_records}", str(quota))
        task["command"] = command
    requirements = manifest.get("requirements")
    if isinstance(requirements, dict):
        requirements["inputs"] = [
            item
            for item in requirements.get("inputs", [])
            if not isinstance(item, dict) or item.get("tree") != "lc0-inventory"
        ]


def verify_inventory_tree(root: Path, digest: str) -> dict[str, Any]:
    inventory = json.loads((root / "inventory.json").read_text(encoding="utf-8"))
    files = inventory.get("files")
    if not schema_matches(inventory.get("schema"), "lc0-inventory") or not isinstance(files, list):
        raise ValueError("invalid LC0 inventory")
    if inventory_digest(files) != digest:
        raise ValueError("LC0 inventory digest mismatch")
    total = 0
    for item in files:
        path = root / str(item["path"])
        size = path.stat().st_size
        if size != int(item["size"]):
            raise ValueError(f"LC0 cache size mismatch: {item['path']}")
        if sha256_file(path) != item["sha256"]:
            raise ValueError(f"LC0 cache checksum mismatch: {item['path']}")
        total += size
    marker = {
        "schema": INVENTORY_SCHEMA,
        "digest": digest,
        "files": len(files),
        "bytes": total,
        "verified_at": now(),
    }
    write_json(root / VERIFIED_MARKER_NAME, marker)
    return marker


def marker_matches(root: Path, digest: str) -> bool:
    try:
        marker = json.loads((root / VERIFIED_MARKER_NAME).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return schema_matches(marker.get("schema"), "lc0-inventory") and marker.get("digest") == digest


def cleanup_partials(manifest: dict[str, Any], finalizer: dict[str, Any]) -> None:
    paths = [path for task in manifest["tasks"] for path in task_outputs(task)]
    output = expand_path(str(finalizer.get("output") or ""))
    if str(output):
        paths.append(output)
    for path in paths:
        for partial in path.parent.glob(f".{path.name}.partial.*"):
            partial.unlink(missing_ok=True)


def finalize_labeling(manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any] | None:
    finalizer = manifest.get("finalize")
    if not isinstance(finalizer, dict) or finalizer.get("type") != "bullet-labels":
        return None
    output = expand_path(str(finalizer["output"]))
    summary_path = manifest_path.parent / "dataset-summary.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not bool(finalizer.get("replace", False)):
        raise ValueError(f"{output}: exists; pass --replace to overwrite")

    ranged = all(
        isinstance(task.get("adaptive_range"), dict)
        for task in manifest["tasks"]
    )
    task_scoped_inputs = all(
        isinstance(task.get("inputs"), list) and bool(task["inputs"])
        for task in manifest["tasks"]
    )
    tasks = sorted(
        manifest["tasks"],
        key=lambda task: int(
            task["adaptive_range"].get("start", 0)
            if ranged
            else task.get("index", 0)
        ),
    )
    totals: dict[str, int] = {}
    expected_bytes = 0
    expected_positions = 0
    shard_rows = []
    expected_start = 0
    for expected_index, task in enumerate(tasks):
        adaptive_range = task.get("adaptive_range")
        if ranged:
            start = int(adaptive_range.get("start", -1))
            count = int(adaptive_range.get("count", 0))
            if start != expected_start or count <= 0:
                raise ValueError("labeling adaptive ranges are not contiguous")
            expected_start += count
        elif int(task.get("index", -1)) != expected_index:
            raise ValueError("labeling shard indices are not contiguous")
        outputs = task_outputs(task)
        if len(outputs) != 2:
            raise ValueError(f"{task['id']}: expected Bullet and stats outputs")
        bullet, stats_path = outputs
        size = bullet.stat().st_size
        if size % RECORD_BYTES:
            raise ValueError(f"{task['id']}: invalid Bullet shard size {size}")
        stats = json.loads(stats_path.read_text(encoding="utf-8"))
        if stats.get("schema") != STATS_SCHEMA:
            raise ValueError(f"{task['id']}: invalid stats schema")
        written = int(stats.get("written", -1))
        if written != size // RECORD_BYTES:
            raise ValueError(f"{task['id']}: written count does not match bytes")
        if (
            not ranged
            and not task_scoped_inputs
            and int(stats.get("shard_index", -1)) != expected_index
        ):
            raise ValueError(f"{task['id']}: stats shard index mismatch")
        expected_bytes += size
        expected_positions += written
        for key, value in stats.items():
            if key in {"read", "selected", "written"} or key.startswith("skipped_"):
                if isinstance(value, int):
                    totals[key] = totals.get(key, 0) + value
        shard_row = {"index": expected_index, "bytes": size, "written": written}
        if ranged:
            shard_row.update({"start": start, "count": count})
        shard_rows.append(shard_row)

    temp = output.with_name(f".{output.name}.partial.{os.getpid()}")
    temp.unlink(missing_ok=True)
    try:
        with temp.open("wb") as destination:
            for task in tasks:
                with task_outputs(task)[0].open("rb") as source:
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
            destination.flush()
            os.fsync(destination.fileno())
        if temp.stat().st_size != expected_bytes:
            raise ValueError("merged Bullet byte count mismatch")
        os.replace(temp, output)
        directory_fd = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)

    summary = {
        "schema": "forge.label-summary.v1",
        "completed_at": now(),
        "output": str(output),
        "merged_bytes": expected_bytes,
        "positions": expected_positions,
        "shard_count": len(tasks),
        "inventory_digest": str(finalizer.get("inventory_digest") or ""),
        "task_totals": totals,
        "shards": shard_rows,
    }
    write_json(summary_path, summary)
    return summary
