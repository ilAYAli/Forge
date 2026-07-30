from __future__ import annotations

import hashlib
import functools
import json
from pathlib import Path
from typing import Any

from .core import expand_path


FILE_SET_TREE = "file-set"
FILE_SET_MARKER = ".forge-file-set.json"


@functools.lru_cache(maxsize=16)
def _pgn_entries(
    source_text: str,
    directory_mtime_ns: int,
) -> tuple[dict[str, Any], ...]:
    del directory_mtime_ns
    source = Path(source_text)
    entries = []
    for path in sorted(item for item in source.glob("*.pgn") if item.is_file()):
        stat = path.stat()
        entries.append({
            "path": path.relative_to(source).as_posix(),
            "bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        })
    return tuple(entries)


def _file_set_digest(source: Path, files: list[dict[str, Any]]) -> str:
    payload = {
        "source": str(source.resolve()),
        "files": files,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def attach_pgn_task_inputs(manifest: dict[str, Any]) -> None:
    template = manifest.get("template")
    params = template.get("parameters") if isinstance(template, dict) else None
    tasks = manifest.get("tasks")
    if not isinstance(params, dict) or not isinstance(tasks, list):
        raise ValueError("PGN conversion manifest is missing parameters or tasks")
    source_value = params.get("pgn_dir") or params.get("input")
    if not isinstance(source_value, str) or not source_value:
        raise ValueError("PGN conversion manifest is missing input")
    source = expand_path(source_value).resolve()
    try:
        entries = list(_pgn_entries(str(source), source.stat().st_mtime_ns))
    except OSError as exc:
        raise ValueError(f"cannot scan PGN input {source}: {exc}") from exc
    if not entries:
        raise ValueError(f"PGN input contains no *.pgn files: {source}")

    for task in tasks:
        if not isinstance(task, dict):
            raise ValueError("PGN conversion task must be an object")
        adaptive_range = task.get("adaptive_range")
        if not isinstance(adaptive_range, dict):
            raise ValueError(f"{task.get('id', 'task')}: PGN task has no adaptive range")
        start = int(adaptive_range.get("start", -1))
        count = int(adaptive_range.get("count", 0))
        selected = entries[start:start + count]
        if start < 0 or count <= 0 or len(selected) != count:
            raise ValueError(
                f"{task.get('id', 'task')}: invalid PGN range {start}/{count}"
            )
        digest = _file_set_digest(source, selected)
        worker_root = f"~/.cache/forge/task-inputs/pgn/{digest}"
        task["inputs"] = [{
            "name": "self-play PGNs",
            "source": str(source),
            "path": worker_root,
            "tree": FILE_SET_TREE,
            "digest": digest,
            "files": [
                {"path": item["path"], "bytes": item["bytes"]}
                for item in selected
            ],
            "bytes": sum(int(item["bytes"]) for item in selected),
        }]
        task["command"] = str(task.get("command") or "").replace(
            "{task_input}", worker_root
        )

    requirements = manifest.get("requirements")
    if isinstance(requirements, dict):
        requirements["inputs"] = [
            item
            for item in requirements.get("inputs", [])
            if not (
                isinstance(item, dict)
                and item.get("tree") == "directory"
                and str(item.get("name") or "") in {"self-play PGN directory", "PGN directory"}
            )
        ]


def file_set_marker_matches(root: Path, digest: str) -> bool:
    try:
        payload = json.loads((root / FILE_SET_MARKER).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return payload.get("digest") == digest
