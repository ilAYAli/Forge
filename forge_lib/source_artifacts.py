from __future__ import annotations

import fnmatch
import hashlib
import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from .config import cache_dir
from .core import write_json


SOURCE_ARTIFACT_SCHEMA = "forge.source-artifact.v1"
SOURCE_ARTIFACT_MARKER = ".forge-source.json"


def _safe_relative_path(value: str) -> Path:
    posix = PurePosixPath(value)
    if not value or posix.is_absolute() or ".." in posix.parts:
        raise SystemExit(f"unsafe source path: {value!r}")
    return Path(*posix.parts)


def _matches(path: str, patterns: list[str]) -> bool:
    return any(
        fnmatch.fnmatch(path, pattern)
        or fnmatch.fnmatch(path + "/", pattern.rstrip("/") + "/")
        for pattern in patterns
    )


def _source_files(
    root: Path,
    *,
    include: list[str],
    exclude: list[str],
) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        parts = PurePosixPath(relative).parts
        if ".git" in parts or ".forge-source.json" in parts:
            continue
        if exclude and _matches(relative, exclude):
            continue
        if include and not _matches(relative, include):
            continue
        if path.is_file() or path.is_symlink():
            files.append(path)
    return sorted(files, key=lambda item: item.relative_to(root).as_posix())


def _copy_and_digest(source: Path, stage: Path, files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in files:
        relative_text = path.relative_to(source).as_posix()
        relative = _safe_relative_path(relative_text)
        destination = stage / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest.update(relative_text.encode("utf-8", errors="surrogateescape") + b"\0")
        if path.is_symlink():
            target = os.readlink(path)
            target_path = PurePosixPath(target)
            if target_path.is_absolute() or ".." in target_path.parts:
                raise SystemExit(
                    f"{source}: source symlink escapes directory: "
                    f"{relative_text!r} -> {target!r}"
                )
            destination.symlink_to(target)
            digest.update(b"L\0" + target.encode("utf-8", errors="surrogateescape"))
            continue
        executable = bool(path.stat().st_mode & 0o111)
        digest.update(b"F\0" + (b"X" if executable else b"-"))
        with path.open("rb") as input_file, destination.open("wb") as output_file:
            for chunk in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(chunk)
                output_file.write(chunk)
        shutil.copymode(path, destination)
    return digest.hexdigest()


def source_artifact_matches(root: Path, *, digest: str) -> bool:
    try:
        import json

        marker = json.loads(
            (root / SOURCE_ARTIFACT_MARKER).read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return False
    return (
        marker.get("schema") == SOURCE_ARTIFACT_SCHEMA
        and marker.get("digest") == digest
    )


def build_source_artifact(
    source: Path,
    *,
    name: str,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    cache_home: Path | None = None,
) -> dict[str, str]:
    source = source.resolve()
    if not source.is_dir():
        raise SystemExit(f"source {name!r} is not a directory: {source}")
    includes = [str(item) for item in (include or [])]
    excludes = [str(item) for item in (exclude or [])]
    files = _source_files(source, include=includes, exclude=excludes)
    if not files:
        raise SystemExit(f"source {name!r} contains no matching files: {source}")

    cache_home = cache_home or cache_dir()
    safe_name = "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in name
    ).strip("-") or "source"
    artifact_root = cache_home / "source-artifacts" / safe_name
    artifact_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".stage.", dir=artifact_root))
    try:
        digest = _copy_and_digest(source, stage, files)
        target = artifact_root / digest
        write_json(stage / SOURCE_ARTIFACT_MARKER, {
            "schema": SOURCE_ARTIFACT_SCHEMA,
            "digest": digest,
            "name": name,
        })
        if source_artifact_matches(target, digest=digest):
            shutil.rmtree(stage)
        else:
            if target.exists():
                shutil.rmtree(target)
            os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {
        "path": str(target),
        "digest": digest,
        "source_id": digest[:16],
    }


def attach_source_artifacts(
    template: dict[str, Any],
    manifest: dict[str, Any],
    params: dict[str, Any],
) -> None:
    specs = template.get("parameters")
    requirements = manifest.get("requirements")
    if not isinstance(specs, dict) or not isinstance(requirements, dict):
        return
    inputs = requirements.setdefault("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for name, spec in specs.items():
        if not isinstance(spec, dict) or str(spec.get("type") or "") != "source":
            continue
        key = str(name)
        artifact = params.get(f"{key}_artifact")
        destination = params.get(key)
        digest = params.get(f"{key}_digest")
        if not all(isinstance(value, str) and value for value in (
            artifact, destination, digest
        )):
            raise SystemExit(f"{key}: unresolved source artifact")
        inputs.append({
            "name": f"{key} source",
            "source": artifact,
            "path": destination,
            "tree": "source-artifact",
            "digest": digest,
        })
