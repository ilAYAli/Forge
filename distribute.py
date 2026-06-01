#!/usr/bin/env python3
"""Generic deterministic shard runner for CPU/IO-heavy work."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any


SCHEMA = "distribute.task.v1"


def expand_path(value: str | Path) -> Path:
    return Path(os.path.expandvars(str(value))).expanduser().resolve()


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def epoch() -> float:
    return time.time()


def manifest_checksum(manifest: dict[str, Any]) -> str:
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: expected JSON object")
    return data


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def translate_string(value: str, path_map: dict[str, str]) -> str:
    out = value
    for src, dst in sorted(path_map.items(), key=lambda item: len(item[0]), reverse=True):
        out = out.replace(src, dst)
    return out


def translate_obj(value: Any, path_map: dict[str, str]) -> Any:
    if isinstance(value, str):
        return translate_string(value, path_map)
    if isinstance(value, list):
        return [translate_obj(item, path_map) for item in value]
    if isinstance(value, dict):
        return {key: translate_obj(item, path_map) for key, item in value.items()}
    return value


def apply_profile(manifest: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return manifest
    path_maps = manifest.get("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    selected = path_maps.get(profile)
    if selected is None:
        raise SystemExit(f"unknown profile {profile!r}")
    if not isinstance(selected, dict):
        raise SystemExit(f"path_maps.{profile} must be an object")
    return translate_obj(copy.deepcopy(manifest), {str(k): str(v) for k, v in selected.items()})


def parse_version(text: str) -> tuple[int, ...]:
    match = re.search(r"(\d+(?:\.\d+)+)", text)
    if not match:
        return ()
    return tuple(int(part) for part in match.group(1).split("."))


def require_min_version(label: str, actual: str, minimum: str) -> None:
    actual_v = parse_version(actual)
    min_v = parse_version(minimum)
    if not actual_v or actual_v < min_v:
        raise SystemExit(f"{label} {actual!r} is below required {minimum}")


def git_output(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), *args],
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


def command_output(command: list[str]) -> str:
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = read_json(path)
    if manifest.get("schema") != SCHEMA:
        raise SystemExit(f"{path}: unsupported schema {manifest.get('schema')!r}")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        raise SystemExit(f"{path}: tasks must be a list")
    seen: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict):
            raise SystemExit(f"{path}: each task must be an object")
        task_id = str(task.get("id", ""))
        if not task_id:
            raise SystemExit(f"{path}: task is missing id")
        if task_id in seen:
            raise SystemExit(f"{path}: duplicate task id {task_id}")
        seen.add(task_id)
    return manifest


def manifest_base(manifest: dict[str, Any], key: str, fallback: str) -> Path:
    return expand_path(str(manifest.get(key) or fallback))


def state_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "state_dir", "state")


def log_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "log_dir", "logs")


def claim_dir(manifest: dict[str, Any]) -> Path:
    return expand_path(str(manifest.get("claim_dir") or (state_dir(manifest).parent / "claims")))


def work_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "work_dir", ".")


def find_task(manifest: dict[str, Any], *, task_id: str | None, index: int | None) -> dict[str, Any]:
    tasks = manifest["tasks"]
    matches = []
    for task in tasks:
        if task_id is not None and task.get("id") == task_id:
            matches.append(task)
        elif index is not None and int(task.get("index", -1)) == index:
            matches.append(task)
    if not matches:
        target = task_id if task_id is not None else index
        raise SystemExit(f"no task matches {target!r}")
    if len(matches) > 1:
        raise SystemExit("task selector matched multiple tasks")
    return matches[0]


def check_requirements(manifest: dict[str, Any]) -> None:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")

    python_min = str(req.get("python_min", "")).strip()
    if python_min:
        actual = ".".join(map(str, sys.version_info[:3]))
        require_min_version("python", actual, python_min)

    git_min = str(req.get("git_min", "")).strip()
    if git_min:
        actual = command_output(["git", "--version"])
        require_min_version("git", actual, git_min)

    git_req = req.get("git")
    if isinstance(git_req, dict):
        repo = expand_path(str(git_req.get("repo") or manifest.get("work_dir") or "."))
        expected_commit = str(git_req.get("commit") or "").strip()
        if expected_commit:
            actual_commit = git_output(repo, "rev-parse", "HEAD")
            if actual_commit != expected_commit:
                raise SystemExit(
                    f"{repo}: git commit {actual_commit} != required {expected_commit}"
                )
        if bool(git_req.get("require_clean", False)):
            status = git_output(repo, "status", "--porcelain")
            if status:
                raise SystemExit(f"{repo}: git worktree is not clean")

    inputs = req.get("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for item in inputs:
        if not isinstance(item, dict):
            raise SystemExit("requirements.inputs entries must be objects")
        path = expand_path(str(item.get("path", "")))
        if not path.exists():
            raise SystemExit(f"required input missing: {path}")
        expected_sha = str(item.get("sha256") or "").strip()
        if expected_sha and sha256_file(path) != expected_sha:
            raise SystemExit(f"required input checksum mismatch: {path}")


def task_done_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.done.json"


def task_fail_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.fail.json"


def task_running_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.running.json"


def task_claim_dir(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return claim_dir(manifest) / f"{task['id']}.claim"


def task_claim_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return task_claim_dir(manifest, task) / "claim.json"


def task_heartbeat_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return task_claim_dir(manifest, task) / "heartbeat.json"


def task_stale_lock_dir(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return claim_dir(manifest) / f".{task['id']}.stale.lock"


def task_log_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return log_dir(manifest) / f"{task['id']}.log"


def task_outputs(task: dict[str, Any]) -> list[Path]:
    outputs = task.get("outputs", [])
    if not isinstance(outputs, list):
        raise SystemExit(f"{task['id']}: outputs must be a list")
    return [expand_path(str(path)) for path in outputs]


def format_command(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    vars_obj = manifest.get("vars", {})
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    outputs = task_outputs(task)
    values: dict[str, Any] = {
        "task_id": task["id"],
        "index": task.get("index", ""),
        "shards": len(manifest["tasks"]),
        "output": str(outputs[0]) if outputs else "",
    }
    values.update({str(k): v for k, v in vars_obj.items()})
    values.update({str(k): v for k, v in task.items() if k not in {"command"}})
    command = str(task.get("command") or "")
    if not command:
        raise SystemExit(f"{task['id']}: missing command")
    return command.format(**values)


def task_complete(manifest: dict[str, Any], task: dict[str, Any]) -> bool:
    if not task_done_path(manifest, task).exists():
        return False
    return all(path.exists() for path in task_outputs(task))


def task_claim_state(manifest: dict[str, Any], task: dict[str, Any], *, lease_seconds: int) -> str | None:
    path = task_claim_path(manifest, task)
    if not path.exists():
        return None
    try:
        payload = read_json(path)
    except (OSError, json.JSONDecodeError):
        return "claimed"
    heartbeat = task_heartbeat_path(manifest, task)
    heartbeat_epoch = float(payload.get("heartbeat_epoch") or payload.get("claimed_epoch") or 0)
    if heartbeat.exists():
        try:
            heartbeat_payload = read_json(heartbeat)
            heartbeat_epoch = float(heartbeat_payload.get("heartbeat_epoch") or heartbeat_epoch)
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    if lease_seconds > 0 and heartbeat_epoch and epoch() - heartbeat_epoch > lease_seconds:
        return "stale"
    return "claimed"


def task_state(manifest: dict[str, Any], task: dict[str, Any], *, lease_seconds: int = 600) -> str:
    if task_complete(manifest, task):
        return "done"
    if task_fail_path(manifest, task).exists():
        return "fail"
    if task_running_path(manifest, task).exists():
        return "running"
    claim_state = task_claim_state(manifest, task, lease_seconds=lease_seconds)
    if claim_state:
        return claim_state
    return "pending"


def terminate_process_group(proc: subprocess.Popen[Any], log: Any, reason: str) -> None:
    log.write(f"{now()} terminate {reason}: pid={proc.pid}\n")
    log.flush()
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        log.write(f"{now()} kill {reason}: pid={proc.pid}\n")
        log.flush()
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log.write(f"{now()} process still alive after SIGKILL {reason}: pid={proc.pid}\n")
        log.flush()


def run_task(manifest: dict[str, Any], task: dict[str, Any], *, force: bool) -> int:
    if task_complete(manifest, task) and not force:
        print(f"skip {task['id']}: already complete")
        return 0

    check_requirements(manifest)
    state_dir(manifest).mkdir(parents=True, exist_ok=True)
    log_dir(manifest).mkdir(parents=True, exist_ok=True)
    for output in task_outputs(task):
        output.parent.mkdir(parents=True, exist_ok=True)

    task_fail_path(manifest, task).unlink(missing_ok=True)
    task_done_path(manifest, task).unlink(missing_ok=True)
    command = format_command(manifest, task)
    log_path = task_log_path(manifest, task)
    start = time.monotonic()
    started_at = now()
    running = {
        "task": task["id"],
        "host": socket.gethostname(),
        "started_at": started_at,
        "command": command,
        "pid": os.getpid(),
    }
    write_json(task_running_path(manifest, task), running)

    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"{started_at} start {task['id']}: {command}\n")
        log.flush()
        proc = subprocess.Popen(
            ["bash", "-lc", command],
            cwd=work_dir(manifest),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        timeout = task.get("timeout_s", manifest.get("timeout_s"))
        try:
            rc = proc.wait(timeout=float(timeout)) if timeout else proc.wait()
        except subprocess.TimeoutExpired:
            terminate_process_group(proc, log, "timeout")
            rc = 124
        except KeyboardInterrupt:
            terminate_process_group(proc, log, "interrupt")
            rc = 130
        finished_at = now()
        log.write(f"{finished_at} done {task['id']} rc={rc}\n")

    elapsed = round(time.monotonic() - start, 3)
    task_running_path(manifest, task).unlink(missing_ok=True)
    payload = {
        "schema": SCHEMA,
        "task": task["id"],
        "index": task.get("index"),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_s": elapsed,
        "command": command,
        "rc": rc,
        "log": str(log_path),
        "manifest_sha256": manifest_checksum(manifest),
        "outputs": [file_record(path) for path in task_outputs(task) if path.exists()],
    }
    if rc == 0 and all(path.exists() for path in task_outputs(task)):
        write_json(task_done_path(manifest, task), payload)
    else:
        write_json(task_fail_path(manifest, task), payload)
    return rc


def claim_task(
    manifest: dict[str, Any],
    *,
    worker: str,
    profile: str | None,
    lease_seconds: int,
    retry_failed: bool,
) -> dict[str, Any] | None:
    claim_dir(manifest).mkdir(parents=True, exist_ok=True)
    digest = manifest_checksum(manifest)
    for task in sorted(manifest["tasks"], key=lambda item: int(item.get("index", 0))):
        if task_complete(manifest, task):
            continue
        if task_fail_path(manifest, task).exists() and not retry_failed:
            continue
        claim_state = task_claim_state(manifest, task, lease_seconds=lease_seconds)
        task_claim = task_claim_dir(manifest, task)
        if claim_state == "claimed":
            continue
        if claim_state == "stale":
            if not remove_stale_claim(manifest, task, lease_seconds=lease_seconds):
                continue
        try:
            task_claim.mkdir(parents=True)
        except FileExistsError:
            continue
        payload = {
            "schema": SCHEMA,
            "task": task["id"],
            "index": task.get("index"),
            "claim_id": uuid.uuid4().hex,
            "worker": worker,
            "profile": profile,
            "claimed_at": now(),
            "claimed_epoch": epoch(),
            "heartbeat_epoch": epoch(),
            "lease_seconds": lease_seconds,
            "manifest_sha256": digest,
        }
        write_json(task_claim_path(manifest, task), payload)
        write_json(task_heartbeat_path(manifest, task), payload)
        return {"task": task["id"], "index": task.get("index"), "claim": payload}
    return None


def remove_stale_claim(manifest: dict[str, Any], task: dict[str, Any], *, lease_seconds: int) -> bool:
    lock = task_stale_lock_dir(manifest, task)
    try:
        lock.mkdir(parents=True)
    except FileExistsError:
        return False
    try:
        if task_claim_state(manifest, task, lease_seconds=lease_seconds) != "stale":
            return False
        shutil.rmtree(task_claim_dir(manifest, task), ignore_errors=True)
        return True
    finally:
        shutil.rmtree(lock, ignore_errors=True)


def check_claim_owner(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    worker: str | None,
    claim_id: str | None,
) -> dict[str, Any]:
    claim_path = task_claim_path(manifest, task)
    if not claim_path.exists():
        raise SystemExit(f"{task['id']}: no active claim")
    payload = read_json(claim_path)
    expected_worker = str(payload.get("worker") or "")
    if worker and expected_worker and expected_worker != worker:
        raise SystemExit(f"{task['id']}: claimed by {expected_worker}, not {worker}")
    expected_claim = str(payload.get("claim_id") or "")
    if claim_id and expected_claim and expected_claim != claim_id:
        raise SystemExit(f"{task['id']}: claim id mismatch")
    return payload


def release_claim(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    worker: str | None = None,
    claim_id: str | None = None,
) -> None:
    if worker or claim_id:
        check_claim_owner(manifest, task, worker=worker, claim_id=claim_id)
    shutil.rmtree(task_claim_dir(manifest, task), ignore_errors=True)


def update_claim_heartbeat(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    worker: str,
    claim_id: str | None,
) -> None:
    payload = check_claim_owner(manifest, task, worker=worker, claim_id=claim_id)
    payload["heartbeat_at"] = now()
    payload["heartbeat_epoch"] = epoch()
    write_json(task_heartbeat_path(manifest, task), payload)


def parse_coordinator(value: str) -> tuple[str | None, str]:
    if ":" in value and not value.startswith("/"):
        host, path = value.split(":", 1)
        if host:
            return host, path
    return None, value


def shell_join(args: list[str]) -> str:
    return " ".join(shlex.quote(item) for item in args)


def remote_distrib_command(args: list[str]) -> str:
    quoted_args = shell_join(args)
    return (
        "if command -v distrib >/dev/null 2>&1; then "
        f"exec distrib {quoted_args}; "
        "else "
        f'exec "$HOME/.local/bin/distrib" {quoted_args}; '
        "fi"
    )


def run_coordinator_cli(coordinator: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    host, manifest_path = parse_coordinator(coordinator)
    resolved_args = [item.replace("{manifest}", manifest_path) for item in args]
    if host:
        return subprocess.run(
            ["ssh", host, remote_distrib_command(resolved_args)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    script = str(Path(__file__).resolve())
    local_args = [sys.executable, script, *resolved_args]
    return subprocess.run(
        local_args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def ensure_remote_parent(coordinator: str, path: Path) -> None:
    host, _ = parse_coordinator(coordinator)
    if not host:
        path.parent.mkdir(parents=True, exist_ok=True)
        return
    command = "mkdir -p " + shlex.quote(str(path.parent))
    result = subprocess.run(["ssh", host, command], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"failed to create {host}:{path.parent}")


def sync_file_to_coordinator(local: Path, coordinator: str, remote_path: Path) -> None:
    ensure_remote_parent(coordinator, remote_path)
    host, _ = parse_coordinator(coordinator)
    if host:
        result = subprocess.run(
            ["rsync", "-a", str(local), f"{host}:{remote_path}"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"rsync failed for {local}")
        return
    if local.resolve() == remote_path.resolve():
        return
    shutil.copy2(local, remote_path)


def coordinator_result_payload(
    *,
    coordinator_manifest: dict[str, Any],
    local_payload: dict[str, Any],
    local_manifest: dict[str, Any],
    coordinator_task: dict[str, Any],
    local_task: dict[str, Any],
    state: str,
) -> tuple[dict[str, Any], Path]:
    coordinator_outputs = task_outputs(coordinator_task)
    local_outputs = task_outputs(local_task)
    output_records = []
    worker_outputs = []
    for local_path, coordinator_path in zip(local_outputs, coordinator_outputs, strict=False):
        if local_path.exists():
            local_record = file_record(local_path)
            output_records.append({
                "path": str(coordinator_path),
                "bytes": local_record["bytes"],
                "sha256": local_record["sha256"],
            })
            worker_outputs.append(local_record)
    state_path = (
        task_done_path(coordinator_manifest, coordinator_task)
        if state == "done"
        else task_fail_path(coordinator_manifest, coordinator_task)
    )
    payload = dict(local_payload)
    payload["manifest_sha256"] = manifest_checksum(coordinator_manifest)
    payload["log"] = str(task_log_path(coordinator_manifest, coordinator_task))
    payload["outputs"] = output_records
    payload["worker"] = {
        "host": local_payload.get("host"),
        "platform": local_payload.get("platform"),
        "profile_manifest_sha256": manifest_checksum(local_manifest),
        "log": local_payload.get("log"),
        "outputs": worker_outputs,
    }
    payload["synced_at"] = now()
    payload["sync_state"] = "synced"
    return payload, state_path


def sync_task_result(
    *,
    coordinator: str,
    coordinator_manifest: dict[str, Any],
    local_manifest: dict[str, Any],
    coordinator_task: dict[str, Any],
    local_task: dict[str, Any],
    worker: str,
    claim_id: str | None,
) -> None:
    local_done = task_done_path(local_manifest, local_task)
    local_fail = task_fail_path(local_manifest, local_task)
    state = "done" if local_done.exists() else "fail"
    local_state_path = local_done if local_done.exists() else local_fail
    if not local_state_path.exists():
        raise RuntimeError(f"{local_task['id']}: no local result state found")
    local_payload = read_json(local_state_path)
    heartbeat_on_coordinator(coordinator, str(coordinator_task["id"]), worker, claim_id)

    for local_path, coordinator_path in zip(task_outputs(local_task), task_outputs(coordinator_task), strict=False):
        if local_path.exists():
            sync_file_to_coordinator(local_path, coordinator, coordinator_path)

    sync_file_to_coordinator(
        task_log_path(local_manifest, local_task),
        coordinator,
        task_log_path(coordinator_manifest, coordinator_task),
    )

    payload, coordinator_state_path = coordinator_result_payload(
        coordinator_manifest=coordinator_manifest,
        local_payload=local_payload,
        local_manifest=local_manifest,
        coordinator_task=coordinator_task,
        local_task=local_task,
        state=state,
    )
    heartbeat_on_coordinator(coordinator, str(coordinator_task["id"]), worker, claim_id)
    sync_payload_path = local_state_path.with_suffix(local_state_path.suffix + ".sync.json")
    write_json(sync_payload_path, payload)
    sync_file_to_coordinator(sync_payload_path, coordinator, coordinator_state_path)


def cmd_plan(args: argparse.Namespace) -> int:
    out = expand_path(args.out)
    state = expand_path(args.state_dir) if args.state_dir else out.parent / "state"
    logs = expand_path(args.log_dir) if args.log_dir else out.parent / "logs"
    vars_obj: dict[str, str] = {}
    for item in args.var:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise SystemExit(f"invalid --var {item!r}; expected KEY=VALUE")
        vars_obj[key] = value

    path_maps: dict[str, dict[str, str]] = {}
    for item in args.path_map:
        profile, sep, mapping = item.partition(":")
        src, sep2, dst = mapping.partition("=")
        if not sep or not sep2 or not profile or not src:
            raise SystemExit(
                f"invalid --path-map {item!r}; expected PROFILE:/source=/dest"
            )
        path_maps.setdefault(profile, {})[src] = dst

    tasks = []
    width = max(4, len(str(args.shards - 1)))
    for index in range(args.shards):
        task_id = f"{args.task_prefix}_{index:0{width}d}"
        outputs = [
            template.format(index=index, shard=index, task_id=task_id, shards=args.shards, **vars_obj)
            for template in args.output_template
        ]
        tasks.append({
            "id": task_id,
            "index": index,
            "command": args.command_template,
            "outputs": outputs,
        })

    manifest = {
        "schema": SCHEMA,
        "name": args.name,
        "created_at": now(),
        "work_dir": str(expand_path(args.work_dir)),
        "state_dir": str(state),
        "log_dir": str(logs),
        "claim_dir": str(out.parent / "claims"),
        "vars": vars_obj,
        "path_maps": path_maps,
        "requirements": {
            "python_min": args.python_min,
            "git_min": args.git_min,
            "git": {
                "repo": str(expand_path(args.git_repo or args.work_dir)),
                "commit": args.git_commit or git_output(expand_path(args.git_repo or args.work_dir), "rev-parse", "HEAD"),
                "require_clean": args.require_clean_git,
            },
            "inputs": [],
        },
        "tasks": tasks,
    }
    write_json(out, manifest)
    print(out)
    return 0


def cmd_add_input(args: argparse.Namespace) -> int:
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    inputs = req.setdefault("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for raw in args.path:
        input_path = expand_path(raw)
        if not input_path.exists():
            raise SystemExit(f"input does not exist: {input_path}")
        record = file_record(input_path)
        inputs[:] = [
            item for item in inputs
            if not isinstance(item, dict) or item.get("path") != str(input_path)
        ]
        inputs.append(record)
    write_json(path, manifest)
    print(path)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    task = find_task(manifest, task_id=args.task, index=args.index)
    return run_task(manifest, task, force=args.force)


def cmd_claim(args: argparse.Namespace) -> int:
    manifest = load_manifest(expand_path(args.manifest))
    result = claim_task(
        manifest,
        worker=args.worker,
        profile=args.profile,
        lease_seconds=args.lease_seconds,
        retry_failed=args.retry_failed,
    )
    if args.json:
        print(json.dumps(result or {"task": None}, sort_keys=True))
    elif result:
        print(result["task"])
    else:
        print("no pending tasks")
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    manifest = load_manifest(expand_path(args.manifest))
    task = find_task(manifest, task_id=args.task, index=None)
    release_claim(manifest, task, worker=args.worker, claim_id=args.claim_id)
    print(task["id"])
    return 0


def coordinator_manifest_path(coordinator: str) -> str:
    _, path = parse_coordinator(coordinator)
    return path


def claim_from_coordinator(args: argparse.Namespace) -> dict[str, Any] | None:
    command = [
        "claim",
        "--manifest", "{manifest}",
        "--worker", args.worker,
        "--lease-seconds", str(args.lease_seconds),
        "--json",
    ]
    if args.profile:
        command.extend(["--profile", args.profile])
    if args.retry_failed:
        command.append("--retry-failed")
    result = run_coordinator_cli(args.coordinator, command)
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or result.stdout.strip() or "coordinator claim failed")
    payload = json.loads(result.stdout)
    if not payload.get("task"):
        return None
    return payload


def release_on_coordinator(
    coordinator: str,
    task_id: str,
    *,
    worker: str | None,
    claim_id: str | None,
) -> None:
    command = ["release", "--manifest", "{manifest}", "--task", task_id]
    if worker:
        command.extend(["--worker", worker])
    if claim_id:
        command.extend(["--claim-id", claim_id])
    result = run_coordinator_cli(
        coordinator,
        command,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "coordinator release failed")


def heartbeat_on_coordinator(
    coordinator: str,
    task_id: str,
    worker: str,
    claim_id: str | None,
) -> None:
    command = ["heartbeat", "--manifest", "{manifest}", "--task", task_id, "--worker", worker]
    if claim_id:
        command.extend(["--claim-id", claim_id])
    result = run_coordinator_cli(
        coordinator,
        command,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "coordinator heartbeat failed")


def heartbeat_loop(
    *,
    coordinator: str,
    task_id: str,
    worker: str,
    claim_id: str | None,
    lease_seconds: int,
    stop: threading.Event,
) -> None:
    interval = max(5.0, min(60.0, lease_seconds / 3 if lease_seconds > 0 else 30.0))
    while not stop.wait(interval):
        try:
            heartbeat_on_coordinator(coordinator, task_id, worker, claim_id)
        except Exception as exc:
            print(f"{task_id}: heartbeat failed: {exc}", file=sys.stderr)


def cmd_work(args: argparse.Namespace) -> int:
    coordinator_manifest = load_manifest(expand_path(args.manifest))
    local_manifest = apply_profile(copy.deepcopy(coordinator_manifest), args.profile)
    completed = 0
    failed = 0

    while args.max_tasks <= 0 or completed < args.max_tasks:
        claim = claim_from_coordinator(args)
        if claim is None:
            break
        task_id = str(claim["task"])
        claim_payload = claim.get("claim") if isinstance(claim.get("claim"), dict) else {}
        claim_id = str(claim_payload.get("claim_id") or "")
        coordinator_task = find_task(coordinator_manifest, task_id=task_id, index=None)
        local_task = find_task(local_manifest, task_id=task_id, index=None)
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=heartbeat_loop,
            kwargs={
                "coordinator": args.coordinator,
                "task_id": task_id,
                "worker": args.worker,
                "claim_id": claim_id,
                "lease_seconds": args.lease_seconds,
                "stop": stop_heartbeat,
            },
            daemon=True,
        )
        heartbeat.start()
        sync_failed = False
        try:
            rc = run_task(local_manifest, local_task, force=args.force)
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=5)
        try:
            sync_task_result(
                coordinator=args.coordinator,
                coordinator_manifest=coordinator_manifest,
                local_manifest=local_manifest,
                coordinator_task=coordinator_task,
                local_task=local_task,
                worker=args.worker,
                claim_id=claim_id,
            )
        except Exception as exc:
            sync_failed = True
            failed += 1
            print(f"{task_id}: sync failed: {exc}", file=sys.stderr)
        finally:
            try:
                release_on_coordinator(
                    args.coordinator,
                    task_id,
                    worker=args.worker,
                    claim_id=claim_id,
                )
            except Exception as exc:
                print(f"{task_id}: release failed: {exc}", file=sys.stderr)
        if sync_failed:
            return 1
        completed += 1
        if rc != 0:
            failed += 1
            if args.fail_fast or rc in {130, 143}:
                return rc

    print(f"worker={args.worker} completed={completed} failed={failed}")
    return 0 if failed == 0 else 1


def cmd_doctor(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    check_requirements(manifest)
    if args.role == "coordinator" and args.notify_command:
        command = shlex.split(args.notify_command)
        if not command:
            raise SystemExit("--notify-command is empty")
        if shutil.which(command[0]) is None:
            raise SystemExit(f"notify command not found: {command[0]}")
    print("ok")
    return 0


def cmd_heartbeat(args: argparse.Namespace) -> int:
    manifest = load_manifest(expand_path(args.manifest))
    task = find_task(manifest, task_id=args.task, index=None)
    update_claim_heartbeat(manifest, task, worker=args.worker, claim_id=args.claim_id)
    print(task["id"])
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    counts = {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}
    rows = []
    for task in manifest["tasks"]:
        state = task_state(manifest, task, lease_seconds=args.lease_seconds)
        counts[state] += 1
        info = ""
        if args.verbose:
            if state == "done":
                try:
                    done = read_json(task_done_path(manifest, task))
                    info = str(done.get("host") or "")
                except Exception:
                    pass
            elif state == "fail":
                try:
                    fail = read_json(task_fail_path(manifest, task))
                    info = str(fail.get("host") or "")
                except Exception:
                    pass
            elif state in {"claimed", "running", "stale"}:
                try:
                    claim = read_json(task_claim_path(manifest, task))
                    info = str(claim.get("worker") or "")
                except Exception:
                    pass
        rows.append((task["id"], state, info))
    print(
        f"tasks={len(manifest['tasks'])} done={counts['done']} "
        f"running={counts['running']} claimed={counts['claimed']} stale={counts['stale']} "
        f"fail={counts['fail']} pending={counts['pending']}"
    )
    if args.verbose:
        for task_id, state, info in rows:
            print(f"{task_id}\t{state}\t{info}")
    return 0 if counts["fail"] == 0 else 1


def verify_manifest(manifest: dict[str, Any]) -> None:
    check_requirements(manifest)
    failures = []
    expected_manifest_sha = manifest_checksum(manifest)
    for task in manifest["tasks"]:
        if not task_complete(manifest, task):
            failures.append(f"{task['id']}: {task_state(manifest, task)}")
            continue
        done = read_json(task_done_path(manifest, task))
        done_manifest_sha = str(done.get("manifest_sha256") or "")
        if done_manifest_sha and done_manifest_sha != expected_manifest_sha:
            failures.append(f"{task['id']}: manifest checksum mismatch")
        records = done.get("outputs", [])
        outputs = task_outputs(task)
        if not outputs:
            failures.append(f"{task['id']}: no outputs declared")
        if isinstance(records, list):
            by_path = {
                str(item.get("path")): item
                for item in records
                if isinstance(item, dict) and item.get("path")
            }
            for output in outputs:
                if str(output) not in by_path:
                    failures.append(f"{task['id']}: missing output record: {output}")
                    continue
                record = by_path.get(str(output))
                if record and record.get("sha256") and sha256_file(output) != record["sha256"]:
                    failures.append(f"{task['id']}: output checksum mismatch: {output}")
        else:
            failures.append(f"{task['id']}: invalid output records")
    if failures:
        raise SystemExit("incomplete tasks:\n" + "\n".join(failures))


def cmd_verify(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    verify_manifest(manifest)
    print("ok")
    return 0


def append_file(src: Path, dst: Path, *, newline_between: bool) -> None:
    with src.open("rb") as in_handle, dst.open("ab") as out_handle:
        shutil.copyfileobj(in_handle, out_handle, length=1024 * 1024)
        if newline_between:
            out_handle.write(b"\n")


def cmd_merge(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    verify_manifest(manifest)
    output = expand_path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not args.force:
        raise SystemExit(f"{output}: exists; pass --force to overwrite")
    output.unlink(missing_ok=True)
    merged = 0
    for task in sorted(manifest["tasks"], key=lambda t: int(t.get("index", 0))):
        outputs = task_outputs(task)
        if not outputs:
            continue
        append_file(outputs[0], output, newline_between=args.newline_between)
        merged += 1
    sidecar = output.with_suffix(output.suffix + ".manifest.json")
    write_json(sidecar, {
        "schema": SCHEMA,
        "merged_at": now(),
        "manifest": str(expand_path(args.manifest)),
        "tasks": merged,
        "output": file_record(output),
    })
    print(output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Create a numbered shard manifest.")
    plan.add_argument("--name", required=True)
    plan.add_argument("--shards", type=int, required=True)
    plan.add_argument("--work-dir", required=True)
    plan.add_argument("--out", required=True)
    plan.add_argument("--command-template", required=True)
    plan.add_argument("--output-template", action="append", required=True)
    plan.add_argument("--var", action="append", default=[])
    plan.add_argument(
        "--path-map",
        action="append",
        default=[],
        help="Host path mapping, PROFILE:/source=/dest. Repeatable.",
    )
    plan.add_argument("--task-prefix", default="task")
    plan.add_argument("--state-dir")
    plan.add_argument("--log-dir")
    plan.add_argument("--python-min", default="3.11")
    plan.add_argument("--git-min", default="2.38")
    plan.add_argument("--git-repo")
    plan.add_argument("--git-commit")
    plan.add_argument("--require-clean-git", action="store_true")
    plan.set_defaults(func=cmd_plan)

    add_input = sub.add_parser("add-input", help="Record input file checksums.")
    add_input.add_argument("--manifest", required=True)
    add_input.add_argument("--path", action="append", required=True)
    add_input.set_defaults(func=cmd_add_input)

    run = sub.add_parser("run", help="Run one task by id or index.")
    run.add_argument("--manifest", required=True)
    run.add_argument("--task")
    run.add_argument("--index", type=int)
    run.add_argument("--profile")
    run.add_argument("--force", action="store_true")
    run.set_defaults(func=cmd_run)

    claim = sub.add_parser("claim", help="Atomically claim the next coordinator task.")
    claim.add_argument("--manifest", required=True)
    claim.add_argument("--worker", required=True)
    claim.add_argument("--profile")
    claim.add_argument("--lease-seconds", type=int, default=600)
    claim.add_argument("--retry-failed", action="store_true")
    claim.add_argument("--json", action="store_true")
    claim.set_defaults(func=cmd_claim)

    release = sub.add_parser("release", help="Release a coordinator task claim.")
    release.add_argument("--manifest", required=True)
    release.add_argument("--task", required=True)
    release.add_argument("--worker")
    release.add_argument("--claim-id")
    release.set_defaults(func=cmd_release)

    heartbeat = sub.add_parser("heartbeat", help="Refresh a coordinator task claim heartbeat.")
    heartbeat.add_argument("--manifest", required=True)
    heartbeat.add_argument("--task", required=True)
    heartbeat.add_argument("--worker", required=True)
    heartbeat.add_argument("--claim-id")
    heartbeat.set_defaults(func=cmd_heartbeat)

    work = sub.add_parser("work", help="Claim, run, sync, and repeat worker tasks.")
    work.add_argument("--manifest", required=True)
    work.add_argument(
        "--coordinator",
        required=True,
        help="Coordinator manifest path, either /path/manifest.json or host:/path/manifest.json.",
    )
    work.add_argument("--profile")
    work.add_argument("--worker", default=socket.gethostname())
    work.add_argument("--lease-seconds", type=int, default=600)
    work.add_argument("--max-tasks", type=int, default=0)
    work.add_argument("--retry-failed", action="store_true")
    work.add_argument("--force", action="store_true")
    work.add_argument("--fail-fast", action="store_true")
    work.set_defaults(func=cmd_work)

    doctor = sub.add_parser("doctor", help="Check host prerequisites for a manifest.")
    doctor.add_argument("--manifest", required=True)
    doctor.add_argument("--profile")
    doctor.add_argument("--role", choices=["worker", "coordinator"], default="worker")
    doctor.add_argument("--notify-command")
    doctor.set_defaults(func=cmd_doctor)

    status = sub.add_parser("status", help="Show task status.")
    status.add_argument("--manifest", required=True)
    status.add_argument("-v", "--verbose", action="store_true")
    status.add_argument("--profile")
    status.add_argument("--lease-seconds", type=int, default=600)
    status.set_defaults(func=cmd_status)

    verify = sub.add_parser("verify", help="Verify all tasks are complete.")
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--profile")
    verify.set_defaults(func=cmd_verify)

    merge = sub.add_parser("merge", help="Concatenate first output from each task.")
    merge.add_argument("--manifest", required=True)
    merge.add_argument("--output", required=True)
    merge.add_argument("--force", action="store_true")
    merge.add_argument("--newline-between", action="store_true")
    merge.add_argument("--profile")
    merge.set_defaults(func=cmd_merge)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "command", None) == "run" and args.task is None and args.index is None:
        parser.error("run requires --task or --index")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
