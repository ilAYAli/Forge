#!/usr/bin/env python3
"""Generic deterministic shard runner for CPU/IO-heavy work."""
from __future__ import annotations

import argparse
import concurrent.futures
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
SSH_CONNECT_TIMEOUT_SECONDS = 15
SSH_SERVER_ALIVE_INTERVAL_SECONDS = 30
SSH_SERVER_ALIVE_COUNT_MAX = 3
REMOTE_PROCESS_STOP_GRACE_SECONDS = 2
DEFAULT_REQUIREMENT_TIMEOUT_SECONDS = 15.0
HEARTBEAT_MIN_INTERVAL_SECONDS = 5.0
HEARTBEAT_MAX_INTERVAL_SECONDS = 60.0
HEARTBEAT_DEFAULT_INTERVAL_SECONDS = 30.0
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT_SECONDS}",
    "-o", f"ServerAliveInterval={SSH_SERVER_ALIVE_INTERVAL_SECONDS}",
    "-o", f"ServerAliveCountMax={SSH_SERVER_ALIVE_COUNT_MAX}",
]
RSYNC_SSH = "ssh " + " ".join(shlex.quote(item) for item in SSH_OPTS)


def script_path() -> Path:
    return Path(__file__).resolve()


def script_repo(path: Path | None = None) -> Path:
    path = path or script_path()
    try:
        root = subprocess.check_output(
            ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.output.strip() or f"{path}: not inside a git checkout") from exc
    return Path(root)


def expand_path(value: str | Path) -> Path:
    path = Path(os.path.expandvars(str(value))).expanduser()
    if path.is_absolute():
        return path
    return Path.cwd() / path


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


def resolve_git_commit(repo: Path, commit: str, *, label: str) -> str:
    try:
        return git_output(repo, "rev-parse", "--verify", f"{commit}^{{commit}}")
    except subprocess.CalledProcessError as exc:
        output = exc.output.strip()
        detail = f": {output}" if output else ""
        raise SystemExit(f"{label}: git commit {commit!r} is not available{detail}") from exc


def fetch_git_remote(repo: Path, remote: str) -> None:
    refspec = f"+refs/heads/*:refs/remotes/{remote}/*"
    run_checked(["git", "-C", str(repo), "fetch", "--no-tags", remote, refspec])
    try:
        shallow = git_output(repo, "rev-parse", "--is-shallow-repository")
    except subprocess.CalledProcessError:
        shallow = "false"
    if shallow == "true":
        run_checked(["git", "-C", str(repo), "fetch", "--unshallow", remote])


def ensure_git_remote(repo: Path, remote: str, url: str) -> None:
    if not remote or not url:
        return
    remotes = set(git_output(repo, "remote").splitlines())
    if remote in remotes:
        current = git_output(repo, "remote", "get-url", remote)
        if current != url:
            run_checked(["git", "-C", str(repo), "remote", "set-url", remote, url])
        return
    run_checked(["git", "-C", str(repo), "remote", "add", remote, url])


def command_output(command: list[str]) -> str:
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def command_result(command: str | list[str], *, cwd: Path | None = None, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    if isinstance(command, str):
        return subprocess.run(
            ["bash", "-lc", command],
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    return subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )


def bounded_timeout(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    timeout = max(0.0, float(value))
    return timeout if timeout > 0 else None


class ProcessTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processes: set[subprocess.Popen[str]] = set()

    def run(self, command: list[str], *, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
        proc = subprocess.Popen(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        with self._lock:
            self._processes.add(proc)
        try:
            try:
                stdout, stderr = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._terminate(proc, signal.SIGTERM)
                try:
                    stdout, stderr = proc.communicate(
                        timeout=REMOTE_PROCESS_STOP_GRACE_SECONDS)
                except subprocess.TimeoutExpired:
                    self._terminate(proc, signal.SIGKILL)
                    stdout, stderr = proc.communicate()
                    return subprocess.CompletedProcess(command, 124, stdout=stdout, stderr=stderr)
                return subprocess.CompletedProcess(command, 124, stdout=stdout, stderr=stderr)
            return subprocess.CompletedProcess(command, proc.returncode, stdout=stdout, stderr=stderr)
        finally:
            with self._lock:
                self._processes.discard(proc)

    def terminate_all(self) -> None:
        with self._lock:
            processes = list(self._processes)
        for proc in processes:
            self._terminate(proc, signal.SIGTERM)
        deadline = time.monotonic() + REMOTE_PROCESS_STOP_GRACE_SECONDS
        for proc in processes:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                self._terminate(proc, signal.SIGKILL)

    @staticmethod
    def _terminate(proc: subprocess.Popen[str], sig: signal.Signals) -> None:
        if proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        except OSError:
            if sig == signal.SIGKILL:
                proc.kill()
            else:
                proc.terminate()


def run_checked(command: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stdout.strip() or f"command failed: {shell_join(command)}")


def distrib_version_payload() -> dict[str, Any]:
    path = script_path()
    repo = script_repo(path)
    commit = git_output(repo, "rev-parse", "HEAD")
    branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
    dirty = bool(git_output(repo, "status", "--porcelain"))
    remote = ""
    upstream = ""
    try:
        remote = git_output(repo, "config", "--get", f"branch.{branch}.remote")
    except subprocess.CalledProcessError:
        remote = ""
    try:
        upstream = git_output(repo, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    except subprocess.CalledProcessError:
        upstream = ""
    return {
        "schema": SCHEMA,
        "tool": "distrib",
        "path": str(path),
        "repo": str(repo),
        "branch": branch,
        "commit": commit,
        "dirty": dirty,
        "remote": remote,
        "upstream": upstream,
    }


def print_version_payload(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    suffix = " dirty" if payload["dirty"] else ""
    print(f"distrib {payload['commit']} {payload['branch']}{suffix}")


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = read_json(path)
    if manifest.get("schema") != SCHEMA:
        raise SystemExit(f"{path}: unsupported schema {manifest.get('schema')!r}")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        raise SystemExit(f"{path}: tasks must be a list")
    seen: set[str] = set()
    seen_outputs: dict[str, str] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise SystemExit(f"{path}: each task must be an object")
        task_id = str(task.get("id", ""))
        if not task_id:
            raise SystemExit(f"{path}: task is missing id")
        if task_id in seen:
            raise SystemExit(f"{path}: duplicate task id {task_id}")
        seen.add(task_id)
        outputs = task.get("outputs", [])
        if not isinstance(outputs, list):
            raise SystemExit(f"{path}: {task_id}: outputs must be a list")
        for output in outputs:
            output_key = str(output)
            other_task = seen_outputs.get(output_key)
            if other_task is not None:
                raise SystemExit(
                    f"{path}: duplicate output path {output_key!r} in "
                    f"{other_task} and {task_id}")
            seen_outputs[output_key] = task_id
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


def iter_git_requirements(manifest: dict[str, Any], req: dict[str, Any]) -> list[dict[str, Any]]:
    repos = []
    git_req = req.get("git")
    if isinstance(git_req, dict):
        repos.append({
            "name": git_req.get("name") or "git",
            "path": git_req.get("repo") or manifest.get("work_dir") or ".",
            "commit": git_req.get("commit") or "",
            "require_clean": bool(git_req.get("require_clean", False)),
            "remote": git_req.get("remote") or "",
        })
    raw_repos = req.get("repos", [])
    if raw_repos in (None, ""):
        raw_repos = []
    if not isinstance(raw_repos, list):
        raise SystemExit("requirements.repos must be a list")
    for item in raw_repos:
        if not isinstance(item, dict):
            raise SystemExit("requirements.repos entries must be objects")
        repos.append(item)
    return repos


def check_git_requirement(item: dict[str, Any]) -> None:
    repo = expand_path(str(item.get("path") or item.get("repo") or "."))
    label = str(item.get("name") or repo)
    if not (repo / ".git").exists():
        raise SystemExit(f"{label}: not a git checkout: {repo}")
    expected_commit = str(item.get("commit") or "").strip()
    if expected_commit:
        actual_commit = git_output(repo, "rev-parse", "HEAD")
        resolved_commit = resolve_git_commit(repo, expected_commit, label=label)
        if actual_commit != resolved_commit:
            raise SystemExit(
                f"{label}: git commit {actual_commit} != required {resolved_commit}"
            )
    branch = str(item.get("branch") or "").strip()
    if branch and not expected_commit:
        actual_branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
        if actual_branch != branch:
            raise SystemExit(f"{label}: git branch {actual_branch} != required {branch}")
    if bool(item.get("require_clean", False)):
        status = git_output(repo, "status", "--porcelain")
        if status:
            raise SystemExit(f"{label}: git worktree is not clean")


def check_command_requirement(item: dict[str, Any]) -> None:
    if not isinstance(item, dict):
        raise SystemExit("requirements.commands entries must be objects")
    label = str(item.get("name") or item.get("path") or item.get("command") or "command")
    path = str(item.get("path") or "").strip()
    if path:
        resolved = shutil.which(path) if os.path.sep not in path else str(expand_path(path))
        if not resolved or not Path(resolved).exists():
            raise SystemExit(f"{label}: required command not found: {path}")
        if not os.access(resolved, os.X_OK):
            raise SystemExit(f"{label}: required command is not executable: {resolved}")

    command = item.get("command")
    if command is None and path and (item.get("contains") or item.get("regex")):
        command = [path, "--version"]
    if command is None:
        return
    timeout = bounded_timeout(
        item.get("timeout_s"), DEFAULT_REQUIREMENT_TIMEOUT_SECONDS)
    result = command_result(command, timeout=timeout)
    output = result.stdout
    if result.returncode != int(item.get("rc", 0)):
        raise SystemExit(f"{label}: command failed rc={result.returncode}: {output.strip()}")
    contains = str(item.get("contains") or "")
    if contains and contains not in output:
        raise SystemExit(f"{label}: output does not contain {contains!r}: {output.strip()}")
    regex = str(item.get("regex") or "")
    if regex and not re.search(regex, output):
        raise SystemExit(f"{label}: output does not match {regex!r}: {output.strip()}")


def requirement_commands(value: Any, field: str) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    raise SystemExit(f"{field} must be a string or list of strings")


def run_repo_hook(item: dict[str, Any], field: str, *, repo: Path) -> None:
    label = str(item.get("name") or repo)
    for index, command in enumerate(requirement_commands(item.get(field), f"{label}.{field}"), start=1):
        result = command_result(
            command, cwd=repo,
            timeout=bounded_timeout(item.get(f"{field}_timeout_s")))
        if result.returncode != 0:
            raise SystemExit(
                f"{label}: {field}[{index}] failed rc={result.returncode}: {result.stdout.strip()}"
            )


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

    for item in iter_git_requirements(manifest, req):
        check_git_requirement(item)
        run_repo_hook(item, "probe", repo=expand_path(str(item.get("path") or item.get("repo") or ".")))

    commands = req.get("commands", [])
    if commands in (None, ""):
        commands = []
    if not isinstance(commands, list):
        raise SystemExit("requirements.commands must be a list")
    for item in commands:
        check_command_requirement(item)

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
    task_running_path(manifest, task).unlink(missing_ok=True)
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
        timeout = bounded_timeout(task.get("timeout_s", manifest.get("timeout_s")))
        try:
            rc = proc.wait(timeout=timeout) if timeout else proc.wait()
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
        "command -v distrib >/dev/null 2>&1 || "
        "{ echo 'distrib not found in PATH on coordinator' >&2; exit 127; }; "
        f"exec distrib {quoted_args}"
    )


def run_coordinator_cli(coordinator: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    host, manifest_path = parse_coordinator(coordinator)
    resolved_args = [item.replace("{manifest}", manifest_path) for item in args]
    if host:
        return subprocess.run(
            ["ssh", *SSH_OPTS, host, remote_distrib_command(resolved_args)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    script = str(script_path())
    local_args = [sys.executable, script, *resolved_args]
    return subprocess.run(
        local_args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


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


def sync_files_to_coordinator(coordinator: str, transfers: list[tuple[Path, Path]]) -> None:
    if not transfers:
        return
    host, _ = parse_coordinator(coordinator)
    if not host:
        for local, remote in transfers:
            remote.parent.mkdir(parents=True, exist_ok=True)
            if local.resolve() != remote.resolve():
                shutil.copy2(local, remote)
        return

    parents = {str(remote.parent) for _, remote in transfers}
    quoted_parents = " ".join(shlex.quote(p) for p in sorted(parents))
    mkdir_result = subprocess.run(
        ["ssh", *SSH_OPTS, host, f"mkdir -p {quoted_parents}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if mkdir_result.returncode != 0:
        raise RuntimeError(
            mkdir_result.stderr.strip() or f"failed to create remote output dirs on {host}"
        )

    for local, remote in transfers:
        result = subprocess.run(
            ["rsync", "-a", "-e", RSYNC_SSH, str(local), f"{host}:{remote}"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"rsync failed for {local}")


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

    transfers = []
    for local_path, coordinator_path in zip(task_outputs(local_task), task_outputs(coordinator_task), strict=False):
        if local_path.exists():
            transfers.append((local_path, coordinator_path))

    transfers.append((
        task_log_path(local_manifest, local_task),
        task_log_path(coordinator_manifest, coordinator_task),
    ))

    payload, coordinator_state_path = coordinator_result_payload(
        coordinator_manifest=coordinator_manifest,
        local_payload=local_payload,
        local_manifest=local_manifest,
        coordinator_task=coordinator_task,
        local_task=local_task,
        state=state,
    )
    sync_payload_path = local_state_path.with_suffix(local_state_path.suffix + ".sync.json")
    write_json(sync_payload_path, payload)
    transfers.append((sync_payload_path, coordinator_state_path))

    sync_files_to_coordinator(coordinator, transfers)
    heartbeat_on_coordinator(coordinator, str(coordinator_task["id"]), worker, claim_id)


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
    generated_outputs: dict[str, str] = {}
    width = max(4, len(str(args.shards - 1)))
    for index in range(args.shards):
        task_id = f"{args.task_prefix}_{index:0{width}d}"
        outputs = [
            template.format(index=index, shard=index, task_id=task_id, shards=args.shards, **vars_obj)
            for template in args.output_template
        ]
        for output in outputs:
            other_task = generated_outputs.get(output)
            if other_task is not None:
                raise SystemExit(
                    f"{out}: duplicate output path {output!r} in "
                    f"{other_task} and {task_id}")
            generated_outputs[output] = task_id
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


def cmd_add_repo(args: argparse.Namespace) -> int:
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    repos = req.setdefault("repos", [])
    if not isinstance(repos, list):
        raise SystemExit("requirements.repos must be a list")
    repo = expand_path(args.path)
    commit = args.commit or git_output(repo, "rev-parse", "HEAD")
    record = {
        "name": args.name or repo.name,
        "path": str(repo),
        "commit": commit,
        "require_clean": args.require_clean,
    }
    if args.branch:
        record["branch"] = args.branch
    if args.remote:
        record["remote"] = args.remote
    repos[:] = [
        item for item in repos
        if not isinstance(item, dict) or item.get("path") != str(repo)
    ]
    repos.append(record)
    write_json(path, manifest)
    print(path)
    return 0


def sync_git_requirement(item: dict[str, Any]) -> None:
    repo = expand_path(str(item.get("path") or item.get("repo") or "."))
    label = str(item.get("name") or repo)
    url = str(item.get("url") or "").strip()
    if not repo.exists() and url:
        repo.parent.mkdir(parents=True, exist_ok=True)
        run_checked(["git", "clone", url, str(repo)])
    if not (repo / ".git").exists():
        raise SystemExit(f"{label}: not a git checkout: {repo}")
    status = git_output(repo, "status", "--porcelain")
    if status and not bool(item.get("allow_dirty", False)):
        raise SystemExit(f"{label}: git worktree is not clean")
    remote = str(item.get("remote") or "").strip()
    ensure_git_remote(repo, remote, url)
    if remote:
        fetch_git_remote(repo, remote)
    branch = str(item.get("branch") or "").strip()
    commit = str(item.get("commit") or "").strip()
    if branch:
        run_checked(["git", "-C", str(repo), "checkout", branch])
        if remote:
            run_checked(["git", "-C", str(repo), "pull", "--ff-only", remote, branch])
    if commit:
        resolved_commit = resolve_git_commit(repo, commit, label=label)
        run_checked(["git", "-C", str(repo), "checkout", resolved_commit])
    run_repo_hook(item, "build", repo=repo)
    run_repo_hook(item, "install", repo=repo)
    run_repo_hook(item, "probe", repo=repo)
    print(f"{label}: {git_output(repo, 'rev-parse', 'HEAD')}")


def cmd_sync_repos(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    repos = iter_git_requirements(manifest, req)
    if not repos:
        raise SystemExit("no git repositories declared")
    for item in repos:
        sync_git_requirement(item)
    check_requirements(manifest)
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
    interval = max(
        HEARTBEAT_MIN_INTERVAL_SECONDS,
        min(
            HEARTBEAT_MAX_INTERVAL_SECONDS,
            lease_seconds / 3 if lease_seconds > 0
            else HEARTBEAT_DEFAULT_INTERVAL_SECONDS,
        ),
    )
    while not stop.wait(interval):
        try:
            heartbeat_on_coordinator(coordinator, task_id, worker, claim_id)
        except Exception as exc:
            print(f"{task_id}: heartbeat failed: {exc}", file=sys.stderr)


def check_coordinator_cli(coordinator: str) -> None:
    result = run_coordinator_cli(coordinator, ["version", "--json"])
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "coordinator distrib check failed"
        raise SystemExit(detail)


def cmd_work(args: argparse.Namespace) -> int:
    coordinator_manifest = load_manifest(expand_path(args.manifest))
    local_manifest = apply_profile(copy.deepcopy(coordinator_manifest), args.profile)
    check_coordinator_cli(args.coordinator)
    check_requirements(local_manifest)
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
        error_rc = 1
        try:
            rc = run_task(local_manifest, local_task, force=args.force)
            sync_task_result(
                coordinator=args.coordinator,
                coordinator_manifest=coordinator_manifest,
                local_manifest=local_manifest,
                coordinator_task=coordinator_task,
                local_task=local_task,
                worker=args.worker,
                claim_id=claim_id,
            )
        except SystemExit as exc:
            sync_failed = True
            failed += 1
            if isinstance(exc.code, int):
                error_rc = exc.code
            print(f"{task_id}: worker failed: {exc}", file=sys.stderr)
        except Exception as exc:
            sync_failed = True
            failed += 1
            print(f"{task_id}: sync failed: {exc}", file=sys.stderr)
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=5)
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
            return error_rc
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

    for cmd in ["ssh", "rsync"]:
        if shutil.which(cmd) is None:
            raise SystemExit(f"required command not found: {cmd}")

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
    counts = task_counts(manifest, lease_seconds=args.lease_seconds)
    rows = []
    if args.verbose:
        for task in manifest["tasks"]:
            state = task_state(manifest, task, lease_seconds=args.lease_seconds)
            info = ""
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


def task_counts(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, int]:
    counts = {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}
    for task in manifest["tasks"]:
        counts[task_state(manifest, task, lease_seconds=lease_seconds)] += 1
    return counts


def cmd_wait(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout_seconds if args.timeout_seconds > 0 else None
    last_line = ""
    while True:
        manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
        counts = task_counts(manifest, lease_seconds=args.lease_seconds)
        total = len(manifest["tasks"])
        line = (
            f"tasks={total} done={counts['done']} running={counts['running']} "
            f"claimed={counts['claimed']} stale={counts['stale']} "
            f"fail={counts['fail']} pending={counts['pending']}"
        )
        if line != last_line:
            print(line, flush=True)
            last_line = line

        if counts["fail"] and args.fail_on_fail:
            return 1
        if counts["done"] == total:
            return 0
        if deadline is not None and time.monotonic() >= deadline:
            print("timeout waiting for tasks", file=sys.stderr)
            return 124
        time.sleep(max(0.1, args.interval_seconds))


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


def cmd_self_update(args: argparse.Namespace) -> int:
    repo = script_repo()
    if args.require_clean and git_output(repo, "status", "--porcelain"):
        raise SystemExit(f"{repo}: git worktree is not clean")
    current_branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
    branch = args.branch or current_branch
    if branch == "HEAD":
        raise SystemExit("distrib is on a detached HEAD; pass --branch explicitly")
    run_checked(["git", "-C", str(repo), "fetch", args.remote])
    if current_branch != branch:
        run_checked(["git", "-C", str(repo), "checkout", branch])
    run_checked(["git", "-C", str(repo), "pull", "--ff-only", args.remote, branch])
    print_version_payload(distrib_version_payload(), as_json=args.json)
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    print_version_payload(distrib_version_payload(), as_json=args.json)
    return 0


def parse_worker_spec(value: str) -> tuple[str, str]:
    if "=" in value:
        host, profile = value.split("=", 1)
    else:
        host, profile = value, value
    host = host.strip()
    profile = profile.strip()
    if not host:
        raise SystemExit("worker host must not be empty")
    if not profile:
        raise SystemExit(f"{host}: worker profile must not be empty")
    return host, profile


def parse_worker_record(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        host, profile = parse_worker_spec(item)
        return {"host": host, "profile": profile, "local": host in ("localhost", "127.0.0.1")}
    if not isinstance(item, dict):
        raise SystemExit("workers entries must be strings or objects")
    if not bool(item.get("enabled", True)):
        return {}
    host = str(item.get("host") or "").strip()
    if not host:
        raise SystemExit("workers entries must include host")
    profile = str(item.get("profile") or host).strip()
    if not profile:
        raise SystemExit(f"{host}: worker profile must not be empty")
    record = {
        "host": host,
        "profile": profile,
        "local": bool(item.get("local", host in ("localhost", "127.0.0.1"))),
    }
    if item.get("cache_dir"):
        record["cache_dir"] = str(item["cache_dir"])
    return record


def load_workers(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if payload.get("schema") not in (None, "distribute.workers.v1"):
        raise SystemExit(f"{path}: unsupported schema {payload.get('schema')!r}")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise SystemExit(f"{path}: workers must be a list")
    workers = [worker for worker in (parse_worker_record(item) for item in raw_workers) if worker]
    if not workers:
        raise SystemExit(f"{path}: no enabled workers")
    return workers


def worker_cache_dir(worker: dict[str, Any]) -> str:
    if worker.get("cache_dir"):
        return str(worker["cache_dir"])
    if is_local_worker(worker):
        return str(Path.home() / ".cache/distrib")
    return ".cache/distrib"


def worker_cache_file(worker: dict[str, Any], name: str) -> str:
    return str(Path(worker_cache_dir(worker)) / name)


def is_local_worker(worker: dict[str, Any]) -> bool:
    return bool(worker.get("local", False))


def managed_remote_script(script: str) -> str:
    quoted = shlex.quote(script)
    cleanup = "kill -TERM -$pid 2>/dev/null; sleep 1; kill -KILL -$pid 2>/dev/null"
    return (
        "if command -v setsid >/dev/null 2>&1; then "
        f"setsid sh -lc {quoted} & pid=$!; "
        f"trap '{cleanup}' INT TERM HUP; "
        "wait $pid; rc=$?; trap - INT TERM HUP; exit $rc; "
        f"else sh -lc {quoted}; fi"
    )


def run_worker_shell(
    worker: dict[str, Any],
    script: str,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
) -> subprocess.CompletedProcess[str]:
    command = ["sh", "-lc", script] if is_local_worker(worker) else ["ssh", *SSH_OPTS, str(worker["host"]), managed_remote_script(script)]
    if tracker is not None:
        return tracker.run(command, timeout=timeout)
    if is_local_worker(worker):
        return subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
        )
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
    )


def worker_rsync_target(worker: dict[str, Any], path: str) -> str:
    if is_local_worker(worker):
        return str(expand_path(path))
    return f"{worker['host']}:{path}"


def copy_to_worker(
    worker: dict[str, Any],
    src: Path,
    dst: str,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
) -> None:
    if is_local_worker(worker):
        target = expand_path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        return
    command = ["rsync", "-a", "-e", RSYNC_SSH, str(src), worker_rsync_target(worker, dst)]
    if tracker is not None:
        result = tracker.run(command, timeout=timeout)
    else:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
        )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"rsync failed for {src}")


def bootstrap_worker_runner(
    worker: dict[str, Any],
    manifest_path: Path,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
) -> None:
    if not is_local_worker(worker):
        cache = worker_cache_dir(worker)
        result = run_worker_shell(worker, "mkdir -p " + shlex.quote(cache), timeout=timeout, tracker=tracker)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "failed to create worker cache")
    copy_to_worker(worker, script_path(), worker_cache_file(worker, "distrib.py"), timeout=timeout, tracker=tracker)
    copy_to_worker(worker, manifest_path, worker_cache_file(worker, "manifest.json"), timeout=timeout, tracker=tracker)


def worker_profile_args(manifest: dict[str, Any], worker: dict[str, Any]) -> list[str]:
    profile = str(worker.get("profile") or "").strip()
    if not profile:
        return []
    path_maps = manifest.get("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    if profile not in path_maps:
        return []
    return ["--profile", profile]


def manifest_has_git_requirements(manifest: dict[str, Any]) -> bool:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        return False
    return bool(iter_git_requirements(manifest, req))


def deploy_prereq_command(*, worker: dict[str, Any], manifest: dict[str, Any]) -> str:
    runner = worker_cache_file(worker, "distrib.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    commands = ["set -e"]
    if manifest_has_git_requirements(manifest):
        commands.append(shell_join(["python3", runner, "sync-repos", "--manifest", manifest_path, *profile_args]))
    commands.append(shell_join(["python3", runner, "doctor", "--manifest", manifest_path, *profile_args, "--role", "worker"]))
    return "\n".join(commands)


def deploy_work_command(*, worker: dict[str, Any], manifest: dict[str, Any], coordinator: str) -> str:
    runner = worker_cache_file(worker, "distrib.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    return shell_join([
        "python3", runner,
        "work",
        "--manifest", manifest_path,
        "--coordinator", coordinator,
        "--worker", str(worker["host"]),
        *profile_args,
    ])


def coordinator_for_worker(worker: dict[str, Any], manifest_path: Path) -> str:
    if is_local_worker(worker):
        return str(manifest_path)
    return f"{socket.gethostname()}:{manifest_path}"


def worker_label(host: str, profile: str) -> str:
    return host if profile == host else f"{host}/{profile}"


def colorize(text: str, color: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    colors = {
        "red": "31",
        "green": "32",
        "yellow": "33",
        "cyan": "36",
    }
    return f"\033[{colors[color]}m{text}\033[0m"


def cmd_deploy(args: argparse.Namespace) -> int:
    workers = load_workers(expand_path(args.workers))
    jobs = max(1, int(args.jobs))
    remote_timeout = max(1.0, float(args.remote_timeout_seconds))
    manifest_path = expand_path(args.manifest)
    manifest = load_manifest(manifest_path)
    has_tasks = bool(manifest.get("tasks"))
    color_enabled = not bool(args.no_color) and sys.stdout.isatty()
    print_lock = threading.Lock()
    tracker = ProcessTracker()

    def status(message: str, *, error: bool = False) -> None:
        with print_lock:
            print(message, file=sys.stderr if error else sys.stdout, flush=True)

    def verbose_status(message: str, *, error: bool = False) -> None:
        if args.verbose:
            status(message, error=error)

    def run_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile)

        def result_payload(stage: str, result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
            return {
                "index": index,
                "host": host,
                "profile": profile,
                "stage": stage,
                "ok": result.returncode == 0,
                "rc": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        try:
            verbose_status(f"{label}: syncing runner and manifest")
            bootstrap_worker_runner(worker, manifest_path, timeout=remote_timeout, tracker=tracker)
            verbose_status(f"{label}: checking prerequisites")
            script = deploy_prereq_command(worker=worker, manifest=manifest)
            prereq = run_worker_shell(worker, script, timeout=remote_timeout, tracker=tracker)
        except Exception as exc:
            prereq = subprocess.CompletedProcess(["deploy", host], 1, stdout="", stderr=str(exc))

        if prereq.returncode != 0:
            verbose_status(f"{label}: prerequisites FAILED rc={prereq.returncode}", error=True)
            return result_payload("setup", prereq)

        verbose_status(f"{label}: prerequisites OK")
        if not has_tasks:
            return result_payload("setup", prereq)

        verbose_status(f"{worker_label(host, profile)}: running tasks")
        script = deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator=coordinator_for_worker(worker, manifest_path),
        )
        result = run_worker_shell(worker, script, timeout=remote_timeout, tracker=tracker)
        if result.returncode == 0:
            verbose_status(f"{label}: tasks OK")
            return result_payload("tasks", result)
        verbose_status(f"{label}: tasks FAILED rc={result.returncode}", error=True)
        return result_payload("tasks", result)

    status(colorize(f"deploy: {len(workers)} worker(s), jobs={jobs}", "cyan", enabled=color_enabled))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=jobs)
    interrupted = False
    futures: list[concurrent.futures.Future[dict[str, Any]]] = []
    try:
        futures = [
            executor.submit(run_worker, index, worker)
            for index, worker in enumerate(workers)
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    except KeyboardInterrupt:
        interrupted = True
        status(colorize("deploy: interrupted; stopping active worker commands", "yellow", enabled=color_enabled), error=True)
        tracker.terminate_all()
        concurrent.futures.wait(futures, timeout=2)
        for future in futures:
            future.cancel()
        return 130
    finally:
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    failed = sum(1 for item in results if not item["ok"])
    stage = "setup" if not has_tasks else "tasks"
    for item in sorted(results, key=lambda entry: int(entry["index"])):
        ok = bool(item["ok"])
        marker_text = "OK" if ok else "FAILED"
        marker_color = "green" if ok else "red"
        marker = colorize(f"{marker_text:<6}", marker_color, enabled=color_enabled)
        suffix = "" if ok else f" rc={item['rc']}"
        status(f"  {marker} {worker_label(str(item['host']), str(item['profile']))}: {stage}{suffix}")
        output = "\n".join(
            part.strip()
            for part in (str(item.get("stdout") or ""), str(item.get("stderr") or ""))
            if part.strip()
        )
        if output and (args.verbose or not ok):
            for line in output.splitlines():
                status(f"         {line}")
    if not has_tasks:
        status(colorize("deploy: setup complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: setup failed", "red", enabled=color_enabled))
    else:
        status(colorize("deploy: tasks complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: tasks failed", "red", enabled=color_enabled))
    return 1 if failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="store_true", help="Print distrib version and exit.")
    parser.add_argument("--version-json", action="store_true", help="Print distrib version as JSON and exit.")
    sub = parser.add_subparsers(dest="command")

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

    add_repo = sub.add_parser("add-repo", help="Record a git checkout requirement.")
    add_repo.add_argument("--manifest", required=True)
    add_repo.add_argument("--path", required=True)
    add_repo.add_argument("--name")
    add_repo.add_argument("--commit")
    add_repo.add_argument("--branch")
    add_repo.add_argument("--remote")
    add_repo.add_argument("--require-clean", action="store_true")
    add_repo.set_defaults(func=cmd_add_repo)

    sync_repos = sub.add_parser("sync-repos", help="Fetch and checkout declared git requirements.")
    sync_repos.add_argument("--manifest", required=True)
    sync_repos.add_argument("--profile")
    sync_repos.set_defaults(func=cmd_sync_repos)

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

    wait = sub.add_parser("wait", help="Wait until coordinator tasks finish.")
    wait.add_argument("--manifest", required=True)
    wait.add_argument("--profile")
    wait.add_argument("--lease-seconds", type=int, default=600)
    wait.add_argument("--interval-seconds", type=float, default=30.0)
    wait.add_argument("--timeout-seconds", type=float, default=0.0)
    wait.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True)
    wait.set_defaults(func=cmd_wait)

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

    self_update = sub.add_parser("self-update", help="Fast-forward the distrib checkout itself.")
    self_update.add_argument("--remote", default="origin")
    self_update.add_argument("--branch")
    self_update.add_argument("--require-clean", action=argparse.BooleanOptionalAction, default=True)
    self_update.add_argument("--json", action="store_true")
    self_update.set_defaults(func=cmd_self_update)

    version = sub.add_parser("version", help="Print distrib version.")
    version.add_argument("--json", action="store_true")
    version.set_defaults(func=cmd_version)

    deploy = sub.add_parser("deploy", help="Verify workers and run manifest tasks.")
    deploy.add_argument("workers")
    deploy.add_argument("manifest")
    deploy.add_argument("--jobs", type=int, default=4)
    deploy.add_argument("--remote-timeout-seconds", type=float, default=1800.0)
    deploy.add_argument("--no-color", action="store_true")
    deploy.add_argument("--verbose", action="store_true")
    deploy.set_defaults(func=cmd_deploy)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.version or args.version_json:
        args.json = bool(args.version_json)
        return cmd_version(args)
    if getattr(args, "command", None) is None:
        parser.error("a command is required")
    if getattr(args, "command", None) == "run" and args.task is None and args.index is None:
        parser.error("run requires --task or --index")
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
