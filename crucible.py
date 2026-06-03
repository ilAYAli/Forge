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


SCHEMA = "crucible.task.v1"
RUNS_DIR_NAME = "runs"
RUN_MANIFEST_NAME = "manifest.json"
RUN_WORKERS_NAME = "workers.json"
RUN_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
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


def runs_dir() -> Path:
    return script_repo() / RUNS_DIR_NAME


def validate_run_name(name: str) -> str:
    run_name = name.strip()
    if not RUN_NAME_RE.fullmatch(run_name):
        raise SystemExit(
            f"{name!r}: invalid run name; use letters, numbers, dot, underscore, or dash"
        )
    return run_name


def run_dir_for_name(name: str) -> Path:
    return runs_dir() / validate_run_name(name)


def run_dir_from_arg(value: str | Path) -> Path:
    raw = str(value)
    path = Path(raw)
    if not path.is_absolute() and len(path.parts) == 1:
        return run_dir_for_name(raw)
    if not path.is_absolute() and path.parts and path.parts[0] == RUNS_DIR_NAME:
        return script_repo() / path
    return expand_path(raw)


def run_manifest_path(run: str | Path) -> Path:
    path = run_dir_from_arg(run)
    return path / RUN_MANIFEST_NAME


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


def count_file_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _ in handle)


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


def item_profiles(item: dict[str, Any], *, field: str) -> set[str]:
    values: list[Any] = []
    if "profile" in item:
        values.append(item.get("profile"))
    if "profiles" in item:
        raw = item.get("profiles")
        if isinstance(raw, str):
            values.append(raw)
        elif isinstance(raw, list):
            values.extend(raw)
        else:
            raise SystemExit(f"{field}.profiles must be a string or list of strings")
    profiles = {str(value).strip() for value in values if str(value).strip()}
    if "" in profiles:
        profiles.remove("")
    return profiles


def item_matches_profile(item: dict[str, Any], active_profile: str | None, *, field: str) -> bool:
    profiles = item_profiles(item, field=field)
    if not profiles:
        return True
    return bool(active_profile and active_profile in profiles)


def manifest_profile_names(manifest: dict[str, Any]) -> set[str]:
    profiles = set()
    path_maps = manifest.get("path_maps", {})
    if isinstance(path_maps, dict):
        profiles.update(str(profile) for profile in path_maps)
    req = manifest.get("requirements", {})
    if isinstance(req, dict):
        for field in ("inputs", "commands", "repos"):
            raw = req.get(field, [])
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        profiles.update(item_profiles(item, field=f"requirements.{field}"))
        git_req = req.get("git")
        if isinstance(git_req, dict):
            profiles.update(item_profiles(git_req, field="requirements.git"))
    return profiles


def apply_profile(manifest: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return manifest
    path_maps = manifest.get("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    selected = path_maps.get(profile)
    if selected is None:
        if profile not in manifest_profile_names(manifest):
            raise SystemExit(f"unknown profile {profile!r}")
        selected = {}
    if not isinstance(selected, dict):
        raise SystemExit(f"path_maps.{profile} must be an object")
    translated = translate_obj(copy.deepcopy(manifest), {str(k): str(v) for k, v in selected.items()})
    translated["_active_profile"] = profile
    return translated


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


def crucible_version_payload() -> dict[str, Any]:
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
        "tool": "crucible",
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
    print(f"crucible {payload['commit']} {payload['branch']}{suffix}")


def validate_manifest_payload(manifest: dict[str, Any], *, label: str) -> None:
    if manifest.get("schema") != SCHEMA:
        raise SystemExit(f"{label}: unsupported schema {manifest.get('schema')!r}")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        raise SystemExit(f"{label}: tasks must be a list")
    seen: set[str] = set()
    seen_outputs: dict[str, str] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise SystemExit(f"{label}: each task must be an object")
        task_id = str(task.get("id", ""))
        if not task_id:
            raise SystemExit(f"{label}: task is missing id")
        if task_id in seen:
            raise SystemExit(f"{label}: duplicate task id {task_id}")
        seen.add(task_id)
        outputs = task.get("outputs", [])
        if not isinstance(outputs, list):
            raise SystemExit(f"{label}: {task_id}: outputs must be a list")
        for output in outputs:
            output_key = str(output)
            other_task = seen_outputs.get(output_key)
            if other_task is not None:
                raise SystemExit(
                    f"{label}: duplicate output path {output_key!r} in "
                    f"{other_task} and {task_id}")
            seen_outputs[output_key] = task_id


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = read_json(path)
    validate_manifest_payload(manifest, label=str(path))
    return manifest


def task_template_values(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    vars_obj = manifest.get("vars", {})
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    values: dict[str, Any] = {
        "task_id": task["id"],
        "index": task.get("index", ""),
        "shards": len(manifest["tasks"]),
    }
    values.update({str(k): v for k, v in vars_obj.items()})
    values.update({str(k): v for k, v in task.items() if k not in {"command", "outputs"}})
    return values


def materialize_manifest(template: dict[str, Any], *, run_name: str, run_dir: Path) -> dict[str, Any]:
    manifest = copy.deepcopy(template)
    manifest["name"] = run_name
    manifest["state_dir"] = str(run_dir / "state")
    manifest["log_dir"] = str(run_dir / "logs")
    manifest["claim_dir"] = str(run_dir / "claims")
    vars_obj = manifest.get("vars", {})
    if vars_obj in (None, ""):
        vars_obj = {}
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    vars_obj = dict(vars_obj)
    vars_obj["run_dir"] = str(run_dir)
    vars_obj["output_dir"] = str(run_dir / "outputs")
    manifest["vars"] = vars_obj
    for task in manifest["tasks"]:
        outputs = task.get("outputs", [])
        if not isinstance(outputs, list):
            raise SystemExit(f"{task['id']}: outputs must be a list")
        values = task_template_values(manifest, task)
        task["outputs"] = [
            str(expand_path(str(output).format(**values)))
            for output in outputs
        ]
    validate_manifest_payload(manifest, label=f"run {run_name}")
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


def input_requirement_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    inputs = req.get("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    active_profile = str(manifest.get("_active_profile") or "").strip() or None
    items = []
    for item in inputs:
        if not isinstance(item, dict):
            raise SystemExit("requirements.inputs entries must be objects")
        if item_matches_profile(item, active_profile, field="requirements.inputs"):
            items.append(item)
    return items


def check_requirements(manifest: dict[str, Any]) -> None:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    active_profile = str(manifest.get("_active_profile") or "").strip() or None

    python_min = str(req.get("python_min", "")).strip()
    if python_min:
        actual = ".".join(map(str, sys.version_info[:3]))
        require_min_version("python", actual, python_min)

    git_min = str(req.get("git_min", "")).strip()
    if git_min:
        actual = command_output(["git", "--version"])
        require_min_version("git", actual, git_min)

    for item in iter_git_requirements(manifest, req):
        if not item_matches_profile(item, active_profile, field="requirements.repos"):
            continue
        check_git_requirement(item)
        run_repo_hook(item, "probe", repo=expand_path(str(item.get("path") or item.get("repo") or ".")))

    commands = req.get("commands", [])
    if commands in (None, ""):
        commands = []
    if not isinstance(commands, list):
        raise SystemExit("requirements.commands must be a list")
    for item in commands:
        if not isinstance(item, dict):
            raise SystemExit("requirements.commands entries must be objects")
        if not item_matches_profile(item, active_profile, field="requirements.commands"):
            continue
        check_command_requirement(item)

    for item in input_requirement_items(manifest):
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


def stop_request_path(manifest: dict[str, Any]) -> Path:
    return state_dir(manifest) / "stop.json"


def stop_requested(manifest: dict[str, Any]) -> bool:
    return stop_request_path(manifest).exists()


def task_outputs(task: dict[str, Any]) -> list[Path]:
    outputs = task.get("outputs", [])
    if not isinstance(outputs, list):
        raise SystemExit(f"{task['id']}: outputs must be a list")
    return [expand_path(str(path)) for path in outputs]


def format_command(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    outputs = task_outputs(task)
    values = task_template_values(manifest, task)
    values["output"] = str(outputs[0]) if outputs else ""
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
    if stop_requested(manifest):
        return None
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
    progress_data: dict[str, Any] | None = None,
) -> None:
    payload = check_claim_owner(manifest, task, worker=worker, claim_id=claim_id)
    payload["heartbeat_at"] = now()
    payload["heartbeat_epoch"] = epoch()
    if progress_data is not None:
        payload["progress_data"] = progress_data
    write_json(task_heartbeat_path(manifest, task), payload)


def parse_coordinator(value: str) -> tuple[str | None, str]:
    if ":" in value and not value.startswith("/"):
        host, path = value.split(":", 1)
        if host:
            return host, path
    return None, value


def shell_join(args: list[str]) -> str:
    return " ".join(shlex.quote(item) for item in args)


def remote_crucible_command(args: list[str]) -> str:
    quoted_args = shell_join(args)
    return (
        "command -v crucible >/dev/null 2>&1 || "
        "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
        f"exec crucible {quoted_args}"
    )


def run_coordinator_cli(coordinator: str, args: list[str]) -> subprocess.CompletedProcess[str]:
    host, manifest_path = parse_coordinator(coordinator)
    resolved_args = [item.replace("{manifest}", manifest_path) for item in args]
    if host:
        return subprocess.run(
            ["ssh", *SSH_OPTS, host, remote_crucible_command(resolved_args)],
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
    progress_enabled = any([
        args.progress_unit,
        args.progress_total,
        args.progress_total_lines,
        args.progress_log_regex,
        args.progress_output_unit,
    ])
    if args.progress_total and args.progress_total_lines:
        raise SystemExit("choose only one of --progress-total or --progress-total-lines")
    if progress_enabled and not args.progress_log_regex:
        raise SystemExit("--progress-log-regex is required when progress metadata is set")
    progress_line_totals: list[int] | None = None
    if args.progress_total_lines:
        total_lines = count_file_lines(expand_path(str(args.progress_total_lines).format(**vars_obj)))
        base = total_lines // args.shards
        extra = total_lines % args.shards
        progress_line_totals = [
            base + (1 if index < extra else 0)
            for index in range(args.shards)
        ]
    for index in range(args.shards):
        task_id = f"{args.task_prefix}_{index:0{width}d}"
        format_vars = {"index": index, "shard": index, "task_id": task_id, "shards": args.shards, **vars_obj}
        outputs = [
            template.format(**format_vars)
            for template in args.output_template
        ]
        for output in outputs:
            other_task = generated_outputs.get(output)
            if other_task is not None:
                raise SystemExit(
                    f"{out}: duplicate output path {output!r} in "
                    f"{other_task} and {task_id}")
            generated_outputs[output] = task_id
        task = {
            "id": task_id,
            "index": index,
            "command": args.command_template,
            "outputs": outputs,
        }
        if progress_enabled:
            progress: dict[str, Any] = {
                "log_regex": str(args.progress_log_regex),
            }
            if args.progress_unit:
                progress["unit"] = str(args.progress_unit)
            if args.progress_output_unit:
                progress["output_unit"] = str(args.progress_output_unit)
            if args.progress_total:
                total = str(args.progress_total).format(**format_vars)
                try:
                    progress["total"] = int(total)
                except ValueError:
                    progress["total"] = total
            elif progress_line_totals is not None:
                progress["total"] = progress_line_totals[index]
            task["progress"] = progress
        tasks.append(task)

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
    progress_data: dict[str, Any] | None = None,
) -> None:
    command = ["heartbeat", "--manifest", "{manifest}", "--task", task_id, "--worker", worker]
    if claim_id:
        command.extend(["--claim-id", claim_id])
    if progress_data:
        command.extend([
            "--progress-json",
            json.dumps(progress_data, separators=(",", ":"), sort_keys=True),
        ])
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
    local_manifest: dict[str, Any],
    local_task: dict[str, Any],
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
    while not stop.is_set():
        try:
            heartbeat_on_coordinator(
                coordinator,
                task_id,
                worker,
                claim_id,
                task_log_progress_data(local_manifest, local_task),
            )
        except Exception as exc:
            print(f"{task_id}: heartbeat failed: {exc}", file=sys.stderr)
        stop.wait(interval)


def check_coordinator_cli(coordinator: str) -> None:
    result = run_coordinator_cli(coordinator, ["version", "--json"])
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "coordinator crucible check failed"
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
                "local_manifest": local_manifest,
                "local_task": local_task,
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
    progress_data = None
    if args.progress_json:
        try:
            progress_data = json.loads(args.progress_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"--progress-json is not valid JSON: {exc}") from exc
        if not isinstance(progress_data, dict):
            raise SystemExit("--progress-json must be a JSON object")
    update_claim_heartbeat(
        manifest,
        task,
        worker=args.worker,
        claim_id=args.claim_id,
        progress_data=progress_data,
    )
    print(task["id"])
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    color_enabled = not bool(args.no_color) and sys.stdout.isatty()
    if args.run is None:
        summaries = load_run_summaries(lease_seconds=args.lease_seconds)
        return print_run_list(
            summaries,
            color_enabled=color_enabled,
            as_json=args.json,
            filter_name="all",
        )

    manifest_path = run_manifest_path(args.run)
    manifest = load_manifest(manifest_path)
    summary = status_summary(manifest, lease_seconds=args.lease_seconds)
    summary["manifest"] = str(manifest_path)
    summary["run"] = manifest_path.parent.name
    if args.json:
        print(json.dumps(summary, sort_keys=True))
        return 0 if summary["counts"]["fail"] == 0 else 1
    print(colorize(f"== {format_manifest_path(manifest_path)} ==", "cyan", enabled=color_enabled))
    print_compact_status(
        summary,
        color_enabled=color_enabled,
        show_done=args.show_done or args.verbose,
    )
    if args.verbose:
        for row in summary["rows"]:
            print(f"{row['id']}\t{row['state']}\t{row['worker']}")
    return 0 if summary["counts"]["fail"] == 0 else 1


def run_manifest_paths() -> list[Path]:
    root = runs_dir()
    if not root.exists():
        return []
    return sorted(
        path / RUN_MANIFEST_NAME
        for path in root.iterdir()
        if path.is_dir() and (path / RUN_MANIFEST_NAME).is_file()
    )


def load_run_summaries(*, lease_seconds: int) -> list[dict[str, Any]]:
    summaries = []
    for path in run_manifest_paths():
        manifest = load_manifest(path)
        summary = status_summary(manifest, lease_seconds=lease_seconds)
        summary["manifest"] = str(path)
        summary["run"] = path.parent.name
        summaries.append(summary)
    return summaries


def run_done(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    return (
        counts["done"] == summary["tasks"]
        and counts["fail"] == 0
        and counts["running"] == 0
        and counts["claimed"] == 0
        and counts["stale"] == 0
        and counts["pending"] == 0
    )


def run_failed(summary: dict[str, Any]) -> bool:
    return summary["counts"]["fail"] > 0


def run_current(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    return counts["running"] + counts["claimed"] + counts["stale"] + counts["pending"] > 0


def run_state(summary: dict[str, Any]) -> str:
    if run_failed(summary):
        return "failed"
    if run_current(summary):
        return "current"
    if run_done(summary):
        return "done"
    return "empty"


def filtered_run_summaries(summaries: list[dict[str, Any]], filter_name: str | None) -> list[dict[str, Any]]:
    if filter_name == "done":
        return [item for item in summaries if run_done(item)]
    if filter_name in {None, "current"}:
        return [item for item in summaries if run_current(item)]
    if filter_name == "all":
        return summaries
    if filter_name == "failed":
        return [item for item in summaries if run_failed(item)]
    return summaries


def compact_count(value: int) -> str:
    if value >= 1_000_000:
        text = f"{value / 1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{text}m"
    if value >= 10_000:
        return f"{value // 1000}k"
    return str(value)


def compact_duration(seconds: float) -> str:
    remaining = max(0, int(round(seconds)))
    minutes, seconds = divmod(remaining, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d{hours:02d}h"
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def progress_eta_text(done: Any, total: Any, rate: Any) -> str:
    if not isinstance(done, int) or not isinstance(total, int) or total <= 0:
        return ""
    if not isinstance(rate, (int, float)) or rate <= 0:
        return ""
    return f"eta={compact_duration((total - done) / float(rate))}"


def progress_text(data: dict[str, Any]) -> str:
    if data.get("kind") == "subtask":
        done = data.get("done")
        total = data.get("total")
        done_text = str(done) if isinstance(done, int) else "?"
        total_text = str(total) if isinstance(total, int) and total > 0 else "?"
        parts = [f"subtask={done_text}/{total_text}"]
        eta = progress_eta_text(done, total, data.get("rate"))
        if eta:
            parts.append(eta)
        return ", ".join(parts)

    done = data.get("done")
    total = data.get("total")
    unit = str(data.get("unit") or "").strip()
    output = data.get("output")
    output_unit = str(data.get("output_unit") or "output").strip() or "output"

    if isinstance(done, int) and isinstance(total, int) and total > 0:
        main = f"{compact_count(done)}/{compact_count(total)}"
    elif isinstance(done, int):
        main = f"~{compact_count(done)}"
    else:
        main = ""
    if main and unit:
        main = f"{main} {unit}"

    parts = [main] if main else []
    if isinstance(output, int):
        parts.append(f"{output_unit}={compact_count(output)}")
    eta = progress_eta_text(done, total, data.get("rate"))
    if eta:
        parts.append(eta)
    return ", ".join(parts)


def summary_progress(summary: dict[str, Any]) -> str:
    subtask_done = 0
    subtask_total = 0
    subtask_rate = 0.0
    subtask_count = 0
    subtask_known_total = True
    subtask_known_rate = True
    by_unit: dict[str, dict[str, Any]] = {}
    outputs: dict[str, int] = {}
    etas: list[str] = []
    for row in summary["active"]:
        data = row.get("progress_data")
        if isinstance(data, dict) and data.get("kind") == "metadata":
            done = data.get("done")
            if isinstance(done, int):
                unit = str(data.get("unit") or "items")
                bucket = by_unit.setdefault(
                    unit,
                    {"done": 0, "total": 0, "rate": 0.0, "count": 0, "all_total": True, "all_rate": True},
                )
                bucket["count"] += 1
                bucket["done"] += done
                total = data.get("total")
                if isinstance(total, int) and total > 0:
                    bucket["total"] += total
                else:
                    bucket["all_total"] = False
                rate = data.get("rate")
                if isinstance(rate, (int, float)) and rate > 0:
                    bucket["rate"] += float(rate)
                else:
                    bucket["all_rate"] = False
            output = data.get("output")
            if isinstance(output, int):
                output_unit = str(data.get("output_unit") or "output")
                outputs[output_unit] = outputs.get(output_unit, 0) + output
            continue
        if isinstance(data, dict) and data.get("kind") == "subtask":
            done = data.get("done")
            total = data.get("total")
            if isinstance(done, int):
                subtask_count += 1
                subtask_done += done
                if isinstance(total, int) and total > 0:
                    subtask_total += total
                else:
                    subtask_known_total = False
                rate = data.get("rate")
                if isinstance(rate, (int, float)) and rate > 0:
                    subtask_rate += float(rate)
                else:
                    subtask_known_rate = False
                continue

    parts: list[str] = []
    if subtask_done or subtask_total:
        total = str(subtask_total) if subtask_known_total and subtask_total > 0 else "?"
        text = f"subtask={subtask_done}/{total}"
        if (
            subtask_known_total and subtask_known_rate and subtask_count == summary["active_count"]
            and summary["counts"]["pending"] == 0
        ):
            eta = progress_eta_text(subtask_done, subtask_total, subtask_rate)
            if eta:
                etas.append(eta)
        parts.append(text)
    for unit, bucket in sorted(by_unit.items()):
        done = int(bucket["done"])
        total = int(bucket["total"])
        if bucket["all_total"] and total > 0:
            text = f"{compact_count(done)}/{compact_count(total)} {unit}"
            if (
                bucket["all_rate"] and bucket["count"] == summary["active_count"]
                and summary["counts"]["pending"] == 0
            ):
                eta = progress_eta_text(done, total, bucket["rate"])
                if eta:
                    etas.append(eta)
            parts.append(text)
        else:
            parts.append(f"~{compact_count(done)} {unit}")
    for output_unit, value in sorted(outputs.items()):
        parts.append(f"{output_unit}={compact_count(value)}")
    parts.extend(etas)
    return f"progress={', '.join(parts)}" if parts else ""


def list_run_payload(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary["counts"]
    payload = {
        "run": summary["run"],
        "state": run_state(summary),
        "manifest": summary["manifest"],
        "tasks": summary["tasks"],
        "done": counts["done"],
        "active": counts["running"] + counts["claimed"] + counts["stale"],
        "running": counts["running"],
        "claimed": counts["claimed"],
        "pending": counts["pending"],
        "fail": counts["fail"],
        "stale": counts["stale"],
    }
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
    return payload


def print_run_list(
    summaries: list[dict[str, Any]],
    *,
    color_enabled: bool,
    as_json: bool,
    filter_name: str | None,
) -> int:
    filtered = filtered_run_summaries(summaries, filter_name)
    if as_json:
        print(json.dumps({"runs": [list_run_payload(item) for item in filtered]}, sort_keys=True))
        return 0 if all(item["counts"]["fail"] == 0 for item in filtered) else 1
    if not filtered:
        print(f"no Crucible runs found in {runs_dir()}")
        return 0
    for summary in filtered:
        counts = summary["counts"]
        active = counts["running"] + counts["claimed"] + counts["stale"]
        state = run_state(summary)
        state_color = "red" if state == "failed" else "yellow" if state == "current" else "green"
        progress = summary_progress(summary)
        fields = [
            f"{summary['run']:<36} "
            f"{colorize(state, state_color, enabled=color_enabled):<8}",
            f"tasks={counts['done']}/{summary['tasks']}",
            f"active={active}",
            f"running={counts['running']}",
            f"claimed={counts['claimed']}",
        ]
        if progress:
            fields.append(progress)
        fields.extend([
            f"fail={counts['fail']}",
            f"pending={counts['pending']}",
            f"stale={counts['stale']}",
        ])
        print(" ".join(fields))
    return 0 if all(item["counts"]["fail"] == 0 for item in filtered) else 1


def cmd_list(args: argparse.Namespace) -> int:
    selected = [name for name in ("all", "done", "failed") if bool(getattr(args, name))]
    if len(selected) > 1:
        raise SystemExit("choose only one of --all, --done, or --failed")
    summaries = load_run_summaries(lease_seconds=args.lease_seconds)
    return print_run_list(
        summaries,
        color_enabled=not bool(args.no_color) and sys.stdout.isatty(),
        as_json=args.json,
        filter_name=selected[0] if selected else None,
    )


def load_run_manifest(run: str | Path) -> tuple[Path, dict[str, Any]]:
    path = run_manifest_path(run)
    if not path.exists():
        raise SystemExit(f"{run}: no Crucible run manifest found at {path}")
    return path, load_manifest(path)


def run_summary_for(run: str | Path, *, lease_seconds: int) -> dict[str, Any]:
    path, manifest = load_run_manifest(run)
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    summary["manifest"] = str(path)
    summary["run"] = path.parent.name
    return summary


def cmd_stop(args: argparse.Namespace) -> int:
    if not args.all and not args.run:
        raise SystemExit("stop requires a run name or --all")
    targets = (
        [Path(item["manifest"]) for item in load_run_summaries(lease_seconds=args.lease_seconds) if run_current(item)]
        if args.all else
        [run_manifest_path(args.run)]
    )
    stopped = 0
    for path in targets:
        manifest = load_manifest(path)
        payload = {
            "schema": SCHEMA,
            "run": path.parent.name,
            "requested_at": now(),
            "requested_epoch": epoch(),
            "host": socket.gethostname(),
        }
        write_json(stop_request_path(manifest), payload)
        stopped += 1
        print(f"stop requested: {path.parent.name}")
    if stopped == 0:
        print("no current runs to stop")
    return 0


def confirm_delete(paths: list[Path], *, force: bool) -> None:
    if force:
        return
    if not sys.stdin.isatty():
        raise SystemExit("pass --force to delete runs non-interactively")
    names = ", ".join(path.name for path in paths)
    answer = input(f"delete Crucible run(s) {names}? type 'yes' to confirm: ")
    if answer != "yes":
        raise SystemExit("delete cancelled")


def cmd_del(args: argparse.Namespace) -> int:
    if args.done and args.run:
        raise SystemExit("del accepts either a run name or --done")
    if not args.done and not args.run:
        raise SystemExit("del requires a run name or --done")
    summaries = load_run_summaries(lease_seconds=args.lease_seconds)
    targets: list[Path] = []
    if args.done:
        targets = [Path(item["manifest"]).parent for item in summaries if run_done(item)]
    else:
        summary = run_summary_for(args.run, lease_seconds=args.lease_seconds)
        if run_current(summary) and not args.force:
            raise SystemExit(f"{args.run}: run is active; stop it first or pass --force")
        targets = [Path(summary["manifest"]).parent]
    if not targets:
        print("no runs matched")
        return 0
    confirm_delete(targets, force=bool(args.force))
    for path in targets:
        shutil.rmtree(path)
        print(f"deleted: {path.name}")
    return 0


def format_manifest_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        try:
            return str(path.resolve().relative_to(Path.cwd().resolve()))
        except ValueError:
            return str(path)


def task_counts(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, int]:
    counts = {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}
    for task in manifest["tasks"]:
        counts[task_state(manifest, task, lease_seconds=lease_seconds)] += 1
    return counts


def task_display_index(task: dict[str, Any]) -> int | str:
    index = task.get("index")
    if isinstance(index, int):
        return index
    match = re.search(r"(\d+)$", str(task.get("id", "")))
    if match:
        return int(match.group(1))
    return str(task.get("id", ""))


def status_worker(manifest: dict[str, Any], task: dict[str, Any], state: str) -> str:
    paths: list[Path] = []
    if state == "done":
        paths.append(task_done_path(manifest, task))
    elif state == "fail":
        paths.append(task_fail_path(manifest, task))
    elif state in {"claimed", "running", "stale"}:
        paths.append(task_claim_path(manifest, task))
        paths.append(task_running_path(manifest, task))

    for path in paths:
        try:
            payload = read_json(path)
        except Exception:
            continue
        worker = payload.get("worker")
        if isinstance(worker, dict):
            worker = worker.get("host")
        worker = worker or payload.get("host")
        if worker:
            return str(worker)
    return ""


def log_tail(path: Path, *, max_bytes: int = 8192) -> str:
    with path.open("rb") as file:
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(max(0, size - max_bytes))
        return file.read().decode("utf-8", errors="replace")


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.replace("_", "").replace(",", ""))
    except ValueError:
        return None


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value.replace("_", "").replace(",", ""))
    except ValueError:
        return None


def normalize_progress_data(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    data = dict(raw)
    done = data.get("done")
    if not isinstance(done, int):
        done = parse_int(str(done)) if done is not None else None
    if done is None:
        return None
    data["done"] = done

    total = data.get("total")
    if not isinstance(total, int):
        total = parse_int(str(total)) if total is not None else None
    if total is not None:
        data["total"] = total

    output = data.get("output")
    if not isinstance(output, int):
        output = parse_int(str(output)) if output is not None else None
    if output is not None:
        data["output"] = output

    rate = data.get("rate")
    if not isinstance(rate, (int, float)):
        rate = parse_float(str(rate)) if rate is not None else None
    if isinstance(rate, (int, float)) and rate > 0:
        data["rate"] = float(rate)

    kind = str(data.get("kind") or "").strip()
    if kind not in {"metadata", "subtask"}:
        kind = "metadata" if data.get("unit") else "subtask"
    data["kind"] = kind
    data["text"] = progress_text(data)
    return data


def task_log_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    path = task_log_path(manifest, task)
    try:
        text = log_tail(path)
    except OSError:
        return None

    config = task.get("progress")
    if isinstance(config, dict) and config.get("log_regex"):
        try:
            progress_re = re.compile(str(config["log_regex"]))
        except re.error:
            return None
        for line in reversed(text.splitlines()):
            match = progress_re.search(line)
            if not match:
                continue
            done = parse_int(match.groupdict().get("done"))
            if done is None:
                continue
            total = parse_int(match.groupdict().get("total"))
            if total is None:
                total_value = config.get("total")
                total = total_value if isinstance(total_value, int) else parse_int(str(total_value)) if total_value is not None else None
            output = parse_int(match.groupdict().get("output"))
            rate = parse_float(match.groupdict().get("rate"))
            data: dict[str, Any] = {
                "done": done,
                "unit": str(config.get("unit") or ""),
                "output_unit": str(config.get("output_unit") or "output"),
                "kind": "metadata",
            }
            if total is not None:
                data["total"] = total
            if output is not None:
                data["output"] = output
            if rate is not None:
                data["rate"] = rate
            return normalize_progress_data(data)

    progress_re = re.compile(r"\[\s*\d+/\d+\s+(\d+)\s*/\s*(\d+)\]")
    for line in reversed(text.splitlines()):
        match = progress_re.search(line)
        if match:
            done = int(match.group(1))
            total = int(match.group(2))
            return normalize_progress_data({"kind": "subtask", "done": done, "total": total})
    return None


def task_heartbeat_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    try:
        payload = read_json(task_heartbeat_path(manifest, task))
    except Exception:
        return None
    return normalize_progress_data(payload.get("progress_data"))


def status_summary(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, Any]:
    counts = {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}
    rows: list[dict[str, Any]] = []
    groups: dict[str, dict[str, list[int | str]]] = {
        "done": {},
        "fail": {},
        "stale": {},
    }
    active: list[dict[str, Any]] = []
    pending: list[int | str] = []

    for task in manifest["tasks"]:
        state = task_state(manifest, task, lease_seconds=lease_seconds)
        counts[state] += 1
        worker = status_worker(manifest, task, state)
        index = task_display_index(task)
        progress_data = task_log_progress_data(manifest, task) if state == "running" else None
        if progress_data is None and state in {"running", "claimed", "stale"}:
            progress_data = task_heartbeat_progress_data(manifest, task)
        progress = str(progress_data.get("text") or "") if isinstance(progress_data, dict) else ""
        row = {
            "id": task["id"],
            "index": index,
            "state": state,
            "worker": worker,
            "progress": progress,
        }
        if progress_data:
            row["progress_data"] = progress_data
        rows.append(row)

        if state in {"done", "fail", "stale"}:
            key = worker or "unknown"
            groups[state].setdefault(key, []).append(index)
        elif state in {"running", "claimed"}:
            active.append(row)
        elif state == "pending":
            pending.append(index)

    return {
        "tasks": len(manifest["tasks"]),
        "counts": counts,
        "active_count": counts["running"] + counts["claimed"],
        "groups": groups,
        "active": active,
        "pending": pending,
        "rows": rows,
    }


def sort_status_values(values: list[int | str]) -> list[int | str]:
    return sorted(values, key=status_value_key)


def status_value_key(value: int | str) -> tuple[bool, int | str]:
    return (not isinstance(value, int), value)


def format_status_values(values: list[int | str]) -> str:
    values = sort_status_values(values)
    if not values:
        return "-"
    if not all(isinstance(value, int) for value in values):
        return ",".join(str(value) for value in values)

    ranges: list[str] = []
    start = prev = int(values[0])
    for value in values[1:]:
        current = int(value)
        if current == prev + 1:
            prev = current
            continue
        ranges.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = current
    ranges.append(f"{start}-{prev}" if start != prev else str(start))
    return ",".join(ranges)


def status_color(state: str) -> str:
    if state == "done":
        return "green"
    if state in {"running", "claimed", "pending"}:
        return "yellow"
    return "red"


def print_status_group(title: str, groups: dict[str, list[int | str]], *, color_enabled: bool) -> None:
    if not groups:
        return
    print(colorize(f"{title}:", status_color(title), enabled=color_enabled))
    for worker, values in sorted(groups.items()):
        print(f"  {worker} [{len(values)}]: {format_status_values(values)}")


def print_compact_status(
    summary: dict[str, Any],
    *,
    color_enabled: bool = False,
    show_done: bool = False,
) -> None:
    counts = summary["counts"]
    print(
        f"tasks={counts['done']}/{summary['tasks']} "
        f"active={summary['active_count']} running={counts['running']} "
        f"claimed={counts['claimed']} pending={counts['pending']} "
        f"fail={counts['fail']} stale={counts['stale']}"
    )
    if show_done:
        print_status_group("done", summary["groups"]["done"], color_enabled=color_enabled)
    if summary["active"]:
        print(colorize("active:", "yellow", enabled=color_enabled))
        for row in sorted(summary["active"], key=lambda item: status_value_key(item["index"])):
            progress = f" [{row['progress']}]" if row.get("progress") else ""
            worker = row["worker"] or "unknown"
            state = colorize(str(row["state"]), status_color(str(row["state"])), enabled=color_enabled)
            print(f"  {worker} {state}: {row['index']}{progress}")
    print_status_group("fail", summary["groups"]["fail"], color_enabled=color_enabled)
    print_status_group("stale", summary["groups"]["stale"], color_enabled=color_enabled)
    if summary["pending"]:
        print(colorize("pending:", "yellow", enabled=color_enabled))
        print(f"  {format_status_values(summary['pending'])}")


def cmd_wait(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + args.timeout_seconds if args.timeout_seconds > 0 else None
    last_line = ""
    while True:
        manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
        counts = task_counts(manifest, lease_seconds=args.lease_seconds)
        total = len(manifest["tasks"])
        line = (
            f"tasks={counts['done']}/{total} running={counts['running']} "
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
    _, manifest = load_run_manifest(args.run)
    verify_manifest(manifest)
    print("ok")
    return 0


def append_file(src: Path, dst: Path, *, newline_between: bool) -> None:
    with src.open("rb") as in_handle, dst.open("ab") as out_handle:
        shutil.copyfileobj(in_handle, out_handle, length=1024 * 1024)
        if newline_between:
            out_handle.write(b"\n")


def cmd_merge(args: argparse.Namespace) -> int:
    manifest_path, manifest = load_run_manifest(args.run)
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
        "manifest": str(manifest_path),
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
        raise SystemExit("crucible is on a detached HEAD; pass --branch explicitly")
    run_checked(["git", "-C", str(repo), "fetch", args.remote])
    if current_branch != branch:
        run_checked(["git", "-C", str(repo), "checkout", branch])
    run_checked(["git", "-C", str(repo), "pull", "--ff-only", args.remote, branch])
    print_version_payload(crucible_version_payload(), as_json=args.json)
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    print_version_payload(crucible_version_payload(), as_json=args.json)
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
    slots = parse_worker_slots(item.get("slots", 1), label=host)
    if slots != 1:
        record["slots"] = slots
    if item.get("cache_dir"):
        record["cache_dir"] = str(item["cache_dir"])
    if item.get("python"):
        record["python"] = str(item["python"])
    return record


def parse_worker_slots(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise SystemExit(f"{label}: worker slots must be a positive integer")
    try:
        slots = int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{label}: worker slots must be a positive integer") from exc
    if slots < 1:
        raise SystemExit(f"{label}: worker slots must be a positive integer")
    return slots


def expand_worker_record(worker: dict[str, Any]) -> list[dict[str, Any]]:
    slots = int(worker.pop("slots", 1))
    if slots == 1:
        return [worker]
    workers = []
    for slot in range(1, slots + 1):
        expanded = dict(worker)
        expanded["_slot"] = slot
        expanded["_slots"] = slots
        workers.append(expanded)
    return workers


def load_workers(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if payload.get("schema") not in (None, "crucible.workers.v1"):
        raise SystemExit(f"{path}: unsupported schema {payload.get('schema')!r}")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise SystemExit(f"{path}: workers must be a list")
    workers: list[dict[str, Any]] = []
    for item in raw_workers:
        worker = parse_worker_record(item)
        if worker:
            workers.extend(expand_worker_record(worker))
    if not workers:
        raise SystemExit(f"{path}: no enabled workers")
    return workers


def worker_cache_dir(worker: dict[str, Any]) -> str:
    if worker.get("cache_dir"):
        return str(worker["cache_dir"])
    if is_local_worker(worker):
        return str(Path.home() / ".cache/crucible")
    return ".cache/crucible"


def worker_cache_file(worker: dict[str, Any], name: str) -> str:
    return str(Path(worker_cache_dir(worker)) / name)


def is_local_worker(worker: dict[str, Any]) -> bool:
    return bool(worker.get("local", False))


def worker_python(worker: dict[str, Any]) -> str:
    return str(worker.get("python") or "python3")


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
    copy_to_worker(worker, script_path(), worker_cache_file(worker, "crucible.py"), timeout=timeout, tracker=tracker)
    copy_to_worker(worker, manifest_path, worker_cache_file(worker, "manifest.json"), timeout=timeout, tracker=tracker)


def active_worker_profile(manifest: dict[str, Any], worker: dict[str, Any]) -> str | None:
    profile = str(worker.get("profile") or "").strip()
    if not profile:
        return None
    if profile not in manifest_profile_names(manifest):
        return None
    return profile


def worker_profile_args(manifest: dict[str, Any], worker: dict[str, Any]) -> list[str]:
    profile = active_worker_profile(manifest, worker)
    if not profile:
        return []
    return ["--profile", profile]


def worker_manifest(manifest: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    profile = active_worker_profile(manifest, worker)
    if profile:
        return apply_profile(manifest, profile)
    return manifest


def local_manifest_for_worker(manifest: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    profile = active_worker_profile(manifest, worker)
    local_manifest = copy.deepcopy(manifest)
    if profile:
        local_manifest["_active_profile"] = profile
    return local_manifest


def current_host_names() -> set[str]:
    return {socket.gethostname(), socket.getfqdn(), "localhost", "127.0.0.1"}


def worker_is_current_host(worker: dict[str, Any]) -> bool:
    return str(worker.get("host") or "") in current_host_names()


def make_parent_command(worker: dict[str, Any], path: str) -> str:
    code = (
        "from pathlib import Path; "
        f"Path({json.dumps(path)}).expanduser().parent.mkdir(parents=True, exist_ok=True)"
    )
    return shell_join([worker_python(worker), "-c", code])


def sync_inputs_to_worker(
    worker: dict[str, Any],
    manifest: dict[str, Any],
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
) -> int:
    local_inputs = input_requirement_items(local_manifest_for_worker(manifest, worker))
    remote_inputs = input_requirement_items(worker_manifest(manifest, worker))
    if len(local_inputs) != len(remote_inputs):
        raise RuntimeError("profile input mapping changed input count")

    synced = 0
    for local_item, remote_item in zip(local_inputs, remote_inputs, strict=True):
        src = expand_path(str(local_item.get("path") or ""))
        dst = str(remote_item.get("path") or "")
        if not src.exists():
            raise RuntimeError(f"input does not exist: {src}")
        if is_local_worker(worker):
            target = expand_path(dst)
            if src.resolve() == target.resolve():
                continue
            copy_to_worker(worker, src, dst, timeout=timeout, tracker=tracker)
            synced += 1
            continue
        if worker_is_current_host(worker) and src.resolve() == expand_path(dst).resolve():
            continue
        mkdir = run_worker_shell(worker, make_parent_command(worker, dst), timeout=timeout, tracker=tracker)
        if mkdir.returncode != 0:
            raise RuntimeError(mkdir.stderr.strip() or mkdir.stdout.strip() or f"failed to create parent for {dst}")
        copy_to_worker(worker, src, dst, timeout=timeout, tracker=tracker)
        synced += 1
    return synced


def manifest_has_git_requirements(manifest: dict[str, Any]) -> bool:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        return False
    return bool(iter_git_requirements(manifest, req))


def deploy_prereq_command(*, worker: dict[str, Any], manifest: dict[str, Any]) -> str:
    runner = worker_cache_file(worker, "crucible.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    python = worker_python(worker)
    commands = ["set -e"]
    if manifest_has_git_requirements(manifest):
        commands.append(shell_join([python, runner, "sync-repos", "--manifest", manifest_path, *profile_args]))
    commands.append(shell_join([python, runner, "doctor", "--manifest", manifest_path, *profile_args, "--role", "worker"]))
    return "\n".join(commands)


def deploy_work_command(*, worker: dict[str, Any], manifest: dict[str, Any], coordinator: str) -> str:
    runner = worker_cache_file(worker, "crucible.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    python = worker_python(worker)
    return shell_join([
        python, runner,
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


def worker_label(host: str, profile: str, slot: Any = None, slots: Any = None) -> str:
    label = host if profile == host else f"{host}/{profile}"
    try:
        slot_index = int(slot or 0)
        slot_count = int(slots or 0)
    except (TypeError, ValueError):
        return label
    if slot_index > 0 and slot_count > 1:
        return f"{label}#{slot_index}"
    return label


def deploy_setup_key(worker: dict[str, Any]) -> tuple[Any, ...]:
    return (
        worker.get("host"),
        worker.get("profile"),
        worker.get("local"),
        worker.get("cache_dir"),
        worker.get("python"),
    )


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


def default_run_name(template_path: Path, manifest: dict[str, Any]) -> str:
    name = str(manifest.get("name") or "").strip()
    if name:
        return validate_run_name(name)
    stem = template_path.name
    for suffix in (".manifest.json", ".json"):
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
            break
    return validate_run_name(stem)


def materialize_run(
    *,
    template_path: Path,
    workers_path: Path,
    run_name_arg: str | None,
    resume: bool,
    replace: bool,
) -> tuple[Path, Path, Path]:
    template = load_manifest(template_path)
    run_name = validate_run_name(run_name_arg) if run_name_arg else default_run_name(template_path, template)
    run_dir = run_dir_for_name(run_name)
    manifest_path = run_dir / RUN_MANIFEST_NAME
    workers_snapshot = run_dir / RUN_WORKERS_NAME

    if resume and replace:
        raise SystemExit("--resume and --replace are mutually exclusive")
    if resume:
        if not manifest_path.exists() or not workers_snapshot.exists():
            raise SystemExit(f"{run_name}: cannot resume; run manifest or workers file is missing")
        return run_dir, manifest_path, workers_snapshot
    if run_dir.exists():
        if not replace:
            raise SystemExit(f"{run_name}: run already exists; pass --resume or --replace")
        shutil.rmtree(run_dir)

    run_dir.mkdir(parents=True)
    materialized = materialize_manifest(template, run_name=run_name, run_dir=run_dir)
    state_dir(materialized).mkdir(parents=True)
    log_dir(materialized).mkdir(parents=True)
    claim_dir(materialized).mkdir(parents=True)
    (run_dir / "outputs").mkdir(parents=True)
    write_json(manifest_path, materialized)
    shutil.copy2(workers_path, workers_snapshot)
    return run_dir, manifest_path, workers_snapshot


def cmd_deploy(args: argparse.Namespace) -> int:
    workers_arg = expand_path(args.workers)
    template_path = expand_path(args.manifest)
    run_dir, manifest_path, workers_path = materialize_run(
        template_path=template_path,
        workers_path=workers_arg,
        run_name_arg=getattr(args, "run", None),
        resume=bool(getattr(args, "resume", False)),
        replace=bool(getattr(args, "replace", False)),
    )
    workers = load_workers(workers_path)
    jobs = max(1, int(args.jobs))
    remote_timeout = max(1.0, float(args.remote_timeout_seconds))
    manifest = load_manifest(manifest_path)
    has_tasks = bool(manifest.get("tasks"))
    color_enabled = not bool(args.no_color) and sys.stdout.isatty()
    print_lock = threading.Lock()
    tracker = ProcessTracker()
    setup_lock = threading.Condition()
    setup_results: dict[tuple[Any, ...], subprocess.CompletedProcess[str] | None] = {}

    def status(message: str, *, error: bool = False) -> None:
        with print_lock:
            print(message, file=sys.stderr if error else sys.stdout, flush=True)

    def verbose_status(message: str, *, error: bool = False) -> None:
        if args.verbose:
            status(message, error=error)

    def run_worker_setup(worker: dict[str, Any]) -> subprocess.CompletedProcess[str]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile)
        try:
            verbose_status(f"{label}: syncing runner and manifest")
            bootstrap_worker_runner(worker, manifest_path, timeout=remote_timeout, tracker=tracker)
            synced_inputs = sync_inputs_to_worker(worker, manifest, timeout=remote_timeout, tracker=tracker)
            if synced_inputs:
                verbose_status(f"{label}: synced {synced_inputs} input file(s)")
            verbose_status(f"{label}: checking prerequisites")
            script = deploy_prereq_command(worker=worker, manifest=manifest)
            return run_worker_shell(worker, script, timeout=remote_timeout, tracker=tracker)
        except Exception as exc:
            return subprocess.CompletedProcess(["deploy", host], 1, stdout="", stderr=str(exc))

    def ensure_worker_setup(worker: dict[str, Any]) -> subprocess.CompletedProcess[str]:
        key = deploy_setup_key(worker)
        with setup_lock:
            if key not in setup_results:
                setup_results[key] = None
                owner = True
            else:
                owner = False
                while setup_results[key] is None:
                    setup_lock.wait()
                return setup_results[key]

        result = run_worker_setup(worker)
        with setup_lock:
            setup_results[key] = result
            setup_lock.notify_all()
        return result

    def run_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))

        def result_payload(stage: str, result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
            return {
                "index": index,
                "host": host,
                "profile": profile,
                "slot": worker.get("_slot"),
                "slots": worker.get("_slots"),
                "stage": stage,
                "ok": result.returncode == 0,
                "rc": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        prereq = ensure_worker_setup(worker)
        if prereq.returncode != 0:
            verbose_status(f"{label}: prerequisites FAILED rc={prereq.returncode}", error=True)
            return result_payload("setup", prereq)

        verbose_status(f"{label}: prerequisites OK")
        if not has_tasks:
            return result_payload("setup", prereq)

        verbose_status(f"{label}: running tasks")
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

    status(colorize(f"deploy: run={run_dir.name} workers={len(workers)} jobs={jobs}", "cyan", enabled=color_enabled))
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
        status(
            f"  {marker} "
            f"{worker_label(str(item['host']), str(item['profile']), item.get('slot'), item.get('slots'))}: "
            f"{stage}{suffix}"
        )
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
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="store_true", help="Print crucible version and exit.")
    parser.add_argument("--version-json", action="store_true", help="Print crucible version as JSON and exit.")
    sub = parser.add_subparsers(dest="command")

    plan = sub.add_parser(
        "plan",
        help="Create a numbered task manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    plan.add_argument("--name", required=True, help="Run name stored in the manifest.")
    plan.add_argument(
        "--tasks", "--shards",
        dest="shards",
        metavar="N",
        type=int,
        required=True,
        help="Number of numbered tasks to create.",
    )
    plan.add_argument("--work-dir", required=True, help="Working directory for task commands.")
    plan.add_argument("--out", required=True, help="Manifest path to write.")
    plan.add_argument(
        "--command-template",
        required=True,
        help="Shell command template. Supports {index}, {shard}, {task_id}, {output}, and --var keys.",
    )
    plan.add_argument(
        "--output-template",
        action="append",
        required=True,
        help="Output path template for each task. Repeat for multiple outputs.",
    )
    plan.add_argument("--var", action="append", default=[], help="Template variable KEY=VALUE. Repeatable.")
    plan.add_argument(
        "--path-map",
        action="append",
        default=[],
        help="Host path mapping, PROFILE:/source=/dest. Repeatable.",
    )
    plan.add_argument("--task-prefix", default="task", help="Prefix for generated task ids.")
    plan.add_argument("--state-dir", help="Directory for .done/.fail/.running state files.")
    plan.add_argument("--log-dir", help="Directory for task logs.")
    plan.add_argument("--python-min", default="3.11", help="Minimum Python version required on workers.")
    plan.add_argument("--git-min", default="2.38", help="Minimum Git version required on workers.")
    plan.add_argument("--git-repo", help="Git checkout to pin; defaults to --work-dir.")
    plan.add_argument("--git-commit", help="Required Git commit; defaults to current HEAD.")
    plan.add_argument("--require-clean-git", action="store_true", help="Require a clean pinned Git checkout.")
    plan.add_argument("--progress-unit", help="Unit name for declared task progress, for example rows or games.")
    plan.add_argument("--progress-total", help="Per-task total for declared progress. Supports templates.")
    plan.add_argument(
        "--progress-total-lines",
        help="Count lines in this file and split the total across generated tasks.",
    )
    plan.add_argument(
        "--progress-log-regex",
        help="Regex read from task logs. Use named group (?P<done>...), optional total and output.",
    )
    plan.add_argument("--progress-output-unit", help="Label for optional (?P<output>...) progress value.")
    plan.set_defaults(func=cmd_plan)

    add_input = sub.add_parser(
        "add-input",
        help="Record input file checksums.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_input.add_argument("--manifest", required=True, help="Manifest to update.")
    add_input.add_argument("--path", action="append", required=True, help="Input file to checksum. Repeatable.")
    add_input.set_defaults(func=cmd_add_input)

    add_repo = sub.add_parser(
        "add-repo",
        help="Record a git checkout requirement.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_repo.add_argument("--manifest", required=True, help="Manifest to update.")
    add_repo.add_argument("--path", required=True, help="Worker-local checkout path.")
    add_repo.add_argument("--name", help="Human-readable repository label.")
    add_repo.add_argument("--commit", help="Required commit, short or full SHA.")
    add_repo.add_argument("--branch", help="Branch to checkout before optional commit pinning.")
    add_repo.add_argument("--remote", help="Remote to fetch/pull before checkout.")
    add_repo.add_argument("--require-clean", action="store_true", help="Require a clean worktree.")
    add_repo.set_defaults(func=cmd_add_repo)

    sync_repos = sub.add_parser(
        "sync-repos",
        help="Fetch and checkout declared git requirements.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sync_repos.add_argument("--manifest", required=True, help="Manifest containing repo requirements.")
    sync_repos.add_argument("--profile", help="Path-map/profile name for this worker.")
    sync_repos.set_defaults(func=cmd_sync_repos)

    run = sub.add_parser(
        "run",
        help="Run one task by id or index.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run.add_argument("--manifest", required=True, help="Manifest containing the task.")
    run.add_argument("--task", help="Task id to run.")
    run.add_argument("--index", type=int, help="Task index to run.")
    run.add_argument("--profile", help="Path-map/profile name for this worker.")
    run.add_argument("--force", action="store_true", help="Re-run even if the task is complete.")
    run.set_defaults(func=cmd_run)

    claim = sub.add_parser(
        "claim",
        help="Atomically claim the next coordinator task.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    claim.add_argument("--manifest", required=True, help="Coordinator manifest.")
    claim.add_argument("--worker", required=True, help="Worker name for the claim.")
    claim.add_argument("--profile", help="Profile name recorded with the claim.")
    claim.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    claim.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    claim.add_argument("--json", action="store_true", help="Print claim payload as JSON.")
    claim.set_defaults(func=cmd_claim)

    release = sub.add_parser(
        "release",
        help="Release a coordinator task claim.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    release.add_argument("--manifest", required=True, help="Coordinator manifest.")
    release.add_argument("--task", required=True, help="Task id whose claim should be released.")
    release.add_argument("--worker", help="Expected worker owner.")
    release.add_argument("--claim-id", help="Expected claim id.")
    release.set_defaults(func=cmd_release)

    heartbeat = sub.add_parser(
        "heartbeat",
        help="Refresh a coordinator task claim heartbeat.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    heartbeat.add_argument("--manifest", required=True, help="Coordinator manifest.")
    heartbeat.add_argument("--task", required=True, help="Task id to refresh.")
    heartbeat.add_argument("--worker", required=True, help="Expected worker owner.")
    heartbeat.add_argument("--claim-id", help="Expected claim id.")
    heartbeat.add_argument("--progress-json", help="Optional JSON progress object to store with the heartbeat.")
    heartbeat.set_defaults(func=cmd_heartbeat)

    work = sub.add_parser(
        "work",
        help="Claim, run, sync, and repeat worker tasks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    work.add_argument("--manifest", required=True, help="Worker-local manifest copy.")
    work.add_argument(
        "--coordinator",
        required=True,
        help="Coordinator manifest path, either /path/manifest.json or host:/path/manifest.json.",
    )
    work.add_argument("--profile", help="Path-map/profile name for this worker.")
    work.add_argument("--worker", default=socket.gethostname(), help="Worker name recorded in claims.")
    work.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    work.add_argument("--max-tasks", type=int, default=0, help="Maximum tasks to run; 0 means until no work remains.")
    work.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    work.add_argument("--force", action="store_true", help="Re-run tasks even if local state says complete.")
    work.add_argument("--fail-fast", action="store_true", help="Stop after the first task failure.")
    work.set_defaults(func=cmd_work)

    doctor = sub.add_parser(
        "doctor",
        help="Check host prerequisites for a manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    doctor.add_argument("--manifest", required=True, help="Manifest to validate.")
    doctor.add_argument("--profile", help="Path-map/profile name for this host.")
    doctor.add_argument("--role", choices=["worker", "coordinator"], default="worker", help="Host role to validate.")
    doctor.add_argument("--notify-command", help="Coordinator notification command to check.")
    doctor.set_defaults(func=cmd_doctor)

    status = sub.add_parser(
        "status",
        help="Show run or task status.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    status.add_argument(
        "run",
        nargs="?",
        help="Run name or run directory. Omit to summarize all runs.",
    )
    status.add_argument("-v", "--verbose", action="store_true", help="Show one row per task.")
    status.add_argument("--show-done", action="store_true", help="Show completed shard breakdown.")
    status.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    status.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    status.add_argument(
        "--lease-seconds",
        type=int,
        default=600,
        help="Seconds before a claimed task is considered stale. Default: 600.",
    )
    status.set_defaults(func=cmd_status)

    list_runs = sub.add_parser(
        "list",
        help="List Crucible runs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    list_runs.add_argument("--all", action="store_true", help="Show all runs, including completed runs.")
    list_runs.add_argument("--done", action="store_true", help="Show completed runs only.")
    list_runs.add_argument("--failed", action="store_true", help="Show runs with failed tasks only.")
    list_runs.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    list_runs.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    list_runs.add_argument(
        "--lease-seconds",
        type=int,
        default=600,
        help="Seconds before a claimed task is considered stale. Default: 600.",
    )
    list_runs.set_defaults(func=cmd_list)

    wait = sub.add_parser(
        "wait",
        help="Wait until coordinator tasks finish.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    wait.add_argument("--manifest", required=True, help="Coordinator manifest.")
    wait.add_argument("--profile", help="Path-map/profile name for this host.")
    wait.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    wait.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval.")
    wait.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall timeout; 0 means no timeout.")
    wait.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when any task fails.")
    wait.set_defaults(func=cmd_wait)

    verify = sub.add_parser(
        "verify",
        help="Verify all run tasks are complete.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    verify.add_argument("run", help="Run name or run directory.")
    verify.set_defaults(func=cmd_verify)

    merge = sub.add_parser(
        "merge",
        help="Concatenate first output from each task.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    merge.add_argument("run", help="Run name or run directory.")
    merge.add_argument("--output", required=True, help="Merged output path.")
    merge.add_argument("--force", action="store_true", help="Overwrite an existing merged output.")
    merge.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    merge.set_defaults(func=cmd_merge)

    stop = sub.add_parser(
        "stop",
        help="Request a run stop.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    stop.add_argument("run", nargs="?", help="Run name or run directory to stop.")
    stop.add_argument("--all", action="store_true", help="Request stop for all current runs.")
    stop.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    stop.set_defaults(func=cmd_stop)

    delete = sub.add_parser(
        "del",
        help="Delete run directories.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    delete.add_argument("run", nargs="?", help="Run name or run directory to delete.")
    delete.add_argument("--done", action="store_true", help="Delete all completed runs.")
    delete.add_argument("--force", action="store_true", help="Do not prompt before deleting.")
    delete.add_argument(
        "--lease-seconds",
        type=int,
        default=600,
        help="Seconds before a claimed task is considered stale. Default: 600.",
    )
    delete.set_defaults(func=cmd_del)

    self_update = sub.add_parser(
        "self-update",
        help="Fast-forward the crucible checkout itself.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    self_update.add_argument("--remote", default="origin", help="Git remote to pull from.")
    self_update.add_argument("--branch", help="Branch to update; defaults to current branch.")
    self_update.add_argument("--require-clean", action=argparse.BooleanOptionalAction, default=True, help="Require clean worktree before updating.")
    self_update.add_argument("--json", action="store_true", help="Print version payload as JSON after updating.")
    self_update.set_defaults(func=cmd_self_update)

    version = sub.add_parser(
        "version",
        help="Print crucible version.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    version.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    version.set_defaults(func=cmd_version)

    deploy = sub.add_parser(
        "deploy",
        help="Verify workers and run manifest tasks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    deploy.add_argument("workers", help="workers.json file.")
    deploy.add_argument("manifest", help="Manifest template to materialize into runs/<name>.")
    deploy.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    deploy.add_argument("--run", help="Run directory name under runs/. Defaults to manifest name.")
    deploy.add_argument("--resume", action="store_true", help="Resume an existing materialized run.")
    deploy.add_argument("--replace", action="store_true", help="Delete and recreate an existing run.")
    deploy.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    deploy.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    deploy.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
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
