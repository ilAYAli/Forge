from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime
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


SCHEMA = "forge.task.v1"
RUNS_DIR_NAME = "runs"
RUN_MANIFEST_NAME = "manifest.json"
RUN_WORKERS_NAME = "workers.json"
RUNS_DIR_ENV = "FORGE_RUNS_DIR"
COORDINATOR_RUNS_ENV = "FORGE_COORDINATOR_RUNS"
COORDINATOR_RUNS_FILE = "coordinator"
COLOR_ENV = "FORGE_COLOR"
STATUS_BLUE_SGR = "38;5;004"
RUN_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SSH_CONNECT_TIMEOUT_SECONDS = 15
SSH_SERVER_ALIVE_INTERVAL_SECONDS = 30
SSH_SERVER_ALIVE_COUNT_MAX = 3
SSH_TRANSIENT_RETRY_COUNT = 3
SSH_TRANSIENT_RETRY_DELAY_SECONDS = 1.0
SSH_TRANSIENT_RETRY_PATTERNS = (
    "connection timed out during banner exchange",
    "banner exchange",
    "kex_exchange_identification:",
    "ssh_exchange_identification:",
    "connection closed by remote host",
    "connection reset by peer",
    "operation timed out",
)
REMOTE_PROCESS_STOP_GRACE_SECONDS = 2
DEFAULT_REQUIREMENT_TIMEOUT_SECONDS = 15.0
HEARTBEAT_MIN_INTERVAL_SECONDS = 5.0
HEARTBEAT_MAX_INTERVAL_SECONDS = 10.0
HEARTBEAT_DEFAULT_INTERVAL_SECONDS = 10.0
WORK_IDLE_POLL_SECONDS = 5.0
DEFAULT_DEPLOY_WORK_IDLE_SECONDS = 120.0
LAUNCH_REQUEST_LEASE_SECONDS = 300
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ControlMaster=no",
    "-o", "ControlPath=none",
    "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT_SECONDS}",
    "-o", f"ServerAliveInterval={SSH_SERVER_ALIVE_INTERVAL_SECONDS}",
    "-o", f"ServerAliveCountMax={SSH_SERVER_ALIVE_COUNT_MAX}",
]
RSYNC_SSH = "ssh " + " ".join(shlex.quote(item) for item in SSH_OPTS)


def schema_matches(value: Any, kind: str, version: str = "v1") -> bool:
    parts = str(value or "").split(".")
    return len(parts) >= 3 and parts[-2:] == [kind, version]


def transient_ssh_transport_failure(command: list[str], result: subprocess.CompletedProcess[str]) -> bool:
    if not command or command[0] not in ("ssh", "rsync"):
        return False
    if result.returncode == 0:
        return False
    if result.returncode == 255:
        return True
    output = "\n".join(
        part for part in (result.stdout or "", result.stderr or "") if part
    ).lower()
    return any(pattern in output for pattern in SSH_TRANSIENT_RETRY_PATTERNS)


def run_process_command(
    command: list[str],
    *,
    timeout: float | None = None,
    tracker: "ProcessTracker | None" = None,
) -> subprocess.CompletedProcess[str]:
    if tracker is not None:
        return tracker.run(command, timeout=timeout)
    return subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
    )


def run_process_command_with_retries(
    command: list[str],
    *,
    timeout: float | None = None,
    tracker: "ProcessTracker | None" = None,
    retries: int = 0,
) -> subprocess.CompletedProcess[str]:
    attempts = max(1, retries + 1)
    result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(attempts):
        result = run_process_command(command, timeout=timeout, tracker=tracker)
        if not transient_ssh_transport_failure(command, result) or attempt + 1 >= attempts:
            return result
        delay = SSH_TRANSIENT_RETRY_DELAY_SECONDS * (attempt + 1)
        if delay > 0:
            time.sleep(delay)
    assert result is not None
    return result


def script_path() -> Path:
    path = Path(__file__).resolve()
    if path.name == "forge.py":
        return path
    wrapper = path.parent.parent / "forge.py"
    return wrapper if wrapper.exists() else path

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

_expand_path_cache: dict[tuple[str, str], Path] = {}

def expand_path(value: str | Path) -> Path:
    raw = str(value)
    cwd = os.getcwd()
    key = (raw, cwd)
    cached = _expand_path_cache.get(key)
    if cached is not None:
        return cached
    path = Path(os.path.expandvars(raw)).expanduser()
    if not path.is_absolute():
        path = Path(cwd) / path
    _expand_path_cache[key] = path
    return path

def runs_dir() -> Path:
    override = os.environ.get(RUNS_DIR_ENV)
    if override:
        return expand_path(override)
    return script_repo() / RUNS_DIR_NAME

def configured_coordinator_runs() -> str | None:
    if COORDINATOR_RUNS_ENV in os.environ:
        value = os.environ[COORDINATOR_RUNS_ENV].strip()
        if value:
            return value
        if RUNS_DIR_ENV in os.environ:
            return None
    try:
        path = runs_dir() / COORDINATOR_RUNS_FILE
    except SystemExit:
        return None
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None

def validate_run_name(name: str) -> str:
    run_name = name.strip()
    if not RUN_NAME_RE.fullmatch(run_name):
        raise SystemExit(
            f"{name!r}: invalid run name; use letters, numbers, dot, underscore, or dash"
        )
    return run_name

def clean_manifest_text(value: str | None, *, label: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if any(char in text for char in "\r\n\t"):
        raise SystemExit(f"{label} must be single-line text")
    return text

def manifest_metadata(manifest: dict[str, Any]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    kind = clean_manifest_text(manifest.get("kind"), label="kind")
    description = clean_manifest_text(manifest.get("description"), label="description")
    comment = clean_manifest_text(manifest.get("comment"), label="comment")
    if kind:
        metadata["kind"] = kind
    if description:
        metadata["description"] = description
    if comment:
        metadata["comment"] = comment
    return metadata

def task_display_label(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    explicit = clean_manifest_text(task.get("label"), label=f"{task.get('id', 'task')}.label")
    if explicit:
        return explicit
    manifest_kind = clean_manifest_text(manifest.get("kind"), label="kind")
    if manifest_kind:
        return manifest_kind
    return infer_task_label(task)

def infer_task_label(task: dict[str, Any]) -> str:
    unlabeled_commands = {
        "bash",
        "cat",
        "echo",
        "env",
        "false",
        "printf",
        "sh",
        "timeout",
        "true",
        "zsh",
    }
    command = task.get("command")
    if not isinstance(command, str) or not command.strip():
        return ""
    try:
        parts = shlex.split(command)
    except ValueError:
        return ""
    if not parts:
        return ""

    index = 0
    first = Path(parts[0]).name
    is_python = first.startswith("python")
    if is_python and len(parts) > 1:
        index = 1

    name = Path(parts[index]).name
    if not is_python and name in unlabeled_commands:
        return ""
    if name.endswith(".py"):
        name = name[:-3]
    elif is_python:
        return ""
    if not name:
        return ""

    if index + 1 < len(parts) and not parts[index + 1].startswith("-"):
        return f"{name}:{parts[index + 1]}"
    return name

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

def task_execution_checksum(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    """Hash inputs that define a task's execution, excluding mutable routing."""
    execution_task = dict(task)
    execution_task.pop("claim_worker", None)
    execution = {
        "schema": manifest.get("schema"),
        "kind": manifest.get("kind"),
        "work_dir": manifest.get("work_dir"),
        "vars": manifest.get("vars"),
        "requirements": manifest.get("requirements"),
        "task": execution_task,
    }
    payload = json.dumps(execution, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass

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

def git_output_or_empty(repo: Path, *args: str) -> str:
    try:
        return git_output(repo, *args)
    except subprocess.CalledProcessError:
        return ""

def git_requirement_record(repo: Path, commit: str, *, require_clean: bool) -> dict[str, Any]:
    record: dict[str, Any] = {
        "repo": str(repo),
        "commit": commit,
        "require_clean": require_clean,
    }
    branch = git_output_or_empty(repo, "rev-parse", "--abbrev-ref", "HEAD")
    if branch and branch != "HEAD":
        record["branch"] = branch
        remote = git_output_or_empty(repo, "config", "--get", f"branch.{branch}.remote")
    else:
        remote = git_output_or_empty(repo, "remote").splitlines()[0:1]
        remote = remote[0] if remote else ""
    if remote:
        record["remote"] = remote
        url = git_output_or_empty(repo, "remote", "get-url", remote)
        if url:
            record["url"] = url
    return record

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

def forge_version_payload() -> dict[str, Any]:
    path = script_path()
    try:
        repo = script_repo(path)
        commit = git_output(repo, "rev-parse", "HEAD")
        branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
        dirty = bool(git_output(repo, "status", "--porcelain"))
    except SystemExit:
        return {
            "schema": SCHEMA,
            "tool": "forge",
            "path": str(path),
            "repo": "",
            "branch": "packaged",
            "commit": "unknown",
            "dirty": False,
            "remote": "",
            "upstream": "",
        }
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
        "tool": "forge",
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
    print(f"forge {payload['commit']} {payload['branch']}{suffix}")

def cmd_self_update(args: argparse.Namespace) -> int:
    repo = script_repo()
    if args.require_clean and git_output(repo, "status", "--porcelain"):
        raise SystemExit(f"{repo}: git worktree is not clean")
    current_branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
    branch = args.branch or current_branch
    if branch == "HEAD":
        raise SystemExit("forge is on a detached HEAD; pass --branch explicitly")
    run_checked(["git", "-C", str(repo), "fetch", args.remote])
    if current_branch != branch:
        run_checked(["git", "-C", str(repo), "checkout", branch])
    run_checked(["git", "-C", str(repo), "pull", "--ff-only", args.remote, branch])
    print_version_payload(forge_version_payload(), as_json=args.json)
    return 0

def cmd_version(args: argparse.Namespace) -> int:
    print_version_payload(forge_version_payload(), as_json=args.json)
    return 0

def terminal_color_enabled(*, no_color: bool = False) -> bool:
    if no_color:
        return False
    mode = os.environ.get(COLOR_ENV, "").strip().lower()
    if mode in {"1", "always", "force", "on", "true", "yes"}:
        return True
    if mode in {"0", "false", "never", "no", "off"}:
        return False
    return sys.stdout.isatty() or running_under_watch()

def running_under_watch() -> bool:
    pid = os.getppid()
    for _ in range(3):
        if pid <= 1:
            return False
        try:
            output = subprocess.check_output(
                ["ps", "-o", "ppid=,comm=", "-p", str(pid)],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            return False
        if not output:
            return False
        fields = output.split(None, 1)
        if len(fields) < 2:
            return False
        try:
            pid = int(fields[0])
        except ValueError:
            return False
        if Path(fields[1]).name == "watch":
            return True
    return False

def colorize(text: str, color: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    colors = {
        "red": "31",
        "green": "32",
        "yellow": "33",
        "blue": STATUS_BLUE_SGR,
        "cyan": "36",
    }
    return f"\033[{colors[color]}m{text}\033[0m"
