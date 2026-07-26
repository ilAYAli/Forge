from __future__ import annotations

import contextlib
import fcntl

from collections.abc import Callable

from .config import config_workers_path, default_coordinator_host, default_notify_command
from .core import *
from .manifest import *
from .tasks import *
from .status import *
from .labeling import VERIFIED_MARKER_NAME, marker_matches


DEFAULT_INLINE_WORKER_SLOTS = 8
SSH_CONFIG_TIMEOUT_SECONDS = 5.0
DEPLOY_BOOTSTRAP_TIMEOUT_SECONDS = 60.0
DEPLOY_STARTUP_TIMEOUT_SECONDS = 20.0
DEPLOY_STARTUP_POLL_SECONDS = 0.5
QUEUE_LAUNCHER_POLL_SECONDS = 1.0
QUEUE_MONITOR_NAME = "queue-monitor.json"
SCHEDULER_LOCK_NAME = ".scheduler.lock"
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
_SSH_CONFIG_HOSTNAME_CACHE: dict[str, str | None] = {}
_QUEUE_LAUNCHER_CODE = r"""
import json
import sys

from forge_lib.workers import run_queue_launcher

raise SystemExit(run_queue_launcher(json.loads(sys.argv[1])))
"""
_INPUT_TREE_SYNC_LOCK = threading.Lock()
_SYNCED_INPUT_TREES: set[tuple[str, str, str]] = set()


@contextlib.contextmanager
def scheduler_lock():
    root = runs_dir()
    root.mkdir(parents=True, exist_ok=True)
    with (root / SCHEDULER_LOCK_NAME).open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def update_worker_label(worker: dict[str, Any]) -> str:
    return str(worker.get("name") or worker.get("host") or "local")

def update_worker_key(worker: dict[str, Any]) -> tuple[Any, ...]:
    if is_local_worker(worker):
        return ("local", str(worker.get("host") or "localhost"))
    return ("remote", str(worker.get("host") or ""))

def worker_host(worker: dict[str, Any]) -> str:
    return str(worker.get("host") or "").strip()

def assign_task_claim_workers(manifest: dict[str, Any], workers: list[dict[str, Any]]) -> int:
    claim_workers = [worker_host(worker) for worker in workers if worker_host(worker)]
    if not claim_workers:
        return 0
    changed = 0
    for index, task in enumerate(manifest.get("tasks") or []):
        if not isinstance(task, dict):
            continue
        claim_worker = claim_workers[index % len(claim_workers)]
        if task.get("claim_worker") != claim_worker:
            task["claim_worker"] = claim_worker
            changed += 1
    return changed

def update_workers_from_arg(value: str | None, *, default_ok: bool) -> list[dict[str, Any]]:
    workers = load_workers_arg(value, default_ok=default_ok)
    unique: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for worker in workers:
        key = update_worker_key(worker)
        if key in seen:
            continue
        seen.add(key)
        deduped = dict(worker)
        deduped.pop("_slot", None)
        deduped.pop("_slots", None)
        unique.append(deduped)
    return unique

def default_workers_path() -> Path:
    path = config_workers_path()
    if path.exists():
        return path
    raise SystemExit(
        f"no default workers file found; pass --workers or create {path}"
    )

def worker_arg_looks_like_path(value: str) -> bool:
    return (
        value.endswith(".json")
        or value.startswith(("/", "./", "../", "~"))
        or os.path.sep in value
    )

def parse_inline_worker_spec(value: str) -> tuple[str, str | None, int | None]:
    spec = value.strip()
    slots: int | None = None
    if ":" in spec:
        spec, slot_text = spec.rsplit(":", 1)
        try:
            slots = int(slot_text)
        except ValueError as exc:
            raise SystemExit(f"{value}: worker slots must be a non-negative integer") from exc
        if slots < 0:
            raise SystemExit(f"{value}: worker slots must be a non-negative integer")
    if "=" in spec:
        host, profile = spec.split("=", 1)
        profile = profile.strip()
    else:
        host, profile = spec, None
    host = host.strip()
    if not host:
        raise SystemExit("worker host must not be empty")
    if profile is not None and not profile:
        raise SystemExit(f"{host}: worker profile must not be empty")
    return host, profile, slots

def default_inline_worker_records(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    if path is None:
        try:
            path = default_workers_path()
        except SystemExit:
            return {}
    else:
        path = expand_path(str(path))
        if not path.exists():
            return {}
    payload = read_json(path)
    if payload.get("schema") is not None and not schema_matches(payload.get("schema"), "workers"):
        return {}
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        return {}
    records: dict[str, dict[str, Any]] = {}
    for item in raw_workers:
        try:
            worker = parse_worker_record(item)
        except SystemExit:
            continue
        if not worker:
            continue
        records[str(worker["host"])] = worker
    return records

def inline_worker_record(entry: str, defaults: dict[str, dict[str, Any]]) -> dict[str, Any]:
    host, profile, slots = parse_inline_worker_spec(entry)
    base = dict(defaults.get(host, {}))
    record = base if base else {
        "host": host,
        "profile": host,
        "local": host in ("localhost", "127.0.0.1"),
    }
    record["host"] = host
    if profile is not None:
        record["profile"] = profile
    elif not record.get("profile"):
        record["profile"] = host
    if "local" not in record:
        record["local"] = host in ("localhost", "127.0.0.1")
    if slots is None:
        slots = int(record.get("slots", DEFAULT_INLINE_WORKER_SLOTS if not base else 1))
    if slots == 0:
        return {}
    if slots == 1:
        record.pop("slots", None)
    else:
        record["slots"] = slots
    return record

def parse_inline_workers(value: str, *, defaults_path: str | Path | None = None) -> dict[str, Any]:
    entries = [entry.strip() for entry in value.split(",")]
    if not entries or any(not entry for entry in entries):
        raise SystemExit("inline workers must be a comma-separated host list")
    defaults = default_inline_worker_records(defaults_path)
    workers = [worker for entry in entries if (worker := inline_worker_record(entry, defaults))]
    return {
        "schema": "forge.workers.v1",
        "workers": workers,
    }

def workers_payload_from_arg(
    value: str | None,
    *,
    default_ok: bool,
    inline_defaults_path: str | Path | None = None,
) -> dict[str, Any]:
    if value is None:
        if not default_ok:
            raise SystemExit("workers are required")
        return read_json(default_workers_path())
    path = expand_path(value)
    if path.exists():
        return read_json(path)
    if worker_arg_looks_like_path(value):
        raise SystemExit(f"workers file not found: {value}")
    return parse_inline_workers(value, defaults_path=inline_defaults_path)

def load_workers_arg(
    value: str | None,
    *,
    default_ok: bool = False,
    inline_defaults_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    payload = workers_payload_from_arg(
        value,
        default_ok=default_ok,
        inline_defaults_path=inline_defaults_path,
    )
    return workers_from_payload(payload, label=value or str(default_workers_path()))

def worker_update_script(*, commit: str, remote: str, branch: str, require_clean: bool) -> str:
    clean_check = (
        "if [ -n \"$(git -C \"$repo\" status --porcelain)\" ]; then "
        "echo \"$repo: git worktree is not clean\" >&2; exit 1; fi; "
        if require_clean else ""
    )
    return (
        "set -e; "
        "bin=$(command -v forge); "
        "repo=$(python3 - \"$bin\" <<'PY'\n"
        "import pathlib, subprocess, sys\n"
        "path = pathlib.Path(sys.argv[1]).resolve()\n"
        "print(subprocess.check_output(['git', '-C', str(path.parent), "
        "'rev-parse', '--show-toplevel'], text=True).strip())\n"
        "PY\n"
        "); "
        f"{clean_check}"
        f"git -C \"$repo\" fetch {shlex.quote(remote)} {shlex.quote(branch)}; "
        f"if git -C \"$repo\" show-ref --verify --quiet refs/heads/{shlex.quote(branch)}; then "
        f"git -C \"$repo\" checkout {shlex.quote(branch)}; "
        f"git -C \"$repo\" merge --ff-only {shlex.quote(commit)}; "
        "else "
        f"git -C \"$repo\" checkout -B {shlex.quote(branch)} {shlex.quote(commit)}; "
        "fi; "
        f"test \"$(git -C \"$repo\" rev-parse HEAD)\" = {shlex.quote(commit)}; "
        "mkdir -p \"$HOME/.local/bin\"; "
        "ln -sf \"$repo/forge.py\" \"$HOME/.local/bin/forge\"; "
        "forge version"
    )

def cmd_update(args: argparse.Namespace) -> int:
    remote = str(args.remote)
    branch = str(args.branch or "main")
    timeout = max(0.0, float(args.timeout_seconds))
    workers = update_workers_from_arg(args.workers, default_ok=True)
    failed = 0

    if args.all:
        try:
            rc = cmd_self_update(
                argparse.Namespace(
                    remote=remote,
                    branch=branch,
                    require_clean=args.require_clean,
                    json=False,
                )
            )
        except SystemExit as exc:
            rc = int(exc.code) if isinstance(exc.code, int) else 1
        if rc == 0:
            print("coordinator: OK")
        else:
            print(f"coordinator: FAILED rc={rc}", file=sys.stderr)
            failed += 1
    commit = git_output(script_repo(), "rev-parse", "HEAD")
    print(f"target: {commit}")
    update_command = worker_update_script(
        commit=commit,
        remote=remote,
        branch=branch,
        require_clean=bool(args.require_clean),
    )

    for worker in workers:
        label = update_worker_label(worker)
        result = run_worker_shell(worker, update_command, timeout=timeout)
        if result.returncode == 0:
            summary = result.stdout.strip().splitlines()[-1:] or ["OK"]
            print(f"{label}: OK {summary[0]}")
            continue
        failed += 1
        detail = (result.stderr.strip() or result.stdout.strip()).splitlines()
        print(f"{label}: FAILED rc={result.returncode}", file=sys.stderr)
        if detail:
            print(f"  {detail[-1]}", file=sys.stderr)
    return 1 if failed else 0

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
    if item.get("path_map") is not None:
        path_map = item["path_map"]
        if not isinstance(path_map, dict):
            raise SystemExit(f"{host}: path_map must be an object")
        record["path_map"] = {str(src): str(dst) for src, dst in path_map.items()}
    return record

def parse_worker_slots(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise SystemExit(f"{label}: worker slots must be a non-negative integer")
    try:
        slots = int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{label}: worker slots must be a non-negative integer") from exc
    if slots < 0:
        raise SystemExit(f"{label}: worker slots must be a non-negative integer")
    return slots

def expand_worker_record(worker: dict[str, Any]) -> list[dict[str, Any]]:
    record = dict(worker)
    slots = int(record.pop("slots", 1))
    if slots == 0:
        return []
    if slots == 1:
        return [record]
    workers = []
    for slot in range(1, slots + 1):
        expanded = dict(record)
        expanded["_slot"] = slot
        expanded["_slots"] = slots
        workers.append(expanded)
    return workers

def load_workers(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    return workers_from_payload(payload, label=str(path))

def worker_records_from_payload(
    payload: dict[str, Any],
    *,
    label: str,
    allow_empty: bool = False,
) -> list[dict[str, Any]]:
    if payload.get("schema") is not None and not schema_matches(payload.get("schema"), "workers"):
        raise SystemExit(f"{label}: unsupported schema {payload.get('schema')!r}")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise SystemExit(f"{label}: workers must be a list")
    records: list[dict[str, Any]] = []
    for item in raw_workers:
        worker = parse_worker_record(item)
        if worker:
            records.append(worker)
    if not records and not allow_empty:
        raise SystemExit(f"{label}: no enabled workers")
    return records

def workers_payload_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": "forge.workers.v1",
        "workers": records,
    }

def workers_from_payload(payload: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    workers: list[dict[str, Any]] = []
    for worker in worker_records_from_payload(payload, label=label):
        workers.extend(expand_worker_record(worker))
    if not workers:
        raise SystemExit(f"{label}: no enabled workers")
    return workers

def worker_record_slots(worker: dict[str, Any]) -> int:
    return int(worker.get("slots", 1))

def set_worker_record_slots(worker: dict[str, Any], slots: int) -> dict[str, Any]:
    record = dict(worker)
    if slots <= 0:
        record["slots"] = 0
    elif slots == 1:
        record.pop("slots", None)
    else:
        record["slots"] = slots
    return record

def worker_set_record_from_arg(
    value: str,
    *,
    inline_defaults_path: str | Path | None = None,
) -> dict[str, Any]:
    if "," in value:
        raise SystemExit("worker set accepts a single worker spec")
    host, profile, slots = parse_inline_worker_spec(value)
    if slots is None:
        raise SystemExit(f"{value}: worker set requires an explicit slot count")
    # A run snapshot may predate metadata added to the global worker config
    # (for example an explicit Homebrew Python path).  Keep per-run values,
    # but fill fields missing from the snapshot from current defaults.
    defaults = default_inline_worker_records()
    for default_host, run_record in default_inline_worker_records(inline_defaults_path).items():
        merged = dict(defaults.get(default_host, {}))
        merged.update(run_record)
        defaults[default_host] = merged
    base = dict(defaults.get(host, {}))
    record = base if base else {
        "host": host,
        "profile": host,
        "local": host in ("localhost", "127.0.0.1"),
    }
    record["host"] = host
    if profile is not None:
        record["profile"] = profile
    elif not record.get("profile"):
        record["profile"] = host
    if "local" not in record:
        record["local"] = host in ("localhost", "127.0.0.1")
    return set_worker_record_slots(record, slots)

def worker_slots_for_host(payload: dict[str, Any], host: str) -> int:
    slots = 0
    for worker in worker_records_from_payload(payload, label="workers", allow_empty=True):
        if worker_host(worker) == host:
            slots = worker_record_slots(worker)
    return slots

def set_worker_payload_slots(base_payload: dict[str, Any], set_worker: dict[str, Any]) -> dict[str, Any]:
    host = worker_host(set_worker)
    desired_slots = worker_record_slots(set_worker)
    records: list[dict[str, Any]] = []
    replaced = False
    for worker in worker_records_from_payload(base_payload, label="base workers", allow_empty=True):
        if worker_host(worker) != host:
            records.append(worker)
            continue
        if desired_slots > 0 and not replaced:
            records.append(set_worker_record_slots(set_worker, desired_slots))
            replaced = True
    if desired_slots > 0 and not replaced:
        records.append(set_worker_record_slots(set_worker, desired_slots))
    return workers_payload_from_records(records)

def worker_inline_arg_for_slots(worker: dict[str, Any], slots: int) -> str:
    host = worker_host(worker)
    profile = str(worker.get("profile") or host).strip()
    prefix = host if profile == host else f"{host}={profile}"
    return f"{prefix}:{slots}"

def remove_worker_from_payload(payload: dict[str, Any], host: str) -> tuple[dict[str, Any], int]:
    removed = 0
    records: list[dict[str, Any]] = []
    for worker in worker_records_from_payload(payload, label="workers", allow_empty=True):
        if worker_host(worker) == host:
            removed += 1
            continue
        records.append(worker)
    return workers_payload_from_records(records), removed

def worker_hosts_from_payload(payload: dict[str, Any]) -> list[str]:
    hosts = {worker_host(worker) for worker in worker_records_from_payload(payload, label="workers", allow_empty=True)}
    return sorted(host for host in hosts if host)

def worker_cache_dir(worker: dict[str, Any]) -> str:
    if worker.get("cache_dir"):
        return str(worker["cache_dir"])
    if is_local_worker(worker):
        return str(Path.home() / ".cache/forge")
    return "/tmp/forge-cache"

def worker_cache_file(worker: dict[str, Any], name: str) -> str:
    return str(Path(worker_cache_dir(worker)) / name)

def is_local_worker(worker: dict[str, Any]) -> bool:
    return bool(worker.get("local", False))

def worker_python(worker: dict[str, Any]) -> str:
    return str(worker.get("python") or "python3")

def managed_remote_script(script: str) -> str:
    remote_path = "PATH=\"$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH\"; export PATH; "
    quoted = shlex.quote(remote_path + script)
    cleanup = "kill -TERM -$pid 2>/dev/null; sleep 1; kill -KILL -$pid 2>/dev/null"
    return (
        "if command -v setsid >/dev/null 2>&1; then "
        f"setsid sh -lc {quoted} & pid=$!; "
        f"trap '{cleanup}' INT TERM HUP; "
        "wait $pid; rc=$?; trap - INT TERM HUP; exit $rc; "
        f"else sh -lc {quoted}; fi"
    )

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
    tracker: ProcessTracker | None = None,
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
    tracker: ProcessTracker | None = None,
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

def run_worker_shell(
    worker: dict[str, Any],
    script: str,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
    retries: int = 0,
) -> subprocess.CompletedProcess[str]:
    command = ["sh", "-lc", script] if is_local_worker(worker) else ["ssh", *SSH_OPTS, str(worker["host"]), managed_remote_script(script)]
    return run_process_command_with_retries(command, timeout=timeout, tracker=tracker, retries=retries)

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
    retries: int = 0,
) -> None:
    if is_local_worker(worker):
        target = expand_path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        return
    command = ["rsync", "-a", "-e", RSYNC_SSH, str(src), worker_rsync_target(worker, dst)]
    result = run_process_command_with_retries(command, timeout=timeout, tracker=tracker, retries=retries)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"rsync failed for {src}")

def copy_dir_to_worker(
    worker: dict[str, Any],
    src: Path,
    dst: str,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
    retries: int = 0,
) -> None:
    if is_local_worker(worker):
        target = expand_path(dst)
        shutil.copytree(src, target, dirs_exist_ok=True)
        return
    mkdir = run_worker_shell(
        worker,
        "mkdir -p " + shlex.quote(dst),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    if mkdir.returncode != 0:
        raise RuntimeError(mkdir.stderr.strip() or mkdir.stdout.strip() or f"failed to create {dst}")
    command = [
        "rsync", "-a", "--delete", "-e", RSYNC_SSH,
        str(src) + "/",
        worker_rsync_target(worker, dst.rstrip("/") + "/"),
    ]
    result = run_process_command_with_retries(command, timeout=timeout, tracker=tracker, retries=retries)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"rsync failed for {src}")

def bootstrap_worker_runner(
    worker: dict[str, Any],
    manifest_path: Path,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
    retries: int = 0,
) -> None:
    if not is_local_worker(worker):
        cache = worker_cache_dir(worker)
        result = run_worker_shell(
            worker,
            "mkdir -p " + shlex.quote(cache),
            timeout=timeout,
            tracker=tracker,
            retries=retries,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "failed to create worker cache")
    wrapper = script_path()
    copy_to_worker(
        worker,
        wrapper,
        worker_cache_file(worker, "forge.py"),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    copy_dir_to_worker(
        worker,
        wrapper.parent / "forge_lib",
        worker_cache_file(worker, "forge_lib"),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    copy_to_worker(
        worker,
        manifest_path,
        worker_cache_file(worker, "manifest.json"),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )

def active_worker_profile(manifest: dict[str, Any], worker: dict[str, Any]) -> str | None:
    profile = str(worker.get("profile") or "").strip()
    if not profile:
        return None
    profiles = manifest_profile_names(manifest)
    if profile not in profiles:
        host = worker_host(worker)
        path_maps = manifest.get("path_maps", {})
        path_profiles = set(str(item) for item in path_maps) if isinstance(path_maps, dict) else set()
        if host in path_profiles:
            return host
        return None
    return profile

def worker_path_map(worker: dict[str, Any]) -> dict[str, str]:
    raw = worker.get("path_map")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SystemExit(f"{worker_host(worker)}: path_map must be an object")
    return {str(src): str(dst) for src, dst in raw.items()}

def worker_profile_name(manifest: dict[str, Any], worker: dict[str, Any]) -> str | None:
    profile = active_worker_profile(manifest, worker)
    if profile:
        return profile
    if worker_path_map(worker):
        profile = str(worker.get("profile") or worker_host(worker)).strip()
        return profile or None
    return None

def manifest_with_worker_path_map(manifest: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    path_map = worker_path_map(worker)
    if not path_map:
        return copy.deepcopy(manifest)
    profile = str(worker.get("profile") or worker_host(worker)).strip()
    if not profile:
        raise SystemExit(f"{worker_host(worker)}: worker profile must not be empty")
    local_manifest = copy.deepcopy(manifest)
    path_maps = local_manifest.setdefault("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    path_maps[profile] = path_map
    return local_manifest

def workers_payload_for_manifest(payload: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for worker in worker_records_from_payload(payload, label="workers", allow_empty=True):
        record = dict(worker)
        profile = active_worker_profile(manifest, record)
        if profile:
            record["profile"] = profile
        records.append(record)
    return workers_payload_from_records(records)

def worker_profile_args(manifest: dict[str, Any], worker: dict[str, Any]) -> list[str]:
    profile = worker_profile_name(manifest, worker)
    if not profile:
        return []
    return ["--profile", profile]

def worker_manifest(manifest: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    profile = active_worker_profile(manifest, worker)
    if profile:
        return apply_profile(manifest, profile)
    if worker_path_map(worker):
        profile = worker_profile_name(manifest, worker)
        return apply_profile(manifest_with_worker_path_map(manifest, worker), profile)
    return manifest

def local_manifest_for_worker(manifest: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    profile = worker_profile_name(manifest, worker)
    local_manifest = manifest_with_worker_path_map(manifest, worker)
    if profile:
        local_manifest["_active_profile"] = profile
    return local_manifest

def normalize_host_name(value: str) -> str:
    return value.strip().lower().rstrip(".")

def current_host_names() -> set[str]:
    names = {socket.gethostname(), socket.getfqdn(), "localhost", "127.0.0.1", "::1"}
    result = {normalize_host_name(name) for name in names if str(name).strip()}
    for name in list(result):
        if name in {"127.0.0.1", "::1"}:
            continue
        try:
            canonical, aliases, _ = socket.gethostbyname_ex(name)
        except OSError:
            continue
        result.add(normalize_host_name(canonical))
        result.update(normalize_host_name(alias) for alias in aliases)
    return result

def ssh_config_hostname(host: str) -> str | None:
    host = host.strip()
    if not host:
        return None
    if host in _SSH_CONFIG_HOSTNAME_CACHE:
        return _SSH_CONFIG_HOSTNAME_CACHE[host]
    try:
        result = subprocess.run(
            ["ssh", "-G", host],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=SSH_CONFIG_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        _SSH_CONFIG_HOSTNAME_CACHE[host] = None
        return None
    if result.returncode != 0:
        _SSH_CONFIG_HOSTNAME_CACHE[host] = None
        return None
    for line in result.stdout.splitlines():
        key, _, value = line.partition(" ")
        if key == "hostname" and value.strip():
            _SSH_CONFIG_HOSTNAME_CACHE[host] = value.strip()
            return value.strip()
    _SSH_CONFIG_HOSTNAME_CACHE[host] = None
    return None

def worker_targets_current_host(worker: dict[str, Any]) -> bool:
    if is_local_worker(worker):
        return True
    current = current_host_names()
    host = worker_host(worker)
    if normalize_host_name(host) in current:
        return True
    configured = ssh_config_hostname(host)
    return bool(configured and normalize_host_name(configured) in current)

def worker_is_current_host(worker: dict[str, Any]) -> bool:
    return worker_targets_current_host(worker)

def make_parent_command(worker: dict[str, Any], path: str) -> str:
    code = (
        "from pathlib import Path; "
        f"Path({json.dumps(path)}).expanduser().parent.mkdir(parents=True, exist_ok=True)"
    )
    return shell_join([worker_python(worker), "-c", code])

def local_input_matches(path: Path, item: dict[str, Any]) -> bool:
    if not path.exists():
        return False
    expected_bytes = item.get("bytes")
    if expected_bytes is not None and path.stat().st_size != int(expected_bytes):
        return False
    expected_sha = str(item.get("sha256") or "").strip()
    return not expected_sha or sha256_file(path) == expected_sha

def worker_input_matches(
    worker: dict[str, Any],
    path: str,
    item: dict[str, Any],
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
    retries: int = 0,
) -> bool:
    expected_bytes = item.get("bytes")
    expected_sha = str(item.get("sha256") or "").strip()
    if expected_bytes is None and not expected_sha:
        return False
    code = "\n".join([
        "from pathlib import Path",
        "import hashlib",
        "import sys",
        f"path = Path({json.dumps(path)}).expanduser()",
        "ok = path.exists()",
        f"expected_bytes = {json.dumps(expected_bytes)}",
        "ok = ok and (expected_bytes is None or path.stat().st_size == int(expected_bytes))",
        f"expected_sha = {json.dumps(expected_sha)}",
        "if ok and expected_sha:",
        "    h = hashlib.sha256()",
        "    with path.open('rb') as f:",
        "        for chunk in iter(lambda: f.read(1024 * 1024), b''):",
        "            h.update(chunk)",
        "    ok = h.hexdigest() == expected_sha",
        "sys.exit(0 if ok else 1)",
    ])
    result = run_worker_shell(
        worker,
        shell_join([worker_python(worker), "-c", code]),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    return result.returncode == 0

def sync_inputs_to_worker(
    worker: dict[str, Any],
    manifest: dict[str, Any],
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
    retries: int = 0,
) -> int:
    local_inputs = input_requirement_items(local_manifest_for_worker(manifest, worker))
    remote_inputs = input_requirement_items(worker_manifest(manifest, worker))
    if len(local_inputs) != len(remote_inputs):
        raise RuntimeError("profile input mapping changed input count")

    synced = 0
    for local_item, remote_item in zip(local_inputs, remote_inputs):
        src = expand_path(str(local_item.get("path") or ""))
        dst = str(remote_item.get("path") or "")
        if not src.exists():
            raise RuntimeError(f"input does not exist: {src}")
        if remote_item.get("tree") == "lc0-inventory":
            digest = str(remote_item.get("digest") or "")
            key = (worker_host(worker), dst, digest)
            with _INPUT_TREE_SYNC_LOCK:
                if key in _SYNCED_INPUT_TREES:
                    continue
                if sync_lc0_inventory_tree(
                    worker,
                    src,
                    dst,
                    digest,
                    timeout=timeout,
                    tracker=tracker,
                    retries=retries,
                ):
                    synced += 1
                _SYNCED_INPUT_TREES.add(key)
            continue
        if is_local_worker(worker):
            target = expand_path(dst)
            if src.resolve() == target.resolve() or local_input_matches(target, remote_item):
                continue
            copy_to_worker(worker, src, dst, timeout=timeout, tracker=tracker, retries=retries)
            synced += 1
            continue
        if worker_input_matches(worker, dst, remote_item, timeout=timeout, tracker=tracker, retries=retries):
            continue
        mkdir = run_worker_shell(
            worker,
            make_parent_command(worker, dst),
            timeout=timeout,
            tracker=tracker,
            retries=retries,
        )
        if mkdir.returncode != 0:
            raise RuntimeError(mkdir.stderr.strip() or mkdir.stdout.strip() or f"failed to create parent for {dst}")
        copy_to_worker(worker, src, dst, timeout=timeout, tracker=tracker, retries=retries)
        synced += 1
    return synced


def sync_lc0_inventory_tree(
    worker: dict[str, Any],
    src: Path,
    dst: str,
    digest: str,
    *,
    timeout: float | None,
    tracker: ProcessTracker | None,
    retries: int,
) -> bool:
    if not digest:
        raise RuntimeError("LC0 inventory input is missing digest")
    if is_local_worker(worker):
        target = expand_path(dst)
        if src.resolve() != target.resolve():
            copy_dir_to_worker(worker, src, dst, timeout=timeout, tracker=tracker, retries=retries)
        if not marker_matches(target, digest):
            from .labeling import verify_inventory_tree

            verify_inventory_tree(target, digest)
        return src.resolve() != target.resolve()

    marker_code = "\n".join([
        "from pathlib import Path",
        "import json, sys",
        "root = Path(sys.argv[1]).expanduser()",
        f"marker = root / {VERIFIED_MARKER_NAME!r}",
        "try:",
        "    data = json.loads(marker.read_text())",
        "except (OSError, json.JSONDecodeError):",
        "    sys.exit(1)",
        "sys.exit(0 if data.get('digest') == sys.argv[2] else 1)",
    ])
    if run_worker_shell(
        worker,
        shell_join([worker_python(worker), "-c", marker_code, dst, digest]),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    ).returncode == 0:
        return False

    mkdir = run_worker_shell(
        worker,
        shell_join([worker_python(worker), "-c", "from pathlib import Path; import sys; Path(sys.argv[1]).expanduser().mkdir(parents=True, exist_ok=True)", dst]),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    if mkdir.returncode != 0:
        raise RuntimeError(mkdir.stderr.strip() or f"failed to create {dst}")
    command = [
        "rsync", "-aL", "--delete", "--exclude", VERIFIED_MARKER_NAME,
        "-e", RSYNC_SSH, str(src) + "/", worker_rsync_target(worker, dst.rstrip("/") + "/"),
    ]
    result = run_process_command_with_retries(
        command,
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"rsync failed for {src}")

    verify_code = "\n".join([
        "from pathlib import Path",
        "import hashlib, json, os, sys, tempfile",
        "root = Path(sys.argv[1]).expanduser()",
        "digest = sys.argv[2]",
        "inventory_path = root / 'inventory.json'",
        "inventory = json.loads(inventory_path.read_text())",
        "canonical = json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()",
        "if hashlib.sha256(canonical).hexdigest() != digest: sys.exit(2)",
        "total = 0",
        "for item in inventory['files']:",
        "    path = root / item['path']",
        "    if path.stat().st_size != int(item['size']): sys.exit(3)",
        "    h = hashlib.sha256()",
        "    with path.open('rb') as handle:",
        "        for chunk in iter(lambda: handle.read(1024 * 1024), b''): h.update(chunk)",
        "    if h.hexdigest() != item['sha256']: sys.exit(4)",
        "    total += path.stat().st_size",
        "marker = {'schema': inventory['schema'], 'digest': digest, 'files': len(inventory['files']), 'bytes': total}",
        f"target = root / {VERIFIED_MARKER_NAME!r}",
        "tmp = target.with_name(target.name + '.tmp')",
        "tmp.write_text(json.dumps(marker, sort_keys=True) + '\\n')",
        "os.replace(tmp, target)",
    ])
    verified = run_worker_shell(
        worker,
        shell_join([worker_python(worker), "-c", verify_code, dst, digest]),
        timeout=timeout,
        tracker=tracker,
        retries=retries,
    )
    if verified.returncode != 0:
        raise RuntimeError(verified.stderr.strip() or f"LC0 cache verification failed rc={verified.returncode}")
    return True

def manifest_has_git_requirements(manifest: dict[str, Any]) -> bool:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        return False
    return bool(iter_git_requirements(manifest, req))

def deploy_prereq_command(
    *,
    worker: dict[str, Any],
    manifest: dict[str, Any],
    coordinator: str | None = None,
) -> str:
    runner = worker_cache_file(worker, "forge.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    python = worker_python(worker)
    commands = ["set -e"]
    if coordinator:
        commands.append(shell_join([
            python, runner, "doctor",
            "--manifest", manifest_path,
            *profile_args,
            "--role", "worker",
            "--skip-requirements",
            "--coordinator", coordinator,
        ]))
    if manifest_has_git_requirements(manifest):
        commands.append(shell_join([python, runner, "sync-repos", "--manifest", manifest_path, *profile_args]))
    doctor_command = [python, runner, "doctor", "--manifest", manifest_path, *profile_args, "--role", "worker"]
    commands.append(shell_join(doctor_command))
    return "\n".join(commands)

def deploy_work_command(
    *,
    worker: dict[str, Any],
    manifest: dict[str, Any],
    coordinator: str,
    retry_failed: bool = False,
    task_attempts: int = 0,
    task_complete_command: str | None = None,
    task_complete_sync: bool = False,
) -> str:
    runner = worker_cache_file(worker, "forge.py")
    manifest_path = worker_cache_file(worker, "manifest.json")
    profile_args = worker_profile_args(manifest, worker)
    python = worker_python(worker)
    command = [
        python, runner,
        "work",
        "--manifest", manifest_path,
        "--coordinator", coordinator,
        "--worker", str(worker["host"]),
        "--idle-seconds", str(DEFAULT_DEPLOY_WORK_IDLE_SECONDS),
        *profile_args,
    ]
    if retry_failed:
        command.append("--retry-failed")
    if task_attempts > 0:
        command.extend(["--task-attempts", str(task_attempts)])
    if task_complete_command:
        command.extend(["--task-complete-command", task_complete_command])
    if task_complete_sync:
        command.append("--task-complete-sync")
    return shell_join(command)

def launch_file_stem(run_name: str, index: int, worker: dict[str, Any]) -> str:
    label = worker_label(
        str(worker["host"]),
        str(worker["profile"]),
        worker.get("_slot"),
        worker.get("_slots"),
    )
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{index}-{label}")
    return f"{run_name}-{safe}"

def worker_launch_files(run_name: str, index: int, worker: dict[str, Any]) -> tuple[str, str]:
    stem = launch_file_stem(run_name, index, worker)
    return (
        worker_cache_file(worker, f"{stem}.pid"),
        worker_cache_file(worker, f"{stem}.launch.log"),
    )

def detached_shell_command(worker: dict[str, Any], script: str, *, pid_file: str, log_file: str) -> str:
    launcher = (
        "import os, subprocess, sys\n"
        "script, pid_path, log_path = sys.argv[1:4]\n"
        "os.makedirs(os.path.dirname(pid_path) or '.', exist_ok=True)\n"
        "os.makedirs(os.path.dirname(log_path) or '.', exist_ok=True)\n"
        "log = open(log_path, 'ab', buffering=0)\n"
        "process = subprocess.Popen(\n"
        "    ['sh', '-lc', script],\n"
        "    stdin=subprocess.DEVNULL,\n"
        "    stdout=log,\n"
        "    stderr=subprocess.STDOUT,\n"
        "    start_new_session=True,\n"
        ")\n"
        "with open(pid_path, 'w', encoding='utf-8') as handle:\n"
        "    handle.write(str(process.pid) + '\\n')\n"
    )
    return shell_join([worker_python(worker), "-c", launcher, script, pid_file, log_file])

def cleanup_run_pid_files_command(cache_dir: str, run_name: str) -> str:
    quoted_cache = shlex.quote(cache_dir)
    quoted_run = shlex.quote(run_name)
    return (
        f"cache_dir={quoted_cache}; found=0; "
        f"for pid_file in \"$cache_dir\"/{quoted_run}-*.pid; do "
        "test -e \"$pid_file\" || continue; "
        "pid=$(sed -n '1p' \"$pid_file\" 2>/dev/null || true); "
        "case \"$pid\" in ''|*[!0-9]*) rm -f \"$pid_file\"; continue;; esac; "
        "found=1; "
        "printf '%s\\n' \"$pid_file\"; "
        "kill -TERM -\"$pid\" 2>/dev/null || kill -TERM \"$pid\" 2>/dev/null || true; "
        "done; "
        "remaining=200; while test \"$found\" -ne 0 && test \"$remaining\" -gt 0; do "
        "alive=0; "
        f"for pid_file in \"$cache_dir\"/{quoted_run}-*.pid; do "
        "test -e \"$pid_file\" || continue; "
        "pid=$(sed -n '1p' \"$pid_file\" 2>/dev/null || true); "
        "case \"$pid\" in ''|*[!0-9]*) continue;; esac; "
        "if kill -0 -\"$pid\" 2>/dev/null || kill -0 \"$pid\" 2>/dev/null; then alive=1; fi; "
        "done; "
        "test \"$alive\" -ne 0 || break; "
        "remaining=$((remaining - 1)); sleep 0.1; "
        "done; "
        f"for pid_file in \"$cache_dir\"/{quoted_run}-*.pid; do "
        "test -e \"$pid_file\" || continue; "
        "pid=$(sed -n '1p' \"$pid_file\" 2>/dev/null || true); "
        "case \"$pid\" in ''|*[!0-9]*) rm -f \"$pid_file\"; continue;; esac; "
        "kill -KILL -\"$pid\" 2>/dev/null || kill -KILL \"$pid\" 2>/dev/null || true; "
        "rm -f \"$pid_file\"; "
        "done"
    )

def kill_run_workers(run_dir: Path, *, timeout: float = 30.0) -> int:
    workers_path = run_dir / RUN_WORKERS_NAME
    if not workers_path.exists():
        try:
            workers_path = default_workers_path()
        except SystemExit:
            return 0
    workers = load_workers(workers_path)
    run_name = run_dir.name
    targets: dict[tuple[bool, str], list[str]] = {}
    seen: set[tuple[bool, str, str]] = set()
    for worker in workers:
        local = is_local_worker(worker)
        host = str(worker["host"])
        cache_dir = worker_cache_dir(worker)
        key = (local, host, cache_dir)
        if key in seen:
            continue
        seen.add(key)
        targets.setdefault((local, host), []).append(cleanup_run_pid_files_command(cache_dir, run_name))

    def kill_target(target: tuple[tuple[bool, str], list[str]]) -> int:
        (local, host), scripts = target
        script = "; ".join(scripts)
        command = (
            ["sh", "-lc", script]
            if local
            else ["ssh", *SSH_OPTS, host, managed_remote_script(script)]
        )
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return len([line for line in result.stdout.splitlines() if line.strip()])
        except (subprocess.TimeoutExpired, OSError):
            pass
        return 0

    if not targets:
        return 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as executor:
        return sum(executor.map(kill_target, targets.items()))


def start_wait_monitor(
    *,
    run_dir: Path,
    manifest_path: Path,
    args: argparse.Namespace,
    notify_command: str | None,
) -> int:
    monitor_path = run_dir / "wait-monitor.json"
    if monitor_path.is_file():
        try:
            existing = read_json(monitor_path)
            pid = int(existing.get("pid") or 0)
            if pid > 0:
                os.kill(pid, 0)
                return pid
        except (OSError, SystemExit, TypeError, ValueError):
            pass
    command = [
        sys.executable,
        str(script_path()),
        "wait",
        "--manifest", str(manifest_path),
        "--lease-seconds", str(getattr(args, "lease_seconds", 600)),
        "--interval-seconds", str(getattr(args, "interval_seconds", 30.0)),
        "--timeout-seconds", str(getattr(args, "timeout_seconds", 0.0)),
    ]
    if notify_command:
        command.extend(["--notify-command", notify_command])
    if not bool(getattr(args, "fail_on_fail", True)):
        command.append("--no-fail-on-fail")
    log = (run_dir / "wait.log").open("ab", buffering=0)
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    write_json(run_dir / "wait-monitor.json", {
        "schema": SCHEMA,
        "pid": process.pid,
        "command": command,
        "log": str(run_dir / "wait.log"),
        "started_at": now(),
        "started_epoch": epoch(),
    })
    return process.pid

def remember_run_for_web(run_name: str, *, coordinator_runs: str | None = None) -> None:
    try:
        from .web import remember_web_run

        remember_web_run(run_name, coordinator_runs=coordinator_runs)
    except ModuleNotFoundError as exc:
        if exc.name != "forge_lib.web":
            raise

def queued_run_sort_key(summary: dict[str, Any]) -> tuple[float, str]:
    queue = summary.get("queue")
    if not isinstance(queue, dict):
        return (0.0, str(summary.get("run") or ""))
    try:
        requested_epoch = float(queue.get("requested_epoch") or 0.0)
    except (TypeError, ValueError):
        requested_epoch = 0.0
    return (requested_epoch, str(summary.get("run") or ""))


def run_blocks_queue(summary: dict[str, Any]) -> bool:
    if run_done(summary):
        return False
    return run_active_count(summary) > 0 or run_current(summary)


def queue_blockers_for_run(run_name: str, summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    target = next((summary for summary in summaries if str(summary.get("run")) == run_name), None)
    target_key = queued_run_sort_key(target or {"run": run_name})
    target_has_turn = bool(target and (run_blocks_queue(target) or run_queue_requested(target)))
    blockers = []
    for summary in summaries:
        name = str(summary.get("run") or "")
        if name == run_name:
            continue
        if run_blocks_queue(summary):
            blockers.append(summary)
            continue
        if run_queue_requested(summary) and (
            not target_has_turn
            or (run_queue_requested(target or {}) and queued_run_sort_key(summary) < target_key)
        ):
            blockers.append(summary)
    return blockers

def active_or_queued_blockers(run_name: str, *, lease_seconds: int) -> list[dict[str, Any]]:
    return queue_blockers_for_run(
        run_name,
        load_run_summaries(lease_seconds=lease_seconds),
    )

def queue_resume_command(args: argparse.Namespace, run_name: str) -> list[str]:
    command_name = "retry" if bool(getattr(args, "retry_failed", False)) else "resume"
    command = [sys.executable, str(script_path()), command_name, run_name]
    if int(getattr(args, "jobs", 4)) != 4:
        command.extend(["--jobs", str(args.jobs)])
    if int(getattr(args, "task_attempts", 2)) != 2:
        command.extend(["--task-attempts", str(args.task_attempts)])
    if getattr(args, "coordinator_host", None):
        command.extend(["--coordinator-host", str(args.coordinator_host)])
    if float(getattr(args, "remote_timeout_seconds", 1800.0)) != 1800.0:
        command.extend(["--remote-timeout-seconds", str(args.remote_timeout_seconds)])
    if bool(getattr(args, "wait", False)):
        command.append("--wait")
    command.extend(["--lease-seconds", str(getattr(args, "lease_seconds", 600))])
    command.extend(["--interval-seconds", str(getattr(args, "interval_seconds", 30.0))])
    command.extend(["--timeout-seconds", str(getattr(args, "timeout_seconds", 0.0))])
    if getattr(args, "verify", None) is True:
        command.append("--verify")
    elif getattr(args, "verify", None) is False:
        command.append("--no-verify")
    if getattr(args, "merge", None):
        command.extend(["--merge", str(args.merge)])
    if bool(getattr(args, "merge_force", False)):
        command.append("--merge-force")
    if bool(getattr(args, "newline_between", False)):
        command.append("--newline-between")
    if getattr(args, "notify_command", None):
        command.extend(["--notify-command", str(args.notify_command)])
    if getattr(args, "task_complete_command", None):
        command.extend(["--task-complete-command", str(args.task_complete_command)])
    if bool(getattr(args, "task_complete_sync", False)):
        command.append("--task-complete-sync")
    if not bool(getattr(args, "fail_on_fail", True)):
        command.append("--no-fail-on-fail")
    if bool(getattr(args, "no_color", False)):
        command.append("--no-color")
    if bool(getattr(args, "verbose", False)):
        command.append("--verbose")
    return command

def clear_restart_requests(manifest: dict[str, Any], run_name: str, *, label: str) -> None:
    stop_path = stop_request_path(manifest)
    if stop_path.exists():
        stop_path.unlink()
        print(f"{label}: cleared stop request for {run_name}")
    queue_path = queue_request_path(manifest)
    if queue_path.exists():
        queue_path.unlink()
        print(f"{label}: cleared queue request for {run_name}")
    launch_path = launch_request_path(manifest)
    if launch_path.exists():
        launch_path.unlink()
        print(f"{label}: cleared launch request for {run_name}")


def clear_resume_markers(manifest: dict[str, Any], run_name: str, *, label: str) -> None:
    stop_path = stop_request_path(manifest)
    if stop_path.exists():
        stop_path.unlink()
        print(f"{label}: cleared stop request for {run_name}")
    launch_path = launch_request_path(manifest)
    launch = read_state_json(launch_path)
    if launch_path.exists() and launch_request_stale(launch):
        launch_path.unlink()
        print(f"{label}: cleared launch request for {run_name}")

def archive_failed_tasks_for_retry(manifest: dict[str, Any], run_name: str) -> list[str]:
    failed_tasks: list[str] = []
    archive_root = state_dir(manifest) / "retry-failures"
    archive_dir = archive_root / str(int(epoch() * 1000))
    suffix = 1
    while archive_dir.exists():
        suffix += 1
        archive_dir = archive_root / f"{int(epoch() * 1000)}-{suffix}"
    for task in sorted(manifest.get("tasks") or [], key=lambda item: int(item.get("index", 0))):
        if not isinstance(task, dict):
            continue
        fail_path = task_fail_path(manifest, task)
        if not fail_path.exists():
            continue
        archive_dir.mkdir(parents=True, exist_ok=True)
        fail_path.replace(archive_dir / fail_path.name)
        task_running_path(manifest, task).unlink(missing_ok=True)
        shutil.rmtree(task_claim_dir(manifest, task), ignore_errors=True)
        failed_tasks.append(str(task.get("id") or fail_path.stem))
    if failed_tasks:
        print(f"retry: archived {len(failed_tasks)} failed task marker(s) for {run_name}")
    return failed_tasks

def queue_monitor_path(run_dir: Path) -> Path:
    return run_dir / QUEUE_MONITOR_NAME

def write_queue_request(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
    command: list[str],
    blockers: list[dict[str, Any]],
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    requested_at = str(existing.get("requested_at") or "") if isinstance(existing, dict) else ""
    try:
        requested_epoch = float(existing.get("requested_epoch")) if isinstance(existing, dict) else epoch()
    except (TypeError, ValueError):
        requested_epoch = epoch()
    payload = {
        "schema": SCHEMA,
        "run": run_dir.name,
        "requested_at": requested_at or now(),
        "requested_epoch": requested_epoch,
        "host": socket.gethostname(),
        "command": command,
        "blocked_by": [str(summary.get("run") or "") for summary in blockers],
    }
    state_dir(manifest).mkdir(parents=True, exist_ok=True)
    write_json(queue_request_path(manifest), payload)
    return payload

def write_launch_request(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
    reason: str,
    pid: int | None = None,
) -> dict[str, Any]:
    payload = {
        "schema": SCHEMA,
        "run": run_dir.name,
        "reason": reason,
        "requested_at": now(),
        "requested_epoch": epoch(),
        "pid": int(pid or os.getpid()),
        "lease_seconds": LAUNCH_REQUEST_LEASE_SECONDS,
        "host": socket.gethostname(),
    }
    state_dir(manifest).mkdir(parents=True, exist_ok=True)
    write_json(launch_request_path(manifest), payload)
    return payload

def clear_launch_request(manifest: dict[str, Any]) -> None:
    launch_request_path(manifest).unlink(missing_ok=True)

def run_queue_launcher(payload: dict[str, Any]) -> int:
    run_name = str(payload["run"])
    command = [str(item) for item in payload["command"]]
    lease_seconds = int(payload.get("lease_seconds") or 600)
    poll_seconds = max(0.5, float(payload.get("poll_seconds") or QUEUE_LAUNCHER_POLL_SECONDS))
    fail_if_unqueued = bool(payload.get("fail_if_unqueued"))
    if payload.get("runs_dir"):
        os.environ[RUNS_DIR_ENV] = str(payload["runs_dir"])
    os.environ[COORDINATOR_RUNS_ENV] = ""

    last_blockers: list[str] | None = None
    while True:
        with scheduler_lock():
            summaries = load_run_summaries(lease_seconds=lease_seconds)
            target = next((summary for summary in summaries if str(summary.get("run")) == run_name), None)
            if target is None:
                print(f"queue: {run_name} disappeared", flush=True)
                return 1
            if not run_queue_requested(target):
                print(f"queue: {run_name} is no longer queued", flush=True)
                return 1 if fail_if_unqueued else 0
            blockers = queue_blockers_for_run(run_name, summaries)
            blocker_names = [str(summary.get("run") or "") for summary in blockers]
            if not blockers:
                manifest_path = run_manifest_path(run_name)
                manifest = load_manifest(manifest_path)
                write_launch_request(run_dir=manifest_path.parent, manifest=manifest, reason="queue")
                break
        if blocker_names != last_blockers:
            print(f"queue: {run_name} waiting for {', '.join(blocker_names)}", flush=True)
            last_blockers = blocker_names
        time.sleep(poll_seconds)

    print(f"queue: launching {run_name}: {shell_join(command)}", flush=True)
    return subprocess.run(command, check=False).returncode

def queue_launcher_payload(
    *,
    run_dir: Path,
    command: list[str],
    lease_seconds: int,
    fail_if_unqueued: bool = False,
) -> dict[str, Any]:
    return {
        "run": run_dir.name,
        "runs_dir": str(runs_dir()),
        "command": command,
        "lease_seconds": lease_seconds,
        "poll_seconds": QUEUE_LAUNCHER_POLL_SECONDS,
        "fail_if_unqueued": fail_if_unqueued,
    }

def run_queue_launcher_foreground(*, run_dir: Path, command: list[str], lease_seconds: int) -> int:
    old_runs_dir = os.environ.get(RUNS_DIR_ENV)
    old_coordinator_runs = os.environ.get(COORDINATOR_RUNS_ENV)
    try:
        return run_queue_launcher(
            queue_launcher_payload(
                run_dir=run_dir,
                command=command,
                lease_seconds=lease_seconds,
                fail_if_unqueued=True,
            )
        )
    finally:
        if old_runs_dir is None:
            os.environ.pop(RUNS_DIR_ENV, None)
        else:
            os.environ[RUNS_DIR_ENV] = old_runs_dir
        if old_coordinator_runs is None:
            os.environ.pop(COORDINATOR_RUNS_ENV, None)
        else:
            os.environ[COORDINATOR_RUNS_ENV] = old_coordinator_runs

def start_queue_monitor(
    *,
    run_dir: Path,
    command: list[str],
    lease_seconds: int,
) -> int:
    with scheduler_lock():
        manifest_path = run_dir / RUN_MANIFEST_NAME
        if not manifest_path.is_file():
            return 0
        manifest = load_manifest(manifest_path)
        if not queue_request_path(manifest).is_file():
            return 0
        monitor_path = queue_monitor_path(run_dir)
        if monitor_path.is_file():
            try:
                monitor = read_json(monitor_path)
                pid = int(monitor.get("pid") or 0)
                if pid > 0 and process_is_alive(pid):
                    return pid
            except (OSError, SystemExit, TypeError, ValueError):
                pass
        payload = queue_launcher_payload(run_dir=run_dir, command=command, lease_seconds=lease_seconds)
        log = (run_dir / "queue.log").open("ab", buffering=0)
        env = os.environ.copy()
        env[RUNS_DIR_ENV] = str(runs_dir())
        env[COORDINATOR_RUNS_ENV] = ""
        process = subprocess.Popen(
            [sys.executable, "-c", _QUEUE_LAUNCHER_CODE, json.dumps(payload)],
            cwd=script_repo(),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        write_json(monitor_path, {
            "schema": SCHEMA,
            "pid": process.pid,
            "command": command,
            "log": str(run_dir / "queue.log"),
            "started_at": now(),
            "started_epoch": epoch(),
        })
        return process.pid


def recover_queue_monitors(
    *,
    lease_seconds: int = 600,
    exclude: set[str] | None = None,
) -> dict[str, int]:
    monitors: dict[str, int] = {}
    excluded = exclude or set()
    summaries = sorted(
        (summary for summary in load_run_summaries(lease_seconds=lease_seconds) if run_queue_requested(summary)),
        key=queued_run_sort_key,
    )
    for summary in summaries:
        queue = summary.get("queue")
        command = queue.get("command") if isinstance(queue, dict) else None
        if not isinstance(command, list) or not command:
            continue
        run_name = str(summary.get("run") or "")
        if run_name in excluded:
            continue
        pid = start_queue_monitor(
            run_dir=run_dir_for_name(run_name),
            command=[str(item) for item in command],
            lease_seconds=lease_seconds,
        )
        if pid:
            monitors[run_name] = pid
    return monitors

def coordinator_host_for_workers(workers: list[dict[str, Any]], coordinator_host: str | None = None) -> str:
    if coordinator_host:
        return coordinator_host
    for worker in workers:
        if worker_targets_current_host(worker):
            return worker_host(worker)
    return socket.gethostname()

def coordinator_for_worker(
    worker: dict[str, Any],
    manifest_path: Path,
    coordinator_host: str | None = None,
    *,
    force_remote: bool = False,
) -> str:
    host = str(coordinator_host or "").strip()
    if force_remote and host:
        return f"{host}:{manifest_path}"
    if worker_targets_current_host(worker):
        return str(manifest_path)
    host = host or socket.gethostname()
    return f"{host}:{manifest_path}"

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

def prepare_resume_manifest(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    label: str,
) -> dict[str, Any]:
    result = prepare_run_resume(manifest_path, manifest)
    if result["preserved"]:
        print(
            f"{label}: preserved {result['preserved']} completed game(s); "
            f"created {result['continued']} remainder shard(s)"
        )
        return load_manifest(manifest_path)
    return manifest

def materialize_run(
    *,
    template_path: Path,
    workers_arg: str,
    inline_worker_defaults: str | Path | None = None,
    workers_snapshot_payload: dict[str, Any] | None = None,
    preserve_workers_snapshot: bool = False,
    comment: str | None = None,
    run_name_arg: str | None,
    resume: bool,
    replace: bool,
    attach_existing: bool = False,
) -> tuple[Path, Path, Path]:
    template = load_manifest(template_path)
    run_name = validate_run_name(run_name_arg) if run_name_arg else default_run_name(template_path, template)
    run_dir = run_dir_for_name(run_name)
    manifest_path = run_dir / RUN_MANIFEST_NAME
    workers_snapshot = run_dir / RUN_WORKERS_NAME

    if resume and replace:
        raise SystemExit("--resume and --replace are mutually exclusive")
    if attach_existing:
        if not manifest_path.exists():
            raise SystemExit(f"{run_name}: cannot attach workers; run manifest is missing")
        if not workers_snapshot.exists():
            payload = (
                workers_snapshot_payload
                if workers_snapshot_payload is not None
                else workers_payload_from_arg(
                    workers_arg or None,
                    default_ok=not bool(workers_arg),
                    inline_defaults_path=inline_worker_defaults,
                )
            )
            write_json(workers_snapshot, payload)
        return run_dir, manifest_path, workers_snapshot
    if resume:
        if not manifest_path.exists():
            raise SystemExit(f"{run_name}: cannot resume; run manifest is missing")
        if not workers_snapshot.exists():
            payload = (
                workers_snapshot_payload
                if workers_snapshot_payload is not None
                else workers_payload_from_arg(
                    workers_arg or None,
                    default_ok=not bool(workers_arg),
                    inline_defaults_path=inline_worker_defaults,
                )
            )
            write_json(workers_snapshot, payload)
        manifest = load_manifest(manifest_path)
        manifest = prepare_resume_manifest(manifest_path, manifest, label="resume")
        clear_restart_requests(manifest, run_name, label="resume")
        if not preserve_workers_snapshot:
            payload = (
                workers_snapshot_payload
                if workers_snapshot_payload is not None
                else workers_payload_from_arg(
                    workers_arg,
                    default_ok=False,
                    inline_defaults_path=inline_worker_defaults,
                )
            )
            write_json(
                workers_snapshot,
                payload,
            )
        return run_dir, manifest_path, workers_snapshot
    if run_dir.exists():
        if not replace:
            raise SystemExit(f"{run_name}: run already exists; pass --resume or --replace")
        archive_tmp = run_dir.parent / f".{run_name}.archive.{os.getpid()}"
        archive_tmp.mkdir(parents=True, exist_ok=True)
        for name in ("logs", "summary.json", "dataset-summary.json", "wait.log"):
            source = run_dir / name
            if not source.exists():
                continue
            target = archive_tmp / name
            if source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copy2(source, target)
        shutil.rmtree(run_dir)

    run_dir.mkdir(parents=True)
    archive_tmp = run_dir.parent / f".{run_name}.archive.{os.getpid()}"
    if archive_tmp.exists():
        archive_root = run_dir / "archive" / time.strftime("%Y%m%d-%H%M%S")
        archive_root.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(archive_tmp), str(archive_root))
    materialized = materialize_manifest(template, run_name=run_name, run_dir=run_dir)
    clean_comment = clean_manifest_text(comment, label="--comment")
    if clean_comment:
        materialized["comment"] = clean_comment
    payload = (
        workers_snapshot_payload
        if workers_snapshot_payload is not None
        else workers_payload_from_arg(
            workers_arg,
            default_ok=False,
            inline_defaults_path=inline_worker_defaults,
        )
    )
    if materialized.get("tasks"):
        assign_task_claim_workers(
            materialized,
            workers_from_payload(payload, label=workers_arg),
        )
    state_dir(materialized).mkdir(parents=True)
    log_dir(materialized).mkdir(parents=True)
    claim_dir(materialized).mkdir(parents=True)
    (run_dir / "outputs").mkdir(parents=True)
    write_json(manifest_path, materialized)
    write_json(
        workers_snapshot,
        payload,
    )
    return run_dir, manifest_path, workers_snapshot

def cmd_deploy(args: argparse.Namespace) -> int:
    template_path = expand_path(args.manifest)
    _run_dir, manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    manifest_existed = manifest_path.exists()
    try:
        return run_deploy(args)
    except KeyboardInterrupt:
        coordinator_host, _runs_path = parse_coordinator(configured_coordinator_runs() or "")
        owns_run = (
            bool(getattr(args, "resume", False))
            or bool(getattr(args, "replace", False))
            or not manifest_existed
        )
        if not coordinator_host and owns_run and manifest_path.exists():
            manifest = load_manifest(manifest_path)
            if not stop_request_path(manifest).exists():
                cleanup = request_stop_for_run(
                    manifest_path,
                    manifest,
                    lease_seconds=int(getattr(args, "lease_seconds", 600)),
                    reason="deploy interrupted",
                )
                print(
                    f"deploy: interrupt cleanup complete killed={cleanup['killed']} "
                    f"released={len(cleanup['released'])} reset={len(cleanup['reset'])}",
                    file=sys.stderr,
                    flush=True,
                )
        raise

def run_deploy(args: argparse.Namespace) -> int:
    if not bool(getattr(args, "attach_existing", False)):
        delegated = deploy_on_coordinator(args, configured_coordinator_runs() or "")
        if delegated is not None:
            return delegated

    started_at = time.monotonic()
    template_path = expand_path(args.manifest)
    planned_run_dir, _planned_manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    run_name = planned_run_dir.name
    lease_seconds = int(getattr(args, "lease_seconds", 600))
    label = str(getattr(args, "command_label", "deploy"))
    if bool(getattr(args, "replace", False)) and planned_run_dir.exists():
        kill_run_workers(planned_run_dir)
    with scheduler_lock():
        summaries = load_run_summaries(lease_seconds=lease_seconds)
        target = next((summary for summary in summaries if str(summary.get("run")) == run_name), None)
        existing_queue = target.get("queue") if isinstance(target, dict) else None
        blockers = queue_blockers_for_run(run_name, summaries)
        run_dir, manifest_path, workers_path = materialize_run(
            template_path=template_path,
            workers_arg=str(args.workers),
            inline_worker_defaults=getattr(args, "worker_defaults", None),
            workers_snapshot_payload=getattr(args, "worker_snapshot_payload", None),
            preserve_workers_snapshot=bool(getattr(args, "preserve_workers_snapshot", False)),
            comment=getattr(args, "comment", None),
            run_name_arg=getattr(args, "run", None),
            resume=bool(getattr(args, "resume", False)),
            replace=bool(getattr(args, "replace", False)),
            attach_existing=bool(getattr(args, "attach_existing", False)),
        )
        manifest = load_manifest(manifest_path)
        command = queue_resume_command(args, run_name)
        queued = bool(blockers)
        if queued:
            clear_launch_request(manifest)
            write_queue_request(
                run_dir=run_dir,
                manifest=manifest,
                command=command,
                blockers=blockers,
                existing=existing_queue if isinstance(existing_queue, dict) else None,
            )
        else:
            queue_request_path(manifest).unlink(missing_ok=True)
            write_launch_request(run_dir=run_dir, manifest=manifest, reason="deploy")

    if queued:
        setattr(args, "queue_handled", True)
        blocker_names = ", ".join(str(summary.get("run") or "") for summary in blockers)
        print(f"{label}: queued {run_name}; waiting for {blocker_names}")
        foreground = bool(getattr(args, "queue_foreground", False))
        monitors = recover_queue_monitors(
            lease_seconds=lease_seconds,
            exclude={run_name} if foreground else None,
        )
        if foreground:
            print(f"{label}: waiting for queued run to finish")
            print(f"{label}: status: forge status {run_name} --verbose")
            return run_queue_launcher_foreground(
                run_dir=run_dir,
                command=command,
                lease_seconds=lease_seconds,
            )
        monitor_pid = monitors.get(run_name) or start_queue_monitor(
            run_dir=run_dir,
            command=command,
            lease_seconds=lease_seconds,
        )
        print(f"{label}: queue monitor pid={monitor_pid}")
        print(f"{label}: status: forge status {run_name} --verbose")
        return 0

    launch_workers_payload = getattr(args, "launch_workers_payload", None)
    workers = (
        workers_from_payload(launch_workers_payload, label=str(args.workers))
        if launch_workers_payload is not None
        else load_workers(workers_path)
    )
    jobs = max(1, int(args.jobs))
    remote_timeout = max(1.0, float(args.remote_timeout_seconds))
    bootstrap_timeout = min(remote_timeout, DEPLOY_BOOTSTRAP_TIMEOUT_SECONDS)
    setup_retries = SSH_TRANSIENT_RETRY_COUNT
    has_tasks = bool(manifest.get("tasks"))
    launch_marker_written = True
    configured_coordinator_host = getattr(args, "coordinator_host", None) or default_coordinator_host()
    coordinator_host = coordinator_host_for_workers(workers, configured_coordinator_host)
    force_remote_coordinator = bool(configured_coordinator_host)
    color_enabled = not bool(args.no_color) and sys.stdout.isatty()
    print_lock = threading.Lock()
    tracker = ProcessTracker()

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
            bootstrap_worker_runner(
                worker,
                manifest_path,
                timeout=bootstrap_timeout,
                tracker=tracker,
                retries=setup_retries,
            )
            synced_inputs = sync_inputs_to_worker(
                worker,
                manifest,
                timeout=remote_timeout,
                tracker=tracker,
                retries=setup_retries,
            )
            if synced_inputs:
                verbose_status(f"{label}: synced {synced_inputs} input file(s)")
            verbose_status(f"{label}: checking prerequisites")
            coordinator = (
                coordinator_for_worker(
                    worker,
                    manifest_path,
                    coordinator_host,
                    force_remote=force_remote_coordinator,
                )
                if has_tasks else None
            )
            script = deploy_prereq_command(worker=worker, manifest=manifest, coordinator=coordinator)
            return run_worker_shell(
                worker,
                script,
                timeout=remote_timeout,
                tracker=tracker,
                retries=setup_retries,
            )
        except Exception as exc:
            return subprocess.CompletedProcess(["deploy", host], 1, stdout="", stderr=str(exc))

    def result_payload(
        index: int,
        worker: dict[str, Any],
        stage: str,
        result: subprocess.CompletedProcess[str],
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        payload = {
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
        if extra:
            payload.update(extra)
        return payload

    def setup_worker_groups() -> list[list[tuple[int, dict[str, Any]]]]:
        groups: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = {}
        for index, worker in enumerate(workers):
            groups.setdefault(deploy_setup_key(worker), []).append((index, worker))
        return list(groups.values())

    def setup_group_label(group: list[tuple[int, dict[str, Any]]]) -> str:
        worker = group[0][1]
        label = worker_label(str(worker["host"]), str(worker["profile"]))
        if len(group) > 1:
            return f"{label} [{len(group)} slots]"
        return label

    def setup_worker_group(group: list[tuple[int, dict[str, Any]]]) -> list[dict[str, Any]]:
        status(
            f"  {colorize('SETUP ', 'cyan', enabled=color_enabled)} "
            f"{setup_group_label(group)}: setup"
        )
        representative = group[0][1]
        prereq = run_worker_setup(representative)
        payloads = []
        for index, worker in group:
            host = str(worker["host"])
            profile = str(worker["profile"])
            label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))
            if prereq.returncode != 0:
                verbose_status(f"{label}: prerequisites FAILED rc={prereq.returncode}", error=True)
            else:
                verbose_status(f"{label}: prerequisites OK")
            payloads.append(result_payload(index, worker, "setup", prereq))
        return payloads

    def run_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))
        verbose_status(f"{label}: running tasks")
        script = deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator=coordinator_for_worker(
                worker,
                manifest_path,
                coordinator_host,
                force_remote=force_remote_coordinator,
            ),
            retry_failed=bool(getattr(args, "retry_failed", False)),
            task_attempts=int(getattr(args, "task_attempts", 0)),
            task_complete_command=getattr(args, "task_complete_command", None),
            task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
        )
        result = run_worker_shell(worker, script, timeout=remote_timeout, tracker=tracker)
        if result.returncode == 0:
            verbose_status(f"{label}: tasks OK")
            return result_payload(index, worker, "tasks", result)
        verbose_status(f"{label}: tasks FAILED rc={result.returncode}", error=True)
        return result_payload(index, worker, "tasks", result)

    def launch_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))
        verbose_status(f"{label}: launching worker")
        pid_file, log_file = worker_launch_files(run_dir.name, index, worker)
        script = deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator=coordinator_for_worker(
                worker,
                manifest_path,
                coordinator_host,
                force_remote=force_remote_coordinator,
            ),
            retry_failed=bool(getattr(args, "retry_failed", False)),
            task_attempts=int(getattr(args, "task_attempts", 0)),
            task_complete_command=getattr(args, "task_complete_command", None),
            task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
        )
        result = run_worker_shell(
            worker,
            detached_shell_command(worker, script, pid_file=pid_file, log_file=log_file),
            timeout=remote_timeout,
            tracker=tracker,
            retries=setup_retries,
        )
        if result.returncode == 0:
            verbose_status(f"{label}: launch OK")
            return result_payload(index, worker, "launch", result, {"pid_file": pid_file, "log_file": log_file})
        verbose_status(f"{label}: launch FAILED rc={result.returncode}", error=True)
        return result_payload(index, worker, "launch", result, {"pid_file": pid_file, "log_file": log_file})

    def cleanup_failed_launch(reason: str) -> dict[str, Any]:
        return request_stop_for_run(
            manifest_path,
            manifest,
            lease_seconds=lease_seconds,
            reason=reason,
        )

    def started_task_count(counts: dict[str, int]) -> int:
        return counts["done"] + counts["fail"] + counts["running"] + counts["claimed"] + counts["stale"]

    def wait_for_launch_started(before_started: int, expected_new_started: int) -> int:
        if expected_new_started <= 0:
            return 0
        deadline = time.monotonic() + DEPLOY_STARTUP_TIMEOUT_SECONDS
        observed_new_started = 0
        while True:
            summary = status_summary(manifest, lease_seconds=600)
            counts = summary["counts"]
            started = started_task_count(counts)
            observed_new_started = max(observed_new_started, max(0, started - before_started))
            if started >= before_started + expected_new_started:
                return observed_new_started
            if time.monotonic() >= deadline:
                return observed_new_started
            time.sleep(DEPLOY_STARTUP_POLL_SECONDS)

    def result_output(item: dict[str, Any]) -> str:
        return "\n".join(
            part.strip()
            for part in (str(item.get("stdout") or ""), str(item.get("stderr") or ""))
            if part.strip()
        )

    def launch_log_diagnostic(item: dict[str, Any]) -> str:
        log_file = str(item.get("log_file") or "").strip()
        if not log_file:
            return ""
        index = int(item["index"])
        result = run_worker_shell(
            workers[index],
            f"tail -n 40 -- {shlex.quote(log_file)} 2>/dev/null || true",
            timeout=min(remote_timeout, 30.0),
            tracker=tracker,
            retries=setup_retries,
        )
        return result_output({"stdout": result.stdout, "stderr": result.stderr})

    def result_host_label(item: dict[str, Any]) -> str:
        return worker_label(str(item["host"]), str(item["profile"]))

    def failure_summary_reason(output: str) -> str:
        text = output.strip()
        lower = text.lower()
        if not text:
            return "failed without output"
        if "no route to host" in lower or "connection timed out" in lower or "could not resolve hostname" in lower:
            return "not responding"
        missing_engine = re.search(
            r"engine\s+(\S+)\s+is missing and Enyo commit\s+([0-9A-Fa-f]+)\s+is unavailable",
            text,
        )
        if missing_engine:
            engine = Path(missing_engine.group(1)).name
            commit = missing_engine.group(2)
            return f"missing {engine} (Enyo commit {commit} unavailable)"
        missing_input = re.search(r"required input missing:\s*(\S+)", text)
        if missing_input:
            return f"missing {Path(missing_input.group(1)).name}"
        missing_command = re.search(r"required command not found:\s*(\S+)", text)
        if missing_command:
            return f"missing {Path(missing_command.group(1)).name}"
        invalid_reference = re.search(r"fatal: invalid reference:\s*([0-9A-Fa-f]+)", text)
        if invalid_reference:
            return f"missing Enyo commit {invalid_reference.group(1)}"
        first_line = next((line.strip() for line in text.splitlines() if line.strip()), text)
        return first_line[:160]

    def print_failure_summary(items: list[dict[str, Any]]) -> None:
        failed_items = [item for item in items if not bool(item["ok"])]
        if not failed_items:
            return
        host_reasons: dict[str, list[str]] = {}
        for item in failed_items:
            host = result_host_label(item)
            reason = failure_summary_reason(result_output(item))
            reasons = host_reasons.setdefault(host, [])
            if reason not in reasons:
                reasons.append(reason)
        total_hosts = len({worker_label(str(worker["host"]), str(worker["profile"])) for worker in workers})
        failed_hosts = len(host_reasons)
        status(f"deploy: failed on {failed_hosts}/{total_hosts} host(s):", error=True)
        for host in sorted(host_reasons):
            status(f"  {host}:", error=True)
            for reason in host_reasons[host]:
                status(f"      {reason}", error=True)

    def result_label(items: list[dict[str, Any]]) -> str:
        first = min(items, key=lambda entry: int(entry["index"]))
        if len(items) == 1:
            return worker_label(
                str(first["host"]),
                str(first["profile"]),
                first.get("slot"),
                first.get("slots"),
            )
        label = worker_label(str(first["host"]), str(first["profile"]))
        return f"{label} [{len(items)} slots]"

    def result_groups(items: list[dict[str, Any]], *, show_success: bool) -> list[list[dict[str, Any]]]:
        if args.verbose:
            return [[item] for item in sorted(items, key=lambda entry: int(entry["index"]))]
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        order: list[tuple[Any, ...]] = []
        for item in sorted(items, key=lambda entry: int(entry["index"])):
            ok = bool(item["ok"])
            if ok and not show_success:
                continue
            key = (
                ok,
                item.get("host"),
                item.get("profile"),
                item.get("stage"),
                item.get("rc"),
                result_output(item),
            )
            if key not in groups:
                groups[key] = []
                order.append(key)
            groups[key].append(item)
        return [groups[key] for key in order]

    def print_results(items: list[dict[str, Any]], *, show_success: bool = False) -> None:
        for group in result_groups(items, show_success=show_success):
            item = min(group, key=lambda entry: int(entry["index"]))
            ok = bool(item["ok"])
            marker_text = "OK" if ok else "FAILED"
            marker_color = "green" if ok else "red"
            marker = colorize(f"{marker_text:<6}", marker_color, enabled=color_enabled)
            suffix = "" if ok else f" rc={item['rc']}"
            status(
                f"  {marker} "
                f"{result_label(group)}: "
                f"{item['stage']}{suffix}"
            )
            output = result_output(item)
            if output and (args.verbose or not ok):
                for line in output.splitlines():
                    status(f"         {line}")

    def collect_setup_results(
        future_to_group: dict[concurrent.futures.Future[Any], list[tuple[int, dict[str, Any]]]],
        on_ready: Callable[[list[dict[str, Any]]], None] | None = None,
        stop_when_tasks_done: bool = False,
    ) -> list[dict[str, Any]]:
        setup_results_list: list[dict[str, Any]] = []
        pending = set(future_to_group)
        while pending:
            done, pending = concurrent.futures.wait(
                pending,
                timeout=0.5 if stop_when_tasks_done else None,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            if not done:
                if stop_when_tasks_done:
                    try:
                        current = load_manifest(manifest_path)
                        current_summary = status_summary(current, lease_seconds=lease_seconds)
                        current_counts = current_summary["counts"]
                        if current_counts["done"] == len(current.get("tasks") or []):
                            status(
                                colorize(
                                    "deploy: tasks completed while setup was still pending; stopping setup",
                                    "yellow",
                                    enabled=color_enabled,
                                ),
                                error=True,
                            )
                            tracker.terminate_all()
                            for future in pending:
                                future.cancel()
                            concurrent.futures.wait(pending, timeout=2)
                            break
                    except (OSError, ValueError, json.JSONDecodeError):
                        pass
                continue
            for future in done:
                pending.discard(future)
                group_results = future.result()
                setup_results_list.extend(group_results)
                print_results(group_results, show_success=bool(args.verbose))
                if on_ready is not None and all(item["ok"] for item in group_results):
                    on_ready(group_results)
        return setup_results_list

    status(colorize(f"deploy: run={run_dir.name} workers={len(workers)} jobs={jobs}", "cyan", enabled=color_enabled))
    task_count = len(manifest.get("tasks") or [])
    if (
        has_tasks
        and str(manifest.get("kind") or "") != "spsa"
        and len(workers) > 1
        and task_count < len(workers) * 4
    ):
        status(
            colorize(
                f"deploy: warning: {task_count} shard(s) for {len(workers)} worker slot(s); "
                "long shards balance better with at least 4x more shards than slots",
                "yellow",
                enabled=color_enabled,
            ),
            error=True,
        )
    setup_groups = setup_worker_groups()
    setup_jobs = jobs
    if has_tasks and bool(getattr(args, "launch_only", False)):
        setup_jobs = max(setup_jobs, len(setup_groups))
    executor: concurrent.futures.ThreadPoolExecutor | None = concurrent.futures.ThreadPoolExecutor(max_workers=setup_jobs)
    launch_executor: concurrent.futures.ThreadPoolExecutor | None = None
    interrupted = False
    startup_failed = False
    futures: list[concurrent.futures.Future[Any]] = []
    launch_futures: list[concurrent.futures.Future[Any]] = []
    launch_only = has_tasks and bool(getattr(args, "launch_only", False))
    before_started = 0
    before_pending = 0
    if launch_only:
        before_launch = status_summary(manifest, lease_seconds=600)
        before_counts = before_launch["counts"]
        before_started = started_task_count(before_counts)
        before_pending = before_counts["pending"]
        launch_executor = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(workers)))

    def launch_ready_workers(group_results: list[dict[str, Any]]) -> None:
        assert launch_executor is not None
        status(
            colorize(
                f"deploy: {result_label(group_results)} ready; launching now",
                "green",
                enabled=color_enabled,
            )
        )
        for item in group_results:
            index = int(item["index"])
            launch_futures.append(launch_executor.submit(launch_worker, index, workers[index]))

    try:
        future_to_group = {
            executor.submit(setup_worker_group, group): group
            for group in setup_groups
        }
        futures = list(future_to_group)
        setup_results_list = collect_setup_results(
            future_to_group,
            launch_ready_workers if launch_only else None,
            stop_when_tasks_done=launch_only,
        )
        setup_failed = sum(1 for item in setup_results_list if not item["ok"])
        setup_failed_results = [item for item in setup_results_list if not item["ok"]]
        ready_worker_items = [
            (int(item["index"]), workers[int(item["index"])])
            for item in setup_results_list
            if item["ok"]
        ]
        if not has_tasks or not ready_worker_items:
            results = setup_results_list
        else:
            if setup_failed:
                status(
                    colorize(
                        f"deploy: setup partial; {len(ready_worker_items)}/{len(workers)} slots ready",
                        "yellow",
                        enabled=color_enabled,
                    ),
                )
            executor.shutdown(wait=not launch_only, cancel_futures=launch_only)
            executor = None
            if launch_only:
                expected_new_started = min(before_pending, len(ready_worker_items))
                launch_results = [future.result() for future in concurrent.futures.as_completed(launch_futures)]
                results = setup_failed_results + launch_results
                print_results(launch_results, show_success=bool(args.verbose))
                if all(item["ok"] for item in launch_results):
                    status(colorize("deploy: verifying workers started", "cyan", enabled=color_enabled))
                    started_workers = wait_for_launch_started(before_started, expected_new_started)
                    if started_workers >= expected_new_started:
                        status(colorize("deploy: workers active", "green", enabled=color_enabled))
                    elif started_workers > 0:
                        status(
                            colorize(
                                f"deploy: workers partially active; {started_workers}/{expected_new_started} slots started",
                                "yellow",
                                enabled=color_enabled,
                            ),
                            error=True,
                        )
                    else:
                        status(
                            colorize(
                                "deploy: workers launched; task claims are still pending",
                                "yellow",
                                enabled=color_enabled,
                            ),
                        )
                        diagnostics: dict[str, list[str]] = {}
                        for item in launch_results:
                            diagnostic = launch_log_diagnostic(item)
                            if diagnostic:
                                diagnostics.setdefault(result_host_label(item), []).append(diagnostic)
                        if diagnostics:
                            status("deploy: startup diagnostics:", error=True)
                            for host, messages in sorted(diagnostics.items()):
                                status(f"  {host}:", error=True)
                                for message in messages:
                                    for line in message.splitlines():
                                        status(f"      {line}", error=True)
            else:
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(ready_worker_items)))
                task_message = (
                    "deploy: setup complete; starting tasks"
                    if setup_failed == 0
                    else "deploy: setup partial; starting tasks on ready workers"
                )
                status(colorize(task_message, "green" if setup_failed == 0 else "yellow", enabled=color_enabled))
                futures = [
                    executor.submit(run_worker, index, worker)
                    for index, worker in ready_worker_items
                ]
                task_results = [future.result() for future in concurrent.futures.as_completed(futures)]
                results = setup_failed_results + task_results
                print_results(task_results, show_success=bool(args.verbose))
        print_failure_summary(results)
    except KeyboardInterrupt:
        interrupted = True
        status(colorize("deploy: interrupted; stopping active worker commands", "yellow", enabled=color_enabled), error=True)
        tracker.terminate_all()
        all_futures = futures + launch_futures
        concurrent.futures.wait(all_futures, timeout=2)
        for future in all_futures:
            future.cancel()
        cleanup = cleanup_failed_launch("deploy interrupted")
        status(
            "deploy: interrupt cleanup complete "
            f"killed={cleanup['killed']} released={len(cleanup['released'])} "
            f"reset={len(cleanup['reset'])}",
            error=True,
        )
        return 130
    except BaseException:
        if launch_marker_written:
            clear_launch_request(manifest)
        raise
    finally:
        if executor is not None:
            executor.shutdown(wait=not interrupted, cancel_futures=interrupted)
        if launch_executor is not None:
            launch_executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    command_stage_started = any(item["stage"] in ("launch", "tasks") for item in results)
    if not has_tasks:
        failed = sum(1 for item in results if not item["ok"])
    elif not command_stage_started:
        failed = max(1, sum(1 for item in results if not item["ok"]))
    else:
        failed = (
            sum(1 for item in results if item["stage"] in ("launch", "tasks") and not item["ok"])
            + (1 if startup_failed else 0)
        )
    if not has_tasks:
        status(colorize("deploy: setup complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: setup failed", "red", enabled=color_enabled))
    elif not command_stage_started:
        status(colorize("deploy: setup failed; no tasks started", "red", enabled=color_enabled))
    elif bool(getattr(args, "launch_only", False)):
        status(colorize("deploy: launch complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: launch failed", "red", enabled=color_enabled))
    else:
        status(colorize("deploy: tasks complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: tasks failed", "red", enabled=color_enabled))
    notify_command = getattr(args, "notify_command", None) or default_notify_command()
    completion_deferred_to_wait = False
    if failed == 0 and has_tasks and bool(getattr(args, "launch_only", False)):
        if bool(getattr(args, "start_wait_monitor", False)):
            monitor_pid = start_wait_monitor(
                run_dir=run_dir,
                manifest_path=manifest_path,
                args=args,
                notify_command=notify_command,
            )
            if monitor_pid:
                completion_deferred_to_wait = True
                write_launch_request(
                    run_dir=run_dir,
                    manifest=manifest,
                    reason="wait-monitor",
                    pid=monitor_pid,
                )
                status(colorize(f"deploy: wait monitor pid={monitor_pid}", "green", enabled=color_enabled))
        elif bool(getattr(args, "defer_completion_notify", False)):
            completion_deferred_to_wait = True
    if notify_command and not completion_deferred_to_wait:
        run_notify_command(
            notify_command,
            deploy_event_payload(
                run_dir=run_dir,
                manifest_path=manifest_path,
                workers_path=workers_path,
                manifest=manifest,
                results=results,
                failed=failed,
                has_tasks=has_tasks,
                elapsed_seconds=time.monotonic() - started_at,
            ),
        )
    commit_payload = getattr(args, "worker_snapshot_commit_payload", None)
    if failed == 0 and commit_payload is not None:
        write_json(workers_path, commit_payload)
    if launch_marker_written and not completion_deferred_to_wait:
        clear_launch_request(manifest)
    return 1 if failed else 0

def planned_run_name(template_path: Path, run_name_arg: str | None) -> str:
    template = load_manifest(template_path)
    return validate_run_name(run_name_arg) if run_name_arg else default_run_name(template_path, template)

def planned_run_paths(template_path: Path, run_name_arg: str | None) -> tuple[Path, Path]:
    run_name = planned_run_name(template_path, run_name_arg)
    run_dir = run_dir_for_name(run_name)
    return run_dir, run_dir / RUN_MANIFEST_NAME

def command_rc_from_system_exit(exc: SystemExit) -> int:
    if isinstance(exc.code, int):
        return int(exc.code)
    if exc.code:
        print(str(exc.code), file=sys.stderr)
        return 1
    return 0

def run_command_stage(label: str, name: str, func: Any, args: argparse.Namespace) -> int:
    try:
        rc = int(func(args))
    except SystemExit as exc:
        rc = command_rc_from_system_exit(exc)
    if rc:
        print(f"{label}: {name} failed rc={rc}", file=sys.stderr)
    return rc

def run_control_command_args(name: str, args: argparse.Namespace) -> list[str]:
    command = [name]
    if args.run is not None:
        command.append(str(args.run))
    if getattr(args, "workers", None):
        command.extend(["--workers", str(args.workers)])
    if bool(getattr(args, "wait", False)):
        command.append("--wait")
    if getattr(args, "verify", None) is True:
        command.append("--verify")
    elif getattr(args, "verify", None) is False:
        command.append("--no-verify")
    if getattr(args, "merge", None):
        command.extend(["--merge", str(args.merge)])
    if bool(getattr(args, "merge_force", False)):
        command.append("--merge-force")
    if bool(getattr(args, "newline_between", False)):
        command.append("--newline-between")
    if getattr(args, "notify_command", None):
        command.extend(["--notify-command", str(args.notify_command)])
    if getattr(args, "task_complete_command", None):
        command.extend(["--task-complete-command", str(args.task_complete_command)])
    if bool(getattr(args, "task_complete_sync", False)):
        command.append("--task-complete-sync")
    if bool(getattr(args, "no_color", False)):
        command.append("--no-color")
    if bool(getattr(args, "verbose", False)):
        command.append("--verbose")
    if int(getattr(args, "jobs", 4)) != 4:
        command.extend(["--jobs", str(args.jobs)])
    if int(getattr(args, "task_attempts", 2)) != 2:
        command.extend(["--task-attempts", str(args.task_attempts)])
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    return command

def interruptible_coordinator_command(
    host: str,
    runs_path: str,
    command: list[str],
) -> subprocess.CompletedProcess[str]:
    pid_file = f"/tmp/forge-control-{uuid.uuid4().hex}.pid"
    reset_sigint_exec = (
        "import os, signal, sys; "
        "signal.signal(signal.SIGINT, signal.SIG_DFL); "
        "os.execvp(sys.argv[1], sys.argv[1:])"
    )
    child = (
        f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
        f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
        f"python3 -c {shlex.quote(reset_sigint_exec)} forge {shell_join(command)}"
    )
    remote = (
        "command -v forge >/dev/null 2>&1 || "
        "{ echo 'forge not found in PATH on coordinator' >&2; exit 127; }; "
        f"pid_file={shlex.quote(pid_file)}; rm -f \"$pid_file\"; "
        f"sh -lc {shlex.quote(child)} & pid=$!; "
        "printf '%s\\n' \"$pid\" > \"$pid_file\"; "
        "trap 'kill -INT \"$pid\" 2>/dev/null; wait \"$pid\" 2>/dev/null; "
        "rm -f \"$pid_file\"; exit 130' INT TERM HUP; "
        "wait \"$pid\"; rc=$?; rm -f \"$pid_file\"; exit $rc"
    )
    return run_interruptible_coordinator_process(host, remote, pid_file)

def run_control_on_coordinator(name: str, args: argparse.Namespace, coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = run_control_command_args(name, args)
    if host:
        result = interruptible_coordinator_command(host, runs_path, command)
    else:
        env = os.environ.copy()
        env[COORDINATOR_RUNS_ENV] = ""
        env[RUNS_DIR_ENV] = runs_path
        result = subprocess.run(
            [sys.executable, str(script_path()), *command],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=env,
        )
    print(f"coordinator: {coordinator_runs}")
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode

def coordinator_temp_file(host: str) -> str:
    result = subprocess.run(
        ["ssh", *SSH_OPTS, host, "mktemp /tmp/forge.XXXXXX"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or "failed to create coordinator temp file")
    path = result.stdout.strip().splitlines()[-1:]
    if not path:
        raise SystemExit("coordinator did not return a temp file path")
    return path[0]

def copy_file_to_coordinator(host: str, path: Path) -> str:
    remote_path = coordinator_temp_file(host)
    try:
        result = run_process_command_with_retries(
            ["rsync", "-a", "-e", RSYNC_SSH, str(path), f"{host}:{remote_path}"],
            retries=SSH_TRANSIENT_RETRY_COUNT,
        )
    except BaseException:
        remove_coordinator_temp_files(host, [remote_path])
        raise
    if result.returncode != 0:
        remove_coordinator_temp_files(host, [remote_path])
        raise SystemExit(result.stderr.strip() or f"failed to copy {path} to {host}")
    return remote_path

def remove_coordinator_temp_files(host: str, paths: list[str]) -> None:
    safe_paths = [path for path in paths if path.startswith("/tmp/forge.")]
    if not safe_paths:
        return
    try:
        subprocess.run(
            ["ssh", *SSH_OPTS, host, shell_join(["rm", "-f", "--", *safe_paths])],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass

def coordinator_workers_arg(host: str, workers_arg: str) -> str:
    path = expand_path(workers_arg)
    if path.exists():
        return copy_file_to_coordinator(host, path)
    if worker_arg_looks_like_path(workers_arg):
        raise SystemExit(f"workers file not found: {workers_arg}")
    return workers_arg

def append_deploy_options(command: list[str], args: argparse.Namespace, *, coordinator_host: str) -> None:
    if getattr(args, "run", None):
        command.extend(["--run", str(args.run)])
    if getattr(args, "comment", None):
        command.extend(["--comment", str(args.comment)])
    for flag in ("resume", "replace", "retry_failed", "no_color", "verbose", "task_complete_sync"):
        if bool(getattr(args, flag, False)):
            command.append("--" + flag.replace("_", "-"))
    if int(getattr(args, "jobs", 4)) != 4:
        command.extend(["--jobs", str(args.jobs)])
    if int(getattr(args, "task_attempts", 2)) != 2:
        command.extend(["--task-attempts", str(args.task_attempts)])
    command.extend(["--coordinator-host", str(getattr(args, "coordinator_host", None) or coordinator_host)])
    command.extend(["--remote-timeout-seconds", str(args.remote_timeout_seconds)])
    if getattr(args, "notify_command", None):
        command.extend(["--notify-command", str(args.notify_command)])
    if getattr(args, "task_complete_command", None):
        command.extend(["--task-complete-command", str(args.task_complete_command)])

def append_start_options(command: list[str], args: argparse.Namespace, *, coordinator_host: str) -> None:
    append_deploy_options(command, args, coordinator_host=coordinator_host)
    for flag in ("wait", "merge_force", "newline_between"):
        if bool(getattr(args, flag, False)):
            command.append("--" + flag.replace("_", "-"))
    if getattr(args, "verify", None) is True:
        command.append("--verify")
    elif getattr(args, "verify", None) is False:
        command.append("--no-verify")
    if getattr(args, "merge", None):
        command.extend(["--merge", str(args.merge)])
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    command.extend(["--interval-seconds", str(args.interval_seconds)])
    command.extend(["--timeout-seconds", str(args.timeout_seconds)])
    if getattr(args, "fail_on_fail", True) is False:
        command.append("--no-fail-on-fail")

def run_launch_on_coordinator(command: list[str], coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    if not host:
        return 0
    result = interruptible_coordinator_command(host, runs_path, command)
    print(f"coordinator: {coordinator_runs}")
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode

def deploy_on_coordinator(args: argparse.Namespace, coordinator_runs: str) -> int | None:
    host, _ = parse_coordinator(coordinator_runs)
    if not host:
        return None
    temporary_paths: list[str] = []
    try:
        remote_manifest = copy_file_to_coordinator(host, expand_path(args.manifest))
        temporary_paths.append(remote_manifest)
        remote_workers = coordinator_workers_arg(host, str(args.workers))
        if remote_workers.startswith("/tmp/forge."):
            temporary_paths.append(remote_workers)
        command = ["deploy", remote_workers, remote_manifest]
        append_deploy_options(command, args, coordinator_host=host)
        return run_launch_on_coordinator(command, coordinator_runs)
    finally:
        remove_coordinator_temp_files(host, temporary_paths)

def start_on_coordinator(args: argparse.Namespace, coordinator_runs: str) -> int | None:
    host, _ = parse_coordinator(coordinator_runs)
    if not host:
        return None
    remember_run_for_web(
        planned_run_name(expand_path(args.manifest), getattr(args, "run", None)),
        coordinator_runs=coordinator_runs,
    )
    temporary_paths: list[str] = []
    try:
        remote_manifest = copy_file_to_coordinator(host, expand_path(args.manifest))
        temporary_paths.append(remote_manifest)
        remote_workers = coordinator_workers_arg(host, str(args.workers))
        if remote_workers.startswith("/tmp/forge."):
            temporary_paths.append(remote_workers)
        command = ["start", remote_manifest, "--workers", remote_workers]
        append_start_options(command, args, coordinator_host=host)
        return run_launch_on_coordinator(command, coordinator_runs)
    finally:
        remove_coordinator_temp_files(host, temporary_paths)

def run_coordinator_command(coordinator_runs: str, command: list[str]) -> subprocess.CompletedProcess[str]:
    host, runs_path = parse_coordinator(coordinator_runs)
    if host:
        remote = (
            "command -v forge >/dev/null 2>&1 || "
            "{ echo 'forge not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(COLOR_ENV)}=never "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"forge {shell_join(command)}"
        )
        return subprocess.run(
            ["ssh", *SSH_OPTS, host, remote],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    env = os.environ.copy()
    env[COORDINATOR_RUNS_ENV] = ""
    env[COLOR_ENV] = "never"
    env[RUNS_DIR_ENV] = runs_path
    return subprocess.run(
        [sys.executable, str(script_path()), *command],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        env=env,
    )

def single_active_run_name(summaries: list[dict[str, Any]], *, source: str = "") -> str:
    active = [
        status_summary_from_payload(summary)
        for summary in summaries
        if isinstance(summary, dict) and run_current(status_summary_from_payload(summary))
    ]
    qualifier = f" {source}" if source else ""
    if not active:
        raise SystemExit(f"no active Forge run found{qualifier}")
    if len(active) > 1:
        names = ", ".join(str(summary["run"]) for summary in sorted(active, key=lambda item: str(item["run"])))
        raise SystemExit(f"multiple active Forge runs found{qualifier}; specify one: {names}")
    return str(active[0]["run"])

def single_active_local_run_name(*, lease_seconds: int) -> str:
    return single_active_run_name(load_run_summaries(lease_seconds=lease_seconds))

def single_active_coordinator_run_name(coordinator_runs: str, *, lease_seconds: int) -> str:
    result = run_coordinator_command(
        coordinator_runs,
        ["status", "--json", "--lease-seconds", str(lease_seconds)],
    )
    if result.returncode not in (0, 1):
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        raise SystemExit(result.returncode)
    try:
        payload = normalize_status_json_payload(json.loads(result.stdout))
    except json.JSONDecodeError as exc:
        raise SystemExit("coordinator returned invalid status JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("runs"), list):
        raise SystemExit("coordinator returned unexpected status JSON")
    return single_active_run_name(payload["runs"], source="on coordinator")

def worker_control_command_args(action: str, args: argparse.Namespace) -> list[str]:
    command = ["worker", action, str(args.worker)]
    if args.run is not None:
        command.append(str(args.run))
    if action in ("add", "set"):
        if bool(getattr(args, "wait", False)):
            command.append("--wait")
        if getattr(args, "verify", None) is True:
            command.append("--verify")
        elif getattr(args, "verify", None) is False:
            command.append("--no-verify")
        if getattr(args, "merge", None):
            command.extend(["--merge", str(args.merge)])
        if bool(getattr(args, "merge_force", False)):
            command.append("--merge-force")
        if bool(getattr(args, "newline_between", False)):
            command.append("--newline-between")
        if getattr(args, "notify_command", None):
            command.extend(["--notify-command", str(args.notify_command)])
        if getattr(args, "task_complete_command", None):
            command.extend(["--task-complete-command", str(args.task_complete_command)])
        if bool(getattr(args, "task_complete_sync", False)):
            command.append("--task-complete-sync")
        if bool(getattr(args, "no_color", False)):
            command.append("--no-color")
        if bool(getattr(args, "verbose", False)):
            command.append("--verbose")
        if int(getattr(args, "jobs", 4)) != 4:
            command.extend(["--jobs", str(args.jobs)])
        if int(getattr(args, "task_attempts", 2)) != 2:
            command.extend(["--task-attempts", str(args.task_attempts)])
        if getattr(args, "coordinator_host", None):
            command.extend(["--coordinator-host", str(args.coordinator_host)])
        if float(getattr(args, "remote_timeout_seconds", 1800.0)) != 1800.0:
            command.extend(["--remote-timeout-seconds", str(args.remote_timeout_seconds)])
        if float(getattr(args, "interval_seconds", 30.0)) != 30.0:
            command.extend(["--interval-seconds", str(args.interval_seconds)])
        if float(getattr(args, "timeout_seconds", 0.0)) != 0.0:
            command.extend(["--timeout-seconds", str(args.timeout_seconds)])
        if not bool(getattr(args, "fail_on_fail", True)):
            command.append("--no-fail-on-fail")
    elif bool(getattr(args, "keep_claims", False)):
        command.append("--keep-claims")
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    return command

def run_worker_control_on_coordinator(action: str, args: argparse.Namespace, coordinator_runs: str) -> int:
    if args.run is None:
        args = copy.copy(args)
        args.run = single_active_coordinator_run_name(
            coordinator_runs,
            lease_seconds=int(args.lease_seconds),
        )
    host, runs_path = parse_coordinator(coordinator_runs)
    command = worker_control_command_args(action, args)
    if host:
        result = interruptible_coordinator_command(host, runs_path, command)
    else:
        env = os.environ.copy()
        env[COORDINATOR_RUNS_ENV] = ""
        env[RUNS_DIR_ENV] = runs_path
        result = subprocess.run(
            [sys.executable, str(script_path()), *command],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=env,
        )
    print(f"coordinator: {coordinator_runs}")
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode

def cancel_remote_coordinator_process(host: str, pid_file: str) -> None:
    cleanup = (
        f"pid_file={shlex.quote(pid_file)}; "
        "if test -r \"$pid_file\"; then "
        "pid=$(cat \"$pid_file\"); kill -INT \"$pid\" 2>/dev/null || true; "
        "i=0; while kill -0 \"$pid\" 2>/dev/null && test \"$i\" -lt 20; do "
        "sleep 1; i=$((i + 1)); done; "
        "if kill -0 \"$pid\" 2>/dev/null; then kill -TERM \"$pid\" 2>/dev/null || true; sleep 2; fi; "
        "if kill -0 \"$pid\" 2>/dev/null; then kill -KILL \"$pid\" 2>/dev/null || true; fi; "
        "fi; rm -f \"$pid_file\""
    )
    try:
        subprocess.run(
            ["ssh", *SSH_OPTS, host, cleanup],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass

def run_interruptible_coordinator_process(
    host: str,
    remote: str,
    pid_file: str,
) -> subprocess.CompletedProcess[str]:
    command = ["ssh", *SSH_OPTS, host, remote]
    proc = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate()
    except KeyboardInterrupt:
        cancel_remote_coordinator_process(host, pid_file)
        try:
            proc.communicate(timeout=REMOTE_PROCESS_STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            ProcessTracker._terminate(proc, signal.SIGTERM)
            try:
                proc.communicate(timeout=REMOTE_PROCESS_STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                ProcessTracker._terminate(proc, signal.SIGKILL)
                proc.communicate()
        raise
    return subprocess.CompletedProcess(command, proc.returncode, stdout=stdout, stderr=stderr)

def cmd_run_job(args: argparse.Namespace) -> int:
    if not bool(getattr(args, "attach_existing", False)):
        delegated = start_on_coordinator(args, configured_coordinator_runs() or "")
        if delegated is not None:
            return delegated

    template_path = expand_path(args.manifest)
    _run_dir, manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    manifest_existed = manifest_path.exists()
    try:
        return run_local_job(args)
    except KeyboardInterrupt:
        owns_run = (
            bool(getattr(args, "resume", False))
            or bool(getattr(args, "replace", False))
            or not manifest_existed
        )
        if owns_run and manifest_path.exists():
            manifest = load_manifest(manifest_path)
            if not stop_request_path(manifest).exists():
                cleanup = request_stop_for_run(
                    manifest_path,
                    manifest,
                    lease_seconds=int(args.lease_seconds),
                    reason="start interrupted",
                )
                print(
                    f"start: interrupt cleanup complete killed={cleanup['killed']} "
                    f"released={len(cleanup['released'])} reset={len(cleanup['reset'])}",
                    file=sys.stderr,
                    flush=True,
                )
        raise

def run_local_job(args: argparse.Namespace) -> int:
    label = str(getattr(args, "command_label", "start"))
    template_path = expand_path(args.manifest)
    run_dir, manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    run_name = run_dir.name
    explicit_verify = getattr(args, "verify", None)
    should_wait = bool(getattr(args, "wait", False) or getattr(args, "merge", None) or explicit_verify is True)
    should_verify = bool((should_wait and explicit_verify is not False) or getattr(args, "merge", None))
    ephemeral = bool(getattr(args, "ephemeral", False))
    remember_run_for_web(run_name, coordinator_runs=configured_coordinator_runs())

    deploy_args = argparse.Namespace(
        workers=args.workers,
        worker_defaults=getattr(args, "worker_defaults", None),
        worker_snapshot_payload=getattr(args, "worker_snapshot_payload", None),
        preserve_workers_snapshot=bool(getattr(args, "preserve_workers_snapshot", False)),
        worker_snapshot_commit_payload=getattr(args, "worker_snapshot_commit_payload", None),
        launch_workers_payload=getattr(args, "launch_workers_payload", None),
        launch_only=True,
        manifest=args.manifest,
        jobs=args.jobs,
        run=args.run,
        comment=getattr(args, "comment", None),
        resume=args.resume,
        attach_existing=bool(getattr(args, "attach_existing", False)),
        replace=args.replace,
        coordinator_host=args.coordinator_host,
        remote_timeout_seconds=args.remote_timeout_seconds,
        retry_failed=args.retry_failed,
        task_attempts=getattr(args, "task_attempts", 2),
        notify_command=args.notify_command,
        task_complete_command=getattr(args, "task_complete_command", None),
        task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
        lease_seconds=args.lease_seconds,
        interval_seconds=args.interval_seconds,
        timeout_seconds=args.timeout_seconds,
        fail_on_fail=args.fail_on_fail,
        wait=args.wait,
        verify=args.verify,
        merge=args.merge,
        merge_force=args.merge_force,
        newline_between=args.newline_between,
        queue_foreground=should_wait,
        command_label=label,
        start_wait_monitor=not should_wait,
        defer_completion_notify=should_wait,
        no_color=args.no_color,
        verbose=args.verbose,
    )
    rc = run_command_stage(label, "deploy", cmd_deploy, deploy_args)
    if bool(getattr(deploy_args, "queue_handled", False)):
        return rc
    if rc:
        if not ephemeral:
            print(f"{label}: inspect with: forge status {run_name} --verbose", file=sys.stderr)
        return rc

    if should_wait:
        wait_args = argparse.Namespace(
            manifest=str(manifest_path),
            profile=None,
            lease_seconds=args.lease_seconds,
            interval_seconds=args.interval_seconds,
            timeout_seconds=args.timeout_seconds,
            fail_on_fail=args.fail_on_fail,
            notify_command=args.notify_command,
        )
        rc = run_command_stage(label, "wait", cmd_wait, wait_args)
        print("deploy: tasks complete" if rc == 0 else "deploy: tasks failed")
        if rc:
            print(f"{label}: deploy failed rc={rc}", file=sys.stderr)
            if not ephemeral:
                print(f"{label}: inspect with: forge status {run_name} --verbose", file=sys.stderr)
            return rc

    if should_verify:
        rc = run_command_stage(
            label,
            "verify",
            cmd_verify,
            argparse.Namespace(run=str(manifest_path.parent), skip_requirements=True),
        )
        if rc:
            if not ephemeral:
                print(f"{label}: inspect with: forge status {run_name} --verbose", file=sys.stderr)
            return rc

    if args.merge:
        merge_args = argparse.Namespace(
            run=str(manifest_path.parent),
            output=args.merge,
            force=args.merge_force,
            newline_between=args.newline_between,
            skip_requirements=True,
        )
        rc = run_command_stage(label, "merge", cmd_merge, merge_args)
        if rc:
            if not ephemeral:
                print(f"{label}: inspect with: forge status {run_name} --verbose", file=sys.stderr)
            return rc

    if ephemeral:
        print(f"{label}: ok")
    else:
        print(f"{label}: ok; status: forge status {run_name} --verbose")
    return 0

def materialized_run_paths(run_dir: Path, *, label: str) -> tuple[str, Path, Path]:
    manifest_path = run_dir / RUN_MANIFEST_NAME
    workers_path = run_dir / RUN_WORKERS_NAME
    if not manifest_path.exists():
        raise SystemExit(f"{label}: no materialized Forge run found")
    if not workers_path.exists():
        write_json(workers_path, workers_payload_from_arg(None, default_ok=True))
    return run_dir.name, manifest_path, workers_path

def resume_run_paths(run: str | None, *, lease_seconds: int) -> list[tuple[str, Path, Path]]:
    if run:
        return [materialized_run_paths(run_dir_from_arg(run), label=run)]

    incomplete = [
        summary for summary in load_run_summaries(lease_seconds=lease_seconds)
        if not run_done(summary)
    ]
    if not incomplete:
        raise SystemExit("no incomplete Forge run found")
    if len(incomplete) > 1:
        names = ", ".join(str(summary["run"]) for summary in sorted(incomplete, key=lambda item: str(item["run"])))
        raise SystemExit(f"multiple incomplete Forge runs found; specify one: {names}")
    summary = incomplete[0]
    return [materialized_run_paths(run_dir_for_name(str(summary["run"])), label=str(summary["run"]))]

def worker_run_paths(run: str | None, *, lease_seconds: int) -> list[tuple[str, Path, Path]]:
    if run:
        return [materialized_run_paths(run_dir_from_arg(run), label=run)]
    run_name = single_active_local_run_name(lease_seconds=lease_seconds)
    return [materialized_run_paths(run_dir_for_name(run_name), label=run_name)]

def run_active_task_count(manifest: dict[str, Any], *, lease_seconds: int) -> int:
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    try:
        return int(summary.get("active_count") or 0)
    except (TypeError, ValueError):
        return 0

def run_active_worker_task_count(manifest: dict[str, Any], worker: str, *, lease_seconds: int) -> int:
    count = 0
    for task in manifest.get("tasks") or []:
        state = task_state(manifest, task, lease_seconds=lease_seconds)
        if state not in {"claimed", "running", "stale"}:
            continue
        if status_worker(manifest, task, state) == worker:
            count += 1
    return count

def run_pending_task_count(manifest: dict[str, Any], *, lease_seconds: int) -> int:
    return sum(
        task_state(manifest, task, lease_seconds=lease_seconds) == "pending"
        for task in manifest.get("tasks") or []
    )

def clear_worker_drains(
    manifest: dict[str, Any],
    hosts: list[str],
    *,
    run_name: str,
    label: str = "worker set",
) -> int:
    cleared = 0
    for host in hosts:
        marker = worker_drain_path(manifest, host)
        if not marker.exists():
            continue
        marker.unlink()
        cleared += 1
        print(f"{label}: cleared drain for {run_name} {host}")
    return cleared

def cmd_worker_set(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_worker_control_on_coordinator("set", args, coordinator_runs)
    targets = worker_run_paths(args.run, lease_seconds=args.lease_seconds)
    failed = 0
    for run_name, manifest_path, workers_path in targets:
        set_worker = worker_set_record_from_arg(
            str(args.worker),
            inline_defaults_path=workers_path,
        )
        host = worker_host(set_worker)
        desired_slots = worker_record_slots(set_worker)
        current_payload = read_json(workers_path)
        current_slots = worker_slots_for_host(current_payload, host)
        next_payload = set_worker_payload_slots(current_payload, set_worker)
        delta = desired_slots - current_slots
        manifest = load_manifest(manifest_path)
        active_slots = run_active_worker_task_count(
            manifest,
            host,
            lease_seconds=int(args.lease_seconds),
        )
        pending_tasks = run_pending_task_count(manifest, lease_seconds=int(args.lease_seconds))
        launch_slots = max(0, desired_slots - active_slots) if pending_tasks else 0

        if launch_slots > 0:
            launch_worker = set_worker_record_slots(set_worker, launch_slots)
            launch_payload = workers_payload_from_records([launch_worker])
            launch_workers = worker_inline_arg_for_slots(set_worker, launch_slots)
            clear_worker_drains(manifest, [host], run_name=run_name, label="worker set")
            resume_args = argparse.Namespace(
                manifest=str(manifest_path),
                workers=launch_workers,
                worker_defaults=str(workers_path),
                worker_snapshot_payload=None,
                preserve_workers_snapshot=True,
                worker_snapshot_commit_payload=next_payload,
                launch_workers_payload=launch_payload,
                launch_only=True,
                jobs=args.jobs,
                run=run_name,
                resume=False,
                attach_existing=True,
                replace=False,
                retry_failed=False,
                task_attempts=getattr(args, "task_attempts", 2),
                coordinator_host=args.coordinator_host,
                remote_timeout_seconds=args.remote_timeout_seconds,
                wait=args.wait,
                lease_seconds=args.lease_seconds,
                interval_seconds=args.interval_seconds,
                timeout_seconds=args.timeout_seconds,
                fail_on_fail=args.fail_on_fail,
                verify=args.verify,
                merge=args.merge,
                merge_force=args.merge_force,
                newline_between=args.newline_between,
                notify_command=args.notify_command,
                task_complete_command=getattr(args, "task_complete_command", None),
                task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
                no_color=args.no_color,
                verbose=args.verbose,
                command_label="worker set",
            )
            failed += 1 if cmd_run_job(resume_args) else 0
            continue

        write_json(workers_path, next_payload)
        if delta >= 0:
            if delta > 0:
                print(f"worker set: {run_name} {host} slots={current_slots}->{desired_slots}; no pending tasks")
                continue
            print(f"worker set: {run_name} {host} slots={desired_slots} unchanged")
            continue

        release_count = 0 if desired_slots <= 0 else max(0, active_slots - desired_slots)
        released = release_worker_claims(
            manifest,
            worker=host,
            count=release_count,
            lease_seconds=args.lease_seconds,
        )
        if desired_slots <= 0:
            drain_dir(manifest).mkdir(parents=True, exist_ok=True)
            write_json(worker_drain_path(manifest, host), {
                "schema": SCHEMA,
                "run": run_name,
                "worker": host,
                "requested_at": now(),
                "requested_epoch": epoch(),
                "host": socket.gethostname(),
            })
        print(f"worker set: {run_name} {host} slots={current_slots}->{desired_slots} released={len(released)}")
    return 1 if failed else 0

def cmd_resume(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_control_on_coordinator("resume", args, coordinator_runs)
    targets = resume_run_paths(args.run, lease_seconds=args.lease_seconds)
    failed = 0
    for run_name, manifest_path, workers_path in targets:
        manifest = load_manifest(manifest_path)
        active_count = run_active_task_count(manifest, lease_seconds=int(args.lease_seconds))
        if active_count > 0 and args.workers:
            raise SystemExit(
                f"resume: {run_name} already has {active_count} active/claimed/stale task(s); "
                "use worker set HOST:N to change capacity"
            )
        if active_count > 0:
            clear_restart_requests(manifest, run_name, label="resume")
            print(f"resume: {run_name} already active ({active_count} task(s)); no workers launched")
            continue
        clear_resume_markers(manifest, run_name, label="resume")
        resume_args = argparse.Namespace(
            manifest=str(manifest_path),
            workers=str(args.workers) if args.workers else str(workers_path),
            jobs=args.jobs,
            run=run_name,
            resume=True,
            replace=False,
            retry_failed=False,
            task_attempts=getattr(args, "task_attempts", 2),
            coordinator_host=args.coordinator_host,
            remote_timeout_seconds=args.remote_timeout_seconds,
            wait=args.wait,
            lease_seconds=args.lease_seconds,
            interval_seconds=args.interval_seconds,
            timeout_seconds=args.timeout_seconds,
            fail_on_fail=args.fail_on_fail,
            verify=args.verify,
            merge=args.merge,
            merge_force=args.merge_force,
            newline_between=args.newline_between,
            notify_command=args.notify_command,
            task_complete_command=getattr(args, "task_complete_command", None),
            task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
            no_color=args.no_color,
            verbose=args.verbose,
            command_label="resume",
        )
        failed += 1 if cmd_run_job(resume_args) else 0
    return 1 if failed else 0

def retry_run_paths(run: str | None, *, lease_seconds: int) -> list[tuple[str, Path, Path]]:
    if run:
        return [materialized_run_paths(run_dir_from_arg(run), label=run)]

    failed = [
        summary for summary in load_run_summaries(lease_seconds=lease_seconds)
        if summary["counts"]["fail"] > 0 and not run_done(summary)
    ]
    if not failed:
        raise SystemExit("no failed Forge run found")
    return [
        materialized_run_paths(run_dir_for_name(str(summary["run"])), label=str(summary["run"]))
        for summary in sorted(failed, key=lambda item: str(item["run"]))
    ]

def cmd_retry(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_control_on_coordinator("retry", args, coordinator_runs)
    targets = retry_run_paths(args.run, lease_seconds=args.lease_seconds)
    failed = 0
    for index, (run_name, manifest_path, workers_path) in enumerate(targets):
        if len(targets) > 1:
            print(f"retry: {run_name} ({index + 1}/{len(targets)})")
        manifest = load_manifest(manifest_path)
        active_count = run_active_task_count(manifest, lease_seconds=int(args.lease_seconds))
        if active_count > 0 and args.workers:
            raise SystemExit(
                f"retry: {run_name} already has {active_count} active/claimed/stale task(s); "
                "retry without --workers to requeue failures, or use worker set HOST:N to change capacity"
            )
        if active_count == 0:
            manifest = prepare_resume_manifest(manifest_path, manifest, label="retry")
        if args.workers:
            clear_resume_markers(manifest, run_name, label="retry")
        else:
            clear_restart_requests(manifest, run_name, label="retry")
        archived = archive_failed_tasks_for_retry(manifest, run_name)
        if not args.workers:
            print(f"retry: {run_name} requeued={len(archived)}; no workers launched")
            continue
        retry_args = argparse.Namespace(
            manifest=str(manifest_path),
            workers=str(args.workers),
            jobs=args.jobs,
            run=run_name,
            resume=True,
            replace=False,
            retry_failed=True,
            task_attempts=getattr(args, "task_attempts", 2),
            coordinator_host=args.coordinator_host,
            remote_timeout_seconds=args.remote_timeout_seconds,
            wait=args.wait,
            lease_seconds=args.lease_seconds,
            interval_seconds=args.interval_seconds,
            timeout_seconds=args.timeout_seconds,
            fail_on_fail=args.fail_on_fail,
            verify=args.verify,
            merge=args.merge,
            merge_force=args.merge_force,
            newline_between=args.newline_between,
            notify_command=args.notify_command,
            task_complete_command=getattr(args, "task_complete_command", None),
            task_complete_sync=bool(getattr(args, "task_complete_sync", False)),
            no_color=args.no_color,
            verbose=args.verbose,
            command_label="retry",
        )
        failed += 1 if cmd_run_job(retry_args) else 0
    return 1 if failed else 0

def cmd_worker_del(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_worker_control_on_coordinator("del", args, coordinator_runs)
    worker, _profile, _slots = parse_inline_worker_spec(str(args.worker))
    targets = worker_run_paths(args.run, lease_seconds=args.lease_seconds)
    for run_name, manifest_path, workers_path in targets:
        manifest = load_manifest(manifest_path)
        drain_dir(manifest).mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": SCHEMA,
            "run": run_name,
            "worker": worker,
            "requested_at": now(),
            "requested_epoch": epoch(),
            "host": socket.gethostname(),
        }
        write_json(worker_drain_path(manifest, worker), payload)
        released: list[str] = []
        if not bool(getattr(args, "keep_claims", False)):
            released = release_worker_claims(
                manifest,
                worker=worker,
                count=0,
                lease_seconds=args.lease_seconds,
            )
        next_payload, removed = remove_worker_from_payload(read_json(workers_path), worker)
        if removed:
            write_json(workers_path, next_payload)
        print(f"worker del: {run_name} {worker} released={len(released)}")
    return 0
