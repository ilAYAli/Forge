from __future__ import annotations

from .config import default_coordinator_host, default_notify_command
from .core import *
from .manifest import *
from .tasks import *
from .status import *


DEFAULT_INLINE_WORKER_SLOTS = 8
SSH_CONFIG_TIMEOUT_SECONDS = 5.0
DEPLOY_STARTUP_TIMEOUT_SECONDS = 20.0
DEPLOY_STARTUP_POLL_SECONDS = 0.5
_SSH_CONFIG_HOSTNAME_CACHE: dict[str, str | None] = {}


def update_worker_label(worker: dict[str, Any]) -> str:
    return str(worker.get("name") or worker.get("host") or "local")

def update_worker_key(worker: dict[str, Any]) -> tuple[Any, ...]:
    if is_local_worker(worker):
        return ("local", str(worker.get("host") or "localhost"))
    return ("remote", str(worker.get("host") or ""))

def worker_host(worker: dict[str, Any]) -> str:
    return str(worker.get("host") or "").strip()

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
    candidates = [
        runs_dir() / RUN_WORKERS_NAME,
        expand_path(RUN_WORKERS_NAME),
    ]
    for path in candidates:
        if path.exists():
            return path
    raise SystemExit(
        f"no workers file found; pass workers path or create {runs_dir() / RUN_WORKERS_NAME}"
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
    if payload.get("schema") not in (None, "crucible.workers.v1"):
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
        "schema": "crucible.workers.v1",
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
        "bin=$(command -v crucible); "
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
        "ln -sf \"$repo/crucible.py\" \"$HOME/.local/bin/crucible\"; "
        "crucible version"
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
    if payload.get("schema") not in (None, "crucible.workers.v1"):
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
        "schema": "crucible.workers.v1",
        "workers": records,
    }

def workers_from_payload(payload: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    workers: list[dict[str, Any]] = []
    for worker in worker_records_from_payload(payload, label=label):
        workers.extend(expand_worker_record(worker))
    if not workers:
        raise SystemExit(f"{label}: no enabled workers")
    return workers

def merge_worker_payloads(base_payload: dict[str, Any], update_payload: dict[str, Any]) -> dict[str, Any]:
    merged: list[dict[str, Any]] = []
    positions: dict[str, int] = {}
    for worker in worker_records_from_payload(base_payload, label="base workers", allow_empty=True):
        host = worker_host(worker)
        if host in positions:
            merged[positions[host]] = worker
        else:
            positions[host] = len(merged)
            merged.append(worker)
    for worker in worker_records_from_payload(update_payload, label="updated workers", allow_empty=True):
        host = worker_host(worker)
        if host in positions:
            merged[positions[host]] = worker
        else:
            positions[host] = len(merged)
            merged.append(worker)
    return workers_payload_from_records(merged)

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
        return str(Path.home() / ".cache/crucible")
    return "/tmp/crucible-cache"

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

def copy_dir_to_worker(
    worker: dict[str, Any],
    src: Path,
    dst: str,
    *,
    timeout: float | None = None,
    tracker: ProcessTracker | None = None,
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
    )
    if mkdir.returncode != 0:
        raise RuntimeError(mkdir.stderr.strip() or mkdir.stdout.strip() or f"failed to create {dst}")
    command = [
        "rsync", "-a", "--delete", "-e", RSYNC_SSH,
        str(src) + "/",
        worker_rsync_target(worker, dst.rstrip("/") + "/"),
    ]
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
    wrapper = script_path()
    copy_to_worker(worker, wrapper, worker_cache_file(worker, "crucible.py"), timeout=timeout, tracker=tracker)
    copy_dir_to_worker(
        worker,
        wrapper.parent / "crucible_lib",
        worker_cache_file(worker, "crucible_lib"),
        timeout=timeout,
        tracker=tracker,
    )
    copy_to_worker(worker, manifest_path, worker_cache_file(worker, "manifest.json"), timeout=timeout, tracker=tracker)

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
    )
    return result.returncode == 0

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
            if src.resolve() == target.resolve() or local_input_matches(target, remote_item):
                continue
            copy_to_worker(worker, src, dst, timeout=timeout, tracker=tracker)
            synced += 1
            continue
        if worker_is_current_host(worker) and src.resolve() == expand_path(dst).resolve():
            continue
        if worker_input_matches(worker, dst, remote_item, timeout=timeout, tracker=tracker):
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

def deploy_prereq_command(
    *,
    worker: dict[str, Any],
    manifest: dict[str, Any],
    coordinator: str | None = None,
) -> str:
    runner = worker_cache_file(worker, "crucible.py")
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
) -> str:
    runner = worker_cache_file(worker, "crucible.py")
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

def cleanup_detached_shell_command(pid_file: str) -> str:
    quoted = shlex.quote(pid_file)
    return (
        f"pid_file={quoted}; "
        "if test -s \"$pid_file\"; then "
        "pid=$(sed -n '1p' \"$pid_file\" 2>/dev/null); "
        "case \"$pid\" in ''|*[!0-9]*) exit 0;; esac; "
        "kill -TERM -\"$pid\" 2>/dev/null || kill -TERM \"$pid\" 2>/dev/null || true; "
        "sleep 1; "
        "kill -KILL -\"$pid\" 2>/dev/null || kill -KILL \"$pid\" 2>/dev/null || true; "
        "rm -f \"$pid_file\"; "
        "fi"
    )

def start_wait_monitor(
    *,
    run_dir: Path,
    manifest_path: Path,
    args: argparse.Namespace,
    notify_command: str | None,
) -> int | None:
    if not notify_command:
        return None
    command = [
        sys.executable,
        str(script_path()),
        "wait",
        "--manifest", str(manifest_path),
        "--lease-seconds", str(getattr(args, "lease_seconds", 600)),
        "--interval-seconds", str(getattr(args, "interval_seconds", 30.0)),
        "--timeout-seconds", str(getattr(args, "timeout_seconds", 0.0)),
        "--notify-command", notify_command,
    ]
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

def coordinator_host_for_workers(workers: list[dict[str, Any]], coordinator_host: str | None = None) -> str:
    if coordinator_host:
        return coordinator_host
    for worker in workers:
        if worker_targets_current_host(worker):
            return worker_host(worker)
    return socket.gethostname()

def coordinator_for_worker(worker: dict[str, Any], manifest_path: Path, coordinator_host: str | None = None) -> str:
    if worker_targets_current_host(worker):
        return str(manifest_path)
    host = coordinator_host or socket.gethostname()
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

def materialize_run(
    *,
    template_path: Path,
    workers_arg: str,
    inline_worker_defaults: str | Path | None = None,
    workers_snapshot_payload: dict[str, Any] | None = None,
    preserve_workers_snapshot: bool = False,
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
        shutil.rmtree(run_dir)

    run_dir.mkdir(parents=True)
    materialized = materialize_manifest(template, run_name=run_name, run_dir=run_dir)
    state_dir(materialized).mkdir(parents=True)
    log_dir(materialized).mkdir(parents=True)
    claim_dir(materialized).mkdir(parents=True)
    (run_dir / "outputs").mkdir(parents=True)
    write_json(manifest_path, materialized)
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

def cmd_deploy(args: argparse.Namespace) -> int:
    started_at = time.monotonic()
    template_path = expand_path(args.manifest)
    run_dir, manifest_path, workers_path = materialize_run(
        template_path=template_path,
        workers_arg=str(args.workers),
        inline_worker_defaults=getattr(args, "worker_defaults", None),
        workers_snapshot_payload=getattr(args, "worker_snapshot_payload", None),
        preserve_workers_snapshot=bool(getattr(args, "preserve_workers_snapshot", False)),
        run_name_arg=getattr(args, "run", None),
        resume=bool(getattr(args, "resume", False)),
        replace=bool(getattr(args, "replace", False)),
    )
    launch_workers_payload = getattr(args, "launch_workers_payload", None)
    workers = (
        workers_from_payload(launch_workers_payload, label=str(args.workers))
        if launch_workers_payload is not None
        else load_workers(workers_path)
    )
    jobs = max(1, int(args.jobs))
    remote_timeout = max(1.0, float(args.remote_timeout_seconds))
    manifest = load_manifest(manifest_path)
    has_tasks = bool(manifest.get("tasks"))
    coordinator_host = coordinator_host_for_workers(
        workers,
        getattr(args, "coordinator_host", None) or default_coordinator_host(),
    )
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
            coordinator = (
                coordinator_for_worker(worker, manifest_path, coordinator_host)
                if has_tasks else None
            )
            script = deploy_prereq_command(worker=worker, manifest=manifest, coordinator=coordinator)
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

        try:
            result = run_worker_setup(worker)
        except BaseException as exc:
            result = subprocess.CompletedProcess(
                ["deploy", str(worker.get("host", "worker"))],
                1,
                stdout="",
                stderr=f"internal setup error: {type(exc).__name__}: {exc}",
            )
        with setup_lock:
            setup_results[key] = result
            setup_lock.notify_all()
        return result

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

    def setup_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))
        prereq = ensure_worker_setup(worker)
        if prereq.returncode != 0:
            verbose_status(f"{label}: prerequisites FAILED rc={prereq.returncode}", error=True)
        else:
            verbose_status(f"{label}: prerequisites OK")
        return result_payload(index, worker, "setup", prereq)

    def run_worker(index: int, worker: dict[str, Any]) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
        label = worker_label(host, profile, worker.get("_slot"), worker.get("_slots"))
        verbose_status(f"{label}: running tasks")
        script = deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator=coordinator_for_worker(worker, manifest_path, coordinator_host),
            retry_failed=bool(getattr(args, "retry_failed", False)),
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
            coordinator=coordinator_for_worker(worker, manifest_path, coordinator_host),
            retry_failed=bool(getattr(args, "retry_failed", False)),
        )
        result = run_worker_shell(
            worker,
            detached_shell_command(worker, script, pid_file=pid_file, log_file=log_file),
            timeout=remote_timeout,
            tracker=tracker,
        )
        if result.returncode == 0:
            verbose_status(f"{label}: launch OK")
            return result_payload(index, worker, "launch", result, {"pid_file": pid_file, "log_file": log_file})
        verbose_status(f"{label}: launch FAILED rc={result.returncode}", error=True)
        return result_payload(index, worker, "launch", result, {"pid_file": pid_file, "log_file": log_file})

    def cleanup_launched_workers(items: list[dict[str, Any]]) -> None:
        for item in items:
            if item.get("stage") != "launch" or not item.get("pid_file"):
                continue
            worker = workers[int(item["index"])]
            try:
                run_worker_shell(
                    worker,
                    cleanup_detached_shell_command(str(item["pid_file"])),
                    timeout=10.0,
                    tracker=tracker,
                )
            except Exception as exc:
                verbose_status(f"{worker_label(str(item['host']), str(item['profile']), item.get('slot'), item.get('slots'))}: cleanup failed: {exc}", error=True)

    def cleanup_failed_launch(items: list[dict[str, Any]], reason: str) -> None:
        payload = {
            "schema": SCHEMA,
            "run": run_dir.name,
            "requested_at": now(),
            "requested_epoch": epoch(),
            "host": socket.gethostname(),
            "reason": reason,
        }
        write_json(stop_request_path(manifest), payload)
        release_active_claims(manifest, lease_seconds=600)
        cleanup_launched_workers(items)

    def launch_started_enough(expected_started: int) -> bool:
        if expected_started <= 0:
            return True
        deadline = time.monotonic() + DEPLOY_STARTUP_TIMEOUT_SECONDS
        while True:
            summary = status_summary(manifest, lease_seconds=600)
            counts = summary["counts"]
            if counts["fail"] > 0:
                return False
            started = counts["done"] + counts["running"] + counts["claimed"] + counts["stale"]
            if started >= expected_started:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(DEPLOY_STARTUP_POLL_SECONDS)

    def print_results(items: list[dict[str, Any]]) -> None:
        for item in sorted(items, key=lambda entry: int(entry["index"])):
            ok = bool(item["ok"])
            marker_text = "OK" if ok else "FAILED"
            marker_color = "green" if ok else "red"
            marker = colorize(f"{marker_text:<6}", marker_color, enabled=color_enabled)
            suffix = "" if ok else f" rc={item['rc']}"
            status(
                f"  {marker} "
                f"{worker_label(str(item['host']), str(item['profile']), item.get('slot'), item.get('slots'))}: "
                f"{item['stage']}{suffix}"
            )
            output = "\n".join(
                part.strip()
                for part in (str(item.get("stdout") or ""), str(item.get("stderr") or ""))
                if part.strip()
            )
            if output and (args.verbose or not ok):
                for line in output.splitlines():
                    status(f"         {line}")

    status(colorize(f"deploy: run={run_dir.name} workers={len(workers)} jobs={jobs}", "cyan", enabled=color_enabled))
    task_count = len(manifest.get("tasks") or [])
    if has_tasks and len(workers) > 1 and task_count < len(workers) * 4:
        status(
            colorize(
                f"deploy: warning: {task_count} task(s) for {len(workers)} worker slot(s); "
                "long tasks balance better with at least 4x more tasks than slots",
                "yellow",
                enabled=color_enabled,
            ),
            error=True,
        )
    executor: concurrent.futures.ThreadPoolExecutor | None = concurrent.futures.ThreadPoolExecutor(max_workers=jobs)
    interrupted = False
    startup_failed = False
    futures: list[concurrent.futures.Future[dict[str, Any]]] = []
    try:
        futures = [
            executor.submit(setup_worker, index, worker)
            for index, worker in enumerate(workers)
        ]
        setup_results_list = [future.result() for future in concurrent.futures.as_completed(futures)]
        print_results(setup_results_list)
        setup_failed = sum(1 for item in setup_results_list if not item["ok"])
        if setup_failed or not has_tasks:
            results = setup_results_list
        else:
            executor.shutdown(wait=True)
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(workers)))
            if bool(getattr(args, "launch_only", False)):
                status(colorize("deploy: setup complete; launching workers", "green", enabled=color_enabled))
                before_launch = status_summary(manifest, lease_seconds=600)
                before_counts = before_launch["counts"]
                before_started = (
                    before_counts["done"]
                    + before_counts["running"]
                    + before_counts["claimed"]
                    + before_counts["stale"]
                )
                expected_started = min(
                    int(before_launch["tasks"]),
                    before_started + min(before_counts["pending"], len(workers)),
                )
                futures = [
                    executor.submit(launch_worker, index, worker)
                    for index, worker in enumerate(workers)
                ]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
                print_results(results)
                if all(item["ok"] for item in results):
                    status(colorize("deploy: verifying workers started", "cyan", enabled=color_enabled))
                    if launch_started_enough(expected_started):
                        status(colorize("deploy: workers active", "green", enabled=color_enabled))
                    else:
                        startup_failed = True
                        status(
                            colorize(
                                "deploy: workers did not become active; stopping launched workers",
                                "red",
                                enabled=color_enabled,
                            ),
                            error=True,
                        )
                        cleanup_failed_launch(results, "workers did not become active after launch")
            else:
                status(colorize("deploy: setup complete; starting tasks", "green", enabled=color_enabled))
                futures = [
                    executor.submit(run_worker, index, worker)
                    for index, worker in enumerate(workers)
                ]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
                print_results(results)
    except KeyboardInterrupt:
        interrupted = True
        status(colorize("deploy: interrupted; stopping active worker commands", "yellow", enabled=color_enabled), error=True)
        tracker.terminate_all()
        concurrent.futures.wait(futures, timeout=2)
        for future in futures:
            future.cancel()
        return 130
    finally:
        if executor is not None:
            executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    failed = sum(1 for item in results if not item["ok"]) + (1 if startup_failed else 0)
    if not has_tasks:
        status(colorize("deploy: setup complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: setup failed", "red", enabled=color_enabled))
    elif any(item["stage"] == "setup" for item in results):
        status(colorize("deploy: setup failed; no tasks started", "red", enabled=color_enabled))
    elif bool(getattr(args, "launch_only", False)):
        status(colorize("deploy: launch complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: launch failed", "red", enabled=color_enabled))
    else:
        status(colorize("deploy: tasks complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: tasks failed", "red", enabled=color_enabled))
    notify_command = getattr(args, "notify_command", None) or default_notify_command()
    if notify_command:
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
    if failed == 0 and has_tasks and bool(getattr(args, "launch_only", False)):
        monitor_pid = start_wait_monitor(
            run_dir=run_dir,
            manifest_path=manifest_path,
            args=args,
            notify_command=notify_command,
        )
        if monitor_pid:
            status(colorize(f"deploy: wait monitor pid={monitor_pid}", "green", enabled=color_enabled))
    commit_payload = getattr(args, "worker_snapshot_commit_payload", None)
    if failed == 0 and commit_payload is not None:
        write_json(workers_path, commit_payload)
    return 1 if failed else 0

def planned_run_paths(template_path: Path, run_name_arg: str | None) -> tuple[Path, Path]:
    template = load_manifest(template_path)
    run_name = validate_run_name(run_name_arg) if run_name_arg else default_run_name(template_path, template)
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

def run_control_on_coordinator(name: str, args: argparse.Namespace, coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = run_control_command_args(name, args)
    if host:
        remote = (
            "command -v crucible >/dev/null 2>&1 || "
            "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"crucible {shell_join(command)}"
        )
        result = subprocess.run(
            ["ssh", *SSH_OPTS, host, remote],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
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

def run_coordinator_command(coordinator_runs: str, command: list[str]) -> subprocess.CompletedProcess[str]:
    host, runs_path = parse_coordinator(coordinator_runs)
    if host:
        remote = (
            "command -v crucible >/dev/null 2>&1 || "
            "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(COLOR_ENV)}=never "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"crucible {shell_join(command)}"
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
        raise SystemExit(f"no active Crucible run found{qualifier}")
    if len(active) > 1:
        names = ", ".join(str(summary["run"]) for summary in sorted(active, key=lambda item: str(item["run"])))
        raise SystemExit(f"multiple active Crucible runs found{qualifier}; specify one: {names}")
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
    if action == "add":
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
        remote = (
            "command -v crucible >/dev/null 2>&1 || "
            "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"crucible {shell_join(command)}"
        )
        result = subprocess.run(
            ["ssh", *SSH_OPTS, host, remote],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
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

def cmd_run_job(args: argparse.Namespace) -> int:
    label = str(getattr(args, "command_label", "start"))
    template_path = expand_path(args.manifest)
    run_dir, manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    run_name = run_dir.name
    explicit_verify = getattr(args, "verify", None)
    should_wait = bool(getattr(args, "wait", False) or getattr(args, "merge", None) or explicit_verify is True)
    should_verify = bool((should_wait and explicit_verify is not False) or getattr(args, "merge", None))

    deploy_args = argparse.Namespace(
        workers=args.workers,
        worker_defaults=getattr(args, "worker_defaults", None),
        worker_snapshot_payload=getattr(args, "worker_snapshot_payload", None),
        preserve_workers_snapshot=bool(getattr(args, "preserve_workers_snapshot", False)),
        worker_snapshot_commit_payload=getattr(args, "worker_snapshot_commit_payload", None),
        launch_workers_payload=getattr(args, "launch_workers_payload", None),
        launch_only=bool(getattr(args, "launch_only", False) or not should_wait),
        manifest=args.manifest,
        jobs=args.jobs,
        run=args.run,
        resume=args.resume,
        replace=args.replace,
        coordinator_host=args.coordinator_host,
        remote_timeout_seconds=args.remote_timeout_seconds,
        retry_failed=args.retry_failed,
        task_attempts=getattr(args, "task_attempts", 2),
        notify_command=args.notify_command,
        lease_seconds=args.lease_seconds,
        interval_seconds=args.interval_seconds,
        timeout_seconds=args.timeout_seconds,
        fail_on_fail=args.fail_on_fail,
        no_color=args.no_color,
        verbose=args.verbose,
    )
    rc = run_command_stage(label, "deploy", cmd_deploy, deploy_args)
    if rc:
        print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
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
        if rc:
            print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
            return rc

    if should_verify:
        rc = run_command_stage(
            label,
            "verify",
            cmd_verify,
            argparse.Namespace(run=str(manifest_path.parent), skip_requirements=True),
        )
        if rc:
            print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
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
            print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
            return rc

    print(f"{label}: ok; status: crucible status {run_name} --verbose")
    return 0

def materialized_run_paths(run_dir: Path, *, label: str) -> tuple[str, Path, Path]:
    manifest_path = run_dir / RUN_MANIFEST_NAME
    workers_path = run_dir / RUN_WORKERS_NAME
    if not manifest_path.exists() or not workers_path.exists():
        raise SystemExit(f"{label}: no materialized Crucible run found")
    return run_dir.name, manifest_path, workers_path

def resume_run_paths(run: str | None, *, lease_seconds: int) -> list[tuple[str, Path, Path]]:
    if run:
        return [materialized_run_paths(run_dir_from_arg(run), label=run)]

    incomplete = [
        summary for summary in load_run_summaries(lease_seconds=lease_seconds)
        if not run_done(summary)
    ]
    if not incomplete:
        raise SystemExit("no incomplete Crucible run found")
    if len(incomplete) > 1:
        names = ", ".join(str(summary["run"]) for summary in sorted(incomplete, key=lambda item: str(item["run"])))
        raise SystemExit(f"multiple incomplete Crucible runs found; specify one: {names}")
    summary = incomplete[0]
    return [materialized_run_paths(run_dir_for_name(str(summary["run"])), label=str(summary["run"]))]

def worker_run_paths(run: str | None, *, lease_seconds: int) -> list[tuple[str, Path, Path]]:
    if run:
        return [materialized_run_paths(run_dir_from_arg(run), label=run)]
    run_name = single_active_local_run_name(lease_seconds=lease_seconds)
    return [materialized_run_paths(run_dir_for_name(run_name), label=run_name)]

def worker_hosts_from_inline(value: str) -> list[str]:
    workers = load_workers_arg(value, default_ok=False)
    hosts = {str(worker.get("host") or "").strip() for worker in workers}
    return sorted(host for host in hosts if host)

def clear_worker_drains(manifest: dict[str, Any], hosts: list[str], *, run_name: str) -> int:
    cleared = 0
    for host in hosts:
        marker = worker_drain_path(manifest, host)
        if not marker.exists():
            continue
        marker.unlink()
        cleared += 1
        print(f"worker add: cleared drain for {run_name} {host}")
    return cleared

def cmd_worker_add(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_worker_control_on_coordinator("add", args, coordinator_runs)
    targets = worker_run_paths(args.run, lease_seconds=args.lease_seconds)
    failed = 0
    for run_name, manifest_path, workers_path in targets:
        add_payload = workers_payload_from_arg(
            str(args.worker),
            default_ok=False,
            inline_defaults_path=workers_path,
        )
        manifest = load_manifest(manifest_path)
        add_payload = workers_payload_for_manifest(add_payload, manifest)
        merged_payload = merge_worker_payloads(read_json(workers_path), add_payload)
        hosts = worker_hosts_from_payload(add_payload)
        clear_worker_drains(manifest, hosts, run_name=run_name)
        resume_args = argparse.Namespace(
            manifest=str(manifest_path),
            workers=str(args.worker),
            worker_defaults=str(workers_path),
            worker_snapshot_payload=None,
            preserve_workers_snapshot=True,
            worker_snapshot_commit_payload=merged_payload,
            launch_workers_payload=add_payload,
            launch_only=True,
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
            no_color=args.no_color,
            verbose=args.verbose,
            command_label="worker add",
        )
        failed += 1 if cmd_run_job(resume_args) else 0
    return 1 if failed else 0

def cmd_resume(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_control_on_coordinator("resume", args, coordinator_runs)
    targets = resume_run_paths(args.run, lease_seconds=args.lease_seconds)
    failed = 0
    for run_name, manifest_path, workers_path in targets:
        manifest = load_manifest(manifest_path)
        stop_path = stop_request_path(manifest)
        if stop_path.exists():
            stop_path.unlink()
            print(f"resume: cleared stop request for {run_name}")
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
        raise SystemExit("no failed Crucible run found")
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
        retry_args = argparse.Namespace(
            manifest=str(manifest_path),
            workers=str(args.workers) if args.workers else str(workers_path),
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
