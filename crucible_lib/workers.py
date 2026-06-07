from __future__ import annotations

from .core import *
from .manifest import *
from .tasks import *
from .status import *


def update_worker_label(worker: dict[str, Any]) -> str:
    return str(worker.get("name") or worker.get("host") or "local")

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

def parse_inline_workers(value: str) -> dict[str, Any]:
    entries = [entry.strip() for entry in value.split(",")]
    if not entries or any(not entry for entry in entries):
        raise SystemExit("inline workers must be a comma-separated host list")
    return {
        "schema": "crucible.workers.v1",
        "workers": entries,
    }

def workers_payload_from_arg(value: str | None, *, default_ok: bool) -> dict[str, Any]:
    if value is None:
        if not default_ok:
            raise SystemExit("workers are required")
        return read_json(default_workers_path())
    path = expand_path(value)
    if path.exists():
        return read_json(path)
    if worker_arg_looks_like_path(value):
        raise SystemExit(f"workers file not found: {value}")
    return parse_inline_workers(value)

def load_workers_arg(value: str | None, *, default_ok: bool = False) -> list[dict[str, Any]]:
    payload = workers_payload_from_arg(value, default_ok=default_ok)
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
    workers = load_workers_arg(args.workers, default_ok=True)
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
    record = dict(worker)
    slots = int(record.pop("slots", 1))
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

def workers_from_payload(payload: dict[str, Any], *, label: str) -> list[dict[str, Any]]:
    if payload.get("schema") not in (None, "crucible.workers.v1"):
        raise SystemExit(f"{label}: unsupported schema {payload.get('schema')!r}")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise SystemExit(f"{label}: workers must be a list")
    workers: list[dict[str, Any]] = []
    for item in raw_workers:
        worker = parse_worker_record(item)
        if worker:
            workers.extend(expand_worker_record(worker))
    if not workers:
        raise SystemExit(f"{label}: no enabled workers")
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
    if manifest_has_git_requirements(manifest):
        commands.append(shell_join([python, runner, "sync-repos", "--manifest", manifest_path, *profile_args]))
    doctor_command = [python, runner, "doctor", "--manifest", manifest_path, *profile_args, "--role", "worker"]
    if coordinator:
        doctor_command.extend(["--coordinator", coordinator])
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

def coordinator_for_worker(worker: dict[str, Any], manifest_path: Path, coordinator_host: str | None = None) -> str:
    if is_local_worker(worker):
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
        write_json(workers_snapshot, workers_payload_from_arg(workers_arg, default_ok=False))
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
    write_json(workers_snapshot, workers_payload_from_arg(workers_arg, default_ok=False))
    return run_dir, manifest_path, workers_snapshot

def cmd_deploy(args: argparse.Namespace) -> int:
    started_at = time.monotonic()
    template_path = expand_path(args.manifest)
    run_dir, manifest_path, workers_path = materialize_run(
        template_path=template_path,
        workers_arg=str(args.workers),
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
            coordinator = (
                coordinator_for_worker(worker, manifest_path, getattr(args, "coordinator_host", None))
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
    ) -> dict[str, Any]:
        host = str(worker["host"])
        profile = str(worker["profile"])
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
            coordinator=coordinator_for_worker(worker, manifest_path, getattr(args, "coordinator_host", None)),
            retry_failed=bool(getattr(args, "retry_failed", False)),
        )
        result = run_worker_shell(worker, script, timeout=remote_timeout, tracker=tracker)
        if result.returncode == 0:
            verbose_status(f"{label}: tasks OK")
            return result_payload(index, worker, "tasks", result)
        verbose_status(f"{label}: tasks FAILED rc={result.returncode}", error=True)
        return result_payload(index, worker, "tasks", result)

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
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=jobs)
    interrupted = False
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
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

    failed = sum(1 for item in results if not item["ok"])
    if not has_tasks:
        status(colorize("deploy: setup complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: setup failed", "red", enabled=color_enabled))
    elif any(item["stage"] == "setup" for item in results):
        status(colorize("deploy: setup failed; no tasks started", "red", enabled=color_enabled))
    else:
        status(colorize("deploy: tasks complete", "green", enabled=color_enabled) if failed == 0 else colorize("deploy: tasks failed", "red", enabled=color_enabled))
    notify_command = getattr(args, "notify_command", None) or os.environ.get("CRUCIBLE_NOTIFY_COMMAND")
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

def cmd_run_job(args: argparse.Namespace) -> int:
    label = str(getattr(args, "command_label", "start"))
    template_path = expand_path(args.manifest)
    run_dir, manifest_path = planned_run_paths(template_path, getattr(args, "run", None))
    run_name = run_dir.name

    deploy_args = argparse.Namespace(
        workers=args.workers,
        manifest=args.manifest,
        jobs=args.jobs,
        run=args.run,
        resume=args.resume,
        replace=args.replace,
        coordinator_host=args.coordinator_host,
        remote_timeout_seconds=args.remote_timeout_seconds,
        retry_failed=args.retry_failed,
        notify_command=args.notify_command,
        no_color=args.no_color,
        verbose=args.verbose,
    )
    rc = run_command_stage(label, "deploy", cmd_deploy, deploy_args)
    if rc:
        print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
        return rc

    if args.wait:
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

    if args.verify or args.merge:
        rc = run_command_stage(label, "verify", cmd_verify, argparse.Namespace(run=str(manifest_path.parent)))
        if rc:
            print(f"{label}: inspect with: crucible status {run_name} --verbose", file=sys.stderr)
            return rc

    if args.merge:
        merge_args = argparse.Namespace(
            run=str(manifest_path.parent),
            output=args.merge,
            force=args.merge_force,
            newline_between=args.newline_between,
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

def cmd_resume(args: argparse.Namespace) -> int:
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
