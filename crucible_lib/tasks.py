from __future__ import annotations

from .core import *
from .manifest import *


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

_STATE_DIAGNOSTICS: dict[str, str] = {}

def clear_state_diagnostics() -> None:
    _STATE_DIAGNOSTICS.clear()

def state_diagnostics() -> list[str]:
    return list(_STATE_DIAGNOSTICS.values())

def record_state_diagnostic(path: Path, detail: str) -> None:
    _STATE_DIAGNOSTICS[str(path)] = f"{path}: {detail}"

def read_state_json(path: Path, *, missing_ok: bool = True) -> dict[str, Any] | None:
    try:
        return read_json(path)
    except OSError as exc:
        if not missing_ok:
            record_state_diagnostic(path, f"state JSON unreadable: {exc}")
        return None
    except SystemExit as exc:
        record_state_diagnostic(path, f"state JSON invalid: {exc}")
        return None

def path_epoch(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0

def marker_is_stale(path: Path, *, lease_seconds: int) -> bool:
    if lease_seconds <= 0:
        return False
    marker_epoch = path_epoch(path)
    return bool(marker_epoch and epoch() - marker_epoch > lease_seconds)

def stop_request_path(manifest: dict[str, Any]) -> Path:
    return state_dir(manifest) / "stop.json"

def stop_requested(manifest: dict[str, Any]) -> bool:
    return stop_request_path(manifest).exists()

def drain_dir(manifest: dict[str, Any]) -> Path:
    return state_dir(manifest) / "drain"

def worker_drain_path(manifest: dict[str, Any], worker: str) -> Path:
    digest = hashlib.sha256(worker.encode("utf-8")).hexdigest()[:16]
    return drain_dir(manifest) / f"{digest}.json"

def worker_drained(manifest: dict[str, Any], worker: str) -> bool:
    return worker_drain_path(manifest, worker).exists()

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
    claim = task_claim_dir(manifest, task)
    if not claim.exists():
        return None
    path = task_claim_path(manifest, task)
    payload = read_state_json(path, missing_ok=False)
    if payload is None:
        if marker_is_stale(claim, lease_seconds=lease_seconds):
            return "stale"
        return "claimed"
    heartbeat = task_heartbeat_path(manifest, task)
    try:
        heartbeat_epoch = float(payload.get("heartbeat_epoch") or payload.get("claimed_epoch") or 0)
    except (TypeError, ValueError):
        record_state_diagnostic(path, "claim heartbeat_epoch is not numeric")
        heartbeat_epoch = 0
    if heartbeat.exists():
        heartbeat_payload = read_state_json(heartbeat)
        if heartbeat_payload is not None:
            try:
                heartbeat_epoch = float(heartbeat_payload.get("heartbeat_epoch") or heartbeat_epoch)
            except (TypeError, ValueError):
                record_state_diagnostic(heartbeat, "heartbeat_epoch is not numeric")
        else:
            try:
                heartbeat_epoch = float(heartbeat_epoch)
            except (TypeError, ValueError):
                heartbeat_epoch = 0
    if lease_seconds > 0 and heartbeat_epoch and epoch() - heartbeat_epoch > lease_seconds:
        return "stale"
    return "claimed"

def task_running_state(manifest: dict[str, Any], task: dict[str, Any], *, lease_seconds: int) -> str | None:
    running = task_running_path(manifest, task)
    if not running.exists():
        return None
    claim_state = task_claim_state(manifest, task, lease_seconds=lease_seconds)
    if claim_state == "stale":
        return "stale"
    if claim_state == "claimed":
        return "running"
    if marker_is_stale(running, lease_seconds=lease_seconds):
        return "stale"
    return "running"

def task_state(manifest: dict[str, Any], task: dict[str, Any], *, lease_seconds: int = 600) -> str:
    if task_complete(manifest, task):
        return "done"
    running_state = task_running_state(manifest, task, lease_seconds=lease_seconds)
    if running_state:
        return running_state
    claim_state = task_claim_state(manifest, task, lease_seconds=lease_seconds)
    if claim_state:
        return claim_state
    if task_fail_path(manifest, task).exists():
        return "fail"
    return "pending"

class TaskSignalInterrupt(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum

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
        running = {
            "task": task["id"],
            "host": socket.gethostname(),
            "started_at": started_at,
            "command": command,
            "pid": os.getpid(),
            "child_pid": proc.pid,
        }
        try:
            running["child_pgid"] = os.getpgid(proc.pid)
        except OSError:
            pass
        write_json(task_running_path(manifest, task), running)

        previous_handlers: dict[int, Any] = {}

        def handle_task_signal(signum: int, _frame: Any) -> None:
            raise TaskSignalInterrupt(signum)

        for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGHUP", None)):
            if sig is None:
                continue
            previous_handlers[int(sig)] = signal.getsignal(sig)
            signal.signal(sig, handle_task_signal)

        timeout = bounded_timeout(task.get("timeout_s", manifest.get("timeout_s")))
        try:
            rc = proc.wait(timeout=timeout) if timeout else proc.wait()
        except TaskSignalInterrupt as exc:
            terminate_process_group(proc, log, f"signal {exc.signum}")
            rc = 128 + exc.signum
        except subprocess.TimeoutExpired:
            terminate_process_group(proc, log, "timeout")
            rc = 124
        except KeyboardInterrupt:
            terminate_process_group(proc, log, "interrupt")
            rc = 130
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
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
    if worker_drained(manifest, worker):
        return None
    claim_dir(manifest).mkdir(parents=True, exist_ok=True)
    digest = manifest_checksum(manifest)
    for task in sorted(manifest["tasks"], key=lambda item: int(item.get("index", 0))):
        if task_complete(manifest, task):
            continue
        if task_fail_path(manifest, task).exists() and not retry_failed:
            continue
        running_state = task_running_state(manifest, task, lease_seconds=lease_seconds)
        if running_state == "running":
            continue
        if running_state == "stale":
            task_running_path(manifest, task).unlink(missing_ok=True)
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
    payload = read_state_json(claim_path, missing_ok=False)
    if payload is None:
        raise SystemExit(f"{task['id']}: invalid claim: {claim_path}")
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
    task_running_path(manifest, task).unlink(missing_ok=True)

def release_active_claims(manifest: dict[str, Any], *, lease_seconds: int) -> list[str]:
    released: list[str] = []
    for task in sorted(manifest["tasks"], key=lambda item: int(item.get("index", 0))):
        if task_state(manifest, task, lease_seconds=lease_seconds) not in {"claimed", "running", "stale"}:
            continue
        release_claim(manifest, task)
        released.append(str(task["id"]))
    return released

def claim_payload_worker(payload: dict[str, Any]) -> str:
    worker = payload.get("worker")
    if isinstance(worker, dict):
        worker = worker.get("host")
    return str(worker or payload.get("host") or "")

def release_worker_claims(
    manifest: dict[str, Any],
    *,
    worker: str,
    count: int,
    lease_seconds: int,
) -> list[str]:
    released: list[str] = []
    for task in sorted(manifest["tasks"], key=lambda item: int(item.get("index", 0))):
        if count > 0 and len(released) >= count:
            break
        if task_state(manifest, task, lease_seconds=lease_seconds) != "claimed":
            continue
        payload = read_state_json(task_claim_path(manifest, task), missing_ok=False)
        if payload is None:
            continue
        if claim_payload_worker(payload) != worker:
            continue
        shutil.rmtree(task_claim_dir(manifest, task), ignore_errors=True)
        released.append(str(task["id"]))
    return released

def update_claim_heartbeat(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    worker: str,
    claim_id: str | None,
    progress_data: dict[str, Any] | None = None,
    running: bool = False,
) -> None:
    payload = check_claim_owner(manifest, task, worker=worker, claim_id=claim_id)
    payload["heartbeat_at"] = now()
    payload["heartbeat_epoch"] = epoch()
    if progress_data is not None:
        payload["progress_data"] = progress_data
    write_json(task_heartbeat_path(manifest, task), payload)
    if running:
        running_payload = {
            "schema": SCHEMA,
            "task": task["id"],
            "index": task.get("index"),
            "worker": worker,
            "claim_id": claim_id,
            "started_at": payload["heartbeat_at"],
            "started_epoch": payload["heartbeat_epoch"],
        }
        write_json(task_running_path(manifest, task), running_payload)

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

def load_coordinator_manifest(coordinator: str) -> dict[str, Any]:
    host, manifest_path = parse_coordinator(coordinator)
    if host:
        code = (
            "from pathlib import Path; "
            "import sys; "
            "sys.stdout.write(Path(sys.argv[1]).read_text(encoding='utf-8'))"
        )
        result = subprocess.run(
            ["ssh", *SSH_OPTS, host, shell_join(["python3", "-c", code, manifest_path])],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "coordinator manifest load failed"
            raise SystemExit(detail)
        return json.loads(result.stdout)
    return load_manifest(expand_path(manifest_path))

def worker_execution_manifest(
    manifest: dict[str, Any],
    *,
    profile: str | None,
    manifest_path: Path,
) -> dict[str, Any]:
    local_manifest = apply_profile(copy.deepcopy(manifest), profile)
    local_run_dir = expand_path(manifest_path).parent / "worker-state"
    local_manifest["state_dir"] = str(local_run_dir / "state")
    local_manifest["log_dir"] = str(local_run_dir / "logs")
    local_manifest["claim_dir"] = str(local_run_dir / "claims")
    return local_manifest

def coordinator_result_payload(
    *,
    coordinator_manifest: dict[str, Any],
    local_payload: dict[str, Any],
    local_manifest: dict[str, Any],
    coordinator_task: dict[str, Any],
    local_task: dict[str, Any],
    worker: str,
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
        "name": worker,
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
        worker=worker,
        state=state,
    )
    sync_payload_path = local_state_path.with_suffix(local_state_path.suffix + ".sync.json")
    write_json(sync_payload_path, payload)
    transfers.append((sync_payload_path, coordinator_state_path))

    sync_files_to_coordinator(coordinator, transfers)
    heartbeat_on_coordinator(coordinator, str(coordinator_task["id"]), worker, claim_id)

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
    if args.task:
        task = find_task(manifest, task_id=args.task, index=None)
        release_claim(manifest, task, worker=args.worker, claim_id=args.claim_id)
        print(task["id"])
        return 0
    if not args.worker:
        raise SystemExit("release requires --task or --worker")
    if args.claim_id:
        raise SystemExit("--claim-id requires --task")
    released = release_worker_claims(
        manifest,
        worker=args.worker,
        count=max(0, int(args.count)),
        lease_seconds=args.lease_seconds,
    )
    if released:
        print("\n".join(released))
    else:
        print("no reserved claims released")
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

def coordinator_status_summary(coordinator: str, *, lease_seconds: int) -> dict[str, Any] | None:
    run_dir = str(Path(coordinator_manifest_path(coordinator)).parent)
    result = run_coordinator_cli(
        coordinator,
        [
            "status",
            run_dir,
            "--json",
            "--lease-seconds", str(lease_seconds),
            "--no-color",
        ],
    )
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None

def coordinator_has_claimable_or_active_work(coordinator: str, *, lease_seconds: int) -> bool:
    summary = coordinator_status_summary(coordinator, lease_seconds=lease_seconds)
    counts = summary.get("counts") if isinstance(summary, dict) else None
    if not isinstance(counts, dict):
        return False
    return any(int(counts.get(name) or 0) > 0 for name in ("pending", "running", "claimed", "stale"))

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
    running: bool = False,
) -> None:
    command = ["heartbeat", "--manifest", "{manifest}", "--task", task_id, "--worker", worker]
    if claim_id:
        command.extend(["--claim-id", claim_id])
    if running:
        command.append("--running")
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
    from .status import task_log_progress_data

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
    local_manifest_path = expand_path(args.manifest)
    local_manifest_base = load_manifest(local_manifest_path)
    coordinator_manifest = load_coordinator_manifest(args.coordinator)
    local_manifest = worker_execution_manifest(
        local_manifest_base,
        profile=args.profile,
        manifest_path=local_manifest_path,
    )
    check_coordinator_cli(args.coordinator)
    check_requirements(local_manifest)
    completed = 0
    failed = 0
    idle_deadline: float | None = None

    while args.max_tasks <= 0 or completed < args.max_tasks:
        claim = claim_from_coordinator(args)
        if claim is None:
            idle_seconds = max(0.0, float(args.idle_seconds))
            if idle_seconds <= 0.0:
                break
            if not coordinator_has_claimable_or_active_work(args.coordinator, lease_seconds=args.lease_seconds):
                break
            if idle_deadline is None:
                idle_deadline = time.monotonic() + idle_seconds
            remaining = idle_deadline - time.monotonic()
            if remaining <= 0.0:
                break
            time.sleep(min(WORK_IDLE_POLL_SECONDS, remaining))
            continue
        idle_deadline = None
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
            heartbeat_on_coordinator(
                args.coordinator,
                task_id,
                args.worker,
                claim_id,
                running=True,
            )
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
    if getattr(args, "coordinator", None):
        check_coordinator_cli(str(args.coordinator))
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
        running=bool(args.running),
    )
    print(task["id"])
    return 0
