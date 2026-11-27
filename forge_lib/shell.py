from __future__ import annotations

from .core import *
from .manifest import SCHEMA
from .workers import (
    run_worker_shell,
    sync_inputs_to_worker,
    worker_host,
    worker_path_map,
    worker_records_from_payload,
    workers_payload_from_arg,
)
from .templates import worker_path_parameter


def one_slot_per_host_payload(workers_arg: str | None) -> dict[str, Any]:
    payload = workers_payload_from_arg(workers_arg, default_ok=True)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for worker in worker_records_from_payload(payload, label=workers_arg or str(runs_dir() / RUN_WORKERS_NAME)):
        host = worker_host(worker)
        if not host or host in seen:
            continue
        seen.add(host)
        record = dict(worker)
        record.pop("slots", None)
        records.append(record)
    if not records:
        raise SystemExit("no enabled workers")
    return {
        "schema": "forge.workers.v1",
        "workers": records,
    }


def shell_worker_label(worker: dict[str, Any]) -> str:
    return str(worker.get("name") or worker_host(worker) or "local")


def run_shell_host(
    worker: dict[str, Any],
    command: str,
    *,
    timeout: float | None,
    tracker: ProcessTracker,
) -> subprocess.CompletedProcess[str]:
    try:
        return run_worker_shell(
            worker,
            command,
            timeout=timeout,
            tracker=tracker,
            retries=0,
        )
    except OSError as exc:
        return subprocess.CompletedProcess(["sh"], 127, stdout="", stderr=str(exc))


def indent_lines(text: str, *, prefix: str = "  ") -> list[str]:
    if not text:
        return []
    return [prefix + line for line in text.rstrip("\n").splitlines()]


def format_shell_result(label: str, result: subprocess.CompletedProcess[str]) -> str:
    suffix = "" if result.returncode == 0 else f" FAILED rc={result.returncode}"
    lines = [f"{label}:{suffix}"]
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if stdout and stderr:
        lines.append("  stdout:")
        lines.extend(indent_lines(stdout, prefix="    "))
        lines.append("  stderr:")
        lines.extend(indent_lines(stderr, prefix="    "))
    else:
        output = stdout or stderr
        if output:
            lines.extend(indent_lines(output))
    return "\n".join(lines) + "\n"


def shell_failure_reason(result: subprocess.CompletedProcess[str]) -> str:
    output = "\n".join(
        part.strip()
        for part in (result.stdout or "", result.stderr or "")
        if part.strip()
    )
    lower = output.lower()
    if "no route to host" in lower or "connection timed out" in lower or "could not resolve hostname" in lower:
        return "not responding"
    if result.returncode == 255 and not output:
        return "not responding"
    if not output:
        return f"failed rc={result.returncode}"
    return next((line.strip() for line in output.splitlines() if line.strip()), output)[:160]


def format_shell_failure_summary(
    failed: dict[str, subprocess.CompletedProcess[str]],
    labels: list[str],
) -> str:
    lines = [f"sh: failed on {len(failed)}/{len(labels)} host(s):"]
    for label in labels:
        result = failed.get(label)
        if result is None:
            continue
        lines.append(f"  {label}:")
        lines.append(f"      {shell_failure_reason(result)}")
    return "\n".join(lines) + "\n"


def shell_stream_text(stream: Any, text: str) -> str:
    is_tty = False
    isatty = getattr(stream, "isatty", None)
    if callable(isatty):
        try:
            is_tty = bool(isatty())
        except OSError:
            is_tty = False
    if not is_tty:
        return text
    return text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")


def write_shell_stream(stream: Any, text: str) -> None:
    stream.write(shell_stream_text(stream, text))
    stream.flush()


def home_relative_command(command: str) -> str:
    return command.replace(str(Path.home()) + os.sep, "~" + os.sep)


def shell_jobs_limit(value: Any, host_count: int) -> int:
    if value is None:
        return max(1, host_count)
    return max(1, int(value))


def run_on_hosts(
    workers: list[dict[str, Any]],
    action: Any,
    *,
    jobs: int,
    tracker: ProcessTracker,
) -> Any:
    """Yield (label, result) as each host finishes ``action(worker)``."""
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(workers))) as executor:
            futures = {executor.submit(action, worker): shell_worker_label(worker) for worker in workers}
            for future in concurrent.futures.as_completed(futures):
                yield futures[future], future.result()
    except KeyboardInterrupt:
        tracker.terminate_all()
        raise


def cmd_sh(args: argparse.Namespace) -> int:
    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sh workers")
    command = home_relative_command(args.command)
    timeout = bounded_timeout(args.remote_timeout_seconds)
    jobs = shell_jobs_limit(args.jobs, len(workers))
    if getattr(args, "detach", False):
        return start_detached_sh(workers, command, timeout=timeout, jobs=jobs)
    tracker = ProcessTracker()
    failed: dict[str, subprocess.CompletedProcess[str]] = {}

    labels = [shell_worker_label(worker) for worker in workers]
    write_shell_stream(
        sys.stdout,
        f"sh: running on {len(workers)} host(s): {', '.join(labels)}\n",
    )
    for label, result in run_on_hosts(
        workers,
        lambda worker: run_shell_host(worker, command, timeout=timeout, tracker=tracker),
        jobs=jobs,
        tracker=tracker,
    ):
        write_shell_stream(sys.stdout, format_shell_result(label, result))
        if result.returncode != 0:
            failed[label] = result

    if failed:
        write_shell_stream(sys.stderr, format_shell_failure_summary(failed, labels))
        return 1
    return 0


# Detached sh jobs: each host runs the command under setsid/nohup and records
# its output, pid and exit code in ~/.forge/sh/<id>/ on that host. The
# coordinator keeps runs/.sh-jobs/<id>.json so `forge jobs` can poll later.

SH_JOB_SCHEMA = "forge.sh-job.v1"
SH_JOBS_DIR_NAME = ".sh-jobs"
SH_JOB_ID_PATTERN = re.compile(r"^[0-9]{8}-[0-9]{6}-[0-9a-f]{4}$")
SH_JOB_PENDING_STATES = {"starting", "running", "unreachable"}
SH_JOB_UNREACHABLE_LIMIT = 5
SH_JOB_FAILURE_TAIL_LINES = 20
SH_JOB_PROGRESS_SECONDS = 60.0


def sh_jobs_dir() -> Path:
    return runs_dir() / SH_JOBS_DIR_NAME


def new_sh_job_id() -> str:
    return time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]


def remote_sh_job_dir(job_id: str) -> str:
    if not SH_JOB_ID_PATTERN.match(job_id):
        raise SystemExit(f"invalid sh job id: {job_id}")
    return f'"$HOME/.forge/sh/{job_id}"'


def detached_launch_script(job_id: str, command: str) -> str:
    runner = (
        'echo $$ >"$d/pid"; '
        f"sh -c {shlex.quote(command)}; "
        'echo $? >"$d/rc.tmp"; mv "$d/rc.tmp" "$d/rc"'
    )
    quoted = shlex.quote(runner)
    return (
        f"d={remote_sh_job_dir(job_id)}; export d; mkdir -p \"$d\"; "
        "if command -v setsid >/dev/null 2>&1; then "
        f"setsid sh -c {quoted} </dev/null >\"$d/log\" 2>&1 & "
        f"else nohup sh -c {quoted} </dev/null >\"$d/log\" 2>&1 & fi; "
        "echo started"
    )


def detached_status_script(job_id: str) -> str:
    return (
        f"d={remote_sh_job_dir(job_id)}; "
        'if [ -f "$d/rc" ]; then state="done $(cat "$d/rc")"; '
        'elif [ -f "$d/pid" ] && kill -0 "$(cat "$d/pid")" 2>/dev/null; then state=running; '
        'elif [ -f "$d/pid" ]; then state=lost; '
        'elif [ -d "$d" ]; then state=starting; '
        "else state=missing; fi; "
        'echo "$state"; '
        "case \"$state\" in 'done 0'|running|starting) lines=1;; *) lines="
        f"{SH_JOB_FAILURE_TAIL_LINES};; esac; "
        "tail -c 65536 \"$d/log\" 2>/dev/null | tr '\\r' '\\n' | grep -v '^[[:space:]]*$' | tail -n \"$lines\""
    )


def detached_log_script(job_id: str) -> str:
    return f'cat {remote_sh_job_dir(job_id)}/log'


def sh_job_path(job_id: str) -> Path:
    return sh_jobs_dir() / f"{job_id}.json"


def start_detached_sh(
    workers: list[dict[str, Any]],
    command: str,
    *,
    timeout: float | None,
    jobs: int,
) -> int:
    job_id = new_sh_job_id()
    script = detached_launch_script(job_id, command)
    tracker = ProcessTracker()
    started: dict[str, dict[str, Any]] = {}
    failed: dict[str, subprocess.CompletedProcess[str]] = {}
    labels = [shell_worker_label(worker) for worker in workers]
    by_label = dict(zip(labels, workers))
    for label, result in run_on_hosts(
        workers,
        lambda worker: run_shell_host(worker, script, timeout=timeout, tracker=tracker),
        jobs=jobs,
        tracker=tracker,
    ):
        if result.returncode == 0:
            started[label] = by_label[label]
        else:
            failed[label] = result

    if started:
        write_json(sh_job_path(job_id), {
            "schema": SH_JOB_SCHEMA,
            "id": job_id,
            "command": command,
            "started_at": utc_timestamp(),
            "workers": [started[label] for label in labels if label in started],
        })
        write_shell_stream(
            sys.stdout,
            f"sh: started job {job_id} on {len(started)} host(s): "
            f"{', '.join(label for label in labels if label in started)}\n"
            f"  status: forge jobs {job_id}\n"
            f"  wait:   forge jobs {job_id} --wait\n"
            f"  log:    forge jobs {job_id} --log <host>\n",
        )
    if failed:
        write_shell_stream(sys.stderr, format_shell_failure_summary(failed, labels))
        return 1
    return 0


def utc_timestamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sh_job_ids() -> list[str]:
    root = sh_jobs_dir()
    if not root.is_dir():
        return []
    return sorted(
        (path.stem for path in root.glob("*.json") if SH_JOB_ID_PATTERN.match(path.stem)),
        reverse=True,
    )


def resolve_sh_job(value: str) -> dict[str, Any]:
    ids = sh_job_ids()
    if not ids:
        raise SystemExit("no detached sh jobs; start one with forge sh --detach")
    if value == "last":
        matches = ids[:1]
    else:
        matches = [job_id for job_id in ids if job_id.startswith(value)]
    if not matches:
        raise SystemExit(f"unknown sh job: {value}")
    if len(matches) > 1:
        raise SystemExit(f"ambiguous sh job {value}: {', '.join(matches)}")
    return read_json(sh_job_path(matches[0]))


def parse_sh_job_status(result: subprocess.CompletedProcess[str]) -> tuple[str, int | None, list[str]]:
    if result.returncode != 0:
        return "unreachable", None, [shell_failure_reason(result)]
    lines = (result.stdout or "").splitlines()
    head = lines[0].strip() if lines else ""
    tail = [line.rstrip() for line in lines[1:]]
    if head.startswith("done "):
        try:
            rc = int(head.split()[1])
        except (IndexError, ValueError):
            return "lost", None, tail
        return ("ok" if rc == 0 else "failed"), rc, tail
    if head in ("running", "starting", "lost", "missing"):
        return head, None, tail
    return "unreachable", None, [head or "no status output"]


def format_sh_job_status(label: str, state: str, rc: int | None, tail: list[str]) -> str:
    title = {
        "ok": "ok",
        "failed": f"FAILED rc={rc}",
        "lost": "LOST (process gone without an exit code)",
        "missing": "MISSING (no job directory on host)",
    }.get(state, state)
    if state in ("failed", "lost", "unreachable") and len(tail) > 1:
        return "\n".join([f"{label}: {title}", *indent_lines("\n".join(tail))]) + "\n"
    last = f"  {tail[-1]}" if tail else ""
    return f"{label}: {title}{last}\n"


def elapsed_text(started_at: str) -> str:
    try:
        started = datetime.datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
    except (TypeError, ValueError):
        return "?"
    seconds = max(0, int((datetime.datetime.now(datetime.timezone.utc) - started).total_seconds()))
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m{seconds:02d}s"


def list_sh_jobs(limit: int) -> int:
    ids = sh_job_ids()
    if not ids:
        write_shell_stream(sys.stdout, "no detached sh jobs\n")
        return 0
    for job_id in ids[:limit]:
        job = read_json(sh_job_path(job_id))
        command = " ".join(str(job.get("command", "")).split())
        if len(command) > 70:
            command = command[:67] + "..."
        hosts = len(job.get("workers", []))
        write_shell_stream(
            sys.stdout,
            f"{job_id}  {elapsed_text(job.get('started_at', '')):>7} ago  {hosts} host(s)  {command}\n",
        )
    return 0


def show_sh_job_log(job: dict[str, Any], host: str, timeout: float | None) -> int:
    workers = job.get("workers", [])
    matches = [worker for worker in workers if host in (shell_worker_label(worker), worker_host(worker))]
    if not matches:
        labels = ", ".join(shell_worker_label(worker) for worker in workers)
        raise SystemExit(f"job {job['id']} did not run on {host}; hosts: {labels}")
    result = run_shell_host(matches[0], detached_log_script(job["id"]), timeout=timeout, tracker=ProcessTracker())
    if result.stdout:
        write_shell_stream(sys.stdout, result.stdout)
    if result.returncode != 0:
        write_shell_stream(sys.stderr, f"{host}: {shell_failure_reason(result)}\n")
    return result.returncode


def poll_sh_job(
    job: dict[str, Any],
    *,
    timeout: float | None,
) -> dict[str, tuple[str, int | None, list[str]]]:
    workers = job.get("workers", [])
    script = detached_status_script(job["id"])
    tracker = ProcessTracker()
    return {
        label: parse_sh_job_status(result)
        for label, result in run_on_hosts(
            workers,
            lambda worker: run_shell_host(worker, script, timeout=timeout, tracker=tracker),
            jobs=len(workers),
            tracker=tracker,
        )
    }


def sh_job_summary(states: dict[str, tuple[str, int | None, list[str]]]) -> str:
    counts: dict[str, int] = {}
    for state, _rc, _tail in states.values():
        counts[state] = counts.get(state, 0) + 1
    order = ["ok", "running", "starting", "failed", "lost", "missing", "unreachable"]
    return ", ".join(f"{counts[state]} {state}" for state in order if counts.get(state))


def cmd_jobs(args: argparse.Namespace) -> int:
    if args.id is None:
        if args.wait or args.log:
            args.id = "last"
        else:
            return list_sh_jobs(max(1, int(args.limit)))
    job = resolve_sh_job(args.id)
    timeout = bounded_timeout(args.remote_timeout_seconds)
    if args.log:
        return show_sh_job_log(job, args.log, timeout)

    workers = job.get("workers", [])
    labels = [shell_worker_label(worker) for worker in workers]
    write_shell_stream(
        sys.stdout,
        f"job {job['id']} ({elapsed_text(job.get('started_at', ''))}): {job.get('command', '')}\n",
    )
    interval = max(1.0, float(args.interval_seconds))
    printed: dict[str, str] = {}
    unreachable: dict[str, int] = {}
    last_output = time.monotonic()
    while True:
        states = poll_sh_job(job, timeout=timeout)
        for label in labels:
            state, rc, tail = states[label]
            unreachable[label] = unreachable.get(label, 0) + 1 if state == "unreachable" else 0
            key = state if args.wait else ""
            if not args.wait or printed.get(label) != key:
                write_shell_stream(sys.stdout, format_sh_job_status(label, state, rc, tail))
                printed[label] = key
                last_output = time.monotonic()
        if args.wait and time.monotonic() - last_output >= SH_JOB_PROGRESS_SECONDS:
            write_shell_stream(
                sys.stdout,
                f"  ... {sh_job_summary(states)} ({elapsed_text(job.get('started_at', ''))})\n",
            )
            last_output = time.monotonic()
        pending = [
            label
            for label, (state, _rc, _tail) in states.items()
            if state in ("running", "starting")
            or (state == "unreachable" and unreachable[label] < SH_JOB_UNREACHABLE_LIMIT)
        ]
        if not args.wait or not pending:
            break
        time.sleep(interval)

    write_shell_stream(sys.stdout, f"job {job['id']}: {sh_job_summary(states)}\n")
    bad = [label for label in labels if states[label][0] not in ("ok", "running", "starting")]
    if bad:
        write_shell_stream(sys.stderr, f"  log: forge jobs {job['id']} --log {bad[0]}\n")
        return 1
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    source = expand_path(args.source)
    if not source.exists():
        raise SystemExit(f"sync source does not exist: {source}")
    destination_arg = args.destination if args.destination is not None else worker_path_parameter(args.source)

    if source.is_dir():
        source_files = sorted(path for path in source.rglob("*") if path.is_file())
        if not source_files:
            raise SystemExit(f"sync source directory contains no files: {source}")
        if destination_arg.endswith("/"):
            destination_arg = destination_arg.rstrip("/") + "/" + source.name
        destination = worker_path_parameter(destination_arg)
        destinations = {
            str(path): destination.rstrip("/") + "/" + path.relative_to(source).as_posix()
            for path in source_files
        }
    elif source.is_file():
        source_files = [source]
        destination = worker_path_parameter(destination_arg)
        destinations = {str(source): destination}
    else:
        raise SystemExit(f"sync source is not a file or directory: {source}")

    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sync workers")
    timeout = bounded_timeout(args.remote_timeout_seconds)
    jobs = shell_jobs_limit(args.jobs, len(workers))
    tracker = ProcessTracker()
    manifest = {
        "schema": SCHEMA,
        "name": "sync",
        "requirements": {"inputs": [file_record(path) for path in source_files]},
        "tasks": [],
    }
    sync_workers = []
    for index, worker in enumerate(workers):
        record = dict(worker)
        record["profile"] = f"sync-{index}"
        record["path_map"] = {**worker_path_map(record), **destinations}
        sync_workers.append(record)

    coordinator = socket.gethostname().split(".", 1)[0]
    write_shell_stream(
        sys.stdout,
        f"syncing {source.name} from {coordinator} to {destination}\n",
    )
    failed = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(sync_workers))) as executor:
            futures = {
                executor.submit(
                    sync_inputs_to_worker,
                    worker,
                    manifest,
                    timeout=timeout,
                    tracker=tracker,
                    retries=0,
                ): shell_worker_label(worker)
                for worker in sync_workers
            }
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                try:
                    synced = future.result()
                except (OSError, RuntimeError) as exc:
                    failed += 1
                    write_shell_stream(sys.stderr, f"{label}: failed: {exc}\n")
                else:
                    state = "synced" if synced else "current"
                    write_shell_stream(sys.stdout, f"{label}: {state}\n")
    except KeyboardInterrupt:
        tracker.terminate_all()
        raise
    return 1 if failed else 0
