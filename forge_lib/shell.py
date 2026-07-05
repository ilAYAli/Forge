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


def cmd_sh(args: argparse.Namespace) -> int:
    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sh workers")
    command = home_relative_command(args.command)
    timeout = bounded_timeout(args.remote_timeout_seconds)
    jobs = max(1, int(args.jobs))
    tracker = ProcessTracker()
    failed = 0

    labels = [shell_worker_label(worker) for worker in workers]
    write_shell_stream(
        sys.stdout,
        f"sh: running on {len(workers)} host(s): {', '.join(labels)}\n",
    )
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(workers))) as executor:
            futures = {
                executor.submit(run_shell_host, worker, command, timeout=timeout, tracker=tracker): shell_worker_label(worker)
                for worker in workers
            }
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                result = future.result()
                write_shell_stream(sys.stdout, format_shell_result(label, result))
                if result.returncode != 0:
                    failed += 1
    except KeyboardInterrupt:
        tracker.terminate_all()
        raise

    if failed:
        write_shell_stream(sys.stderr, f"sh: failed on {failed}/{len(workers)} host(s)\n")
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
    jobs = max(1, int(args.jobs))
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
