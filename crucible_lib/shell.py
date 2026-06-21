from __future__ import annotations

from .core import *
from .workers import (
    run_worker_shell,
    worker_host,
    worker_records_from_payload,
    workers_payload_from_arg,
)


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
        "schema": "crucible.workers.v1",
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
    status = "OK" if result.returncode == 0 else f"FAILED rc={result.returncode}"
    lines = [f"{label}: {status}"]
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


def cmd_sh(args: argparse.Namespace) -> int:
    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sh workers")
    timeout = bounded_timeout(args.remote_timeout_seconds)
    jobs = max(1, int(args.jobs))
    tracker = ProcessTracker()
    failed = 0
    print_lock = threading.Lock()

    labels = [shell_worker_label(worker) for worker in workers]
    print(f"sh: running on {len(workers)} host(s): {', '.join(labels)}", flush=True)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(jobs, len(workers))) as executor:
            futures = {
                executor.submit(run_shell_host, worker, args.command, timeout=timeout, tracker=tracker): shell_worker_label(worker)
                for worker in workers
            }
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                result = future.result()
                with print_lock:
                    sys.stdout.write(format_shell_result(label, result))
                    sys.stdout.flush()
                if result.returncode != 0:
                    failed += 1
    except KeyboardInterrupt:
        tracker.terminate_all()
        raise

    if failed:
        print(f"sh: failed on {failed}/{len(workers)} host(s)", file=sys.stderr)
        return 1
    print("sh: ok")
    return 0
