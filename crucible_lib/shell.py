from __future__ import annotations

import contextlib
import io

from .core import *
from .status import cmd_del, cmd_status, run_summary_for
from .tasks import task_fail_path, task_log_path
from .workers import (
    cmd_run_job,
    worker_host,
    worker_records_from_payload,
    workers_payload_from_arg,
    workers_payload_from_records,
)


def shell_run_name() -> str:
    return time.strftime("sh-%Y%m%d-%H%M%S")

def shell_escape_format(value: str) -> str:
    return value.replace("{", "{{").replace("}", "}}")

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
    return workers_payload_from_records(records)

def shell_manifest(command: str, *, run_name: str, run_dir: Path, workers: list[dict[str, Any]]) -> dict[str, Any]:
    outputs_dir = run_dir / "outputs"
    escaped = shell_escape_format(command)
    tasks = []
    for index, worker in enumerate(workers):
        host = worker_host(worker)
        task_id = f"sh_{index:04d}"
        output = outputs_dir / f"{host or task_id}.log"
        tasks.append({
            "id": task_id,
            "index": index,
            "label": "sh",
            "claim_worker": host,
            "command": f"({escaped}) > {{output}} 2>&1",
            "outputs": [str(output)],
        })
    return {
        "schema": SCHEMA,
        "name": run_name,
        "kind": "sh",
        "description": "Run shell command once per host",
        "work_dir": str(Path.home()),
        "state_dir": str(run_dir / "state"),
        "log_dir": str(run_dir / "logs"),
        "claim_dir": str(run_dir / "claims"),
        "created_at": now(),
        "metadata": {
            "command": command,
            "ephemeral": True,
        },
        "tasks": tasks,
    }

def print_shell_failures(run_name: str, *, lease_seconds: int) -> None:
    print("sh: failure summary", file=sys.stderr)
    try:
        cmd_status(argparse.Namespace(
            run=run_name,
            verbose=True,
            show_done=False,
            all=False,
            done=False,
            failed=False,
            json=False,
            no_color=True,
            lease_seconds=lease_seconds,
        ))
    except SystemExit:
        pass
    try:
        summary = run_summary_for(run_name, lease_seconds=lease_seconds)
    except SystemExit:
        return
    manifest = read_json(Path(summary["manifest"]))
    failed = [task for task in manifest.get("tasks", []) if isinstance(task, dict) and task_fail_path(manifest, task).exists()]
    for task in failed:
        payload = read_json(task_fail_path(manifest, task))
        worker = str(payload.get("worker") or task.get("claim_worker") or task.get("id") or "?")
        rc = payload.get("rc")
        print(f"FAILED {worker}: rc={rc}", file=sys.stderr)
        log = task_log_path(manifest, task)
        try:
            lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            lines = []
        for line in lines[-20:]:
            print(f"  {line}", file=sys.stderr)

def cleanup_shell_run(run_name: str, *, lease_seconds: int) -> None:
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = cmd_del(argparse.Namespace(run=run_name, done=False, force=True, lease_seconds=lease_seconds))
    except SystemExit as exc:
        print(f"sh: cleanup failed for {run_name}: {exc}", file=sys.stderr)
    else:
        if rc:
            detail = stderr.getvalue().strip() or stdout.getvalue().strip() or f"rc={rc}"
            print(f"sh: cleanup failed for {run_name}: {detail}", file=sys.stderr)
    shutil.rmtree(run_dir_for_name(run_name), ignore_errors=True)
    prune_empty_web_state()

def prune_empty_web_state() -> None:
    path = runs_dir() / ".web-state.json"
    if not path.exists():
        return
    try:
        payload = read_json(path)
    except (OSError, SystemExit):
        return
    if any(payload.get(name) for name in ("seen_runs", "lingering_runs", "hidden_runs", "command_checked_runs")):
        return
    try:
        path.unlink()
    except OSError:
        pass

def cmd_sh(args: argparse.Namespace) -> int:
    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sh workers")
    run_name = validate_run_name(args.run or shell_run_name())
    run_dir = run_dir_for_name(run_name)
    temp_dir = runs_dir() / ".sh-tmp" / run_name
    template_path = temp_dir / "template.json"
    workers_path = temp_dir / "workers.json"
    manifest = shell_manifest(args.command, run_name=run_name, run_dir=run_dir, workers=workers)
    write_json(template_path, manifest)
    write_json(workers_path, workers_payload)
    try:
        rc = cmd_run_job(argparse.Namespace(
            manifest=str(template_path),
            workers=str(workers_path),
            worker_snapshot_payload=workers_payload,
            worker_defaults=None,
            preserve_workers_snapshot=False,
            worker_snapshot_commit_payload=None,
            launch_workers_payload=workers_payload,
            launch_only=False,
            jobs=args.jobs,
            run=run_name,
            resume=False,
            replace=True,
            coordinator_host=args.coordinator_host,
            remote_timeout_seconds=args.remote_timeout_seconds,
            retry_failed=False,
            task_attempts=1,
            wait=True,
            lease_seconds=args.lease_seconds,
            interval_seconds=args.interval_seconds,
            timeout_seconds=args.timeout_seconds,
            fail_on_fail=True,
            verify=False,
            merge=None,
            merge_force=False,
            newline_between=False,
            notify_command=args.notify_command,
            no_color=args.no_color,
            verbose=args.verbose,
            command_label="sh",
            ephemeral=True,
        ))
        if rc:
            print_shell_failures(run_name, lease_seconds=args.lease_seconds)
        return rc
    finally:
        cleanup_shell_run(run_name, lease_seconds=args.lease_seconds)
        shutil.rmtree(temp_dir, ignore_errors=True)
        try:
            temp_dir.parent.rmdir()
        except OSError:
            pass
