from __future__ import annotations

import math
import shlex

from .calibration import adaptive_blended_throughput, adaptive_rate_warmup_units
from .config import default_notify_command
from .core import *
from .manifest import *
from .tasks import *


QUEUE_MONITOR_NAME = "queue-monitor.json"
PROGRESS_HISTORY_NAME = "progress-history.jsonl"
PROGRESS_GRAPH_VERSION = 7
SIGNED_PROGRESS_SERIES = {"balance", "elo", "llr"}
PROGRESS_GRAPH_SERIES_OPTION_KEYS = {
    "band_max_fraction",
    "band_scale",
    "band_series",
    "max",
    "min",
    "scale",
    "smooth",
    "y_max",
    "y_min",
}


def cmd_status(args: argparse.Namespace) -> int:
    color_enabled = status_color_enabled(args)
    selected = [name for name in ("all", "done", "failed") if bool(getattr(args, name))]
    if len(selected) > 1:
        raise SystemExit("choose only one of --all, --done, or --failed")
    filter_name = selected[0] if selected else None
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        if args.run is None and filter_name == "failed":
            summaries = load_run_summaries(
                lease_seconds=args.lease_seconds,
                attention_only=True,
            )
            if any(run_failed(summary) for summary in summaries):
                return print_run_status(
                    summaries,
                    color_enabled=color_enabled,
                    as_json=args.json,
                    filter_name=filter_name,
                    include_started_pending=bool(getattr(args, "verbose", False)),
                    collapse_inactive_failures=False,
                    show_details=True,
                    show_done=bool(getattr(args, "show_done", False)),
                    show_commands=bool(getattr(args, "verbose", False)),
                )
        if args.run is None and filter_name is None:
            summaries = load_run_summaries(
                lease_seconds=args.lease_seconds,
                attention_only=bool(not getattr(args, "verbose", False)),
            )
            if any(run_current(summary) for summary in summaries):
                return print_run_status(
                    summaries,
                    color_enabled=color_enabled,
                    as_json=args.json,
                    filter_name=None,
                    include_started_pending=bool(getattr(args, "verbose", False)),
                    collapse_inactive_failures=bool(not getattr(args, "verbose", False)),
                    show_details=True,
                    show_done=bool(getattr(args, "show_done", False)),
                    show_commands=bool(getattr(args, "verbose", False)),
                )
        return run_status_on_coordinator(args, coordinator_runs)
    if args.run is None:
        summaries = load_run_summaries(
            lease_seconds=args.lease_seconds,
            attention_only=bool(filter_name is None and not getattr(args, "verbose", False)),
        )
        return print_run_status(
            summaries,
            color_enabled=color_enabled,
            as_json=args.json,
            filter_name=filter_name,
            include_started_pending=bool(getattr(args, "verbose", False)),
            collapse_inactive_failures=bool(filter_name is None and not getattr(args, "verbose", False)),
            show_details=bool(filter_name is None or args.all or args.done or args.failed or getattr(args, "show_done", False) or getattr(args, "verbose", False)),
            show_done=bool(args.done or getattr(args, "show_done", False)),
            show_commands=bool(getattr(args, "verbose", False)),
        )
    if selected:
        raise SystemExit("--all, --done, and --failed cannot be used with a run name")

    manifest_path = run_manifest_path(args.run)
    if not manifest_path.is_file():
        raise SystemExit(f"run not found: {args.run} ({manifest_path})")
    manifest = load_manifest(manifest_path)
    summary = status_summary(manifest, lease_seconds=args.lease_seconds)
    summary["manifest"] = str(manifest_path)
    summary["run"] = manifest_path.parent.name
    summary["metadata"] = manifest_metadata(manifest)
    annotate_run_ordering(summary, manifest_path=manifest_path, manifest=manifest)
    if args.json:
        print(json.dumps(run_status_payload(summary), sort_keys=True))
        return 1 if run_failed(summary) else 0
    print_compact_status(
        summary,
        color_enabled=color_enabled,
        show_done=args.show_done,
        show_diagnostics=args.verbose,
    )
    return 1 if run_failed(summary) else 0

def status_can_delegate(run: str | None) -> bool:
    if run is None:
        return True
    path = Path(str(run))
    return not path.is_absolute() and len(path.parts) == 1 and not run_manifest_path(run).is_file()

def status_command_args(args: argparse.Namespace, *, force_json: bool = False) -> list[str]:
    command = ["status"]
    if args.run is not None:
        command.append(str(args.run))
    for name in ("verbose", "show_done", "all", "done", "failed", "json", "no_color"):
        if name == "json" and force_json:
            command.append("--json")
        elif bool(getattr(args, name)):
            command.append("--" + name.replace("_", "-"))
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    return command

def status_color_enabled(args: argparse.Namespace) -> bool:
    return terminal_color_enabled(no_color=bool(args.no_color) or bool(args.json))

def status_color_mode(args: argparse.Namespace) -> str:
    return "always" if status_color_enabled(args) else "never"

def run_status_on_coordinator(args: argparse.Namespace, coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = status_command_args(args, force_json=True)
    color_mode = status_color_mode(args)
    if host:
        remote = (
            "command -v forge >/dev/null 2>&1 || "
            "{ echo 'forge not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(COLOR_ENV)}={shlex.quote(color_mode)} "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"forge {shell_join(command)}"
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
        env[COLOR_ENV] = color_mode
        env[RUNS_DIR_ENV] = runs_path
        result = subprocess.run(
            [sys.executable, str(script_path()), *command],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=env,
        )
    if result.returncode not in (0, 1):
        if not bool(getattr(args, "json", False)):
            print(f"coordinator: {coordinator_runs}")
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        return result.returncode
    try:
        payload = normalize_status_json_payload(json.loads(result.stdout))
    except json.JSONDecodeError:
        if not bool(getattr(args, "json", False)):
            print(f"coordinator: {coordinator_runs}")
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        return result.returncode
    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, sort_keys=True))
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        return result.returncode
    print(f"coordinator: {coordinator_runs}")
    rc = print_delegated_status_payload(args, payload, runs_label=coordinator_runs)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return rc

def state_control_command_args(name: str, args: argparse.Namespace) -> list[str]:
    command = [name]
    if getattr(args, "run", None) is not None:
        command.append(str(args.run))
    if name == "stop":
        if bool(getattr(args, "all", False)):
            command.append("--all")
        if bool(getattr(args, "force", False)):
            command.append("--force")
        if not bool(getattr(args, "wait", True)):
            command.append("--no-wait")
        command.extend([
            "--interval-seconds",
            str(getattr(args, "interval_seconds", 5.0)),
        ])
    elif name == "drain":
        if bool(getattr(args, "all", False)):
            command.append("--all")
        command.extend(["--worker", str(args.worker)])
        if bool(getattr(args, "clear", False)):
            command.append("--clear")
    elif name == "del":
        if bool(getattr(args, "done", False)):
            command.append("--done")
        if bool(getattr(args, "all", False)):
            command.append("--all")
        if bool(getattr(args, "force", False)):
            command.append("--force")
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    return command

def run_state_control_on_coordinator(name: str, args: argparse.Namespace, coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = state_control_command_args(name, args)
    if host:
        remote = (
            "command -v forge >/dev/null 2>&1 || "
            "{ echo 'forge not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(RUNS_DIR_ENV)}={shlex.quote(runs_path)} "
            f"forge {shell_join(command)}"
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

def normalize_status_summary_payload(summary: Any) -> Any:
    if not isinstance(summary, dict):
        return summary
    summary = status_summary_from_payload(summary)
    try:
        return run_status_payload(summary)
    except (KeyError, TypeError, ValueError):
        return summary

def status_summary_from_payload(summary: Any) -> Any:
    if not isinstance(summary, dict):
        return summary
    normalized = copy.deepcopy(summary)
    sections = normalized.get("sections")
    pending_section = sections.get("pending") if isinstance(sections, dict) else None
    if not isinstance(normalized.get("pending"), list):
        tasks = pending_section.get("tasks") if isinstance(pending_section, dict) else None
        normalized["pending"] = tasks if isinstance(tasks, list) else []
    if not isinstance(normalized.get("pending_groups"), dict):
        groups = pending_section.get("groups") if isinstance(pending_section, dict) else None
        normalized["pending_groups"] = groups if isinstance(groups, dict) else {}
    return normalized

def normalize_status_json_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and isinstance(payload.get("runs"), list):
        normalized = copy.deepcopy(payload)
        normalized["runs"] = [
            normalize_status_summary_payload(summary)
            for summary in normalized["runs"]
        ]
        return normalized
    return normalize_status_summary_payload(payload)

def print_delegated_status_payload(args: argparse.Namespace, payload: Any, *, runs_label: str | None = None) -> int:
    selected = [name for name in ("all", "done", "failed") if bool(getattr(args, name))]
    filter_name = selected[0] if selected else None
    color_enabled = status_color_enabled(args)
    if args.run is None and isinstance(payload, dict) and isinstance(payload.get("runs"), list):
        summaries = [
            status_summary_from_payload(summary)
            for summary in payload["runs"]
        ]
        return print_run_status(
            summaries,
            color_enabled=color_enabled,
            as_json=False,
            filter_name=filter_name,
            include_started_pending=bool(getattr(args, "verbose", False)),
            collapse_inactive_failures=bool(filter_name is None and not getattr(args, "verbose", False)),
            show_details=bool(filter_name is None or args.all or args.done or args.failed or getattr(args, "show_done", False) or getattr(args, "verbose", False)),
            show_done=bool(args.done or getattr(args, "show_done", False)),
            show_commands=bool(getattr(args, "verbose", False)),
            runs_label=runs_label,
        )
    if args.run is not None and isinstance(payload, dict) and isinstance(payload.get("counts"), dict):
        summary = status_summary_from_payload(payload)
        print_compact_status(
            summary,
            color_enabled=color_enabled,
            show_done=args.show_done,
            show_diagnostics=args.verbose,
        )
        return 1 if run_failed(summary) else 0
    raise SystemExit("coordinator returned unexpected status JSON")

def run_manifest_paths() -> list[Path]:
    root = runs_dir()
    if not root.exists():
        return []
    return sorted(
        path / RUN_MANIFEST_NAME
        for path in root.iterdir()
        if path.is_dir() and (path / RUN_MANIFEST_NAME).is_file()
    )

def queue_monitor_path_for_manifest(manifest_path: Path) -> Path:
    return manifest_path.parent / QUEUE_MONITOR_NAME

def process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True

def queue_monitor_state(manifest_path: Path) -> tuple[str | None, dict[str, Any] | None]:
    path = queue_monitor_path_for_manifest(manifest_path)
    if not path.exists():
        return None, None
    payload = read_state_json(path, missing_ok=False)
    if payload is None:
        return "invalid", None
    try:
        pid = int(payload.get("pid"))
    except (TypeError, ValueError):
        record_state_diagnostic(path, "queue monitor pid is not numeric")
        return "invalid", payload
    return ("running" if process_is_alive(pid) else "dead"), payload

def load_run_summary_from_manifest(path: Path, *, lease_seconds: int, attention_only: bool) -> dict[str, Any] | None:
    try:
        manifest = load_manifest(path)
    except OSError:
        return None
    except SystemExit:
        return None
    if attention_only and not run_manifest_has_attention_state(manifest):
        return None
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    summary["manifest"] = str(path)
    summary["run"] = path.parent.name
    summary["metadata"] = manifest_metadata(manifest)
    if run_queue_requested(summary):
        monitor_state, monitor_payload = queue_monitor_state(path)
        if monitor_state is not None:
            summary["queue_monitor_state"] = monitor_state
        if monitor_payload is not None:
            summary["queue_monitor"] = monitor_payload
        if monitor_state in {"dead", "invalid"}:
            summary.setdefault("diagnostics", []).append(f"queue monitor {monitor_state}")
    annotate_run_ordering(summary, manifest_path=path, manifest=manifest)
    return summary

RUN_SUMMARY_NAME = "summary.json"


def run_summary_path(manifest_path: Path) -> Path:
    return manifest_path.parent / RUN_SUMMARY_NAME


def format_epoch_timestamp(value: float) -> str:
    return datetime.datetime.fromtimestamp(value).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def file_mtime_epoch(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def request_payload_epoch(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in ("requested_epoch", "queued_epoch"):
        value = parse_timestamp_epoch(payload.get(key))
        if value is not None:
            return value
    return parse_timestamp_epoch(payload.get("requested_at"))


def completion_payload_epoch(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    for key in ("completed_epoch", "finished_epoch"):
        value = parse_timestamp_epoch(payload.get(key))
        if value is not None:
            return value
    for key in ("completed_at", "finished_at"):
        value = parse_timestamp_epoch(payload.get(key))
        if value is not None:
            return value
    return None


def read_run_summary_payload(manifest_path: Path) -> dict[str, Any] | None:
    try:
        return read_json(run_summary_path(manifest_path))
    except (OSError, SystemExit):
        return None


def latest_done_file_epoch(manifest: dict[str, Any]) -> float | None:
    latest: float | None = None
    for task in manifest.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        value = file_mtime_epoch(task_done_path(manifest, task))
        if value is not None and (latest is None or value > latest):
            latest = value
    return latest


def annotate_run_ordering(
    summary: dict[str, Any],
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> None:
    queued_epoch = request_payload_epoch(summary.get("queue"))
    if queued_epoch is not None:
        summary["queued_epoch"] = queued_epoch
        summary["queued_at"] = format_epoch_timestamp(queued_epoch)
    if not run_done(summary):
        return
    completed_epoch = completion_payload_epoch(read_run_summary_payload(manifest_path))
    if completed_epoch is None:
        completed_epoch = file_mtime_epoch(run_summary_path(manifest_path))
    if completed_epoch is None:
        completed_epoch = latest_done_file_epoch(manifest)
    if completed_epoch is not None:
        summary["completed_epoch"] = completed_epoch
        summary["completed_at"] = format_epoch_timestamp(completed_epoch)


def run_manifest_has_attention_state(manifest: dict[str, Any]) -> bool:
    try:
        state = state_dir(manifest)
        if queue_request_path(manifest).exists() or launch_request_path(manifest).exists() or stop_request_path(manifest).exists():
            return True
        if any(state.glob("*.fail.json")) or any(state.glob("*.running.json")):
            return True
        claims = claim_dir(manifest)
        if any(claims.glob("*.claim/claim.json")):
            return True
    except OSError:
        return True
    return False

def load_run_summaries(*, lease_seconds: int, attention_only: bool = False) -> list[dict[str, Any]]:
    summaries = []
    for path in run_manifest_paths():
        summary = load_run_summary_from_manifest(
            path,
            lease_seconds=lease_seconds,
            attention_only=attention_only,
        )
        if summary is not None:
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
    return summary["counts"]["fail"] > 0 or run_launch_stale(summary)

def run_started(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    return counts["done"] + counts["fail"] + counts["running"] + counts["claimed"] + counts["stale"] > 0

def run_launching(summary: dict[str, Any]) -> bool:
    if summary.get("launch_state") == "launching":
        return True
    if summary.get("launch_state") == "stale":
        return False
    return isinstance(summary.get("launch"), dict) and not run_launch_stale(summary)

def run_queued(summary: dict[str, Any]) -> bool:
    return run_queue_requested(summary)

def run_queue_requested(summary: dict[str, Any]) -> bool:
    return isinstance(summary.get("queue"), dict)

def run_queue_stalled(summary: dict[str, Any]) -> bool:
    return run_queue_requested(summary) and summary.get("queue_monitor_state") in {"dead", "invalid"}

def run_stopped(summary: dict[str, Any]) -> bool:
    return isinstance(summary.get("stop"), dict)

def run_stopped_incomplete(summary: dict[str, Any]) -> bool:
    return run_stopped(summary) and not run_done(summary)

def launch_request_started_epoch(payload: dict[str, Any]) -> float | None:
    for key in ("started_epoch", "requested_epoch"):
        value = parse_timestamp_epoch(payload.get(key))
        if value is not None:
            return value
    return parse_timestamp_epoch(payload.get("requested_at"))

def launch_request_lease_seconds(payload: dict[str, Any]) -> int:
    try:
        value = int(payload.get("lease_seconds") or LAUNCH_REQUEST_LEASE_SECONDS)
    except (TypeError, ValueError):
        return LAUNCH_REQUEST_LEASE_SECONDS
    return max(1, value)

def launch_request_pid(payload: dict[str, Any]) -> int | None:
    try:
        value = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None

def launch_request_stale(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    pid = launch_request_pid(payload)
    if pid is not None:
        return not process_is_alive(pid)
    started_epoch = launch_request_started_epoch(payload)
    if started_epoch is None:
        return True
    return epoch() - started_epoch > launch_request_lease_seconds(payload)

def run_launch_stale(summary: dict[str, Any]) -> bool:
    if summary.get("launch_state") == "stale":
        return True
    if summary.get("launch_state") == "launching":
        return False
    return launch_request_stale(summary.get("launch"))

def run_current(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    active = counts["running"] + counts["claimed"] + counts["stale"]
    if run_launch_stale(summary):
        return False
    if run_stopped(summary) and active == 0 and not run_launching(summary):
        return False
    return run_launching(summary) or active > 0 or (counts["pending"] > 0 and run_started(summary))

def run_active_count(summary: dict[str, Any]) -> int:
    counts = summary["counts"]
    return int(counts["running"] + counts["claimed"] + counts["stale"])

def run_attention(summary: dict[str, Any]) -> bool:
    return (
        run_active_count(summary) > 0
        or run_launching(summary)
        or run_launch_stale(summary)
        or summary["counts"]["fail"] > 0
        or run_queue_requested(summary)
        or run_stopped_incomplete(summary)
    )

def run_queued_sort_key(summary: dict[str, Any]) -> tuple[float, str]:
    value = parse_timestamp_epoch(summary.get("queued_epoch"))
    if value is None:
        value = request_payload_epoch(summary.get("queue"))
    return (value if value is not None else 0.0, str(summary.get("run") or ""))


def run_completed_sort_key(summary: dict[str, Any]) -> tuple[float, str]:
    value = parse_timestamp_epoch(summary.get("completed_epoch"))
    return (-(value if value is not None else 0.0), str(summary.get("run") or ""))


def run_list_sort_key(summary: dict[str, Any]) -> tuple[Any, ...]:
    state = run_state(summary)
    if state == "stalled":
        return (0, *run_queued_sort_key(summary))
    if state == "queued":
        return (1, *run_queued_sort_key(summary))
    if state == "current":
        return (2, -run_active_count(summary), str(summary.get("run") or ""))
    if state == "failed":
        return (3, -int(summary["counts"]["fail"]), str(summary.get("run") or ""))
    if state == "stopped":
        return (4, str(summary.get("run") or ""))
    if state == "done":
        return (5, *run_completed_sort_key(summary))
    return (6, str(summary.get("run") or ""))


def run_attention_sort_key(summary: dict[str, Any]) -> tuple[Any, ...]:
    active = run_active_count(summary)
    fail = int(summary["counts"]["fail"])
    if run_queue_stalled(summary):
        return (0, *run_queued_sort_key(summary))
    if run_queue_requested(summary):
        return (1, *run_queued_sort_key(summary))
    if active > 0 or run_launching(summary):
        return (2, -active, str(summary["run"]))
    if fail > 0 or run_launch_stale(summary):
        return (3, -fail, str(summary["run"]))
    if run_stopped_incomplete(summary):
        return (4, 0, str(summary["run"]))
    return (5, 0, str(summary["run"]))

def run_state(summary: dict[str, Any]) -> str:
    if run_queue_stalled(summary):
        return "stalled"
    if run_queued(summary):
        return "queued"
    if run_stopped(summary) and run_active_count(summary) > 0:
        return "stopping"
    if run_current(summary):
        return "current"
    if run_failed(summary):
        return "failed"
    if run_done(summary):
        return "done"
    if run_stopped(summary):
        return "stopped"
    return "empty"

def run_state_fields(summary: dict[str, Any]) -> list[str]:
    if run_queue_stalled(summary):
        return ["state=stalled"]
    if run_queued(summary):
        return ["state=queued"]
    if run_launch_stale(summary):
        return ["state=launch-stale"]
    if run_stopped(summary) and run_active_count(summary) > 0:
        return ["state=stopping"]
    return []

def filtered_run_summaries(
    summaries: list[dict[str, Any]],
    filter_name: str | None,
    *,
    include_started_pending: bool = False,
) -> list[dict[str, Any]]:
    if filter_name == "done":
        return sorted((item for item in summaries if run_done(item)), key=run_completed_sort_key)
    if filter_name in {None, "current"}:
        filtered = [
            item for item in summaries
            if run_attention(item) or (include_started_pending and run_current(item))
        ]
        return sorted(filtered, key=run_attention_sort_key)
    if filter_name == "all":
        return sorted(summaries, key=run_list_sort_key)
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

def compact_rate(value: float) -> str:
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}".rstrip("0").rstrip(".")

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

def progress_eta_seconds(done: Any, total: Any, rate: Any) -> float | None:
    if not isinstance(done, int) or not isinstance(total, int) or total <= 0:
        return None
    if done >= total:
        return None
    if not isinstance(rate, (int, float)) or rate <= 0:
        return None
    return max(0.0, (total - done) / float(rate))

def eta_field(seconds: float, *, unknown_when_elapsed: bool = False, estimated: bool = False) -> str:
    key = "eta~" if estimated else "eta"
    if seconds <= 0 and unknown_when_elapsed:
        return f"{key}=unknown"
    return f"{key}={compact_duration(max(0.0, seconds))}"

def progress_eta_text(done: Any, total: Any, rate: Any) -> str:
    eta_s = progress_eta_seconds(done, total, rate)
    if eta_s is None:
        return ""
    return eta_field(eta_s)

def progress_data_eta_seconds(data: dict[str, Any], done: Any, total: Any) -> tuple[float, bool] | None:
    eta_s = data.get("eta_s")
    unknown_when_elapsed = bool(data.get("eta_overdue"))
    if isinstance(eta_s, (int, float)):
        seconds = float(eta_s)
        eta_epoch = data.get("eta_epoch")
        if isinstance(eta_epoch, (int, float)):
            seconds -= max(0.0, epoch() - float(eta_epoch))
            unknown_when_elapsed = True
        return seconds, unknown_when_elapsed
    computed = progress_eta_seconds(done, total, data.get("rate"))
    if computed is None:
        return None
    return computed, False

def progress_data_eta_text(data: dict[str, Any], done: Any, total: Any) -> str:
    eta = progress_data_eta_seconds(data, done, total)
    if eta is None:
        return ""
    seconds, unknown_when_elapsed = eta
    return eta_field(seconds, unknown_when_elapsed=unknown_when_elapsed, estimated=bool(data.get("eta_estimate")))

def parse_timestamp_epoch(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").timestamp()
    except ValueError:
        return None

def running_elapsed_seconds(payload: dict[str, Any] | None) -> float | None:
    if not isinstance(payload, dict):
        return None
    started_epoch = parse_timestamp_epoch(payload.get("started_epoch"))
    if started_epoch is None:
        started_epoch = parse_timestamp_epoch(payload.get("started_at"))
    if started_epoch is None:
        return None
    elapsed = epoch() - started_epoch
    return elapsed if elapsed > 0 else None

def task_running_payload(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    payload = read_state_json(task_running_path(manifest, task))
    return payload if isinstance(payload, dict) else None

def task_done_payload(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    payload = read_state_json(task_done_path(manifest, task))
    return payload if isinstance(payload, dict) else None

def infer_progress_rate(
    progress_data: dict[str, Any] | None,
    running_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(progress_data, dict):
        return None
    if isinstance(progress_data.get("rate"), (int, float)) and float(progress_data["rate"]) > 0:
        return progress_data
    done = progress_data.get("done")
    if not isinstance(done, int) or done <= 0:
        return progress_data
    elapsed = running_elapsed_seconds(running_payload)
    if elapsed is None:
        return progress_data
    inferred = dict(progress_data)
    inferred["rate"] = done / elapsed
    inferred["text"] = progress_text(inferred)
    return inferred

def calibrated_progress_rate(
    manifest: dict[str, Any],
    worker: str | None,
    worker_id: str | None,
    fallback_rate: float | None,
) -> float | None:
    try:
        from .chunking import (
            adaptive_worker_cached_throughput,
        )
        from .calibration import adaptive_template_parameters

        params = adaptive_template_parameters(manifest)
        cached_rate = (
            None
            if params.get("__forge_work_unit")
            else adaptive_worker_cached_throughput(
                manifest,
                worker=worker,
                worker_id=worker_id,
            )
        )
    except (OSError, SystemExit, ValueError):
        cached_rate = None
    if isinstance(cached_rate, (int, float)) and cached_rate > 0:
        return float(cached_rate)
    if isinstance(fallback_rate, (int, float)) and fallback_rate > 0:
        return float(fallback_rate)
    return None

def stabilize_progress_eta(
    progress_data: dict[str, Any] | None,
    calibrated_rate: float | None,
) -> dict[str, Any] | None:
    if not isinstance(progress_data, dict):
        return progress_data
    if not isinstance(calibrated_rate, (int, float)) or calibrated_rate <= 0:
        return progress_data
    done = progress_data.get("done")
    total = progress_data.get("total")
    observed_rate = progress_data.get("rate")
    if (
        not isinstance(done, int)
        or done <= 0
        or not isinstance(total, int)
        or total <= 0
        or done >= total
        or not isinstance(observed_rate, (int, float))
        or observed_rate <= 0
    ):
        return progress_data

    warmup_units = adaptive_rate_warmup_units(total)
    if done >= warmup_units:
        return progress_data

    eta_rate = adaptive_blended_throughput(
        float(calibrated_rate),
        float(observed_rate),
        observed_units=done,
        warmup_units=warmup_units,
    )
    eta_s = progress_eta_seconds(done, total, eta_rate)
    if eta_s is None:
        return progress_data
    stabilized = dict(progress_data)
    stabilized["eta_s"] = eta_s
    stabilized["eta_estimate"] = True
    stabilized["eta_calibrated"] = True
    stabilized["text"] = progress_text(stabilized)
    return stabilized

def progress_output_rate(progress_data: dict[str, Any] | None, elapsed_s: float | None) -> float | None:
    if not isinstance(progress_data, dict) or elapsed_s is None or elapsed_s <= 0:
        return None
    output = progress_data.get("output")
    if not isinstance(output, int) or output <= 0:
        return None
    return output / elapsed_s

def progress_data_with_global_position(
    progress_data: dict[str, Any] | None,
    *,
    unit: str,
    offset: int,
    total: int,
) -> dict[str, Any] | None:
    if not isinstance(progress_data, dict):
        return None
    done = progress_data.get("done")
    if not isinstance(done, int):
        return progress_data
    data = dict(progress_data)
    data["global_done"] = min(total, max(0, offset + done))
    data["global_total"] = total
    if not data.get("unit"):
        data["unit"] = unit
    return data

def progress_config_unit_total(task: dict[str, Any]) -> tuple[str, int] | None:
    progress_config = task.get("progress")
    if not isinstance(progress_config, dict):
        return None
    unit = str(progress_config.get("unit") or "").strip()
    total = progress_config.get("total")
    if not unit or not isinstance(total, int) or total <= 0:
        return None
    return unit, total

def fallback_progress_data(
    unit: str,
    total: int,
    rate: float | None,
    elapsed_s: float | None = None,
) -> dict[str, Any] | None:
    if not rate or rate <= 0:
        return None
    data: dict[str, Any] = {
        "kind": "metadata",
        "done": 0,
        "total": total,
        "unit": unit,
        "eta_estimate": True,
        "fallback_estimate": True,
    }
    full_eta = progress_eta_seconds(0, total, rate)
    if full_eta is not None:
        data["eta_s"] = full_eta
    return normalize_progress_data(data)

def progress_text(data: dict[str, Any]) -> str:
    if data.get("kind") == "subtask":
        done = data.get("done")
        total = data.get("total")
        done_text = str(done) if isinstance(done, int) else "?"
        total_text = str(total) if isinstance(total, int) and total > 0 else "?"
        parts = [f"subtask={done_text}/{total_text}"]
        if isinstance(done, int) and isinstance(total, int) and total > 0 and done >= total:
            parts.append("finalizing")
            return ", ".join(parts)
        eta = progress_data_eta_text(data, done, total)
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
        main = f"{unit}={main}"

    parts = [main] if main else []
    if isinstance(output, int):
        parts.append(f"{output_unit}={compact_count(output)}")
    fields = data.get("fields")
    if isinstance(fields, dict):
        for key, value in fields.items():
            parts.append(f"{key}={value}")
    if isinstance(done, int) and isinstance(total, int) and total > 0 and done >= total:
        parts.append("finalizing")
        return ", ".join(parts)
    eta = progress_data_eta_text(data, done, total)
    if eta:
        parts.append(eta)
    return ", ".join(parts)


def summary_kind(summary: dict[str, Any]) -> str:
    metadata = summary.get("metadata")
    if isinstance(metadata, dict) and metadata.get("kind"):
        return str(metadata["kind"]).lower()
    manifest = summary.get("manifest_payload")
    return str(manifest.get("kind") or "").lower() if isinstance(manifest, dict) else ""


def spsa_summary_progress(summary: dict[str, Any]) -> str:
    if summary_kind(summary) != "spsa":
        return ""
    manifest = summary.get("manifest_payload")
    if not isinstance(manifest, dict):
        progress = str(summary.get("progress") or "")
        if progress.startswith("progress=iterations="):
            return progress
        return ""
    config = manifest.get("spsa")
    if not isinstance(config, dict):
        return ""

    manifest_path = summary.get("manifest")
    if isinstance(manifest_path, str) and manifest_path:
        state_path = Path(manifest_path).parent / "spsa-state.json"
    else:
        state_path = expand_path(str(config.get("state") or ""))
    state = read_state_json(state_path)
    if not isinstance(state, dict) or state.get("run") != manifest.get("name"):
        return ""

    start = int(state.get("start_iteration", config.get("start_iteration", 0)))
    total = int(state.get("iterations", config.get("iterations", 0)))
    done = min(total, max(0, int(state.get("k", start)) - start))
    parts = [f"iterations={done}/{total}"]

    iteration_seconds = state.get("iteration_seconds_ema")
    rate = (
        1.0 / float(iteration_seconds)
        if isinstance(iteration_seconds, (int, float)) and iteration_seconds > 0
        else None
    )
    if rate is None:
        rates = active_task_rates(summary)
        rate = min(rates) if rates else None
    eta = progress_eta_text(done, total, rate)
    if eta:
        parts.append(eta)
    return f"progress={', '.join(parts)}"


def run_created_epoch(summary: dict[str, Any]) -> float | None:
    created_at = summary.get("created_at")
    manifest_payload = summary.get("manifest_payload")
    if isinstance(manifest_payload, dict) and isinstance(manifest_payload.get("created_at"), str):
        created_at = manifest_payload["created_at"]
    return parse_timestamp_epoch(created_at)

def run_cumulative_rate(summary: dict[str, Any], done: float) -> float | None:
    if done <= 0:
        return None
    created_epoch = run_created_epoch(summary)
    if created_epoch is None:
        return None
    elapsed = epoch() - created_epoch
    if elapsed <= 0:
        return None
    return done / elapsed

def summary_progress(summary: dict[str, Any]) -> str:
    if progress := spsa_summary_progress(summary):
        return progress
    subtask_done = 0
    subtask_total = 0
    subtask_rate = 0.0
    subtask_count = 0
    subtask_known_total = True
    subtask_known_rate = True
    counts = summary.get("counts", {})
    running_count = int(counts.get("running") or 0)
    waiting_count = sum(int(counts.get(name) or 0) for name in ("pending", "claimed", "stale"))
    by_unit: dict[str, dict[str, Any]] = {}
    outputs: dict[str, int] = {}
    etas: list[str] = []

    def append_aggregate_eta(done: Any, total: Any, rate: Any, active_etas: list[tuple[float, bool]]) -> None:
        aggregate_eta = progress_eta_seconds(done, total, rate)
        max_active_eta = max(active_etas, key=lambda item: item[0]) if active_etas else None
        if aggregate_eta is not None:
            unknown_when_elapsed = False
            if max_active_eta is not None:
                active_eta, active_unknown_when_elapsed = max_active_eta
                if active_unknown_when_elapsed and active_eta <= 0:
                    aggregate_eta = 0.0
                    unknown_when_elapsed = True
                elif not waiting_count and active_eta > aggregate_eta:
                    aggregate_eta = active_eta
                    unknown_when_elapsed = active_unknown_when_elapsed
            etas.append(eta_field(aggregate_eta, unknown_when_elapsed=unknown_when_elapsed))
        elif max_active_eta is not None:
            active_eta, active_unknown_when_elapsed = max_active_eta
            etas.append(eta_field(active_eta, unknown_when_elapsed=active_unknown_when_elapsed))

    for row in summary["active"]:
        if row.get("state", "running") != "running":
            continue
        data = row.get("progress_data")
        if isinstance(data, dict) and data.get("kind") == "metadata":
            done = data.get("done")
            if isinstance(done, int):
                unit = str(data.get("unit") or "items")
                bucket = by_unit.setdefault(
                    unit,
                    {
                        "done": 0,
                        "total": 0,
                        "rate": 0.0,
                        "count": 0,
                        "all_total": True,
                        "all_rate": True,
                        "etas": [],
                    },
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
                eta = progress_data_eta_seconds(data, done, total)
                if eta is not None:
                    bucket["etas"].append(eta)
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
            subtask_known_total and subtask_known_rate and subtask_count == running_count
            and waiting_count == 0
        ):
            eta = progress_eta_text(subtask_done, subtask_total, subtask_rate)
            if eta:
                etas.append(eta)
        parts.append(text)
    emitted_units: set[str] = set()
    for unit, bucket in sorted(by_unit.items()):
        emitted_units.add(unit)
        active_done = int(bucket["done"])
        active_total = int(bucket["total"])
        total_work = summary.get("progress_totals", {}).get(unit)
        done_work = summary.get("progress_done", {}).get(unit, 0)
        if isinstance(total_work, int) and total_work > 0:
            done = min(total_work, int(done_work) + active_done)
            text = f"{unit}={compact_count(done)}/{compact_count(total_work)}"
            cumulative_rate = run_cumulative_rate(summary, float(done))
            eta_rate = cumulative_rate if cumulative_rate is not None else bucket["rate"]
            if eta_rate > 0:
                append_aggregate_eta(done, total_work, eta_rate, bucket["etas"])
            elif waiting_count == 0 and bucket["etas"]:
                append_aggregate_eta(done, total_work, eta_rate, bucket["etas"])
            parts.append(text)
        elif bucket["all_total"] and active_total > 0:
            text = f"{unit}={compact_count(active_done)}/{compact_count(active_total)}"
            if (
                bucket["all_rate"] and bucket["count"] == running_count
                and waiting_count == 0
            ):
                eta = progress_eta_text(active_done, active_total, bucket["rate"])
                if eta:
                    etas.append(eta)
            elif bucket["all_rate"] and bucket["rate"] > 0:
                append_aggregate_eta(active_done, active_total, bucket["rate"], bucket["etas"])
            parts.append(text)
        else:
            parts.append(f"{unit}=~{compact_count(active_done)}")
    progress_totals = summary.get("progress_totals", {})
    progress_done = summary.get("progress_done", {})
    if isinstance(progress_totals, dict) and isinstance(progress_done, dict):
        for unit, total_work in sorted(progress_totals.items()):
            unit = str(unit)
            if unit in emitted_units or not isinstance(total_work, int) or total_work <= 0:
                continue
            done_work = progress_done.get(unit, 0)
            done = int(done_work) if isinstance(done_work, int) else 0
            done = min(total_work, max(0, done))
            parts.append(f"{unit}={compact_count(done)}/{compact_count(total_work)}")
    if not outputs:
        summary_outputs = summary.get("progress_outputs", {})
        if isinstance(summary_outputs, dict):
            outputs = {
                str(output_unit): value
                for output_unit, value in summary_outputs.items()
                if isinstance(value, int)
            }
    for output_unit, value in sorted(outputs.items()):
        parts.append(f"{output_unit}={compact_count(value)}")
    parts.extend(sprt_aggregate_fields(summary))
    parts.extend(etas)
    return f"progress={', '.join(parts)}" if parts else ""

def worker_group_name(worker: str) -> str:
    name = worker.strip() or "unknown"
    if name.lower() in {"localhost", "127.0.0.1", "::1"}:
        name = socket.gethostname()
    hash_slot = re.fullmatch(r"(.+)#\d+", name)
    if hash_slot:
        return hash_slot.group(1)
    for pattern in (r"(.+)-slot\d+", r"(.+)-extra-\d+", r"(.+)-reclaim-\d+"):
        match = re.fullmatch(pattern, name)
        if match:
            return match.group(1)
    return name

def active_worker_metrics(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    workers: dict[str, dict[str, Any]] = {}
    for row in summary["active"]:
        if row.get("state") == "stale":
            continue
        worker = worker_group_name(str(row.get("worker") or "unknown"))
        output_metric = summary.get("worker_outputs", {}).get(worker, {})
        rate_metric = summary.get("worker_rates", {}).get(worker, 0.0)
        entry = workers.setdefault(
            worker,
            {
                "worker": worker,
                "tasks": 0,
                "output": int(output_metric.get("output", 0)) if isinstance(output_metric, dict) else 0,
                "output_seen": bool(output_metric.get("output_seen")) if isinstance(output_metric, dict) else False,
                "rate": float(rate_metric) if isinstance(rate_metric, (int, float)) else 0.0,
                "progress_done": 0,
                "progress_rate": 0.0,
                "progress_rate_seen": False,
                "progress_unit": "",
            },
        )
        entry["tasks"] += 1
        data = row.get("progress_data")
        if isinstance(data, dict) and data.get("kind") == "metadata":
            done = data.get("done")
            if isinstance(done, int):
                entry["progress_done"] += done
            unit = str(data.get("unit") or "").strip()
            if unit:
                if not entry["progress_unit"]:
                    entry["progress_unit"] = unit
                elif entry["progress_unit"] != unit:
                    entry["progress_unit"] = "items"
            rate = data.get("rate")
            if isinstance(rate, (int, float)) and rate > 0:
                entry["progress_rate"] += float(rate)
                entry["progress_rate_seen"] = True
    return workers

def compact_rate_per_second(value: float) -> str:
    return f"{compact_rate(value)}/s"

def active_task_rates(summary: dict[str, Any]) -> list[float]:
    rates: list[float] = []
    for row in summary.get("active", []):
        if row.get("state") == "stale":
            continue
        data = row.get("progress_data")
        rate = data.get("rate") if isinstance(data, dict) and data.get("kind") == "metadata" else None
        if isinstance(rate, (int, float)) and rate > 0:
            rates.append(float(rate))
    return rates

def run_speedup_value(summary: dict[str, Any]) -> float | None:
    if summary_kind(summary) == "spsa":
        return None
    rates = active_task_rates(summary)
    if not rates:
        return None
    baseline = max(rates)
    return sum(rates) / baseline if baseline > 0 else None

def active_worker_summary(summary: dict[str, Any]) -> str:
    metrics = active_worker_metrics(summary)
    if not metrics:
        return ""
    values = list(metrics.values())
    output_values = [metric for metric in values if bool(metric.get("output_seen"))]
    rate_values = [metric for metric in values if bool(metric.get("progress_rate_seen"))]
    prefer_rates = str((summary.get("metadata") or {}).get("kind") or "").lower() == "label"
    if prefer_rates and len(rate_values) > 1:
        total_rate = sum(float(metric.get("progress_rate", 0.0)) for metric in rate_values)
        parts = [f"workers={len(values)}"]
        if output_values:
            parts.append(f"output={compact_count(sum(int(metric['output']) for metric in values))}")
        parts.append(f"rate={compact_rate_per_second(total_rate)}")
        ordered = sorted(rate_values, key=lambda metric: (float(metric["progress_rate"]), str(metric["worker"])))
        max_rate = float(ordered[-1]["progress_rate"])
        parts.append(f"min={str(ordered[0]['worker'])}:{compact_rate_per_second(float(ordered[0]['progress_rate']))}")
        parts.append(f"max={str(ordered[-1]['worker'])}:{compact_rate_per_second(max_rate)}")
        return " ".join(parts)
    if output_values:
        total_output = sum(int(metric["output"]) for metric in values)
        if len(values) == 1:
            return f"output={compact_count(total_output)}"
        parts = [f"workers={len(values)}", f"output={compact_count(total_output)}"]
        ordered = sorted(values, key=lambda metric: (int(metric["output"]), str(metric["worker"])))
        max_output = int(ordered[-1]["output"])
        parts.append(f"min={str(ordered[0]['worker'])}:{compact_count(int(ordered[0]['output']))}")
        parts.append(f"max={str(ordered[-1]['worker'])}:{compact_count(max_output)}")
        return " ".join(parts)

    if not rate_values:
        return ""
    if len(values) == 1:
        return ""
    total_rate = sum(float(metric.get("progress_rate", 0.0)) for metric in rate_values)
    parts = [f"workers={len(values)}", f"rate={compact_rate_per_second(total_rate)}"]
    ordered = sorted(rate_values, key=lambda metric: (float(metric["progress_rate"]), str(metric["worker"])))
    max_rate = float(ordered[-1]["progress_rate"])
    parts.append(f"min={str(ordered[0]['worker'])}:{compact_rate_per_second(float(ordered[0]['progress_rate']))}")
    parts.append(f"max={str(ordered[-1]['worker'])}:{compact_rate_per_second(max_rate)}")
    return " ".join(parts)

def active_worker_summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    metrics = active_worker_metrics(summary)
    if not metrics:
        return {}
    values = list(metrics.values())
    rate_values = [metric for metric in values if bool(metric.get("progress_rate_seen"))]
    prefer_rates = str((summary.get("metadata") or {}).get("kind") or "").lower() == "label"
    payload: dict[str, Any] = {
        "workers": len(values),
        "tasks": sum(int(metric.get("tasks", 0)) for metric in values),
    }
    if any(bool(metric.get("output_seen")) for metric in values):
        total_output = sum(int(metric["output"]) for metric in values)
        payload["output"] = total_output
        payload["output_text"] = compact_count(total_output)
        if len(values) > 1:
            ordered = sorted(values, key=lambda metric: (int(metric["output"]), str(metric["worker"])))
            payload["min"] = {
                "worker": str(ordered[0]["worker"]),
                "output": int(ordered[0]["output"]),
                "output_text": compact_count(int(ordered[0]["output"])),
            }
            payload["max"] = {
                "worker": str(ordered[-1]["worker"]),
                "output": int(ordered[-1]["output"]),
                "output_text": compact_count(int(ordered[-1]["output"])),
            }
        if prefer_rates and len(rate_values) > 1:
            total_rate = sum(float(metric.get("progress_rate", 0.0)) for metric in rate_values)
            ordered = sorted(rate_values, key=lambda metric: (float(metric["progress_rate"]), str(metric["worker"])))
            max_rate = float(ordered[-1]["progress_rate"])
            payload["rate"] = total_rate
            payload["rate_name"] = "rate"
            payload["rate_text"] = compact_rate_per_second(total_rate)
            payload["min_rate"] = {
                "worker": str(ordered[0]["worker"]),
                "rate": float(ordered[0]["progress_rate"]),
                "rate_text": compact_rate_per_second(float(ordered[0]["progress_rate"])),
            }
            payload["max_rate"] = {
                "worker": str(ordered[-1]["worker"]),
                "rate": max_rate,
                "rate_text": compact_rate_per_second(max_rate),
            }
    elif any(bool(metric.get("progress_rate_seen")) for metric in values):
        rate_values = [metric for metric in values if bool(metric.get("progress_rate_seen"))]
        total_rate = sum(float(metric.get("progress_rate", 0.0)) for metric in rate_values)
        payload["rate"] = total_rate
        payload["rate_name"] = "rate"
        payload["rate_text"] = compact_rate_per_second(total_rate)
        if len(values) > 1:
            ordered = sorted(rate_values, key=lambda metric: (float(metric["progress_rate"]), str(metric["worker"])))
            payload["min_rate"] = {
                "worker": str(ordered[0]["worker"]),
                "rate": float(ordered[0]["progress_rate"]),
                "rate_text": compact_rate_per_second(float(ordered[0]["progress_rate"])),
            }
            payload["max_rate"] = {
                "worker": str(ordered[-1]["worker"]),
                "rate": float(ordered[-1]["progress_rate"]),
                "rate_text": compact_rate_per_second(float(ordered[-1]["progress_rate"])),
            }
    speedup = run_speedup_value(summary)
    if speedup is not None:
        payload["speedup"] = round(speedup, 3)
        payload["speedup_text"] = f"{speedup:.1f}x"
    return payload

def run_speedup_field(summary: dict[str, Any]) -> str:
    metrics = active_worker_metrics(summary)
    if len(metrics) <= 1:
        return ""
    speedup = run_speedup_value(summary)
    return f"speedup={speedup:.1f}x" if speedup is not None else ""

def command_display_word(value: Any) -> str:
    text = str(value)
    if "~" in text and not any(char.isspace() for char in text):
        return text
    if re.fullmatch(r"\$[A-Za-z_][A-Za-z0-9_]*(?:[/:._A-Za-z0-9{}+-].*)?", text):
        return text
    if re.fullmatch(r"\{[A-Za-z_][A-Za-z0-9_]*\}", text):
        return text
    return shlex.quote(text)

def command_display_join(parts: list[str]) -> str:
    return " ".join(command_display_word(part) for part in parts if str(part))

def template_command_group(manifest: dict[str, Any]) -> dict[str, Any] | None:
    template = manifest.get("template")
    if not isinstance(template, dict):
        return None
    template_name = str(template.get("name") or "").strip()
    parameters = template.get("parameters")
    if not template_name or not isinstance(parameters, dict):
        return None
    parts = ["forge", template_name] if template_name == "sprt" else ["forge", "run", template_name]
    extra_args: list[str] = []
    for key, value in parameters.items():
        name = str(key)
        if name.endswith("_extra_args") and isinstance(value, list):
            extra_args.extend(str(item) for item in value if str(item))
            continue
        flag = "--" + name.replace("_", "-")
        if isinstance(value, bool):
            if value:
                parts.append(flag)
            continue
        if isinstance(value, list):
            for item in value:
                if item is not None and str(item):
                    parts.extend([flag, str(item)])
            continue
        if value is not None and str(value):
            parts.extend([flag, str(value)])
    parts.extend(extra_args)
    label = " ".join(parts[:2]) if template_name == "sprt" else " ".join(parts[:3])
    return {"label": label, "count": 1, "command": command_display_join(parts)}

def display_task_command(command: str) -> str:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        parts = list(lexer)
    except ValueError:
        return command
    if parts and parts[0].rsplit("/", 1)[-1] != "sprt":
        for index, part in enumerate(parts):
            if part.rsplit("/", 1)[-1] == "sprt":
                end = len(parts)
                for offset, item in enumerate(parts[index + 1:], start=index + 1):
                    if item in {";", "&&", "||"}:
                        end = offset
                        break
                parts = parts[index:end]
                break
    if not parts or parts[0].rsplit("/", 1)[-1] != "sprt":
        return command
    kept: list[str] = []
    skip_next = False
    shard_options = {"--games", "--log-dir", "--name"}
    normalized_options = {"--srand": "{seed}"}
    for part in parts:
        if skip_next:
            skip_next = False
            continue
        if part in shard_options:
            skip_next = True
            continue
        if any(part.startswith(option + "=") for option in shard_options):
            continue
        if part in normalized_options:
            kept.extend([part, normalized_options[part]])
            skip_next = True
            continue
        normalized = False
        for option, value in normalized_options.items():
            if part.startswith(option + "="):
                kept.append(f"{option}={value}")
                normalized = True
                break
        if normalized:
            continue
        kept.append(part)
    return command_display_join(kept)

def command_group_label(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    label = task_display_label(manifest, task) or "task"
    if label.startswith("sprt:"):
        return "sprt"
    return label

def run_command_groups(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    template_group = template_command_group(manifest)
    if template_group is not None:
        return [template_group]
    groups: dict[str, dict[str, Any]] = {}
    for task in manifest.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        command = display_task_command(str(task.get("command") or "").strip())
        if not command:
            continue
        label = command_group_label(manifest, task)
        key = f"{label}\n{command}"
        group = groups.setdefault(
            key,
            {
                "label": label,
                "count": 0,
                "command": command,
            },
        )
        group["count"] = int(group["count"]) + 1
    return list(groups.values())

def status_command_groups(summary: dict[str, Any]) -> list[dict[str, Any]]:
    commands = summary.get("commands")
    if isinstance(commands, list):
        return [item for item in commands if isinstance(item, dict)]
    manifest = summary.get("manifest_payload")
    return run_command_groups(manifest) if isinstance(manifest, dict) else []

def print_status_commands(summary: dict[str, Any], *, indent: str = "") -> None:
    for item in status_command_groups(summary):
        command = str(item.get("command") or "").strip()
        if command:
            print(f"{indent}command: {command}")

def run_status_payload(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary["counts"]
    fields = run_display_fields(summary)
    active_summary = active_worker_summary(summary)
    active_fields = active_section_fields(summary)
    manifest_payload = summary.get("manifest_payload")
    created_at = summary.get("created_at")
    if isinstance(manifest_payload, dict) and isinstance(manifest_payload.get("created_at"), str):
        created_at = manifest_payload["created_at"]
    payload = {
        key: copy.deepcopy(value)
        for key, value in summary.items()
        if key not in {"manifest_payload", "rows"}
    }
    payload.update({
        "run": summary["run"],
        "state": run_state(summary),
        "manifest": summary["manifest"],
        "tasks": summary["tasks"],
        "done": counts["done"],
        "running": counts["running"],
        "claimed": counts["claimed"],
        "pending": counts["pending"],
        "fail": counts["fail"],
        "stale": counts["stale"],
        "display": {
            "title": run_title(summary),
            "description": run_description_field(summary),
            "comment": run_comment_field(summary),
            "fields": split_eta_last(fields),
            "active_fields": active_fields,
            "active_summary": active_worker_summary_payload(summary),
            "active_summary_text": active_summary,
        },
        "sections": {
            "done": {
                "count": counts["done"],
                "groups": copy.deepcopy(summary["groups"]["done"]),
            },
            "active": {
                "count": counts["running"] + counts["claimed"] + counts["stale"],
                "tasks": copy.deepcopy(summary["active"]),
                "fields": active_fields,
                "summary": active_worker_summary_payload(summary),
                "summary_text": active_summary,
            },
            "workers": {
                "count": len(summary.get("worker_processes", [])),
                "processes": copy.deepcopy(summary.get("worker_processes", [])),
            },
            "pending": {
                "count": counts["pending"],
                "tasks": copy.deepcopy(summary["pending"]),
                "groups": copy.deepcopy(summary.get("pending_groups", {})),
            },
            "fail": {
                "count": counts["fail"],
                "groups": copy.deepcopy(summary["groups"]["fail"]),
            },
            "stale": {
                "count": counts["stale"],
                "groups": copy.deepcopy(summary["groups"]["stale"]),
            },
        },
    })
    # Pending shards are already represented by count/groups. Including every
    # pending task here makes status --json scale with the queue size and is
    # not useful to the normal CLI/API consumers.
    payload["sections"]["pending"].pop("tasks", None)
    if isinstance(created_at, str) and created_at.strip():
        payload["created_at"] = created_at.strip()
    metadata = summary.get("metadata")
    if isinstance(metadata, dict):
        payload.update(metadata)
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
        payload["progress_fields"] = summary_progress_fields(summary)
    if isinstance(manifest_payload, dict):
        commands = run_command_groups(manifest_payload)
        if commands:
            payload["commands"] = commands
    return payload

def run_title(summary: dict[str, Any]) -> str:
    return f"run={str(summary['run'])}"

def run_description_field(summary: dict[str, Any]) -> str:
    metadata = summary.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    description = str(metadata.get("description") or "").strip()
    if description:
        return f"desc={description}"
    return ""

def run_comment_field(summary: dict[str, Any]) -> str:
    metadata = summary.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    comment = str(metadata.get("comment") or "").strip()
    if comment:
        return '"' + comment.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return ""

def summary_progress_fields(summary: dict[str, Any]) -> list[str]:
    progress = summary_progress(summary)
    if not progress:
        return []
    if progress.startswith("progress="):
        progress = progress[len("progress="):]
    return [part.strip() for part in progress.split(", ") if part.strip()]

def split_eta_last(fields: list[str]) -> list[str]:
    eta = [field for field in fields if field.startswith("eta=")]
    rest = [field for field in fields if not field.startswith("eta=")]
    return rest + eta

def elo_value_color(value: str) -> str:
    try:
        elo = float(value.rstrip("%"))
    except ValueError:
        return "yellow"
    if math.isnan(elo) or elo == 0.0:
        return "yellow"
    return "green" if elo > 0.0 else "red"

def status_field_color(field: str, summary: dict[str, Any] | None = None) -> str | None:
    key, sep, value = field.partition("=")
    if key == "tasks":
        if summary and summary["counts"]["fail"]:
            return "red"
        if summary and run_done(summary):
            return "green"
        return "yellow"
    if key in {"elo", "elo~", "Elo"} and sep:
        return elo_value_color(value)
    if key in {"fail", "stale"}:
        return "red"
    if key in {"active", "claimed", "pending", "running", "state"}:
        return "yellow"
    if key == "eta" and field == "eta=unknown":
        return "yellow"
    if key in {"run", "eta", "speedup"}:
        return "blue"
    return None

def color_progress_text(text: str, *, color_enabled: bool) -> str:
    fields = [field.strip() for field in text.split(", ")]
    colored: list[str] = []
    for field in fields:
        if field == "finalizing":
            colored.append(colorize(field, "green", enabled=color_enabled))
        elif field.partition("=")[0] in {"elo", "elo~", "Elo"}:
            colored.append(color_status_field(field, color_enabled=color_enabled))
        elif field == "eta=unknown":
            colored.append(color_status_field(field, color_enabled=color_enabled))
        elif field.startswith("eta="):
            colored.append(color_status_field(field, color_enabled=color_enabled))
        else:
            colored.append(field)
    return ", ".join(colored)

def color_status_field(
    field: str,
    *,
    summary: dict[str, Any] | None = None,
    color_enabled: bool,
) -> str:
    color = status_field_color(field, summary)
    if not color:
        return field
    if "=" in field:
        key, value = field.split("=", 1)
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            return f'{key}="{colorize(value[1:-1], color, enabled=color_enabled)}"'
        return f"{key}={colorize(value, color, enabled=color_enabled)}"
    return colorize(field, color, enabled=color_enabled)

def color_status_fields(
    fields: list[str],
    *,
    summary: dict[str, Any] | None = None,
    color_enabled: bool,
) -> list[str]:
    return [
        color_status_field(field, summary=summary, color_enabled=color_enabled)
        for field in fields
    ]

def run_progress_fields(summary: dict[str, Any]) -> list[str]:
    fields = summary_progress_fields(summary)
    if summary_kind(summary) == "spsa":
        return split_eta_last([
            field
            for field in fields
            if field.startswith("eta=") or field.startswith("iterations=")
        ])
    return split_eta_last([
        field
        for field in fields
        if (
            field.startswith("eta=")
            or field.startswith("games=")
            or field.split("=", 1)[0] in SPRT_PROGRESS_FIELD_KEYS
        )
    ])

def run_display_fields(summary: dict[str, Any]) -> list[str]:
    counts = summary["counts"]
    progress = run_progress_fields(summary)
    eta = [field for field in progress if field.startswith("eta=")]
    remaining = [field for field in progress if not field.startswith("eta=")]
    ordered: list[str] = []
    for key in ("elo", "llr", "los", "ci", "draw", "games"):
        matches = [
            field for field in remaining
            if field.split("=", 1)[0].removesuffix("~") == key
        ]
        ordered.extend(matches)
        remaining = [field for field in remaining if field not in matches]
    fields = [
        *run_state_fields(summary),
        *ordered,
        *remaining,
        f"tasks={counts['done']}/{summary['tasks']}",
    ]
    if speedup := run_speedup_field(summary):
        fields.append(speedup)
    if counts["fail"]:
        fields.append(f"fail={counts['fail']}")
    if counts["stale"]:
        fields.append(f"stale={counts['stale']}")
    return [*fields, *eta]

def section_progress_fields(summary: dict[str, Any]) -> list[str]:
    return split_eta_last(summary_progress_fields(summary))

def active_section_fields(summary: dict[str, Any]) -> list[str]:
    run_fields = set(run_progress_fields(summary))
    worker_summary = active_worker_summary(summary)
    if worker_summary:
        worker_fields = worker_summary.split()
        worker_keys = {field.split("=", 1)[0] for field in worker_fields}
        progress_fields = [
            field for field in summary_progress_fields(summary)
            if field not in run_fields and field.split("=", 1)[0] not in worker_keys
        ]
        return split_eta_last([*progress_fields, *worker_fields])
    return [field for field in section_progress_fields(summary) if field not in run_fields]

def format_status_key(key: str, values: list[int | str]) -> str:
    return key

def color_count_line(label: str, count: int, color: str, *, color_enabled: bool) -> str:
    return f"{label}: {colorize(str(count), color, enabled=color_enabled)}"

def color_failure_summary(count: int, *, color_enabled: bool) -> str:
    noun = "run" if count == 1 else "runs"
    return f"failed: {colorize(str(count), 'red', enabled=color_enabled)} {noun} (use forge status --failed)"

def print_run_status(
    summaries: list[dict[str, Any]],
    *,
    color_enabled: bool,
    as_json: bool,
    filter_name: str | None,
    include_started_pending: bool = False,
    collapse_inactive_failures: bool = False,
    show_details: bool = False,
    show_done: bool = False,
    show_commands: bool = False,
    runs_label: str | None = None,
) -> int:
    filtered = filtered_run_summaries(
        summaries,
        filter_name,
        include_started_pending=include_started_pending,
    )
    if as_json:
        collapsed_failures = [
            summary for summary in filtered
            if collapse_inactive_failures and run_failed(summary) and run_active_count(summary) == 0
        ]
        payload = {
            "runs": [run_status_payload(item) for item in filtered],
            "collapsed_failures": [str(item["run"]) for item in collapsed_failures],
            "collapsed_failures_count": len(collapsed_failures),
        }
        print(json.dumps(payload, sort_keys=True))
        return 0 if all(not run_failed(item) for item in filtered) else 1
    if not filtered:
        labels = {
            None: "active",
            "all": "",
            "done": "completed",
            "failed": "failed",
        }
        label = labels.get(filter_name, "")
        qualifier = f"{label} " if label else ""
        print(f"no {qualifier}Forge runs found in {runs_label or runs_dir()}")
        return 0
    collapsed_failures = [
        summary for summary in filtered
        if collapse_inactive_failures and run_failed(summary) and run_active_count(summary) == 0
    ]
    collapsed_ids = {id(summary) for summary in collapsed_failures}
    visible = [
        summary for summary in filtered
        if id(summary) not in collapsed_ids
    ]
    if collapsed_failures:
        print(color_failure_summary(len(collapsed_failures), color_enabled=color_enabled))
        if visible and show_details:
            print()
    for index, summary in enumerate(visible):
        if show_details and index:
            print()
        counts = summary["counts"]
        active = counts["running"] + counts["claimed"] + counts["stale"]
        if show_details:
            print(color_status_field(run_title(summary), summary=summary, color_enabled=color_enabled))
            if description := run_description_field(summary):
                print(f"  {color_status_field(description, summary=summary, color_enabled=color_enabled)}")
            if comment := run_comment_field(summary):
                print(f"  {color_status_field(comment, summary=summary, color_enabled=color_enabled)}")
            if show_commands:
                print_status_commands(summary, indent="  ")
        if show_details:
            fields = run_display_fields(summary)
        else:
            fields = [
                run_title(summary),
                f"active={active}",
                f"running={counts['running']}",
                f"claimed={counts['claimed']}",
                *section_progress_fields(summary),
                f"fail={counts['fail']}",
                f"pending={counts['pending']}",
                f"stale={counts['stale']}",
            ]
        indent = "  " if show_details else ""
        print(f"{indent}{' '.join(color_status_fields(fields, summary=summary, color_enabled=color_enabled))}")
        if show_details:
            print_status_details(
                summary,
                color_enabled=color_enabled,
                show_done=show_done,
                show_diagnostics=True,
                indent="  ",
            )
    return 0 if all(not run_failed(item) for item in filtered) else 1

def load_run_manifest(run: str | Path) -> tuple[Path, dict[str, Any]]:
    path = run_manifest_path(run)
    if not path.exists():
        raise SystemExit(f"{run}: no Forge run manifest found at {path}")
    return path, load_manifest(path)

def run_summary_for(run: str | Path, *, lease_seconds: int) -> dict[str, Any]:
    path, manifest = load_run_manifest(run)
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    summary["manifest"] = str(path)
    summary["run"] = path.parent.name
    summary["metadata"] = manifest_metadata(manifest)
    annotate_run_ordering(summary, manifest_path=path, manifest=manifest)
    return summary

def replace_resume_count_arg(command: str, count_arg: str, count: int) -> str:
    if not count_arg or any(char.isspace() for char in count_arg):
        raise SystemExit(f"invalid resume count argument: {count_arg!r}")
    pattern = re.compile(
        rf"(?<!\S){re.escape(count_arg)}(?:(?P<equals>=)(?P<equal_value>\S+)|(?P<space>\s+)(?P<value>\S+))"
    )
    matches = list(pattern.finditer(command))
    if len(matches) != 1:
        raise SystemExit(
            f"resume requires exactly one {count_arg} argument; found {len(matches)} in: {command}"
        )
    match = matches[0]
    replacement = f"{count_arg}={count}" if match.group("equals") else f"{count_arg} {count}"
    return command[:match.start()] + replacement + command[match.end():]

def resumed_task_id(root: str, existing: set[str]) -> str:
    sequence = 1
    while True:
        candidate = f"{root}_resume{sequence}"
        if candidate not in existing:
            return candidate
        sequence += 1

def resumed_task_label(label: Any, total: int) -> str | None:
    if label is None:
        return None
    text = str(label)
    if re.search(r":\d+$", text):
        return re.sub(r":\d+$", f":{total}", text)
    return text

def partial_task_done_payload(
    manifest: dict[str, Any],
    task: dict[str, Any],
    *,
    recovered_games: int,
) -> dict[str, Any]:
    finished_at = now()
    outputs = task_outputs(task)
    return {
        "schema": SCHEMA,
        "task": task["id"],
        "index": task.get("index"),
        "host": str(task.get("claim_worker") or socket.gethostname()),
        "platform": platform.platform(),
        "started_at": finished_at,
        "finished_at": finished_at,
        "elapsed_s": 0.0,
        "command": format_command(manifest, task),
        "rc": 0,
        "attempt": 1,
        "log": str(task_log_path(manifest, task)),
        "manifest_sha256": manifest_checksum(manifest),
        "task_execution_sha256": task_execution_checksum(manifest, task),
        "outputs": [file_record(path) for path in outputs],
        "partial_segment": True,
        "recovered_games": recovered_games,
    }

def prepare_run_resume(manifest_path: Path, manifest: dict[str, Any]) -> dict[str, int]:
    incomplete = [task for task in manifest["tasks"] if not task_complete(manifest, task)]
    if not incomplete:
        return {"preserved": 0, "continued": 0}

    history_max = max(
        (int(float(entry["x"])) for entry in load_progress_history(manifest)),
        default=0,
    )
    stop_payload = read_state_json(stop_request_path(manifest))
    stopped_progress = stop_payload.get("task_progress") if isinstance(stop_payload, dict) else None
    if not isinstance(stopped_progress, dict):
        stopped_progress = {}
    stopped_total = 0
    for task in manifest["tasks"]:
        config_total = progress_config_unit_total(task)
        if config_total is None:
            continue
        _unit, total = config_total
        if task_complete(manifest, task):
            stopped_total += total
            continue
        stopped_done = parse_int(stopped_progress.get(str(task["id"]))) or 0
        stopped_total += min(total, max(0, stopped_done))
    resumable = [task for task in incomplete if task_resume_strategy(task) == "split-progress"]
    kind = str(manifest_metadata(manifest).get("kind") or "").lower()
    if not resumable:
        recorded = max(history_max, stopped_total)
        if kind == "sprt" and recorded > 0:
            raise SystemExit(
                f"resume: {manifest_path.parent.name} cannot safely resume: "
                f"{recorded} recorded games have no resumable shard state; restart refused"
            )
        return {"preserved": 0, "continued": 0}

    recoverable = 0
    recorded_at_stop = 0
    plans: list[tuple[str, int, int]] = []
    for task in manifest["tasks"]:
        config_total = progress_config_unit_total(task)
        if config_total is None:
            continue
        _unit, total = config_total
        if task_complete(manifest, task):
            done_payload = task_done_payload(manifest, task)
            if isinstance(done_payload, dict) and (done_payload.get("soft_cancel") or done_payload.get("skipped")):
                progress = task_log_progress_data(manifest, task)
                observed = progress.get("done") if isinstance(progress, dict) else 0
                completed = min(total, observed) if isinstance(observed, int) else 0
                recoverable += completed
                recorded_at_stop += completed
            else:
                recoverable += total
                recorded_at_stop += total
            continue
        if task_resume_strategy(task) != "split-progress":
            continue
        progress = task_log_progress_data(manifest, task)
        observed = progress.get("done") if isinstance(progress, dict) else 0
        done = min(total, observed) if isinstance(observed, int) and observed > 0 else 0
        stopped_done = parse_int(stopped_progress.get(str(task["id"]))) or 0
        recorded_at_stop += min(total, max(0, stopped_done))
        if done <= 0:
            continue
        missing = [path for path in task_outputs(task) if not path.is_file()]
        if missing:
            raise SystemExit(
                f"resume: {manifest_path.parent.name} cannot safely resume {task['id']}: "
                f"checkpoint output is missing: {missing[0]}"
            )
        recoverable += done
        plans.append((str(task["id"]), done, total))

    recorded = max(history_max, recorded_at_stop)
    if recoverable < recorded:
        raise SystemExit(
            f"resume: {manifest_path.parent.name} cannot safely resume: "
            f"only {recoverable} of {recorded} recorded games are recoverable; restart refused"
        )
    if not plans:
        return {"preserved": 0, "continued": 0}

    updated = copy.deepcopy(manifest)
    tasks_by_id = {str(task["id"]): task for task in updated["tasks"]}
    existing_ids = set(tasks_by_id)
    next_index = max((int(task.get("index", -1)) for task in updated["tasks"]), default=-1) + 1
    sealed: list[tuple[dict[str, Any], int]] = []
    continued = 0
    for task_id, done, original_total in plans:
        task = tasks_by_id[task_id]
        progress = task.get("progress")
        if not isinstance(progress, dict):
            raise SystemExit(f"resume: {task_id} has no progress configuration")
        segment = task.get("resume_segment")
        if isinstance(segment, dict):
            continued_by = str(segment.get("continued_by") or "")
            if continued_by and continued_by not in existing_ids:
                raise SystemExit(f"resume: {task_id} continuation {continued_by} is missing")
            progress["total"] = done
            sealed.append((task, done))
            continue

        remaining = original_total - done
        continuation_id = ""
        if remaining > 0:
            root = str(task.get("resume_root") or task_id)
            continuation_id = resumed_task_id(root, existing_ids)
            continuation = copy.deepcopy(task)
            continuation["id"] = continuation_id
            continuation["index"] = next_index
            continuation["resume_root"] = root
            continuation.pop("resume_segment", None)
            continuation["command"] = replace_resume_count_arg(
                str(continuation.get("command") or "").replace(task_id, continuation_id),
                str(task_resume_config(task).get("count_arg") or ""),
                remaining,
            )
            continuation["outputs"] = [
                str(path).replace(task_id, continuation_id)
                for path in task.get("outputs", [])
            ]
            if any(old == new for old, new in zip(task.get("outputs", []), continuation["outputs"])):
                raise SystemExit(f"resume: {task_id} output path does not contain its task id")
            continuation_progress = continuation.get("progress")
            if not isinstance(continuation_progress, dict):
                raise SystemExit(f"resume: {task_id} has no progress configuration")
            continuation_progress["total"] = remaining
            label = resumed_task_label(continuation.get("label"), remaining)
            if label is not None:
                continuation["label"] = label
            updated["tasks"].append(continuation)
            tasks_by_id[continuation_id] = continuation
            existing_ids.add(continuation_id)
            next_index += 1
            continued += 1

        progress["total"] = done
        label = resumed_task_label(task.get("label"), done)
        if label is not None:
            task["label"] = label
        task["resume_segment"] = {
            "completed": done,
            "original_total": original_total,
            "continued_by": continuation_id,
        }
        sealed.append((task, done))

    validate_manifest_payload(updated, label=f"resume {manifest_path.parent.name}")
    write_json(manifest_path, updated)
    for task, done in sealed:
        write_json(
            task_done_path(updated, task),
            partial_task_done_payload(updated, task, recovered_games=done),
        )
        task_fail_path(updated, task).unlink(missing_ok=True)
        task_running_path(updated, task).unlink(missing_ok=True)
        task_cancel_path(updated, task).unlink(missing_ok=True)
        shutil.rmtree(task_claim_dir(updated, task), ignore_errors=True)
    return {"preserved": sum(done for _task, done in sealed), "continued": continued}

def request_stop_for_run(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    lease_seconds: int,
    reason: str | None = None,
    preserve_active_tasks: bool = False,
) -> dict[str, Any]:
    from .workers import kill_run_workers

    active_tasks = [
        task
        for task in manifest["tasks"]
        if task_state(manifest, task, lease_seconds=lease_seconds) in {"claimed", "running", "stale"}
    ]
    payload = {
        "schema": SCHEMA,
        "run": manifest_path.parent.name,
        "requested_at": now(),
        "requested_epoch": epoch(),
        "host": socket.gethostname(),
    }
    task_progress: dict[str, int] = {}
    for task in active_tasks:
        progress = task_heartbeat_progress_data(manifest, task)
        done = progress.get("done") if isinstance(progress, dict) else None
        if isinstance(done, int) and done > 0:
            task_progress[str(task["id"])] = done
    if task_progress:
        payload["task_progress"] = task_progress
    if reason:
        payload["reason"] = reason
    write_json(stop_request_path(manifest), payload)
    cleared: list[str] = []
    queue_path = queue_request_path(manifest)
    if queue_path.exists():
        queue_path.unlink()
        cleared.append("queue")
    launch_path = launch_request_path(manifest)
    if launch_path.exists():
        launch_path.unlink()
        cleared.append("launch")
    reset: list[str] = []
    killed = 0
    released: list[str] = []
    if not preserve_active_tasks:
        killed = kill_run_workers(manifest_path.parent)
        released = release_active_claims(manifest, lease_seconds=lease_seconds)
        for task in active_tasks:
            task_id = str(task["id"])
            if task_id in released:
                continue
            release_claim(manifest, task)
            released.append(task_id)
        for task in active_tasks:
            if task_complete(manifest, task):
                continue
            task_done_path(manifest, task).unlink(missing_ok=True)
            task_fail_path(manifest, task).unlink(missing_ok=True)
            task_running_path(manifest, task).unlink(missing_ok=True)
            task_cancel_path(manifest, task).unlink(missing_ok=True)
            shutil.rmtree(task_claim_dir(manifest, task), ignore_errors=True)
            if not task_preserves_partial_outputs(task):
                for output in task_outputs(task):
                    if output.is_dir() and not output.is_symlink():
                        shutil.rmtree(output, ignore_errors=True)
                    else:
                        output.unlink(missing_ok=True)
            reset.append(str(task["id"]))
    if not preserve_active_tasks:
        finalizer = manifest.get("finalize")
        if isinstance(finalizer, dict):
            from .labeling import cleanup_partials

            cleanup_partials(manifest, finalizer)
    return {
        "cleared": cleared,
        "killed": killed,
        "released": released,
        "reset": reset,
    }

def soft_cancel_done_payload(
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
    task: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    finished_at = now()
    try:
        command = format_command(manifest, task)
    except SystemExit:
        command = str(task.get("command") or "")
    return {
        "schema": SCHEMA,
        "task": task["id"],
        "index": task.get("index"),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "started_at": finished_at,
        "finished_at": finished_at,
        "elapsed_s": 0.0,
        "command": command,
        "rc": 0,
        "log": str(task_log_path(manifest, task)),
        "manifest_sha256": manifest_checksum(manifest),
        "task_execution_sha256": task_execution_checksum(manifest, task),
        "outputs": [],
        "soft_cancel": True,
        "skipped": True,
        "skip_reason": "aggregate_sprt_decision",
        "decision": decision,
        "run": manifest_path.parent.name,
    }

def finalize_sprt_aggregate_decision(
    manifest_path: Path,
    manifest: dict[str, Any],
    decision: dict[str, Any],
    *,
    lease_seconds: int,
) -> dict[str, Any]:
    state_dir(manifest).mkdir(parents=True, exist_ok=True)
    stop_payload = {
        "schema": SCHEMA,
        "run": manifest_path.parent.name,
        "requested_at": now(),
        "requested_epoch": epoch(),
        "host": socket.gethostname(),
        "reason": "aggregate_sprt_decision",
        "soft_cancel_tasks": True,
        "decision": decision,
    }
    changed = False
    existing_stop = read_state_json(stop_request_path(manifest))
    if not isinstance(existing_stop, dict) or existing_stop.get("reason") != "aggregate_sprt_decision":
        write_json(stop_request_path(manifest), stop_payload)
        changed = True
    for path in (queue_request_path(manifest), launch_request_path(manifest)):
        if path.exists():
            path.unlink()
            changed = True

    skipped = 0
    cancelled = 0
    for task in manifest["tasks"]:
        state = task_state(manifest, task, lease_seconds=lease_seconds)
        if state in {"done", "fail"}:
            continue
        cancel_payload = {
            **stop_payload,
            "task": task["id"],
            "index": task.get("index"),
        }
        if state in {"running", "claimed"}:
            cancel_path = task_cancel_path(manifest, task)
            if not cancel_path.exists():
                write_json(cancel_path, cancel_payload)
                changed = True
                cancelled += 1
            continue
        payload = soft_cancel_done_payload(
            manifest_path=manifest_path,
            manifest=manifest,
            task=task,
            decision=decision,
        )
        write_json(task_done_path(manifest, task), payload)
        task_fail_path(manifest, task).unlink(missing_ok=True)
        task_cancel_path(manifest, task).unlink(missing_ok=True)
        release_claim(manifest, task)
        skipped += 1
        changed = True
    return {
        "changed": changed,
        "skipped": skipped,
        "cancelled": cancelled,
        "decision": decision,
    }

def cmd_stop(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_state_control_on_coordinator("stop", args, coordinator_runs)
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
        stop_result = request_stop_for_run(
            path,
            manifest,
            lease_seconds=args.lease_seconds,
            preserve_active_tasks=not bool(getattr(args, "force", False)),
        )
        stopped += 1
        if bool(getattr(args, "force", False)):
            message = f"stop requested: {path.parent.name}"
            if stop_result["released"]:
                message += f" released={len(stop_result['released'])}"
            if stop_result["killed"]:
                message += f" killed={stop_result['killed']}"
            if stop_result["cleared"]:
                message += f" cleared={','.join(stop_result['cleared'])}"
            if stop_result["reset"]:
                message += f" reset={len(stop_result['reset'])}"
            print(message)
            continue
        if not bool(getattr(args, "wait", True)):
            print(f"stop requested: {path.parent.name} draining active chunks")
            continue
        if stop_result["cleared"]:
            print(f"stop requested: {path.parent.name} waiting; cleared={','.join(stop_result['cleared'])}")
        else:
            print(f"stop requested: {path.parent.name} waiting for active chunks to finish")
        while True:
            summary = status_summary(load_manifest(path), lease_seconds=args.lease_seconds)
            counts = summary["counts"]
            active = counts["running"] + counts["claimed"] + counts["stale"]
            if active == 0:
                print(f"stop complete: {path.parent.name}")
                break
            time.sleep(max(0.5, float(args.interval_seconds)))
    if stopped == 0:
        print("no current runs to stop")
    return 0

def cmd_drain(args: argparse.Namespace) -> int:
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_state_control_on_coordinator("drain", args, coordinator_runs)
    if not args.all and not args.run:
        raise SystemExit("drain requires a run name or --all")
    worker = str(args.worker or "").strip()
    if not worker:
        raise SystemExit("drain requires --worker")
    targets = (
        [Path(item["manifest"]) for item in load_run_summaries(lease_seconds=args.lease_seconds) if run_current(item)]
        if args.all else
        [run_manifest_path(args.run)]
    )
    changed = 0
    for path in targets:
        manifest = load_manifest(path)
        marker = worker_drain_path(manifest, worker)
        if args.clear:
            if marker.exists():
                marker.unlink()
                changed += 1
                print(f"drain cleared: {path.parent.name} {worker}")
            continue
        drain_dir(manifest).mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": SCHEMA,
            "run": path.parent.name,
            "worker": worker,
            "requested_at": now(),
            "requested_epoch": epoch(),
            "host": socket.gethostname(),
        }
        write_json(marker, payload)
        changed += 1
        print(f"drain requested: {path.parent.name} {worker}")
    if changed == 0:
        action = "cleared" if args.clear else "requested"
        print(f"no drains {action}")
    return 0

def confirm_delete(paths: list[Path], *, force: bool) -> None:
    if force:
        return
    if not sys.stdin.isatty():
        raise SystemExit("pass --force to delete runs non-interactively")
    names = ", ".join(path.name for path in paths)
    answer = input(f"delete Forge run(s) {names}? type 'yes' to confirm: ")
    if answer != "yes":
        raise SystemExit("delete cancelled")

def mark_deleted_run_for_web(run: str) -> None:
    try:
        from .web import _delete_web_run

        _delete_web_run(run)
    except ModuleNotFoundError as exc:
        if exc.name != "forge_lib.web":
            raise
    except Exception as exc:
        print(f"warning: web state not updated for {run}: {exc}", file=sys.stderr)

def cmd_del(args: argparse.Namespace) -> int:
    delete_done = bool(getattr(args, "done", False))
    delete_all = bool(getattr(args, "all", False))
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        rc = run_state_control_on_coordinator("del", args, coordinator_runs)
        if rc == 0 and args.run:
            mark_deleted_run_for_web(str(args.run))
        return rc
    if (delete_done or delete_all) and args.run:
        raise SystemExit("del accepts a run name, --done, or --all")
    if not delete_done and not delete_all and not args.run:
        raise SystemExit("del requires a run name, --done, or --all")
    summaries = load_run_summaries(lease_seconds=args.lease_seconds)
    targets: list[Path] = []
    kept: list[str] = []
    if delete_all:
        for item in summaries:
            path = Path(item["manifest"]).parent
            if run_state(item) in {"current", "queued", "stalled"}:
                kept.append(path.name)
            else:
                targets.append(path)
    elif delete_done:
        targets = [Path(item["manifest"]).parent for item in summaries if run_done(item)]
    else:
        summary = run_summary_for(args.run, lease_seconds=args.lease_seconds)
        if run_current(summary) and not args.force:
            raise SystemExit(f"{args.run}: run is active; stop it first or pass --force")
        targets = [Path(summary["manifest"]).parent]
    if not targets:
        print("no inactive runs matched" if delete_all else "no runs matched")
        if kept:
            print(f"kept active: {', '.join(kept)}")
        return 0
    confirm_delete(targets, force=bool(args.force))
    for path in targets:
        shutil.rmtree(path)
        mark_deleted_run_for_web(path.name)
        print(f"deleted: {path.name}")
    if kept:
        print(f"kept active: {', '.join(kept)}")
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
        payload = read_state_json(path)
        if payload is None:
            continue
        worker = payload.get("worker")
        if isinstance(worker, dict):
            worker = worker.get("name") or worker.get("host")
        worker = worker or payload.get("host")
        if worker:
            return worker_group_name(str(worker))
    return ""

def status_worker_id(manifest: dict[str, Any], task: dict[str, Any], state: str) -> str:
    paths: list[Path] = []
    if state == "done":
        paths.append(task_done_path(manifest, task))
    elif state == "fail":
        paths.append(task_fail_path(manifest, task))
    elif state in {"claimed", "running", "stale"}:
        paths.extend([
            task_claim_path(manifest, task),
            task_running_path(manifest, task),
        ])
    for path in paths:
        payload = read_state_json(path)
        if payload is None:
            continue
        worker_id = payload.get("worker_id")
        worker = payload.get("worker")
        if isinstance(worker, dict):
            worker_id = worker_id or worker.get("process_id")
        if worker_id:
            return str(worker_id)
    return status_worker(manifest, task, state)

def worker_process_status_rows(
    manifest: dict[str, Any],
    *,
    pending: int,
    stale_seconds: float = 30.0,
    starved_seconds: float = 15.0,
) -> list[dict[str, Any]]:
    directory = worker_process_dir(manifest)
    if not directory.is_dir():
        return []
    current_epoch = epoch()
    rows = []
    for path in sorted(directory.glob("*.json")):
        payload = read_state_json(path)
        if payload is None:
            continue
        worker_id = str(payload.get("worker_id") or "").strip()
        if not worker_id:
            continue
        state = str(payload.get("state") or "unknown")
        heartbeat_epoch = parse_float(payload.get("heartbeat_epoch")) or 0.0
        state_since_epoch = parse_float(payload.get("state_since_epoch")) or heartbeat_epoch
        heartbeat_age = max(0.0, current_epoch - heartbeat_epoch) if heartbeat_epoch else 0.0
        state_age = max(0.0, current_epoch - state_since_epoch) if state_since_epoch else 0.0
        if state == "stopped":
            continue
        if heartbeat_epoch and heartbeat_age > stale_seconds:
            display_state = "stale"
        elif state == "idle" and pending > 0 and state_age >= starved_seconds:
            display_state = "starved"
        else:
            display_state = state
        rows.append({
            "worker": str(payload.get("worker") or worker_group_name(worker_id)),
            "worker_id": worker_id,
            "state": display_state,
            "state_age_s": round(state_age, 1),
            "heartbeat_age_s": round(heartbeat_age, 1),
        })
    return rows

def log_tail(path: Path, *, max_bytes: int = 8192) -> str:
    with path.open("rb") as file:
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(max(0, size - max_bytes))
        return file.read().decode("utf-8", errors="replace")

def task_failure_reason(manifest: dict[str, Any], task: dict[str, Any]) -> str | None:
    """Return a concise actionable reason from a failed task's log."""
    payload = read_state_json(task_fail_path(manifest, task))
    if not isinstance(payload, dict):
        return None
    log_name = payload.get("log")
    path = Path(str(log_name)) if log_name else task_log_path(manifest, task)
    try:
        text = log_tail(path, max_bytes=16384)
    except (OSError, ValueError):
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None
    # Prefer explicit diagnostics, then use the final non-bookkeeping line.
    diagnostic = next(
        (
            line
            for line in reversed(lines)
            if any(token in line.lower() for token in ("error", "fatal", "exception", "traceback"))
        ),
        None,
    )
    reason = diagnostic or next(
        (line for line in reversed(lines) if " done " not in line),
        lines[-1],
    )
    return reason[:500]

def parse_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
    try:
        return int(str(value).replace("_", "").replace(",", ""))
    except ValueError:
        return None

def parse_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else None
    try:
        return float(str(value).replace("_", "").replace(",", ""))
    except ValueError:
        return None

SPRT_PROGRESS_RE = re.compile(
    r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]\s+"
    r"Elo\s+(?P<elo>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+\+/-\s+"
    r"(?P<err>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+\|\s+"
    r"LLR\s+(?P<llr>[+-]?\d+(?:\.\d+)?)/(?P<llr_bound>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+"
    r"\(\s*(?P<llr_pct>[+-]?\d+(?:\.\d+)?)%\)\s+\|\s+"
    r"LOS\s+(?P<los>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%"
    r"(?:\s+\|\s+WDL\s+(?P<wdl>\d+/\d+/\d+)"
    r"\s+\|\s+Ptnml\s+(?P<pentanomial>\d+-\d+-\d+-\d+-\d+))?\s+\|\s+"
    r"draw\s+(?P<draw>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%",
    re.IGNORECASE,
)

FASTCHESS_ELO_RE = re.compile(
    r"Elo\s+difference:\s+(?P<elo>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+\+/-\s+"
    r"(?P<err>[+-]?(?:\d+(?:\.\d+)?|inf|nan)),\s+"
    r"LOS:\s+(?P<los>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%,\s+"
    r"DrawRatio:\s+(?P<draw>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%",
    re.IGNORECASE,
)

FASTCHESS_SPRT_RE = re.compile(
    r"SPRT:\s+llr\s+(?P<llr>[+-]?\d+(?:\.\d+)?)\s+"
    r"\((?P<llr_pct>[+-]?\d+(?:\.\d+)?)%\),\s+"
    r"lbound\s+(?P<lbound>[+-]?(?:\d+(?:\.\d+)?|inf|nan)),\s+"
    r"ubound\s+(?P<ubound>[+-]?(?:\d+(?:\.\d+)?|inf|nan))",
    re.IGNORECASE,
)
FASTCHESS_GAMES_RE = re.compile(
    r"Games:\s+(?P<done>\d+),\s+Wins:\s+(?P<wins>\d+),\s+"
    r"Losses:\s+(?P<losses>\d+),\s+Draws:\s+(?P<draws>\d+)",
    re.IGNORECASE,
)

SPRT_PROGRESS_FIELD_KEYS = {"elo", "ci", "llr", "los", "wdl", "pentanomial", "draw"}

def sprt_progress_line_fields(line: str) -> dict[str, str]:
    match = SPRT_PROGRESS_RE.search(line)
    if not match:
        return {}
    values = match.groupdict()
    llr = values["llr"].strip()
    llr_bound = values["llr_bound"].strip()
    llr_pct = values["llr_pct"].strip()
    fields = {
        "elo": values["elo"].strip(),
        "ci": values["err"].strip(),
        "llr": f"{llr}/{llr_bound} ({llr_pct}%)",
        "los": values["los"].strip(),
        "draw": values["draw"].strip(),
    }
    for key in ("wdl", "pentanomial"):
        if values.get(key):
            fields[key] = values[key].strip()
    return fields

def fastchess_progress_fields(text: str) -> dict[str, str]:
    elo_match = FASTCHESS_ELO_RE.search(text)
    sprt_match = FASTCHESS_SPRT_RE.search(text)
    if not elo_match or not sprt_match:
        return {}
    elo_vals = elo_match.groupdict()
    sprt_vals = sprt_match.groupdict()
    llr = sprt_vals["llr"].strip()
    ubound = sprt_vals["ubound"].strip()
    llr_pct = sprt_vals["llr_pct"].strip()
    return {
        "elo": elo_vals["elo"].strip(),
        "ci": elo_vals["err"].strip(),
        "llr": f"{llr}/{ubound} ({llr_pct}%)",
        "los": elo_vals["los"].strip(),
        "draw": elo_vals["draw"].strip(),
    }

def sprt_progress_line_data(line: str, config: dict[str, Any], *, eta_epoch: float) -> dict[str, Any] | None:
    match = SPRT_PROGRESS_RE.search(line)
    if not match:
        return None
    done = parse_int(match.group("done"))
    total = parse_int(match.group("total"))
    if done is None:
        return None
    data: dict[str, Any] = {
        "done": done,
        "unit": str(config.get("unit") or "games"),
        "output_unit": str(config.get("output_unit") or "output"),
        "kind": "metadata",
        "eta_epoch": eta_epoch,
        "fields": sprt_progress_line_fields(line),
    }
    if total is not None:
        data["total"] = total
    return normalize_progress_data(data)

def fastchess_games_line_data(line: str, config: dict[str, Any], *, eta_epoch: float) -> dict[str, Any] | None:
    match = FASTCHESS_GAMES_RE.search(line)
    if not match:
        return None
    values = match.groupdict()
    done = parse_int(values.get("done"))
    if done is None:
        return None
    data: dict[str, Any] = {
        "done": done,
        "unit": str(config.get("unit") or "games"),
        "output_unit": str(config.get("output_unit") or "output"),
        "kind": "metadata",
        "eta_epoch": eta_epoch,
        "fields": {
            "wins": values["wins"],
            "losses": values["losses"],
            "draws": values["draws"],
        },
    }
    total_value = config.get("total")
    if total_value is not None:
        data["total"] = total_value if isinstance(total_value, int) else parse_int(str(total_value))
    return normalize_progress_data(data)

def progress_field_float(fields: dict[str, str], *names: str) -> float | None:
    for name in names:
        value = parse_float(fields.get(name))
        if value is not None and math.isfinite(value):
            return value
    return None

def progress_field_llr(fields: dict[str, str]) -> tuple[float, float | None]:
    value = str(fields.get("llr") or fields.get("LLR") or "").strip()
    match = re.match(
        r"(?P<llr>[+-]?\d+(?:\.\d+)?)(?:/(?P<bound>[+-]?(?:\d+(?:\.\d+)?|inf|nan)))?",
        value,
        re.IGNORECASE,
    )
    if not match:
        return (math.nan, None)
    llr = parse_float(match.group("llr"))
    bound = parse_float(match.group("bound"))
    if llr is None or not math.isfinite(llr):
        return (math.nan, None)
    if bound is None or not math.isfinite(bound):
        return (llr, None)
    return (llr, abs(bound))

def format_llr_field(llr: float, bound: float | None, mixed_bound: bool) -> str:
    if bound is None or mixed_bound or bound <= 0:
        return f"llr={llr:.2f}"
    pct = llr / bound * 100.0
    return f"llr={llr:.2f}/{bound:.2f} ({pct:.0f}%)"

def sprt_progress_series_options(field: str) -> dict[str, Any]:
    key = field.lower()
    if key == "elo":
        return {
            "band_max_fraction": 0.24,
            "band_scale": 0.35,
            "band_series": "ci",
            "scale": "symmetric",
            "smooth": True,
        }
    if key == "llr":
        return {"scale": "symmetric"}
    return {}

def progress_series_options(item: dict[str, Any], *, kind: str | None = None, field: str = "") -> dict[str, Any]:
    options: dict[str, Any] = {}
    if str(kind or "").lower() == "sprt":
        options.update(sprt_progress_series_options(field))
    for key in PROGRESS_GRAPH_SERIES_OPTION_KEYS:
        if key in item:
            options[key] = item[key]
    return options

def graph_series_with_options(name: str, points: list[dict[str, float]], options: dict[str, Any] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "points": points}
    for key, value in (options or {}).items():
        if value is not None:
            result[key] = value
    return result

def progress_series_specs(config: dict[str, Any], *, kind: str | None = None) -> list[dict[str, Any]]:
    raw = config.get("series")
    if not isinstance(raw, list):
        if str(kind or "").lower() == "sprt":
            return [
                {"name": "llr", "field": "llr", "x": "done", **sprt_progress_series_options("llr")},
                {"name": "elo", "field": "elo", "x": "done", **sprt_progress_series_options("elo")},
                {"name": "ci", "field": "ci", "x": "done"},
            ]
        if str(kind or "").lower() == "label":
            return [
                {"name": "read", "field": "done", "x": "done", "scale": "linear"},
                {"name": "selected", "field": "selected", "x": "done", "scale": "linear"},
                {"name": "written", "field": "output", "x": "done", "scale": "linear"},
            ]
        return []
    specs: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("field") or "").strip()
        field = str(item.get("field") or name).strip()
        if not name or not field:
            continue
        specs.append({
            "name": name,
            "field": field,
            "x": str(item.get("x") or "done").strip() or "done",
            **progress_series_options(item, kind=kind, field=field),
        })
    return specs

def progress_numeric_value(name: str, data: dict[str, Any], fields: dict[str, str]) -> float | None:
    if name in {"done", "total", "output", "output_rate", "rate", "global_done", "global_total"}:
        value = data.get(name)
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
        return None
    if name.lower() == "llr":
        value, _bound = progress_field_llr(fields)
        return value if math.isfinite(value) else None
    value = progress_field_float(fields, name)
    if value is not None:
        return value
    parsed = parse_float(fields.get(name))
    if parsed is None or not math.isfinite(parsed):
        return None
    return parsed

def task_log_progress_graph(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    config = task.get("progress")
    if not isinstance(config, dict):
        return None
    metadata = manifest_metadata(manifest)
    specs = progress_series_specs(config, kind=str(metadata.get("kind") or ""))
    if not specs:
        return None
    pattern = config.get("log_regex")
    if not pattern:
        return None
    try:
        progress_re = re.compile(str(pattern))
    except re.error:
        return None
    try:
        text = log_tail(task_log_path(manifest, task), max_bytes=65536)
    except OSError:
        return None

    points_by_name: dict[str, list[dict[str, float]]] = {spec["name"]: [] for spec in specs}
    for line in text.splitlines():
        match = progress_re.search(line)
        if not match:
            continue
        groups = match.groupdict()
        done = parse_int(groups.get("done"))
        if done is None:
            continue
        total = parse_int(groups.get("total"))
        output = parse_int(groups.get("output"))
        rate = parse_float(groups.get("rate"))
        data: dict[str, Any] = {"done": done}
        if total is not None:
            data["total"] = total
        if output is not None:
            data["output"] = output
        if rate is not None:
            data["rate"] = rate
        fields = progress_match_fields(match, config)
        for key, value in sprt_progress_line_fields(line).items():
            fields.setdefault(key, value)
        fields = normalize_progress_fields(fields)
        for spec in specs:
            x = progress_numeric_value(spec["x"], data, fields)
            y = progress_numeric_value(spec["field"], data, fields)
            if x is None or y is None:
                continue
            points_by_name[spec["name"]].append({"x": x, "y": y})

    options_by_name = {spec["name"]: progress_series_options(spec, kind=str(metadata.get("kind") or ""), field=spec["field"]) for spec in specs}
    series = [
        graph_series_with_options(name, points, options_by_name.get(name))
        for name, points in points_by_name.items()
        if len(points) >= 2
    ]
    if not series:
        return None
    result = {
        "version": PROGRESS_GRAPH_VERSION,
        "x_unit": str(config.get("unit") or ""),
        "series": series,
    }
    if total := parse_int(config.get("total")):
        if total > 0:
            result["x_total"] = total
    return result

def task_progress_graph_point_specs(manifest: dict[str, Any], task: dict[str, Any]) -> list[dict[str, Any]]:
    config = task.get("progress")
    if not isinstance(config, dict):
        return []
    metadata = manifest_metadata(manifest)
    return progress_series_specs(config, kind=str(metadata.get("kind") or ""))

def run_progress_graph_from_rows(
    manifest: dict[str, Any],
    rows: list[dict[str, Any]],
    progress_task_offsets: dict[str, tuple[str, int]],
    progress_unit_totals: dict[str, int],
) -> dict[str, Any] | None:
    tasks_by_id = {str(task["id"]): task for task in manifest["tasks"]}
    points_by_name: dict[str, list[dict[str, float]]] = {}
    options_by_name: dict[str, dict[str, Any]] = {}
    sprt_rows: list[dict[str, Any]] = []
    sprt_series_names: dict[str, str] = {}
    sprt_llr_bound: float | None = None
    kind = str(manifest_metadata(manifest).get("kind") or "").lower()
    is_sprt = kind == "sprt"
    is_labeling = kind == "label"
    is_selfplay = kind == "selfplay"
    is_conversion = kind in {"selfplay-to-bullet", "selfplay-to-sf-oracle"}
    label_x = 0.0
    label_worker_rates: dict[str, float] = {}
    conversion_rows: list[dict[str, float | str]] = []
    conversion_series_names: set[str] = set()
    selfplay_rows: list[dict[str, Any]] = []
    x_unit = ""
    for row in rows:
        task = tasks_by_id.get(str(row.get("id") or ""))
        if task is None:
            continue
        specs = task_progress_graph_point_specs(manifest, task)
        if not specs:
            continue
        for spec in specs:
            options_by_name.setdefault(
                spec["name"],
                progress_series_options(spec, kind="sprt" if is_sprt else "", field=spec["field"]),
            )
        data = row.get("progress_data")
        if not isinstance(data, dict) or data.get("kind") != "metadata":
            continue
        fields = data.get("fields")
        fields = normalize_progress_fields(fields if isinstance(fields, dict) else {})
        graph_data = dict(data)
        if is_conversion:
            done = progress_numeric_value("done", graph_data, fields)
            total = progress_numeric_value("total", graph_data, fields)
            output = progress_numeric_value("output", graph_data, fields)
            rate = progress_numeric_value("rate", graph_data, fields)
            if done is not None and done > 0:
                conversion_rows.append({
                    "state": str(row.get("state") or ""),
                    "done": done,
                    "total": total or 0.0,
                    "output": output or 0.0,
                    "rate": rate or 0.0,
                })
        config_progress = progress_config_unit_total(task)
        if config_progress is not None and isinstance(data.get("done"), int):
            unit, task_total = config_progress
            x_unit = x_unit or unit
            offset_unit, offset = progress_task_offsets.get(str(task["id"]), (unit, 0))
            if offset_unit == unit:
                graph_data.setdefault("global_done", offset + int(data["done"]))
                graph_data.setdefault("global_total", progress_unit_totals.get(unit, task_total))
        if is_labeling:
            done = progress_numeric_value("done", graph_data, fields)
            if done is not None:
                label_x += done
            rate = progress_numeric_value("rate", graph_data, fields)
            if row.get("state") == "running" and rate is not None and rate > 0:
                worker = worker_group_name(str(row.get("worker") or "unknown"))
                label_worker_rates[worker] = label_worker_rates.get(worker, 0.0) + rate
            continue
        sprt_x: float | None = None
        selfplay_done: float | None = None
        selfplay_values: dict[str, float] = {}
        selfplay_rate: float | None = (
            progress_numeric_value("rate", graph_data, fields) if is_selfplay else None
        )
        if is_selfplay:
            selfplay_done = progress_numeric_value("done", graph_data, fields)
        for spec in specs:
            x_name = "global_done" if spec["x"] == "done" and "global_done" in graph_data else spec["x"]
            x = progress_numeric_value(x_name, graph_data, fields)
            y = progress_numeric_value(spec["field"], graph_data, fields)
            if x is None or y is None:
                continue
            field_name = str(spec["field"]).lower()
            if is_conversion and field_name in {"output_rate", "rate"}:
                conversion_series_names.add(spec["name"])
                continue
            if is_selfplay and field_name in {"wins", "losses", "draws"}:
                selfplay_values[spec["name"]] = y
                if selfplay_done is None:
                    selfplay_done = progress_numeric_value("done", graph_data, fields)
                continue
            if is_selfplay and field_name == "rate":
                selfplay_rate = y
                if selfplay_done is None:
                    selfplay_done = progress_numeric_value("done", graph_data, fields)
                continue
            if is_sprt and field_name in {"elo", "llr", "ci"}:
                sprt_series_names.setdefault(spec["name"], field_name)
                if field_name == "llr":
                    _llr, bound = progress_field_llr(fields)
                    if bound is not None and math.isfinite(bound) and bound > 0:
                        sprt_llr_bound = max(sprt_llr_bound or 0.0, abs(bound))
                sprt_x = x if sprt_x is None else sprt_x
                continue
            points_by_name.setdefault(spec["name"], []).append({"x": x, "y": y})
        done = graph_data.get("done")
        if sprt_x is not None and isinstance(done, int) and done > 0:
            sprt_rows.append({"x": sprt_x, "done": done, "fields": fields})
        if selfplay_done is not None and (selfplay_values or selfplay_rate is not None):
            selfplay_rows.append({
                "done": selfplay_done,
                "series": selfplay_values,
                "rate": selfplay_rate,
            })

    if is_labeling and label_x > 0 and label_worker_rates:
        points_by_name["total"] = [{"x": label_x, "y": sum(label_worker_rates.values())}]
        for worker, rate in sorted(label_worker_rates.items()):
            points_by_name[worker] = [{"x": label_x, "y": rate}]

    conversion_total_estimate: int | None = None
    if is_conversion and conversion_rows:
        completed = [item for item in conversion_rows if item["state"] == "done" and item["output"] > 0]
        observed = completed or [item for item in conversion_rows if item["output"] > 0]
        observed_done = sum(float(item["done"]) for item in observed)
        observed_output = sum(float(item["output"]) for item in observed)
        positions_per_pgn = observed_output / observed_done if observed_done > 0 else 0.0
        completed_output = sum(float(item["output"]) for item in completed)
        running_done = sum(
            float(item["done"]) for item in conversion_rows if item["state"] == "running"
        )
        conversion_x = completed_output + positions_per_pgn * running_done
        aggregate_pgn_rate = sum(
            float(item["rate"]) for item in conversion_rows if item["state"] == "running"
        )
        known_totals = [float(item["total"]) for item in conversion_rows if item["total"] > 0]
        average_total = sum(known_totals) / len(known_totals) if known_totals else 0.0
        total_pgns = average_total * len(manifest["tasks"])
        if positions_per_pgn > 0 and total_pgns > 0:
            conversion_total_estimate = max(
                int(conversion_x),
                int(round(positions_per_pgn * total_pgns)),
            )
        x_unit = "positions"
        for name in conversion_series_names:
            points_by_name[name] = [{
                "x": conversion_x,
                "y": aggregate_pgn_rate * positions_per_pgn,
            }]

    if is_selfplay and selfplay_rows:
        totals: dict[str, float] = {}
        games_done = 0.0
        aggregate_rate = 0.0
        for item in selfplay_rows:
            row_done = parse_float(item.get("done"))
            raw_series = item.get("series")
            if row_done is None or row_done <= 0 or not isinstance(raw_series, dict):
                continue
            games_done += row_done
            for name, value in raw_series.items():
                totals[name] = totals.get(name, 0.0) + float(value)
            rate = parse_float(item.get("rate"))
            if rate is not None and rate > 0:
                aggregate_rate += rate
        if aggregate_rate > 0:
            points_by_name["games/s"] = [{
                "x": games_done,
                "y": aggregate_rate,
            }]
        if "games/s" in points_by_name:
            options_by_name["games/s"] = {"scale": "linear", "smooth": True}

    x_total = progress_unit_totals.get(x_unit) if x_unit else None
    for name, points in sprt_aggregate_graph_points(
        sprt_rows,
        sprt_series_names,
        x_total=x_total if isinstance(x_total, int) else None,
    ).items():
        points_by_name[name] = points
        if sprt_series_names.get(name) == "llr" and sprt_llr_bound is not None:
            options_by_name.setdefault(name, {}).update({
                "max": sprt_llr_bound,
                "min": -sprt_llr_bound,
            })

    series = []
    for name, points in points_by_name.items():
        points.sort(key=lambda point: point["x"])
        if len(points) >= 2 or (is_labeling and points) or (is_conversion and points) or (is_selfplay and name == "games/s" and points) or (name.lower() == "ci" and points):
            series.append(graph_series_with_options(name, points, options_by_name.get(name)))
    if not series:
        return None
    result = {
        "version": PROGRESS_GRAPH_VERSION,
        "x_unit": x_unit,
        "series": series,
    }
    if is_conversion:
        if conversion_total_estimate is not None:
            result["x_total"] = conversion_total_estimate
    if is_selfplay and selfplay_rows:
        wins = int(totals.get("wins", 0.0))
        draws = int(totals.get("draws", 0.0))
        losses = int(totals.get("losses", 0.0))
        games = wins + draws + losses
        result["selfplay"] = {
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "draw_percent": 100.0 * draws / games if games else 0.0,
        }
    if isinstance(x_total, int) and x_total > 0:
        result["x_total"] = x_total
    return result

def elo_score_rate(elo: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -elo / 400.0))

def score_rate_elo(score_rate: float) -> float:
    bounded = min(1.0 - 1e-12, max(1e-12, score_rate))
    return 400.0 * math.log10(bounded / (1.0 - bounded))

def sprt_aggregate_graph_points(
    rows: list[dict[str, Any]],
    series_names: dict[str, str],
    *,
    x_total: int | None = None,
) -> dict[str, list[dict[str, float]]]:
    if not rows or not series_names:
        return {}
    points: dict[str, list[dict[str, float]]] = {
        name: ([{"x": 0.0, "y": 0.0}] if field_name in {"elo", "llr"} else [])
        for name, field_name in series_names.items()
    }
    weighted_score = 0.0
    weight_total = 0
    weighted_ci_squares = 0.0
    ci_weight_total = 0
    cumulative_done = 0
    llr_total = 0.0
    llr_seen = False
    def row_x(row: dict[str, Any]) -> float:
        try:
            return float(row["x"])
        except (KeyError, TypeError, ValueError):
            return math.inf

    for row in sorted(rows, key=row_x):
        x = row_x(row)
        if not math.isfinite(x):
            continue
        done = row.get("done")
        fields = row.get("fields")
        if not isinstance(done, int) or done <= 0 or not isinstance(fields, dict):
            continue
        cumulative_done += done
        x = float(cumulative_done)
        elo = progress_field_float(fields, "elo", "elo~", "Elo")
        if elo is not None:
            weighted_score += elo_score_rate(elo) * done
            weight_total += done
        ci = progress_field_float(fields, "ci", "err", "error", "+/-", "elo_ci")
        if ci is not None and ci >= 0:
            weighted_ci_squares += (ci * done) ** 2
            ci_weight_total += done
        llr, _bound = progress_field_llr(fields)
        if math.isfinite(llr):
            llr_total += llr
            llr_seen = True
        for name, field_name in series_names.items():
            if field_name == "elo" and weight_total > 0:
                points[name].append({"x": x, "y": score_rate_elo(weighted_score / weight_total)})
            elif field_name == "llr" and llr_seen:
                points[name].append({"x": x, "y": llr_total})
            elif field_name == "ci" and ci_weight_total > 0:
                points[name].append({"x": x, "y": math.sqrt(weighted_ci_squares) / ci_weight_total})
    return {name: values for name, values in points.items() if values}

def progress_history_path(manifest: dict[str, Any]) -> Path:
    return state_dir(manifest) / PROGRESS_HISTORY_NAME

def progress_history_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    x = parse_float(raw.get("x"))
    if x is None or x < 0:
        return None
    raw_series = raw.get("series")
    if not isinstance(raw_series, dict):
        return None
    series: dict[str, float] = {}
    for key, value in raw_series.items():
        name = str(key).strip()
        y = parse_float(value)
        if name and y is not None:
            series[name] = y
    if not series:
        return None
    entry: dict[str, Any] = {
        "epoch": parse_float(raw.get("epoch")) or 0.0,
        "x": float(x),
        "series": series,
    }
    x_unit = str(raw.get("x_unit") or "").strip()
    if x_unit:
        entry["x_unit"] = x_unit
    x_total = parse_int(raw.get("x_total"))
    if x_total is not None and x_total > 0:
        entry["x_total"] = x_total
    return entry

def load_progress_history(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    path = progress_history_path(manifest)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    entries: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        entry = progress_history_entry(raw)
        if entry is not None:
            entries.append(entry)
    entries.sort(key=lambda entry: (float(entry["x"]), float(entry.get("epoch") or 0.0)))
    return entries

def progress_graph_snapshot(graph: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(graph, dict):
        return None
    raw_series = graph.get("series")
    if not isinstance(raw_series, list):
        return None
    values: dict[str, float] = {}
    x_values: list[float] = []
    for item in raw_series:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        points = item.get("points")
        if not name or not isinstance(points, list):
            continue
        best: dict[str, float] | None = None
        for point in points:
            if not isinstance(point, dict):
                continue
            x = parse_float(point.get("x"))
            y = parse_float(point.get("y"))
            if x is None or y is None:
                continue
            if best is None or x > best["x"]:
                best = {"x": float(x), "y": float(y)}
        if best is None or best["x"] <= 0:
            continue
        values[name] = best["y"]
        x_values.append(best["x"])
    if not values or not x_values:
        return None
    snapshot: dict[str, Any] = {
        "epoch": time.time(),
        "x": max(x_values),
        "series": values,
    }
    x_unit = str(graph.get("x_unit") or "").strip()
    if x_unit:
        snapshot["x_unit"] = x_unit
    x_total = parse_int(graph.get("x_total"))
    if x_total is not None and x_total > 0:
        snapshot["x_total"] = x_total
    return snapshot

def append_progress_history(
    manifest: dict[str, Any],
    snapshot: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    history = load_progress_history(manifest)
    if snapshot is None:
        return history
    entry = progress_history_entry(snapshot)
    if entry is None:
        return history
    last_x = max((float(item["x"]) for item in history), default=-1.0)
    if float(entry["x"]) <= last_x:
        return history
    path = progress_history_path(manifest)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
    except OSError:
        return history
    history.append(entry)
    return history

def progress_graph_from_history(
    history: list[dict[str, Any]],
    fallback_graph: dict[str, Any] | None,
    *,
    kind: str = "",
) -> dict[str, Any] | None:
    points_by_name: dict[str, list[dict[str, float]]] = {}
    options_by_name: dict[str, dict[str, Any]] = {}
    x_unit = ""
    x_total: int | None = None
    selfplay_rate_graph = (
        isinstance(fallback_graph, dict)
        and any(
            str(item.get("name") or "").lower() == "games/s"
            for item in fallback_graph.get("series", [])
            if isinstance(item, dict)
        )
    ) or any(
        isinstance(entry.get("series"), dict) and "games/s" in entry["series"]
        for entry in history
    )
    is_selfplay = kind.lower() == "selfplay" or selfplay_rate_graph or (
        isinstance(fallback_graph, dict) and isinstance(fallback_graph.get("selfplay"), dict)
    )
    current_max_x: float | None = None
    previous_selfplay_sample: tuple[float, float] | None = None
    if isinstance(fallback_graph, dict):
        raw_fallback_series = fallback_graph.get("series")
        if isinstance(raw_fallback_series, list):
            if is_selfplay:
                max_points: list[float] = []
                for item in raw_fallback_series:
                    if not isinstance(item, dict):
                        continue
                    points = item.get("points")
                    if not isinstance(points, list):
                        continue
                    for point in points:
                        if not isinstance(point, dict):
                            continue
                        x = parse_float(point.get("x"))
                        if x is not None:
                            max_points.append(x)
                if max_points:
                    current_max_x = max(max_points)
            for item in raw_fallback_series:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if name:
                    options_by_name[name] = progress_series_options(item)
    for entry in history:
        x = parse_float(entry.get("x"))
        if x is None:
            continue
        if not x_unit:
            x_unit = str(entry.get("x_unit") or "").strip()
        entry_x_total = parse_int(entry.get("x_total"))
        if entry_x_total is not None and entry_x_total > 0:
            x_total = entry_x_total
        series = entry.get("series")
        if not isinstance(series, dict):
            continue
        if is_selfplay and current_max_x is not None and x > current_max_x:
            continue
        if is_selfplay and selfplay_rate_graph:
            epoch = parse_float(entry.get("epoch"))
            measured_rate: float | None = None
            if epoch is not None and previous_selfplay_sample is not None:
                previous_x, previous_epoch = previous_selfplay_sample
                elapsed = epoch - previous_epoch
                if elapsed > 0 and x > previous_x:
                    measured_rate = (x - previous_x) / elapsed
            if epoch is not None:
                previous_selfplay_sample = (x, epoch)
            series = {"games/s": measured_rate} if measured_rate is not None else {}
        elif is_selfplay and "games/s" in series:
            series = {"games/s": series["games/s"]}
        elif is_selfplay and all(name in series for name in ("wins", "draws", "losses")):
            series = {}
        elif is_selfplay:
            series = {name: value for name, value in series.items() if str(name).lower() == "balance"}
        for name, value in series.items():
            y = parse_float(value)
            if y is None:
                continue
            points_by_name.setdefault(str(name), []).append({"x": float(x), "y": y})
    if selfplay_rate_graph and not points_by_name:
        for item in fallback_graph.get("series", []):
            if not isinstance(item, dict) or str(item.get("name") or "").lower() != "games/s":
                continue
            points = item.get("points")
            if isinstance(points, list):
                points_by_name["games/s"] = [
                    {"x": float(point["x"]), "y": float(point["y"])}
                    for point in points
                    if isinstance(point, dict)
                    and parse_float(point.get("x")) is not None
                    and parse_float(point.get("y")) is not None
                ]
            break
    if not x_unit and isinstance(fallback_graph, dict):
        x_unit = str(fallback_graph.get("x_unit") or "").strip()
    if x_total is None and isinstance(fallback_graph, dict):
        parsed_total = parse_int(fallback_graph.get("x_total"))
        if parsed_total is not None and parsed_total > 0:
            x_total = parsed_total
    series: list[dict[str, Any]] = []
    for name, raw_points in points_by_name.items():
        by_x: dict[float, dict[str, float]] = {}
        for point in raw_points:
            by_x[float(point["x"])] = point
        points = [by_x[x] for x in sorted(by_x)]
        if name.lower() in SIGNED_PROGRESS_SERIES and (not points or points[0]["x"] > 0.0):
            points = [{"x": 0.0, "y": 0.0}, *points]
        if len(points) >= 2:
            series.append(graph_series_with_options(name, points, options_by_name.get(name)))
    if not series:
        return None
    result: dict[str, Any] = {
        "version": PROGRESS_GRAPH_VERSION,
        "x_unit": x_unit,
        "series": series,
    }
    if x_total is not None and x_total > 0:
        result["x_total"] = x_total
    if is_selfplay and isinstance(fallback_graph, dict) and isinstance(fallback_graph.get("selfplay"), dict):
        result["selfplay"] = fallback_graph["selfplay"]
    return result

def run_progress_history_graph(
    manifest: dict[str, Any],
    current_graph: dict[str, Any] | None,
) -> dict[str, Any] | None:
    history = append_progress_history(manifest, progress_graph_snapshot(current_graph))
    kind = str(manifest_metadata(manifest).get("kind") or manifest.get("kind") or "")
    if (
        current_graph is not None
        and kind.lower() in {"selfplay-to-bullet", "selfplay-to-sf-oracle"}
        and len(history) < 2
    ):
        return current_graph
    if all(task_complete(manifest, task) for task in manifest.get("tasks", [])):
        if current_graph is not None and (
            not isinstance(current_graph.get("selfplay"), dict)
            or any(
                str(item.get("name") or "").lower() == "games/s"
                for item in current_graph.get("series", [])
                if isinstance(item, dict)
            )
        ):
            return current_graph
        return progress_graph_from_history(
            history,
            current_graph,
            kind=kind,
        )
    return progress_graph_from_history(
        history,
        current_graph,
        kind=kind,
    )

def sprt_summary_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows = summary.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    active = summary.get("active")
    if isinstance(active, list):
        return [row for row in active if isinstance(row, dict)]
    return []

def summary_is_sprt(summary: dict[str, Any]) -> bool:
    metadata = summary.get("metadata")
    if isinstance(metadata, dict) and str(metadata.get("kind") or "").lower() == "sprt":
        return True
    for row in sprt_summary_rows(summary):
        label = str(row.get("label") or "").lower()
        if label == "sprt" or label.startswith("sprt:") or label.startswith("sprt-"):
            return True
    return False

def sprt_aggregate_stats(summary: dict[str, Any]) -> dict[str, Any]:
    if not summary_is_sprt(summary):
        return {}

    weighted_score = 0.0
    weighted_los = 0.0
    weighted_draw = 0.0
    weighted_ci_squares = 0.0
    weight_total = 0
    los_weight_total = 0
    draw_weight_total = 0
    ci_weight_total = 0
    llr_total = 0.0
    llr_count = 0
    llr_bound: float | None = None
    llr_mixed_bound = False
    wins = draws = losses = 0
    pentanomial = [0, 0, 0, 0, 0]
    wdl_available = False
    pentanomial_available = False

    for row in sprt_summary_rows(summary):
        data = row.get("progress_data")
        if not isinstance(data, dict):
            continue
        done = data.get("done")
        if not isinstance(done, int) or done <= 0:
            continue
        fields = data.get("fields")
        if not isinstance(fields, dict):
            continue
        normalized_fields = normalize_progress_fields(fields)
        wdl_text = normalized_fields.get("wdl")
        if isinstance(wdl_text, str):
            parts = wdl_text.split("/")
            if len(parts) == 3 and all(part.isdigit() for part in parts):
                row_wins, row_draws, row_losses = (int(part) for part in parts)
                wins += row_wins
                draws += row_draws
                losses += row_losses
                wdl_available = True
        pentanomial_text = normalized_fields.get("pentanomial")
        if isinstance(pentanomial_text, str):
            parts = pentanomial_text.split("-")
            if len(parts) == 5 and all(part.isdigit() for part in parts):
                for index, part in enumerate(parts):
                    pentanomial[index] += int(part)
                pentanomial_available = True
        elo = progress_field_float(normalized_fields, "elo", "elo~", "Elo")
        if elo is None:
            continue
        weighted_score += elo_score_rate(elo) * done
        weight_total += done
        ci = progress_field_float(normalized_fields, "ci", "err", "error", "+/-", "elo_ci")
        if ci is not None and ci >= 0:
            weighted_ci_squares += (ci * done) ** 2
            ci_weight_total += done
        llr, bound = progress_field_llr(normalized_fields)
        if math.isfinite(llr):
            llr_total += llr
            llr_count += 1
            if bound is not None:
                if llr_bound is None:
                    llr_bound = bound
                elif not math.isclose(llr_bound, bound, rel_tol=1e-9, abs_tol=1e-9):
                    llr_mixed_bound = True
        los = progress_field_float(normalized_fields, "los", "LOS")
        if los is not None:
            weighted_los += los * done
            los_weight_total += done
        draw = progress_field_float(normalized_fields, "draw", "DrawRatio", "draw_ratio")
        if draw is not None:
            weighted_draw += draw * done
            draw_weight_total += done

    if weight_total <= 0:
        return {}
    return {
        "weighted_score": weighted_score,
        "weighted_los": weighted_los,
        "weighted_draw": weighted_draw,
        "weighted_ci_squares": weighted_ci_squares,
        "weight_total": weight_total,
        "los_weight_total": los_weight_total,
        "draw_weight_total": draw_weight_total,
        "ci_weight_total": ci_weight_total,
        "llr_total": llr_total,
        "llr_count": llr_count,
        "llr_bound": llr_bound,
        "llr_mixed_bound": llr_mixed_bound,
        "wdl": [wins, draws, losses] if wdl_available else None,
        "pentanomial": pentanomial if pentanomial_available else None,
    }

def pooled_los_percent(elo: float, ci: float) -> float:
    """LOS from a pooled elo estimate and its 95% half-width.

    LOS = Phi(elo / sigma) with sigma = ci / 1.96. Per-shard LOS values
    cannot be averaged: with ~26-game shards each one sits near 50% no
    matter how strong the pooled evidence is, so the mean regresses to
    50% (observed: elo=+7.4 ci=14.8 displayed los=54.6%, true ~84%).
    """
    z = elo / (ci / 1.96)
    return 50.0 * (1.0 + math.erf(z / math.sqrt(2.0)))

def sprt_aggregate_fields(summary: dict[str, Any]) -> list[str]:
    stats = sprt_aggregate_stats(summary)
    if not stats:
        return []
    weight_total = int(stats["weight_total"])
    pooled_elo = score_rate_elo(float(stats["weighted_score"]) / weight_total)
    fields = [f"elo={pooled_elo:+.1f}"]
    ci_weight_total = int(stats["ci_weight_total"])
    pooled_ci = 0.0
    if ci_weight_total > 0:
        pooled_ci = math.sqrt(float(stats["weighted_ci_squares"])) / ci_weight_total
        fields.append(f"ci={pooled_ci:.1f}")
    if int(stats["llr_count"]) > 0:
        fields.append(format_llr_field(
            float(stats["llr_total"]),
            stats.get("llr_bound") if isinstance(stats.get("llr_bound"), (int, float)) else None,
            bool(stats.get("llr_mixed_bound")),
        ))
    # LOS is only emitted when it can be pooled from elo and ci; the
    # per-shard average kept in weighted_los is misleading (see
    # pooled_los_percent) so a run without ci gets no los field at all.
    if math.isfinite(pooled_elo) and pooled_ci > 0:
        fields.append(f"los={pooled_los_percent(pooled_elo, pooled_ci):.1f}%")
    draw_weight_total = int(stats["draw_weight_total"])
    if draw_weight_total > 0:
        fields.append(f"draw={float(stats['weighted_draw']) / draw_weight_total:.1f}%")
    return fields

def sprt_aggregate_decision(summary: dict[str, Any]) -> dict[str, Any] | None:
    stats = sprt_aggregate_stats(summary)
    if not stats or int(stats.get("llr_count") or 0) <= 0:
        return None
    if bool(stats.get("llr_mixed_bound")):
        return None
    bound = stats.get("llr_bound")
    if not isinstance(bound, (int, float)) or not math.isfinite(float(bound)) or float(bound) <= 0.0:
        return None
    llr = float(stats["llr_total"])
    bound = abs(float(bound))
    if llr >= bound:
        side = "H1"
        result = "accepted"
    elif llr <= -bound:
        side = "H0"
        result = "rejected"
    else:
        return None
    return {
        "reason": "aggregate_sprt_bound",
        "side": side,
        "result": result,
        "llr": llr,
        "bound": bound,
        "percent": llr / bound * 100.0,
        "games": int(stats["weight_total"]),
    }

PROGRESS_RESERVED_FIELDS = {"done", "total", "output", "output_rate", "rate"}

def normalize_progress_fields(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    fields: dict[str, str] = {}
    for key, value in raw.items():
        name = str(key).strip()
        text = str(value).strip() if value is not None else ""
        if text.lower() in {"nan", "+nan", "-nan", "inf", "+inf", "-inf"}:
            continue
        if name and text:
            fields[name] = text
    return fields

def progress_display_fields(config: dict[str, Any]) -> dict[str, str]:
    for key in ("display_fields", "fields"):
        fields = config.get(key)
        if isinstance(fields, dict):
            return {str(k): str(v) for k, v in fields.items()}
    return {}

def progress_match_fields(match: re.Match[str], config: dict[str, Any]) -> dict[str, str]:
    display_fields = progress_display_fields(config)
    fields: dict[str, str] = {}
    for key, value in match.groupdict().items():
        if key in PROGRESS_RESERVED_FIELDS or value is None:
            continue
        name = display_fields.get(key, key)
        text = str(value).strip()
        if name and text:
            fields[str(name)] = text
    return fields

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
    fields = normalize_progress_fields(data.get("fields"))
    if fields:
        data["fields"] = fields
    else:
        data.pop("fields", None)

    kind = str(data.get("kind") or "").strip()
    if kind not in {"metadata", "subtask"}:
        kind = "metadata" if data.get("unit") else "subtask"
    data["kind"] = kind
    if data.get("eta_overdue"):
        data["eta_overdue"] = True
    else:
        data.pop("eta_overdue", None)
    if data.get("eta_estimate"):
        data["eta_estimate"] = True
    else:
        data.pop("eta_estimate", None)
    eta_s = data.get("eta_s")
    if isinstance(eta_s, (int, float)):
        data["eta_s"] = max(0.0, float(eta_s))
    else:
        eta_s = progress_eta_seconds(data.get("done"), data.get("total"), data.get("rate"))
        if eta_s is not None:
            data["eta_s"] = eta_s
        else:
            data.pop("eta_s", None)
    eta_epoch = data.get("eta_epoch")
    if isinstance(eta_epoch, (int, float)):
        data["eta_epoch"] = float(eta_epoch)
    else:
        data.pop("eta_epoch", None)
    data["text"] = progress_text(data)
    return data

def task_progress_candidate_paths(manifest: dict[str, Any], task: dict[str, Any]) -> list[Path]:
    paths = [task_log_path(manifest, task)]
    for output in task_outputs(task, manifest):
        if output not in paths:
            paths.append(output)
    return paths


def selfplay_pgn_progress_data(
    path: Path,
    config: Any,
    *,
    eta_epoch: float,
) -> dict[str, Any] | None:
    if path.suffix.lower() != ".pgn" or not isinstance(config, dict):
        return None
    try:
        text = log_tail(path)
    except OSError:
        return None
    done = len(re.findall(r"(?m)^\[Event(?:\s|$)", text))
    if done <= 0:
        return None
    total_value = config.get("total")
    total = total_value if isinstance(total_value, int) else parse_int(str(total_value)) if total_value is not None else None
    data: dict[str, Any] = {
        "kind": "metadata",
        "done": min(done, total) if isinstance(total, int) and total > 0 else done,
        "unit": str(config.get("unit") or "games"),
        "eta_epoch": eta_epoch,
    }
    if isinstance(total, int) and total > 0:
        data["total"] = total
    return normalize_progress_data(data)


def parse_task_progress_text(text: str, config: Any, *, eta_epoch: float) -> dict[str, Any] | None:
    if isinstance(config, dict) and config.get("log_regex"):
        try:
            progress_re = re.compile(str(config["log_regex"]))
        except re.error:
            return None
        matched_lines: list[tuple[str, re.Match[str]]] = []
        for line in text.splitlines():
            match = progress_re.search(line)
            if match:
                matched_lines.append((line, match))
        cumulative_output: int | None = None
        if config.get("output_mode") == "sum":
            cumulative_output = 0
            previous_done: int | None = None
            for _line, match in matched_lines:
                matched_done = parse_int(match.groupdict().get("done"))
                matched_output = parse_int(match.groupdict().get("output"))
                if matched_done is not None and previous_done is not None and matched_done <= previous_done:
                    cumulative_output = 0
                if matched_output is not None:
                    cumulative_output += matched_output
                if matched_done is not None:
                    previous_done = matched_done
        for line, match in reversed(matched_lines):
            done = parse_int(match.groupdict().get("done"))
            if done is None:
                continue
            total = parse_int(match.groupdict().get("total"))
            if total is None:
                total_value = config.get("total")
                total = total_value if isinstance(total_value, int) else parse_int(str(total_value)) if total_value is not None else None
            output = parse_int(match.groupdict().get("output"))
            if cumulative_output is not None:
                output = cumulative_output
            rate = parse_float(match.groupdict().get("rate"))
            data: dict[str, Any] = {
                "done": done,
                "unit": str(config.get("unit") or ""),
                "output_unit": str(config.get("output_unit") or "output"),
                "kind": "metadata",
                "eta_epoch": eta_epoch,
            }
            if total is not None:
                data["total"] = total
            if output is not None:
                data["output"] = output
            if rate is not None:
                data["rate"] = rate
            fields = progress_match_fields(match, config)
            for key, value in sprt_progress_line_fields(line).items():
                fields.setdefault(key, value)
            for key, value in fastchess_progress_fields(text).items():
                fields.setdefault(key, value)
            if fields:
                data["fields"] = fields
            return normalize_progress_data(data)

    if isinstance(config, dict):
        for line in reversed(text.splitlines()):
            data = sprt_progress_line_data(line, config, eta_epoch=eta_epoch)
            if data is not None:
                return data
            data = fastchess_games_line_data(line, config, eta_epoch=eta_epoch)
            if data is not None:
                return data
        fc_fields = fastchess_progress_fields(text)
        if fc_fields:
            score_match = re.search(r"Score\s+of\s+\w+\s+vs\s+\w+:.*?\[\d+\.\d+\]\s+(?P<done>\d+)", text)
            if score_match:
                done = parse_int(score_match.group("done"))
                if done is not None:
                    data = {
                        "done": done,
                        "unit": str(config.get("unit") or "games"),
                        "kind": "metadata",
                        "eta_epoch": eta_epoch,
                        "fields": fc_fields,
                    }
                    total_value = config.get("total")
                    if total_value is not None:
                        data["total"] = total_value if isinstance(total_value, int) else parse_int(str(total_value))
                    return normalize_progress_data(data)

    progress_re = re.compile(r"\[\s*\d+/\d+\s+(\d+)\s*/\s*(\d+)\]")
    for line in reversed(text.splitlines()):
        match = progress_re.search(line)
        if match:
            done = int(match.group(1))
            total = int(match.group(2))
            return normalize_progress_data({"kind": "subtask", "done": done, "total": total, "eta_epoch": eta_epoch})
    return None

def task_log_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    config = task.get("progress")
    best: dict[str, Any] | None = None
    for path in task_progress_candidate_paths(manifest, task):
        try:
            text = log_tail(path)
            eta_epoch = path.stat().st_mtime
        except OSError:
            continue
        data = parse_task_progress_text(text, config, eta_epoch=eta_epoch)
        pgn_data = (
            selfplay_pgn_progress_data(path, config, eta_epoch=eta_epoch)
            if str(manifest.get("kind") or "").lower() == "selfplay"
            else None
        )
        for candidate in (data, pgn_data):
            if candidate is None:
                continue
            candidate_done = candidate.get("done")
            best_done = best.get("done") if isinstance(best, dict) else None
            if best is None or (
                isinstance(candidate_done, int)
                and (not isinstance(best_done, int) or candidate_done > best_done)
            ):
                best = candidate
    return best

def task_heartbeat_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    path = task_heartbeat_path(manifest, task)
    payload = read_state_json(path)
    if payload is None:
        return None
    progress_data = payload.get("progress_data")
    if isinstance(progress_data, dict) and "eta_epoch" not in progress_data:
        try:
            progress_data = {**progress_data, "eta_epoch": path.stat().st_mtime}
        except OSError:
            pass
    return normalize_progress_data(progress_data)

def status_summary(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, Any]:
    clear_state_diagnostics()
    manifest_kind = str(manifest_metadata(manifest).get("kind") or "").lower()
    queue_payload = read_state_json(queue_request_path(manifest))
    launch_payload = read_state_json(launch_request_path(manifest))
    stop_payload = read_state_json(stop_request_path(manifest))
    launch_state = "stale" if launch_request_stale(launch_payload) else "launching" if isinstance(launch_payload, dict) else None
    if launch_state == "stale":
        record_state_diagnostic(launch_request_path(manifest), "launch request is stale; no launcher is alive")
    counts = {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}
    rows: list[dict[str, Any]] = []
    groups: dict[str, dict[str, list[int | str]]] = {
        "done": {},
        "fail": {},
        "stale": {},
    }
    active: list[dict[str, Any]] = []
    pending: list[int | str] = []
    pending_groups: dict[str, list[int | str]] = {}
    progress_totals: dict[str, int] = {}
    progress_done: dict[str, int] = {}
    progress_outputs: dict[str, int] = {}
    worker_outputs: dict[str, dict[str, Any]] = {}
    worker_rates: dict[str, float] = {}
    completed_worker_task_rates: dict[str, list[float]] = {}
    completed_units: dict[str, int] = {}
    completed_elapsed: dict[str, float] = {}
    progress_task_offsets: dict[str, tuple[str, int]] = {}
    progress_unit_totals: dict[str, int] = {}
    failure_reasons: dict[str, dict[str, Any]] = {}

    for task in manifest["tasks"]:
        config_progress = progress_config_unit_total(task)
        if config_progress is None:
            continue
        unit, total = config_progress
        offset = progress_unit_totals.get(unit, 0)
        progress_task_offsets[str(task["id"])] = (unit, offset)
        progress_unit_totals[unit] = offset + total

    for task in manifest["tasks"]:
        config_progress = progress_config_unit_total(task)
        if config_progress is None or not task_complete(manifest, task):
            continue
        unit, total = config_progress
        payload = task_done_payload(manifest, task)
        elapsed = payload.get("elapsed_s") if isinstance(payload, dict) else None
        if isinstance(elapsed, (int, float)) and elapsed > 0:
            completed_units[unit] = completed_units.get(unit, 0) + total
            completed_elapsed[unit] = completed_elapsed.get(unit, 0.0) + float(elapsed)
            worker = status_worker(manifest, task, "done") or "unknown"
            completed_worker_task_rates.setdefault(worker, []).append(total / float(elapsed))
    fallback_rates = {
        unit: completed_units[unit] / completed_elapsed[unit]
        for unit in completed_units
        if completed_elapsed.get(unit, 0.0) > 0
    }
    fallback_worker_rates = {
        worker: sum(rates) / len(rates)
        for worker, rates in completed_worker_task_rates.items()
        if rates
    }
    calibrated_process_rates: dict[str, float | None] = {}

    def process_calibrated_rate(worker: str | None, worker_id: str | None, unit: str) -> float | None:
        key = worker_id or worker or "unknown"
        if key not in calibrated_process_rates:
            calibrated_process_rates[key] = calibrated_progress_rate(
                manifest,
                worker,
                worker_id,
                fallback_worker_rates.get(worker or "unknown") or fallback_rates.get(unit),
            )
        return calibrated_process_rates[key]

    for task in manifest["tasks"]:
        state = task_state(manifest, task, lease_seconds=lease_seconds)
        counts[state] += 1
        worker = status_worker(manifest, task, state)
        worker_id = status_worker_id(manifest, task, state)
        index = task_display_index(task)
        config_progress = progress_config_unit_total(task)
        progress_config = task.get("progress")
        if config_progress is not None:
            unit, total = config_progress
            progress_totals[unit] = progress_totals.get(unit, 0) + total
        progress_data = task_log_progress_data(manifest, task) if state in {"done", "fail", "pending"} else None
        if state in {"running", "claimed", "stale"}:
            progress_data = task_heartbeat_progress_data(manifest, task)
            if progress_data is None and state in {"running", "stale"}:
                progress_data = task_log_progress_data(manifest, task)
            elif manifest_kind in {"selfplay-to-bullet", "selfplay-to-sf-oracle"}:
                progress_data = task_log_progress_data(manifest, task) or progress_data
        running_payload = task_running_payload(manifest, task) if state in {"running", "claimed", "stale"} else None
        if isinstance(progress_config, dict):
            progress_data = infer_progress_rate(progress_data, running_payload)
        output_rate = progress_output_rate(progress_data, running_elapsed_seconds(running_payload))
        if isinstance(progress_data, dict) and output_rate is not None:
            progress_data = {**progress_data, "output_rate": output_rate}
        if config_progress is not None and state == "done":
            unit, task_total = config_progress
            done_payload = task_done_payload(manifest, task)
            soft_cancelled = isinstance(done_payload, dict) and bool(
                done_payload.get("soft_cancel") or done_payload.get("skipped")
            )
            observed = progress_data.get("done") if isinstance(progress_data, dict) else None
            if soft_cancelled:
                completed = min(observed, task_total) if isinstance(observed, int) else 0
            else:
                completed = task_total
            progress_done[unit] = progress_done.get(unit, 0) + completed
        if progress_data is not None and config_progress is not None and state in {"fail", "pending"}:
            unit, task_total = config_progress
            observed = progress_data.get("done")
            if isinstance(observed, int) and observed > 0:
                progress_done[unit] = progress_done.get(unit, 0) + min(observed, task_total)
        progress_rate = progress_data.get("rate") if isinstance(progress_data, dict) else None
        progress_done_value = progress_data.get("done") if isinstance(progress_data, dict) else None
        needs_fallback_rate = (
            progress_data is None
            or (
                progress_done_value == 0
                and not (
                    isinstance(progress_rate, (int, float))
                    and progress_rate > 0
                )
            )
        )
        if needs_fallback_rate and config_progress is not None and state in {"running", "claimed", "stale"}:
            unit, total = config_progress
            fallback_data = fallback_progress_data(
                unit,
                total,
                process_calibrated_rate(worker, worker_id, unit),
                running_elapsed_seconds(running_payload),
            )
            if fallback_data is not None:
                if isinstance(progress_data, dict):
                    fallback_data["fields"] = progress_data.get("fields", {})
                    fallback_data["text"] = progress_text(fallback_data)
                progress_data = fallback_data
        if progress_data is not None and config_progress is not None and state in {"running", "claimed", "stale"}:
            unit, _task_total = config_progress
            progress_data = stabilize_progress_eta(
                progress_data,
                process_calibrated_rate(worker, worker_id, unit),
            )
        if progress_data is not None and config_progress is not None and state in {"running", "claimed", "stale"}:
            unit, task_total = config_progress
            offset_unit, offset = progress_task_offsets.get(str(task["id"]), (unit, 0))
            if offset_unit == unit:
                progress_data = progress_data_with_global_position(
                    progress_data,
                    unit=unit,
                    offset=offset,
                    total=progress_unit_totals.get(unit, task_total),
                )
        progress = str(progress_data.get("text") or "") if isinstance(progress_data, dict) else ""
        row = {
            "id": task["id"],
            "index": index,
            "label": task_display_label(manifest, task),
            "state": state,
            "worker": worker,
            "worker_id": worker_id,
            "progress": progress,
        }
        if state == "fail":
            reason = task_failure_reason(manifest, task)
            if reason:
                entry = failure_reasons.setdefault(reason, {"reason": reason, "count": 0, "tasks": []})
                entry["count"] += 1
                if len(entry["tasks"]) < 3:
                    entry["tasks"].append(index)
        if progress_data:
            row["progress_data"] = progress_data
            if progress_data.get("kind") == "metadata":
                output = progress_data.get("output")
                if isinstance(output, int):
                    output_unit = str(progress_data.get("output_unit") or "output")
                    progress_outputs[output_unit] = progress_outputs.get(output_unit, 0) + output
                    key = worker or "unknown"
                    worker_metric = worker_outputs.setdefault(
                        key,
                        {"worker": key, "output": 0, "output_seen": False},
                    )
                    worker_metric["output"] += output
                    worker_metric["output_seen"] = True
        progress_graph = task_log_progress_graph(manifest, task) if state == "running" else None
        if progress_graph:
            row["progress_graph"] = progress_graph
        if state == "running":
            key = worker or "unknown"
            elapsed = running_elapsed_seconds(running_payload)
            output_rate = progress_output_rate(progress_data, elapsed)
            fallback_rate = fallback_worker_rates.get(worker or "unknown")
            if output_rate is None and isinstance(fallback_rate, (int, float)) and fallback_rate > 0:
                output_rate = float(fallback_rate)
            if output_rate is not None:
                worker_rates[key] = worker_rates.get(key, 0.0) + output_rate
        rows.append(row)

        if state in {"done", "fail", "stale"}:
            key = worker or "unknown"
            groups[state].setdefault(key, []).append(index)
        if state in {"running", "claimed", "stale"}:
            active.append(row)
        elif state == "pending":
            pending.append(index)
            pending_groups.setdefault(row["label"], []).append(index)

    current_progress_graph = run_progress_graph_from_rows(
        manifest,
        rows,
        progress_task_offsets,
        progress_unit_totals,
    )
    progress_graph = run_progress_history_graph(manifest, current_progress_graph)
    worker_processes = worker_process_status_rows(
        manifest,
        pending=counts["pending"],
    )

    return {
        "tasks": len(manifest["tasks"]),
        "counts": counts,
        "active_count": counts["running"] + counts["claimed"] + counts["stale"],
        "groups": groups,
        "active": active,
        "pending": pending,
        "pending_groups": pending_groups,
        "rows": rows,
        "progress_totals": progress_totals,
        "progress_done": progress_done,
        "progress_outputs": progress_outputs,
        "worker_outputs": worker_outputs,
        "worker_rates": worker_rates,
        "worker_processes": worker_processes,
        "queue": queue_payload,
        "launch": launch_payload,
        "launch_state": launch_state,
        "stop": stop_payload,
        "progress_graph": progress_graph,
        "diagnostics": state_diagnostics(),
        "failure_reasons": sorted(
            failure_reasons.values(),
            key=lambda item: (-int(item["count"]), str(item["reason"])),
        ),
        "manifest_payload": manifest,
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

def print_status_group(
    title: str,
    groups: dict[str, list[int | str]],
    *,
    color_enabled: bool,
    show_empty: bool = False,
    indent: str = "",
) -> None:
    if not groups:
        if show_empty:
            print(f"{indent}{color_count_line(title, 0, status_color(title), color_enabled=color_enabled)}")
        return
    count = sum(len(values) for values in groups.values())
    print(f"{indent}{color_count_line(title, count, status_color(title), color_enabled=color_enabled)}")
    for worker, values in sorted(groups.items()):
        print(f"{indent}  {format_status_key(worker, values)} [{len(values)}]: {format_status_values(values)}")

def print_pending_status(
    summary: dict[str, Any],
    *,
    color_enabled: bool,
    show_empty: bool = False,
    indent: str = "",
) -> None:
    if not summary["pending"]:
        if show_empty:
            print(f"{indent}{color_count_line('pending', 0, 'yellow', color_enabled=color_enabled)}")
        return
    print(f"{indent}{color_count_line('pending', len(summary['pending']), 'yellow', color_enabled=color_enabled)}")
    groups = summary.get("pending_groups")
    if not isinstance(groups, dict) or not groups:
        print(f"{indent}  {format_status_values(summary['pending'])}")
        return
    for label, values in sorted(groups.items(), key=lambda item: (not item[0], item[0])):
        label_text = label if label else "unlabeled"
        print(f"{indent}  {label_text} [{len(values)}]: {format_status_values(values)}")

def active_status_sort_key(row: dict[str, Any]) -> tuple[int, float, tuple[bool, int | str]]:
    state = str(row.get("state") or "")
    if state in {"claimed", "stale"}:
        return (0, 0.0, status_value_key(row["index"]))
    data = row.get("progress_data")
    if isinstance(data, dict):
        if data.get("fallback_estimate"):
            return (3, 0.0, status_value_key(row["index"]))
        done = data.get("done")
        total = data.get("total")
        eta = progress_data_eta_seconds(data, done, total)
        if eta is not None:
            eta_s, _unknown_when_elapsed = eta
            return (1, max(0.0, float(eta_s)), status_value_key(row["index"]))
        done = data.get("done")
        total = data.get("total")
        if isinstance(done, int) and isinstance(total, int) and total > 0:
            return (2, -(done / total), status_value_key(row["index"]))
    return (3, 0.0, status_value_key(row["index"]))

def print_status_details(
    summary: dict[str, Any],
    *,
    color_enabled: bool = False,
    show_done: bool = False,
    show_diagnostics: bool = False,
    indent: str = "",
) -> None:
    show_empty = False
    if show_done:
        print_status_group("done", summary["groups"]["done"], color_enabled=color_enabled, show_empty=show_empty, indent=indent)
    active_rows = [row for row in summary["active"] if row.get("state") != "stale"]
    stale_rows = [row for row in summary["active"] if row.get("state") == "stale"]

    def print_active_rows(rows: list[dict[str, Any]]) -> None:
        for row in sorted(rows, key=active_status_sort_key):
            state_name = str(row.get("state") or "")
            progress = (
                f" [{color_progress_text(str(row['progress']), color_enabled=color_enabled)}]"
                if state_name == "running" and row.get("progress")
                else ""
            )
            label = f" {str(row['label'])}" if row.get("label") else ""
            worker = format_status_key(
                str(row.get("worker_id") or row["worker"] or "unknown"),
                [],
            )
            state = (
                f" [{colorize(state_name, status_color(state_name), enabled=color_enabled)}]"
                if state_name != "running"
                else ""
            )
            print(f"{indent}  {worker}{state}{label} shard={row['index']}{progress}")

    if active_rows:
        section_fields = active_section_fields(summary)
        section_fields = color_status_fields(section_fields, summary=summary, color_enabled=color_enabled)
        suffix = f" {' '.join(section_fields)}" if section_fields else ""
        active_title = color_count_line("active", len(active_rows), "yellow", color_enabled=color_enabled)
        print(f"{indent}{active_title}{suffix}")
        print_active_rows(active_rows)
    elif show_empty:
        print(f"{indent}{color_count_line('active', 0, 'yellow', color_enabled=color_enabled)}")
    if stale_rows:
        print(f"{indent}{color_count_line('stale', len(stale_rows), 'red', color_enabled=color_enabled)}")
        print_active_rows(stale_rows)
    process_rows = summary.get("worker_processes")
    if isinstance(process_rows, list) and process_rows:
        print(f"{indent}{color_count_line('worker processes', len(process_rows), 'cyan', color_enabled=color_enabled)}")
        for process in process_rows:
            worker_id = str(process.get("worker_id") or "unknown")
            state_name = str(process.get("state") or "unknown")
            age = compact_duration(float(process.get("state_age_s") or 0))
            print(f"{indent}  {worker_id} [{state_name}] {age}")
    print_pending_status(summary, color_enabled=color_enabled, show_empty=show_empty, indent=indent)
    print_status_group("fail", summary["groups"]["fail"], color_enabled=color_enabled, show_empty=show_empty, indent=indent)
    for item in summary.get("failure_reasons", []):
        reason = str(item.get("reason") or "").strip()
        if not reason:
            continue
        count = int(item.get("count") or 0)
        tasks = format_status_values([value for value in item.get("tasks", []) if isinstance(value, (int, str))])
        suffix = f" tasks={tasks}" if tasks != "-" else ""
        print(f"{indent}  reason [{count}]: {reason}{suffix}")
    if show_diagnostics and summary.get("diagnostics"):
        print(f"{indent}{colorize('diagnostics:', 'yellow', enabled=color_enabled)}")
        for item in summary["diagnostics"]:
            print(f"{indent}  {item}")

def print_compact_status(
    summary: dict[str, Any],
    *,
    color_enabled: bool = False,
    show_done: bool = False,
    show_diagnostics: bool = False,
) -> None:
    counts = summary["counts"]
    print(color_status_field(run_title(summary), summary=summary, color_enabled=color_enabled))
    if description := run_description_field(summary):
        print(f"  {color_status_field(description, summary=summary, color_enabled=color_enabled)}")
    if comment := run_comment_field(summary):
        print(f"  {color_status_field(comment, summary=summary, color_enabled=color_enabled)}")
    if show_diagnostics:
        print_status_commands(summary, indent="  ")
    fields = run_display_fields(summary)
    print(f"  {' '.join(color_status_fields(fields, summary=summary, color_enabled=color_enabled))}")
    print_status_details(
        summary,
        color_enabled=color_enabled,
        show_done=show_done,
        show_diagnostics=show_diagnostics,
        indent="  ",
    )

def _cmd_wait(args: argparse.Namespace) -> int:
    started_at = time.monotonic()
    manifest_path = expand_path(args.manifest)
    deadline = time.monotonic() + args.timeout_seconds if args.timeout_seconds > 0 else None
    notify_command = getattr(args, "notify_command", None) or default_notify_command()
    last_line = ""

    def deleted() -> int:
        print(f"run deleted while waiting: {manifest_path.parent.name}", file=sys.stderr)
        return 130

    def finish(event: str, state: str, rc: int, manifest: dict[str, Any]) -> int:
        if not manifest_path.exists():
            return deleted()
        finalization = None
        if event == "run_done" and state == "ok" and isinstance(manifest.get("finalize"), dict):
            try:
                verify_manifest(manifest, verify_requirements=False)
                from .labeling import finalize_labeling

                finalization = finalize_labeling(manifest_path, manifest)
            except (OSError, ValueError, json.JSONDecodeError, SystemExit) as exc:
                print(f"finalization failed: {exc}", file=sys.stderr)
                event, state, rc = "run_finalize_fail", "failed", 1
        if state != "ok":
            from .labeling import cleanup_partials

            finalizer = manifest.get("finalize")
            if isinstance(finalizer, dict):
                cleanup_partials(manifest, finalizer)
        launch_request_path(manifest).unlink(missing_ok=True)
        payload = run_completion_event_payload(
            manifest_path=manifest_path,
            manifest=manifest,
            event=event,
            state=state,
            rc=rc,
            elapsed_seconds=time.monotonic() - started_at,
            lease_seconds=args.lease_seconds,
        )
        if isinstance(finalization, dict):
            payload["finalization"] = finalization
            for key in ("merged_bytes", "positions", "shard_count", "inventory_digest", "task_totals"):
                if key in finalization:
                    payload[key] = finalization[key]
        write_json(run_summary_path(manifest_path), payload)
        try:
            from .web import remember_web_run_payload

            remember_web_run_payload(payload, coordinator_runs=configured_coordinator_runs())
        except ModuleNotFoundError as exc:
            if exc.name != "forge_lib.web":
                raise
        if notify_command:
            run_notify_command(notify_command, payload)
        return rc

    while True:
        try:
            manifest = apply_profile(load_manifest(manifest_path), args.profile)
        except FileNotFoundError:
            return deleted()
        requeued = reap_stale_tasks(
            manifest,
            lease_seconds=args.lease_seconds,
        )
        if requeued:
            print(f"requeued stale tasks={len(requeued)}", flush=True)
        summary = status_summary(manifest, lease_seconds=args.lease_seconds)
        summary["metadata"] = manifest_metadata(manifest)
        counts = summary["counts"]
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
            request_stop_for_run(manifest_path, manifest, lease_seconds=args.lease_seconds)
            return finish("run_fail", "failed", 1, manifest)
        decision = sprt_aggregate_decision(summary)
        if decision is not None:
            result = finalize_sprt_aggregate_decision(
                manifest_path,
                manifest,
                decision,
                lease_seconds=args.lease_seconds,
            )
            if result["changed"]:
                side = decision["side"]
                llr = float(decision["llr"])
                bound = float(decision["bound"])
                print(
                    f"sprt: aggregate {side} reached llr={llr:.2f}/{bound:.2f}; "
                    f"skipped={result['skipped']} cancel={result['cancelled']}",
                    flush=True,
                )
            summary = status_summary(manifest, lease_seconds=args.lease_seconds)
            summary["metadata"] = manifest_metadata(manifest)
            counts = summary["counts"]
        if counts["done"] == total:
            return finish("run_done", "ok", 0, manifest)
        active = counts["running"] + counts["claimed"] + counts["stale"]
        if run_launch_stale(summary) and active == 0 and counts["pending"] > 0:
            print("launch failed: no active workers and launch request is stale", file=sys.stderr)
            return finish("run_launch_failed", "failed", 1, manifest)
        if stop_requested(manifest) and active == 0:
            return finish("run_stopped", "stopped", 130, manifest)
        if deadline is not None and time.monotonic() >= deadline:
            print("timeout waiting for tasks", file=sys.stderr)
            return finish("run_timeout", "timeout", 124, manifest)
        time.sleep(max(0.1, args.interval_seconds))

def cmd_wait(args: argparse.Namespace) -> int:
    manifest_path = expand_path(args.manifest)
    try:
        return _cmd_wait(args)
    except FileNotFoundError:
        if not manifest_path.exists():
            print(f"run deleted while waiting: {manifest_path.parent.name}", file=sys.stderr)
            return 130
        raise
    except KeyboardInterrupt:
        if manifest_path.exists():
            manifest = load_manifest(manifest_path)
            cleanup = request_stop_for_run(
                manifest_path,
                manifest,
                lease_seconds=int(args.lease_seconds),
                reason="wait interrupted",
            )
            print(
                f"wait: interrupt cleanup complete killed={cleanup['killed']} "
                f"released={len(cleanup['released'])} reset={len(cleanup['reset'])}",
                file=sys.stderr,
                flush=True,
            )
        raise

def run_notify_command(command_text: str | None, payload: dict[str, Any], *, wait: bool = True) -> int:
    if not command_text:
        return 0
    command = shlex.split(command_text)
    if not command:
        return 0
    executable = Path(command[0]).expanduser()
    if not executable.is_absolute() and os.sep in command[0]:
        candidate = script_repo() / executable
        if candidate.is_file():
            command[0] = str(candidate)
    else:
        command[0] = str(executable)
    data = json.dumps(payload, sort_keys=True)
    env = os.environ.copy()
    env["FORGE_EVENT_JSON"] = data
    if not wait:
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
            env=env,
        )
        if proc.stdin is not None:
            try:
                proc.stdin.write(data + "\n")
                proc.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        return 0
    result = subprocess.run(
        command,
        input=data + "\n",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "notification command failed"
        print(f"notify failed rc={result.returncode}: {detail}", file=sys.stderr)
    return int(result.returncode)

def deploy_event_payload(
    *,
    run_dir: Path,
    manifest_path: Path,
    workers_path: Path,
    manifest: dict[str, Any],
    results: list[dict[str, Any]],
    failed: int,
    has_tasks: bool,
    elapsed_seconds: float,
    lease_seconds: int = 600,
) -> dict[str, Any]:
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    metadata = manifest_metadata(manifest)
    payload: dict[str, Any] = {
        "event": "deploy_fail" if failed else "deploy_done",
        "run": str(run_dir.name),
        "run_dir": str(run_dir),
        "manifest": str(manifest_path),
        "workers": str(workers_path),
        "state": "failed" if failed else "ok",
        "rc": 1 if failed else 0,
        "has_tasks": has_tasks,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "tasks": summary["tasks"],
        "counts": summary["counts"],
        "active_count": summary["active_count"],
        "worker_results": [
            {
                "host": str(item.get("host") or ""),
                "profile": str(item.get("profile") or ""),
                "slot": item.get("slot"),
                "slots": item.get("slots"),
                "stage": str(item.get("stage") or ""),
                "ok": bool(item.get("ok")),
                "rc": int(item.get("rc") or 0),
            }
            for item in sorted(results, key=lambda entry: int(entry["index"]))
        ],
    }
    payload.update(metadata)
    return payload

def run_completion_event_payload(
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
    event: str,
    state: str,
    rc: int,
    elapsed_seconds: float,
    lease_seconds: int,
) -> dict[str, Any]:
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    metadata = manifest_metadata(manifest)
    completed_epoch = epoch() if event == "run_done" and state == "ok" else None
    summary["manifest"] = str(manifest_path)
    summary["run"] = manifest_path.parent.name
    summary["metadata"] = metadata
    if completed_epoch is not None:
        summary["completed_epoch"] = completed_epoch
        summary["completed_at"] = format_epoch_timestamp(completed_epoch)
    status_payload = run_status_payload(summary)
    display = status_payload.get("display")
    commands = status_payload.get("commands")
    fields = display.get("fields") if isinstance(display, dict) else None
    payload: dict[str, Any] = {
        "event": event,
        "run": manifest_path.parent.name,
        "run_dir": str(manifest_path.parent),
        "manifest": str(manifest_path),
        "state": state,
        "rc": rc,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "tasks": summary["tasks"],
        "counts": summary["counts"],
        "active_count": summary["active_count"],
    }
    if completed_epoch is not None:
        payload["completed_epoch"] = completed_epoch
        payload["completed_at"] = format_epoch_timestamp(completed_epoch)
    if isinstance(display, dict):
        payload["display"] = display
    if isinstance(fields, list):
        payload["status_line"] = " ".join(str(field) for field in fields)
    if isinstance(commands, list):
        payload["commands"] = commands
    progress_graph = summary.get("progress_graph")
    if isinstance(progress_graph, dict):
        payload["progress_graph"] = progress_graph
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
    if summary_is_sprt(summary):
        stats = sprt_aggregate_stats(summary)
        sprt: dict[str, Any] = {}
        if isinstance(stats.get("wdl"), list):
            sprt["wdl"] = stats["wdl"]
            sprt["wins"], sprt["draws"], sprt["losses"] = stats["wdl"]
        if isinstance(stats.get("pentanomial"), list):
            sprt["pentanomial"] = stats["pentanomial"]
        decision = sprt_aggregate_decision(summary)
        if decision is not None:
            sprt["stopping_reason"] = decision
        if sprt:
            payload["sprt"] = sprt
    payload.update(metadata)
    return payload

def verify_manifest(manifest: dict[str, Any], *, verify_requirements: bool = True) -> None:
    if verify_requirements:
        check_requirements(manifest)
    failures = []
    expected_manifest_sha = manifest_checksum(manifest)
    for task in manifest["tasks"]:
        if not task_complete(manifest, task):
            failures.append(f"{task['id']}: {task_state(manifest, task)}")
            continue
        done = read_json(task_done_path(manifest, task))
        if done.get("soft_cancel"):
            continue
        expected_task_sha = task_execution_checksum(manifest, task)
        done_task_sha = str(done.get("task_execution_sha256") or "")
        done_manifest_sha = str(done.get("manifest_sha256") or "")
        if done_task_sha and done_task_sha != expected_task_sha:
            failures.append(f"{task['id']}: task execution checksum mismatch")
        elif (
            not done_task_sha
            and done_manifest_sha
            and done_manifest_sha != expected_manifest_sha
        ):
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
    verify_manifest(
        manifest,
        verify_requirements=not bool(getattr(args, "skip_requirements", False)),
    )
    print("ok")
    return 0

def append_file(src: Path, dst: Path, *, newline_between: bool) -> None:
    with src.open("rb") as in_handle, dst.open("ab") as out_handle:
        shutil.copyfileobj(in_handle, out_handle, length=1024 * 1024)
        if newline_between:
            out_handle.write(b"\n")

def cmd_merge(args: argparse.Namespace) -> int:
    manifest_path, manifest = load_run_manifest(args.run)
    verify_manifest(
        manifest,
        verify_requirements=not bool(getattr(args, "skip_requirements", False)),
    )
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
