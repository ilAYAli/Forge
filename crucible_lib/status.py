from __future__ import annotations

import math

from .config import default_notify_command
from .core import *
from .manifest import *
from .tasks import *


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
    if args.json:
        print(json.dumps(run_status_payload(summary), sort_keys=True))
        return 0 if summary["counts"]["fail"] == 0 else 1
    print_compact_status(
        summary,
        color_enabled=color_enabled,
        show_done=args.show_done,
        show_diagnostics=args.verbose,
    )
    return 0 if summary["counts"]["fail"] == 0 else 1

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
            "command -v crucible >/dev/null 2>&1 || "
            "{ echo 'crucible not found in PATH on coordinator' >&2; exit 127; }; "
            f"exec env {shlex.quote(COORDINATOR_RUNS_ENV)}= "
            f"{shlex.quote(COLOR_ENV)}={shlex.quote(color_mode)} "
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
        return 0 if summary["counts"]["fail"] == 0 else 1
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

def run_manifest_has_attention_state(manifest: dict[str, Any]) -> bool:
    try:
        state = state_dir(manifest)
        if queue_request_path(manifest).exists() or launch_request_path(manifest).exists():
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
        manifest = load_manifest(path)
        if attention_only and not run_manifest_has_attention_state(manifest):
            continue
        summary = status_summary(manifest, lease_seconds=lease_seconds)
        summary["manifest"] = str(path)
        summary["run"] = path.parent.name
        summary["metadata"] = manifest_metadata(manifest)
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
    return summary["counts"]["fail"] > 0

def run_started(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    return counts["done"] + counts["fail"] + counts["running"] + counts["claimed"] + counts["stale"] > 0

def run_launching(summary: dict[str, Any]) -> bool:
    return isinstance(summary.get("launch"), dict)

def run_queued(summary: dict[str, Any]) -> bool:
    return run_queue_requested(summary)

def run_queue_requested(summary: dict[str, Any]) -> bool:
    return isinstance(summary.get("queue"), dict)

def run_current(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    active = counts["running"] + counts["claimed"] + counts["stale"]
    return run_launching(summary) or active > 0 or (counts["pending"] > 0 and run_started(summary))

def run_active_count(summary: dict[str, Any]) -> int:
    counts = summary["counts"]
    return int(counts["running"] + counts["claimed"] + counts["stale"])

def run_attention(summary: dict[str, Any]) -> bool:
    return (
        run_active_count(summary) > 0
        or run_launching(summary)
        or summary["counts"]["fail"] > 0
        or run_queue_requested(summary)
    )

def run_attention_sort_key(summary: dict[str, Any]) -> tuple[int, int, str]:
    active = run_active_count(summary)
    fail = int(summary["counts"]["fail"])
    if active > 0 or run_launching(summary):
        return (0, -active, str(summary["run"]))
    if fail > 0:
        return (1, -fail, str(summary["run"]))
    if run_queue_requested(summary):
        return (2, 0, str(summary["run"]))
    return (3, 0, str(summary["run"]))

def run_state(summary: dict[str, Any]) -> str:
    if run_failed(summary):
        return "failed"
    if run_current(summary):
        return "current"
    if run_done(summary):
        return "done"
    if run_queued(summary):
        return "queued"
    return "empty"

def filtered_run_summaries(
    summaries: list[dict[str, Any]],
    filter_name: str | None,
    *,
    include_started_pending: bool = False,
) -> list[dict[str, Any]]:
    if filter_name == "done":
        return [item for item in summaries if run_done(item)]
    if filter_name in {None, "current"}:
        filtered = [
            item for item in summaries
            if run_attention(item) or (include_started_pending and run_current(item))
        ]
        return sorted(filtered, key=run_attention_sort_key)
    if filter_name == "all":
        return summaries
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

def eta_field(seconds: float, *, unknown_when_elapsed: bool = False) -> str:
    if seconds <= 0 and unknown_when_elapsed:
        return "eta=unknown"
    return f"eta={compact_duration(max(0.0, seconds))}"

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
    return eta_field(seconds, unknown_when_elapsed=unknown_when_elapsed)

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
        "rate": rate,
    }
    full_eta = progress_eta_seconds(0, total, rate)
    if full_eta is not None and isinstance(elapsed_s, (int, float)) and elapsed_s > 0:
        remaining = full_eta - float(elapsed_s)
        data["eta_s"] = max(0.0, remaining)
        if remaining <= 0:
            data["eta_overdue"] = True
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

def summary_progress(summary: dict[str, Any]) -> str:
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

    def append_eta_with_active_floor(done: Any, total: Any, rate: Any, active_etas: list[tuple[float, bool]]) -> None:
        aggregate_eta = progress_eta_seconds(done, total, rate)
        if aggregate_eta is None:
            return
        unknown_when_elapsed = False
        if active_etas:
            max_active_eta, active_unknown_when_elapsed = max(active_etas, key=lambda item: item[0])
            if max_active_eta > aggregate_eta:
                aggregate_eta = max_active_eta
                unknown_when_elapsed = active_unknown_when_elapsed
        etas.append(eta_field(aggregate_eta, unknown_when_elapsed=unknown_when_elapsed))

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
            if waiting_count == 0 and bucket["etas"]:
                max_eta, unknown_when_elapsed = max(bucket["etas"], key=lambda item: item[0])
                etas.append(eta_field(max_eta, unknown_when_elapsed=unknown_when_elapsed))
            elif bucket["rate"] > 0:
                append_eta_with_active_floor(done, total_work, bucket["rate"], bucket["etas"])
            parts.append(text)
        elif bucket["all_total"] and active_total > 0:
            text = f"{unit}={compact_count(active_done)}/{compact_count(active_total)}"
            if (
                bucket["all_rate"] and bucket["count"] == running_count
                and waiting_count == 0
            ):
                if bucket["etas"]:
                    max_eta, unknown_when_elapsed = max(bucket["etas"], key=lambda item: item[0])
                    eta = eta_field(max_eta, unknown_when_elapsed=unknown_when_elapsed)
                else:
                    eta = progress_eta_text(active_done, active_total, bucket["rate"])
                if eta:
                    etas.append(eta)
            elif bucket["all_rate"] and bucket["rate"] > 0:
                append_eta_with_active_floor(active_done, active_total, bucket["rate"], bucket["etas"])
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

def active_worker_summary(summary: dict[str, Any]) -> str:
    metrics = active_worker_metrics(summary)
    if not metrics:
        return ""
    values = list(metrics.values())
    output_values = [metric for metric in values if bool(metric.get("output_seen"))]
    if output_values:
        total_output = sum(int(metric["output"]) for metric in values)
        if len(values) == 1:
            return f"output={compact_count(total_output)}"
        parts = [f"workers={len(values)}", f"output={compact_count(total_output)}"]
        ordered = sorted(values, key=lambda metric: (int(metric["output"]), str(metric["worker"])))
        max_output = int(ordered[-1]["output"])
        parts.append(f"min={str(ordered[0]['worker'])}:{compact_count(int(ordered[0]['output']))}")
        parts.append(f"max={str(ordered[-1]['worker'])}:{compact_count(max_output)}")
        if speedup := run_speedup_field(summary):
            parts.append(speedup)
        return " ".join(parts)

    rate_values = [metric for metric in values if bool(metric.get("progress_rate_seen"))]
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
    if speedup := run_speedup_field(summary):
        parts.append(speedup)
    return " ".join(parts)

def active_worker_summary_payload(summary: dict[str, Any]) -> dict[str, Any]:
    metrics = active_worker_metrics(summary)
    if not metrics:
        return {}
    values = list(metrics.values())
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
            max_output = int(ordered[-1]["output"])
            if max_output > 0:
                speedup = total_output / max_output
                payload["speedup"] = round(speedup, 3)
                payload["speedup_text"] = f"{speedup:.1f}x"
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
            max_rate = float(ordered[-1]["progress_rate"])
            if max_rate > 0:
                speedup = total_rate / max_rate
                payload["speedup"] = round(speedup, 3)
                payload["speedup_text"] = f"{speedup:.1f}x"
    return payload

def run_speedup_field(summary: dict[str, Any]) -> str:
    metrics = active_worker_metrics(summary)
    if len(metrics) <= 1:
        return ""
    values = list(metrics.values())
    if any(bool(metric.get("output_seen")) for metric in values):
        outputs = [int(metric.get("output", 0)) for metric in values]
        max_output = max(outputs)
        if max_output <= 0:
            return ""
        return f"speedup={sum(outputs) / max_output:.1f}x"
    rates = [
        float(metric.get("progress_rate", 0.0))
        for metric in values
        if bool(metric.get("progress_rate_seen"))
    ]
    if not rates:
        return ""
    max_rate = max(rates)
    if max_rate <= 0:
        return ""
    return f"speedup={sum(rates) / max_rate:.1f}x"

def run_status_payload(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary["counts"]
    fields = [
        f"tasks={counts['done']}/{summary['tasks']}",
    ]
    if counts["fail"]:
        fields.append(f"fail={counts['fail']}")
    if counts["stale"]:
        fields.append(f"stale={counts['stale']}")
    fields.extend(run_progress_fields(summary))
    active_summary = active_worker_summary(summary)
    active_fields = active_section_fields(summary)
    payload = copy.deepcopy(summary)
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
    metadata = summary.get("metadata")
    if isinstance(metadata, dict):
        payload.update(metadata)
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
        payload["progress_fields"] = summary_progress_fields(summary)
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
    if key in {"active", "claimed", "pending", "running"}:
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
    return split_eta_last([
        field
        for field in fields
        if (
            field.startswith("eta=")
            or field.startswith("games=")
            or field.split("=", 1)[0] in SPRT_PROGRESS_FIELD_KEYS
        )
    ])

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
    return f"failed: {colorize(str(count), 'red', enabled=color_enabled)} {noun} (use crucible status --failed)"

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
        return 0 if all(item["counts"]["fail"] == 0 for item in filtered) else 1
    if not filtered:
        labels = {
            None: "active",
            "all": "",
            "done": "completed",
            "failed": "failed",
        }
        label = labels.get(filter_name, "")
        qualifier = f"{label} " if label else ""
        print(f"no {qualifier}Crucible runs found in {runs_label or runs_dir()}")
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
        fields = [
            f"tasks={counts['done']}/{summary['tasks']}",
        ]
        if not show_details:
            fields = [run_title(summary), *fields]
        if show_details and counts["fail"]:
            fields.append(f"fail={counts['fail']}")
        if show_details:
            if counts["stale"]:
                fields.append(f"stale={counts['stale']}")
            fields.extend(run_progress_fields(summary))
        else:
            fields.extend([
                f"active={active}",
                f"running={counts['running']}",
                f"claimed={counts['claimed']}",
                *section_progress_fields(summary),
                f"fail={counts['fail']}",
                f"pending={counts['pending']}",
                f"stale={counts['stale']}",
            ])
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
    return 0 if all(item["counts"]["fail"] == 0 for item in filtered) else 1

def load_run_manifest(run: str | Path) -> tuple[Path, dict[str, Any]]:
    path = run_manifest_path(run)
    if not path.exists():
        raise SystemExit(f"{run}: no Crucible run manifest found at {path}")
    return path, load_manifest(path)

def run_summary_for(run: str | Path, *, lease_seconds: int) -> dict[str, Any]:
    path, manifest = load_run_manifest(run)
    summary = status_summary(manifest, lease_seconds=lease_seconds)
    summary["manifest"] = str(path)
    summary["run"] = path.parent.name
    summary["metadata"] = manifest_metadata(manifest)
    return summary

def cmd_stop(args: argparse.Namespace) -> int:
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
        payload = {
            "schema": SCHEMA,
            "run": path.parent.name,
            "requested_at": now(),
            "requested_epoch": epoch(),
            "host": socket.gethostname(),
        }
        write_json(stop_request_path(manifest), payload)
        released = release_active_claims(manifest, lease_seconds=args.lease_seconds)
        stopped += 1
        print(f"stop requested: {path.parent.name} released={len(released)}")
    if stopped == 0:
        print("no current runs to stop")
    return 0

def cmd_drain(args: argparse.Namespace) -> int:
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
    answer = input(f"delete Crucible run(s) {names}? type 'yes' to confirm: ")
    if answer != "yes":
        raise SystemExit("delete cancelled")

def cmd_del(args: argparse.Namespace) -> int:
    if args.done and args.run:
        raise SystemExit("del accepts either a run name or --done")
    if not args.done and not args.run:
        raise SystemExit("del requires a run name or --done")
    summaries = load_run_summaries(lease_seconds=args.lease_seconds)
    targets: list[Path] = []
    if args.done:
        targets = [Path(item["manifest"]).parent for item in summaries if run_done(item)]
    else:
        summary = run_summary_for(args.run, lease_seconds=args.lease_seconds)
        if run_current(summary) and not args.force:
            raise SystemExit(f"{args.run}: run is active; stop it first or pass --force")
        targets = [Path(summary["manifest"]).parent]
    if not targets:
        print("no runs matched")
        return 0
    confirm_delete(targets, force=bool(args.force))
    for path in targets:
        shutil.rmtree(path)
        print(f"deleted: {path.name}")
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

def log_tail(path: Path, *, max_bytes: int = 8192) -> str:
    with path.open("rb") as file:
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(max(0, size - max_bytes))
        return file.read().decode("utf-8", errors="replace")

def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.replace("_", "").replace(",", ""))
    except ValueError:
        return None

def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value.replace("_", "").replace(",", ""))
    except ValueError:
        return None

SPRT_PROGRESS_RE = re.compile(
    r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]\s+"
    r"Elo\s+(?P<elo>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+\+/-\s+"
    r"(?P<err>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+\|\s+"
    r"LLR\s+(?P<llr>[+-]?\d+(?:\.\d+)?)/(?P<llr_bound>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s+"
    r"\(\s*(?P<llr_pct>[+-]?\d+(?:\.\d+)?)%\)\s+\|\s+"
    r"LOS\s+(?P<los>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%\s+\|\s+"
    r"draw\s+(?P<draw>[+-]?(?:\d+(?:\.\d+)?|inf|nan))\s*%",
    re.IGNORECASE,
)

SPRT_PROGRESS_FIELD_KEYS = {"elo", "ci", "llr", "los", "draw"}

def sprt_progress_line_fields(line: str) -> dict[str, str]:
    match = SPRT_PROGRESS_RE.search(line)
    if not match:
        return {}
    values = match.groupdict()
    llr = values["llr"].strip()
    llr_bound = values["llr_bound"].strip()
    llr_pct = values["llr_pct"].strip()
    return {
        "elo": values["elo"].strip(),
        "ci": values["err"].strip(),
        "llr": f"{llr}/{llr_bound} ({llr_pct}%)",
        "los": values["los"].strip(),
        "draw": values["draw"].strip(),
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

def progress_series_specs(config: dict[str, Any], *, kind: str | None = None) -> list[dict[str, str]]:
    raw = config.get("series")
    if not isinstance(raw, list):
        if str(kind or "").lower() == "sprt":
            return [
                {"name": "llr", "field": "llr", "x": "done"},
                {"name": "elo", "field": "elo", "x": "done"},
            ]
        return []
    specs: list[dict[str, str]] = []
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
        })
    return specs

def progress_numeric_value(name: str, data: dict[str, Any], fields: dict[str, str]) -> float | None:
    if name in {"done", "total", "output", "rate", "global_done", "global_total"}:
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

    series = [
        {"name": name, "points": points}
        for name, points in points_by_name.items()
        if len(points) >= 2
    ]
    if not series:
        return None
    return {
        "x_unit": str(config.get("unit") or ""),
        "series": series,
    }

def task_progress_graph_point_specs(manifest: dict[str, Any], task: dict[str, Any]) -> list[dict[str, str]]:
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
    x_unit = ""
    for row in rows:
        task = tasks_by_id.get(str(row.get("id") or ""))
        if task is None:
            continue
        specs = task_progress_graph_point_specs(manifest, task)
        if not specs:
            continue
        data = row.get("progress_data")
        if not isinstance(data, dict) or data.get("kind") != "metadata":
            continue
        fields = data.get("fields")
        if not isinstance(fields, dict):
            continue
        graph_data = dict(data)
        config_progress = progress_config_unit_total(task)
        if config_progress is not None and isinstance(data.get("done"), int):
            unit, task_total = config_progress
            x_unit = x_unit or unit
            offset_unit, offset = progress_task_offsets.get(str(task["id"]), (unit, 0))
            if offset_unit == unit:
                graph_data.setdefault("global_done", offset + int(data["done"]))
                graph_data.setdefault("global_total", progress_unit_totals.get(unit, task_total))
        for spec in specs:
            x_name = "global_done" if spec["x"] == "done" and "global_done" in graph_data else spec["x"]
            x = progress_numeric_value(x_name, graph_data, fields)
            y = progress_numeric_value(spec["field"], graph_data, fields)
            if x is None or y is None:
                continue
            points_by_name.setdefault(spec["name"], []).append({"x": x, "y": y})

    series = []
    for name, points in points_by_name.items():
        points.sort(key=lambda point: point["x"])
        if len(points) >= 2:
            series.append({"name": name, "points": points})
    if not series:
        return None
    return {
        "x_unit": x_unit,
        "series": series,
    }

def elo_score_rate(elo: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, -elo / 400.0))

def score_rate_elo(score_rate: float) -> float:
    bounded = min(1.0 - 1e-12, max(1e-12, score_rate))
    return 400.0 * math.log10(bounded / (1.0 - bounded))

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

def sprt_aggregate_fields(summary: dict[str, Any]) -> list[str]:
    if not summary_is_sprt(summary):
        return []

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
        return []
    fields = [f"elo={score_rate_elo(weighted_score / weight_total):+.1f}"]
    if ci_weight_total > 0:
        fields.append(f"ci={math.sqrt(weighted_ci_squares) / ci_weight_total:.1f}")
    if llr_count > 0:
        fields.append(format_llr_field(llr_total, llr_bound, llr_mixed_bound))
    if los_weight_total > 0:
        fields.append(f"los={weighted_los / los_weight_total:.1f}%")
    if draw_weight_total > 0:
        fields.append(f"draw={weighted_draw / draw_weight_total:.1f}%")
    return fields

PROGRESS_RESERVED_FIELDS = {"done", "total", "output", "rate"}

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

def task_log_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    path = task_log_path(manifest, task)
    try:
        text = log_tail(path)
        eta_epoch = path.stat().st_mtime
    except OSError:
        return None

    config = task.get("progress")
    if isinstance(config, dict) and config.get("log_regex"):
        try:
            progress_re = re.compile(str(config["log_regex"]))
        except re.error:
            return None
        for line in reversed(text.splitlines()):
            match = progress_re.search(line)
            if not match:
                continue
            done = parse_int(match.groupdict().get("done"))
            if done is None:
                continue
            total = parse_int(match.groupdict().get("total"))
            if total is None:
                total_value = config.get("total")
                total = total_value if isinstance(total_value, int) else parse_int(str(total_value)) if total_value is not None else None
            output = parse_int(match.groupdict().get("output"))
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
            if fields:
                data["fields"] = fields
            return normalize_progress_data(data)

    if isinstance(config, dict):
        for line in reversed(text.splitlines()):
            data = sprt_progress_line_data(line, config, eta_epoch=eta_epoch)
            if data is not None:
                return data

    progress_re = re.compile(r"\[\s*\d+/\d+\s+(\d+)\s*/\s*(\d+)\]")
    for line in reversed(text.splitlines()):
        match = progress_re.search(line)
        if match:
            done = int(match.group(1))
            total = int(match.group(2))
            return normalize_progress_data({"kind": "subtask", "done": done, "total": total, "eta_epoch": eta_epoch})
    return None

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
    queue_payload = read_state_json(queue_request_path(manifest))
    launch_payload = read_state_json(launch_request_path(manifest))
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
            progress_data = task_log_progress_data(manifest, task)
            output_rate = progress_output_rate(progress_data, float(elapsed))
            if output_rate is not None:
                worker = status_worker(manifest, task, "done") or "unknown"
                completed_worker_task_rates.setdefault(worker, []).append(output_rate)
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

    for task in manifest["tasks"]:
        state = task_state(manifest, task, lease_seconds=lease_seconds)
        counts[state] += 1
        worker = status_worker(manifest, task, state)
        index = task_display_index(task)
        config_progress = progress_config_unit_total(task)
        progress_config = task.get("progress")
        if config_progress is not None:
            unit, total = config_progress
            progress_totals[unit] = progress_totals.get(unit, 0) + total
            if state == "done":
                progress_done[unit] = progress_done.get(unit, 0) + total
        progress_data = task_log_progress_data(manifest, task) if state in {"done", "running"} else None
        if progress_data is None and state in {"running", "claimed", "stale"}:
            progress_data = task_heartbeat_progress_data(manifest, task)
        running_payload = task_running_payload(manifest, task) if state in {"running", "claimed", "stale"} else None
        if isinstance(progress_config, dict):
            progress_data = infer_progress_rate(progress_data, running_payload)
        if progress_data is None and config_progress is not None and state in {"running", "claimed", "stale"}:
            unit, total = config_progress
            progress_data = fallback_progress_data(
                unit,
                total,
                fallback_rates.get(unit),
                running_elapsed_seconds(running_payload),
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
            "progress": progress,
        }
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

        if state in {"done", "fail"}:
            key = worker or "unknown"
            groups[state].setdefault(key, []).append(index)
        elif state in {"running", "claimed", "stale"}:
            active.append(row)
        elif state == "pending":
            pending.append(index)
            pending_groups.setdefault(row["label"], []).append(index)

    progress_graph = run_progress_graph_from_rows(
        manifest,
        rows,
        progress_task_offsets,
        progress_unit_totals,
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
        "queue": queue_payload,
        "launch": launch_payload,
        "progress_graph": progress_graph,
        "diagnostics": state_diagnostics(),
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
    show_empty = show_diagnostics
    if show_done:
        print_status_group("done", summary["groups"]["done"], color_enabled=color_enabled, show_empty=show_empty, indent=indent)
    if summary["active"]:
        section_fields = active_section_fields(summary)
        section_fields = color_status_fields(section_fields, summary=summary, color_enabled=color_enabled)
        suffix = f" {' '.join(section_fields)}" if section_fields else ""
        active_title = color_count_line("active", len(summary["active"]), "yellow", color_enabled=color_enabled)
        print(f"{indent}{active_title}{suffix}")
        for row in sorted(summary["active"], key=active_status_sort_key):
            state_name = str(row.get("state") or "")
            progress = (
                f" [{color_progress_text(str(row['progress']), color_enabled=color_enabled)}]"
                if state_name == "running" and row.get("progress")
                else ""
            )
            label = f" {str(row['label'])}" if row.get("label") else ""
            worker = format_status_key(str(row["worker"] or "unknown"), [])
            state = (
                f" [{colorize(state_name, status_color(state_name), enabled=color_enabled)}]"
                if state_name != "running"
                else ""
            )
            print(f"{indent}  {worker}{state}{label} shard={row['index']}{progress}")
    elif show_empty:
        print(f"{indent}{color_count_line('active', 0, 'yellow', color_enabled=color_enabled)}")
    print_pending_status(summary, color_enabled=color_enabled, show_empty=show_empty, indent=indent)
    print_status_group("fail", summary["groups"]["fail"], color_enabled=color_enabled, show_empty=show_empty, indent=indent)
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
    fields = [
        f"tasks={counts['done']}/{summary['tasks']}",
    ]
    if counts["fail"]:
        fields.append(f"fail={counts['fail']}")
    if counts["stale"]:
        fields.append(f"stale={counts['stale']}")
    fields.extend(run_progress_fields(summary))
    print(f"  {' '.join(color_status_fields(fields, summary=summary, color_enabled=color_enabled))}")
    print_status_details(
        summary,
        color_enabled=color_enabled,
        show_done=show_done,
        show_diagnostics=show_diagnostics,
        indent="  ",
    )

RUN_SUMMARY_NAME = "summary.json"


def run_summary_path(manifest_path: Path) -> Path:
    return manifest_path.parent / RUN_SUMMARY_NAME


def cmd_wait(args: argparse.Namespace) -> int:
    started_at = time.monotonic()
    manifest_path = expand_path(args.manifest)
    deadline = time.monotonic() + args.timeout_seconds if args.timeout_seconds > 0 else None
    notify_command = getattr(args, "notify_command", None) or default_notify_command()
    last_line = ""

    def finish(event: str, state: str, rc: int, manifest: dict[str, Any]) -> int:
        payload = run_completion_event_payload(
            manifest_path=manifest_path,
            manifest=manifest,
            event=event,
            state=state,
            rc=rc,
            elapsed_seconds=time.monotonic() - started_at,
            lease_seconds=args.lease_seconds,
        )
        write_json(run_summary_path(manifest_path), payload)
        try:
            from .web import remember_web_run_payload

            remember_web_run_payload(payload, coordinator_runs=configured_coordinator_runs())
        except ModuleNotFoundError as exc:
            if exc.name != "crucible_lib.web":
                raise
        if notify_command:
            run_notify_command(notify_command, payload)
        return rc

    while True:
        manifest = apply_profile(load_manifest(manifest_path), args.profile)
        counts = task_counts(manifest, lease_seconds=args.lease_seconds)
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
            return finish("run_fail", "failed", 1, manifest)
        if counts["done"] == total:
            return finish("run_done", "ok", 0, manifest)
        if deadline is not None and time.monotonic() >= deadline:
            print("timeout waiting for tasks", file=sys.stderr)
            return finish("run_timeout", "timeout", 124, manifest)
        time.sleep(max(0.1, args.interval_seconds))

def run_notify_command(command_text: str | None, payload: dict[str, Any]) -> int:
    if not command_text:
        return 0
    command = shlex.split(command_text)
    if not command:
        return 0
    data = json.dumps(payload, sort_keys=True)
    env = os.environ.copy()
    env["CRUCIBLE_EVENT_JSON"] = data
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
    summary["manifest"] = str(manifest_path)
    summary["run"] = manifest_path.parent.name
    summary["metadata"] = metadata
    display = run_status_payload(summary).get("display")
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
    if isinstance(display, dict):
        payload["display"] = display
    if isinstance(fields, list):
        payload["status_line"] = " ".join(str(field) for field in fields)
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
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
        done_manifest_sha = str(done.get("manifest_sha256") or "")
        if done_manifest_sha and done_manifest_sha != expected_manifest_sha:
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
