from __future__ import annotations

from .core import *
from .manifest import *
from .tasks import *


def cmd_status(args: argparse.Namespace) -> int:
    color_enabled = not bool(args.no_color) and sys.stdout.isatty()
    selected = [name for name in ("all", "done", "failed") if bool(getattr(args, name))]
    if len(selected) > 1:
        raise SystemExit("choose only one of --all, --done, or --failed")
    coordinator_runs = configured_coordinator_runs()
    if coordinator_runs and status_can_delegate(args.run):
        return run_status_on_coordinator(args, coordinator_runs)
    filter_name = selected[0] if selected else None
    if args.run is None:
        summaries = load_run_summaries(lease_seconds=args.lease_seconds)
        return print_run_status(
            summaries,
            color_enabled=color_enabled,
            as_json=args.json,
            filter_name=filter_name,
            show_details=bool(args.all or getattr(args, "show_done", False) or getattr(args, "verbose", False)),
            show_done=bool(getattr(args, "show_done", False) or getattr(args, "verbose", False)),
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
        print(json.dumps(summary, sort_keys=True))
        return 0 if summary["counts"]["fail"] == 0 else 1
    print(colorize(f"== {format_manifest_path(manifest_path)} ==", "cyan", enabled=color_enabled))
    print_compact_status(
        summary,
        color_enabled=color_enabled,
        show_done=args.show_done or args.verbose,
        show_diagnostics=args.verbose,
    )
    if args.verbose:
        for row in summary["rows"]:
            print(f"{row['id']}\t{row['state']}\t{row['worker']}\t{row.get('label', '')}")
    return 0 if summary["counts"]["fail"] == 0 else 1

def status_can_delegate(run: str | None) -> bool:
    if run is None:
        return True
    path = Path(str(run))
    return not path.is_absolute() and len(path.parts) == 1

def status_command_args(args: argparse.Namespace) -> list[str]:
    command = ["status"]
    if args.run is not None:
        command.append(str(args.run))
    for name in ("verbose", "show_done", "all", "done", "failed", "json", "no_color"):
        if bool(getattr(args, name)):
            command.append("--" + name.replace("_", "-"))
    command.extend(["--lease-seconds", str(args.lease_seconds)])
    return command

def run_status_on_coordinator(args: argparse.Namespace, coordinator_runs: str) -> int:
    host, runs_path = parse_coordinator(coordinator_runs)
    command = status_command_args(args)
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
    if not bool(getattr(args, "json", False)):
        print(f"coordinator: {coordinator_runs}")
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode

def run_manifest_paths() -> list[Path]:
    root = runs_dir()
    if not root.exists():
        return []
    return sorted(
        path / RUN_MANIFEST_NAME
        for path in root.iterdir()
        if path.is_dir() and (path / RUN_MANIFEST_NAME).is_file()
    )

def load_run_summaries(*, lease_seconds: int) -> list[dict[str, Any]]:
    summaries = []
    for path in run_manifest_paths():
        manifest = load_manifest(path)
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

def run_queued(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    return summary["tasks"] > 0 and counts["pending"] == summary["tasks"] and not run_started(summary)

def run_current(summary: dict[str, Any]) -> bool:
    counts = summary["counts"]
    active = counts["running"] + counts["claimed"] + counts["stale"]
    return active > 0 or (counts["pending"] > 0 and run_started(summary))

def run_state(summary: dict[str, Any]) -> str:
    if run_failed(summary):
        return "failed"
    if run_current(summary):
        return "current"
    if run_queued(summary):
        return "queued"
    if run_done(summary):
        return "done"
    return "empty"

def filtered_run_summaries(summaries: list[dict[str, Any]], filter_name: str | None) -> list[dict[str, Any]]:
    if filter_name == "done":
        return [item for item in summaries if run_done(item)]
    if filter_name in {None, "current"}:
        return [item for item in summaries if run_current(item)]
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
    if not isinstance(rate, (int, float)) or rate <= 0:
        return None
    return max(0.0, (total - done) / float(rate))

def progress_eta_text(done: Any, total: Any, rate: Any) -> str:
    eta_s = progress_eta_seconds(done, total, rate)
    if eta_s is None:
        return ""
    return f"eta={compact_duration(eta_s)}"

def progress_data_eta_text(data: dict[str, Any], done: Any, total: Any) -> str:
    eta_s = data.get("eta_s")
    if isinstance(eta_s, (int, float)):
        if eta_s <= 0 and data.get("eta_overdue"):
            return "eta=unknown"
        return f"eta={compact_duration(eta_s)}"
    return progress_eta_text(done, total, data.get("rate"))

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
        main = f"{main} {unit}"

    parts = [main] if main else []
    if isinstance(output, int):
        parts.append(f"{output_unit}={compact_count(output)}")
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
    by_unit: dict[str, dict[str, Any]] = {}
    outputs: dict[str, int] = {}
    etas: list[str] = []
    for row in summary["active"]:
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
                eta_s = data.get("eta_s")
                if isinstance(eta_s, (int, float)):
                    bucket["etas"].append(max(0.0, float(eta_s)))
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
            subtask_known_total and subtask_known_rate and subtask_count == summary["active_count"]
            and summary["counts"]["pending"] == 0
        ):
            eta = progress_eta_text(subtask_done, subtask_total, subtask_rate)
            if eta:
                etas.append(eta)
        parts.append(text)
    for unit, bucket in sorted(by_unit.items()):
        active_done = int(bucket["done"])
        active_total = int(bucket["total"])
        total_work = summary.get("progress_totals", {}).get(unit)
        done_work = summary.get("progress_done", {}).get(unit, 0)
        if isinstance(total_work, int) and total_work > 0:
            done = min(total_work, int(done_work) + active_done)
            text = f"{compact_count(done)}/{compact_count(total_work)} {unit}"
            if summary["counts"]["pending"] == 0 and bucket["etas"]:
                max_eta = max(bucket["etas"])
                etas.append("eta=unknown" if max_eta <= 0 else f"eta={compact_duration(max_eta)}")
            elif bucket["rate"] > 0:
                eta = progress_eta_text(done, total_work, bucket["rate"])
                if eta:
                    etas.append(eta)
            parts.append(text)
        elif bucket["all_total"] and active_total > 0:
            text = f"{compact_count(active_done)}/{compact_count(active_total)} {unit}"
            if (
                bucket["all_rate"] and bucket["count"] == summary["active_count"]
                and summary["counts"]["pending"] == 0
            ):
                if bucket["etas"]:
                    max_eta = max(bucket["etas"])
                    eta = "eta=unknown" if max_eta <= 0 else f"eta={compact_duration(max_eta)}"
                else:
                    eta = progress_eta_text(active_done, active_total, bucket["rate"])
                if eta:
                    etas.append(eta)
            parts.append(text)
        else:
            parts.append(f"~{compact_count(active_done)} {unit}")
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
        entry = workers.setdefault(
            worker,
            {
                "worker": worker,
                "tasks": 0,
                "output": int(output_metric.get("output", 0)) if isinstance(output_metric, dict) else 0,
                "output_seen": bool(output_metric.get("output_seen")) if isinstance(output_metric, dict) else False,
            },
        )
        entry["tasks"] += 1
    return workers

def format_worker_detail(metric: dict[str, Any]) -> str:
    return f"{metric['worker']} tasks={metric['tasks']} output={compact_count(int(metric['output']))}"

def active_worker_summary(summary: dict[str, Any]) -> str:
    metrics = active_worker_metrics(summary)
    if not metrics:
        return ""
    values = list(metrics.values())
    total_output = sum(int(metric["output"]) for metric in values)
    parts = [f"workers={len(values)}", f"output={compact_count(total_output)}"]
    if any(bool(metric.get("output_seen")) for metric in values):
        ordered = sorted(values, key=lambda metric: (int(metric["output"]), str(metric["worker"])))
        max_output = int(ordered[-1]["output"])
        parts.append(f"min={ordered[0]['worker']}:{compact_count(int(ordered[0]['output']))}")
        parts.append(f"max={ordered[-1]['worker']}:{compact_count(max_output)}")
        if max_output > 0:
            parts.append(f"speedup={total_output / max_output:.1f}x")
    return " ".join(parts)

def sorted_worker_metrics(summary: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = list(active_worker_metrics(summary).values())
    return sorted(metrics, key=lambda item: (-int(item["output"]), str(item["worker"])))

def print_active_worker_metrics(summary: dict[str, Any]) -> None:
    metrics = sorted_worker_metrics(summary)
    if not metrics:
        return
    for metric in metrics:
        print(f"  {format_worker_detail(metric)}")

def run_status_payload(summary: dict[str, Any]) -> dict[str, Any]:
    counts = summary["counts"]
    payload = {
        "run": summary["run"],
        "state": run_state(summary),
        "manifest": summary["manifest"],
        "tasks": summary["tasks"],
        "done": counts["done"],
        "active": counts["running"] + counts["claimed"] + counts["stale"],
        "running": counts["running"],
        "claimed": counts["claimed"],
        "pending": counts["pending"],
        "fail": counts["fail"],
        "stale": counts["stale"],
    }
    metadata = summary.get("metadata")
    if isinstance(metadata, dict):
        payload.update(metadata)
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
    return payload

def run_metadata_fields(summary: dict[str, Any]) -> list[str]:
    metadata = summary.get("metadata")
    if not isinstance(metadata, dict):
        return []
    fields: list[str] = []
    kind = metadata.get("kind")
    description = metadata.get("description")
    if kind:
        fields.append(f"kind={kind}")
    if description:
        fields.append(f"desc={description}")
    return fields

def print_run_status(
    summaries: list[dict[str, Any]],
    *,
    color_enabled: bool,
    as_json: bool,
    filter_name: str | None,
    show_details: bool = False,
    show_done: bool = False,
) -> int:
    filtered = filtered_run_summaries(summaries, filter_name)
    if as_json:
        print(json.dumps({"runs": [run_status_payload(item) for item in filtered]}, sort_keys=True))
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
        print(f"no {qualifier}Crucible runs found in {runs_dir()}")
        return 0
    for summary in filtered:
        counts = summary["counts"]
        active = counts["running"] + counts["claimed"] + counts["stale"]
        state = run_state(summary)
        state_color = "red" if state == "failed" else "yellow" if state in {"current", "queued"} else "green"
        progress = summary_progress(summary)
        fields = [
            f"{summary['run']:<36} "
            f"{colorize(state, state_color, enabled=color_enabled)}",
            f"tasks={counts['done']}/{summary['tasks']}",
            *run_metadata_fields(summary),
            f"active={active}",
            f"running={counts['running']}",
            f"claimed={counts['claimed']}",
        ]
        if progress:
            fields.append(progress)
        fields.extend([
            f"fail={counts['fail']}",
            f"pending={counts['pending']}",
            f"stale={counts['stale']}",
        ])
        print(" ".join(fields))
        if show_details:
            print_status_details(
                summary,
                color_enabled=color_enabled,
                show_done=show_done,
                show_diagnostics=True,
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
        stopped += 1
        print(f"stop requested: {path.parent.name}")
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
    data["text"] = progress_text(data)
    return data

def task_log_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    path = task_log_path(manifest, task)
    try:
        text = log_tail(path)
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
            }
            if total is not None:
                data["total"] = total
            if output is not None:
                data["output"] = output
            if rate is not None:
                data["rate"] = rate
            return normalize_progress_data(data)

    progress_re = re.compile(r"\[\s*\d+/\d+\s+(\d+)\s*/\s*(\d+)\]")
    for line in reversed(text.splitlines()):
        match = progress_re.search(line)
        if match:
            done = int(match.group(1))
            total = int(match.group(2))
            return normalize_progress_data({"kind": "subtask", "done": done, "total": total})
    return None

def task_heartbeat_progress_data(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any] | None:
    payload = read_state_json(task_heartbeat_path(manifest, task))
    if payload is None:
        return None
    return normalize_progress_data(payload.get("progress_data"))

def status_summary(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, Any]:
    clear_state_diagnostics()
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
    completed_units: dict[str, int] = {}
    completed_elapsed: dict[str, float] = {}

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
    fallback_rates = {
        unit: completed_units[unit] / completed_elapsed[unit]
        for unit in completed_units
        if completed_elapsed.get(unit, 0.0) > 0
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
        rows.append(row)

        if state in {"done", "fail"}:
            key = worker or "unknown"
            groups[state].setdefault(key, []).append(index)
        elif state in {"running", "claimed", "stale"}:
            active.append(row)
        elif state == "pending":
            pending.append(index)
            pending_groups.setdefault(row["label"], []).append(index)

    return {
        "tasks": len(manifest["tasks"]),
        "counts": counts,
        "active_count": counts["running"] + counts["claimed"],
        "groups": groups,
        "active": active,
        "pending": pending,
        "pending_groups": pending_groups,
        "rows": rows,
        "progress_totals": progress_totals,
        "progress_done": progress_done,
        "progress_outputs": progress_outputs,
        "worker_outputs": worker_outputs,
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

def print_status_group(title: str, groups: dict[str, list[int | str]], *, color_enabled: bool) -> None:
    if not groups:
        return
    count = sum(len(values) for values in groups.values())
    print(colorize(f"{title}: {count}", status_color(title), enabled=color_enabled))
    for worker, values in sorted(groups.items()):
        print(f"  {worker} [{len(values)}]: {format_status_values(values)}")

def print_pending_status(summary: dict[str, Any], *, color_enabled: bool) -> None:
    if not summary["pending"]:
        return
    print(colorize(f"pending: {len(summary['pending'])}", "yellow", enabled=color_enabled))
    groups = summary.get("pending_groups")
    if not isinstance(groups, dict) or not groups:
        print(f"  {format_status_values(summary['pending'])}")
        return
    for label, values in sorted(groups.items(), key=lambda item: (not item[0], item[0])):
        label_text = json.dumps(label) if label else "unlabeled"
        print(f"  {label_text} [{len(values)}]: {format_status_values(values)}")

def active_status_sort_key(row: dict[str, Any]) -> tuple[int, float, tuple[bool, int | str]]:
    state = str(row.get("state") or "")
    if state in {"claimed", "stale"}:
        return (0, 0.0, status_value_key(row["index"]))
    data = row.get("progress_data")
    if isinstance(data, dict):
        eta_s = data.get("eta_s")
        if isinstance(eta_s, (int, float)):
            return (1, float(eta_s), status_value_key(row["index"]))
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
) -> None:
    if show_done:
        print_status_group("done", summary["groups"]["done"], color_enabled=color_enabled)
    if summary["active"]:
        worker_summary = active_worker_summary(summary)
        suffix = f" {worker_summary}" if worker_summary else ""
        print(colorize(f"active: {len(summary['active'])}{suffix}", "yellow", enabled=color_enabled))
        print_active_worker_metrics(summary)
        for row in sorted(summary["active"], key=active_status_sort_key):
            state_name = str(row.get("state") or "")
            progress = f" [{row['progress']}]" if state_name == "running" and row.get("progress") else ""
            label = f" {json.dumps(str(row['label']))}" if row.get("label") else ""
            worker = row["worker"] or "unknown"
            state = (
                f" [{colorize(state_name, status_color(state_name), enabled=color_enabled)}]"
                if state_name != "running"
                else ""
            )
            print(f"  {worker}{state}{label} task={row['index']}{progress}")
    print_status_group("fail", summary["groups"]["fail"], color_enabled=color_enabled)
    print_pending_status(summary, color_enabled=color_enabled)
    if show_diagnostics and summary.get("diagnostics"):
        print(colorize("diagnostics:", "yellow", enabled=color_enabled))
        for item in summary["diagnostics"]:
            print(f"  {item}")

def print_compact_status(
    summary: dict[str, Any],
    *,
    color_enabled: bool = False,
    show_done: bool = False,
    show_diagnostics: bool = False,
) -> None:
    counts = summary["counts"]
    fields = [
        f"tasks={counts['done']}/{summary['tasks']}",
        *run_metadata_fields(summary),
        f"active={summary['active_count']}",
        f"running={counts['running']}",
        f"claimed={counts['claimed']}",
    ]
    progress = summary_progress(summary)
    if progress:
        fields.append(progress)
    fields.extend([
        f"fail={counts['fail']}",
        f"pending={counts['pending']}",
        f"stale={counts['stale']}",
    ])
    print(" ".join(fields))
    print_status_details(
        summary,
        color_enabled=color_enabled,
        show_done=show_done,
        show_diagnostics=show_diagnostics,
    )

def cmd_wait(args: argparse.Namespace) -> int:
    started_at = time.monotonic()
    manifest_path = expand_path(args.manifest)
    deadline = time.monotonic() + args.timeout_seconds if args.timeout_seconds > 0 else None
    notify_command = getattr(args, "notify_command", None) or os.environ.get("CRUCIBLE_NOTIFY_COMMAND")
    last_line = ""

    def notify(event: str, state: str, rc: int, manifest: dict[str, Any]) -> None:
        if notify_command:
            run_notify_command(
                notify_command,
                run_completion_event_payload(
                    manifest_path=manifest_path,
                    manifest=manifest,
                    event=event,
                    state=state,
                    rc=rc,
                    elapsed_seconds=time.monotonic() - started_at,
                    lease_seconds=args.lease_seconds,
                ),
            )

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
            notify("run_fail", "failed", 1, manifest)
            return 1
        if counts["done"] == total:
            notify("run_done", "ok", 0, manifest)
            return 0
        if deadline is not None and time.monotonic() >= deadline:
            print("timeout waiting for tasks", file=sys.stderr)
            notify("run_timeout", "timeout", 124, manifest)
            return 124
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
    progress = summary_progress(summary)
    if progress:
        payload["progress"] = progress
    payload.update(manifest_metadata(manifest))
    return payload

def verify_manifest(manifest: dict[str, Any]) -> None:
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
    verify_manifest(manifest)
    print("ok")
    return 0

def append_file(src: Path, dst: Path, *, newline_between: bool) -> None:
    with src.open("rb") as in_handle, dst.open("ab") as out_handle:
        shutil.copyfileobj(in_handle, out_handle, length=1024 * 1024)
        if newline_between:
            out_handle.write(b"\n")

def cmd_merge(args: argparse.Namespace) -> int:
    manifest_path, manifest = load_run_manifest(args.run)
    verify_manifest(manifest)
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
