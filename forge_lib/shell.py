from __future__ import annotations

import json
import tempfile

from .core import *
from .config import config_template_dirs
from .manifest import SCHEMA
from .workers import (
    run_worker_shell,
    is_local_worker,
    sync_inputs_to_worker,
    worker_host,
    worker_path_map,
    worker_records_from_payload,
    worker_record_slots,
    workers_payload_from_arg,
)
from .templates import worker_path_parameter


def one_slot_per_host_payload(workers_arg: str | None) -> dict[str, Any]:
    payload = workers_payload_from_arg(workers_arg, default_ok=True)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for worker in worker_records_from_payload(payload, label=workers_arg or str(runs_dir() / RUN_WORKERS_NAME)):
        host = worker_host(worker)
        if not host or host in seen or worker_record_slots(worker) == 0:
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


def cmd_sh(args: argparse.Namespace) -> int:
    workers_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(workers_payload, label="sh workers")
    command = home_relative_command(args.command)
    timeout = bounded_timeout(args.remote_timeout_seconds)
    jobs = max(1, int(args.jobs))
    tracker = ProcessTracker()
    failed: dict[str, subprocess.CompletedProcess[str]] = {}

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
                    failed[label] = result
    except KeyboardInterrupt:
        tracker.terminate_all()
        raise

    if failed:
        write_shell_stream(sys.stderr, format_shell_failure_summary(failed, labels))
        return 1
    return 0


def _build_worker_platform(worker: dict[str, Any], *, timeout: float | None) -> tuple[str, str]:
    result = run_worker_shell(
        worker,
        "printf '%s\\t%s\\n' \"$(uname -s)\" \"$(uname -m)\"",
        timeout=timeout,
        retries=0,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"failed rc={result.returncode}"
        raise RuntimeError(f"{worker_host(worker)}: could not determine platform: {detail}")
    line = next((line.strip() for line in reversed(result.stdout.splitlines()) if line.strip()), "")
    fields = line.split("\t")
    if len(fields) != 2 or not all(fields):
        raise RuntimeError(f"{worker_host(worker)}: unexpected platform response: {line!r}")
    return fields[0], fields[1]


def _build_remote_path(repo_var: str, value: str, *, hash_var: str | None = None) -> str:
    resolver = (
        "import os,sys; p=os.path.expanduser(sys.argv[2]); "
        + ("p=p.replace('{hash}',sys.argv[3]); " if hash_var else "")
        + "print(os.path.abspath(p if os.path.isabs(p) else os.path.join(sys.argv[1],p)))"
    )
    command = "$(python3 -c " + shlex.quote(resolver) + " \"$" + repo_var + "\" " + shlex.quote(value)
    if hash_var:
        command += " \"$" + hash_var + "\""
    return command + ")"


def _copy_from_build_worker(
    worker: dict[str, Any],
    source: str,
    destination: Path,
    *,
    timeout: float | None,
) -> None:
    if is_local_worker(worker):
        shutil.copy2(expand_path(source), destination)
        return
    remote_source = f"{worker_host(worker)}:{shlex.quote(source)}"
    result = run_process_command_with_retries(
        ["rsync", "-a", "-e", RSYNC_SSH, remote_source, str(destination)],
        timeout=timeout,
        retries=2,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"could not copy build artifact from {worker_host(worker)}")


def _cleanup_build_artifact_stage(worker: dict[str, Any], artifact: str, *, timeout: float | None) -> None:
    stage_dir = str(Path(artifact).parent)
    if not stage_dir.startswith("/tmp/forge-build-artifact."):
        return
    cleanup = "rm -rf -- " + shlex.quote(stage_dir)
    try:
        run_worker_shell(worker, cleanup, timeout=timeout, retries=1)
    except (OSError, subprocess.SubprocessError):
        pass


def _load_build_preset(name: str) -> dict[str, Any] | None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name):
        return None
    for directory in config_template_dirs():
        path = directory / f"build_{name}.json"
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"{path}: could not read build preset: {exc}") from exc
        if not isinstance(payload, dict) or payload.get("schema") != "forge.build.v1":
            raise SystemExit(f"{path}: unsupported build preset schema")
        return payload
    return None


def cmd_build(args: argparse.Namespace) -> int:
    preset = _load_build_preset(args.repo)
    if preset is not None:
        variant = "reference" if args.reference else "candidate"
        preset_links = preset.get("links", {})
        if not isinstance(preset_links, dict):
            raise SystemExit(f"build preset {args.repo!r}: links must be an object")
        link = args.link or preset_links.get(variant)
        build_command = args.build_command or preset.get("build_command")
        if isinstance(build_command, str):
            build_command = build_command.replace("{variant}", variant)
        repo = str(preset.get("repo") or "")
        artifact = args.artifact or preset.get("artifact")
        destination_pattern = args.destination or preset.get("destination")
        if not repo:
            raise SystemExit(f"build preset {args.repo!r}: repo is required")
    else:
        if args.reference or args.candidate:
            raise SystemExit("--reference and --candidate are available only with a named build preset")
        variant = ""
        repo = args.repo
        artifact = args.artifact
        destination_pattern = args.destination
        build_command = args.build_command
        link = args.link

    if not re.fullmatch(r"[0-9a-fA-F]{4,40}", args.commit):
        raise SystemExit("build commit must be a 4-40 character hexadecimal commit hash")
    if not build_command and not args.target:
        raise SystemExit("build requires --target when --build-command is not given")
    if not artifact:
        raise SystemExit("build requires --artifact")
    if not destination_pattern:
        raise SystemExit("build requires --destination")

    selected_payload = one_slot_per_host_payload(args.workers)
    workers = worker_records_from_payload(selected_payload, label="build workers")
    timeout = bounded_timeout(args.remote_timeout_seconds)

    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for worker in workers:
        try:
            platform_key = _build_worker_platform(worker, timeout=timeout)
        except (OSError, RuntimeError) as exc:
            print(f"build: {exc}", file=sys.stderr)
            return 1
        groups.setdefault(platform_key, []).append(worker)

    with tempfile.TemporaryDirectory(prefix="forge-build-") as temp_dir:
        for (os_name, arch), group in groups.items():
            builder = group[0]
            hosts = [worker_host(worker) for worker in group]
            label = f"{os_name}/{arch}"
            print(f"build: {label} on {worker_host(builder)}; distribute to {', '.join(hosts)}", flush=True)

            repo_expr = (
                "$(python3 -c "
                + shlex.quote("import os,sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))")
                + " " + shlex.quote(worker_path_parameter(repo)) + ")"
            )
            build_dir_expr = _build_remote_path("worktree", args.build_dir)
            artifact_expr = _build_remote_path("worktree", artifact, hash_var="short_hash")
            command_lines = [
                "set -e",
                f"repo={repo_expr}",
                "git -C \"$repo\" rev-parse --show-toplevel >/dev/null",
                f"git -C \"$repo\" fetch --quiet",
                f"commit=$(git -C \"$repo\" rev-parse --verify {shlex.quote(args.commit + '^{commit}')})",
                "short_hash=$(git -C \"$repo\" rev-parse --short \"$commit\")",
                "worktree=$(mktemp -d /tmp/forge-build.XXXXXX)",
                "rmdir \"$worktree\"",
                "stage_dir=; retain_artifact=0",
                "cleanup() { git -C \"$repo\" worktree remove --force \"$worktree\" >/dev/null 2>&1 || true; rm -rf \"$worktree\"; if [ \"$retain_artifact\" != 1 ] && [ -n \"$stage_dir\" ]; then rm -rf \"$stage_dir\"; fi; }",
                "trap cleanup EXIT",
                "git -C \"$repo\" worktree add --detach \"$worktree\" \"$commit\"",
                f"build_dir={build_dir_expr}",
                "mkdir -p \"$build_dir\"",
                "cd \"$worktree\"",
            ]
            if build_command:
                custom_command = build_command.replace("{hash}", '"$short_hash"')
                command_lines.append(custom_command)
            else:
                cmake_configure = ["cmake", "-S", '"$repo"', "-B", '"$build_dir"', shlex.quote("-DCMAKE_BUILD_TYPE=" + args.build_type)]
                cmake_configure.extend(shlex.quote(value) for value in args.cmake_arg)
                command_lines.append(" ".join(cmake_configure))
                cmake_build = ["cmake", "--build", '"$build_dir"', "--target", shlex.quote(args.target), "--config", shlex.quote(args.build_type), "--parallel"]
                if args.build_arg:
                    cmake_build.extend(["--", *(shlex.quote(value) for value in args.build_arg)])
                command_lines.append(" ".join(cmake_build))
            command_lines.extend([
                f"artifact={artifact_expr}",
                "artifact=$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \"$artifact\")",
                "test -f \"$artifact\"",
                "stage_dir=$(mktemp -d /tmp/forge-build-artifact.XXXXXX)",
                "staged_artifact=\"$stage_dir/$(basename \"$artifact\")\"",
                "cp \"$artifact\" \"$staged_artifact\"",
                "chmod +x \"$staged_artifact\"",
                "retain_artifact=1",
                "printf '\\nFORGE_BUILD_RESULT\\t%s\\t%s\\n' \"$short_hash\" \"$staged_artifact\"",
            ])
            result = run_worker_shell(builder, "\n".join(command_lines), timeout=timeout, retries=0)
            if result.returncode != 0:
                print(f"{worker_host(builder)}: build failed", file=sys.stderr)
                if result.stdout:
                    print(result.stdout, end="", file=sys.stderr)
                if result.stderr:
                    print(result.stderr, end="", file=sys.stderr)
                return 1
            if result.stdout:
                print(result.stdout, end="", flush=True)

            marker = next(
                (line for line in reversed(result.stdout.splitlines()) if line.startswith("FORGE_BUILD_RESULT\t")),
                None,
            )
            if marker is None:
                print(f"{worker_host(builder)}: build did not report its artifact", file=sys.stderr)
                return 1
            fields = marker.split("\t", 2)
            if len(fields) != 3 or not re.fullmatch(r"[0-9a-fA-F]{4,40}", fields[1]):
                print(f"{worker_host(builder)}: invalid build result: {marker!r}", file=sys.stderr)
                return 1
            short_hash, remote_artifact = fields[1], fields[2]
            local_artifact = Path(temp_dir) / f"{short_hash}-{os_name}-{arch}-{Path(remote_artifact).name}"
            try:
                _copy_from_build_worker(builder, remote_artifact, local_artifact, timeout=timeout)
            except (OSError, RuntimeError) as exc:
                print(f"build: {exc}", file=sys.stderr)
                return 1
            finally:
                _cleanup_build_artifact_stage(builder, remote_artifact, timeout=timeout)

            destination = destination_pattern.replace("{hash}", short_hash)
            worker_file = Path(temp_dir) / f"workers-{os_name}-{arch}.json"
            worker_file.write_text(json.dumps({"schema": "forge.workers.v1", "workers": group}))
            sync_args = argparse.Namespace(
                source=str(local_artifact),
                destination=destination,
                workers=str(worker_file),
                jobs=args.jobs,
                remote_timeout_seconds=args.remote_timeout_seconds,
            )
            if cmd_sync(sync_args) != 0:
                return 1

            if link:
                link_path = str(link).replace("{hash}", short_hash)
                remote_resolve = (
                    "python3 -c "
                    + shlex.quote("import os,sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))")
                )
                link_command = (
                    f"destination=$({remote_resolve} {shlex.quote(destination)}); "
                    f"link=$({remote_resolve} {shlex.quote(link_path)}); "
                    "chmod +x \"$destination\"; "
                    "mkdir -p \"$(dirname \"$link\")\"; ln -sfn \"$destination\" \"$link\""
                )
            else:
                remote_resolve = (
                    "python3 -c "
                    + shlex.quote("import os,sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))")
                )
                link_command = f"destination=$({remote_resolve} {shlex.quote(destination)}); chmod +x \"$destination\""
            link_results = [run_shell_host(worker, link_command, timeout=timeout, tracker=ProcessTracker()) for worker in group]
            failed = [worker_host(worker) for worker, link_result in zip(group, link_results) if link_result.returncode != 0]
            if failed:
                print(f"build: could not finalize artifact on {', '.join(failed)}", file=sys.stderr)
                return 1
            print(f"build: {label} artifact {short_hash} distributed", flush=True)
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
