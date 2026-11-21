from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys

from .config import default_coordinator_host
from .core import (
    COORDINATOR_DISABLE_DISPATCH_ENV,
    COORDINATOR_DISPATCH_ENV,
    RSYNC_SSH,
    SSH_OPTS,
    SSH_TRANSIENT_RETRY_COUNT,
    expand_path,
    run_process_command_with_retries,
)
from .workers import current_host_names, normalize_host_name
from .templates import worker_path_parameter


LOCAL_WORKER_COMMANDS = {
    "claim",
    "heartbeat",
    "release",
    "run-task",
    "spsa-next",
    "spsa-submit",
    "spsa-worker",
    "sync-repos",
    "work",
}


def _remote_temp_dir(host: str) -> str:
    result = run_process_command_with_retries(
        ["ssh", *SSH_OPTS, host, "mktemp -d /tmp/forge-dispatch.XXXXXX"],
        timeout=15,
        retries=SSH_TRANSIENT_RETRY_COUNT,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "could not create coordinator staging directory")
    path = result.stdout.strip().splitlines()[-1:]
    if not path or not path[0].startswith("/tmp/forge-dispatch."):
        raise RuntimeError("coordinator returned an invalid staging directory")
    return path[0]


def _stage_local_path(host: str, remote_dir: str, value: str) -> str | None:
    source = expand_path(value)
    if not source.exists():
        return None
    result = run_process_command_with_retries(
        ["rsync", "-a", "-e", RSYNC_SSH, str(source), f"{host}:{remote_dir}/"],
        timeout=1800,
        retries=SSH_TRANSIENT_RETRY_COUNT,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"could not stage {source} on coordinator")
    return f"{remote_dir}/{source.name}"


def _replace_argument(argv: list[str], old: str, new: str) -> None:
    try:
        index = argv.index(old)
    except ValueError:
        raise RuntimeError(f"could not locate argument to stage: {old}") from None
    argv[index] = new


def _stage_command_inputs(args: argparse.Namespace, argv: list[str], host: str, remote_dir: str) -> None:
    command = str(getattr(args, "subcommand", ""))

    workers_value = getattr(args, "workers", None)
    if command == "deploy":
        workers_value = args.workers
    elif command == "update":
        workers_value = args.workers
    if isinstance(workers_value, str):
        staged_workers = _stage_local_path(host, remote_dir, workers_value)
        if staged_workers:
            _replace_argument(argv, workers_value, staged_workers)

    if command in {"start", "deploy"}:
        manifest = str(args.manifest)
        staged_manifest = _stage_local_path(host, remote_dir, manifest)
        if staged_manifest:
            _replace_argument(argv, manifest, staged_manifest)

    if command == "sync":
        source = str(args.source)
        destination = args.destination
        if destination is None:
            destination = worker_path_parameter(source)
        staged_source = _stage_local_path(host, remote_dir, source)
        if staged_source:
            _replace_argument(argv, source, staged_source)
            if args.destination is None:
                argv.insert(argv.index(staged_source) + 1, destination)


def dispatch_to_coordinator(args: argparse.Namespace, original_argv: list[str]) -> int | None:
    if (
        os.environ.get(COORDINATOR_DISPATCH_ENV) == "1"
        or os.environ.get(COORDINATOR_DISABLE_DISPATCH_ENV) == "1"
    ):
        return None
    command = str(getattr(args, "subcommand", ""))
    if command in LOCAL_WORKER_COMMANDS or (command == "doctor" and getattr(args, "role", "worker") == "worker"):
        return None
    host = default_coordinator_host()
    if not host or normalize_host_name(host) in current_host_names():
        return None

    remote_dirs: list[str] = []
    try:
        remote_dir = _remote_temp_dir(host)
        remote_dirs.append(remote_dir)
        remote_argv = list(original_argv)
        _stage_command_inputs(args, remote_argv, host, remote_dir)
        remote = shlex.join([
            "env",
            f"{COORDINATOR_DISPATCH_ENV}=1",
            "forge",
            *remote_argv,
        ])
        result = subprocess.run(["ssh", *SSH_OPTS, host, remote], check=False)
        return result.returncode
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"coordinator {host}: {exc}", file=sys.stderr)
        return 1
    finally:
        for remote_dir in remote_dirs:
            try:
                run_process_command_with_retries(
                    ["ssh", *SSH_OPTS, host, shlex.join(["rm", "-rf", "--", remote_dir])],
                    timeout=15,
                    retries=1,
                )
            except (OSError, subprocess.SubprocessError):
                pass
