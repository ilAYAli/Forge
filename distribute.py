#!/usr/bin/env python3
"""Generic deterministic shard runner for CPU/IO-heavy work."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


SCHEMA = "distribute.task.v1"


def expand_path(value: str | Path) -> Path:
    return Path(os.path.expandvars(str(value))).expanduser().resolve()


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: expected JSON object")
    return data


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def translate_string(value: str, path_map: dict[str, str]) -> str:
    out = value
    for src, dst in sorted(path_map.items(), key=lambda item: len(item[0]), reverse=True):
        out = out.replace(src, dst)
    return out


def translate_obj(value: Any, path_map: dict[str, str]) -> Any:
    if isinstance(value, str):
        return translate_string(value, path_map)
    if isinstance(value, list):
        return [translate_obj(item, path_map) for item in value]
    if isinstance(value, dict):
        return {key: translate_obj(item, path_map) for key, item in value.items()}
    return value


def apply_profile(manifest: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return manifest
    path_maps = manifest.get("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    selected = path_maps.get(profile)
    if selected is None:
        raise SystemExit(f"unknown profile {profile!r}")
    if not isinstance(selected, dict):
        raise SystemExit(f"path_maps.{profile} must be an object")
    return translate_obj(copy.deepcopy(manifest), {str(k): str(v) for k, v in selected.items()})


def parse_version(text: str) -> tuple[int, ...]:
    match = re.search(r"(\d+(?:\.\d+)+)", text)
    if not match:
        return ()
    return tuple(int(part) for part in match.group(1).split("."))


def require_min_version(label: str, actual: str, minimum: str) -> None:
    actual_v = parse_version(actual)
    min_v = parse_version(minimum)
    if not actual_v or actual_v < min_v:
        raise SystemExit(f"{label} {actual!r} is below required {minimum}")


def git_output(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), *args],
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()


def command_output(command: list[str]) -> str:
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = read_json(path)
    if manifest.get("schema") != SCHEMA:
        raise SystemExit(f"{path}: unsupported schema {manifest.get('schema')!r}")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        raise SystemExit(f"{path}: tasks must be a list")
    seen: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict):
            raise SystemExit(f"{path}: each task must be an object")
        task_id = str(task.get("id", ""))
        if not task_id:
            raise SystemExit(f"{path}: task is missing id")
        if task_id in seen:
            raise SystemExit(f"{path}: duplicate task id {task_id}")
        seen.add(task_id)
    return manifest


def manifest_base(manifest: dict[str, Any], key: str, fallback: str) -> Path:
    return expand_path(str(manifest.get(key) or fallback))


def state_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "state_dir", "state")


def log_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "log_dir", "logs")


def work_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "work_dir", ".")


def find_task(manifest: dict[str, Any], *, task_id: str | None, index: int | None) -> dict[str, Any]:
    tasks = manifest["tasks"]
    matches = []
    for task in tasks:
        if task_id is not None and task.get("id") == task_id:
            matches.append(task)
        elif index is not None and int(task.get("index", -1)) == index:
            matches.append(task)
    if not matches:
        target = task_id if task_id is not None else index
        raise SystemExit(f"no task matches {target!r}")
    if len(matches) > 1:
        raise SystemExit("task selector matched multiple tasks")
    return matches[0]


def check_requirements(manifest: dict[str, Any]) -> None:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")

    python_min = str(req.get("python_min", "")).strip()
    if python_min:
        actual = ".".join(map(str, sys.version_info[:3]))
        require_min_version("python", actual, python_min)

    git_min = str(req.get("git_min", "")).strip()
    if git_min:
        actual = command_output(["git", "--version"])
        require_min_version("git", actual, git_min)

    git_req = req.get("git")
    if isinstance(git_req, dict):
        repo = expand_path(str(git_req.get("repo") or manifest.get("work_dir") or "."))
        expected_commit = str(git_req.get("commit") or "").strip()
        if expected_commit:
            actual_commit = git_output(repo, "rev-parse", "HEAD")
            if actual_commit != expected_commit:
                raise SystemExit(
                    f"{repo}: git commit {actual_commit} != required {expected_commit}"
                )
        if bool(git_req.get("require_clean", False)):
            status = git_output(repo, "status", "--porcelain")
            if status:
                raise SystemExit(f"{repo}: git worktree is not clean")

    inputs = req.get("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for item in inputs:
        if not isinstance(item, dict):
            raise SystemExit("requirements.inputs entries must be objects")
        path = expand_path(str(item.get("path", "")))
        if not path.exists():
            raise SystemExit(f"required input missing: {path}")
        expected_sha = str(item.get("sha256") or "").strip()
        if expected_sha and sha256_file(path) != expected_sha:
            raise SystemExit(f"required input checksum mismatch: {path}")


def task_done_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.done.json"


def task_fail_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.fail.json"


def task_running_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return state_dir(manifest) / f"{task['id']}.running.json"


def task_log_path(manifest: dict[str, Any], task: dict[str, Any]) -> Path:
    return log_dir(manifest) / f"{task['id']}.log"


def task_outputs(task: dict[str, Any]) -> list[Path]:
    outputs = task.get("outputs", [])
    if not isinstance(outputs, list):
        raise SystemExit(f"{task['id']}: outputs must be a list")
    return [expand_path(str(path)) for path in outputs]


def format_command(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    vars_obj = manifest.get("vars", {})
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    outputs = task_outputs(task)
    values: dict[str, Any] = {
        "task_id": task["id"],
        "index": task.get("index", ""),
        "shards": len(manifest["tasks"]),
        "output": str(outputs[0]) if outputs else "",
    }
    values.update({str(k): v for k, v in vars_obj.items()})
    values.update({str(k): v for k, v in task.items() if k not in {"command"}})
    command = str(task.get("command") or "")
    if not command:
        raise SystemExit(f"{task['id']}: missing command")
    return command.format(**values)


def task_complete(manifest: dict[str, Any], task: dict[str, Any]) -> bool:
    if not task_done_path(manifest, task).exists():
        return False
    return all(path.exists() for path in task_outputs(task))


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
    command = format_command(manifest, task)
    log_path = task_log_path(manifest, task)
    start = time.monotonic()
    started_at = now()
    running = {
        "task": task["id"],
        "host": socket.gethostname(),
        "started_at": started_at,
        "command": command,
        "pid": os.getpid(),
    }
    write_json(task_running_path(manifest, task), running)

    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"{started_at} start {task['id']}: {command}\n")
        log.flush()
        rc = subprocess.call(
            ["bash", "-lc", command],
            cwd=work_dir(manifest),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
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
        "outputs": [file_record(path) for path in task_outputs(task) if path.exists()],
    }
    if rc == 0 and all(path.exists() for path in task_outputs(task)):
        write_json(task_done_path(manifest, task), payload)
    else:
        write_json(task_fail_path(manifest, task), payload)
    return rc


def cmd_plan(args: argparse.Namespace) -> int:
    out = expand_path(args.out)
    state = expand_path(args.state_dir) if args.state_dir else out.parent / "state"
    logs = expand_path(args.log_dir) if args.log_dir else out.parent / "logs"
    vars_obj: dict[str, str] = {}
    for item in args.var:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise SystemExit(f"invalid --var {item!r}; expected KEY=VALUE")
        vars_obj[key] = value

    path_maps: dict[str, dict[str, str]] = {}
    for item in args.path_map:
        profile, sep, mapping = item.partition(":")
        src, sep2, dst = mapping.partition("=")
        if not sep or not sep2 or not profile or not src:
            raise SystemExit(
                f"invalid --path-map {item!r}; expected PROFILE:/source=/dest"
            )
        path_maps.setdefault(profile, {})[src] = dst

    tasks = []
    width = max(4, len(str(args.shards - 1)))
    for index in range(args.shards):
        task_id = f"{args.task_prefix}_{index:0{width}d}"
        outputs = [
            template.format(index=index, shard=index, task_id=task_id, shards=args.shards, **vars_obj)
            for template in args.output_template
        ]
        tasks.append({
            "id": task_id,
            "index": index,
            "command": args.command_template,
            "outputs": outputs,
        })

    manifest = {
        "schema": SCHEMA,
        "name": args.name,
        "created_at": now(),
        "work_dir": str(expand_path(args.work_dir)),
        "state_dir": str(state),
        "log_dir": str(logs),
        "vars": vars_obj,
        "path_maps": path_maps,
        "requirements": {
            "python_min": args.python_min,
            "git_min": args.git_min,
            "git": {
                "repo": str(expand_path(args.git_repo or args.work_dir)),
                "commit": args.git_commit or git_output(expand_path(args.git_repo or args.work_dir), "rev-parse", "HEAD"),
                "require_clean": args.require_clean_git,
            },
            "inputs": [],
        },
        "tasks": tasks,
    }
    write_json(out, manifest)
    print(out)
    return 0


def cmd_add_input(args: argparse.Namespace) -> int:
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    inputs = req.setdefault("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for raw in args.path:
        input_path = expand_path(raw)
        if not input_path.exists():
            raise SystemExit(f"input does not exist: {input_path}")
        record = file_record(input_path)
        inputs[:] = [
            item for item in inputs
            if not isinstance(item, dict) or item.get("path") != str(input_path)
        ]
        inputs.append(record)
    write_json(path, manifest)
    print(path)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    task = find_task(manifest, task_id=args.task, index=args.index)
    return run_task(manifest, task, force=args.force)


def task_state(manifest: dict[str, Any], task: dict[str, Any]) -> str:
    if task_complete(manifest, task):
        return "done"
    if task_fail_path(manifest, task).exists():
        return "fail"
    if task_running_path(manifest, task).exists():
        return "running"
    return "pending"


def cmd_status(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    counts = {"done": 0, "fail": 0, "running": 0, "pending": 0}
    rows = []
    for task in manifest["tasks"]:
        state = task_state(manifest, task)
        counts[state] += 1
        rows.append((task["id"], state))
    print(
        f"tasks={len(manifest['tasks'])} done={counts['done']} "
        f"running={counts['running']} fail={counts['fail']} pending={counts['pending']}"
    )
    if args.verbose:
        for task_id, state in rows:
            print(f"{task_id}\t{state}")
    return 0 if counts["fail"] == 0 else 1


def verify_manifest(manifest: dict[str, Any]) -> None:
    check_requirements(manifest)
    failures = []
    for task in manifest["tasks"]:
        if not task_complete(manifest, task):
            failures.append(f"{task['id']}: {task_state(manifest, task)}")
    if failures:
        raise SystemExit("incomplete tasks:\n" + "\n".join(failures))


def cmd_verify(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    verify_manifest(manifest)
    print("ok")
    return 0


def append_file(src: Path, dst: Path, *, newline_between: bool) -> None:
    with src.open("rb") as in_handle, dst.open("ab") as out_handle:
        shutil.copyfileobj(in_handle, out_handle, length=1024 * 1024)
        if newline_between:
            out_handle.write(b"\n")


def cmd_merge(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
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
        "manifest": str(expand_path(args.manifest)),
        "tasks": merged,
        "output": file_record(output),
    })
    print(output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Create a numbered shard manifest.")
    plan.add_argument("--name", required=True)
    plan.add_argument("--shards", type=int, required=True)
    plan.add_argument("--work-dir", required=True)
    plan.add_argument("--out", required=True)
    plan.add_argument("--command-template", required=True)
    plan.add_argument("--output-template", action="append", required=True)
    plan.add_argument("--var", action="append", default=[])
    plan.add_argument(
        "--path-map",
        action="append",
        default=[],
        help="Host path mapping, PROFILE:/source=/dest. Repeatable.",
    )
    plan.add_argument("--task-prefix", default="task")
    plan.add_argument("--state-dir")
    plan.add_argument("--log-dir")
    plan.add_argument("--python-min", default="3.11")
    plan.add_argument("--git-min", default="2.38")
    plan.add_argument("--git-repo")
    plan.add_argument("--git-commit")
    plan.add_argument("--require-clean-git", action="store_true")
    plan.set_defaults(func=cmd_plan)

    add_input = sub.add_parser("add-input", help="Record input file checksums.")
    add_input.add_argument("--manifest", required=True)
    add_input.add_argument("--path", action="append", required=True)
    add_input.set_defaults(func=cmd_add_input)

    run = sub.add_parser("run", help="Run one task by id or index.")
    run.add_argument("--manifest", required=True)
    run.add_argument("--task")
    run.add_argument("--index", type=int)
    run.add_argument("--profile")
    run.add_argument("--force", action="store_true")
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="Show task status.")
    status.add_argument("--manifest", required=True)
    status.add_argument("-v", "--verbose", action="store_true")
    status.add_argument("--profile")
    status.set_defaults(func=cmd_status)

    verify = sub.add_parser("verify", help="Verify all tasks are complete.")
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--profile")
    verify.set_defaults(func=cmd_verify)

    merge = sub.add_parser("merge", help="Concatenate first output from each task.")
    merge.add_argument("--manifest", required=True)
    merge.add_argument("--output", required=True)
    merge.add_argument("--force", action="store_true")
    merge.add_argument("--newline-between", action="store_true")
    merge.add_argument("--profile")
    merge.set_defaults(func=cmd_merge)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "command", None) == "run" and args.task is None and args.index is None:
        parser.error("run requires --task or --index")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
