from __future__ import annotations

import tempfile

from .config import *
from .manifest import *


TEMPLATE_SCHEMA = "crucible.template.v1"


class MissingFormat:
    def __init__(self, key: str) -> None:
        self.key = key

    def __format__(self, spec: str) -> str:
        return "{" + self.key + (":" + spec if spec else "") + "}"


class SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> MissingFormat:
        return MissingFormat(key)


def safe_format(value: Any, values: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(SafeFormatDict(values))
    if isinstance(value, list):
        return [safe_format(item, values) for item in value]
    if isinstance(value, dict):
        return {str(key): safe_format(item, values) for key, item in value.items()}
    return value


def template_files() -> list[Path]:
    files: list[Path] = []
    seen: set[str] = set()
    for directory in config_template_dirs():
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.template.json")):
            name = path.name.removesuffix(".template.json")
            if name in seen:
                continue
            seen.add(name)
            files.append(path)
    return files


def template_path(name: str) -> Path:
    if not RUN_NAME_RE.fullmatch(name):
        raise SystemExit(f"{name!r}: invalid template name")
    for path in template_files():
        if path.name == f"{name}.template.json":
            return path
    searched = ", ".join(str(path) for path in config_template_dirs())
    raise SystemExit(f"template not found: {name} (searched: {searched})")


def load_template(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    if payload.get("schema") != TEMPLATE_SCHEMA:
        raise SystemExit(f"{path}: unsupported template schema {payload.get('schema')!r}")
    return payload


def template_display_name(path: Path, template: dict[str, Any] | None = None) -> str:
    template = template if template is not None else load_template(path)
    return str(template.get("name") or path.name.removesuffix(".template.json"))


def cmd_template(args: argparse.Namespace) -> int:
    if args.action == "list":
        for path in template_files():
            template = load_template(path)
            desc = str(template.get("description") or "").strip()
            suffix = f" - {desc}" if desc else ""
            print(f"{template_display_name(path, template)}{suffix}")
        return 0
    if args.action == "show":
        path = template_path(args.name)
        print(json.dumps(load_template(path), indent=2, sort_keys=True))
        return 0
    raise SystemExit(f"unknown template action {args.action!r}")


def template_arg_name(name: str) -> str:
    return "--" + name.replace("_", "-")


def worker_path_parameter(value: str) -> str:
    home = str(Path.home())
    if value == home:
        return "~"
    prefix = home + os.sep
    if value.startswith(prefix):
        return "~/" + value[len(prefix):]
    return value


def parse_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    parser = argparse.ArgumentParser(
        prog=f"crucible run {template_name}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    params = template.get("parameters", {})
    if not isinstance(params, dict):
        raise SystemExit(f"{template_name}: parameters must be an object")
    for name, spec in params.items():
        if not isinstance(spec, dict):
            raise SystemExit(f"{template_name}: parameters.{name} must be an object")
        value_type = str(spec.get("type") or "str")
        kwargs: dict[str, Any] = {
            "dest": name,
            "required": bool(spec.get("required", False)) and "default" not in spec,
            "help": str(spec.get("help") or ""),
        }
        if "default" in spec:
            kwargs["default"] = spec["default"]
        if value_type == "int":
            kwargs["type"] = int
        elif value_type == "float":
            kwargs["type"] = float
        elif value_type == "bool":
            kwargs["action"] = argparse.BooleanOptionalAction
            kwargs.setdefault("default", False)
        elif value_type != "str":
            raise SystemExit(f"{template_name}: unsupported parameter type {value_type!r}")
        parser.add_argument(template_arg_name(str(name)), **kwargs)
    values = {key: value for key, value in vars(parser.parse_args(raw_args)).items() if value is not None}
    for name, spec in params.items():
        if not isinstance(spec, dict) or not bool(spec.get("worker_path", False)):
            continue
        value = values.get(str(name))
        if isinstance(value, str):
            values[str(name)] = worker_path_parameter(value)
    return values


def parse_run_options(raw_args: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--workers")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--run")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--task-attempts", type=int, default=2)
    parser.add_argument("--coordinator-host")
    parser.add_argument("--remote-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--lease-seconds", type=int, default=600)
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--timeout-seconds", type=float, default=0.0)
    parser.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--merge")
    parser.add_argument("--merge-force", action="store_true")
    parser.add_argument("--newline-between", action="store_true")
    parser.add_argument("--notify-command")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--print-manifest", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_known_args(raw_args)


def as_int(value: Any, *, label: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{label} must be an integer") from exc
    if result <= 0:
        raise SystemExit(f"{label} must be positive")
    return result


def template_value(value: Any, values: dict[str, Any]) -> Any:
    if isinstance(value, str) and value in values:
        return values[value]
    return safe_format(value, values)


def resolve_split_count(split: dict[str, Any], values: dict[str, Any]) -> tuple[int, list[int | None], str | None]:
    if "count" in split:
        count = as_int(template_value(split["count"], values), label="task.split.count")
        return count, [None] * count, None
    if "total" not in split or "chunk" not in split:
        raise SystemExit("task.split requires count or total/chunk")
    total = as_int(template_value(split["total"], values), label="task.split.total")
    chunk = as_int(template_value(split["chunk"], values), label="task.split.chunk")
    count = (total + chunk - 1) // chunk
    chunks = [min(chunk, total - index * chunk) for index in range(count)]
    unit = str(split.get("unit") or "")
    return count, chunks, unit or None


def command_string(command: Any, values: dict[str, Any]) -> str:
    command = safe_format(command, values)
    if isinstance(command, list):
        return shlex.join(str(part) for part in command)
    if isinstance(command, str):
        return command
    raise SystemExit("task.command must be a string or list")


def run_name_values(template_name: str, params: dict[str, Any]) -> dict[str, Any]:
    values = dict(params)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    values.update({
        "template": template_name,
        "timestamp": timestamp,
        "date": time.strftime("%Y%m%d"),
    })
    for key, value in list(params.items()):
        if isinstance(value, str):
            stem = Path(value).name or value
            values[f"{key}_stem"] = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-") or key
            commit_match = re.fullmatch(r"[0-9A-Fa-f]{7,40}", stem)
            if commit_match is None:
                commit_match = re.search(r"[_-]([0-9A-Fa-f]{7,40})$", stem)
            if commit_match is not None:
                commit = commit_match.group(1 if commit_match.lastindex else 0).lower()
                values[f"{key}_commit"] = commit
                values[f"{key}_hash"] = commit
    return values


def expand_template(template_name: str, template_path_: Path, template: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    values = run_name_values(template_name, params)
    kind = str(template.get("kind") or template_name)
    name_template = str(template.get("run_name") or f"{template_name}-{{timestamp}}")
    run_name = validate_run_name(str(safe_format(name_template, values)))
    description = clean_manifest_text(safe_format(template.get("description"), values), label="description")
    work_dir = str(safe_format(template.get("work_dir") or ".", values))
    task_template = template.get("task")
    if not isinstance(task_template, dict):
        raise SystemExit(f"{template_name}: task must be an object")
    split = task_template.get("split", {"count": 1})
    if not isinstance(split, dict):
        raise SystemExit(f"{template_name}: task.split must be an object")
    task_count, chunks, split_unit = resolve_split_count(split, values)
    width = max(4, len(str(task_count - 1)))
    task_prefix = str(task_template.get("prefix") or kind or template_name)
    task_prefix = re.sub(r"[^A-Za-z0-9._-]+", "_", task_prefix).strip("._-") or "task"
    output_templates: list[str] = []
    if "outputs" in task_template:
        raw_outputs = task_template["outputs"]
        if not isinstance(raw_outputs, list):
            raise SystemExit("task.outputs must be a list")
        output_templates = [str(item) for item in raw_outputs]
    elif "output" in task_template:
        output_templates = [str(task_template["output"])]
    progress_template = task_template.get("progress") or template.get("progress")
    tasks: list[dict[str, Any]] = []
    for index in range(task_count):
        task_name = f"{task_prefix}_{index:0{width}d}"
        task_values = dict(values)
        task_values.update({
            "task_index": index,
            "task_number": index + 1,
            "task_count": task_count,
            "task_name": task_name,
            "chunk": chunks[index] if chunks[index] is not None else "",
            "output_dir": "{output_dir}",
            "run_dir": "{run_dir}",
        })
        task: dict[str, Any] = {
            "id": task_name,
            "index": index,
            "command": command_string(task_template.get("command"), task_values),
            "outputs": [
                str(safe_format(output_template, task_values))
                for output_template in output_templates
            ],
        }
        label = task_template.get("label")
        if label is not None:
            task["label"] = clean_manifest_text(str(safe_format(label, task_values)), label=f"{task_name}.label")
        if isinstance(progress_template, dict):
            progress = safe_format(progress_template, task_values)
            if not isinstance(progress, dict):
                raise SystemExit("task.progress must be an object")
            if split_unit and "unit" not in progress:
                progress["unit"] = split_unit
            if chunks[index] is not None and "total" not in progress:
                progress["total"] = chunks[index]
            task["progress"] = progress
        tasks.append(task)

    manifest = {
        "schema": SCHEMA,
        "name": run_name,
        "kind": kind,
        "created_at": now(),
        "work_dir": work_dir,
        "vars": {},
        "requirements": safe_format(template.get("requirements") or {}, values),
        "tasks": tasks,
        "template": {
            "name": template_name,
            "path": str(template_path_),
            "sha256": sha256_file(template_path_),
            "parameters": params,
        },
    }
    if description:
        manifest["description"] = description
    path_maps = template.get("path_maps")
    if path_maps is not None:
        manifest["path_maps"] = safe_format(path_maps, values)
    validate_manifest_payload(manifest, label=f"template {template_name}")
    return manifest


def resolved_workers_arg(run_options: argparse.Namespace, config: dict[str, Any]) -> str:
    workers = run_options.workers or default_workers_arg(config)
    if not workers:
        raise SystemExit(
            "crucible run: missing --workers and no default_workers configured; "
            "pass --workers HOST[:SLOTS],... or set default_workers in ~/.config/crucible/config.yml"
        )
    return workers


def cmd_template_run(args: argparse.Namespace) -> int:
    run_options, template_args = parse_run_options(list(args.template_args))
    config = load_config()
    path = template_path(args.template)
    template = load_template(path)
    params = parse_template_parameters(args.template, template, template_args)
    manifest = expand_template(args.template, path, template, params)
    if run_options.print_manifest or run_options.dry_run:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    workers_arg = resolved_workers_arg(run_options, config)
    notify_command = run_options.notify_command or default_notify_command(config)
    with tempfile.TemporaryDirectory(prefix="crucible-template-") as tmp:
        manifest_path = Path(tmp) / f"{manifest['name']}.manifest.json"
        write_json(manifest_path, manifest)
        from .workers import cmd_run_job

        start_args = argparse.Namespace(
            manifest=str(manifest_path),
            workers=workers_arg,
            jobs=run_options.jobs,
            run=run_options.run,
            resume=run_options.resume,
            replace=run_options.replace,
            retry_failed=run_options.retry_failed,
            task_attempts=run_options.task_attempts,
            coordinator_host=run_options.coordinator_host,
            remote_timeout_seconds=run_options.remote_timeout_seconds,
            wait=run_options.wait,
            lease_seconds=run_options.lease_seconds,
            interval_seconds=run_options.interval_seconds,
            timeout_seconds=run_options.timeout_seconds,
            fail_on_fail=run_options.fail_on_fail,
            verify=run_options.verify,
            merge=run_options.merge,
            merge_force=run_options.merge_force,
            newline_between=run_options.newline_between,
            notify_command=notify_command,
            no_color=run_options.no_color,
            verbose=run_options.verbose,
            command_label="run",
        )
        return cmd_run_job(start_args)
