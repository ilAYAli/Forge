from __future__ import annotations

import shlex
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


def template_references(value: Any, name: str) -> bool:
    needle = "{" + name
    if isinstance(value, str):
        return needle in value
    if isinstance(value, list):
        return any(template_references(item, name) for item in value)
    if isinstance(value, dict):
        return any(
            template_references(key, name) or template_references(item, name)
            for key, item in value.items()
        )
    return False


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
    explicit_path = Path(name).expanduser()
    if explicit_path.is_absolute() or os.sep in name:
        if not explicit_path.is_file():
            raise SystemExit(f"template not found: {name}")
        return explicit_path
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


def template_arg_names(name: str, spec: dict[str, Any], *, template_name: str) -> list[str]:
    names = [template_arg_name(name)]
    aliases = spec.get("aliases", [])
    if isinstance(aliases, str):
        aliases = [aliases]
    if not isinstance(aliases, list):
        raise SystemExit(f"{template_name}: parameter {name} aliases must be a list")
    for alias in aliases:
        alias_name = str(alias)
        names.append(alias_name if alias_name.startswith("--") else template_arg_name(alias_name))
    return names


def repeatable_arg_shell_values(template: dict[str, Any], values: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    params = template.get("parameters", {})
    if not isinstance(params, dict):
        return result
    for name, spec in params.items():
        if not isinstance(spec, dict) or not bool(spec.get("repeatable", False)):
            continue
        key = str(name)
        raw_items = values.get(key) or []
        if isinstance(raw_items, str):
            raw_items = [raw_items]
        flag = str(spec.get("arg") or template_arg_name(key))
        if bool(spec.get("quote", True)):
            result[f"{key}_args"] = " ".join(
                shlex.join([flag, str(item)])
                for item in raw_items
                if str(item)
            )
        else:
            result[f"{key}_args"] = " ".join(
                f"{flag} {item}"
                for item in raw_items
                if str(item)
            )
    return result


SPRT_TEMPLATE_CONTROL_ARGS = {"games", "shards"}


def sprt_shell_word(value: str) -> str:
    if "~" in value:
        return value
    return shlex.quote(value)


def sprt_shell_args(template: dict[str, Any], values: dict[str, Any]) -> str:
    params = template.get("parameters", {})
    if not isinstance(params, dict):
        return ""
    parts: list[str] = []
    for name, spec in params.items():
        key = str(name)
        if key in SPRT_TEMPLATE_CONTROL_ARGS:
            continue
        if not isinstance(spec, dict) or key not in values:
            continue
        flag = str(spec.get("arg") or template_arg_name(key))
        value = values[key]
        if bool(spec.get("repeatable", False)):
            items = value if isinstance(value, list) else [value]
            for item in items:
                if str(item):
                    parts.extend([flag, str(item)])
        elif str(spec.get("type") or "str") == "bool":
            if bool(value):
                parts.append(flag)
        else:
            parts.extend([flag, str(value)])
    extra_args = values.get("sprt_extra_args") or []
    if isinstance(extra_args, list):
        parts.extend(str(item) for item in extra_args)
    return " ".join(sprt_shell_word(part) for part in parts)


def worker_path_parameter(value: str, *, directory_executable: str = "") -> str:
    if value and not value.startswith("~") and not Path(value).is_absolute():
        cwd = Path(os.environ.get("PWD") or str(Path.cwd()))
        value = os.path.normpath(str(cwd / value))
    if directory_executable:
        local = Path(os.path.expandvars(value)).expanduser()
        if local.is_dir():
            value = str(local / directory_executable)
    home = str(Path.home())
    if value == home:
        return "~"
    prefix = home + os.sep
    if value.startswith(prefix):
        return "~/" + value[len(prefix):]
    return value


def parse_template_parameters_with_unknown(
    template_name: str,
    template: dict[str, Any],
    raw_args: list[str],
    *,
    allow_unknown: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    parser = argparse.ArgumentParser(
        prog=f"crucible run {template_name}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
    )
    params = template.get("parameters", {})
    if not isinstance(params, dict):
        raise SystemExit(f"{template_name}: parameters must be an object")
    for name, spec in params.items():
        if not isinstance(spec, dict):
            raise SystemExit(f"{template_name}: parameters.{name} must be an object")
        value_type = str(spec.get("type") or "str")
        repeatable = bool(spec.get("repeatable", False))
        kwargs: dict[str, Any] = {
            "dest": name,
            "required": bool(spec.get("required", False)) and "default" not in spec,
            "help": str(spec.get("help") or ""),
        }
        if repeatable:
            default = spec.get("default", [])
            if not isinstance(default, list):
                raise SystemExit(f"{template_name}: repeatable parameter {name} default must be a list")
            kwargs["action"] = "append"
            kwargs["default"] = list(default)
        elif "default" in spec:
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
        parser.add_argument(*template_arg_names(str(name), spec, template_name=template_name), **kwargs)
    if allow_unknown:
        namespace, unknown = parser.parse_known_args(raw_args)
    else:
        namespace = parser.parse_args(raw_args)
        unknown = []
    values = {key: value for key, value in vars(namespace).items() if value is not None}
    for name, spec in params.items():
        if not isinstance(spec, dict) or not bool(spec.get("worker_path", False)):
            continue
        value = values.get(str(name))
        directory_executable = str(spec.get("directory_executable") or "")
        if isinstance(value, str):
            values[str(name)] = worker_path_parameter(
                value,
                directory_executable=directory_executable,
            )
        elif isinstance(value, list):
            values[str(name)] = [
                worker_path_parameter(item, directory_executable=directory_executable)
                if isinstance(item, str) else item
                for item in value
            ]
    return values, unknown


def parse_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    values, _unknown = parse_template_parameters_with_unknown(template_name, template, raw_args)
    return values


def parse_sprt_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    values, unknown = parse_template_parameters_with_unknown(
        template_name,
        template,
        raw_args,
        allow_unknown=True,
    )
    if unknown:
        values["sprt_extra_args"] = unknown
    return values


def template_parameter_supplied(template: dict[str, Any], raw_args: list[str], parameter_names: set[str]) -> bool:
    params = template.get("parameters", {})
    names = {template_arg_name(name) for name in parameter_names}
    if isinstance(params, dict):
        for key in parameter_names:
            spec = params.get(key)
            if isinstance(spec, dict):
                names.update(template_arg_names(key, spec, template_name="sprt"))
    for arg in raw_args:
        if arg.split("=", 1)[0] in names:
            return True
    return False


def parse_run_options(raw_args: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
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
    parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--merge")
    parser.add_argument("--merge-force", action="store_true")
    parser.add_argument("--newline-between", action="store_true")
    parser.add_argument("--notify-command")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--print-manifest", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    passthrough_args: list[str] = []
    if "--" in raw_args:
        separator = raw_args.index("--")
        passthrough_args = raw_args[separator + 1:]
        raw_args = raw_args[:separator]
    run_options, template_args = parser.parse_known_args(raw_args)
    return run_options, [*template_args, *passthrough_args]


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


def split_total_by_count(total: int, count: int, *, unit: str | None) -> list[int]:
    if count <= 0:
        raise SystemExit("task.split.count must be positive")
    count = min(count, total)
    if unit == "games" and total % 2 == 0 and count * 2 <= total:
        base = (total // count) // 2 * 2
        shards = [base for _ in range(count)]
        remaining = total - base * count
        index = 0
        while remaining > 0:
            shards[index] += 2
            remaining -= 2
            index = (index + 1) % count
        return shards
    base = total // count
    extra = total % count
    return [base + (1 if index < extra else 0) for index in range(count)]


def resolve_split_count(split: dict[str, Any], values: dict[str, Any]) -> tuple[int, list[int | None], str | None]:
    unit = str(split.get("unit") or "")
    if "count" in split:
        count = as_int(template_value(split["count"], values), label="task.split.count")
        if "total" in split:
            total = as_int(template_value(split["total"], values), label="task.split.total")
            shards = split_total_by_count(total, count, unit=unit or None)
            return len(shards), shards, unit or None
        return count, [None] * count, None
    shard_key = "shard" if "shard" in split else "chunk" if "chunk" in split else ""
    if "total" not in split or not shard_key:
        raise SystemExit("task.split requires count or total/shard")
    total = as_int(template_value(split["total"], values), label="task.split.total")
    shard = as_int(template_value(split[shard_key], values), label=f"task.split.{shard_key}")
    count = (total + shard - 1) // shard
    shards = [min(shard, total - index * shard) for index in range(count)]
    return count, shards, unit or None


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
                values.setdefault(f"{key}_commit", commit)
                values.setdefault(f"{key}_hash", commit)
    return values


def git_checkout_template_parameters(template: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    result = dict(params)
    specs = template.get("parameters", {})
    if not isinstance(specs, dict):
        return result
    for name, spec in specs.items():
        key = str(name)
        if not isinstance(spec, dict):
            continue
        needs_checkout = bool(spec.get("git_checkout", False)) or template_references(template, f"{key}_commit")
        if not needs_checkout:
            continue
        value = result.get(key)
        if not isinstance(value, str):
            continue
        checkout = expand_path(value)
        if not checkout.is_dir() or not (checkout / ".git").exists():
            continue
        commit = git_output_or_empty(checkout, "rev-parse", "HEAD")
        if not commit:
            continue
        short = commit[:7]
        output_template = str(spec.get("checkout_output") or "~/assets/engines/{repo}_{commit}")
        output_values = {
            "repo": re.sub(r"[^A-Za-z0-9._-]+", "-", checkout.name).strip("-") or key,
            "commit": short,
            "full_commit": commit,
            "source": value,
        }
        result[f"{key}_source"] = value
        result[f"{key}_commit"] = commit
        result[f"{key}_hash"] = commit
        result[key] = safe_format(output_template, output_values)
    return result


def expand_template(template_name: str, template_path_: Path, template: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    params = git_checkout_template_parameters(template, params)
    values = run_name_values(template_name, params)
    values.update(repeatable_arg_shell_values(template, values))
    if template_name == "sprt":
        values["sprt_args"] = sprt_shell_args(template, values)
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
    task_count, shards, split_unit = resolve_split_count(split, values)
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
        shard = shards[index] if shards[index] is not None else ""
        task_values.update({
            "task_index": index,
            "task_number": index + 1,
            "task_count": task_count,
            "task_name": task_name,
            "shard": shard,
            "chunk": shard,
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
            if shards[index] is not None and "total" not in progress:
                progress["total"] = shards[index]
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


def sprt_task_thread_cost(params: dict[str, Any]) -> int:
    concurrency = as_int(params.get("concurrency", 1), label="sprt.concurrency")
    threads = as_int(params.get("threads", 1), label="sprt.threads")
    return 2 * concurrency * threads


def sprt_worker_budget(item: dict[str, Any], *, default_budget: int) -> int:
    for key in ("thread_budget", "threads", "slots"):
        if key in item:
            try:
                return int(item[key])
            except (TypeError, ValueError) as exc:
                label = str(item.get("host") or "worker")
                raise SystemExit(f"{label}: SPRT thread budget must be a non-negative integer") from exc
    return default_budget


def sprt_slots_from_thread_budget(budget: int, task_cost: int, *, label: str) -> int:
    if budget < 0:
        raise SystemExit(f"{label}: SPRT thread budget must be a non-negative integer")
    if budget == 0:
        return 0
    slots = budget // task_cost
    if slots < 1:
        raise SystemExit(
            f"{label}: SPRT thread budget {budget} is below {task_cost} threads per task; "
            "use 0 to disable this worker"
        )
    return slots


def sprt_inline_worker_records(workers_arg: str, *, task_cost: int) -> list[dict[str, Any]]:
    from .workers import (
        DEFAULT_INLINE_WORKER_SLOTS,
        default_inline_worker_records,
        parse_inline_worker_spec,
    )

    entries = [entry.strip() for entry in workers_arg.split(",")]
    if not entries or any(not entry for entry in entries):
        raise SystemExit("inline workers must be a comma-separated host list")

    defaults = default_inline_worker_records()
    records: list[dict[str, Any]] = []
    for entry in entries:
        host, profile, budget = parse_inline_worker_spec(entry)
        base = dict(defaults.get(host, {}))
        record = base if base else {
            "host": host,
            "profile": host,
            "local": host in ("localhost", "127.0.0.1"),
        }
        record["host"] = host
        if profile is not None:
            record["profile"] = profile
        elif not record.get("profile"):
            record["profile"] = host
        if "local" not in record:
            record["local"] = host in ("localhost", "127.0.0.1")
        if budget is None:
            budget = int(record.get("slots", DEFAULT_INLINE_WORKER_SLOTS if not base else task_cost))
        record.pop("slots", None)
        record["thread_budget"] = budget
        records.append(record)
    return records


def sprt_file_worker_records(workers_arg: str, *, task_cost: int) -> list[dict[str, Any]]:
    from .workers import parse_worker_record, worker_arg_looks_like_path

    path = expand_path(workers_arg)
    if not path.exists():
        if worker_arg_looks_like_path(workers_arg):
            raise SystemExit(f"workers file not found: {workers_arg}")
        return sprt_inline_worker_records(workers_arg, task_cost=task_cost)

    payload = read_json(path)
    if payload.get("schema") not in (None, "crucible.workers.v1"):
        raise SystemExit(f"{workers_arg}: unsupported schema {payload.get('schema')!r}")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list):
        raise SystemExit(f"{workers_arg}: workers must be a list")

    records: list[dict[str, Any]] = []
    for raw in raw_workers:
        raw_budget = task_cost
        if isinstance(raw, dict):
            raw_budget = sprt_worker_budget(raw, default_budget=task_cost)
        worker = parse_worker_record(raw)
        if not worker:
            continue
        worker.pop("slots", None)
        worker["thread_budget"] = raw_budget
        records.append(worker)
    return records


def sprt_worker_thread_budget_payload(workers_arg: str, params: dict[str, Any]) -> dict[str, Any]:
    task_cost = sprt_task_thread_cost(params)
    records = sprt_file_worker_records(workers_arg, task_cost=task_cost)

    converted: list[dict[str, Any] | str] = []
    for worker in records:
        host = str(worker.get("host") or "").strip()
        if not host:
            raise SystemExit("SPRT worker host must not be empty")
        budget = sprt_worker_budget(worker, default_budget=task_cost)
        slots = sprt_slots_from_thread_budget(budget, task_cost, label=host)
        if slots == 0:
            continue
        converted_worker = dict(worker)
        converted_worker.pop("threads", None)
        converted_worker.pop("thread_budget", None)
        converted_worker["slots"] = slots
        converted.append(converted_worker)

    if not converted:
        raise SystemExit("SPRT workers resolve to zero task slots")
    return {
        "schema": "crucible.workers.v1",
        "workers": converted,
        "sprt_thread_budget": {
            "threads_per_task": task_cost,
            "note": "SPRT template converts host thread budgets to worker slots.",
        },
    }


def sprt_worker_slot_count(payload: dict[str, Any]) -> int:
    total = 0
    workers = payload.get("workers")
    if not isinstance(workers, list):
        return 0
    for worker in workers:
        if isinstance(worker, dict):
            try:
                total += max(0, int(worker.get("slots", 1)))
            except (TypeError, ValueError):
                continue
        else:
            total += 1
    return total


def auto_sprt_shards(params: dict[str, Any], worker_payload: dict[str, Any]) -> int:
    games = as_int(params.get("games"), label="sprt.games")
    slots = sprt_worker_slot_count(worker_payload)
    if slots <= 0:
        return 1
    return max(1, min(games, slots * 4))


def cmd_template_run(args: argparse.Namespace) -> int:
    run_options, template_args = parse_run_options(list(args.template_args))
    config = load_config()
    path = template_path(args.template)
    template = load_template(path)
    template_name = template_display_name(path, template)
    workers_arg: str | None = None
    sprt_workers_payload: dict[str, Any] | None = None
    if template_name == "sprt":
        params = parse_sprt_template_parameters(template_name, template, template_args)
        shards_supplied = template_parameter_supplied(template, template_args, {"shards"})
        needs_workers = (
            not (run_options.print_manifest or run_options.dry_run)
            or not shards_supplied
        )
        if needs_workers:
            workers_arg = resolved_workers_arg(run_options, config)
            sprt_workers_payload = sprt_worker_thread_budget_payload(workers_arg, params)
        if not shards_supplied:
            params = dict(params)
            params["shards"] = auto_sprt_shards(params, sprt_workers_payload or {})
    else:
        params = parse_template_parameters(template_name, template, template_args)
    manifest = expand_template(template_name, path, template, params)
    if run_options.print_manifest or run_options.dry_run:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    workers_arg = workers_arg or resolved_workers_arg(run_options, config)
    coordinator_host = run_options.coordinator_host or default_coordinator_host(config)
    notify_command = run_options.notify_command
    with tempfile.TemporaryDirectory(prefix="crucible-template-") as tmp:
        manifest_path = Path(tmp) / f"{manifest['name']}.manifest.json"
        write_json(manifest_path, manifest)
        if template_name == "sprt":
            workers_path = Path(tmp) / "workers.json"
            write_json(workers_path, sprt_workers_payload or sprt_worker_thread_budget_payload(workers_arg, params))
            workers_arg = str(workers_path)
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
            coordinator_host=coordinator_host,
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
