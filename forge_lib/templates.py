from __future__ import annotations

import hashlib
import secrets
import shlex
import tempfile

from .config import *
from .manifest import *
from .labeling import attach_label_task_inputs, build_inventory
from .task_inputs import attach_pgn_task_inputs
from .source_artifacts import attach_source_artifacts
from .chunking import adaptive_autosized_shards
from .spsa import (
    attach_spsa_manifest_config,
    auto_spsa_shards,
    cap_spsa_worker_slots,
    prepare_spsa_template_parameters,
    spsa_worker_slots_payload,
)


TEMPLATE_SCHEMA = "forge.template.v1"
TEMPLATE_DEFAULTS_SCHEMA = "forge.template-defaults.v1"
SPRT_AUTO_TARGET_GAMES_PER_SHARD = 25


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


def template_defaults_files(name: str) -> list[Path]:
    """Return defaults overlays from lowest to highest precedence."""
    return [
        path
        for directory in reversed(config_template_dirs())
        if (path := directory / f"{name}.defaults.json").is_file()
    ]


def template_default_parameter_names(name: str) -> set[str]:
    result: set[str] = set()
    for overlay_path in template_defaults_files(name):
        overlay = read_json(overlay_path)
        defaults = overlay.get("parameters")
        if isinstance(defaults, dict):
            result.update(str(key).replace("-", "_") for key in defaults)
    return result


def template_structural_overrides(name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for overlay_path in template_defaults_files(name):
        overlay = read_json(overlay_path)
        for key, value in overlay.items():
            if key not in {"schema", "parameters"}:
                result[str(key)] = value
    return result


def merge_template_override(
    target: Any,
    override: Any,
    *,
    overlay_path: Path,
    field_path: str,
) -> Any:
    if isinstance(target, dict):
        if not isinstance(override, dict):
            raise SystemExit(f"{overlay_path}: {field_path} must be an object")
        unknown = sorted(set(override) - set(target))
        if unknown:
            qualified = ", ".join(f"{field_path}.{key}" for key in unknown)
            raise SystemExit(f"{overlay_path}: unknown template field(s): {qualified}")
        result = copy.deepcopy(target)
        for key, value in override.items():
            result[key] = merge_template_override(
                target[key],
                value,
                overlay_path=overlay_path,
                field_path=f"{field_path}.{key}",
            )
        return result
    if isinstance(target, bool):
        valid_type = isinstance(override, bool)
    elif isinstance(target, int):
        valid_type = isinstance(override, int) and not isinstance(override, bool)
    elif isinstance(target, float):
        valid_type = isinstance(override, (int, float)) and not isinstance(override, bool)
    else:
        valid_type = isinstance(override, type(target))
    if not valid_type:
        raise SystemExit(
            f"{overlay_path}: {field_path} must be {type(target).__name__}"
        )
    return copy.deepcopy(override)


def apply_template_defaults(
    template: dict[str, Any],
    *,
    name: str,
) -> dict[str, Any]:
    result = copy.deepcopy(template)
    parameters = result.get("parameters", {})
    if not isinstance(parameters, dict):
        raise SystemExit(f"{name}: template parameters must be an object")
    for overlay_path in template_defaults_files(name):
        overlay = read_json(overlay_path)
        if overlay.get("schema") != TEMPLATE_DEFAULTS_SCHEMA:
            raise SystemExit(
                f"{overlay_path}: unsupported defaults schema {overlay.get('schema')!r}"
            )
        immutable_fields = sorted(set(overlay) & {"name", "kind"})
        if immutable_fields:
            raise SystemExit(
                f"{overlay_path}: immutable template field(s): "
                f"{', '.join(immutable_fields)}"
            )
        unknown_fields = sorted(
            set(overlay) - {"schema", "parameters"} - set(result)
        )
        if unknown_fields:
            raise SystemExit(
                f"{overlay_path}: unknown defaults field(s): {', '.join(unknown_fields)}"
            )
        defaults = overlay.get("parameters", {})
        if not isinstance(defaults, dict):
            raise SystemExit(f"{overlay_path}: parameters must be an object")
        normalized_defaults: dict[str, Any] = {}
        original_names: dict[str, str] = {}
        for key, value in defaults.items():
            original = str(key)
            normalized = original.replace("-", "_")
            if normalized in normalized_defaults:
                raise SystemExit(
                    f"{overlay_path}: duplicate template parameter spellings: "
                    f"{original_names[normalized]}, {original}"
                )
            normalized_defaults[normalized] = value
            original_names[normalized] = original
        unknown = sorted(
            original_names[key] for key in normalized_defaults if key not in parameters
        )
        if unknown:
            raise SystemExit(
                f"{overlay_path}: unknown template parameter(s): {', '.join(unknown)}"
            )
        for parameter, value in normalized_defaults.items():
            spec = parameters[parameter]
            if not isinstance(spec, dict):
                raise SystemExit(f"{name}: parameter {parameter!r} must be an object")
            spec["default"] = value
            spec.pop("required", None)
        for field, value in overlay.items():
            if field in {"schema", "parameters"}:
                continue
            result[field] = merge_template_override(
                result[field],
                value,
                overlay_path=overlay_path,
                field_path=str(field),
            )
    return result


def load_template(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    if not schema_matches(payload.get("schema"), "template"):
        raise SystemExit(f"{path}: unsupported template schema {payload.get('schema')!r}")
    name = path.name.removesuffix(".template.json")
    return apply_template_defaults(payload, name=name)


def template_display_name(path: Path, template: dict[str, Any] | None = None) -> str:
    template = template if template is not None else load_template(path)
    return str(template.get("name") or path.name.removesuffix(".template.json"))

def template_parameter_placeholder(name: str, spec: dict[str, Any]) -> str:
    explicit = str(spec.get("metavar") or "").strip()
    if explicit:
        return explicit
    placeholders = {
        "book": "EPD_BOOK",
        "candidate": "EXECUTABLE",
        "candidate_net": "NET",
        "config": "CONFIG",
        "engine": "EXECUTABLE",
        "games": "GAMES",
        "input": "INPUT_DIR",
        "net": "NET",
        "output": "OUTPUT.bullet",
        "params": "PARAMS_FILE",
        "pgn_dir": "PGN_DIR",
        "reference": "EXECUTABLE",
        "reference_net": "NET",
        "settings": "SETTINGS_FILE",
        "sf_engine": "EXECUTABLE",
        "sf_net": "NET",
        "state": "STATE_FILE",
        "work_dir": "WORK_DIR",
    }
    if name in placeholders:
        return placeholders[name]
    parameter_type = str(spec.get("type") or "")
    if parameter_type == "int":
        return "N"
    if parameter_type == "float":
        return "VALUE"
    return name.upper()


def template_choice_summary(spec: dict[str, Any]) -> str:
    """Render a choices parameter as `a|b|c` plus how it is supplied.

    A repeatable stage list defaults to `[]`, which says nothing useful, so
    only scalar defaults are shown; the help line carries the rest.
    """
    summary = "|".join(str(choice) for choice in spec.get("choices", []))
    notes = []
    if bool(spec.get("repeatable")):
        notes.append("repeatable")
    default = spec.get("default")
    if default not in (None, "", [], {}):
        notes.append(f"default: {default}")
    return f"{summary} ({', '.join(notes)})" if notes else summary


def cmd_template(args: argparse.Namespace) -> int:
    if args.action == "list":
        paths = template_files()
        visible_index = 0
        for path in paths:
            template = load_template(path)
            if bool(template.get("hidden", False)):
                continue
            parameters = template.get("parameters", {})
            parameter_items = list(parameters.items()) if isinstance(parameters, dict) else []
            display_name = template_display_name(path, template)
            overlay_parameters = template_default_parameter_names(display_name)
            structural_overrides = template_structural_overrides(display_name)
            local_defaulted = [
                (str(name), spec)
                for name, spec in parameter_items
                if (
                    isinstance(spec, dict)
                    and str(name) in overlay_parameters
                )
            ]
            required = [
                (str(name), spec)
                for name, spec in parameter_items
                if isinstance(spec, dict) and bool(spec.get("required", False))
            ]
            constrained = [
                (str(name), spec)
                for name, spec in parameter_items
                if isinstance(spec, dict) and isinstance(spec.get("choices"), list) and spec["choices"]
            ]
            if visible_index:
                print()
            print(display_name)
            description = str(template.get("description") or "").strip()
            if description:
                print(f"  {description}")
            print("  local defaults:")
            if local_defaulted:
                for name, spec in local_defaulted:
                    print(f"    {template_arg_name(name)} {spec.get('default')}")
            else:
                print("    none")
            if structural_overrides:
                print("  local template overrides:")
                for name, value in structural_overrides.items():
                    print(f"    {name}: {json.dumps(value, sort_keys=True)}")
            print("  required now:")
            if required:
                for name, spec in required:
                    placeholder = template_parameter_placeholder(name, spec)
                    print(f"    {template_arg_name(name)} {placeholder}")
            else:
                print("    none")
            if constrained:
                print("  choices:")
                for name, spec in constrained:
                    print(f"    {template_arg_name(name)} {template_choice_summary(spec)}")
                    help_text = str(spec.get("help") or "").strip()
                    if help_text:
                        print(f"      {help_text}")
            visible_index += 1
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


SPRT_TEMPLATE_CONTROL_ARGS = {
    "book_offset",
    "book_seed",
    "candidate_net",
    "chunk_time_seconds",
    "games",
    "hash",
    "reference_net",
    "shards",
}


def sprt_shell_word(value: str) -> str:
    if "~" in value:
        return value
    return shlex.quote(value)


def shell_word(value: Any) -> str:
    text = str(value)
    if "~" in text:
        return text
    return shlex.quote(text)


def shell_value_format_values(values: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in values.items():
        if isinstance(value, (str, int, float, bool)):
            result[f"{key}_shell"] = shell_word(value)
    return result


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


def fastchess_uci_args(values: dict[str, Any], key: str = "uci") -> str:
    items = values.get(key) or []
    if isinstance(items, str):
        items = [items]
    result = []
    for item in items:
        text = str(item)
        if text:
            name, separator, value = text.partition("=")
            if separator and value == "~":
                result.append(f"option.{name}=\"$HOME\"")
            elif separator and value.startswith("~/"):
                home_value = value[2:].replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`")
                result.append(f"option.{name}=\"$HOME/{home_value}\"")
            else:
                result.append(shell_word("option." + text))
    return " ".join(result)


def fastchess_limit_args(values: dict[str, Any]) -> str:
    depth = values.get("depth")
    nodes = values.get("nodes")
    if depth not in (None, "", 0):
        return f"tc=inf depth={depth}"
    if nodes not in (None, "", 0):
        return f"tc=inf nodes={nodes}"
    return f"tc={values.get('tc')}"


def resolve_worker_path_symlink(path: Path) -> Path:
    if path.is_symlink():
        return path.resolve(strict=False)
    return path

def worker_path_parameter(value: str, *, directory_executable: str = "") -> str:
    if value and not value.startswith("~") and not Path(value).is_absolute():
        cwd = Path(os.environ.get("PWD") or str(Path.cwd()))
        value = os.path.normpath(str(cwd / value))
    local = Path(os.path.expandvars(value)).expanduser()
    if directory_executable:
        if local.is_dir():
            local = resolve_worker_path_symlink(local)
            local = local / directory_executable
    local = resolve_worker_path_symlink(local)
    value = str(local)
    home = str(Path.home())
    if value == home:
        return "~"
    prefix = home + os.sep
    if value.startswith(prefix):
        return "~/" + value[len(prefix):]
    return value


def parameter_uses_worker_path(spec: dict[str, Any], *, name: str) -> bool:
    context = spec.get("path_context")
    if context is not None and context not in {"worker", "shared_home"}:
        raise SystemExit(
            f"parameters.{name}.path_context must be 'worker' or 'shared_home', "
            f"got {context!r}"
        )
    return context in {"worker", "shared_home"}


def validate_parameter_path_context(
    value: str,
    *,
    context: str,
    template_name: str,
    parameter_name: str,
) -> None:
    if context != "shared_home":
        return
    if value == "~" or value.startswith("~/"):
        return
    raise SystemExit(
        f"{template_name}: {template_arg_name(parameter_name)} must resolve beneath "
        f"the orchestrator home directory so it can be represented as ~/...: {value}"
    )


def validate_parameter_executable(
    value: str,
    *,
    executable: bool,
    template_name: str,
    parameter_name: str,
) -> None:
    if executable and expand_path(value).is_dir():
        raise SystemExit(
            f"{template_name}: {template_arg_name(parameter_name)} must name an "
            f"executable file, not a directory: {value}"
        )


def raw_template_argument_present(
    raw_args: list[str],
    names: list[str],
) -> bool:
    return any(
        argument == name or argument.startswith(name + "=")
        for argument in raw_args
        for name in names
    )


def require_local_defaults_or_arguments(
    template_name: str,
    params: dict[str, Any],
    raw_args: list[str],
) -> None:
    if "-h" in raw_args or "--help" in raw_args:
        return
    missing = []
    for name, spec in params.items():
        if not isinstance(spec, dict):
            continue
        if not bool(spec.get("required", False)) or not bool(spec.get("local_default", False)):
            continue
        if "default" in spec:
            continue
        argument_names = template_arg_names(str(name), spec, template_name=template_name)
        if not raw_template_argument_present(raw_args, argument_names):
            missing.append(str(name))
    if not missing:
        return
    searched = ", ".join(
        str(directory / f"{template_name}.defaults.json")
        for directory in config_template_dirs()
    )
    arguments = ", ".join(template_arg_name(name) for name in missing)
    raise SystemExit(
        f"{template_name}: missing local defaults: {', '.join(missing)}\n"
        f"searched: {searched}\n"
        f"provide {arguments} explicitly or create the defaults overlay"
    )


def parse_template_parameters_with_unknown(
    template_name: str,
    template: dict[str, Any],
    raw_args: list[str],
    *,
    allow_unknown: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    parser = argparse.ArgumentParser(
        prog=f"forge run {template_name}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
    )
    params = template.get("parameters", {})
    if not isinstance(params, dict):
        raise SystemExit(f"{template_name}: parameters must be an object")
    require_local_defaults_or_arguments(template_name, params, raw_args)
    repeatable_defaults: dict[str, list[Any]] = {}
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
            kwargs["default"] = []
            repeatable_defaults[str(name)] = list(default)
        elif "default" in spec:
            kwargs["default"] = spec["default"]
        choices = spec.get("choices")
        if choices is not None:
            if not isinstance(choices, list) or not choices:
                raise SystemExit(f"{template_name}: parameter {name} choices must be a non-empty list")
            kwargs["choices"] = choices
        if value_type == "int":
            kwargs["type"] = int
        elif value_type == "float":
            kwargs["type"] = float
        elif value_type == "bool":
            kwargs["action"] = argparse.BooleanOptionalAction
            kwargs.setdefault("default", False)
        elif value_type not in {"str", "source"}:
            raise SystemExit(f"{template_name}: unsupported parameter type {value_type!r}")
        parser.add_argument(*template_arg_names(str(name), spec, template_name=template_name), **kwargs)
    if allow_unknown:
        namespace, unknown = parser.parse_known_args(raw_args)
    else:
        namespace = parser.parse_args(raw_args)
        unknown = []
    values = {key: value for key, value in vars(namespace).items() if value is not None}
    for name, default in repeatable_defaults.items():
        if not values.get(name):
            values[name] = list(default)
    for name, spec in params.items():
        if not isinstance(spec, dict) or not parameter_uses_worker_path(
            spec,
            name=str(name),
        ):
            continue
        value = values.get(str(name))
        directory_executable = str(spec.get("directory_executable") or "")
        if isinstance(value, str):
            values[str(name)] = worker_path_parameter(
                value,
                directory_executable=directory_executable,
            )
            validate_parameter_path_context(
                values[str(name)],
                context=str(spec.get("path_context") or ""),
                template_name=template_name,
                parameter_name=str(name),
            )
            validate_parameter_executable(
                values[str(name)],
                executable=bool(spec.get("executable", False)),
                template_name=template_name,
                parameter_name=str(name),
            )
        elif isinstance(value, list):
            values[str(name)] = [
                worker_path_parameter(item, directory_executable=directory_executable)
                if isinstance(item, str) else item
                for item in value
            ]
            for item in values[str(name)]:
                if isinstance(item, str):
                    validate_parameter_path_context(
                        item,
                        context=str(spec.get("path_context") or ""),
                        template_name=template_name,
                        parameter_name=str(name),
                    )
    normalize_file_paths(params, values)
    for name, spec in params.items():
        if not isinstance(spec, dict):
            continue
        required_suffix = spec.get("suffix")
        if required_suffix is None:
            continue
        if not isinstance(required_suffix, str) or not required_suffix:
            raise SystemExit(
                f"{template_name}: parameter {name} suffix must be a non-empty string"
            )
        value = values.get(str(name))
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if isinstance(candidate, str) and not candidate.endswith(required_suffix):
                raise SystemExit(
                    f"{template_name}: {template_arg_name(str(name))} must end with "
                    f"{required_suffix!r}: {candidate}"
                )
    return values, unknown


def parse_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    values, _unknown = parse_template_parameters_with_unknown(template_name, template, raw_args)
    return values


def parse_sprt_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    values = parse_template_parameters(template_name, template, raw_args)
    validate_sprt_uci_options(values)
    elo0 = float(values.get("elo0", 0.0))
    elo1 = float(values.get("elo1", 5.0))
    if elo1 <= elo0:
        raise SystemExit("sprt: --elo1 must be greater than --elo0")
    for key in ("alpha", "beta"):
        probability = float(values.get(key, 0.1))
        if not 0.0 < probability < 1.0:
            raise SystemExit(f"sprt: --{key} must be between 0 and 1")
    if int(values.get("book_offset", 0)) < 0:
        raise SystemExit("sprt: --book-offset must be non-negative")
    if "book_seed" not in values:
        values["book_seed"] = secrets.randbelow(2**31)
    return values


def parse_selfplay_template_parameters(template_name: str, template: dict[str, Any], raw_args: list[str]) -> dict[str, Any]:
    values = parse_template_parameters(template_name, template, raw_args)
    validate_template_uci_options(values.get("uci"), "--uci")
    games = as_int(values.get("games"), label="selfplay.games")
    if games % 2 != 0:
        values["games"] = games + 1
    return values


def validate_sprt_uci_options(values: dict[str, Any]) -> None:
    for key, flag in (("candidate_uci", "--candidate-uci"), ("reference_uci", "--reference-uci")):
        validate_template_uci_options(values.get(key), flag)
        items = values.get(key) or []
        if not isinstance(items, list):
            items = [items]
        for item in items:
            name, separator, _value = str(item).partition("=")
            if separator and name.lower() == "nnue_file":
                net_flag = "--candidate-net" if key == "candidate_uci" else "--reference-net"
                raise SystemExit(
                    f"{flag} {item}: nnue_file is reserved; use {net_flag}"
                )


def validate_template_uci_options(items: Any, flag: str) -> None:
    if items is None:
        return
    if not isinstance(items, list):
        items = [items]
    for item in items:
        text = str(item)
        if "=" in text and text.split("=", 1)[1] == "":
            raise SystemExit(f"{flag} {text}: empty UCI option values are rejected; omit the option instead")


def normalize_file_paths(parameters: dict[str, Any], values: dict[str, Any]) -> None:
    for key, spec in parameters.items():
        if not isinstance(spec, dict):
            continue
        file_options = {str(item).lower() for item in spec.get("file_options", [])}
        if not file_options:
            continue
        items = values.get(key)
        if items is None:
            continue
        if not isinstance(items, list):
            items = [items]
        normalized = []
        for item in items:
            name, separator, value = str(item).partition("=")
            if separator and value and name.lower() in file_options:
                value = worker_path_parameter(value)
            normalized.append(f"{name}={value}" if separator else name)
        values[key] = normalized


def materialize_template_inputs(template: dict[str, Any], manifest: dict[str, Any], params: dict[str, Any]) -> None:
    requirements = manifest.get("requirements")
    if not isinstance(requirements, dict):
        return
    raw_inputs = requirements.get("inputs")
    if not isinstance(raw_inputs, list):
        return
    inputs = [dict(item) for item in raw_inputs if isinstance(item, dict)]
    parameters = template.get("parameters")
    if not isinstance(parameters, dict):
        parameters = {}
    for key, spec in parameters.items():
        if not isinstance(spec, dict):
            continue
        file_options = {str(item).lower() for item in spec.get("file_options", [])}
        items = params.get(key, [])
        if not isinstance(items, list):
            items = [items]
        for item in items:
            name, separator, value = str(item).partition("=")
            if separator and name.lower() in file_options:
                inputs.append({"name": f"{key}.{name}", "path": value})
    materialized = []
    seen: set[str] = set()
    for item in inputs:
        source_path = str(item.get("source") or item.get("path") or "")
        worker_path = str(item.get("path") or source_path)
        local_path = expand_path(source_path)
        if item.get("tree") == "directory":
            if not local_path.is_dir():
                raise SystemExit(f"template input directory does not exist: {source_path}")
            record = {**item, "source": source_path, "path": worker_path}
        elif local_path.is_file():
            record = {
                **item,
                **file_record(local_path),
                "source": source_path,
                "path": worker_path,
            }
        else:
            raise SystemExit(f"template input does not exist: {source_path}")
        if source_path in seen:
            continue
        seen.add(source_path)
        materialized.append(record)
    requirements["inputs"] = materialized


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
    parser.add_argument("--extend")
    parser.add_argument("--comment")
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
    parser.add_argument("--task-complete-command")
    parser.add_argument("--task-complete-sync", action="store_true")
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


def apply_template_run_defaults(
    template_name: str, run_options: argparse.Namespace, template: dict[str, Any] | None = None
) -> argparse.Namespace:
    if template_name == "spsa" and getattr(run_options, "resume", False):
        run_options.retry_failed = True
    if template_name == "selfplay":
        run_options.retry_failed = True
        run_options.fail_on_fail = False
    elif template_name not in ("sprt", "spsa", "label") and isinstance(template, dict):
        split = template.get("task", {}).get("split", {})
        if isinstance(split, dict) and int(split.get("count", 1) if not isinstance(split.get("count"), str) else 2) != 1:
            run_options.retry_failed = True
            run_options.fail_on_fail = False
    return run_options


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
    if unit == "games" and total >= 2:
        # Games are played in color-swapped pairs, so every shard must hold an
        # even count: fastchess rejects the odd remainder as -rounds 0.
        pairs = total // 2
        count = min(count, pairs)
        base, extra = divmod(pairs, count)
        shards = [2 * (base + (1 if index < extra else 0)) for index in range(count)]
        if total % 2:
            shards[0] += 1
        return shards
    count = min(count, total)
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


def source_template_parameters(
    template: dict[str, Any],
    params: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    result = dict(params)
    specs = template.get("parameters", {})
    if not isinstance(specs, dict):
        return result
    for name, spec in specs.items():
        key = str(name)
        if not isinstance(spec, dict) or str(spec.get("type") or "") != "source":
            continue
        if all(
            isinstance(result.get(f"{key}_{suffix}"), str)
            and bool(result.get(f"{key}_{suffix}"))
            for suffix in ("artifact", "digest", "source")
        ):
            # Replanning an existing manifest already carries the immutable
            # resolved source. Do not reinterpret its worker path as an alias.
            continue
        value = result.get(key)
        if not isinstance(value, str) or not value.strip():
            raise SystemExit(f"{key}: source alias or directory is required")
        candidate = expand_path(value)
        alias = value
        if not candidate.is_dir():
            from .config import configured_source

            candidate = configured_source(value, config)
        else:
            alias = candidate.name
        include = spec.get("include", [])
        exclude = spec.get("exclude", [])
        if not isinstance(include, list) or not all(isinstance(item, str) for item in include):
            raise SystemExit(f"{key}: source include must be a list of patterns")
        if not isinstance(exclude, list) or not all(isinstance(item, str) for item in exclude):
            raise SystemExit(f"{key}: source exclude must be a list of patterns")
        from .source_artifacts import build_source_artifact

        artifact = build_source_artifact(
            candidate,
            name=alias,
            include=include,
            exclude=exclude,
        )
        output_template = str(
            spec.get("output") or "~/.cache/forge/sources/{alias}-{digest12}"
        )
        output_values = {
            "alias": re.sub(r"[^A-Za-z0-9._-]+", "-", alias).strip("-") or key,
            "digest": artifact["digest"],
            "digest12": artifact["digest"][:12],
            "source_id": artifact["source_id"],
            "source": str(candidate),
        }
        result[f"{key}_alias"] = alias
        result[f"{key}_source"] = str(candidate)
        result[f"{key}_artifact"] = artifact["path"]
        result[f"{key}_digest"] = artifact["digest"]
        result[f"{key}_source_id"] = artifact["source_id"]
        result[key] = safe_format(output_template, output_values)
    return result


def git_lineage_template_parameters(
    template: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Resolve legacy repository placeholders without packaging repository code."""
    result = dict(params)
    specs = template.get("parameters", {})
    if not isinstance(specs, dict):
        return result
    for name in specs:
        key = str(name)
        if not template_references(template, f"{key}_commit"):
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
        result[f"{key}_source"] = value
        result[f"{key}_commit"] = commit
        result[f"{key}_hash"] = commit
        result[key] = safe_format(
            "~/assets/engines/{repo}_{commit}",
            {
                "repo": re.sub(r"[^A-Za-z0-9._-]+", "-", checkout.name).strip("-") or key,
                "commit": commit[:7],
            },
        )
    return result


def derived_template_parameters(template: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    result = dict(params)
    preserve = {
        str(item)
        for item in result.pop("__forge_preserve_derived", [])
    } if isinstance(result.get("__forge_preserve_derived"), list) else set()
    specs = template.get("parameters")
    if not isinstance(specs, dict):
        return result
    for name, spec in specs.items():
        if not isinstance(spec, dict):
            continue
        derive = spec.get("derive")
        if not isinstance(derive, dict):
            continue
        if str(name) in preserve:
            continue
        derive_type = str(derive.get("type") or "")
        source_name = str(derive.get("from") or "").strip()
        source = result.get(source_name)
        if not isinstance(source, str):
            raise SystemExit(f"{name}: derived source parameter {source_name!r} is missing")
        if derive_type == "path-key":
            resolved = str(expand_path(source).resolve())
            digest = hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:16]
            result[str(name)] = f"{Path(resolved).name}-{digest}"
            continue
        if derive_type != "glob-count":
            continue
        pattern = str(derive.get("pattern") or "*")
        directory = expand_path(source)
        if not directory.is_dir():
            raise SystemExit(f"{name}: derived source is not a directory: {directory}")
        count = sum(1 for path in directory.glob(pattern) if path.is_file())
        if count <= 0:
            raise SystemExit(f"{name}: no files matching {pattern!r} in {directory}")
        result[str(name)] = count
    return result


def expand_template(
    template_name: str,
    template_path_: Path,
    template: dict[str, Any],
    params: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params = derived_template_parameters(template, params)
    params = git_lineage_template_parameters(template, params)
    params = source_template_parameters(template, params, config or load_config())
    values = run_name_values(template_name, params)
    values.update(shell_value_format_values(values))
    values.update(repeatable_arg_shell_values(template, values))
    if template_name == "sprt":
        values["sprt_args"] = sprt_shell_args(template, values)
    if template_name == "selfplay":
        values["uci_args"] = fastchess_uci_args(values)
        values["limit_args"] = fastchess_limit_args(values)
    kind = str(template.get("kind") or template_name)
    name_template = str(template.get("run_name") or f"{template_name}-{{timestamp}}")
    run_name = validate_run_name(str(safe_format(name_template, values)))
    description = clean_manifest_text(safe_format(template.get("description"), values), label="description")
    comment = clean_manifest_text(safe_format(template.get("comment"), values), label="comment")
    work_dir = str(safe_format(template.get("work_dir") or ".", values))
    task_template = template.get("task")
    if not isinstance(task_template, dict):
        raise SystemExit(f"{template_name}: task must be an object")
    split = task_template.get("split", {"count": 1})
    if not isinstance(split, dict):
        raise SystemExit(f"{template_name}: task.split must be an object")
    task_count, shards, split_unit = resolve_split_count(split, values)
    try:
        split_start = int(template_value(split.get("start", 0), values))
    except (TypeError, ValueError) as exc:
        raise SystemExit("task.split.start must be a non-negative integer") from exc
    if split_start < 0:
        raise SystemExit("task.split.start must be a non-negative integer")
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
    range_offset = split_start
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
            "shard_pairs": shard // 2 if isinstance(shard, int) else "",
            "chunk": shard,
            "shard_start": range_offset,
            "chunk_start": range_offset,
            "output_dir": "{output_dir}",
            "run_dir": "{run_dir}",
        })
        if template_name == "selfplay" and "seed" in values:
            task_values["seed"] = int(values["seed"]) + index
        if template_name == "sprt":
            task_values["seed"] = int(values.get("book_seed", 1)) + index
            task_values["book_offset"] = (
                int(values.get("book_offset", 0)) + range_offset // 2
            )
        task_values.update(shell_value_format_values(task_values))
        task: dict[str, Any] = {
            "id": task_name,
            "index": index,
            "command": command_string(task_template.get("command"), task_values),
            "outputs": [
                str(safe_format(output_template, task_values))
                for output_template in output_templates
            ],
        }
        if "cancel_rc" in task_template:
            task["cancel_rc"] = int(template_value(task_template["cancel_rc"], task_values))
        if "resume" in task_template:
            resume = safe_format(task_template["resume"], task_values)
            if not isinstance(resume, dict):
                raise SystemExit("task.resume must be an object")
            task["resume"] = resume
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
        if isinstance(shard, int) and split_unit and "start" in split:
            task["adaptive_range"] = {
                "unit": split_unit,
                "start": range_offset,
                "count": shard,
            }
        tasks.append(task)
        if isinstance(shard, int):
            range_offset += shard

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
    if comment:
        manifest["comment"] = comment
    path_maps = template.get("path_maps")
    if path_maps is not None:
        manifest["path_maps"] = safe_format(path_maps, values)
    finalizer = template.get("finalize")
    if finalizer is not None:
        if not isinstance(finalizer, dict):
            raise SystemExit(f"{template_name}: finalize must be an object")
        manifest["finalize"] = safe_format(finalizer, values)
    if template_name == "spsa":
        attach_spsa_manifest_config(manifest, params)
    validate_manifest_payload(manifest, label=f"template {template_name}")
    return manifest


def resolved_workers_arg(
    run_options: argparse.Namespace,
    config: dict[str, Any],
    template: dict[str, Any] | None = None,
) -> str:
    workers = (
        run_options.workers
        or os.environ.get(WORKERS_ENV)
    )
    if not workers:
        from .workers import default_workers_path

        try:
            workers = str(default_workers_path())
        except SystemExit:
            workers = None
    if not workers:
        raise SystemExit(
            "forge run: missing --workers and no default workers file; "
            "pass --workers HOST[:SLOTS],... or create ~/.config/forge/workers.json"
        )
    return workers


def sprt_worker_slots_payload(workers_arg: str) -> dict[str, Any]:
    from .workers import (
        worker_records_from_payload,
        workers_payload_from_arg,
        workers_payload_from_records,
    )

    payload = workers_payload_from_arg(workers_arg, default_ok=False)
    records = worker_records_from_payload(payload, label=workers_arg)
    payload = workers_payload_from_records(records)
    if sprt_worker_slot_count(payload) <= 0:
        raise SystemExit("SPRT workers resolve to zero task slots")
    return payload


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
    target = max(1, games // SPRT_AUTO_TARGET_GAMES_PER_SHARD)
    slots = sprt_worker_slot_count(worker_payload)
    if slots <= 0:
        return target
    paired_limit = max(1, games // 2)
    rounded = max(slots * 2, ((target + slots - 1) // slots) * slots)
    if rounded <= paired_limit:
        return rounded
    largest_multiple = (paired_limit // slots) * slots
    return largest_multiple if largest_multiple > 0 else paired_limit


def auto_adaptive_shards(
    template_name: str,
    template: dict[str, Any],
    params: dict[str, Any],
    worker_payload: dict[str, Any],
    *,
    cold_units: int,
) -> int:
    result = adaptive_autosized_shards(
        template_name,
        template,
        params,
        worker_payload,
        cold_units=cold_units,
    )
    if result is None:
        raise SystemExit(f"{template_name}: template cannot be adaptively split")
    return result


def auto_selfplay_shards(
    params: dict[str, Any],
    worker_payload: dict[str, Any],
    template: dict[str, Any],
) -> int:
    return auto_adaptive_shards(
        "selfplay",
        template,
        params,
        worker_payload,
        cold_units=15,
    )


def cmd_template_run(args: argparse.Namespace) -> int:
    run_options, template_args = parse_run_options(list(args.template_args))
    config = load_config()
    path = template_path(args.template)
    template = load_template(path)
    template_name = template_display_name(path, template)
    if template_name == "to-bullet":
        public_template = template
        if any(arg in {"-h", "--help"} for arg in template_args):
            parse_template_parameters(template_name, template, template_args)
            return 0

        def option_value(name: str, default: str | None = None) -> str | None:
            flag = template_arg_name(name)
            for index, arg in enumerate(template_args):
                if arg == flag and index + 1 < len(template_args):
                    return template_args[index + 1]
                if arg.startswith(flag + "="):
                    return arg.split("=", 1)[1]
            return default

        def option_values(name: str) -> list[str]:
            flag = template_arg_name(name)
            result: list[str] = []
            index = 0
            while index < len(template_args):
                arg = template_args[index]
                if arg == flag and index + 1 < len(template_args):
                    result.append(template_args[index + 1])
                    index += 2
                    continue
                if arg.startswith(flag + "="):
                    result.append(arg.split("=", 1)[1])
                index += 1
            return result

        label_stages = option_values("label") or ["existing"]
        valid_label_pipelines = {
            ("existing",),
            ("outcome",),
            ("search",),
            ("static",),
            ("tb",),
            ("search", "tb"),
            ("static", "tb"),
        }
        if tuple(label_stages) not in valid_label_pipelines:
            raise SystemExit(
                "to-bullet: invalid label pipeline; use one of existing, outcome, "
                "search, static, tb, 'search --label tb', or 'static --label tb'"
            )
        label_mode = label_stages[0]
        apply_tb = len(label_stages) == 2
        input_value = option_value("input")
        if not input_value:
            raise SystemExit("to-bullet: --input is required")
        input_path = expand_path(input_value)
        if not input_path.is_dir():
            raise SystemExit(f"--input is not a directory: {input_path}")
        has_pgn = any(item.is_file() for item in input_path.glob("*.pgn"))
        has_training = any(item.is_file() for item in input_path.rglob("training.*"))
        if has_pgn and has_training:
            raise SystemExit(f"--input mixes PGN and training.* files: {input_path}")
        if not has_pgn and not has_training:
            raise SystemExit(
                f"--input contains neither *.pgn nor training.* files: {input_path}"
            )
        if label_mode in {"existing", "outcome", "tb"} and not has_pgn:
            raise SystemExit(f"--label {label_mode} requires PGN input")
        if apply_tb and not has_pgn:
            raise SystemExit("to-bullet: a tb override currently requires PGN input")

        backend_name = {
            "existing": "selfplay-to-bullet",
            "outcome": "selfplay-to-bullet",
            "search": "label-pgn" if has_pgn else "label-search",
            "static": "label-pgn" if has_pgn else "label-static",
            "tb": "label-tb",
        }[label_mode]
        path = template_path(backend_name)
        template = load_template(path)
        template["name"] = "to-bullet"
        template["description"] = public_template.get("description")
        parameters = template.get("parameters", {})
        public_parameters = public_template.get("parameters", {})
        if isinstance(parameters, dict) and isinstance(public_parameters, dict):
            for name, backend_spec in parameters.items():
                public_spec = public_parameters.get(name)
                if (
                    isinstance(public_spec, dict)
                    and isinstance(backend_spec, dict)
                    and "default" in public_spec
                ):
                    backend_spec["default"] = public_spec["default"]
            for name in ("engine", "net"):
                public_spec = public_parameters.get(name)
                backend_spec = parameters.get(name)
                if (
                    isinstance(public_spec, dict)
                    and isinstance(backend_spec, dict)
                    and "default" in public_spec
                ):
                    backend_spec.pop("required", None)
            if backend_name == "label-pgn":
                mode_spec = parameters.get("mode")
                if isinstance(mode_spec, dict):
                    mode_spec["default"] = label_mode
                net_option_spec = parameters.get("net_option")
                if isinstance(net_option_spec, dict):
                    net_option_spec["default"] = (
                        "EvalFile" if label_mode == "search" else "nnue_file"
                    )
                tb_pieces_spec = parameters.get("tb_pieces")
                if isinstance(tb_pieces_spec, dict):
                    tb_pieces_spec["default"] = (
                        int(public_parameters["tb_pieces"].get("default", 6))
                        if apply_tb
                        else 0
                    )

        allowed_by_mode = {
            "existing": {"input", "output", "label", "chunk_time_seconds", "skip_plies", "min_depth", "max_abs_cp", "shards"},
            "outcome": {"input", "output", "label", "chunk_time_seconds", "skip_plies", "min_depth", "max_abs_cp", "shards"},
            "search": {"input", "output", "label", "engine", "net", "depth", "threads", "hash", "max_records", "min_ply", "quiet_only", "chunk_time_seconds", "shards", "skip_plies", "min_depth", "max_abs_cp"} | ({"tb_pieces"} if apply_tb else set()),
            "static": {"input", "output", "label", "engine", "net", "threads", "hash", "max_records", "min_ply", "quiet_only", "chunk_time_seconds", "shards", "skip_plies", "min_depth", "max_abs_cp"} | ({"tb_pieces"} if apply_tb else set()),
            "tb": {"input", "output", "label", "tb_pieces", "skip_plies", "min_depth", "max_abs_cp", "chunk_time_seconds", "shards"},
        }[label_mode]
        public_specs = public_template.get("parameters", {})
        if isinstance(public_specs, dict):
            for name, spec in public_specs.items():
                if name in allowed_by_mode or not isinstance(spec, dict):
                    continue
                if template_parameter_supplied(public_template, template_args, {str(name)}):
                    raise SystemExit(f"--{str(name).replace('_', '-')} is not valid with --label {label_mode}")

        rewritten: list[str] = []
        index = 0
        while index < len(template_args):
            arg = template_args[index]
            if arg == "--label":
                index += 2
                continue
            if arg.startswith("--label="):
                index += 1
                continue
            if arg == "--input":
                rewritten.extend([
                    "--pgn-dir" if backend_name in {"selfplay-to-bullet", "label-pgn"} else "--input",
                    template_args[index + 1],
                ])
                index += 2
                continue
            if arg.startswith("--input="):
                flag = "--pgn-dir" if backend_name in {"selfplay-to-bullet", "label-pgn"} else "--input"
                rewritten.append(flag + "=" + arg.split("=", 1)[1])
                index += 1
                continue
            rewritten.append(arg)
            index += 1
        if backend_name == "selfplay-to-bullet":
            rewritten.extend([
                "--label-mode",
                "outcome-only" if label_mode == "outcome" else "self-distillation",
            ])
        template_args = rewritten
        template_name = "to-bullet"

    label_input: Path | None = None
    if template_name in {"label-search", "label-static"}:
        for index, arg in enumerate(template_args):
            if arg == "--input" and index + 1 < len(template_args):
                label_input = expand_path(template_args[index + 1])
                break
            if arg.startswith("--input="):
                label_input = expand_path(arg.split("=", 1)[1])
                break
    label_input_is_pgn = False
    if label_input is not None and label_input.is_dir():
        has_pgn = any(path.is_file() for path in label_input.glob("*.pgn"))
        has_training = any(path.is_file() for path in label_input.rglob("training.*"))
        if has_pgn and has_training:
            raise SystemExit(f"--input mixes PGN and training.* files: {label_input}")
        if not has_pgn and not has_training:
            raise SystemExit(f"--input contains neither *.pgn nor training.* files: {label_input}")
        label_input_is_pgn = has_pgn
    if template_name in {"label-search", "label-static"} and label_input_is_pgn:
        public_template = template
        path = template_path("label-pgn")
        template = load_template(path)
        template["name"] = template_name
        template["description"] = public_template.get("description")
        parameters = template.get("parameters", {})
        public_parameters = public_template.get("parameters", {})
        if isinstance(parameters, dict) and isinstance(public_parameters, dict):
            for name in ("engine", "net"):
                public_spec = public_parameters.get(name)
                backend_spec = parameters.get(name)
                if (
                    isinstance(public_spec, dict)
                    and isinstance(backend_spec, dict)
                    and "default" in public_spec
                ):
                    backend_spec["default"] = public_spec["default"]
                    backend_spec.pop("required", None)
            mode_spec = parameters.get("mode")
            if isinstance(mode_spec, dict):
                mode_spec["default"] = "search" if template_name == "label-search" else "static"
                mode_spec.pop("required", None)
            net_option_spec = parameters.get("net_option")
            if isinstance(net_option_spec, dict):
                net_option_spec["default"] = "EvalFile" if template_name == "label-search" else "nnue_file"
            if template_name == "label-search":
                depth_spec = public_parameters.get("depth")
                if isinstance(depth_spec, dict) and isinstance(parameters.get("depth"), dict):
                    parameters["depth"]["default"] = depth_spec.get("default", 12)
        rewritten_args: list[str] = []
        for arg in template_args:
            if arg == "--input":
                rewritten_args.append("--pgn-dir")
            elif arg.startswith("--input="):
                rewritten_args.append("--pgn-dir=" + arg.split("=", 1)[1])
            else:
                rewritten_args.append(arg)
        template_args = rewritten_args
    if run_options.extend:
        if template_name != "sprt":
            raise SystemExit("--extend is currently supported only for the sprt template")
        extension_parser = argparse.ArgumentParser(
            prog="forge sprt --extend RUN",
            allow_abbrev=False,
        )
        extension_parser.add_argument("--games", type=int, required=True)
        extension = extension_parser.parse_args(template_args)
        from .workers import cmd_extend

        return cmd_extend(argparse.Namespace(
            **vars(run_options),
            run=run_options.extend,
            games=extension.games,
        ))
    workers_arg: str | None = None
    sprt_workers_payload: dict[str, Any] | None = None
    selfplay_workers_payload: dict[str, Any] | None = None
    spsa_workers_payload: dict[str, Any] | None = None
    if template_name == "sprt":
        params = parse_sprt_template_parameters(template_name, template, template_args)
        shards_supplied = template_parameter_supplied(template, template_args, {"shards"})
        needs_workers = (
            not (run_options.print_manifest or run_options.dry_run)
            or not shards_supplied
        )
        if needs_workers:
            workers_arg = resolved_workers_arg(run_options, config)
            sprt_workers_payload = sprt_worker_slots_payload(workers_arg)
        if not shards_supplied:
            params = dict(params)
            chunking = template.get("chunking")
            cold_units = (
                max(1, int(chunking.get("initial_units", 1)))
                if isinstance(chunking, dict)
                else 1
            )
            games = as_int(params.get("games"), label="sprt.games")
            params["shards"] = min(
                max(1, games // 2),
                auto_adaptive_shards(
                    template_name,
                    template,
                    params,
                    sprt_workers_payload or {},
                    cold_units=cold_units,
                ),
            )
    elif template_name == "selfplay":
        params = parse_selfplay_template_parameters(template_name, template, template_args)
        shards_supplied = template_parameter_supplied(template, template_args, {"shards"})
        needs_workers = (
            not (run_options.print_manifest or run_options.dry_run)
            or not shards_supplied
        )
        if needs_workers:
            workers_arg = resolved_workers_arg(run_options, config)
            selfplay_workers_payload = sprt_worker_slots_payload(workers_arg)
        if not shards_supplied:
            params = dict(params)
            params["shards"] = auto_selfplay_shards(
                params,
                selfplay_workers_payload or {},
                template,
            )
    elif template_name == "spsa":
        params = parse_template_parameters(template_name, template, template_args)
        shards_supplied = template_parameter_supplied(template, template_args, {"shards"})
        needs_workers = (
            not (run_options.print_manifest or run_options.dry_run)
            or not shards_supplied
        )
        if needs_workers:
            workers_arg = resolved_workers_arg(run_options, config)
            spsa_workers_payload = spsa_worker_slots_payload(workers_arg)
        if not shards_supplied:
            params = dict(params)
            cold_shards = auto_spsa_shards(params, spsa_workers_payload or {})
            games = as_int(params.get("games"), label="spsa.games")
            params["shards"] = min(
                as_int(params.get("rounds"), label="spsa.rounds"),
                auto_adaptive_shards(
                    template_name,
                    template,
                    params,
                    spsa_workers_payload or {},
                    cold_units=max(1, (games + cold_shards - 1) // cold_shards),
                ),
            )
        params = prepare_spsa_template_parameters(params)
        if spsa_workers_payload is not None:
            spsa_workers_payload = cap_spsa_worker_slots(
                spsa_workers_payload,
                int(params["shards"]),
            )
    else:
        params = parse_template_parameters(template_name, template, template_args)
        params = derived_template_parameters(template, params)
        shards_param = template.get("parameters", {}).get("shards")
        if isinstance(shards_param, dict):
            shards_supplied = template_parameter_supplied(template, template_args, {"shards", "shard_count"})
            split = template.get("task", {}).get("split", {})
            total_key = split.get("total") if isinstance(split, dict) else None
            needs_workers = (
                not (run_options.print_manifest or run_options.dry_run)
                or (not shards_supplied and bool(total_key))
            )
            if needs_workers:
                generic_workers_arg = resolved_workers_arg(run_options, config)
                generic_workers_payload = sprt_worker_slots_payload(generic_workers_arg)
            if not shards_supplied and total_key:
                params = dict(params)
                total = as_int(params.get(total_key), label=f"{template_name}.{total_key}")
                configured = as_int(shards_param.get("default", 1), label=f"{template_name}.shards")
                chunking = template.get("chunking")
                initial_units = (
                    int(chunking.get("initial_units", 0))
                    if isinstance(chunking, dict)
                    else 0
                )
                cold_units = (
                    max(1, initial_units)
                    if initial_units > 0
                    else max(1, (total + configured - 1) // configured)
                )
                params["shards"] = auto_adaptive_shards(
                    template_name,
                    template,
                    params,
                    generic_workers_payload or {},
                    cold_units=cold_units,
                )
    run_options = apply_template_run_defaults(template_name, run_options, template)
    inventory_info: dict[str, Any] | None = None
    if str(template.get("kind") or "").strip() == "label":
        inventory_info = build_inventory(str(params["input"]))
        params = dict(params)
        params["shards"] = min(
            as_int(params.get("shards"), label=f"{template_name}.shards"),
            len(inventory_info["files"]),
        )
        params["source_cache"] = "~/.cache/forge/inputs/" + str(inventory_info["digest"])
        params["inventory"] = params["source_cache"] + "/inventory.json"
        params["inventory_digest"] = str(inventory_info["digest"])
        params["quiet_only_arg"] = "--quiet-only" if bool(params["quiet_only"]) else "--no-quiet-only"
    manifest = expand_template(template_name, path, template, params, config)
    if inventory_info is not None:
        attach_label_task_inputs(manifest, inventory_info)
    if str(template.get("kind") or "") in {"selfplay-to-bullet", "label-pgn", "label-tb"}:
        attach_pgn_task_inputs(manifest)
    if bool(template.get("materialize_inputs", False)):
        materialize_template_inputs(template, manifest, params)
    manifest_template = manifest.get("template")
    resolved_params = (
        manifest_template.get("parameters")
        if isinstance(manifest_template, dict)
        else None
    )
    attach_source_artifacts(
        template,
        manifest,
        resolved_params if isinstance(resolved_params, dict) else params,
    )
    finalizer = manifest.get("finalize")
    if isinstance(finalizer, dict) and finalizer.get("type") == "bullet-labels":
        finalizer["replace"] = bool(run_options.replace)
        output = expand_path(str(finalizer.get("output") or ""))
        if output.exists() and not run_options.replace:
            raise SystemExit(f"{output}: exists; pass --replace to overwrite")
    comment = clean_manifest_text(run_options.comment, label="--comment")
    if comment:
        manifest["comment"] = comment
    if run_options.print_manifest or run_options.dry_run:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    workers_arg = workers_arg or resolved_workers_arg(run_options, config, template)
    coordinator_host = run_options.coordinator_host or default_coordinator_host(config)
    notify_command = run_options.notify_command
    task_complete_command = run_options.task_complete_command
    task_complete_sync = run_options.task_complete_sync
    run_id = run_options.run or str(manifest["name"])
    print(f"run: id={run_id}", flush=True)
    with tempfile.TemporaryDirectory(prefix="forge-template-") as tmp:
        manifest_path = Path(tmp) / f"{manifest['name']}.manifest.json"
        write_json(manifest_path, manifest)
        if template_name in {"sprt", "selfplay", "spsa"}:
            workers_path = Path(tmp) / "workers.json"
            if template_name == "spsa":
                worker_payload = spsa_workers_payload or spsa_worker_slots_payload(workers_arg)
            else:
                worker_payload = (
                    sprt_workers_payload
                    or selfplay_workers_payload
                    or sprt_worker_slots_payload(workers_arg)
                )
            write_json(workers_path, worker_payload)
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
            task_complete_command=task_complete_command,
            task_complete_sync=task_complete_sync,
            comment=comment,
            no_color=run_options.no_color,
            verbose=run_options.verbose,
            command_label="run",
        )
        return cmd_run_job(start_args)
