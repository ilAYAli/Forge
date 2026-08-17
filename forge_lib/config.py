from __future__ import annotations

from .core import *


CONFIG_ENV = "FORGE_CONFIG"
CACHE_DIR_ENV = "FORGE_CACHE_DIR"
WORKERS_ENV = "FORGE_WORKERS"
COORDINATOR_HOST_ENV = "FORGE_COORDINATOR_HOST"
NOTIFY_ENV = "FORGE_NOTIFY_COMMAND"
TASK_COMPLETE_ENV = "FORGE_TASK_COMPLETE_COMMAND"
SPEEDUP_SINGLE_HOST_WORKERS_ENV = "FORGE_SPEEDUP_SINGLE_HOST_WORKERS"
CONFIG_DIR = Path("~/.config/forge")
CONFIG_YML = "config.yml"
CONFIG_TOML = "config.toml"
CONFIG_WORKERS = "workers.json"


def config_dir() -> Path:
    config = os.environ.get(CONFIG_ENV)
    if config:
        return expand_path(config).parent
    return expand_path(CONFIG_DIR)


def config_workers_path() -> Path:
    return config_dir() / CONFIG_WORKERS


def cache_dir() -> Path:
    value = os.environ.get(CACHE_DIR_ENV)
    return expand_path(value) if value else expand_path("~/.cache/forge")


def config_path() -> Path | None:
    override = os.environ.get(CONFIG_ENV)
    if override:
        path = expand_path(override)
        return path if path.exists() else None
    for name in (CONFIG_YML, CONFIG_TOML):
        path = config_dir() / name
        if path.exists():
            return path
    return None


def parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value in {"true", "false"}:
        return value == "true"
    if value in {"null", "~"}:
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def read_simple_yaml(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    current_container: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith((" ", "\t")):
            if current_container is None:
                raise SystemExit(f"{path}: unsupported nested config line {raw_line!r}")
            item = line.strip()
            if item.startswith("- "):
                container = data[current_container]
                if container == {}:
                    container = []
                    data[current_container] = container
                if not isinstance(container, list):
                    raise SystemExit(f"{path}: mixed mapping and list under {current_container}")
                container.append(parse_scalar(item[2:]))
                continue
            child_key, sep, child_value = item.partition(":")
            container = data[current_container]
            if (
                not sep or not child_key.strip() or not child_value.strip()
                or not isinstance(container, dict)
            ):
                raise SystemExit(f"{path}: invalid mapping entry {raw_line!r}")
            container[child_key.strip()] = parse_scalar(child_value)
            continue
        key, sep, value = line.partition(":")
        if not sep or not key.strip():
            raise SystemExit(f"{path}: invalid config line {raw_line!r}")
        key = key.strip()
        value = value.strip()
        if value:
            data[key] = parse_scalar(value)
            current_container = None
        else:
            data[key] = {}
            current_container = key
    return data


def read_simple_toml(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        key, sep, value = line.partition("=")
        if not sep or not key.strip():
            raise SystemExit(f"{path}: invalid config line {raw_line!r}")
        data[key.strip()] = parse_scalar(value)
    return data


def load_config() -> dict[str, Any]:
    path = config_path()
    if path is None:
        return {}
    if path.suffix in {".yml", ".yaml"}:
        return read_simple_yaml(path)
    if path.suffix == ".toml":
        return read_simple_toml(path)
    raise SystemExit(f"{path}: unsupported config file type")


def configured_source(name: str, config: dict[str, Any] | None = None) -> Path:
    config = config if config is not None else load_config()
    sources = config.get("sources", {})
    value = sources.get(name) if isinstance(sources, dict) else None
    if value is None:
        value = config.get(f"sources.{name}", config.get(f"source.{name}"))
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(
            f"source alias {name!r} is not configured; add "
            f"'sources:\\n  {name}: /path/to/source' to Forge config"
        )
    path = expand_path(value)
    if not path.is_dir():
        raise SystemExit(f"configured source {name!r} is not a directory: {path}")
    return path


def config_template_dirs(config: dict[str, Any] | None = None) -> list[Path]:
    config = config if config is not None else load_config()
    raw = config.get("template_dirs")
    dirs: list[str]
    if isinstance(raw, str):
        dirs = [raw]
    elif isinstance(raw, list):
        dirs = [str(item) for item in raw]
    elif raw is None:
        dirs = []
    else:
        raise SystemExit("config template_dirs must be a string or list")
    defaults = [
        ".forge/templates",
        str(config_dir() / "templates"),
        str(script_repo() / "templates"),
    ]
    result: list[Path] = []
    seen: set[Path] = set()
    for item in [*dirs, *defaults]:
        path = expand_path(item)
        if path in seen:
            continue
        seen.add(path)
        result.append(path)
    return result


def default_workers_arg(config: dict[str, Any] | None = None) -> str | None:
    if value := os.environ.get(WORKERS_ENV):
        return value
    config = config if config is not None else load_config()
    value = config.get("default_workers")
    return str(value).strip() if value else None


def default_coordinator_host(config: dict[str, Any] | None = None) -> str | None:
    if value := os.environ.get(COORDINATOR_HOST_ENV):
        return value.strip() or None
    config = config if config is not None else load_config()
    value = config.get("coordinator_host") or config.get("default_coordinator_host")
    return str(value).strip() if value else None


def default_notify_command(config: dict[str, Any] | None = None) -> str | None:
    if value := os.environ.get(NOTIFY_ENV):
        return value.strip() or None
    config = config if config is not None else load_config()
    value = config.get("notify_command") or config.get("default_notify_command")
    return str(value).strip() if value else None


def default_task_complete_command(config: dict[str, Any] | None = None) -> str | None:
    if value := os.environ.get(TASK_COMPLETE_ENV):
        return value.strip() or None
    config = config if config is not None else load_config()
    value = config.get("task_complete_command") or config.get("default_task_complete_command")
    if not value:
        return None
    return str(value).strip() or None


def speedup_single_host_workers(config: dict[str, Any] | None = None) -> int:
    raw = os.environ.get(SPEEDUP_SINGLE_HOST_WORKERS_ENV)
    if raw is None:
        config = config if config is not None else load_config()
        raw = config.get("speedup.single_host_workers", config.get("speedup_single_host_workers", 5))
    try:
        workers = int(raw)
    except (TypeError, ValueError):
        raise SystemExit("config speedup.single_host_workers must be a positive integer") from None
    if workers <= 0:
        raise SystemExit("config speedup.single_host_workers must be a positive integer")
    return workers
