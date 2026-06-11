from __future__ import annotations

from .core import *


CONFIG_ENV = "CRUCIBLE_CONFIG"
WORKERS_ENV = "CRUCIBLE_WORKERS"
COORDINATOR_HOST_ENV = "CRUCIBLE_COORDINATOR_HOST"
CONFIG_DIR = Path("~/.config/crucible")
CONFIG_YML = "config.yml"
CONFIG_TOML = "config.toml"


def config_dir() -> Path:
    config = os.environ.get(CONFIG_ENV)
    if config:
        return expand_path(config).parent
    return expand_path(CONFIG_DIR)


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
    current_list: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith((" ", "\t")):
            if current_list is None:
                raise SystemExit(f"{path}: unsupported nested config line {raw_line!r}")
            item = line.strip()
            if not item.startswith("- "):
                raise SystemExit(f"{path}: expected list item, got {raw_line!r}")
            data[current_list].append(parse_scalar(item[2:]))
            continue
        key, sep, value = line.partition(":")
        if not sep or not key.strip():
            raise SystemExit(f"{path}: invalid config line {raw_line!r}")
        key = key.strip()
        value = value.strip()
        if value:
            data[key] = parse_scalar(value)
            current_list = None
        else:
            data[key] = []
            current_list = key
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
        ".crucible/templates",
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
    config = config if config is not None else load_config()
    value = config.get("notify_command") or config.get("default_notify_command")
    return str(value).strip() if value else None
