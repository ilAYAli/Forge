"""Temporary per-host capacity limits ("holds").

A host's capacity always comes from its detected hardware.  A hold caps it for
a while, for example while a lichess bot plays on that host: ``forge worker
hold pwa-mbp 0 --reason lichess``.  Holds live on the coordinator, apply to
the active run at once and to every run started while they last, and end with
``forge worker release`` or when their ``--for`` duration runs out.
"""

from __future__ import annotations

import re
import socket
import time
from pathlib import Path
from typing import Any

from .capacity import normalize_utilization
from .core import read_json, runs_dir, write_json

HOLDS_FILE = "holds.json"
HOLDS_SCHEMA = "forge.holds.v1"

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$", re.IGNORECASE)
_DURATION_UNITS = {"": 60.0, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def holds_path() -> Path:
    return runs_dir() / HOLDS_FILE


def parse_duration(value: str) -> float:
    """``90m``, ``3h``, ``1d``, ``45s``; a bare number is minutes."""
    match = _DURATION_RE.match(str(value))
    if not match or float(match.group(1)) <= 0:
        raise SystemExit(f"invalid duration {value!r}; use for example 30m, 3h or 1d")
    return float(match.group(1)) * _DURATION_UNITS[match.group(2).lower()]


def _read() -> dict[str, dict[str, Any]]:
    path = holds_path()
    if not path.exists():
        return {}
    try:
        payload = read_json(path)
    except (OSError, ValueError, SystemExit):
        return {}
    holds = payload.get("holds") if isinstance(payload, dict) else None
    return {str(host): hold for host, hold in (holds or {}).items() if isinstance(hold, dict)}


def _write(holds: dict[str, dict[str, Any]]) -> None:
    path = holds_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, {"schema": HOLDS_SCHEMA, "holds": holds})


def _expired(hold: dict[str, Any], now: float) -> bool:
    until = hold.get("until_epoch")
    return until is not None and float(until) <= now


def active_holds(now: float | None = None) -> dict[str, dict[str, Any]]:
    """Holds in force now (expired ones are left for :func:`expire_holds`)."""
    now = time.time() if now is None else now
    try:
        holds = _read()
    except SystemExit:
        return {}
    return {host: hold for host, hold in holds.items() if not _expired(hold, now)}


def host_hold_limit(host: str) -> str | None:
    hold = active_holds().get(host)
    return str(hold["limit"]) if hold else None


def set_hold(host: str, limit: str, *, reason: str | None, seconds: float | None) -> dict[str, Any]:
    now = time.time()
    hold: dict[str, Any] = {
        "limit": normalize_utilization(limit, label=host),
        "reason": reason or "",
        "since_epoch": now,
        "by": socket.gethostname(),
    }
    if seconds is not None:
        hold["until_epoch"] = now + seconds
    holds = _read()
    holds[host] = hold
    _write(holds)
    return hold


def clear_hold(host: str) -> dict[str, Any] | None:
    holds = _read()
    hold = holds.pop(host, None)
    if hold is not None:
        _write(holds)
    return hold


def expire_holds(now: float | None = None) -> list[str]:
    """Remove holds whose time is up; return their hosts."""
    now = time.time() if now is None else now
    holds = _read()
    expired = [host for host, hold in holds.items() if _expired(hold, now)]
    if expired:
        for host in expired:
            holds.pop(host)
        _write(holds)
    return expired


def describe_hold(host: str, hold: dict[str, Any], now: float | None = None) -> str:
    now = time.time() if now is None else now
    text = f"{host} max {hold.get('limit')}"
    if hold.get("reason"):
        text += f" ({hold['reason']})"
    since = hold.get("since_epoch")
    if since is not None:
        text += f" since {time.strftime('%H:%M', time.localtime(float(since)))}"
    until = hold.get("until_epoch")
    if until is not None:
        minutes = max(0, int((float(until) - now) // 60))
        text += f", ends in {minutes // 60}h{minutes % 60:02d}m"
    return text
