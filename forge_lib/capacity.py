"""Host capacity, task resource costs, and utilization-to-slot resolution.

Forge counts capacity in hardware threads.  Each worker reports its own
hardware (threads, physical cores, performance cores, memory), each manifest
declares what one task costs, and a per-host ``utilization`` says how much of
the host a run may use.  The result is a concrete slot count per host that the
rest of Forge consumes exactly like the old static ``slots`` field.
"""

from __future__ import annotations

import ast
import concurrent.futures
import math
import operator
import os
import platform
import re
import sys
from typing import Any

from .core import epoch, expand_path, read_json, write_json

TIMING_FIXED = "fixed"
TIMING_TIMED = "timed"
TIMINGS = (TIMING_FIXED, TIMING_TIMED)

HARDWARE_SCHEMA = "forge.hardware.v1"
HARDWARE_CACHE_TTL_SECONDS = 600.0
HARDWARE_PROBE_TIMEOUT_SECONDS = 20.0
# Share of physical memory Forge plans with; the rest is left for the OS.
MEMORY_PLANNING_FRACTION = 0.9
# Minimum free memory (MiB) a worker keeps in reserve before starting a task.
MEMORY_GATE_MIN_RESERVE_MB = 512
MEMORY_GATE_RESERVE_FRACTION = 0.05

DEFAULT_UTILIZATION = "100%"

# Probe output is KEY=VALUE lines.  Linux reads lscpu (SMT topology), cgroup v2
# CPU and memory limits, and /proc/meminfo; macOS reads sysctl, including the
# performance-core count on Apple Silicon.
HARDWARE_PROBE_SCRIPT = r"""
os=$(uname -s 2>/dev/null)
echo "os=$os"
if [ "$os" = "Darwin" ]; then
  echo "threads=$(sysctl -n hw.logicalcpu 2>/dev/null)"
  echo "cores=$(sysctl -n hw.physicalcpu 2>/dev/null)"
  echo "perf_cores=$(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null)"
  echo "mem_bytes=$(sysctl -n hw.memsize 2>/dev/null)"
else
  echo "threads=$(nproc --all 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null)"
  echo "usable_threads=$(nproc 2>/dev/null)"
  if command -v lscpu >/dev/null 2>&1; then
    echo "cores=$(lscpu -p=CORE,SOCKET 2>/dev/null | grep -v '^#' | sort -u | wc -l | tr -d ' ')"
  fi
  echo "mem_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null)"
  [ -r /sys/fs/cgroup/cpu.max ] && echo "cgroup_cpu=$(cat /sys/fs/cgroup/cpu.max)"
  [ -r /sys/fs/cgroup/memory.max ] && echo "cgroup_mem=$(cat /sys/fs/cgroup/memory.max)"
  # Intel hybrid CPUs list their performance-core CPUs here; count the
  # physical cores behind them (P-cores may have SMT, E-cores do not).
  if [ -r /sys/devices/cpu_core/cpus ]; then
    echo "perf_cores=$(tr ',' '\n' < /sys/devices/cpu_core/cpus | awk -F- 'NF==2 {for (i=$1; i<=$2; i++) print i; next} NF==1 && $1 != "" {print $1}' | while read c; do
      echo "$(cat /sys/devices/system/cpu/cpu$c/topology/physical_package_id 2>/dev/null)-$(cat /sys/devices/system/cpu/cpu$c/topology/core_id 2>/dev/null)"
    done | sort -u | wc -l | tr -d ' ')"
  fi
fi
exit 0
"""


# --------------------------------------------------------------------------
# Utilization specs


def parse_utilization(value: Any, *, label: str = "utilization") -> tuple[str, float]:
    """Parse a utilization spec.

    ``"50%"`` is a share of the host, a bare integer (``4`` or ``"4"``) is a
    hardware-thread budget, and ``"max"``/``"auto"`` mean 100%.  Returns
    ``("percent", value)`` or ``("threads", value)``.
    """
    if isinstance(value, bool):
        raise SystemExit(f"{label}: utilization must be N% or a thread count")
    if isinstance(value, int):
        if value < 0:
            raise SystemExit(f"{label}: utilization must not be negative")
        return "threads", float(value)
    text = str(value if value is not None else "").strip().lower()
    if text in ("", "max", "auto"):
        return "percent", 100.0
    if text.endswith("%"):
        try:
            percent = float(text[:-1].strip())
        except ValueError as exc:
            raise SystemExit(f"{label}: invalid utilization {value!r}") from exc
        if not 0.0 <= percent <= 100.0 or math.isnan(percent):
            raise SystemExit(f"{label}: utilization must be between 0% and 100%")
        return "percent", percent
    try:
        threads = int(text)
    except ValueError as exc:
        raise SystemExit(f"{label}: invalid utilization {value!r}; use N%, a thread count, or max") from exc
    if threads < 0:
        raise SystemExit(f"{label}: utilization must not be negative")
    return "threads", float(threads)


def normalize_utilization(value: Any, *, label: str = "utilization") -> str:
    kind, amount = parse_utilization(value, label=label)
    if kind == "threads":
        return str(int(amount))
    return f"{amount:g}%"


def utilization_is_zero(value: Any, *, label: str = "utilization") -> bool:
    return parse_utilization(value, label=label)[1] <= 0.0


# --------------------------------------------------------------------------
# Task resource declarations

_BINARY_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.FloorDiv: operator.floordiv,
    ast.Div: operator.truediv,
}


def _eval_arith(node: ast.AST) -> float:
    if isinstance(node, ast.Expression):
        return _eval_arith(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
        return float(node.value)
    if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPS:
        return float(_BINARY_OPS[type(node.op)](_eval_arith(node.left), _eval_arith(node.right)))
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        operand = _eval_arith(node.operand)
        return operand if isinstance(node.op, ast.UAdd) else -operand
    raise ValueError("only numbers, + - * / // and parentheses are allowed")


def evaluate_resource_expression(value: Any, values: dict[str, Any], *, label: str) -> int:
    """Evaluate ``"{threads}*{concurrency}"`` style expressions to an int."""
    if isinstance(value, bool):
        raise SystemExit(f"{label}: expected a number or arithmetic expression")
    if isinstance(value, (int, float)):
        return max(0, int(math.ceil(value)))
    text = str(value or "").strip()
    if not text:
        return 0

    def substitute(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise SystemExit(f"{label}: unknown variable {{{key}}}")
        replacement = values[key]
        if replacement in (None, ""):
            return "0"
        return f"({replacement})"

    expression = re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", substitute, text)
    try:
        result = _eval_arith(ast.parse(expression, mode="eval"))
    except (SyntaxError, ValueError, ZeroDivisionError, TypeError) as exc:
        raise SystemExit(f"{label}: cannot evaluate {text!r}: {exc}") from exc
    return max(0, int(math.ceil(result)))


def default_task_resources() -> dict[str, Any]:
    return {"timing": TIMING_FIXED, "threads": 1, "memory_mb": 0}


def evaluate_resources(spec: Any, values: dict[str, Any], *, label: str) -> dict[str, Any]:
    """Turn a template or manifest ``resources`` block into concrete numbers."""
    if spec is None:
        return default_task_resources()
    if not isinstance(spec, dict):
        raise SystemExit(f"{label}: resources must be an object")
    timing_raw = str(spec.get("timing") or TIMING_FIXED)
    timing = re.sub(
        r"\{([A-Za-z_][A-Za-z0-9_]*)\}",
        lambda m: str(values.get(m.group(1), "")),
        timing_raw,
    ).strip().lower()
    if timing not in TIMINGS:
        raise SystemExit(f"{label}: resources.timing must be fixed or timed, got {timing_raw!r}")
    # "timed_unless": ["nodes", "depth"] marks work as timed unless one of the
    # named limits is set (e.g. selfplay with a node limit is fixed work).
    timed_unless = spec.get("timed_unless")
    if timed_unless is not None:
        if not isinstance(timed_unless, list):
            raise SystemExit(f"{label}: resources.timed_unless must be a list")
        limited = any(values.get(str(key)) not in (None, "", 0, "0") for key in timed_unless)
        timing = TIMING_FIXED if limited else TIMING_TIMED
    threads = evaluate_resource_expression(spec.get("threads", 1), values, label=f"{label}.threads")
    memory_mb = evaluate_resource_expression(spec.get("memory_mb", 0), values, label=f"{label}.memory_mb")
    return {"timing": timing, "threads": max(1, threads), "memory_mb": memory_mb}


def manifest_task_resources(manifest: dict[str, Any]) -> dict[str, Any]:
    """Return the per-task cost a manifest declares (defaults: fixed, 1 thread)."""
    spec = manifest.get("resources")
    if spec is None:
        return default_task_resources()
    vars_obj = manifest.get("vars")
    values = dict(vars_obj) if isinstance(vars_obj, dict) else {}
    return evaluate_resources(spec, values, label=str(manifest.get("name") or "manifest"))


def format_task_resources(resources: dict[str, Any]) -> str:
    text = f"{resources.get('timing')} x{resources.get('threads')} thread(s)"
    memory = int(resources.get("memory_mb") or 0)
    if memory:
        text += f", {memory} MiB"
    return text


# --------------------------------------------------------------------------
# Hardware detection


def _int_or_none(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def parse_hardware_probe(text: str) -> dict[str, Any]:
    raw: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            raw[key.strip()] = value.strip()
    threads = _int_or_none(raw.get("threads"))
    usable = _int_or_none(raw.get("usable_threads"))
    if threads and usable:
        threads = min(threads, usable)
    threads = threads or usable
    if not threads:
        raise ValueError("hardware probe did not report a thread count")
    cores = _int_or_none(raw.get("cores"))
    if not cores or cores > threads:
        cores = threads
    # cgroup v2 cpu.max is "<quota> <period>" or "max <period>".
    cgroup_cpu = raw.get("cgroup_cpu", "").split()
    if len(cgroup_cpu) == 2 and cgroup_cpu[0] != "max":
        quota, period = _int_or_none(cgroup_cpu[0]), _int_or_none(cgroup_cpu[1])
        if quota and period:
            limit = max(1, int(math.ceil(quota / period)))
            if limit < threads:
                ratio = threads / cores
                threads = limit
                cores = max(1, min(cores, int(math.ceil(limit / ratio))))
    perf_cores = _int_or_none(raw.get("perf_cores"))
    if not perf_cores or perf_cores > cores:
        perf_cores = cores
    mem_mb = None
    mem_bytes = _int_or_none(raw.get("mem_bytes"))
    if mem_bytes:
        mem_mb = mem_bytes // (1024 * 1024)
    mem_kb = _int_or_none(raw.get("mem_kb"))
    if mem_kb:
        mem_mb = mem_kb // 1024
    cgroup_mem = _int_or_none(raw.get("cgroup_mem"))
    if cgroup_mem:
        limit_mb = cgroup_mem // (1024 * 1024)
        mem_mb = min(mem_mb, limit_mb) if mem_mb else limit_mb
    return {
        "schema": HARDWARE_SCHEMA,
        "os": raw.get("os") or "",
        "threads": threads,
        "cores": cores,
        "perf_cores": perf_cores,
        "memory_mb": mem_mb or 0,
    }


def hardware_cache_path(host: str):
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", host) or "host"
    return expand_path("~/.cache/forge/hardware") / f"{safe}.json"


def read_cached_hardware(host: str, *, max_age: float | None) -> dict[str, Any] | None:
    path = hardware_cache_path(host)
    try:
        payload = read_json(path)
    except (OSError, SystemExit, ValueError):
        return None
    if payload.get("schema") != HARDWARE_SCHEMA:
        return None
    if max_age is not None:
        try:
            age = epoch() - float(payload.get("probed_epoch") or 0)
        except (TypeError, ValueError):
            return None
        if age > max_age:
            return None
    return payload


def write_cached_hardware(host: str, hardware: dict[str, Any]) -> None:
    path = hardware_cache_path(host)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json(path, hardware)
    except OSError:
        pass


HARDWARE_OVERRIDE_ENV = "FORGE_HARDWARE"


def hardware_override() -> dict[str, Any] | None:
    """Return hardware forced through FORGE_HARDWARE (JSON), used by tests."""
    text = os.environ.get(HARDWARE_OVERRIDE_ENV, "").strip()
    if not text:
        return None
    import json

    try:
        payload = json.loads(text)
    except ValueError as exc:
        raise SystemExit(f"{HARDWARE_OVERRIDE_ENV}: invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{HARDWARE_OVERRIDE_ENV}: must be a JSON object")
    threads = _int_or_none(payload.get("threads")) or 1
    cores = _int_or_none(payload.get("cores")) or threads
    return {
        "schema": HARDWARE_SCHEMA,
        "os": str(payload.get("os") or ""),
        "threads": threads,
        "cores": cores,
        "perf_cores": _int_or_none(payload.get("perf_cores")) or cores,
        "memory_mb": _int_or_none(payload.get("memory_mb")) or 0,
    }


def probe_worker_hardware(worker: dict[str, Any], *, refresh: bool = False) -> dict[str, Any] | None:
    """Return detected hardware for a worker, using a short-lived cache.

    Returns ``None`` only when the host cannot be probed and nothing is cached.
    """
    from .workers import run_worker_shell, worker_host

    host = worker_host(worker)
    override = hardware_override()
    if override is not None:
        return {**override, "host": host}
    if not refresh:
        cached = read_cached_hardware(host, max_age=HARDWARE_CACHE_TTL_SECONDS)
        if cached is not None:
            return cached
    try:
        result = run_worker_shell(worker, HARDWARE_PROBE_SCRIPT, timeout=HARDWARE_PROBE_TIMEOUT_SECONDS, retries=1)
        if result.returncode != 0:
            raise ValueError((result.stderr or result.stdout or "").strip() or f"rc={result.returncode}")
        hardware = parse_hardware_probe(result.stdout)
    except Exception as exc:  # noqa: BLE001 - fall back to stale cache below
        stale = read_cached_hardware(host, max_age=None)
        if stale is not None:
            print(f"capacity: {host}: hardware probe failed ({exc}); using cached hardware", file=sys.stderr)
            return stale
        print(f"capacity: {host}: hardware probe failed ({exc})", file=sys.stderr)
        return None
    hardware["host"] = host
    hardware["probed_epoch"] = epoch()
    write_cached_hardware(host, hardware)
    return hardware


def format_hardware(hardware: dict[str, Any]) -> str:
    text = f"{hardware.get('threads')}T/{hardware.get('cores')}C"
    if hardware.get("perf_cores") and hardware.get("perf_cores") != hardware.get("cores"):
        text += f"/{hardware.get('perf_cores')}P"
    memory = int(hardware.get("memory_mb") or 0)
    if memory:
        text += f" {memory / 1024:.0f}GiB"
    return text


# --------------------------------------------------------------------------
# Slot resolution


def resolve_slots(
    hardware: dict[str, Any],
    utilization: Any,
    resources: dict[str, Any],
    *,
    allow_oversubscribe: bool = False,
) -> tuple[int, list[str]]:
    """Return ``(slots, notes)`` for one host.

    Timed tasks need a whole physical core per engine thread (performance
    cores only on hybrid CPUs), so on an SMT host each engine thread costs
    two hardware threads.  Fixed-work tasks cost one hardware thread per
    engine thread.  Memory caps the result when the task declares a size.
    Any non-zero utilization yields at least one task.
    """
    notes: list[str] = []
    kind, amount = parse_utilization(utilization)
    if amount <= 0:
        return 0, notes
    threads = max(1, int(hardware.get("threads") or 1))
    cores = max(1, int(hardware.get("cores") or threads))
    perf_cores = max(1, int(hardware.get("perf_cores") or cores))
    threads_per_core = max(1, threads // cores)
    engine_threads = max(1, int(resources.get("threads") or 1))
    timed = resources.get("timing") == TIMING_TIMED

    if kind == "threads":
        budget_threads = int(amount)
        if budget_threads > threads and not allow_oversubscribe:
            notes.append(f"requested {budget_threads} threads, host has {threads}; capped")
            budget_threads = threads
        share = min(1.0, budget_threads / threads)
    else:
        share = amount / 100.0
        budget_threads = int(math.floor(threads * share))

    if timed:
        if kind == "threads":
            budget_cores = budget_threads // threads_per_core
            if not allow_oversubscribe:
                budget_cores = min(perf_cores, budget_cores)
        else:
            budget_cores = int(math.floor(perf_cores * share))
        slots = budget_cores // engine_threads
    else:
        slots = budget_threads // engine_threads

    memory_mb = int(resources.get("memory_mb") or 0)
    host_memory = int(hardware.get("memory_mb") or 0)
    if memory_mb > 0 and host_memory > 0:
        memory_budget = int(host_memory * MEMORY_PLANNING_FRACTION * share)
        memory_slots = memory_budget // memory_mb
        if memory_slots < slots:
            notes.append(f"memory limits {slots} -> {memory_slots} task(s) ({memory_mb} MiB each)")
            slots = memory_slots

    if slots < 1:
        notes.append("share is smaller than one task; running 1")
        slots = 1
    return slots, notes


_LEGACY_SLOTS_WARNED: set[str] = set()


def warn_legacy_worker_slots(host: str) -> None:
    if host in _LEGACY_SLOTS_WARNED:
        return
    _LEGACY_SLOTS_WARNED.add(host)
    print(
        f"workers: {host}: 'slots' is ignored; Forge sizes hosts from detected hardware. "
        'Use "utilization": "50%" to keep part of a host free.',
        file=sys.stderr,
    )


def record_utilization(record: dict[str, Any]) -> str:
    value = record.get("utilization")
    return normalize_utilization(value if value is not None else DEFAULT_UTILIZATION, label=str(record.get("host") or "worker"))


def resolve_worker_records(
    records: list[dict[str, Any]],
    resources: dict[str, Any],
    *,
    force: bool = False,
    refresh: bool = False,
    allow_oversubscribe: bool = False,
    quiet: bool = False,
) -> list[dict[str, Any]]:
    """Fill ``slots`` and ``capacity`` on worker records from their utilization.

    Records already resolved (carrying ``capacity``) are kept unless ``force``.
    Hosts with zero utilization are dropped.
    """
    pending = [
        index for index, record in enumerate(records)
        if force or not isinstance(record.get("capacity"), dict)
    ]
    hardware_by_index: dict[int, dict[str, Any] | None] = {}
    if pending:
        unique: dict[str, int] = {}
        for index in pending:
            unique.setdefault(str(records[index].get("host") or ""), index)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(16, len(unique)))) as executor:
            futures = {
                host: executor.submit(probe_worker_hardware, records[index], refresh=refresh)
                for host, index in unique.items()
            }
            by_host = {host: future.result() for host, future in futures.items()}
        for index in pending:
            hardware_by_index[index] = by_host.get(str(records[index].get("host") or ""))

    resolved: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if index not in hardware_by_index:
            resolved.append(record)
            continue
        record = dict(record)
        host = str(record.get("host") or "worker")
        if record.get("utilization") is None and "slots" in record and not isinstance(record.get("capacity"), dict):
            warn_legacy_worker_slots(host)
        utilization = record_utilization(record)
        record["utilization"] = utilization
        if utilization_is_zero(utilization):
            if not quiet:
                print(f"capacity: {host} utilization=0%; excluded", file=sys.stderr)
            continue
        hardware = hardware_by_index[index]
        if hardware is None:
            # Unknown hardware: treat a thread budget literally, else one task.
            kind, amount = parse_utilization(utilization)
            slots = max(1, int(amount) // max(1, int(resources.get("threads") or 1))) if kind == "threads" else 1
            notes = ["hardware unknown"]
            hardware_summary: dict[str, Any] = {}
        else:
            slots, notes = resolve_slots(
                hardware,
                utilization,
                resources,
                allow_oversubscribe=allow_oversubscribe,
            )
            hardware_summary = {
                key: hardware.get(key)
                for key in ("os", "threads", "cores", "perf_cores", "memory_mb")
            }
        record["slots"] = slots
        record["capacity"] = {
            "hardware": hardware_summary,
            "task": dict(resources),
        }
        if not quiet:
            detail = f"capacity: {host} {format_hardware(hardware) if hardware else '?'} utilization={utilization} -> {slots} slot(s) [{format_task_resources(resources)}]"
            if notes:
                detail += "; " + "; ".join(notes)
            print(detail, file=sys.stderr)
        resolved.append(record)
    return resolved


def resolve_workers_payload(
    payload: dict[str, Any],
    resources: dict[str, Any],
    *,
    force: bool = False,
    refresh: bool = False,
    allow_oversubscribe: bool = False,
    quiet: bool = False,
) -> dict[str, Any]:
    from .workers import worker_records_from_payload, workers_payload_from_records

    records = worker_records_from_payload(payload, label="workers", allow_empty=True)
    resolved = resolve_worker_records(
        records,
        resources,
        force=force,
        refresh=refresh,
        allow_oversubscribe=allow_oversubscribe,
        quiet=quiet,
    )
    if not resolved:
        raise SystemExit("workers: every worker has zero utilization")
    return workers_payload_from_records(resolved)


# --------------------------------------------------------------------------
# Worker-side runtime: free memory gate, priority, and OOM victim order


def available_memory_mb() -> int | None:
    """Return memory available for new work on this host, or None if unknown."""
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/meminfo", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) // 1024
        except (OSError, ValueError, IndexError):
            return None
        return None
    if sys.platform == "darwin":
        import subprocess

        try:
            output = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.SubprocessError):
            return None
        page_match = re.search(r"page size of (\d+) bytes", output)
        page_size = int(page_match.group(1)) if page_match else 16384
        pages = 0
        for name in ("Pages free", "Pages inactive", "Pages speculative", "Pages purgeable"):
            match = re.search(rf"^{name}:\s+(\d+)\.", output, re.MULTILINE)
            if match:
                pages += int(match.group(1))
        return (pages * page_size) // (1024 * 1024) if pages else None
    return None


def total_memory_mb() -> int | None:
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/meminfo", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("MemTotal:"):
                        return int(line.split()[1]) // 1024
        except (OSError, ValueError, IndexError):
            return None
        return None
    try:
        return int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")) // (1024 * 1024)
    except (ValueError, OSError, AttributeError):
        return None


def memory_gate_reserve_mb(total_mb: int | None) -> int:
    if not total_mb:
        return MEMORY_GATE_MIN_RESERVE_MB
    return max(MEMORY_GATE_MIN_RESERVE_MB, int(total_mb * MEMORY_GATE_RESERVE_FRACTION))


def memory_gate_shortfall(resources: dict[str, Any]) -> tuple[int, int] | None:
    """Return ``(available, required)`` when starting a task would leave too little memory."""
    needed = int(resources.get("memory_mb") or 0)
    if needed <= 0:
        return None
    available = available_memory_mb()
    if available is None:
        return None
    required = needed + memory_gate_reserve_mb(total_memory_mb())
    if available < required:
        return available, required
    return None


OOM_SCORE_ADJ_NEW = 1000
OOM_SCORE_ADJ_FLOOR = 500
OOM_SCORE_ADJ_STEP_SECONDS = 60.0
OOM_SCORE_ADJ_STEP = 10


def is_linux() -> bool:
    return platform.system() == "Linux"


def oom_score_adj_for_age(elapsed_seconds: float) -> int:
    """Newer tasks get a higher score, so the kernel kills the least work."""
    steps = int(max(0.0, elapsed_seconds) // OOM_SCORE_ADJ_STEP_SECONDS)
    return max(OOM_SCORE_ADJ_FLOOR, OOM_SCORE_ADJ_NEW - steps * OOM_SCORE_ADJ_STEP)


def task_preexec(resources: dict[str, Any]):
    """Return a Popen preexec_fn that lowers priority and marks OOM victims."""
    fixed = resources.get("timing") != TIMING_TIMED
    linux = is_linux()

    def apply() -> None:
        if linux:
            try:
                with open("/proc/self/oom_score_adj", "w", encoding="ascii") as handle:
                    handle.write(str(OOM_SCORE_ADJ_NEW))
            except OSError:
                pass
        if not fixed:
            return
        try:
            os.nice(19)
        except OSError:
            pass
        if linux and hasattr(os, "sched_setscheduler") and hasattr(os, "SCHED_IDLE"):
            try:
                os.sched_setscheduler(0, os.SCHED_IDLE, os.sched_param(0))
            except OSError:
                pass

    return apply


def set_process_group_oom_score_adj(pgid: int, value: int) -> None:
    """Set oom_score_adj on every live process in a process group (Linux)."""
    if not is_linux():
        return
    try:
        entries = os.listdir("/proc")
    except OSError:
        return
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", encoding="ascii", errors="replace") as handle:
                stat = handle.read()
            # Fields after the parenthesised command: state ppid pgrp ...
            fields = stat.rsplit(")", 1)[1].split()
            if int(fields[2]) != pgid:
                continue
            with open(f"/proc/{entry}/oom_score_adj", "w", encoding="ascii") as handle:
                handle.write(str(value))
        except (OSError, ValueError, IndexError):
            continue
