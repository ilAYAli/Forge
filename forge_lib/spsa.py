from __future__ import annotations

import argparse
import csv
import datetime
import fcntl
import hashlib
import json
import math
import os
import random
import re
import shlex
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .core import expand_path, now, read_json, sha256_file, write_json
from .manifest import load_manifest
from .tasks import run_coordinator_cli


ALPHA = 0.602
GAMMA = 0.101
STATE_SCHEMA = "forge.spsa-state.v1"
CONFIG_SCHEMA = "forge.spsa-config.v1"
DEFAULT_NNUE_FILE = "~/code/chess/enyo/net/default.net"
SCORE_RE = re.compile(
    r"Games:\s*(\d+),\s*Wins:\s*(\d+),\s*Losses:\s*(\d+),\s*Draws:\s*(\d+)"
)


def positive_int(value: Any, *, label: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{label} must be an integer") from exc
    if result <= 0:
        raise SystemExit(f"{label} must be positive")
    return result


def finite_float(value: Any, *, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{label} must be numeric") from exc
    if not math.isfinite(result):
        raise SystemExit(f"{label} must be finite")
    return result


def parse_spsa_parameters(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SystemExit(f"could not read SPSA parameters {path}: {exc}") from exc

    parameters: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 6:
            raise SystemExit(f"{path}:{line_number}: expected six comma-separated fields")
        name = fields[0]
        if not name or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise SystemExit(f"{path}:{line_number}: invalid UCI parameter name {name!r}")
        default = finite_float(fields[1], label=f"{name} default")
        minimum = finite_float(fields[2], label=f"{name} minimum")
        maximum = finite_float(fields[3], label=f"{name} maximum")
        c_end = finite_float(fields[4], label=f"{name} c_end")
        r_end = finite_float(fields[5], label=f"{name} r_end")
        if minimum > maximum or not minimum <= default <= maximum:
            raise SystemExit(f"{path}:{line_number}: invalid bounds for {name}")
        if c_end <= 0 or r_end <= 0:
            raise SystemExit(f"{path}:{line_number}: c_end and r_end must be positive")
        if default != round(default):
            raise SystemExit(f"{path}:{line_number}: {name} default must be an integer")
        parameters.append({
            "name": name,
            "default": int(round(default)),
            "min": minimum,
            "max": maximum,
            "c_end": c_end,
            "r_end": r_end,
        })

    names = [str(parameter["name"]) for parameter in parameters]
    if not names:
        raise SystemExit(f"{path}: no SPSA parameters found")
    if len(names) != len(set(names)):
        raise SystemExit(f"{path}: duplicate SPSA parameter names")
    return parameters


def initial_theta(
    state_path: Path,
    parameters: list[dict[str, Any]],
) -> dict[str, Any]:
    names = [str(parameter["name"]) for parameter in parameters]
    defaults = [float(parameter["default"]) for parameter in parameters]
    if not state_path.exists():
        return {"k": 0, "names": names, "theta": defaults}

    state = read_json(state_path)
    k = state.get("k")
    theta = state.get("theta")
    if isinstance(k, bool) or not isinstance(k, int) or k < 0:
        raise SystemExit(f"{state_path}: invalid SPSA iteration k")
    if state.get("names") != names:
        raise SystemExit(f"{state_path}: SPSA parameter names do not match {names}")
    if not isinstance(theta, list) or len(theta) != len(parameters):
        raise SystemExit(f"{state_path}: SPSA theta does not match its parameter names")

    values: list[float] = []
    for parameter, value in zip(parameters, theta):
        parsed = finite_float(value, label=f"{parameter['name']} theta")
        if not float(parameter["min"]) <= parsed <= float(parameter["max"]):
            raise SystemExit(f"{state_path}: {parameter['name']} theta is outside its bounds")
        values.append(parsed)
    return {"k": k, "names": names, "theta": values}


def shell_value(value: Any) -> str:
    return shlex.quote(str(value))


def normalize_common_uci(items: list[str]) -> list[str]:
    options: dict[str, tuple[str, str]] = {}
    for item in items:
        name, separator, value = str(item).partition("=")
        if not separator or not name or value == "":
            raise SystemExit(f"invalid --uci option {item!r}; expected NAME=VALUE")
        options[name.lower()] = (name, value)
    return [f"{name}={value}" for name, value in options.values()]


def portable_home_path(value: str) -> str:
    expanded = os.path.expandvars(os.path.expanduser(value))
    home = str(Path.home())
    if expanded == home:
        return "~"
    if expanded.startswith(home + os.sep):
        return "~/" + expanded[len(home) + 1:]
    return value


def uci_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def spsa_uci_defaults(settings_path: Path) -> list[str]:
    options: dict[str, Any] = {}
    if settings_path.exists():
        settings = read_json(settings_path)
        configured = settings.get("uci_options")
        if configured is not None and not isinstance(configured, dict):
            raise SystemExit(f"{settings_path}: uci_options must be an object")
        options = configured or {}

    by_name = {str(name).lower(): (str(name), value) for name, value in options.items()}
    nnue_name, nnue_file = by_name.get("nnue_file", ("nnue_file", DEFAULT_NNUE_FILE))
    if not str(nnue_file):
        nnue_file = DEFAULT_NNUE_FILE
    defaults = [f"{nnue_name}={portable_home_path(str(nnue_file))}"]

    syzygy_name, use_syzygy = by_name.get("use_syzygy", ("use_syzygy", False))
    defaults.append(f"{syzygy_name}={uci_value(use_syzygy)}")
    if str(use_syzygy).lower() == "true" and "syzygypath" in by_name:
        path_name, path = by_name["syzygypath"]
        defaults.append(f"{path_name}={portable_home_path(str(path))}")
    return defaults


def prepare_spsa_template_parameters(params: dict[str, Any]) -> dict[str, Any]:
    prepared = dict(params)
    iterations = positive_int(prepared.get("iterations"), label="spsa.iterations")
    rounds = positive_int(prepared.get("rounds"), label="spsa.rounds")
    shards = positive_int(prepared.get("shards"), label="spsa.shards")
    if shards > rounds:
        raise SystemExit("spsa.shards cannot exceed rounds; every shard needs a color-swapped pair")
    for key in ("threads", "hash", "concurrency", "match_timeout"):
        positive_int(prepared.get(key), label=f"spsa.{key}")
    poll_seconds = finite_float(prepared.get("poll_seconds"), label="spsa.poll_seconds")
    if poll_seconds <= 0:
        raise SystemExit("spsa.poll_seconds must be positive")
    try:
        seed = int(prepared.get("seed"))
    except (TypeError, ValueError) as exc:
        raise SystemExit("spsa.seed must be a non-negative integer") from exc
    if seed < 0:
        raise SystemExit("spsa.seed must be a non-negative integer")
    restart = str(prepared.get("restart") or "")
    if restart not in {"on", "off"}:
        raise SystemExit("spsa.restart must be on or off")

    params_path = expand_path(str(prepared["params"]))
    state_path = expand_path(str(prepared["state"]))
    settings_path = expand_path(str(prepared["settings"]))
    parameters = parse_spsa_parameters(params_path)
    initial = initial_theta(state_path, parameters)
    start_iteration = int(initial["k"])
    common_uci = [
        *spsa_uci_defaults(settings_path),
        *list(prepared.get("uci") or []),
    ]
    prepared.update({
        "games": rounds * 2,
        "seed": seed,
        "start_iteration": start_iteration,
        "target_iteration": start_iteration + iterations,
        "uci": normalize_common_uci(common_uci),
        "parameter_definitions": parameters,
        "parameter_source": {
            "path": str(prepared["params"]),
            "sha256": sha256_file(params_path),
        },
        "initial_state": initial,
        "engine_shell": shell_value(prepared["engine"]),
        "book_shell": shell_value(prepared["book"]),
        "fastchess_shell": shell_value(prepared["fastchess"]),
        "tc_shell": shell_value(prepared["tc"]),
        "restart_shell": shell_value(restart),
    })
    return prepared


def attach_spsa_manifest_config(manifest: dict[str, Any], params: dict[str, Any]) -> None:
    shard_games = []
    for task in sorted(manifest["tasks"], key=lambda item: int(item.get("index", 0))):
        progress = task.get("progress")
        if not isinstance(progress, dict):
            raise SystemExit(f"{task['id']}: SPSA task has no progress declaration")
        shard_games.append(positive_int(progress.get("games"), label=f"{task['id']}.games"))
    manifest["spsa"] = {
        "schema": CONFIG_SCHEMA,
        "iterations": int(params["iterations"]),
        "start_iteration": int(params["start_iteration"]),
        "target_iteration": int(params["target_iteration"]),
        "rounds": int(params["rounds"]),
        "shards": int(params["shards"]),
        "shard_games": shard_games,
        "seed": int(params["seed"]),
        "state": str(params["state"]),
        "parameters": params["parameter_definitions"],
        "parameter_source": params["parameter_source"],
        "initial_state": params["initial_state"],
    }
    template_data = manifest.get("template")
    if isinstance(template_data, dict) and isinstance(template_data.get("parameters"), dict):
        for key in (
            "parameter_definitions",
            "parameter_source",
            "initial_state",
            "start_iteration",
            "target_iteration",
            "engine_shell",
            "book_shell",
            "fastchess_shell",
            "tc_shell",
            "restart_shell",
            "games",
        ):
            template_data["parameters"].pop(key, None)


def spsa_worker_slots_payload(workers_arg: str, resources: dict[str, Any] | None = None) -> dict[str, Any]:
    from .capacity import default_task_resources, resolve_workers_payload
    from .workers import workers_payload_from_arg

    payload = workers_payload_from_arg(workers_arg, default_ok=False)
    payload = resolve_workers_payload(payload, resources or default_task_resources())
    if spsa_worker_slot_count(payload) <= 0:
        raise SystemExit("SPSA workers resolve to zero task slots")
    return payload


def spsa_worker_slot_count(payload: dict[str, Any]) -> int:
    total = 0
    workers = payload.get("workers")
    if not isinstance(workers, list):
        return 0
    for worker in workers:
        if not isinstance(worker, dict):
            total += 1
            continue
        try:
            total += max(0, int(worker.get("slots", 1)))
        except (TypeError, ValueError):
            continue
    return total


def auto_spsa_shards(params: dict[str, Any], worker_payload: dict[str, Any]) -> int:
    rounds = positive_int(params.get("rounds"), label="spsa.rounds")
    slots = spsa_worker_slot_count(worker_payload)
    return min(rounds, max(1, slots))


def cap_spsa_worker_slots(payload: dict[str, Any], shards: int) -> dict[str, Any]:
    workers = payload.get("workers")
    if not isinstance(workers, list):
        raise SystemExit("SPSA workers payload has no workers")
    capacities = [
        max(0, int(worker.get("slots", 1))) if isinstance(worker, dict) else 1
        for worker in workers
    ]
    allocations = [0] * len(workers)
    remaining = min(positive_int(shards, label="spsa.shards"), sum(capacities))
    while remaining:
        changed = False
        for index, capacity in enumerate(capacities):
            if remaining == 0:
                break
            if allocations[index] >= capacity:
                continue
            allocations[index] += 1
            remaining -= 1
            changed = True
        if not changed:
            break
    capped = []
    for worker, slots in zip(workers, allocations):
        if slots <= 0:
            continue
        record = dict(worker) if isinstance(worker, dict) else {"host": str(worker)}
        record["slots"] = slots
        capped.append(record)
    return {**payload, "workers": capped}


def spsa_config(manifest: dict[str, Any]) -> dict[str, Any]:
    config = manifest.get("spsa")
    if not isinstance(config, dict) or config.get("schema") != CONFIG_SCHEMA:
        raise SystemExit("manifest does not contain a supported SPSA configuration")
    return config


def spsa_state_path(manifest_path: Path) -> Path:
    return manifest_path.parent / "spsa-state.json"


def spsa_csv_path(manifest_path: Path) -> Path:
    return manifest_path.parent / "spsa.csv"


def spsa_start_iteration(config: dict[str, Any]) -> int:
    initial = config.get("initial_state")
    fallback = int(initial.get("k", 0)) if isinstance(initial, dict) else 0
    return int(config.get("start_iteration", fallback))


def spsa_target_iteration(config: dict[str, Any]) -> int:
    # Manifests created before relative iteration counts used `iterations`
    # directly as the lifetime target.
    return int(config.get("target_iteration", config["iterations"]))


def spsa_progress_iteration(config: dict[str, Any], iteration: int) -> int:
    if "target_iteration" not in config:
        return iteration
    return iteration - spsa_start_iteration(config)


def config_digest(config: dict[str, Any]) -> str:
    stable = {
        key: config[key]
        for key in ("iterations", "rounds", "shards", "shard_games", "seed", "parameters")
    }
    if "start_iteration" in config:
        stable["start_iteration"] = spsa_start_iteration(config)
    if "target_iteration" in config:
        stable["target_iteration"] = spsa_target_iteration(config)
    encoded = json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@contextmanager
def locked_spsa_state(manifest_path: Path) -> Iterator[None]:
    lock_path = manifest_path.parent / "spsa.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def new_coordinator_state(manifest: dict[str, Any]) -> dict[str, Any]:
    config = spsa_config(manifest)
    initial = config.get("initial_state")
    if not isinstance(initial, dict):
        raise SystemExit("SPSA manifest has no initial state")
    return {
        "schema": STATE_SCHEMA,
        "run": str(manifest["name"]),
        "config_sha256": config_digest(config),
        "iterations": int(config["iterations"]),
        "start_iteration": spsa_start_iteration(config),
        "target_iteration": spsa_target_iteration(config),
        "k": int(initial["k"]),
        "names": list(initial["names"]),
        "theta": [float(value) for value in initial["theta"]],
        "current": None,
        "updated_at": now(),
    }


def load_coordinator_state(manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    path = spsa_state_path(manifest_path)
    state = read_json(path) if path.exists() else new_coordinator_state(manifest)
    if state.get("schema") != STATE_SCHEMA:
        raise SystemExit(f"{path}: unsupported SPSA state schema")
    if state.get("run") != manifest.get("name"):
        raise SystemExit(f"{path}: SPSA state belongs to another run")
    if state.get("config_sha256") != config_digest(spsa_config(manifest)):
        raise SystemExit(f"{path}: SPSA configuration changed")
    config = spsa_config(manifest)
    state.setdefault("start_iteration", spsa_start_iteration(config))
    state.setdefault("target_iteration", spsa_target_iteration(config))
    return state


def public_state(state: dict[str, Any]) -> dict[str, Any]:
    public = {
        "k": int(state["k"]),
        "iterations": int(state["iterations"]),
        "start_iteration": int(state["start_iteration"]),
        "target_iteration": int(state["target_iteration"]),
        "names": list(state["names"]),
        "theta": list(state["theta"]),
        "run": str(state["run"]),
        "updated_at": str(state["updated_at"]),
    }
    iteration_seconds = state.get("iteration_seconds_ema")
    if isinstance(iteration_seconds, (int, float)) and iteration_seconds > 0:
        public["iteration_seconds_ema"] = float(iteration_seconds)
        public["iteration_samples"] = int(state.get("iteration_samples", 0))
    return public


def save_coordinator_state(
    manifest_path: Path,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> None:
    state["updated_at"] = now()
    write_json(spsa_state_path(manifest_path), state)
    output = expand_path(str(spsa_config(manifest)["state"]))
    write_json(output, public_state(state))


def schedules(parameter: dict[str, Any], k: int, total: int) -> tuple[float, float]:
    big_a = 0.1 * total
    c = float(parameter["c_end"]) * total ** GAMMA
    a = (
        float(parameter["r_end"])
        * float(parameter["c_end"]) ** 2
        * (big_a + total) ** ALPHA
    )
    return a / (big_a + k) ** ALPHA, c / k ** GAMMA


def create_iteration(config: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    k = int(state["k"]) + 1
    total = spsa_target_iteration(config)
    rng = random.Random(f"forge-spsa:{config['seed']}:{k}")
    flips: list[int] = []
    plus: dict[str, int] = {}
    minus: dict[str, int] = {}
    for parameter, theta in zip(config["parameters"], state["theta"]):
        _a_k, c_k = schedules(parameter, k, total)
        flip = rng.choice((-1, 1))
        flips.append(flip)
        minimum = float(parameter["min"])
        maximum = float(parameter["max"])
        name = str(parameter["name"])
        plus[name] = round(min(maximum, max(minimum, float(theta) + c_k * flip)))
        minus[name] = round(min(maximum, max(minimum, float(theta) - c_k * flip)))
    return {
        "iteration": k,
        "flips": flips,
        "plus": plus,
        "minus": minus,
        "results": {},
        "created_at": now(),
        "created_epoch": time.time(),
    }


def iteration_elapsed_seconds(current: dict[str, Any]) -> float | None:
    started = current.get("created_epoch")
    if not isinstance(started, (int, float)):
        try:
            started = datetime.datetime.strptime(
                str(current.get("created_at") or ""),
                "%Y-%m-%dT%H:%M:%S%z",
            ).timestamp()
        except ValueError:
            return None
    elapsed = time.time() - float(started)
    return elapsed if elapsed > 0 else None


def update_iteration_timing(state: dict[str, Any], current: dict[str, Any]) -> None:
    elapsed = iteration_elapsed_seconds(current)
    if elapsed is None:
        return
    previous = state.get("iteration_seconds_ema")
    if isinstance(previous, (int, float)) and previous > 0:
        elapsed = float(previous) * 0.8 + elapsed * 0.2
    state["iteration_seconds_ema"] = elapsed
    state["iteration_samples"] = int(state.get("iteration_samples", 0)) + 1


def spsa_next_payload(manifest_path: Path, shard_index: int) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    config = spsa_config(manifest)
    shards = int(config["shards"])
    if not 0 <= shard_index < shards:
        raise SystemExit(f"SPSA shard index {shard_index} is outside 0..{shards - 1}")
    with locked_spsa_state(manifest_path):
        state = load_coordinator_state(manifest_path, manifest)
        if int(state["k"]) >= spsa_target_iteration(config):
            save_coordinator_state(manifest_path, manifest, state)
            return {"status": "done", **public_state(state)}
        current = state.get("current")
        if current is None:
            current = create_iteration(config, state)
            state["current"] = current
            save_coordinator_state(manifest_path, manifest, state)
        if not isinstance(current, dict):
            raise SystemExit("invalid current SPSA iteration state")
        result_key = str(shard_index)
        if result_key in current.get("results", {}):
            return {
                "status": "wait",
                "iteration": int(current["iteration"]),
                "completed": len(current["results"]),
                "shards": shards,
            }
        return {
            "status": "work",
            "iteration": int(current["iteration"]),
            "batch_iteration": spsa_progress_iteration(
                config, int(current["iteration"])
            ),
            "iterations": int(config["iterations"]),
            "plus": dict(current["plus"]),
            "minus": dict(current["minus"]),
            "seed": int(config["seed"]) + int(current["iteration"]) * shards + shard_index,
        }


def append_spsa_csv(
    manifest_path: Path,
    config: dict[str, Any],
    state: dict[str, Any],
    result: float,
    games: int,
) -> None:
    path = spsa_csv_path(manifest_path)
    names = [str(parameter["name"]) for parameter in config["parameters"]]
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as output:
            csv.writer(output).writerow(["iter", "result", "games", *names])
    with path.open("a", newline="", encoding="utf-8") as output:
        csv.writer(output).writerow([
            int(state["k"]),
            f"{result:+.6f}",
            games,
            *[f"{float(value):.6f}" for value in state["theta"]],
        ])


def advance_iteration(
    manifest_path: Path,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    config = spsa_config(manifest)
    current = state["current"]
    results = current["results"]
    ordered = [results[str(index)] for index in range(int(config["shards"]))]
    games = sum(int(result["games"]) for result in ordered)
    wins = sum(int(result["wins"]) for result in ordered)
    losses = sum(int(result["losses"]) for result in ordered)
    draws = sum(int(result["draws"]) for result in ordered)
    result = (wins - losses) / games
    iteration = int(current["iteration"])
    theta = list(state["theta"])
    for index, (parameter, flip) in enumerate(zip(config["parameters"], current["flips"])):
        a_k, c_k = schedules(parameter, iteration, spsa_target_iteration(config))
        value = float(theta[index]) + (a_k / c_k) * int(flip) * result
        theta[index] = min(float(parameter["max"]), max(float(parameter["min"]), value))
    state["theta"] = theta
    state["k"] = iteration
    update_iteration_timing(state, current)
    state["current"] = None
    save_coordinator_state(manifest_path, manifest, state)
    append_spsa_csv(manifest_path, config, state, result, games)
    return {
        "iteration": iteration,
        "batch_iteration": spsa_progress_iteration(config, iteration),
        "iterations": int(config["iterations"]),
        "games": games,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "result": result,
    }


def spsa_submit_payload(
    manifest_path: Path,
    shard_index: int,
    iteration: int,
    wins: int,
    losses: int,
    draws: int,
    games: int,
) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    config = spsa_config(manifest)
    shards = int(config["shards"])
    if not 0 <= shard_index < shards:
        raise SystemExit(f"SPSA shard index {shard_index} is outside 0..{shards - 1}")
    expected_games = int(config["shard_games"][shard_index])
    if games != expected_games or wins < 0 or losses < 0 or draws < 0:
        raise SystemExit(f"SPSA shard {shard_index} expected {expected_games} valid games")
    if wins + losses + draws != games:
        raise SystemExit("SPSA W/L/D does not add up to games")

    result = {"wins": wins, "losses": losses, "draws": draws, "games": games}
    with locked_spsa_state(manifest_path):
        state = load_coordinator_state(manifest_path, manifest)
        current = state.get("current")
        if not isinstance(current, dict):
            if iteration <= int(state["k"]):
                return {"status": "duplicate", "iteration": iteration}
            raise SystemExit("SPSA has no active iteration")
        if int(current["iteration"]) != iteration:
            if iteration < int(current["iteration"]):
                return {"status": "duplicate", "iteration": iteration}
            raise SystemExit(
                f"SPSA submission is for iteration {iteration}, active iteration is {current['iteration']}"
            )
        key = str(shard_index)
        existing = current["results"].get(key)
        if existing is not None:
            if existing != result:
                raise SystemExit(f"SPSA shard {shard_index} submitted conflicting results")
            return {"status": "duplicate", "iteration": iteration}
        current["results"][key] = result
        if len(current["results"]) == shards:
            summary = advance_iteration(manifest_path, manifest, state)
            return {"status": "advanced", **summary}
        save_coordinator_state(manifest_path, manifest, state)
        return {
            "status": "accepted",
            "iteration": iteration,
            "completed": len(current["results"]),
            "shards": shards,
        }


def cmd_spsa_next(args: argparse.Namespace) -> int:
    payload = spsa_next_payload(expand_path(args.manifest), int(args.shard_index))
    print(json.dumps(payload, sort_keys=True))
    return 0


def cmd_spsa_submit(args: argparse.Namespace) -> int:
    payload = spsa_submit_payload(
        expand_path(args.manifest),
        int(args.shard_index),
        int(args.iteration),
        int(args.wins),
        int(args.losses),
        int(args.draws),
        int(args.games),
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


def coordinator_json(coordinator: str, command: list[str]) -> dict[str, Any]:
    result = run_coordinator_cli(coordinator, command)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "SPSA coordinator request failed"
        raise RuntimeError(detail)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid SPSA coordinator response: {result.stdout.strip()}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("invalid SPSA coordinator response")
    return payload


def expand_uci_value(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(value)) if value.startswith(("~", "$")) else value


def parse_common_uci(items: list[str]) -> dict[str, str]:
    options: dict[str, str] = {}
    for item in items:
        name, separator, value = item.partition("=")
        if not separator or not name or value == "":
            raise SystemExit(f"invalid --uci option {item!r}; expected NAME=VALUE")
        options[name] = expand_uci_value(value)
    return options


def fastchess_engine_args(name: str, engine: str, options: dict[str, Any]) -> list[str]:
    args = [f"cmd={engine}", f"name={name}"]
    args.extend(f"option.{option}={value}" for option, value in options.items())
    return args


def run_spsa_match(args: argparse.Namespace, assignment: dict[str, Any]) -> dict[str, int]:
    games = int(args.games)
    if games <= 0 or games % 2:
        raise SystemExit("SPSA worker games must be a positive even number")
    engine = os.path.expandvars(os.path.expanduser(args.engine))
    book = os.path.expandvars(os.path.expanduser(args.book))
    fastchess = os.path.expandvars(os.path.expanduser(args.fastchess))
    common = parse_common_uci(list(args.uci))
    plus = dict(common)
    plus.update(assignment["plus"])
    minus = dict(common)
    minus.update(assignment["minus"])
    command = [
        fastchess,
        "-engine", *fastchess_engine_args("plus", engine, plus),
        "-engine", *fastchess_engine_args("minus", engine, minus),
        "-each", f"tc={args.tc}", f"restart={args.restart}",
        f"option.Threads={args.threads}", f"option.Hash={args.hash}",
        "-openings", f"file={book}", "format=epd", "order=random",
        "-srand", str(assignment["seed"]),
        "-rounds", str(games // 2),
        "-repeat",
        "-concurrency", str(args.concurrency),
        "-recover",
        "-ratinginterval", "0",
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=args.match_timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"fastchess timed out after {args.match_timeout}s") from exc
    scores = SCORE_RE.findall(completed.stdout)
    if completed.returncode != 0 or not scores:
        output = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
        tail = "\n".join(output.splitlines()[-40:])
        raise RuntimeError(
            f"fastchess failed rc={completed.returncode}; could not parse exact match score\n{tail}"
        )
    reported_games, wins, losses, draws = map(int, scores[-1])
    if reported_games != games or wins + losses + draws != games:
        raise RuntimeError(
            f"fastchess reported {reported_games} games ({wins}/{losses}/{draws}), expected {games}"
        )
    return {"games": games, "wins": wins, "losses": losses, "draws": draws}


def cmd_spsa_worker(args: argparse.Namespace) -> int:
    while True:
        assignment = coordinator_json(args.coordinator, [
            "spsa-next",
            "--manifest", "{manifest}",
            "--shard-index", str(args.shard_index),
        ])
        status = assignment.get("status")
        if status == "done":
            write_json(expand_path(args.output), assignment)
            completed = int(assignment["k"]) - int(assignment["start_iteration"])
            print(
                f"SPSA complete {completed}/{assignment['iterations']} "
                f"total={assignment['k']} shard={args.shard_index}",
                flush=True,
            )
            return 0
        if status == "wait":
            time.sleep(args.poll_seconds)
            continue
        if status != "work":
            raise SystemExit(f"unexpected SPSA coordinator status {status!r}")

        result = run_spsa_match(args, assignment)
        response = coordinator_json(args.coordinator, [
            "spsa-submit",
            "--manifest", "{manifest}",
            "--shard-index", str(args.shard_index),
            "--iteration", str(assignment["iteration"]),
            "--wins", str(result["wins"]),
            "--losses", str(result["losses"]),
            "--draws", str(result["draws"]),
            "--games", str(result["games"]),
        ])
        score = (result["wins"] - result["losses"]) / result["games"]
        print(
            f"SPSA iteration {assignment['batch_iteration']}/{assignment['iterations']} "
            f"total={assignment['iteration']} "
            f"shard={args.shard_index} games={result['games']} "
            f"result={score:+.3f} status={response['status']}",
            flush=True,
        )
