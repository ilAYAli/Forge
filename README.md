<img width="1522" height="935" alt="image" src="https://github.com/user-attachments/assets/4ea8d4a7-7abc-46a9-89b9-45338520529a" />
<p align="center">
    <sub>Forge running two distributed tasks: labeling and SPRT.</sub>
</p>

# Forge

Forge distributes manifest-defined work across workers you control.

It keeps each job in `runs/<name>/` with the materialized manifest, workers
file, logs, state files, declared outputs, and coordinator metadata. Workers
connect out through SSH/rsync; no daemon or database is required.

## Install

```sh
git clone git@github.com:ilAYAli/Forge.git ~/code/cpp/chess/forge
ln -s ~/code/cpp/chess/forge/forge.py ~/.local/bin/forge
```


example config:
~/.config/forge/config.yml
```
coordinator_host: "worker1"
notify_command: "/home/petter/code/cpp/chess/forge/scripts/forge_event_ntfy.sh"
template_dirs:
  - "~/.config/forge/templates"
```

default workers:
The name of the respective workers should likely be a hostname rather than worker1..workerN

~/.config/forge/workers.json
```
{
  "schema": "forge.workers.v1",
  "workers": [
    {
      "host": "worker1",
      "profile": "worker1",
      "slots": 11
    },
    {
      "host": "worker2",
      "profile": "worker2",
      "python": "/opt/homebrew/bin/python3",
      "slots": 5
    },
    {
      "host": "worker3",
      "profile": "linux",
      "slots": 8
    }
  ]
}
```

## Basic Commands

```sh
forge --help
forge "cd ~/repo; git pull; cmake --build build"
forge sync ~/code/cpp/chess/enyo/net/default.net
forge sync /tmp/foo ~/bar
forge sync "$PWD/.codex/agents" ~/code/cpp/chess/nnue/
forge start manifest.json --workers worker1,worker2:4
forge stop my-run
forge resume my-run
forge retry
forge worker set worker3:4
forge del --worker worker3
forge status
watch 'forge status --verbose'
forge update
```

Use `forge <command> --help` for full options.

`forge sync SOURCE [DESTINATION]` hashes the local file or directory contents and
synchronizes them to each selected worker host. When `DESTINATION` is omitted,
Forge uses the equivalent source path on each worker; paths beneath the
coordinator's home use each worker's own `~/`. A trailing `/` on a directory
destination places the source directory beneath it; the example above writes to
`~/code/cpp/chess/nnue/agents/`. Files whose destination already has the matching
size and SHA-256 are skipped. Extra destination files are not deleted.

## One-Shot Shell Commands

Run a shell command once selected workers:

```sh
forge hostname --workers worker1,worker3,worker2
```
or multiple commands at once using the configured workers:
```
forge '
    cd ~/path/to/forge
    git pull
    cmake --build build
'
```

`sh` waits, prints grouped output per host, and does not create run history.
Each host gets one slot unless `--workers` says otherwise.
The `sh` prefix is optional when the complete first argument does not exactly
match a Forge command or installed template name. Forge does not split a quoted
command, so `forge "sprt foo bar"` runs that shell command while
`forge sprt ...` selects the `sprt` template.

```sh
forge sh --workers worker1,worker3,worker2 "hostname; git --version"
```

## Start Work

```sh
forge start manifest.json --workers worker1,worker2:4
forge start manifest.json --workers workers.json --run my-run --wait
forge start manifest.json --workers worker1,worker2:4 --run my-run
forge start manifest.json --workers workers.json --merge merged.txt
forge start manifest.json --workers workers.json --replace
```

`start` materializes `runs/<run>/manifest.json` and `workers.json`, syncs inputs,
checks prerequisites, launches workers, and optionally waits/merges. If another
run is active, the new run is queued and shown in status/web.
Worker setup runs in parallel and waits for each host's real success, failure,
or `--remote-timeout-seconds`; a faster host never cancels slower setup.

## Templates

Templates expand to normal manifests and then start.

```sh
forge template list
forge template show sprt

forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --comment "quick smoke"
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers worker1:8,worker3:5,worker2:4
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers worker1:8,worker3:5,worker2:4 --shards 136
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --candidate-uci nnue_file=~/code/cpp/chess/enyo/net/candidate.nn \
  --reference-uci nnue_file=~/code/cpp/chess/enyo/net/reference.nn
```

Notes:

- `forge sprt ...` is shorthand for `forge run sprt ...`.
- Forge consumes orchestration flags: `--workers`, `--jobs`, `--wait`,
  `--notify-command`, `--comment`.
- SPRT flags are passed through. Use `--` only if a flag name conflicts.
- Missing SPRT flags come from template defaults.
- The opening book and `nnue_file` UCI options are hashed, synchronized, and
  verified on every selected worker. Engine executables are not copied between
  platforms; `enyo_<commit>` engines are built and deployed locally when a
  worker does not already have a compatible executable.
- Pinned SPRT and fastchess revisions use commit-specific cache directories, so
  updating a working checkout cannot alter or stop an active run.
- `worker2:4` means 4 Forge worker slots on `worker2`.
- `--concurrency` and `--threads` affect SPRT/engine work inside each shard.
- Candidate and reference engines default to `--hash 128` MiB per process.
  Override it with `--hash MIB` or an engine-specific `--candidate-uci Hash=MIB`
  or `--reference-uci Hash=MIB` option.
- Without `--shards`, Forge targets roughly 25 games per shard, then rounds
  up to a whole multiple of the configured worker slots with at least two
  complete worker waves. Very small runs are capped to preserve at least two
  games per shard. Slow worker startup does not affect this calculation;
  explicit `--shards` still overrides it.

### Distributed SPSA (“Simultaneous Perturbation Stochastic Approximation.”)

`spsa` runs exact fixed-game matches rather than SPRTs. One long-lived Forge
task owns each shard; all shards synchronize after every iteration, the
coordinator aggregates W/L/D, updates theta, and atomically checkpoints the
state. Interrupted or retried shards rejoin the active iteration without
submitting a second gradient result.

```sh
forge spsa
```

This starts a detached 1,000-iteration batch from the theta in the default
state file and reports progress in `forge web`. The default worker pool and an
automatic run name are used. Pass `--wait` only when foreground waiting is
useful. SPSA progress and ETA follow completed coordinator barriers rather than
summing the synchronized shard counters.

| SPSA option | Default | Purpose |
| --- | --- | --- |
| `--engine` | `~/assets/engines/candidate` | Engine used for both perturbations |
| `--params` | `~/code/cpp/chess/enyo/spsa/params.txt` | Parameter ranges and schedules |
| `--state` | `~/code/cpp/chess/enyo/spsa/state.json` | Input theta and atomic checkpoints |
| `--settings` | `~/.config/enyo/settings.json` | Source of NNUE and Syzygy defaults |
| `--iterations` | `1000` | New iterations added to the loaded state |
| `--rounds` | `14` | Paired rounds per iteration; 28 games |
| `--shards` | available slots, capped at `--rounds` | Distributed match shards |
| `--concurrency` | `1` | Concurrent games inside each shard |
| `--threads` | `1` | Engine threads per game |
| `--hash` | `64` | Engine hash MiB per game |
| `--tc` | `5+0.05` | Fastchess time control |
| `--restart` | `on` | Restart engines between games |
| `--book` | WOMP `+140_+169` EPD | Opening book |
| `--fastchess` | Forge-pinned fastchess | Match executable |
| `--uci` | values resolved from `--settings` | Common UCI options; repeat to override |
| `--seed` | `1` | Perturbation and opening seed |
| `--match-timeout` | `3600` | Per-shard match timeout in seconds |
| `--poll-seconds` | `1.0` | Iteration-barrier polling interval |

Forge reads `nnue_file`, `use_syzygy`, and an enabled `SyzygyPath` from the
configured Enyo settings. If settings are absent, it uses `net/default.net` and
disables Syzygy. The resolved NNUE is hashed and synchronized to every worker.
The state file contains the last fully aggregated theta and is compatible with
Enyo's `spsa/promote.py`. Each new `forge spsa` invocation adds another batch.
To resume an interrupted materialized run without creating a new batch, use
its run name:

```sh
forge spsa --run <run-name> --resume
```

Without `--shards`, Forge uses the available worker slots capped at `--rounds`,
because each shard must own at least one color-swapped game pair. Remove or
rename a completed state file before intentionally starting a fresh tuning run.

### Distributed labeling

`label` streams LC0 V6 gzip records directly through Stockfish into a
merged BulletFormat dataset. The default run uses 152 shards across
`worker1:20,worker2:8,worker3:10`.

```sh
forge label \
  --source ~/code/cpp/chess/nnue/data/lc0/test91/training-run2-test91-20260128-1817 \
  --output ~/code/cpp/chess/nnue/data/labeled/test91-depth12.bullet \
  --workers worker1:20,worker2:8,worker3:10,worker4:8,worker5:8 \
  --run test91-depth12 \
  --comment "9M raw LC0 records, quiet positions from ply 16"
```

Add or restore worker slots on an incomplete run without relaunching it:

```sh
forge worker set worker5:8 test91-depth12 --remote-timeout-seconds 1800
```

The first setup on a host may appear idle while its corpus cache is transferred
and verified. Add `--verbose` to show setup progress. Existing workers and
completed shards remain untouched. The `worker5` profile stores this many-file
cache on native WSL ext4 under `/home/petter/.cache/forge`; putting it under
`/mnt/c` or `/mnt/d` makes verification prohibitively slow.

The corpus is inventoried by relative path, size, and SHA-256 under
`~/.cache/forge/inputs/`. Completed shards resume from their done markers;
the detached wait monitor validates every shard and atomically merges them
before publishing success. `--replace` is required to replace either the named
run or final dataset.

Default workers are stored in `~/.config/forge/workers.json`:

```json
{
  "schema": "forge.workers.v1",
  "workers": [
    {"host": "worker1", "profile": "linux", "slots": 8},
    {"host": "worker2", "profile": "mac", "python": "/opt/homebrew/bin/python3", "slots": 4}
  ]
}
```

Other defaults remain in `~/.config/forge/config.yml`:

```yaml
coordinator_host: "worker1"
notify_command: "/path/to/notify-script"
speedup.single_host_workers: 5
template_dirs:
  - "~/.config/forge/templates"
```

Forge reports distributed speedup against a configurable single-host
baseline:

```text
single-host throughput = speedup.single_host_workers × fastest active task rate
speedup = total active task rate ÷ single-host throughput
```

The default models one host as five workers. For example, 40 equally fast
active workers report `8.0x`. Set `speedup.single_host_workers` to the worker
count that should represent one host.

## Status

```sh
forge status
forge status --verbose
forge status my-run --verbose
forge status --failed
forge status --done
```

Default status shows active and queued runs. `--verbose` adds pending, failed,
diagnostics, and more detail.

If a worker should read status from the coordinator by default, put a
coordinator pointer in `runs/coordinator`:

```sh
mkdir -p runs
printf '%s\n' 'coordinator.example.net:/srv/forge/runs' > runs/coordinator
```

`FORGE_RUNS_DIR` changes the local run root. `FORGE_COORDINATOR_RUNS`
overrides `runs/coordinator`.

## Stop And Resume

```sh
forge stop my-run
forge resume my-run
```

`stop` prevents new claims, terminates every local and remote worker slot,
releases incomplete active claims, and removes their partial outputs and
transient state. Completed tasks and the materialized run are preserved.
`resume` restarts incomplete non-failed work using `<run>/workers.json` when
present, otherwise `~/.config/forge/workers.json`.
Use `retry` for failed tasks.

Ctrl+C during setup or `--wait` performs the same cleanup before returning
status 130. Coordinator temporary files are removed, and the stopped run can be
continued with `forge resume <run>` without duplicate output.

Set worker slots for an incomplete run:

```sh
forge worker set worker3:4 my-run
```

`worker set worker3:4` seeds `<run>/workers.json` from the configured defaults
when absent, then updates or adds only `worker3`. Missing workers are started;
existing workers keep running.

Drain a worker from a run:

```sh
forge del --worker worker3
forge del --worker worker3 my-run
forge worker del worker3 my-run
```

`del --worker` drains the host: no new claims, reserved claims released, running
tasks left alone. Add `--keep-claims` to keep already reserved claims. Without
`--worker`, `del` deletes a run.

## Retry Failed Work

```sh
forge retry
forge retry my-run
forge retry my-run --workers worker3,worker4:4
forge retry my-run --workers alternate-workers.json
```

`retry` requeues failed tasks and skips valid completed outputs.

If a dead worker left live claims behind, release that worker before retrying:

```sh
forge release --manifest runs/my-run/manifest.json --worker worker-name
```

## Workers

Inline workers are comma-separated. `:N` sets slots for that invocation. `:0`
omits a host.

```sh
forge start manifest.json --workers worker1,worker2
forge start manifest.json --workers worker1:8,worker2:4
forge start manifest.json --workers worker1=linux:8,worker2=mac:4
forge start manifest.json --workers worker1:0,worker2
```

Workers files as JSON:

```json
{
  "schema": "forge.workers.v1",
  "workers": [
    "worker1",
    {
      "host": "worker2",
      "profile": "linux"
    },
    {
      "host": "worker3",
      "profile": "mac",
      "python": "/opt/homebrew/bin/python3",
      "slots": 2
    },
    {
      "host": "worker4",
      "enabled": false
    }
  ]
}
```

Object workers support `host`, `profile`, `python`, `local`, `cache_dir`,
`slots`, and `enabled`. Inline entries inherit matching worker-file defaults.

## Manifests

A manifest declares prerequisites, path mappings, tasks, progress parsing, and
outputs. `forge start` materializes the template into `runs/<run>/manifest.json`
before work starts.

Minimal setup-only manifest:

```json
{
  "schema": "forge.task.v1",
  "name": "project-deploy",
  "work_dir": "~/work/project",
  "requirements": {
    "python_min": "3.11",
    "git_min": "2.38",
    "repos": [
      {
        "name": "project",
        "url": "git@example.com:owner/project.git",
        "path": "~/work/project",
        "remote": "origin",
        "commit": "REPLACE_WITH_COMMIT_SHA",
        "build": [
          "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release",
          "cmake --build build -j8"
        ],
        "probe": [
          "test -x build/project"
        ]
      }
    ]
  },
  "tasks": []
}
```

If `tasks` is empty, `start` verifies setup and stops. If workers use different
paths, add `path_maps` and use matching worker profiles.

For command requirements, use `requirements.commands`. For tools built from Git,
use `requirements.repos` with a pinned `commit` plus a `probe` that proves the
needed runtime behavior. Failed probes stop work before any task is assigned.

## Planning Tasks

`plan` is a manifest generator. It is useful, but it is not required if another
tool already writes manifests.

```sh
forge plan \
  --name batch-transform \
  --description 'batch transform' \
  --task-label transform \
  --shards 24 \
  --work-dir ~/work/project \
  --manifest manifest.json \
  --command 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '{output_dir}/shard.{index}.txt'
```

Add prerequisites without hand-editing JSON:

```sh
forge add-repo \
  --manifest manifest.json \
  --name tool \
  --path ~/work/tool \
  --url git@example.com:owner/tool.git \
  --remote origin \
  --commit REPLACE_WITH_COMMIT_SHA \
  --probe '~/work/tool/tool --help | grep -- "--needed-option"'

forge add-command \
  --manifest manifest.json \
  --name external-tool \
  --path ~/.local/bin/external-tool \
  --command '~/.local/bin/external-tool --version' \
  --contains 'External Tool'
```

Use `--task-label` so status can show readable task names. Use progress regex
options when long-running tools emit parseable progress.

## Notifications

`run`, `start`, `deploy`, and `wait` support `--notify-command`,
`FORGE_NOTIFY_COMMAND`, or `notify_command` in
`~/.config/forge/config.yml`. The command receives one JSON payload on stdin
and in `FORGE_EVENT_JSON`. Forge does not assume any notification service.

For long jobs, make each task write declared log outputs, add a task label, add
progress parsing when possible, and pass a notification command so completion or
failure is reported without polling.

When `wait` sees a terminal state, Forge writes `<run>/summary.json` with the
same completion payload sent to notifications, including the final status line.
Detached starts run a background wait monitor so completed runs still get that
summary even when no notification command is configured.

## Updates

`update` uses `~/.config/forge/workers.json` by default.
It updates workers to the coordinator's exact current Forge commit.

```sh
forge update
forge update worker1,worker2:4
forge update workers.json
forge update --all
```

With `--all`, the coordinator fast-forwards first, then workers are matched to
the new coordinator commit.

## Advanced Commands

Lower-level commands remain available for diagnostics and automation:

```sh
forge deploy workers.json manifest.json
forge wait --manifest runs/my-run/manifest.json
forge verify my-run
forge merge my-run --output merged.txt
forge stop my-run
forge doctor --manifest runs/my-run/manifest.json
forge sync-repos --manifest runs/my-run/manifest.json --profile worker1
forge work --coordinator host:/path/to/manifest.json --profile worker1
forge run-task --manifest runs/my-run/manifest.json --index 0
```

`deploy` materializes and starts work. `start` is the normal command because it
also wires together wait, verify, and merge.

## Failure Model

- A task is complete only when its `.done.json` and declared outputs exist.
- Output checksums are verified before merge.
- Failed tasks are not merged.
- Active claims prevent duplicate work.
- Stale claims can be reclaimed after their lease expires.
- Wrong Git commits, missing commands, or mismatched input checksums fail before
  a task command starts.
- Ctrl+C propagates through the coordinator, kills every detached worker
  process group, resets interrupted tasks to pending, and removes temporary and
  partial files while preserving completed tasks for resume.

## Examples

Example manifests live under `examples/`. They are samples only; Forge's task
model is domain-agnostic.
