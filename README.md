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

## Basic Commands

```sh
forge --help
forge "cd ~/repo; git pull; cmake --build build"
forge start manifest.json --workers worker-a,worker-b:4
forge stop my-run
forge resume my-run
forge retry
forge worker set worker-c:4
forge del --worker worker-c
forge status
watch 'forge status --verbose'
forge update
```

Use `forge <command> --help` for full options.

## One-Shot Shell Commands

Run the same shell command once on each selected host:

```sh
forge "pwd"
forge sh "cd ~/code/cpp/chess/enyo; git switch main; git pull; cmake --build build && ./scripts/deploy.sh --candidate"
```

`sh` waits, prints grouped output per host, and does not create run history.
Each host gets one slot unless `--workers` says otherwise.
The `sh` prefix is optional when the complete first argument does not exactly
match a Forge command or installed template name. Forge does not split a quoted
command, so `forge "sprt foo bar"` runs that shell command while
`forge sprt ...` selects the `sprt` template.

```sh
forge sh --workers pwa-5090,pwa-mbp0,pwa-mbp "hostname; git --version"
```

## Start Work

```sh
forge start manifest.json --workers worker-a,worker-b:4
forge start manifest.json --workers workers.json --run my-run --wait
forge start manifest.json --workers worker-a,worker-b:4 --run my-run
forge start manifest.json --workers workers.json --merge merged.txt
forge start manifest.json --workers workers.json --replace
```

`start` materializes `runs/<run>/manifest.json` and `workers.json`, syncs inputs,
checks prerequisites, launches workers, and optionally waits/merges. If another
run is active, the new run is queued and shown in status/web.

## Templates

Templates expand to normal manifests and then start.

```sh
forge template list
forge template show sprt

forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --comment "quick smoke"
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers pwa-5090:8,pwa-mbp0:5,pwa-mbp:4
forge sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers pwa-5090:8,pwa-mbp0:5,pwa-mbp:4 --shards 136
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
- `pwa-mbp:4` means 4 Forge worker slots on `pwa-mbp`.
- `--concurrency` and `--threads` affect SPRT/engine work inside each shard.
- Without `--shards`, Forge chooses 8 shards per worker slot, capped by
  `--games`.

### Distributed labeling

`label` streams LC0 V6 gzip records directly through Stockfish into a
merged BulletFormat dataset. The default run uses 152 shards across
`pwa-5090:20,pwa-mbp:8,pwa-mbp0:10`.

```sh
forge label \
  --source ~/code/cpp/chess/nnue/data/lc0/test91/training-run2-test91-20260128-1817 \
  --output ~/code/cpp/chess/nnue/data/labeled/test91-depth12.bullet \
  --workers pwa-5090:20,pwa-mbp:8,pwa-mbp0:10,pwa-rce:8,pwa-wsl:8 \
  --run test91-depth12 \
  --comment "9M raw LC0 records, quiet positions from ply 16"
```

Add or restore worker slots on an incomplete run without relaunching it:

```sh
forge worker set pwa-wsl:8 test91-depth12 --remote-timeout-seconds 1800
```

The first setup on a host may appear idle while its corpus cache is transferred
and verified. Add `--verbose` to show setup progress. Existing workers and
completed shards remain untouched. The `pwa-wsl` profile stores this many-file
cache on native WSL ext4 under `/home/petter/.cache/forge`; putting it under
`/mnt/c` or `/mnt/d` makes verification prohibitively slow.

The corpus is inventoried by relative path, size, and SHA-256 under
`~/.cache/forge/inputs/`. Completed shards resume from their done markers;
the detached wait monitor validates every shard and atomically merges them
before publishing success. `--replace` is required to replace either the named
run or final dataset.

Default workers can be configured in `~/.config/forge/config.yml`:

```yaml
default_workers: "pwa-5090:8,pwa-mbp0:5,pwa-mbp:4"
coordinator_host: "pwa-5090"
notify_command: "/path/to/notify-script"
template_dirs:
  - "~/.config/forge/templates"
```

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

`stop` prevents new claims and releases incomplete active claims.
`resume` restarts incomplete non-failed work from the materialized run.
Use `retry` for failed tasks.

Set worker slots for an incomplete run:

```sh
forge worker set worker-c:4 my-run
```

`worker set worker-c:4` sets and enforces four total slots. Missing workers are
started; existing workers keep running.

Drain a worker from a run:

```sh
forge del --worker worker-c
forge del --worker worker-c my-run
forge worker del worker-c my-run
```

`del --worker` drains the host: no new claims, reserved claims released, running
tasks left alone. Add `--keep-claims` to keep already reserved claims. Without
`--worker`, `del` deletes a run.

## Retry Failed Work

```sh
forge retry
forge retry my-run
forge retry my-run --workers worker-c,worker-d:4
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
forge start manifest.json --workers worker-a,worker-b
forge start manifest.json --workers worker-a:8,worker-b:4
forge start manifest.json --workers worker-a=linux:8,worker-b=mac:4
forge start manifest.json --workers worker-a:0,worker-b
```

Workers files as JSON:

```json
{
  "schema": "forge.workers.v1",
  "workers": [
    "worker-a",
    {
      "host": "worker-b",
      "profile": "linux"
    },
    {
      "host": "worker-c",
      "profile": "mac",
      "python": "/opt/homebrew/bin/python3",
      "slots": 2
    },
    {
      "host": "worker-d",
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

`update` uses `runs/workers.json` by default, falling back to `./workers.json`.
It updates workers to the coordinator's exact current Forge commit.

```sh
forge update
forge update worker-a,worker-b:4
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
forge sync-repos --manifest runs/my-run/manifest.json --profile worker-a
forge work --coordinator host:/path/to/manifest.json --profile worker-a
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
- Ctrl+C in `deploy` signals active SSH/rsync commands and wraps remote commands
  so worker child processes are also terminated.

## Examples

Example manifests live under `examples/`. They are samples only; Forge's task
model is domain-agnostic.
