# Crucible

Crucible distributes manifest-defined work across workers you control.

It keeps each job in `runs/<name>/` with the materialized manifest, workers
file, logs, state files, declared outputs, and coordinator metadata. Workers
connect out through SSH/rsync; no daemon or database is required.

## Basic Commands

```sh
crucible --help
crucible start manifest.json --workers worker-a,worker-b:4
crucible stop my-run
crucible resume my-run
crucible retry
crucible add --worker worker-c:4
crucible del --worker worker-c
crucible status
watch 'crucible status --verbose'
crucible update
```

Use `crucible start --help`, `crucible add --help`, `crucible del --help`,
`crucible stop --help`, `crucible resume --help`, `crucible retry --help`,
`crucible worker --help`, and `crucible status --help` for normal workflow
options.

## Start Work

Start a manifest on a short inline worker list:

```sh
crucible start manifest.json --workers worker-a,worker-b:4
```

Start with a workers file when workers need defaults for profiles, custom
Python paths, local execution, cache directories, disabled entries, or slots:

```sh
crucible start manifest.json --workers workers.json --run my-run --wait
```

`start` creates `runs/<run>/`, writes `manifest.json` and `workers.json` there,
syncs the runner and declared inputs to workers, verifies prerequisites, starts
worker loops, verifies declared outputs, and optionally waits or merges output.
It returns non-zero on setup, task, wait, verification, or merge failure.
If some workers fail setup but at least one slot is ready, task launch continues
on the ready slots and reports the skipped setup failures.
If another run is already using workers, `start` materializes the new run as
queued, shows it in status and web, and starts it when older active or queued
runs clear.

Useful options:

```sh
crucible start manifest.json --workers worker-a,worker-b:4 --run my-run
crucible start manifest.json --workers workers.json --wait
crucible start manifest.json --workers workers.json --merge merged.txt
crucible start manifest.json --workers workers.json --replace
```

## Templates

`run` expands a named template into a normal Crucible manifest, then starts it.
Templates are loaded from `.crucible/templates`, `~/.config/crucible/templates`,
and the built-in `templates/` directory.

```sh
crucible template list
crucible template show sprt
crucible sprt --candidate ~/assets/engines/enyo_9696dac --games 1000
crucible sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers pwa-5090:8,pwa-mbp0:5,pwa-mbp:4,pwa-wsl:3,pwa-rce:3
crucible sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --workers pwa-5090:8,pwa-mbp0:5,pwa-mbp:4,pwa-wsl:3,pwa-rce:3 --shards 184
crucible sprt --candidate ~/assets/engines/enyo_9696dac --games 1000 \
  --candidate-uci nnue_file=~/code/cpp/chess/enyo/net/candidate.nn \
  --reference-uci nnue_file=~/code/cpp/chess/enyo/net/reference.nn
crucible sprt --workers pwa-mbp:4 -- --candidate ~/assets/engines/enyo_9696dac --games 1000
```

`crucible sprt ...` is shorthand for `crucible run sprt ...`.
Crucible consumes only run orchestration flags such as `--workers`, `--jobs`,
`--wait`, and `--notify-command`; SPRT flags can be passed directly, with `--`
available as an optional separator when a flag name conflicts. Template defaults
such as `--reference`, `--book`, `--tc`, `--threads`, and `--concurrency` are
added to the underlying `sprt` command when they are not supplied.

`sprt` derives the Enyo candidate commit from the executable name
(`enyo_9696dac` -> `9696dac`). Before games start, the materialized manifest
syncs declared Git requirements, checks out pinned commits, builds and installs
the candidate/tooling, probes the engines, and syncs the opening book input.
Use `--candidate-uci` and `--reference-uci` for net-vs-net runs where both
sides use the same engine binary but different NNUE files. These flags are
repeatable and are passed through to the underlying `sprt` command.

For `crucible sprt`, inline worker counts are worker slots. `pwa-mbp:4`
launches 4 SPRT shards on `pwa-mbp`. Use `:0` to disable a host.
`--concurrency` and `--threads` are passed to `sprt` and affect the engine work
inside each shard; they do not change the number of Crucible worker slots.
If `--shards` is omitted, Crucible chooses 8 shards per worker slot,
capped by the number of games. Pass `--shards N` to override it.

Default workers can be configured in `~/.config/crucible/config.yml`:

```yaml
default_workers: "pwa-5090:8,pwa-mbp0:5,pwa-mbp:4,pwa-wsl:3,pwa-rce:3"
coordinator_host: "pwa-5090"
notify_command: "/path/to/notify-script"
template_dirs:
  - "~/.config/crucible/templates"
```

## Status

`status` without a run name shows active and explicitly queued runs. Failed
inactive runs are collapsed near the top so they do not hide active work.

```sh
crucible status
crucible status --verbose
crucible status my-run --verbose
crucible status --failed
crucible status --done
```

Default status keeps active task rows visible. `--verbose` adds pending and
failed task sections, diagnostics, and more detail. `--show-done` adds completed
task breakdowns by worker.

If a worker should read status from the coordinator by default, put a
coordinator pointer in `runs/coordinator`:

```sh
mkdir -p runs
printf '%s\n' 'coordinator.example.net:/srv/crucible/runs' > runs/coordinator
```

`CRUCIBLE_RUNS_DIR` changes the local run root. `CRUCIBLE_COORDINATOR_RUNS`
overrides `runs/coordinator`.

## Stop And Resume

Stop a run:

```sh
crucible stop my-run
```

`stop` writes a stop marker and releases active task claims for incomplete
tasks. The stop marker prevents new claims until the run is resumed.

Resume a stopped or incomplete run:

```sh
crucible resume my-run
```

`resume` clears the stop marker and restarts incomplete non-failed work using
the run's materialized `manifest.json` and `workers.json`. Omit the run name
only when exactly one incomplete run exists. Use `retry` for failed tasks.

Add worker slots to an incomplete run:

```sh
crucible add --worker worker-c:4
crucible add --worker worker-c:4 my-run
crucible worker add worker-c:4 my-run
```

`add --worker` starts the selected worker slots against the materialized run. It
uses the same inline worker syntax as `start`, inheriting defaults from the
selected run's materialized `workers.json`. It clears an existing drain marker
for that worker, merges the worker into the run's `workers.json`, and validates
setup before tasks can be claimed. Existing workers are not restarted. Omit the
run name only when exactly one active run exists. `worker add` is the explicit
subcommand form.

Drain a worker from a run:

```sh
crucible del --worker worker-c
crucible del --worker worker-c my-run
crucible worker del worker-c my-run
```

`del --worker` prevents that worker from claiming new tasks and releases its
reserved claims. Running tasks are not killed. Use `--keep-claims` if you only
want to prevent future claims. It also removes that host from the run's
`workers.json`. Without `--worker`, `del` keeps its existing run-deletion
behavior. Omit the run name only when exactly one active run exists.
`worker del` is the explicit subcommand form.

## Retry Failed Work

Retry every failed incomplete run:

```sh
crucible retry
```

Retry one run:

```sh
crucible retry my-run
```

Retry with a different worker pool:

```sh
crucible retry my-run --workers worker-c,worker-d:4
crucible retry my-run --workers alternate-workers.json
```

`retry` resumes the materialized `runs/<run>/manifest.json`, lets workers claim
tasks with `.fail.json`, and skips tasks with valid `.done.json` and declared
outputs. Omit the run name to retry all failed incomplete runs under `runs/`.
Add `--wait` only when you want the command to stay attached and fail if the
retried work still ends incomplete or failed.

If a dead worker left live claims behind, release that worker before retrying:

```sh
crucible release --manifest runs/my-run/manifest.json --worker worker-name
```

## Workers

Inline workers are comma-separated. If `runs/workers.json` or
`./workers.json` contains that host, the inline entry inherits the file's
profile, Python path, local flag, cache directory, and slot default. If no
default worker record exists for that host, an inline entry without `:N` uses
8 slots.

```sh
crucible start manifest.json --workers worker-a,worker-b
crucible start manifest.json --workers worker-a:8,worker-b:4
crucible start manifest.json --workers worker-a=linux:8,worker-b=mac:4
crucible start manifest.json --workers worker-a:0,worker-b
```

Use `:N` to override the slot count for that invocation. Use `:0` to omit a
worker without rewriting the workers file. Negative and non-integer slot counts
are invalid.

Workers files as JSON:

```json
{
  "schema": "crucible.workers.v1",
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

A string worker uses the same value as host and profile. Object workers support
`host`, `profile`, `python`, `local`, `cache_dir`, `slots`, and `enabled`.
`slots` starts multiple worker loops on the same host; use it only when the
task does not already saturate the machine. Set `enabled` to `false`, or set
`slots` to `0`, to omit a worker from the expanded pool.

## Manifests

A manifest declares prerequisites, path mappings, tasks, progress parsing, and
outputs. `crucible start` materializes the template into `runs/<run>/manifest.json`
before work starts.

Minimal setup-only manifest:

```json
{
  "schema": "crucible.task.v1",
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
crucible plan \
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
crucible add-repo \
  --manifest manifest.json \
  --name tool \
  --path ~/work/tool \
  --url git@example.com:owner/tool.git \
  --remote origin \
  --commit REPLACE_WITH_COMMIT_SHA \
  --probe '~/work/tool/tool --help | grep -- "--needed-option"'

crucible add-command \
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
`CRUCIBLE_NOTIFY_COMMAND`, or `notify_command` in
`~/.config/crucible/config.yml`. The command receives one JSON payload on stdin
and in `CRUCIBLE_EVENT_JSON`. Crucible does not assume any notification service.

For long jobs, make each task write declared log outputs, add a task label, add
progress parsing when possible, and pass a notification command so completion or
failure is reported without polling.

When `wait` sees a terminal state, Crucible writes `<run>/summary.json` with the
same completion payload sent to notifications, including the final status line.
Detached starts run a background wait monitor so completed runs still get that
summary even when no notification command is configured.

## Updates

`update` uses `runs/workers.json` by default, falling back to `./workers.json`.
It updates workers to the coordinator's exact current Crucible commit.

```sh
crucible update
crucible update worker-a,worker-b:4
crucible update workers.json
crucible update --all
```

With `--all`, the coordinator fast-forwards first, then workers are matched to
the new coordinator commit.

## Advanced Commands

Lower-level commands remain available for diagnostics and automation:

```sh
crucible deploy workers.json manifest.json
crucible wait --manifest runs/my-run/manifest.json
crucible verify my-run
crucible merge my-run --output merged.txt
crucible stop my-run
crucible doctor --manifest runs/my-run/manifest.json
crucible sync-repos --manifest runs/my-run/manifest.json --profile worker-a
crucible work --coordinator host:/path/to/manifest.json --profile worker-a
crucible run-task --manifest runs/my-run/manifest.json --index 0
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

Example manifests live under `examples/`. They are samples only; Crucible's task
model is domain-agnostic.
