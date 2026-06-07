# Crucible

Crucible distributes manifest-defined work across workers you control.

It keeps each job in `runs/<name>/` with the materialized manifest, workers
file, logs, state files, declared outputs, and coordinator metadata. Workers
connect out through SSH/rsync; no daemon or database is required.

## Basic Commands

```sh
crucible --help
crucible start manifest.json --workers worker-a,worker-b
crucible stop my-run
crucible resume my-run
crucible retry
crucible status
watch 'crucible status --verbose'
crucible update
```

Use `crucible start --help`, `crucible stop --help`, `crucible resume --help`,
`crucible retry --help`, and `crucible status --help` for normal workflow
options.

## Start Work

Start a manifest on a short inline worker list:

```sh
crucible start manifest.json --workers worker-a,worker-b
```

Start with a workers file when workers need profiles, custom Python paths, local
execution, cache directories, disabled entries, or slots:

```sh
crucible start manifest.json --workers workers.json --run my-run --wait
```

`start` creates `runs/<run>/`, writes `manifest.json` and `workers.json` there,
syncs the runner and declared inputs to workers, verifies prerequisites, starts
worker loops, verifies declared outputs, and optionally waits or merges output.
It returns non-zero on setup, task, wait, verification, or merge failure.

Useful options:

```sh
crucible start manifest.json --workers worker-a,worker-b --run my-run
crucible start manifest.json --workers workers.json --wait
crucible start manifest.json --workers workers.json --merge merged.txt
crucible start manifest.json --workers workers.json --replace
```

## Status

`status` without a run name shows active runs. Failed inactive runs are
collapsed near the top so they do not hide active work.

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
crucible retry my-run --workers worker-c,worker-d
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

Inline workers are a comma-separated host list:

```sh
crucible start manifest.json --workers worker-a,worker-b
```

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
      "host": "localhost",
      "local": true
    }
  ]
}
```

A string worker uses the same value as host and profile. Object workers support
`host`, `profile`, `python`, `local`, `cache_dir`, `slots`, and `enabled`.
`slots` starts multiple worker loops on the same host; use it only when the task
does not already saturate the machine.

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

`start`, `deploy`, and `wait` support `--notify-command`, or
`CRUCIBLE_NOTIFY_COMMAND`. The command receives one JSON payload on stdin and in
`CRUCIBLE_EVENT_JSON`. Crucible does not assume any notification service.

For long jobs, make each task write declared log outputs, add a task label, add
progress parsing when possible, and pass a notification command so completion or
failure is reported without polling.

## Updates

`update` uses `runs/workers.json` by default, falling back to `./workers.json`.
It updates workers to the coordinator's exact current Crucible commit.

```sh
crucible update
crucible update worker-a,worker-b
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
