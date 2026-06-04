# Crucible

Crucible distributes manifest-defined work across a private set of workers.

The coordinator prepares workers, syncs declared prerequisites and inputs,
starts worker loops, tracks task state, and collects outputs. Crucible is
repo-agnostic; the manifest defines the work, and the workers file defines
where it may run.

## Suitable Use

Use Crucible when you have machines you control and want one command to fan out
repeatable work across them. It is a good fit for:

- independent tasks that can run in parallel;
- setup-only jobs such as syncing repos, building tools, installing binaries, or
  running probes;
- data generation, transformation, validation, and result collection;
- private experiment loops where you want reproducible inputs, logs, task
  state, and outputs in one run directory.

## Quick Start

Install on the coordinator:

```sh
git clone git@github.com:ilAYAli/Crucible.git ~/src/crucible
mkdir -p ~/.local/bin
ln -sf ~/src/crucible/crucible.py ~/.local/bin/crucible
crucible --help >/dev/null
```

Deploy prerequisites to all workers, then run tasks if the manifest has any:

```sh
crucible deploy workers.json manifest.json --run my-run
```

`deploy` creates `runs/<run>/`, writes the materialized `manifest.json` and
`workers.json` there, copies the current runner package and declared input
files to each worker, runs repository sync/build/install/probe requirements,
runs `doctor`, then starts worker loops. Workers need Python, Git, SSH/rsync,
and the toolchain required by the manifest. Workers do not need `crucible`
installed.

Default output is an ordered per-worker summary. Use `--verbose` for live phase
details. TTY output is colored by default; use `--no-color` to disable it.

## Workers

Example `workers.json`:

```json
{
  "schema": "crucible.workers.v1",
  "workers": [
    "worker-a",
    "worker-b",
    {
      "host": "worker-c",
      "profile": "linux"
    },
    {
      "host": "worker-d",
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
`slots` starts multiple worker loops on the same host; use it for commands that
consume only part of the machine, not for commands that already saturate it.

## Manifest

Example setup-only manifest:

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
        "install": [
          "scripts/install.sh"
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

If workers use different paths, add `path_maps` and set matching worker
profiles. If `tasks` is empty, `deploy` reports `setup complete` after
prerequisites are verified.

## Tasks

Use `plan` for simple numbered tasks:

```sh
crucible plan \
  --name batch-transform \
  --kind batch \
  --task-label transform \
  --shards 24 \
  --work-dir ~/work/project \
  --out run.manifest.json \
  --command-template 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '~/work/project/out/shard.{index}.txt'
```

## Common Commands

```sh
crucible --help
crucible deploy workers.json manifest.json --run my-run
crucible status
watch -n 2 'crucible status --verbose'
crucible wait --manifest runs/my-run/manifest.json --notify-command ./notify
crucible update
```

For everything else, use subcommand help:

```sh
crucible status --help
crucible deploy --help
crucible plan --help
crucible verify --help
crucible merge --help
crucible stop --help
```

### Status

`crucible status` shows active runs under `runs/`. Pass a run name for one
run, and add `--verbose` for one row per task:

```sh
crucible status
crucible status my-run --verbose
```

Use `--all`, `--done`, or `--failed` to change which runs are listed. Use
`--show-done` to include completed task breakdowns by worker.

### Coordinator Runs

Workers can point `status` at the coordinator's run directory by writing
`runs/coordinator`:

```sh
mkdir -p runs
printf '%s\n' 'coordinator.example.net:/srv/crucible/runs' > runs/coordinator
```

Set `CRUCIBLE_RUNS_DIR` to use a different local run root, or
`CRUCIBLE_COORDINATOR_RUNS=host:/path/to/crucible/runs` to override
`runs/coordinator`. Explicit run paths remain local diagnostics.

### Labels

`--kind` labels a run in summary status. `--task-label` labels tasks in verbose
status. Crucible treats both as user-defined text.

### Worker Updates

`update` uses `runs/workers.json` by default, falling back to `./workers.json`.
It updates workers to the coordinator's exact current Crucible commit. With
`--all`, the coordinator fast-forwards first, then workers are matched to the
new coordinator commit.

### Notifications

`deploy` and `wait` support `--notify-command`, or `CRUCIBLE_NOTIFY_COMMAND`.
The command receives one JSON payload on stdin and in `CRUCIBLE_EVENT_JSON`.
Crucible does not assume any notification service.

### Task Count

For long tasks, create more tasks than worker slots. `deploy` warns when a run
has fewer than four tasks per worker slot, because that can leave fast workers
idle behind a slow tail.

Lower-level commands remain available for diagnostics: `sync-repos`, `doctor`,
`work`, `claim`, `release`, `heartbeat`, and `run`.

## Examples

Example manifests live under `examples/`. They are domain-specific samples, not
part of Crucible's task model.

## Failure Model

- A task is complete only when its `done.json` and declared outputs exist.
- Output checksums are verified before merge.
- Failed tasks are not merged.
- Active claims prevent duplicate work.
- Stale claims can be reclaimed after their lease expires.
- Wrong Git commits, missing commands, or mismatched input checksums fail before
  a task command starts.
- Ctrl+C in `deploy` signals active SSH/rsync commands and wraps remote commands
  so worker child processes are also terminated.
