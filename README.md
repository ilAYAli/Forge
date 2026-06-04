# Crucible

Small coordinator/worker runner for deterministic CPU/IO-heavy tasks.

`crucible` is repo-agnostic. A manifest declares prerequisites and optional
task shards. A workers file declares where to run them.

## Suitable Use

Crucible is meant for private, deterministic fan-out across machines you
control. It is a good fit for:

- fixed-size independent batch work;
- data generation, transformation, validation, or aggregation;
- repository/toolchain sync before distributed work;
- CPU/IO-heavy tasks with declared inputs and outputs.

It is not a replacement for public test infrastructure when you need community
workers, long-term public result history, or standardized tournament
administration. Crucible is the fast private loop; larger public infrastructure
is still useful when you need independent external validation.

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
`workers.json` there, copies the current runner and declared input files to
each worker, runs repository sync/build/install/probe requirements, runs
`doctor`, then starts worker loops for task shards. Workers need Python, Git,
SSH/rsync, and the toolchain required by the manifest. Workers do not need
`crucible` installed.
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

## Shard Tasks

Use `plan` for simple numbered shards:

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

Useful run commands:

```sh
crucible status               # active runs in runs/
crucible status --all         # all runs, including completed runs
crucible status --done        # completed runs
crucible status --failed      # failed runs
crucible status my-run        # detailed status for one run
crucible status my-run -v     # one row per task
crucible verify my-run        # require every task to be done
crucible merge my-run --output merged.txt
crucible stop my-run          # ask workers to stop this run
crucible drain my-run --worker worker-a
                              # let current task finish, then stop new claims
crucible drain my-run --worker worker-a --clear
                              # allow that worker to claim again
crucible release --manifest runs/my-run/manifest.json --worker worker-a
                              # release that worker's reserved, non-running claims
crucible update               # update workers to this coordinator commit
crucible update --all         # update coordinator, then workers
crucible del my-run           # delete one run directory
crucible del --done           # delete completed run directories
```

`status` without a run name summarizes active runs in `runs/`. `status <run>` shows detailed
task state for one run. Template manifests under `examples/` are deploy inputs,
not runnable jobs. The default view shows counts, active tasks, failed/stale
tasks, and compressed pending ranges. Use `--kind` for the run type shown in
summary status, and `--task-label` for the per-task label shown in detailed
status. Both are opaque user metadata; Crucible does not attach domain meaning
to them. Use `--all`, `--done`, or `--failed` to change the run filter. Use
`--show-done` for completed task breakdowns by worker, or `--verbose` for the
full task table.

Put a coordinator pointer in `runs/coordinator` on non-coordinator hosts to make
`crucible status` read the coordinator's run state by default:

```sh
mkdir -p runs
printf '%s\n' 'coordinator.example.net:/srv/crucible/runs' > runs/coordinator
```

Set `CRUCIBLE_RUNS_DIR` to use a different local run root, or
`CRUCIBLE_COORDINATOR_RUNS=host:/path/to/crucible/runs` to override
`runs/coordinator`. Explicit run paths remain local diagnostics.

`update` uses `runs/workers.json` by default, falling back to `./workers.json`.
It updates workers to the coordinator's exact current Crucible commit. With
`--all`, the coordinator fast-forwards first, then workers are matched to the
new coordinator commit.

Lower-level commands remain available for diagnostics: `sync-repos`, `doctor`,
`work`, `claim`, `release`, `heartbeat`, and `run`.

For long tasks, create more tasks than worker slots. `deploy` warns when a run
has fewer than four tasks per worker slot, because that can leave fast workers
idle behind a slow tail.

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
