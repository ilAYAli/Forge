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

Create a manifest, then run it with the high-level job command:

```sh
crucible plan \
  --name batch-transform \
  --tasks 24 \
  --work-dir ~/work/project \
  --manifest run.manifest.json \
  --command 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '{output_dir}/shard.{index}.txt'

crucible run-job run.manifest.json \
  --workers workers.json \
  --run my-run \
  --wait
```

`run-job` creates `runs/<run>/`, writes the materialized `manifest.json` and
`workers.json` there, copies the current runner package and declared input
files to each worker, runs repository sync/build/install/probe requirements,
runs worker loops, waits when requested, verifies declared outputs, and returns
non-zero on setup, task, verification, or wait failure. Workers need Python,
Git, SSH/rsync, and the toolchain required by the manifest. Workers do not need
`crucible` installed. For task runs, setup also verifies that each worker can
SSH back to the coordinator manifest selected by `--coordinator-host` before
any task is assigned.

Declare exact tool requirements in the manifest. For tools built from Git, use
`requirements.repos` with a pinned `commit`, optional `build`/`install`, and a
`probe` that proves the required runtime capability. Use `requirements.commands`
for executable or external-binary checks. Failed probes stop `deploy` before any
task is assigned.

Declared outputs and task logs are synced back to the coordinator run
directory. Default output is an ordered per-worker summary. Use `--verbose` for
live phase details. TTY output is colored by default; use `--no-color` to
disable it.

`plan` and `run-job` are separate on purpose: `plan` defines reproducible work,
and `run-job` executes a materialized manifest. A combined convenience command
can still be useful, but it should keep writing `runs/<run>/manifest.json`
before work starts so the run can be inspected, resumed, verified, and merged.

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
  --manifest run.manifest.json \
  --command 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '{output_dir}/shard.{index}.txt'
```

Add prerequisites without hand-editing JSON:

```sh
crucible add-repo \
  --manifest run.manifest.json \
  --name tool \
  --path ~/work/tool \
  --url git@example.com:owner/tool.git \
  --remote origin \
  --commit REPLACE_WITH_COMMIT_SHA \
  --install 'mkdir -p ~/.local/bin && ln -sf "$PWD/tool" ~/.local/bin/tool' \
  --probe '~/.local/bin/tool --help | grep -- "--needed-option"'

crucible add-command \
  --manifest run.manifest.json \
  --name external-tool \
  --path ~/.local/bin/external-tool \
  --command '~/.local/bin/external-tool --version' \
  --contains 'External Tool'
```

Run the manifest:

```sh
crucible run-job run.manifest.json \
  --workers workers.json \
  --run batch-transform-001 \
  --wait \
  --merge merged.txt
```

Use `--task-label` when planning so verbose status shows readable task names.
Use `--progress-regex` and progress totals for long-running tools that emit
parseable progress lines.

## Common Commands

```sh
crucible --help
crucible plan --help
crucible run-job run.manifest.json --workers workers.json --run my-run --wait
crucible status
watch -n 2 'crucible status --verbose'
crucible status --failed
crucible run-job runs/my-run/manifest.json --workers runs/my-run/workers.json --run my-run --resume --retry-failed --wait
crucible wait --manifest runs/my-run/manifest.json --notify-command ./notify
crucible verify my-run
crucible merge my-run --output merged.txt
crucible update
```

For everything else, use subcommand help:

```sh
crucible status --help
crucible run-job --help
crucible plan --help
crucible add-repo --help
crucible add-command --help
crucible verify --help
crucible merge --help
crucible stop --help
```

Lower-level commands remain available for diagnostics and custom automation:
`deploy`, `sync-repos`, `doctor`, `work`, `claim`, `release`, `heartbeat`, and
`run`.

### Status

`crucible status` shows active runs under `runs/`. Pass a run name for one
run, and add `--verbose` for detailed task sections and diagnostics:

```sh
crucible status
crucible status my-run --verbose
```

Default `status` focuses on active work. Inactive failed runs are collapsed near
the top as `failed: N runs (use crucible status --failed)`, so old failures do
not hide the active run. Run headers are printed as `run="..."`; descriptions
are printed on their own `desc="..."` line. Active task rows stay visible in
the default view. Add `--verbose` for pending/fail sections and diagnostics.
Use `--all`, `--done`, or `--failed` to change which runs are listed. Use
`--show-done` to include completed task breakdowns by worker.

### Restart Failed Tasks

First inspect the run:

```sh
crucible status my-run --verbose
crucible status --failed
```

To retry failed tasks in an existing run, resume the materialized run and allow
failed tasks to be claimed again:

```sh
crucible run-job runs/my-run/manifest.json \
  --workers runs/my-run/workers.json \
  --run my-run \
  --resume \
  --retry-failed \
  --wait
```

`--resume` reuses `runs/my-run/manifest.json` and `runs/my-run/workers.json`.
`--retry-failed` lets worker loops claim tasks that already have `.fail.json`.
Tasks with valid `.done.json` and declared outputs are skipped. Active retries
are shown as active even if an older fail marker exists for the same task.

If a worker died and left only reserved claims, release those claims before
restarting:

```sh
crucible release --manifest runs/my-run/manifest.json --worker worker-name
```

Release one known claim when you need to be precise:

```sh
crucible release \
  --manifest runs/my-run/manifest.json \
  --task task_0044 \
  --worker worker-name \
  --claim-id claim-id
```

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

`run-job`, `deploy`, and `wait` support `--notify-command`, or
`CRUCIBLE_NOTIFY_COMMAND`. The command receives one JSON payload on stdin and
in `CRUCIBLE_EVENT_JSON`. Crucible does not assume any notification service.

Long distributed jobs should be launched from a coordinator host that every
worker can SSH back to. From a worker,
`crucible doctor --manifest <manifest> --coordinator <host>:<manifest>` checks
that callback path directly. For SPRT-style smoke tests, make each shard write a
declared log output, use `--task-label`, add a progress regex matching the
tool's live status line, and pass `--notify-command` so completion or failure is
reported without polling.

### Task Count

For long tasks, create more tasks than worker slots. `deploy` warns when a run
has fewer than four tasks per worker slot, because that can leave fast workers
idle behind a slow tail.

`deploy` is the lower-level form of `run-job`: it materializes and starts work
but does not give the same single-command wait/verify/merge workflow.

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
