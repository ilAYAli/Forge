# Crucible

Small coordinator/worker runner for deterministic CPU/IO-heavy tasks.

`crucible` is repo-agnostic. A manifest declares prerequisites and optional
task shards. A workers file declares where to run them.

## Quick Start

Install on the coordinator:

```sh
git clone git@github.com:ilAYAli/Crucible.git ~/code/cpp/chess/crucible
mkdir -p ~/.local/bin
ln -sf ~/code/cpp/chess/crucible/crucible.py ~/.local/bin/crucible
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
    "pwa-5090",
    "pwa-rce",
    {
      "host": "pwa-wsl",
      "profile": "pwa-wsl"
    },
    {
      "host": "pwa-mbp",
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
`slots` starts multiple worker loops on the same host; use it for
one-shard-at-a-time tasks such as labeling, not for commands that already
saturate the machine.

## Manifest

Example setup-only manifest:

```json
{
  "schema": "crucible.task.v1",
  "name": "enyo-candidate-deploy",
  "work_dir": "~/code/cpp/chess/enyo",
  "requirements": {
    "python_min": "3.11",
    "git_min": "2.38",
    "repos": [
      {
        "name": "enyo",
        "url": "git@github.com:ilAYAli/enyo.git",
        "path": "~/code/cpp/chess/enyo",
        "remote": "origin",
        "commit": "REPLACE_WITH_ENYO_COMMIT_SHA",
        "build": [
          "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release",
          "cmake --build build -j8"
        ],
        "install": [
          "scripts/deploy.sh --candidate"
        ],
        "probe": [
          "test -x build/enyo",
          "test -x ../assets/engines/candidate"
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
  --name score \
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
crucible del my-run           # delete one run directory
crucible del --done           # delete completed run directories
```

`status` without a run name summarizes active runs in `runs/`. `status <run>` shows detailed
task state for one run. Template manifests under `examples/` are deploy inputs,
not runnable jobs. The default view shows counts, active tasks, failed/stale
tasks, and compressed pending ranges. Use `--all`, `--done`, or `--failed` to
change the run filter. Use `--show-done` for completed task
breakdowns by worker, or `--verbose` for the full task table.

Lower-level commands remain available for diagnostics: `sync-repos`, `doctor`,
`work`, `claim`, `release`, `heartbeat`, and `run`.

## NNUE Examples

Edit the commit SHA and paths first, then run:

```sh
crucible deploy examples/workers.json examples/nnue-selfplay.manifest.json --run nnue-selfplay-test
crucible deploy examples/workers.json examples/nnue-label-stockfish.manifest.json --run nnue-label-test
```

Use `examples/workers-linux.json` when the coordinator cannot directly enter
the WSL worker shell.

Merge labeled shards:

```sh
crucible merge \
  nnue-label-test \
  --output runs/nnue-label-test/outputs/labeled.jsonl
```

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
