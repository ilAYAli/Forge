# Distribute

Small coordinator/worker runner for deterministic CPU/IO-heavy tasks.

`distrib` is repo-agnostic. A manifest declares prerequisites and optional
task shards. A workers file declares where to run them.

## Quick Start

Install on the coordinator:

```sh
git clone git@github.com:ilAYAli/distrib.git ~/code/cpp/chess/distrib
mkdir -p ~/.local/bin
ln -sf ~/code/cpp/chess/distrib/distrib.py ~/.local/bin/distrib
distrib --help >/dev/null
```

Deploy prerequisites to all workers, then run tasks if the manifest has any:

```sh
distrib deploy workers.json manifest.json
```

`deploy` copies the current runner and manifest to each worker, runs repository
sync/build/install/probe requirements, runs `doctor`, then starts worker loops
for task shards. Workers need Python, Git, SSH/rsync, and the toolchain required
by the manifest. Workers do not need `distrib` installed.
Progress is printed per worker as sync, prerequisite, and task phases start and
finish.

## Workers

Example `workers.json`:

```json
{
  "schema": "distribute.workers.v1",
  "workers": [
    "pwa-5090",
    "pwa-rce",
    "pwa-wsl",
    {
      "host": "localhost",
      "local": true
    }
  ]
}
```

A string worker uses the same value as host and profile. Object workers support
`host`, `profile`, `local`, `cache_dir`, and `enabled`.

## Manifest

Example setup-only manifest:

```json
{
  "schema": "distribute.task.v1",
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
distrib plan \
  --name score \
  --shards 24 \
  --work-dir ~/work/project \
  --out run.manifest.json \
  --command-template 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '~/work/project/out/shard.{index}.txt'
```

Useful commands:

```sh
distrib status --manifest run.manifest.json
distrib verify --manifest run.manifest.json
distrib merge --manifest run.manifest.json --output merged.txt
```

Lower-level commands remain available for diagnostics: `sync-repos`, `doctor`,
`work`, `claim`, `release`, `heartbeat`, and `run`.

## Failure Model

- A task is complete only when its `done.json` and declared outputs exist.
- Output checksums are verified before merge.
- Failed tasks are not merged.
- Active claims prevent duplicate work.
- Stale claims can be reclaimed after their lease expires.
- Wrong Git commits, missing commands, or mismatched input checksums fail before
  a task command starts.
