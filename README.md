# Distribute

Small, repo-agnostic shard runner for CPU/IO-heavy work across machines.

The goal is simple: define deterministic tasks once, run individual shards on
any host, copy the finished shard outputs back, verify metadata, and merge in a
stable order. It is intentionally not NNUE-specific.

## Topology

Use one coordinator and any number of outbound-only workers:

- Coordinator owns the manifest, claims, central state, logs, outputs, and
  notifications.
- Workers only need outbound SSH/rsync access to the coordinator.
- Workers claim work from the coordinator, run it locally, push results back,
  then claim another task.
- Localhost can be a worker even when inbound SSH is blocked.
- `notifai.sh` is required only on the coordinator.

For NNUE work, the normal coordinator is `pwa-5090`.

## Prerequisites

- Python 3.11 or newer.
- Git 2.38 or newer.
- `ssh` and `rsync` for moving manifests and shard outputs between hosts.
- The same project checkout on every worker, at the same Git commit.
- Absolute paths in manifests. Do not rely on `~` expansion across hosts.
- If hosts use different absolute roots, add explicit path maps to the
  manifest.
- Identical input artifacts for deterministic work. Use file checksums for
  engine binaries, nets, books, source JSONL, or other fixed inputs.
- `notifai.sh` in `PATH` on the coordinator for long jobs.

`distrib` is deliberately project-agnostic. It does not know about any
particular repository, binary, or training pipeline. Put project-specific setup
in the manifest as ordinary repository, command, input, build, install, and
probe requirements.

Check a worker:

```sh
python3 --version
git --version
ssh pwa-5090 'python3 --version && git --version'
```

## Worker Setup

A manifest can describe the exact repositories and tools a worker must have.
`sync-repos` mutates the worker by cloning missing repositories, checking out
the requested branch or commit, running optional build/install hooks, and then
running probes. `doctor` validates the same manifest without changing it.

Generic repository setup:

```json
{
  "requirements": {
    "repos": [
      {
        "name": "project",
        "url": "git@example.com:owner/project.git",
        "path": "/home/user/work/project",
        "commit": "abcdef123456...",
        "build": [
          "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release",
          "cmake --build build -j8"
        ],
        "install": [
          "mkdir -p /home/user/.local/bin",
          "ln -sf /home/user/work/project/build/project-tool /home/user/.local/bin/project-tool"
        ],
        "probe": [
          "/home/user/.local/bin/project-tool --version | grep 'project-tool'"
        ]
      }
    ],
    "commands": [
      {
        "name": "external-tool",
        "path": "/home/user/.local/bin/external-tool",
        "command": "/home/user/.local/bin/external-tool --version",
        "regex": "External Tool [0-9]+"
      }
    ]
  }
}
```

Prepare and validate a worker:

```sh
distrib sync-repos --manifest run.manifest.json --profile worker-a
distrib doctor --manifest run.manifest.json --profile worker-a --role worker
```

`work` also runs the manifest preflight before claiming tasks. A worker with the
wrong checkout, missing binary, failed probe, or mismatched input does not claim
coordinator work.

## Concepts

- A **manifest** is a JSON file with task definitions and prerequisites.
- A **task** is one deterministic shard command.
- A task writes:
  - `state/<task-id>.running.json`
  - `state/<task-id>.done.json` or `state/<task-id>.fail.json`
  - `logs/<task-id>.log`
- A coordinator claim writes:
  - `claims/<task-id>.claim/claim.json`
  - `claims/<task-id>.claim/heartbeat.json`
- Each done file records host, start/end time, command, rc, elapsed seconds, and
  checksums for declared output files.
- Only coordinator-side verified `done.json` files count for merge.

## Create A Plan

Use `plan` for a simple numbered shard set:

```sh
./distribute.py plan \
  --name nnue-score-d14 \
  --shards 24 \
  --work-dir /home/petter/code/cpp/chess/nnue \
  --out /home/petter/code/cpp/chess/nnue/runs/my-run/distribute/score.manifest.json \
  --command-template './tools/score/score.py uci --input {source} --output {output} --engine /home/petter/local/bin/stockfish --depth 14 --threads 1 --hash 128 --shard-count {shards} --shard-index {index}' \
  --var source=/home/petter/code/cpp/chess/nnue/runs/my-run/posgen/source.jsonl \
  --path-map localhost:/home/petter/code/cpp/chess=/Users/pwahlman/code/cpp/chess \
  --path-map pwa-wsl:/home/petter/code/cpp/chess=/home/petter/code/cpp/chess \
  --output-template /home/petter/code/cpp/chess/nnue/runs/my-run/score/shards/label.{index}.jsonl
```

Add fixed inputs to the manifest so workers reject mismatches:

```sh
./distribute.py add-input \
  --manifest score.manifest.json \
  --path /home/petter/code/cpp/chess/nnue/runs/my-run/posgen/source.jsonl
```

## Run Shards

Run one shard:

```sh
./distribute.py run --manifest score.manifest.json --task score_0007
```

Claim one task on the coordinator:

```sh
distrib claim --manifest score.manifest.json --worker pwa-wsl --json
```

Run an outbound-only worker loop from another host:

```sh
distrib work \
  --manifest /Users/pwahlman/code/cpp/chess/nnue/runs/my-run/distribute/score.manifest.json \
  --coordinator pwa-5090:/home/petter/code/cpp/chess/nnue/runs/my-run/distribute/score.manifest.json \
  --profile localhost \
  --worker localhost \
  --max-tasks 0
```

The worker repeatedly claims an unclaimed coordinator task over SSH, runs it
locally with the selected profile path map, refreshes a coordinator heartbeat
while the task runs, pushes declared outputs/log/state back to the coordinator,
releases the claim, and asks for another task.

Before claiming, the worker validates repository, command, and input
requirements from the manifest. Run `sync-repos` first when the worker should
clone or rebuild missing prerequisites.

Run several shards:

```sh
for i in 0 1 2 3; do
  ./distribute.py run --manifest score.manifest.json --index "$i"
done
```

Copy the manifest to another host, run a subset there with the matching profile,
then copy only outputs, logs, and state files back:

```sh
rsync -a score.manifest.json pwa-wsl:/tmp/score.manifest.json
ssh pwa-wsl 'cd /home/petter/code/cpp/chess/nnue && distribute run --manifest /tmp/score.manifest.json --profile pwa-wsl --index 8'
rsync -a pwa-wsl:/home/petter/code/cpp/chess/nnue/runs/my-run/score/shards/ /home/petter/code/cpp/chess/nnue/runs/my-run/score/shards/
rsync -a pwa-wsl:/home/petter/code/cpp/chess/nnue/runs/my-run/distribute/state/ /home/petter/code/cpp/chess/nnue/runs/my-run/distribute/state/
```

Manual copy is only a fallback. Prefer `distrib work` for normal distributed
runs.

## Preflight

Check a worker:

```sh
distrib doctor --manifest score.manifest.json --profile pwa-wsl --role worker
```

Check the coordinator notification command:

```sh
distrib doctor \
  --manifest score.manifest.json \
  --role coordinator \
  --notify-command notifai.sh
```

## Status And Merge

```sh
./distribute.py status --manifest score.manifest.json
./distribute.py verify --manifest score.manifest.json
./distribute.py merge --manifest score.manifest.json --output /home/petter/code/cpp/chess/nnue/runs/my-run/score/labeled.jsonl
```

`merge` concatenates declared task outputs in task order and fails if any task is
missing, failed, has missing outputs, or has checksum mismatches.

`status` reports pending, claimed, stale, running, failed, and done tasks.
Claims become stale when their heartbeat is older than the lease timeout.

## Manifest Format

Minimal manifest:

```json
{
  "schema": "distribute.task.v1",
  "name": "example",
  "work_dir": "/abs/project/root",
  "state_dir": "/abs/project/root/distribute/state",
  "log_dir": "/abs/project/root/distribute/logs",
  "claim_dir": "/abs/project/root/distribute/claims",
  "path_maps": {
    "localhost": {
      "/home/petter/code/cpp/chess": "/Users/pwahlman/code/cpp/chess"
    }
  },
  "requirements": {
    "python_min": "3.11",
    "git_min": "2.38",
    "git": {
      "repo": "/abs/project/root",
      "commit": "abcdef...",
      "require_clean": false
    },
    "repos": [
      {
        "name": "dependency",
        "url": "git@example.com:owner/dependency.git",
        "path": "/abs/dependency/root",
        "commit": "123456...",
        "build": "make -j8",
        "install": "ln -sf /abs/dependency/root/bin/tool /home/user/.local/bin/tool",
        "probe": "tool --version | grep dependency"
      }
    ],
    "commands": [
      {
        "name": "tool",
        "path": "/home/user/.local/bin/tool",
        "command": "/home/user/.local/bin/tool --version",
        "contains": "tool"
      }
    ],
    "inputs": []
  },
  "tasks": [
    {
      "id": "score_0000",
      "index": 0,
      "command": "echo shard {index} > {output}",
      "outputs": ["/tmp/score.0.txt"]
    }
  ]
}
```

Commands are shell commands run with `bash -lc` from `work_dir`. Template
variables include manifest `vars`, task fields, and the first output as
`{output}`.

When `--profile NAME` is passed, every string in the manifest is rewritten
through `path_maps.NAME` before running or verifying. This lets one coordinator
manifest use pwa paths while localhost or WSL workers use their local checkout
roots.

## Failure Model

- A task is complete only when its `done.json` exists and all declared outputs
  exist.
- Done outputs are verified against the checksums recorded in `done.json`.
- Done state must match the manifest checksum when the field is present.
- Failed tasks are not merged.
- Active claims prevent duplicate work.
- Stale claims can be reclaimed after the lease timeout.
- Duplicate task ids are rejected.
- Wrong Git commit, too-old Git/Python, or mismatched input checksums fail
  before the command starts.
- Re-running a done task requires `--force`.
- Worker-side task processes run in their own process group. Timeout or
  interruption terminates the whole group before writing fail state.
- Successful worker scratch/output can be discarded after coordinator sync.
  Failed local scratch should be retained for debugging.
