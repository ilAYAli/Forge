# Distribute

Small, repo-agnostic shard runner for CPU/IO-heavy work across machines.

The goal is simple: define deterministic tasks once, run individual shards on
any host, copy the finished shard outputs back, verify metadata, and merge in a
stable order. It is intentionally not NNUE-specific.

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

Check a worker:

```sh
python3 --version
git --version
ssh pwa-5090 'python3 --version && git --version'
```

## Concepts

- A **manifest** is a JSON file with task definitions and prerequisites.
- A **task** is one deterministic shard command.
- A task writes:
  - `state/<task-id>.running.json`
  - `state/<task-id>.done.json` or `state/<task-id>.fail.json`
  - `logs/<task-id>.log`
- Each done file records host, start/end time, command, rc, elapsed seconds, and
  checksums for declared output files.

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

## Status And Merge

```sh
./distribute.py status --manifest score.manifest.json
./distribute.py verify --manifest score.manifest.json
./distribute.py merge --manifest score.manifest.json --output /home/petter/code/cpp/chess/nnue/runs/my-run/score/labeled.jsonl
```

`merge` concatenates declared task outputs in task order and fails if any task is
missing, failed, or has missing outputs.

## Manifest Format

Minimal manifest:

```json
{
  "schema": "distribute.task.v1",
  "name": "example",
  "work_dir": "/abs/project/root",
  "state_dir": "/abs/project/root/distribute/state",
  "log_dir": "/abs/project/root/distribute/logs",
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
- Failed tasks are not merged.
- Duplicate task ids are rejected.
- Wrong Git commit, too-old Git/Python, or mismatched input checksums fail
  before the command starts.
- Re-running a done task requires `--force`.
