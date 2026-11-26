# Forge Examples

These manifests are templates. Replace commit SHAs, run names, and source paths
before use.

For distributed runs, pass a coordinator address that every worker can SSH back
to. Do not rely on short hostnames unless they resolve to the same machine from
every worker.

```sh
forge deploy examples/workers.json examples/nnue-selfplay.manifest.json \
  --run nnue-selfplay \
  --coordinator-host COORDINATOR_HOST_OR_IP
```

## Tooling Update

Sync and verify common chess tooling on all workers:

```sh
forge deploy examples/workers.json examples/tooling-update.manifest.json \
  --run tooling-update \
  --coordinator-host COORDINATOR_HOST_OR_IP
```

This is setup-only. It updates Forge, NNUE, and Enyo checkouts, builds Enyo,
runs `scripts/deploy.sh --candidate`, and probes `fastchess`, Stockfish, and
the candidate engine.

## NNUE Self-Play

Run self-play shards:

```sh
forge deploy examples/workers.json examples/nnue-selfplay.manifest.json \
  --run nnue-selfplay \
  --coordinator-host COORDINATOR_HOST_OR_IP
forge status nnue-selfplay
```

Each self-play shard already runs several engine games concurrently; the
manifest's `resources.threads` (`{threads}*{concurrency}`) makes Forge start
correspondingly fewer shards per host.

## NNUE Stockfish Labeling

Label a sampled source JSONL:

```sh
forge deploy examples/workers.json examples/nnue-label-stockfish.manifest.json \
  --run nnue-label-stockfish \
  --coordinator-host COORDINATOR_HOST_OR_IP
forge status nnue-label-stockfish
forge merge \
  nnue-label-stockfish \
  --output runs/nnue-label-stockfish/outputs/labeled.jsonl
```

Each shard owns one single-thread Stockfish process and is fixed-depth work, so
Forge runs one shard per hardware thread (including SMT siblings) at low CPU
priority. Use `forge worker set <host>:N` to keep part of a host free for a while, and `<host>:max` to give it back.

## NNUE SPRT

Run independent SPRT shards:

```sh
forge deploy examples/workers.json examples/nnue-sprt.manifest.json \
  --run nnue-sprt \
  --coordinator-host COORDINATOR_HOST_OR_IP
forge status nnue-sprt --verbose
```

The manifest pins and installs the `sprt` wrapper from Git, then probes the
exact option used by the task command before any games are started.
SPRT is timed work, so Forge gives each engine thread its own physical core
(performance cores only on Apple Silicon). Keep each SPRT task at low internal
concurrency, usually `--concurrency 1`, and create more shards than total
worker slots. Shards of 50-100 games are usually small enough to avoid a slow
tail while still keeping startup overhead reasonable.
