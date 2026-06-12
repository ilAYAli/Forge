# Crucible Examples

These manifests are templates. Replace commit SHAs, run names, and source paths
before use.

For distributed runs, pass a coordinator address that every worker can SSH back
to. Do not rely on short hostnames unless they resolve to the same machine from
every worker.

```sh
crucible deploy examples/workers.json examples/nnue-selfplay.manifest.json \
  --run nnue-selfplay \
  --coordinator-host COORDINATOR_HOST_OR_IP
```

## Tooling Update

Sync and verify common chess tooling on all workers:

```sh
crucible deploy examples/workers.json examples/tooling-update.manifest.json \
  --run tooling-update \
  --coordinator-host COORDINATOR_HOST_OR_IP
```

This is setup-only. It updates Crucible, NNUE, and Enyo checkouts, builds Enyo,
runs `scripts/deploy.sh --candidate`, and probes `fastchess`, Stockfish, and
the candidate engine.

## NNUE Self-Play

Run self-play shards:

```sh
crucible deploy examples/workers.json examples/nnue-selfplay.manifest.json \
  --run nnue-selfplay \
  --coordinator-host COORDINATOR_HOST_OR_IP
crucible status nnue-selfplay
```

Use workers with few slots for this. Each self-play shard already runs several
engine games concurrently.

## NNUE Stockfish Labeling

Label a sampled source JSONL:

```sh
crucible deploy examples/workers.json examples/nnue-label-stockfish.manifest.json \
  --run nnue-label-stockfish \
  --coordinator-host COORDINATOR_HOST_OR_IP
crucible status nnue-label-stockfish
crucible merge \
  nnue-label-stockfish \
  --output runs/nnue-label-stockfish/outputs/labeled.jsonl
```

Use worker `slots` for labeling. Each shard owns one Stockfish process, so
multiple slots can use idle CPU cores on the same host.

## NNUE SPRT

Run independent SPRT shards:

```sh
crucible deploy examples/workers.json examples/nnue-sprt.manifest.json \
  --run nnue-sprt \
  --coordinator-host COORDINATOR_HOST_OR_IP
crucible status nnue-sprt --verbose
```

The manifest pins and installs the `sprt` wrapper from Git, then probes the
exact option used by the task command before any games are started.
Use worker `slots` for parallelism. Keep each SPRT task at low internal
concurrency, usually `--concurrency 1`, and create more shards than total
worker slots. Shards of 50-100 games are usually small enough to avoid a slow
tail while still keeping startup overhead reasonable.
