# Crucible Examples

These manifests are templates. Replace commit SHAs, run names, and source paths
before use.

## Tooling Update

Sync and verify common chess tooling on all workers:

```sh
crucible deploy examples/workers.json examples/tooling-update.manifest.json --run tooling-update
```

This is setup-only. It updates Crucible, NNUE, and Enyo checkouts, builds Enyo,
runs `scripts/deploy.sh --candidate`, and probes `fastchess`, Stockfish, and
the candidate engine.

## NNUE Self-Play

Run self-play shards:

```sh
crucible deploy examples/workers.json examples/nnue-selfplay.manifest.json --run nnue-selfplay
crucible status nnue-selfplay
```

Use workers with few slots for this. Each self-play shard already runs several
engine games concurrently.

## NNUE Stockfish Labeling

Label a sampled source JSONL:

```sh
crucible deploy examples/workers.json examples/nnue-label-stockfish.manifest.json --run nnue-label-stockfish
crucible status nnue-label-stockfish
crucible merge \
  nnue-label-stockfish \
  --output runs/nnue-label-stockfish/outputs/labeled.jsonl
```

Use worker `slots` for labeling. Each shard owns one Stockfish process, so
multiple slots can use idle CPU cores on the same host.

## NNUE SPRT

Run independent SPRT chunks:

```sh
crucible deploy examples/workers.json examples/nnue-sprt.manifest.json --run nnue-sprt
crucible status nnue-sprt --verbose
```

The manifest pins and installs the `sprt` wrapper from Git, then probes the
exact option used by the task command before any games are started.
