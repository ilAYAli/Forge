# Crucible Examples

These manifests are templates. Replace commit SHAs, run names, and source paths
before use.

## Tooling Update

Sync and verify common chess tooling on all workers:

```sh
crucible deploy examples/workers.json examples/tooling-update.manifest.json
```

This is setup-only. It updates Crucible, NNUE, and Enyo checkouts, builds Enyo,
runs `scripts/deploy.sh --candidate`, and probes `fastchess`, Stockfish, and
the candidate engine.

## NNUE Self-Play

Run self-play shards:

```sh
crucible deploy examples/workers.json examples/nnue-selfplay.manifest.json
crucible status examples/nnue-selfplay.manifest.json
```

Use workers with few slots for this. Each self-play shard already runs several
engine games concurrently.

## NNUE Stockfish Labeling

Label a sampled source JSONL:

```sh
crucible deploy examples/workers.json examples/nnue-label-stockfish.manifest.json
crucible status examples/nnue-label-stockfish.manifest.json
crucible merge \
  --manifest examples/nnue-label-stockfish.manifest.json \
  --output ~/code/cpp/chess/crucible-runs/nnue-label-stockfish/labeled.jsonl
```

Use worker `slots` for labeling. Each shard owns one Stockfish process, so
multiple slots can use idle CPU cores on the same host.
