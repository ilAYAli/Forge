#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH=$(readlink -f "${BASH_SOURCE[0]}")
ROOT=$(cd -- "$(dirname -- "$SCRIPT_PATH")/.." && pwd -P)
ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --verbose)
      ARGS+=(--view runs --min-games 0)
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

FORGE_SPRT_DB="${ROOT}/logs/sprt.db" exec "${ROOT}/forge.py" report sprt score "${ARGS[@]}"
