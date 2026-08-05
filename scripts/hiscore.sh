#!/usr/bin/env bash
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)

LIMIT=20
REFERENCE_NET="nn-0ee0657fb25e.nnue"
CANDIDATE_PATTERN="enyo-%"
ENGINE_FILTER=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --engine)
      ENGINE_FILTER="$2"
      shift 2
      ;;
    --candidate-net)
      CANDIDATE_PATTERN="$2"
      [[ "$CANDIDATE_PATTERN" != *"%"* ]] && CANDIDATE_PATTERN="${CANDIDATE_PATTERN}%"
      shift 2
      ;;
    --reference-net|--reference)
      REFERENCE_NET="$2"
      shift 2
      ;;
    *)
      LIMIT=$1
      shift
      ;;
  esac
done

WHERE_CLAUSE="reference_net = '$REFERENCE_NET' AND candidate_net LIKE '$CANDIDATE_PATTERN'"
[[ -n "$ENGINE_FILTER" ]] && WHERE_CLAUSE="$WHERE_CLAUSE AND engine = '$ENGINE_FILTER'"

sqlite3 -header -box "$ROOT/logs/sprt.db" "
SELECT ROW_NUMBER() OVER (ORDER BY elo DESC) as rank, candidate_net, elo
FROM sprt
WHERE $WHERE_CLAUSE
ORDER BY elo DESC
LIMIT $LIMIT;"
