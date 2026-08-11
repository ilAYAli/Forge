#!/usr/bin/env bash
set -euo pipefail

# Resolve the actual script location (handles symlinks)
SCRIPT_PATH=$(readlink -f "${BASH_SOURCE[0]}")
ROOT=$(cd -- "$(dirname -- "$SCRIPT_PATH")/.." && pwd -P)
DB="${ROOT}/logs/sprt.db"

LIMIT=20
REFERENCE_NET="nn-0ee0657fb25e.nnue"
CANDIDATE_PATTERN="enyo-%"
ENGINE_FILTER=""
VERBOSE=0
MIN_GAMES=1500

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
    --min-games)
      MIN_GAMES="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

[[ ! -f "$DB" ]] && { echo "Database not found: $DB" >&2; exit 1; }

WHERE_CLAUSE="reference_net = '$REFERENCE_NET' AND candidate_net LIKE '$CANDIDATE_PATTERN'"
[[ -n "$ENGINE_FILTER" ]] && WHERE_CLAUSE="$WHERE_CLAUSE AND engine = '$ENGINE_FILTER'"

if [[ $VERBOSE -eq 1 ]]; then
  sqlite3 -header -box "$DB" "
  SELECT ROW_NUMBER() OVER (ORDER BY elo DESC) as rank, candidate_net, elo, ci, engine, games
  FROM sprt
  WHERE reference_net = '$REFERENCE_NET'
  ORDER BY elo DESC;"
else
  GAME_FILTER=""
  [[ $MIN_GAMES -gt 0 ]] && GAME_FILTER="AND games >= $MIN_GAMES"

  sqlite3 -header -box "$DB" "
  SELECT ROW_NUMBER() OVER (ORDER BY elo DESC) as rank, candidate_net, elo, ci, engine, games
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY candidate_net ORDER BY games DESC, date DESC, elo DESC) as rn
    FROM sprt
    WHERE $WHERE_CLAUSE $GAME_FILTER
  )
  WHERE rn = 1
  ORDER BY elo DESC
  LIMIT $LIMIT;"
fi
