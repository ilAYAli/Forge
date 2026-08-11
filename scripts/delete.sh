#!/usr/bin/env bash
set -euo pipefail

# Resolve the actual script location (handles symlinks)
SCRIPT_PATH=$(readlink -f "${BASH_SOURCE[0]}")
ROOT=$(cd -- "$(dirname -- "$SCRIPT_PATH")/.." && pwd -P)
DB="${ROOT}/logs/sprt.db"

CANDIDATE_NET=""
REFERENCE_NET=""
ENGINE_FILTER=""
VERBOSE=0

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --engine)
      ENGINE_FILTER="$2"
      shift 2
      ;;
    --candidate-net)
      CANDIDATE_NET="$2"
      shift 2
      ;;
    --reference-net|--reference)
      REFERENCE_NET="$2"
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

# Validation
[[ ! -f "$DB" ]] && { echo "Database not found: $DB" >&2; exit 1; }
[[ -z "$CANDIDATE_NET" && -z "$REFERENCE_NET" ]] && { echo "Must specify --candidate-net or --reference-net" >&2; exit 1; }

# Build DELETE query
if [[ -n "$CANDIDATE_NET" ]]; then
  query="DELETE FROM sprt WHERE candidate_net LIKE '${CANDIDATE_NET}%';"
  [[ $VERBOSE -eq 1 ]] && echo "Deleting by candidate_net: $CANDIDATE_NET"
elif [[ -n "$REFERENCE_NET" ]]; then
  query="DELETE FROM sprt WHERE reference_net LIKE '${REFERENCE_NET}%';"
  [[ $VERBOSE -eq 1 ]] && echo "Deleting by reference_net: $REFERENCE_NET"
fi

# Execute
if sqlite3 "$DB" "$query"; then
  rows=$(sqlite3 "$DB" "SELECT changes();")
  echo "Deleted $rows rows"
else
  echo "Database error" >&2
  exit 1
fi
