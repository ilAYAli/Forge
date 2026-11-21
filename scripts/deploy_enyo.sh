#!/usr/bin/env bash
# Deploy an Enyo commit as the candidate engine on every forge worker.
#
# Usage: scripts/deploy_enyo.sh <commit> [--workers a,b] [deploy.sh args...]
#   scripts/deploy_enyo.sh 1a2b3c4
#   scripts/deploy_enyo.sh 1a2b3c4 --workers pwa-llm,pwa-5090
#   scripts/deploy_enyo.sh 1a2b3c4 --reference
set -euo pipefail

usage() { echo "Usage: $0 <commit> [--workers a,b] [--candidate|--reference]" >&2; exit 1; }

[[ $# -ge 1 ]] || usage
HASH="$1"; shift
[[ "$HASH" =~ ^[0-9a-fA-F]{4,40}$ ]] || { echo "error: '$HASH' is not a commit hash" >&2; exit 1; }

FORGE_ARGS=()
DEPLOY_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers) [[ $# -ge 2 ]] || usage; FORGE_ARGS+=(--workers "$2"); shift 2 ;;
        --candidate|--reference) DEPLOY_ARGS+=("$1"); shift ;;
        *) usage ;;
    esac
done
[[ ${#DEPLOY_ARGS[@]} -gt 0 ]] || DEPLOY_ARGS=(--candidate)

# Must be a single string: forge quotes each separate argument as one word.
CMD="set -e
cd ~/code/chess/enyo
git fetch --quiet
git reset --hard $HASH
./scripts/deploy.sh ${DEPLOY_ARGS[*]}"

exec forge sh ${FORGE_ARGS[@]+"${FORGE_ARGS[@]}"} --remote-timeout-seconds 3600 "$CMD"
