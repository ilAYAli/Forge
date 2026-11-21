#!/usr/bin/env bash
# Build an Enyo commit once per compatible platform and distribute the binary.
#
# Usage: scripts/deploy_enyo.sh <commit> [--workers a,b] [--candidate|--reference]
set -euo pipefail

usage() { echo "Usage: $0 <commit> [--workers a,b] [--candidate|--reference]" >&2; exit 1; }

[[ $# -ge 1 ]] || usage
HASH="$1"; shift
[[ "$HASH" =~ ^[0-9a-fA-F]{4,40}$ ]] || { echo "error: '$HASH' is not a commit hash" >&2; exit 1; }

WORKER_ARGS=()
MODE="candidate"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers) [[ $# -ge 2 ]] || usage; WORKER_ARGS+=(--workers "$2"); shift 2 ;;
        --candidate|--reference) MODE="${1#--}"; shift ;;
        *) usage ;;
    esac
done

if [[ "$MODE" == "reference" ]]; then
    BUILD_COMMAND='if output=$(./scripts/deploy.sh --reference 2>&1); then rc=0; else rc=$?; fi; printf "%s\\n" "$output"; if [ "$rc" -ne 0 ]; then case "$output" in *"Text file busy"*) test -f build/enyo ;; *) exit "$rc" ;; esac; fi'
else
    BUILD_COMMAND='if output=$(./scripts/deploy.sh --candidate 2>&1); then rc=0; else rc=$?; fi; printf "%s\\n" "$output"; if [ "$rc" -ne 0 ]; then case "$output" in *"Text file busy"*) test -f build/enyo ;; *) exit "$rc" ;; esac; fi'
fi
FORGE_COMMAND=(forge build '~/code/chess/enyo' "$HASH"
    --build-command "$BUILD_COMMAND"
    --artifact build/enyo
    --destination '~/assets/engines/enyo_{hash}'
    --link "~/assets/engines/$MODE")
if [[ ${#WORKER_ARGS[@]} -gt 0 ]]; then
    FORGE_COMMAND+=("${WORKER_ARGS[@]}")
fi
exec "${FORGE_COMMAND[@]}"
