#!/usr/bin/env bash
# Compatibility wrapper for the Enyo build preset.
# Usage: scripts/deploy_enyo.sh <commit> [--candidate|--reference] [--workers hosts]
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <commit> [--candidate|--reference] [--workers hosts]" >&2
    exit 1
fi

commit="$1"
shift
exec forge build enyo "$commit" "$@"
