#!/usr/bin/env bash
# Build Enyo locally on each selected worker, then atomically install it.
# Usage: scripts/deploy_enyo.sh [<commit>] [--workers hosts] [--candidate|--reference]
set -euo pipefail

usage() {
    echo "Usage: $0 [<commit>] [--workers hosts] [--candidate|--reference]" >&2
    exit 1
}

HASH="HEAD"
if [[ $# -gt 0 && "$1" != --* ]]; then
    HASH="$1"
    shift
fi
[[ "$HASH" == "HEAD" || "$HASH" =~ ^[0-9a-fA-F]{4,40}$ ]] || { echo "error: '$HASH' is not a commit hash" >&2; exit 1; }

MODE="candidate"
MODE_SET=0
WORKERS_SET=0
WORKER_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workers)
            [[ $# -ge 2 ]] || usage
            WORKER_ARGS+=(--workers "$2")
            WORKERS_SET=1
            shift 2
            ;;
        --candidate|--reference)
            [[ "$MODE_SET" -eq 0 ]] || usage
            MODE="${1#--}"
            MODE_SET=1
            shift
            ;;
        *) usage ;;
    esac
done

CMD="set -e
cd ~/code/chess/enyo
git fetch --quiet
git reset --hard "$HASH"
if output=\$(./scripts/deploy.sh --$MODE 2>&1); then rc=0; else rc=\$?; fi
printf '%s\\n' \"\$output\"
if [ \"\$rc\" -ne 0 ]; then
    case \"\$output\" in
        *'bench nodes: train='*'Text file busy'*) test -x build/enyo ;;
        *) exit \"\$rc\" ;;
    esac
fi
resolved_hash=\$(git rev-parse --short HEAD)
engines_dir=\$HOME/assets/engines
mkdir -p \"\$engines_dir\"
tmp=\$(mktemp \"\$engines_dir/.enyo_\$resolved_hash.XXXXXX\")
trap 'rm -f \"\$tmp\"' EXIT
cp build/enyo \"\$tmp\"
chmod +x \"\$tmp\"
destination=\"\$engines_dir/enyo_\$resolved_hash\"
mv -f \"\$tmp\" \"\$destination\"
ln -sfn \"\$destination\" \"\$engines_dir/$MODE\"
ls -l \"\$engines_dir/$MODE\"
trap - EXIT"

if [[ "$WORKERS_SET" -eq 1 ]]; then
    exec forge sh "${WORKER_ARGS[@]}" --remote-timeout-seconds 3600 "$CMD"
fi
exec forge sh --remote-timeout-seconds 3600 "$CMD"
