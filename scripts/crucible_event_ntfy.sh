#!/usr/bin/env bash
set -euo pipefail

AI_STDOUT_URL=${CRUCIBLE_AI_STDOUT_URL:-https://ntfy.wahlman.no/AI_stdout}
DRY_RUN=${CRUCIBLE_NTFY_DRY_RUN:-0}

payload=${CRUCIBLE_EVENT_JSON:-}
if [ -z "$payload" ]; then
    payload=$(cat)
fi

rendered=$(
    CRUCIBLE_EVENT_JSON="$payload" python3 - <<'PY'
import json
import os

event = json.loads(os.environ["CRUCIBLE_EVENT_JSON"])
counts = event.get("counts") or {}
workers = event.get("worker_results") or []
failed = [item for item in workers if not item.get("ok")]

title = "Crucible"
kind = event.get("kind")
if kind:
    title += f" {kind}"
title += " done" if event.get("state") == "ok" else " failed"

lines = ["<output>", title]
lines.append(f"  • Run: {event.get('run', '')}")
if event.get("description"):
    lines.append(f"  • Description: {event['description']}")
lines.append(f"  • State: {event.get('state', '')}")
lines.append(f"  • Tasks: {counts.get('done', 0)}/{event.get('tasks', 0)} done")
lines.append(
    "  • Active: "
    f"running={counts.get('running', 0)} "
    f"claimed={counts.get('claimed', 0)} "
    f"pending={counts.get('pending', 0)} "
    f"fail={counts.get('fail', 0)} "
    f"stale={counts.get('stale', 0)}"
)
lines.append(f"  • Workers: {sum(1 for item in workers if item.get('ok'))}/{len(workers)} ok")
if failed:
    summary = ", ".join(
        f"{item.get('host', 'worker')}:{item.get('stage', '')}:rc={item.get('rc', '')}"
        for item in failed[:6]
    )
    lines.append(f"  • Failed workers: {summary}")
lines.append(f"  • Run dir: {event.get('run_dir', '')}")
elapsed = event.get("elapsed_seconds")
if isinstance(elapsed, (int, float)):
    lines.append(f"  • Elapsed: {elapsed:.1f}s")
lines.append("</output>")
print("\n".join(lines))
PY
)

if [ "$DRY_RUN" = "1" ]; then
    printf '%s\n' "$rendered"
    exit 0
fi

source "$HOME/.ntfy" 2>/dev/null || true

priority=4
if printf '%s' "$payload" | grep -q '"state"[[:space:]]*:[[:space:]]*"failed"'; then
    priority=5
fi

if [ -n "${NTFY_AUTH:-${LICHESS_NTFY_AUTH:-}}" ]; then
    curl -fsS -m 10 -u "${NTFY_AUTH:-${LICHESS_NTFY_AUTH:-}}" \
        -H "Title: Crucible" -H "Priority: $priority" \
        --data-binary "$rendered" "$AI_STDOUT_URL" >/dev/null
else
    curl -fsS -m 10 \
        -H "Title: Crucible" -H "Priority: $priority" \
        --data-binary "$rendered" "$AI_STDOUT_URL" >/dev/null
fi
