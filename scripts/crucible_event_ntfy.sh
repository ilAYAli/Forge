#!/usr/bin/env bash
set -euo pipefail

AI_STDOUT_URL=${CRUCIBLE_AI_STDOUT_URL:-https://ntfy.wahlman.no/AI_stdout}
AI_STDIN_URL=${CRUCIBLE_AI_STDIN_URL:-https://ntfy.wahlman.no/AI_stdin}
AI_STDIN_ENABLE=${CRUCIBLE_AI_STDIN_ENABLE:-1}
AI_STDIN_EVENTS=${CRUCIBLE_AI_STDIN_EVENTS:-deploy_done,deploy_fail}
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

wake_message=$(
    CRUCIBLE_EVENT_JSON="$payload" python3 - <<'PY'
import json
import os

event = json.loads(os.environ["CRUCIBLE_EVENT_JSON"])
state = event.get("state", "")
run = event.get("run", "")
run_dir = event.get("run_dir", "")
counts = event.get("counts") or {}
failed = counts.get("fail", 0)
tasks = event.get("tasks", 0)
done = counts.get("done", 0)

verb = "complete" if state == "ok" else "failed"
print(
    f"Crucible run {verb}: run={run} state={state} "
    f"tasks={done}/{tasks} fail={failed} log={run_dir}/deploy.log. "
    "Inspect the result and continue the next task."
)
PY
)

if [ "$DRY_RUN" = "1" ]; then
    printf '%s\n' "$rendered"
    printf '%s\n' "$wake_message"
    exit 0
fi

source "$HOME/.ntfy" 2>/dev/null || true

priority=4
if printf '%s' "$payload" | grep -q '"state"[[:space:]]*:[[:space:]]*"failed"'; then
    priority=5
fi

send_ntfy() {
    local url=$1
    local title=$2
    local message=$3

    if [ -n "${NTFY_AUTH:-${LICHESS_NTFY_AUTH:-}}" ]; then
        curl -fsS -m 10 -u "${NTFY_AUTH:-${LICHESS_NTFY_AUTH:-}}" \
            -H "Title: $title" -H "Priority: $priority" \
            --data-binary "$message" "$url" >/dev/null
    else
        curl -fsS -m 10 \
            -H "Title: $title" -H "Priority: $priority" \
            --data-binary "$message" "$url" >/dev/null
    fi
}

rc=0
send_ntfy "$AI_STDOUT_URL" "Crucible" "$rendered" || rc=$?

event_name=$(
    CRUCIBLE_EVENT_JSON="$payload" python3 - <<'PY'
import json
import os
print(json.loads(os.environ["CRUCIBLE_EVENT_JSON"]).get("event", ""))
PY
)

case ",$AI_STDIN_EVENTS," in
    *,"$event_name",*) send_ai_stdin=1 ;;
    *) send_ai_stdin=0 ;;
esac

if [ "$AI_STDIN_ENABLE" = "1" ] && [ "$send_ai_stdin" = "1" ]; then
    send_ntfy "$AI_STDIN_URL" "Crucible" "$wake_message" || rc=$?
fi

exit "$rc"
