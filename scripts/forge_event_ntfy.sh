#!/usr/bin/env bash
set -euo pipefail

AI_STDOUT_URL=${FORGE_AI_STDOUT_URL:-https://ntfy.wahlman.no/llmsh}
AI_STDIN_URL=${FORGE_AI_STDIN_URL:-https://ntfy.wahlman.no/llmsh}
AI_STDIN_ENABLE=${FORGE_AI_STDIN_ENABLE:-1}
HOOK_EVENTS=${HOOK_EVENTS:-}
DRY_RUN=${FORGE_NTFY_DRY_RUN:-0}

payload=${FORGE_EVENT_JSON:-}
if [ -z "$payload" ]; then
    payload=$(cat)
fi

payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
printf '%s' "$payload" > "$payload_file"

rendered=$(
    python3 - "$payload_file" <<'PY'
import json
import sys

event = json.loads(open(sys.argv[1]).read())
event_name = str(event.get("event") or "")
kind = event.get("kind")


def base_title():
    title = "Forge"
    if kind:
        title += f" {kind}"
    return title


if event_name in ("phase_start", "phase_done"):
    phase = str(event.get("phase") or "")
    state = event.get("state")
    verb = {"running": "starting", "ok": "complete", "failed": "failed"}.get(state, event_name)
    lines = [f"{base_title()} phase={phase} {verb}", f"  • Run: {event.get('run', '')}"]
    groups = event.get("groups")
    if groups is not None:
        lines.append(f"  • Groups: {groups} failed={event.get('groups_failed') or 0}")
    print("\n".join(lines))
elif event_name in ("task_done", "task_fail"):
    lines = [
        f"{base_title()} task {'done' if event_name == 'task_done' else 'FAILED'}",
        f"  • Task: {event.get('task', '')}",
    ]
    host = event.get("host")
    if host:
        lines.append(f"  • Host: {host}")
    rc = event.get("rc")
    if rc is not None:
        lines.append(f"  • rc: {rc}")
    log = event.get("log")
    if log:
        lines.append(f"  • Log: {log}")
    print("\n".join(lines))
else:
    counts = event.get("counts") or {}
    workers = event.get("worker_results") or []
    failed = [item for item in workers if not item.get("ok")]

    title = base_title()
    state = event.get("state")
    if state == "ok":
        title += " done"
    elif state == "stopped":
        title += " stopped"
    else:
        title += " failed"

    display = event.get("display") if isinstance(event.get("display"), dict) else {}
    comment = str(event.get("comment") or "").strip()
    if not comment:
        raw_comment = str(display.get("comment") or "").strip()
        if len(raw_comment) >= 2 and raw_comment[0] == raw_comment[-1] == '"':
            raw_comment = raw_comment[1:-1]
        comment = raw_comment

    status_line = str(event.get("status_line") or "").strip()
    if not status_line:
        progress_fields = event.get("progress_fields")
        fields = [f"tasks={counts.get('done', 0)}/{event.get('tasks', 0)}"]
        if isinstance(progress_fields, list):
            fields.extend(str(field) for field in progress_fields if str(field).strip())
        status_line = " ".join(fields)

    lines = [title, f"  • Run: {event.get('run', '')}"]
    if comment:
        lines.append(f"  • Comment: {comment}")
    if status_line:
        lines.append(f"  • {status_line}")
    if failed:
        summary = ", ".join(
            f"{item.get('host', 'worker')}:{item.get('stage', '')}:rc={item.get('rc', '')}"
            for item in failed[:6]
        )
        lines.append(f"  • Failed workers: {summary}")
    print("\n".join(lines))
PY
)
wake_message=$(
    python3 - "$payload_file" <<'PY'
import json
import math
import sys
import re
from pathlib import Path

event = json.loads(open(sys.argv[1]).read())
event_name = str(event.get("event") or "")

if event_name in ("phase_start", "phase_done"):
    phase = str(event.get("phase") or "")
    state = event.get("state")
    verb = {"running": "starting", "ok": "complete", "failed": "failed"}.get(state, event_name)
    print(f"Forge phase {verb}: phase={phase} run={event.get('run', '')}.")
elif event_name in ("task_done", "task_fail"):
    rc = event.get("rc")
    verb = "done" if event_name == "task_done" else "FAILED"
    print(f"Forge task {verb}: task={event.get('task', '')} rc={rc} log={event.get('log', '')}.")
else:
    state = event.get("state", "")
    run = event.get("run", "")
    run_dir = event.get("run_dir", "")
    counts = event.get("counts") or {}
    failed = counts.get("fail", 0)
    tasks = event.get("tasks", 0)
    done = counts.get("done", 0)
    score_re = re.compile(
        r"Score of candidate vs reference:\s+"
        r"(?P<wins>\d+)\s+-\s+(?P<losses>\d+)\s+-\s+(?P<draws>\d+)"
        r"\s+\[[^\]]+\]\s+(?P<games>\d+)"
    )

    def sprt_summary(path):
        if event.get("kind") != "sprt" or not path:
            return ""
        wins = losses = draws = games = 0
        for log in sorted((Path(path) / "outputs").glob("*/sprt.log")):
            try:
                text = log.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            matches = list(score_re.finditer(text))
            if not matches:
                continue
            match = matches[-1]
            wins += int(match.group("wins"))
            losses += int(match.group("losses"))
            draws += int(match.group("draws"))
            games += int(match.group("games"))
        if games <= 0:
            return ""
        score = (wins + 0.5 * draws) / games
        bounded = min(max(score, 1e-6), 1.0 - 1e-6)
        elo = -400.0 * math.log10(1.0 / bounded - 1.0)
        values = [1.0] * wins + [0.0] * losses + [0.5] * draws
        if games > 1:
            variance = sum((value - score) ** 2 for value in values) / (games - 1)
            stderr = math.sqrt(variance / games)
            derivative = 400.0 / (math.log(10.0) * bounded * (1.0 - bounded))
            ci95 = 1.96 * derivative * stderr
            los = 100.0 * 0.5 * (
                1.0 + math.erf(((score - 0.5) / stderr) / math.sqrt(2.0))
            ) if stderr > 0.0 else (100.0 if score > 0.5 else 0.0 if score < 0.5 else 50.0)
        else:
            ci95 = float("nan")
            los = 50.0
        return (
            f" score={wins}-{losses}-{draws}/{games} "
            f"elo={elo:.2f}+/-{ci95:.2f} los={los:.1f}%."
        )

    if state == "ok":
        verb = "complete"
    elif state == "stopped":
        verb = "stopped"
    else:
        verb = "failed"
    print(
        f"Forge run {verb}: run={run} state={state} "
        f"tasks={done}/{tasks} fail={failed}."
        f"{sprt_summary(run_dir)} log={run_dir}/deploy.log. "
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
AI_STDOUT_ENABLE=0
FORGE_EVENTS_URL=${FORGE_EVENTS_URL:-https://ntfy.wahlman.no/forge}
event_name=$(
    python3 - "$payload_file" <<'PY'
import json
import sys
print(json.loads(open(sys.argv[1]).read()).get("event", ""))
PY
)

case "$event_name" in
    task_done|task_fail) ;;
    *) send_ntfy "$FORGE_EVENTS_URL" "Forge" "$rendered" || rc=$? ;;
esac

event_selected() {
    local event="$1"
    local list
    list=$(printf '%s' "${2:-}" | tr -d '[:space:]')
    [ -n "$list" ] || return 1
    case ",$list," in
        *,"$event",*) return 0 ;;
    esac
    case "$event" in
        run_done) case ",$list," in *,done,*) return 0 ;; esac ;;
        run_fail|run_timeout|run_finalize_fail|run_launch_failed)
            case ",$list," in *,fail,*) return 0 ;; esac
            ;;
    esac
    return 1
}

if [ "$AI_STDIN_ENABLE" = "1" ] && event_selected "$event_name" "$HOOK_EVENTS"; then
    send_ntfy "$AI_STDIN_URL" "Forge" "$wake_message" || rc=$?
fi

exit "$rc"
