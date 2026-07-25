# Forge selfplay handover

Date: 2026-07-12

This handover covers the Enyo selfplay template work in Forge and the related
web graph fixes.

## Current state

Forge `main` has a working `selfplay` template and command alias:

```sh
forge selfplay ...
```

The code has been pushed and deployed to the known worker checkouts:

- `pwa-5090`
- `pwa-mbp0`
- `pwa-wrk`
- `pwa-rce`
- `pwa-wsl`

The pwa-5090 webserver was restarted after the latest graph fixes.

The handover file itself is currently an uncommitted working-tree document. It
can be kept as local handover material or committed later if desired.

## Scope and ownership rules

This work is Forge-only.

Do not move selfplay orchestration into Enyo or NNUE. Enyo owns engine code.
NNUE owns training/data tooling. Forge owns distributed orchestration, worker
setup, templates, web status, notifications, and run lifecycle.

When extending this work:

- Keep template-specific chess details in templates where possible.
- Keep generic orchestration code chess-agnostic.
- Keep web rendering generic unless the data shape is genuinely template-specific
  and would otherwise produce a bad UI, as happened with selfplay W/D/L.
- Do not copy platform-specific engine binaries between hosts.
- Prefer worker-local build/deploy for `enyo_<commit>` engines.

## Code hygiene expectations

The user strongly prefers small, direct fixes. Avoid broad rewrites.

Rules that matter in this repo:

- Work on the actual problem only. Do not touch Enyo/NNUE unless explicitly
  asked.
- Use `rg` / `rg --files` for searches.
- Use `apply_patch` for file edits.
- Do not use destructive git commands.
- Do not commit unless explicitly asked.
- Do not create feature branches unless explicitly asked. Work on `main` and
  keep changes uncommitted until told to commit/push.
- Do not add bot co-author trailers.
- Preserve unrelated untracked/local files. At the time this was written,
  `.claude/` and `del.png` were unrelated untracked files.
- Keep tests focused. Add the smallest regression test that proves the failure.
- Prefer manifest/template behavior over ad hoc wrapper scripts.
- Keep commands copy/pasteable.

For web changes:

- Verify both the backend status payload and `/api/status`.
- If `forge status --json` is correct but the web page is wrong, inspect
  `forge_lib/web.py` snapshot/lingering-run cache behavior.
- Restart the long-running webserver after web code changes.

For template changes:

- Verify `--print-manifest`.
- Verify paths are shell-safe.
- Verify input files are synchronized only when they should be.
- Verify generated commands are valid on Linux/macOS/WSL.

## Main files

- `templates/selfplay.template.json`
  - Defines the distributed Enyo selfplay template.
- `forge_lib/cli.py`
  - Adds the `forge selfplay` alias for `forge run selfplay`.
- `forge_lib/templates.py`
  - Parses selfplay parameters.
  - Rounds odd `--games` values up to the next even value.
  - Builds shell-safe `{param_shell}` values.
  - Converts repeated `--uci NAME=VALUE` options to fastchess `option.NAME=VALUE`.
- `forge_lib/status.py`
  - Parses fastchess selfplay summary lines.
  - Supports existing runs that were created with the old incorrect `Score of ...`
    regex by falling back to the actual `Games: ..., Wins: ..., Losses: ..., Draws: ...`
    output.
  - Aggregates completed selfplay shard W/D/L data into a run-level graph.
- `forge_lib/web.py`
  - Refreshes completed selfplay snapshots that were missing graph data.
  - Renders W/D/L in one compact combined graph.
  - Orders the graph and legend as W/D/L.
- `README.md`
  - Documents `forge selfplay`.
- `tests/test_forge.py`
  - Covers template generation, odd game rounding, UCI validation, input sync,
    selfplay graph parsing, stale web snapshot refresh, and W/D/L graph rendering.

## Forge configuration model

Relevant config files:

- `~/.config/forge/config.yml`
  - General defaults such as coordinator, notification command, speedup
    baseline, template dirs.
- `~/.config/forge/workers.json`
  - Default worker pool.
- `<run>/workers.json`
  - Per-run worker snapshot/override when present.

Worker precedence:

1. Explicit `--workers` on launch.
2. Per-run `<run>/workers.json` when resuming or modifying a specific run.
3. Default `~/.config/forge/workers.json`.

`forge worker set hostname:N <run>` updates the run-specific workers file. If
there is no run-specific workers file yet, it should materialize from defaults
and then apply the requested host change.

`runs/workers.json` should not be treated as the global default. The preferred
default is `~/.config/forge/workers.json`.

## Usage

Basic run:

```sh
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100000
```

With NNUE and merged PGN output:

```sh
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100000 \
  --uci nnue_file=~/code/cpp/chess/enyo/net/default.nn \
  --merge ~/assets/pgn/enyo_9696dac-selfplay.pgn
```

With explicit workers and shards:

```sh
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100000 \
  --workers pwa-5090:20,pwa-mbp:8,pwa-mbp0:10,pwa-rce:8,pwa-wsl:8 \
  --shards 200
```

Useful dry-run check:

```sh
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100 \
  --uci nnue_file=~/code/cpp/chess/enyo/net/default.nn \
  --print-manifest
```

## Defaults

The template defaults are:

- Book: `~/assets/books/AntiDraw_V2.1/WOMP_Openings_V1/WOMP_V1_+150_+159/WOMP_V1_6mvs_big_+140_+169.epd`
- TC: `10+0.1`
- Hash: `128`
- Threads: `1`
- Restart: `on`
- Book order: `random`
- Seed: `1`
- PGN notation: `uci`
- Fastchess: pinned checkout under `~/.cache/forge/tools/fastchess/...`

Odd `--games` values are rounded up to the next even number because fastchess
selfplay uses paired games with `-games 2`.

## Inputs and engine handling

The template syncs declared input files:

- Opening book.
- Any `--uci nnue_file=...` file.

Engine executables are not blindly copied between platforms.

For engines named like `enyo_<commit>`, the template uses the same worker-local
Enyo checkout/build/deploy mechanism as SPRT if the engine is missing on a
worker.

## Output

Each shard writes one PGN:

```text
{output_dir}/selfplay_0000.pgn
{output_dir}/selfplay_0001.pgn
...
```

If `--merge <path>` is supplied, Forge concatenates the shard PGNs into the
requested output.

## Progress and graph behavior

Fastchess writes selfplay result summaries like:

```text
Games: 4, Wins: 1, Losses: 1, Draws: 2, Points: 2.0 (50.00 %)
```

Forge parses those lines and exposes:

- `wins`
- `draws`
- `losses`

The web UI renders those as one compact graph in W/D/L order:

- wins: green
- draws: blue
- losses: red

Existing runs created before the regex fix are still supported by the fallback
parser in `forge_lib/status.py`.

Known limitation: fastchess emits the parsed W/D/L line at shard completion, so
graph points advance per completed shard, not necessarily after every individual
game.

## Relevant commits

- `bf12d0e feat: add Enyo selfplay template`
- `cdcc55b fix: round selfplay games to even count`
- `f234c74 fix: show selfplay progress graphs`
- `e73f9d4 fix: parse existing selfplay progress logs`
- `a27b057 fix: refresh selfplay graphs in web snapshots`
- `a542f7f fix: combine selfplay graph lines`
- `7163bc9 fix: order selfplay graph as WDL`

## Verification run

Existing run used during verification:

```text
selfplay-enyo_9696dac-100-20260712-142256
```

Verified through:

```sh
forge status selfplay-enyo_9696dac-100-20260712-142256 --json
```

Expected graph payload:

```text
x_unit=games
x_total=100
wins/draws/losses series present
44 points per series
```

The final W/D/L for that run was:

```text
wins=29
draws=41
losses=30
```

## Notifications

Forge notification support is generic. It does not know about ntfy or NotifAI
directly. Notification delivery is configured through:

- `--notify-command`
- `FORGE_NOTIFY_COMMAND`
- `notify_command` in `~/.config/forge/config.yml`

The command receives the event payload in two ways:

- JSON on stdin.
- The same JSON in `FORGE_EVENT_JSON`.

Repo-provided ntfy/NotifAI bridge:

```sh
scripts/forge_event_ntfy.sh
```

NotifAI/AI wakeups are optional. They are only relevant when the goal is to wake
an agent after a Forge event. For normal user-facing notifications, the
important part is the generic `--notify-command` / `FORGE_NOTIFY_COMMAND` /
`notify_command` mechanism.

Important defaults in that script:

```sh
AI_STDOUT_URL=${FORGE_AI_STDOUT_URL:-https://ntfy.wahlman.no/AI_stdout}
AI_STDIN_URL=${FORGE_AI_STDIN_URL:-https://ntfy.wahlman.no/AI_stdin}
AI_STDIN_ENABLE=${FORGE_AI_STDIN_ENABLE:-1}
AI_STDIN_EVENTS=${FORGE_AI_STDIN_EVENTS:-deploy_done,deploy_fail}
```

The script sources:

```sh
$HOME/.ntfy
```

It uses `NTFY_AUTH` or `LICHESS_NTFY_AUTH` if present. Do not send raw unauthenticated
requests to protected ntfy topics if credentials are required.

Typical config:

```yaml
notify_command: "/home/petter/code/cpp/chess/forge/scripts/forge_event_ntfy.sh"
```

Typical command override:

```sh
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100000 \
  --notify-command ~/code/cpp/chess/forge/scripts/forge_event_ntfy.sh
```

Optional: to wake NotifAI/Codex on selected events:

```sh
FORGE_AI_STDIN_EVENTS=deploy_done,deploy_fail,run_done,run_fail,run_stopped \
forge selfplay \
  --engine ~/assets/engines/enyo_9696dac \
  --games 100000 \
  --notify-command ~/code/cpp/chess/forge/scripts/forge_event_ntfy.sh
```

Dry-run notification rendering:

```sh
FORGE_NTFY_DRY_RUN=1 \
FORGE_EVENT_JSON='{"event":"run_done","state":"ok","run":"example","tasks":1,"counts":{"done":1}}' \
scripts/forge_event_ntfy.sh
```

Notification event names seen in Forge:

- `deploy_done`
- `deploy_fail`
- `run_done`
- `run_fail`
- `run_stopped`
- `run_finalize_fail`

Detached starts should launch a background wait monitor. The wait monitor writes
`summary.json` and sends terminal notifications if a notify command is configured.
If notifications do not arrive, inspect:

- `<run>/wait-monitor.json`
- `<run>/summary.json`
- `<run>/deploy.log`
- `FORGE_NOTIFY_COMMAND`
- `~/.config/forge/config.yml`
- `$HOME/.ntfy`

## Tests to rerun

Useful focused tests:

```sh
python3 -m unittest \
  tests.test_forge.ForgeTests.test_selfplay_template_generates_pgn_shards \
  tests.test_forge.ForgeTests.test_selfplay_template_rounds_odd_games_up \
  tests.test_forge.ForgeTests.test_selfplay_template_rejects_empty_uci_option_values \
  tests.test_forge.ForgeTests.test_selfplay_template_syncs_book_and_net_only \
  tests.test_forge.ForgeTests.test_selfplay_status_graph_aggregates_completed_fastchess_shards \
  tests.test_forge.ForgeTests.test_web_refreshes_lingering_completed_selfplay_without_graph \
  tests.test_forge.ForgeTests.test_web_renders_progress_fields
```

Compile and whitespace checks:

```sh
python3 -m py_compile forge_lib/templates.py forge_lib/status.py forge_lib/web.py tests/test_forge.py
git diff --check
```

## Host updating and live deployment

Known hosts used in this work:

- `pwa-5090`
- `pwa-mbp0`
- `pwa-wrk`
- `pwa-rce`
- `pwa-wsl`

General update command:

```sh
for h in pwa-5090 pwa-mbp0 pwa-wrk pwa-rce pwa-wsl; do
  ssh "$h" 'cd ~/code/cpp/chess/forge && git pull --ff-only && printf "%s %s\n" "$(hostname)" "$(git rev-parse --short HEAD)"'
done
```

Before assuming a worker is broken, verify:

```sh
ssh <host> 'cd ~/code/cpp/chess/forge && git status --short --branch && git rev-parse --short HEAD'
```

If a host has unrelated untracked files, do not delete them. `git pull --ff-only`
is fine when only untracked files are present and they do not conflict.

`forge update` can also be used, but direct `git pull --ff-only` has been used
successfully for this handover. If `forge update` refuses a dirty tree, inspect
the dirt rather than forcing.

## Webserver operation

Live webserver on pwa-5090:

```sh
python3 /home/petter/.local/bin/forge web --host 0.0.0.0 --port 9000 --foreground
```

Check process and port:

```sh
ssh pwa-5090 '
ps -ef | grep -E "python3 .*forge web" | grep -v grep || true
ss -ltnp 2>/dev/null | grep :9000 || true
tail -80 /tmp/forge-web-9000.log || true
'
```

Restart:

```sh
ssh pwa-5090 '
pid=$(pgrep -f "python3 /home/petter/.local/bin/forge web --host 0.0.0.0 --port 9000 --foreground" | head -1)
if [ -n "$pid" ]; then
  kill "$pid"
  for i in 1 2 3 4 5; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
fi
nohup python3 /home/petter/.local/bin/forge web --host 0.0.0.0 --port 9000 --foreground >/tmp/forge-web-9000.log 2>&1 &
'
```

Verify API graph for the selfplay test run:

```sh
ssh pwa-5090 '
curl -fsS "http://127.0.0.1:9000/api/status?restart=$(date +%s)" >/tmp/forge-api-status.json
python3 -c '"'"'
import json
data=json.load(open("/tmp/forge-api-status.json"))
r=next((x for x in data.get("runs", []) if x.get("run")=="selfplay-enyo_9696dac-100-20260712-142256"), None)
g=(r or {}).get("progress_graph")
print("graph", bool(g), g.get("x_total") if g else None, [(s.get("name"), len(s.get("points", []))) for s in (g or {}).get("series", [])])
'"'"'
'
```

To verify the live web page has the WDL ordering code:

```sh
ssh pwa-5090 '
tmp=$(mktemp)
curl -fsS http://127.0.0.1:9000/ > "$tmp"
grep -F "if (key === '\''wins'\'') return 1" "$tmp"
grep -F "if (key === '\''draws'\'') return 2" "$tmp"
grep -F "if (key === '\''losses'\'') return 3" "$tmp"
rm -f "$tmp"
'
```

## Troubleshooting checklist

No graph in the web UI:

1. Check CLI payload:

   ```sh
   forge status <run> --json
   ```

2. Check web API payload:

   ```sh
   curl -fsS http://127.0.0.1:9000/api/status | python3 -m json.tool
   ```

3. If CLI has `progress_graph` but web does not, inspect completed-run snapshot
   refresh in `forge_lib/web.py`.
4. Restart the webserver after web code changes.
5. Hard-refresh the browser.

No progress points during a selfplay run:

- Fastchess emits the parsed W/D/L summary at shard completion.
- Small shards produce more frequent graph points.
- Very long shards may show no W/D/L graph movement until the first shard
  completes.

A host does not start:

1. Check SSH:

   ```sh
   ssh <host> 'hostname; date; cd ~/code/cpp/chess/forge && git rev-parse --short HEAD'
   ```

2. Check run setup failure:

   ```sh
   forge status <run> --verbose
   ```

3. Check profile/path issues for WSL/macOS/Linux.
4. Do not copy macOS engine binaries to Linux/WSL.
5. Verify required assets exist or are declared as synced inputs.

Notifications do not fire:

1. Confirm notify command:

   ```sh
   forge config show 2>/dev/null || true
   grep -R "notify_command" ~/.config/forge 2>/dev/null || true
   ```

2. Dry-run the script with `FORGE_NTFY_DRY_RUN=1`.
3. Confirm `$HOME/.ntfy` exists and contains valid auth if required.
4. If expecting an AI/NotifAI wakeup, confirm `FORGE_AI_STDIN_EVENTS` includes
   the event. Ignore this for ordinary notifications.
5. Check `<run>/summary.json` and `<run>/wait-monitor.json`.

## Caveats

- Do not move this logic into Enyo or NNUE. The selfplay orchestration lives in
  Forge.
- Do not copy platform-specific engine binaries between hosts. Let the template
  build/deploy `enyo_<commit>` engines worker-locally when needed.
- Completed-run graph visibility depends on the web snapshot refresh logic. If a
  completed selfplay run has no graph in `/api/status` but `forge status --json`
  has one, inspect `forge_lib/web.py` snapshot refresh behavior.
