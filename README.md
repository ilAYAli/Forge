<img width="1522" height="935" alt="image" src="https://github.com/user-attachments/assets/4ea8d4a7-7abc-46a9-89b9-45338520529a" />
<p align="center">
    <sub>Forge running two distributed tasks: labeling and SPRT.</sub>
</p>

# Forge

Forge distributes manifest-defined work across workers you control.

It keeps each job in `runs/<name>/` with the materialized manifest, workers
file, logs, state files, declared outputs, and coordinator metadata. Workers
connect out through SSH/rsync; no daemon or database is required.

## Install

```sh
git clone git@github.com:ilAYAli/Forge.git ~/code/chess/forge
ln -s ~/code/chess/forge/forge.py ~/.local/bin/forge
```


example config:
~/.config/forge/config.yml
```
coordinator_host: "worker1"
notify_command: "/home/petter/code/chess/forge/scripts/forge_event_ntfy.sh"
template_dirs:
  - "~/.config/forge/templates"
```

default workers:
The name of the respective workers should likely be a hostname rather than worker1..workerN

~/.config/forge/workers.json
```
{
  "schema": "forge.workers.v1",
  "workers": [
    {
      "host": "worker1",
      "profile": "worker1",
      "resources": ["cpu"]
    },
    {
      "host": "worker2",
      "profile": "worker2",
      "python": "/opt/homebrew/bin/python3"
    },
    {
      "host": "worker3",
      "profile": "linux"
    }
  ]
}
```

## Basic Commands

```sh
forge --help
forge "cd ~/repo; git pull; cmake --build build"
forge sync ~/code/chess/enyo/net/default.net
forge sync /tmp/foo ~/bar
forge sync "$PWD/.codex/agents" ~/code/chess/nnue/
forge start manifest.json --workers worker1,worker2:50%
forge stop my-run
forge resume my-run
forge retry
forge worker set worker3:50%
forge worker set worker3:max
forge del --worker worker3
forge status
watch 'forge status --verbose'
forge update
```

Use `forge <command> --help` for full options.

`forge sync SOURCE [DESTINATION]` hashes the local file or directory contents and
synchronizes them to each selected worker host. When `DESTINATION` is omitted,
Forge uses the equivalent source path on each worker; paths beneath the
coordinator's home use each worker's own `~/`. A trailing `/` on a directory
destination places the source directory beneath it; the example above writes to
`~/code/chess/nnue/agents/`. Files whose destination already has the matching
size and SHA-256 are skipped. Extra destination files are not deleted.

## One-Shot Shell Commands

Run a shell command once selected workers:

```sh
forge hostname --workers worker1,worker3,worker2
```
or multiple commands at once using the configured workers:
```
forge '
    cd ~/path/to/forge
    git pull
    cmake --build build
'
```

`sh` waits, prints grouped output per host, and does not create run history.
Each host gets one slot unless `--workers` says otherwise.
Paths beneath the coordinator's home are rewritten relative to each worker's
home, so `forge "rm $PWD/net/file.nn"` dispatches `rm ~/code/.../net/file.nn`.
This also applies when paths are separate arguments, such as `forge ls $PWD/net`.
The `sh` prefix is optional when the complete first argument does not exactly
match a Forge command or installed template name. Forge does not split a quoted
command, so `forge "sprt foo bar"` runs that shell command while
`forge sprt ...` selects the `sprt` template.

```sh
forge sh --workers worker1,worker3,worker2 "hostname; git --version"
```

Every host runs concurrently; `--jobs N` caps how many run at once.

For long commands, `--detach` starts the command in the background on each
host and returns a job id at once. Each host keeps the job's output, pid and
exit code in `~/.forge/sh/<id>/`, so the job survives the coordinator's SSH
connection dropping. `--remote-timeout-seconds` then only bounds the launch.

```sh
forge sh --detach "cd ~/repo && git pull && cmake --build build"
forge jobs                      # recent detached jobs
forge jobs last                 # per-host status: running, ok, FAILED rc=N, LOST
forge jobs last --wait          # poll until every host finishes; rc 1 on any failure
forge jobs <id> --log worker1   # full output from one host
```

`scripts/deploy_enyo.sh <commit> --reference --detach` deploys Enyo this way.

## Start Work

```sh
forge start manifest.json --workers worker1,worker2:4
forge start manifest.json --workers workers.json --run my-run --wait
forge start manifest.json --workers worker1,worker2:4 --run my-run
forge start manifest.json --workers workers.json --merge merged.txt
forge start manifest.json --workers workers.json --replace
```

`start` materializes `runs/<run>/manifest.json` and `workers.json`, syncs inputs,
checks prerequisites, launches workers, and optionally waits/merges. Forge runs
at most one run at a time; additional runs wait in FIFO order and are shown in
status/web. Dead queue monitors are restarted when a new run is queued or the
coordinator web server starts.
Worker setup runs in parallel and waits for each host's real success, failure,
or `--remote-timeout-seconds`; a faster host never cancels slower setup.

## Templates

Templates expand into ordinary manifests. Use the live template metadata for
the current catalog, configured defaults, required arguments, and optional
parameters:

```sh
forge template list
forge template show NAME
forge run NAME --help
```

`forge template show NAME` prints the complete template definition, including
its parameters, task command, inputs, outputs, and scheduling policy. On the
orchestrator, user-specific defaults and policy overrides live in
`~/.config/forge/templates/NAME.defaults.json`; they are not read from
workers. For example, `sprt.defaults.json` can provide the default engines,
network files, game count, and chunking policy for that machine.

`forge NAME ...` is shorthand for `forge run NAME ...` when `NAME` is an
installed template. Command-line values override orchestrator-local defaults
from `~/.config/forge/templates/NAME.defaults.json`. Defaults are resolved only
on the coordinator; workers receive the resolved manifest and verified inputs.

Paths beneath the coordinator's home are mapped to the corresponding `~/` path
on each worker. Declared file inputs are content-hashed, transferred on demand,
and verified.

SPRT games run at `--tc` by default. `--depth N` or `--nodes N` replaces the
clock with a fixed search limit per move (both together apply both limits), so
the match measures the quality of the moves each side plays rather than its
speed:

```sh
forge sprt --candidate ... --games 2000 --depth 8
forge sprt --candidate ... --games 2000 --nodes 20000
```

## Status

```sh
forge status
forge status --verbose
forge status my-run --verbose
forge status --failed
forge status --done
```

Default status shows active and queued runs. `--verbose` adds pending, failed,
diagnostics, and more detail.

The web UI polls only the single live run. Completed history is loaded ten runs
at a time with `previous` and `next`; changing history pages does not affect the
live status refresh.

If a worker should read status from the coordinator by default, put a
coordinator pointer in `runs/coordinator`:

```sh
mkdir -p runs
printf '%s\n' 'coordinator.example.net:/srv/forge/runs' > runs/coordinator
```

`FORGE_RUNS_DIR` changes the local run root. `FORGE_COORDINATOR_RUNS`
overrides `runs/coordinator`.

## Reports

Templates may declare reports alongside their run configuration. The SPRT
template provides a ranking backed by `logs/sprt.db`: the latest result per
candidate (engine and net) and reference engine, by default against
`stockfish` with reference net `nn-1a298aa575a0.nnue`. Fixed-depth and
fixed-node results are recorded but left out of the ranking:

```sh
forge report sprt score
forge report sprt score --verbose                     # add each result's run name
forge report sprt score --reference-engine '*'
forge report sprt score --reference-net reference.nn --candidate-net 'enyo-*'
# Reference net filters accept regexes or shell-style '*' wildcards.
forge report sprt score --reference-net 'nn-*.nnue' --engine '*'
```

The same report is available at `/reports/sprt` when `forge web` is running.
Its filters and tables load separately from status, so reports do not add work
to the normal status refresh loop. `FORGE_SPRT_DB` overrides the database path.

## Stop And Resume

```sh
forge stop my-run
forge resume my-run
```

`stop` prevents new claims, terminates every local and remote worker slot,
releases incomplete active claims, and removes transient state. Completed tasks
and the materialized run are preserved. For a queued run, `stop` removes it
from the queue; use `forge del my-run --force` to delete it entirely. Templates
may retain checkpoint outputs; other incomplete task outputs are removed.
`resume` restarts incomplete non-failed work using `<run>/workers.json` when
present, otherwise `~/.config/forge/workers.json`.
Use `retry` for failed tasks.

The SPRT template preserves every completed game pair when stopped. Resume
seals those partial shards and launches new shards for only the remaining
games. If the preserved files cannot account for all recorded games, Forge
refuses to resume instead of silently restarting from zero.

Ctrl+C during setup or `--wait` performs the same cleanup before returning
status 130. Coordinator temporary files are removed, and the stopped run can be
continued with `forge resume <run>` without duplicate output.

Limit a host while you need it for something else:

```sh
forge worker set worker3:0 --force           # out of Forge now
forge worker set worker3:50% --reason lichess
forge worker set worker3:4 --for 3h          # a four-thread budget for three hours
forge worker set worker3:max                 # back to its full detected capacity
forge worker set worker3:50% my-run          # only that run
forge worker set aws                         # rent cloud cores for the active run
```

Without a run name the value is the host's limit: it applies to the active
run at once and to every run started until it is changed, `host:max` clears it,
and `--for` clears it automatically (the run's wait monitor does this). The
limit is kept on the coordinator, not in `workers.json`, and `forge status`
lists it with its `--reason`. With a run name only that run changes.

Changing a run resizes the host from its detected hardware (see
[Capacity](#capacity)). Missing worker processes are started. Lowering the
share drains the extra processes: they finish their current task, stop
claiming, and exit. `--force` kills them instead, and also allows a thread
count above the detected hardware. A lichess bot, say, runs
`forge worker set <host>:0 --force` when it starts and
`forge worker set <host>:max` when it exits.

Drain a worker from a run:

```sh
forge del --worker worker3
forge del --worker worker3 my-run
forge worker del worker3 my-run
```

`del --worker` drains the host: no new claims, reserved claims released, running
tasks left alone. Add `--keep-claims` to keep already reserved claims. Without
`--worker`, `del` deletes runs:

```sh
forge del my-run
forge del --done
forge del --all
```

`del --all` deletes completed, failed, stopped, and empty runs while keeping
current, queued, and stalled runs. Deletion prompts for confirmation unless
`--force` is supplied.

## Retry Failed Work

```sh
forge retry
forge retry my-run
forge retry my-run --workers worker3,worker4:4
forge retry my-run --workers alternate-workers.json
```

`retry` requeues failed tasks and skips valid completed outputs.

If a dead worker left live claims behind, release that worker before retrying:

```sh
forge release --manifest runs/my-run/manifest.json --worker worker-name
```

## Workers

Inline workers are comma-separated. A `:` suffix sets the host's utilization
for that invocation: `:50%` is a share of the host, `:8` a hardware-thread
budget, `:max` the whole host, and `:0` omits the host.

```sh
forge start manifest.json --workers worker1,worker2
forge start manifest.json --workers worker1:50%,worker2:8
forge start manifest.json --workers worker1=linux:max,worker2=mac:25%
forge start manifest.json --workers worker1:0,worker2
```

Workers files as JSON:

```json
{
  "schema": "forge.workers.v1",
  "workers": [
    "worker1",
    {
      "host": "worker2",
      "profile": "linux"
    },
    {
      "host": "worker3",
      "profile": "mac",
      "python": "/opt/homebrew/bin/python3"
    },
    {
      "host": "worker4",
      "enabled": false
    },
    {
      "host": "aws",
      "cloud": {"profile": "forge", "region": "eu-north-1", "family": "c7a"},
      "cores": "max",
      "enabled": false
    }
  ]
}
```

Object workers support `host`, `profile`, `python`, `local`, `cache_dir`,
`resources`, `env`, and `enabled`. The file lists the pool's hosts and how to
reach them; it holds no capacity numbers (Forge measures each host, and
`utilization` or `slots` values in it are ignored with a warning; use
`forge worker set host:N` for a temporary limit). `resources` is a non-empty list
containing `cpu` and/or `gpu`; omitted workers are CPU-only. `env` maps shell
variable names to literal string values and is exported for every Forge command
on that worker (for example, a CUDA toolkit path). Inline entries inherit matching
worker-file defaults. An entry with a `cloud` object is a [cloud pool](#cloud-workers),
not a host.

## Cloud Workers

A cloud pool rents workers for a run and returns them when it ends:

```sh
forge sprt ... --workers aws                # default workers + the pool's cores
forge sprt ... --workers aws:16             # default workers + 16 cloud cores
forge sprt ... --workers pwa-llm,aws:128    # pwa-llm + 128 cloud cores
forge worker set aws                        # add the pool's cores to the active run
forge worker set aws:192 my-run             # resize a running run's cloud share
forge worker set aws:0 my-run               # back to the home workers only
forge cloud ls                              # instances, their runs and state
forge cloud down                            # terminate every cloud worker now
```

`N` in `aws:N` is physical cores (one SPRT game each); `aws:max` is whatever the
account's quota still allows, and a bare `aws` uses the pool's `cores` (a number,
or `"max"` for the whole free quota: cost barely depends on the core count, see
below). The run starts on the other workers immediately; each instance joins it
when it is ready, about two minutes after launch (`runs/<run>/cloud.log`). Forge picks the
fewest instances that cover `N` (64 cores is one `c7a.16xlarge`, 80 is a
`16xlarge` and a `4xlarge`). A pool with `"enabled": true` is added to every run
that uses the default workers, labeling included, so it is off by default.

Instances are terminated when their run finishes, is stopped or deleted. As a
safety net each one also powers itself off (which terminates it) after
`idle_minutes` without Forge processes and after `max_hours` in any case, so a
coordinator that goes away never leaves one running.

### Setup

The pool is reached over [Tailscale](https://tailscale.com); instances accept
no inbound traffic. On the coordinator:

1. Install the AWS CLI v2 and Tailscale (`tailscale up`).
2. Create an IAM user with `AmazonEC2FullAccess` and `ServiceQuotasFullAccess`
   and an access key, then `aws configure --profile forge`.
3. In the Tailscale admin console add `"tag:forge-cloud"` to `tagOwners` and
   create a *reusable*, *ephemeral* auth key with that tag. Save it to
   `~/.config/forge/tailscale.key` (mode 600).
4. Add the pool to `~/.config/forge/workers.json` (above) and check the quota:
   EC2 *Running On-Demand Standard instances* (or *Spot Instance Requests*) in
   the region, in vCPUs.

The first launch creates a `forge-cloud` security group without inbound rules,
an SSH key the instances use to reach the coordinator (`~/.ssh/forge_cloud`,
accepted only from Tailscale addresses), and an `Include` of
`~/.ssh/forge-cloud.conf` at the top of `~/.ssh/config`.

`cloud` keys (all optional):

| Key | Default | Meaning |
|---|---|---|
| `profile`, `region` | `forge`, `eu-north-1` | AWS CLI profile and region |
| `family` | `c7a` | Instance family; sizes come from AWS. c7a has one core per vCPU |
| `market` | `on-demand` | `on-demand` or `spot` |
| `idle_minutes`, `max_hours` | `15`, `12` | Self-termination limits |
| `sync` | `["~/.cache/forge/tools"]` | Paths copied to each instance |
| `engines` | candidate and reference links | Engine links to install |
| `engine_repo`, `engine_net` | `~/code/chess/enyo`, `~/assets/nets/reference.net` | Engine sources and the net they embed |
| `tailscale_key`, `user`, `worker_profile` | `~/.config/forge/tailscale.key`, `$USER`, `linux` | Access and the profile workers run with |

Engines built with `-march=native` on the coordinator may use instructions the
cloud CPU lacks (a Zen 5 build crashes on Zen 4). Each `enyo_<commit>` an engine
link points at is therefore built once on a cloud instance with the repo's
`scripts/deploy.sh`, cached in `~/.cache/forge/cloud/engines/<family>/`, and
copied to later instances of that family; the first build adds a few minutes.

Costs are per second while an instance runs, so a run costs its core-hours
whether they come from 10 cores or 100; only each instance's two-minute start
and the last shards' tail grow with the core count. As a guide, a 5k-game SPRT
at 10+0.1 is about 30 core-hours: on-demand c7a is roughly 0.05 USD per
core-hour (about 1.5 USD, some 15% more on 100 cores than on 10) and spot about
a quarter of that.

## Capacity

Forge sizes each host from its own hardware, and nothing about the hardware
goes in `workers.json`. When a run starts, every worker reports its hardware
threads, physical cores, performance cores, and memory: `lscpu`,
`/sys/devices/cpu_core/cpus` (Intel hybrid), cgroup v2 limits and
`/proc/meminfo` on Linux, `sysctl` on macOS. Results are cached for
ten minutes in `~/.cache/forge/hardware/`.

Each manifest declares what one task costs:

```json
"resources": {
  "timing": "timed",
  "threads": "{threads}*{concurrency}",
  "memory_mb": "2*({hash}+64)*{concurrency}+64"
}
```

- `timing: "timed"` (SPRT, SPSA): every engine thread needs a whole physical
  core, so on an SMT host it costs two hardware threads, and on hybrid CPUs
  (Apple Silicon, Intel Core Ultra) only performance cores count.
- `timing: "fixed"` (labeling, datagen, fixed-node selfplay): every engine
  thread costs one hardware thread. `"timed_unless": ["nodes", "depth"]`
  treats the task as fixed when either limit is set.
- `threads` and `memory_mb` are arithmetic expressions over the manifest's
  variables (`+ - * / //` and parentheses only). A manifest without
  `resources` costs one fixed thread and declares no memory.

A host's share (100% unless a run's `--workers host:…` suffix or
`forge worker set host:N my-run` says otherwise) is then turned into a slot
count: the threads or cores in that share, divided by the per-task cost, and
capped so the tasks fit in 90% of the host's memory share. A host limit
(`forge worker set host:N` without a run) caps it further. Any share above zero runs at least one task.

Example on a 12-core/24-thread Linux host and a 10P+4E Mac:

| Utilization | Labeling (fixed x1) | SPRT (timed x1) |
|---|---|---|
| Linux `100%` | 24 | 12 |
| Linux `50%` | 12 | 6 |
| Mac `100%` | 14 | 10 |

While a run is active, each worker also:

- waits before claiming a task when free memory is less than the task's
  `memory_mb` plus a reserve (5% of RAM, at least 512 MiB);
- starts fixed tasks at the lowest CPU priority (`nice 19`, plus `SCHED_IDLE`
  on Linux), so they give way to other work on the host; timed tasks keep
  normal priority because CPU contention would change their results;
- before claiming a timed task (SPRT/SPSA games), waits until the host's
  performance cores have room for it: CPU used by non-Forge processes (peak
  over the last minute, ignoring processes under a quarter core), plus the
  Forge games already running there, plus the new task. Playing a lichess game
  on a worker therefore shrinks Forge's share as its current games finish, and
  Forge takes the cores back when the host is idle again. Utilization stays the
  upper limit. `FORGE_CPU_GATE=0` disables the check;
- on Linux, marks every task as the OOM killer's first choice
  (`oom_score_adj` 1000), lowering the score by 10 per minute of runtime down
  to 500, so a kill hits the task with the least completed work.

## Manifests

A manifest declares prerequisites, path mappings, tasks, progress parsing, and
outputs. `forge start` materializes the template into `runs/<run>/manifest.json`
before work starts.

Minimal setup-only manifest:

```json
{
  "schema": "forge.task.v1",
  "name": "project-deploy",
  "work_dir": "~/work/project",
  "requirements": {
    "python_min": "3.11",
    "git_min": "2.38",
    "repos": [
      {
        "name": "project",
        "url": "git@example.com:owner/project.git",
        "path": "~/work/project",
        "remote": "origin",
        "commit": "REPLACE_WITH_COMMIT_SHA",
        "build": [
          "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release",
          "cmake --build build -j8"
        ],
        "probe": [
          "test -x build/project"
        ]
      }
    ]
  },
  "tasks": []
}
```

If `tasks` is empty, `start` verifies setup and stops. If workers use different
paths, add `path_maps` and use matching worker profiles. Templates should use
portable profile names such as `linux` or `macos`; machine-specific mappings
belong in the user-owned worker configuration.

For command requirements, use `requirements.commands`. For tools built from Git,
use `requirements.repos` with a pinned `commit` plus a `probe` that proves the
needed runtime behavior. Failed probes stop work before any task is assigned.

## Planning Tasks

`plan` is a manifest generator. It is useful, but it is not required if another
tool already writes manifests.

```sh
forge plan \
  --name batch-transform \
  --description 'batch transform' \
  --task-label transform \
  --shards 24 \
  --work-dir ~/work/project \
  --manifest manifest.json \
  --command 'tool --shard {index} --shards {shards} --out {output}' \
  --output-template '{output_dir}/shard.{index}.txt'
```

Add prerequisites without hand-editing JSON:

```sh
forge add-repo \
  --manifest manifest.json \
  --name tool \
  --path ~/work/tool \
  --url git@example.com:owner/tool.git \
  --remote origin \
  --commit REPLACE_WITH_COMMIT_SHA \
  --probe '~/work/tool/tool --help | grep -- "--needed-option"'

forge add-command \
  --manifest manifest.json \
  --name external-tool \
  --path ~/.local/bin/external-tool \
  --command '~/.local/bin/external-tool --version' \
  --contains 'External Tool'
```

Use `--task-label` so status can show readable task names. Use progress regex
options when long-running tools emit parseable progress.

## Notifications

`run`, `start`, `deploy`, and `wait` support `--notify-command`,
`FORGE_NOTIFY_COMMAND`, or `notify_command` in
`~/.config/forge/config.yml`. The command receives one JSON payload on stdin
and in `FORGE_EVENT_JSON`. Forge does not assume any notification service.

For long jobs, make each task write declared log outputs, add a task label, add
progress parsing when possible, and pass a notification command so completion or
failure is reported without polling.

When `wait` sees a terminal state, Forge writes `<run>/summary.json` with the
same completion payload sent to notifications, including the final status line.
Detached starts run a background wait monitor so completed runs still get that
summary even when no notification command is configured.

### Events

Every payload has an `"event"` field. Three families exist, fired from
different places and at different granularity:

| Event | Fired by | When |
| --- | --- | --- |
| `run_done` | `wait` | The run reached a terminal state with no failed tasks. |
| `run_fail` | `wait` | The run reached a terminal state with failed tasks. |
| `run_stopped` | `wait` | The run was stopped (e.g. `forge stop`). |
| `run_timeout` | `wait` | `--timeout-seconds` elapsed before the run finished. |
| `run_finalize_fail` | `wait` | Tasks finished but the manifest's `finalize` step raised. |
| `run_launch_failed` | `wait` | Workers never reached a running state. |
| `deploy_done` / `deploy_fail` | `deploy` | The `deploy` invocation itself finished (setup + launch + tasks, when not deferred to a wait monitor). |
| `phase_start` / `phase_done` | `deploy` | A coordinator-level sub-stage of `deploy` started/finished. Currently emitted once around the worker **setup** stage (source sync, dependency install/build), tagged `"phase": "setup"`, since that step can take real wall-clock time before any task runs. Suppressed when the run has no tasks, or when completion is deferred to a background wait monitor (`--launch-only` with `--start-wait-monitor`). |
| `task_done` / `task_fail` | `work` (per worker) | One shard/task finished. See [Per-task hook](#per-task-hook). |

The run- and deploy-level events fire once, from the coordinator process (or
wherever `deploy`/`wait` itself runs — see the coordinator note below).
`task_done`/`task_fail` fire once per task, from wherever that task actually
ran, which on a remote worker is that worker's own environment, not the
coordinator's.

When `coordinator_host` is configured in `~/.config/forge/config.yml`,
user-facing Forge commands run on that host, so worker selection, configuration,
run state, builds, and status all use the coordinator's view. `--help`,
`--version`, and worker-internal commands stay local. `sync` stages a local
source on the coordinator; `start` and `deploy` stage their manifest and worker
file when needed. Other file paths must be accessible from the coordinator.
Set `FORGE_DISABLE_COORDINATOR_DISPATCH=1` to run a command locally for
debugging or isolated tests.
An SSH child does not inherit the caller's shell environment, so configure
settings such as `notify_command` in the coordinator's config file or pass
supported command options explicitly.

### Per-task hook

`work` (and anything that deploys workers — `run`/`selfplay`/`start`/`resume`/
`retry`/`worker set`) also supports `--task-complete-command`,
`FORGE_TASK_COMPLETE_COMMAND`, or `task_complete_command` in
`~/.config/forge/config.yml`, which runs after *each* task/shard finishes,
not just at the end of the whole run. It gets the same
`FORGE_EVENT_JSON`/stdin payload convention as `--notify-command`, tagged
with `"event": "task_done"` or `"task_fail"`, plus `progress_data` when
available.

Task-level notifications are opt-in. `notify_command` handles run-level
events only; configure `--task-complete-command`,
`FORGE_TASK_COMPLETE_COMMAND`, or `task_complete_command` explicitly when a
consumer actually needs one event per shard.

By default this fires fire-and-forget: the worker does not wait for the
command to finish before claiming its next task, so shard throughput never
blocks on it. Pass `--task-complete-sync` to wait for it instead.

**Concurrency is your responsibility.** With dozens of parallel shards
finishing close together, fire-and-forget means multiple invocations of your
command can run at the same time. Forge makes no serialization guarantee
across them. If your command only appends to a file or otherwise doesn't
share mutable state between invocations, this is harmless. If it maintains
shared state on disk (e.g. "which shards have I already converted" tracked in
a JSON file, with output appended to a shared file), concurrent invocations
can race on that state: two invocations can both read the same "already
processed" list, each append a different new shard's data, and each write
back state that clobbers the other's update — silently dropping one shard
from tracked state while its data is already in the output (and duplicating
it on a later re-run that reprocesses the "missing" shard). Guard against
this in the consumer, not in Forge: take an exclusive file lock (e.g.
`fcntl.flock` on a lock file next to the output) around the whole
read-state → process → append → write-state sequence, and save state
incrementally rather than only once at the end, so a killed process doesn't
lose track of work it already appended. Use `--task-complete-sync` instead if
you'd rather trade shard throughput for simplicity.

**High task counts mean high event counts.** A run chunked into hundreds or
thousands of shards fires `task_done`/`task_fail` that many times when a
`task_complete_command` is explicitly configured.

### Reference hook script and filtering

`scripts/forge_event_ntfy.sh` is a working example: it renders event payloads
into short [ntfy](https://ntfy.sh) messages. Its base channel ignores
`task_done`/`task_fail` so a mistakenly configured per-task hook cannot flood
the queue.

It separately supports an opt-in `HOOK_EVENTS` environment variable that
gates a second, interruptive notification on top of the base one (e.g. to
wake a human or an agent, rather than just log). `HOOK_EVENTS` is a
comma-separated list matched against `"event"`; entries ending in `_done` or
run-level completion events also match bare `done`/`fail`; task and deploy
events require their explicit names. This mirrors the
`NNUE_HOOK_EVENTS` convention in the sibling `nnue` pipeline
(`tools/events/nnue_event_ntfy.sh`), which uses the same `phase_start`/
`phase_done`/`done`/`fail` vocabulary for its own (unrelated) training
pipeline stages.

## Updates

`update` uses `~/.config/forge/workers.json` by default.
It updates workers to the coordinator's exact current Forge commit.

```sh
forge update
forge update worker1,worker2:4
forge update workers.json
forge update --all
```

With `--all`, the coordinator fast-forwards first, then workers are matched to
the new coordinator commit.

## Advanced Commands

Lower-level commands remain available for diagnostics and automation:

```sh
forge deploy workers.json manifest.json
forge wait --manifest runs/my-run/manifest.json
forge verify my-run
forge merge my-run --output merged.txt
forge stop my-run
forge doctor --manifest runs/my-run/manifest.json
forge sync-repos --manifest runs/my-run/manifest.json --profile worker1
forge work --coordinator host:/path/to/manifest.json --profile worker1
forge run-task --manifest runs/my-run/manifest.json --index 0
```

`deploy` materializes and starts work. `start` is the normal command because it
also wires together wait, verify, and merge.

## Failure Model

- A task is complete only when its `.done.json` and declared outputs exist.
- Output checksums are verified before merge.
- Failed tasks are not merged.
- Active claims prevent duplicate work.
- Stale claims can be reclaimed after their lease expires.
- Wrong Git commits, missing commands, or mismatched input checksums fail before
  a task command starts.
- Ctrl+C propagates through the coordinator, kills every detached worker
  process group, allows checkpoint files to synchronize, and resets interrupted
  tasks to pending. Checkpoint-enabled templates preserve recoverable progress;
  other partial outputs are removed.

## Examples

Example manifests live under `examples/`. They are samples only; Forge's task
model is domain-agnostic.
