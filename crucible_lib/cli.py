from __future__ import annotations

from typing import Any

from .core import *
from .manifest import *
from .tasks import *
from .status import *
from .workers import *

try:
    from .web import cmd_web
except ModuleNotFoundError as exc:
    if exc.name != "crucible_lib.web":
        raise
    cmd_web = None


WORKERS_ARG_HELP = (
    "workers.json file or comma-separated workers, e.g. worker-a,worker-b:4,worker-c:0."
)
WORKERS_ARG_HELP_WITH_DEFAULT = (
    WORKERS_ARG_HELP + " Defaults to runs/workers.json, then ./workers.json."
)
WORKERS_ARG_HELP_RUN_DEFAULT = (
    WORKERS_ARG_HELP + " Defaults to the run's workers.json."
)
WORKERS_ARG_HELP_RETRY_DEFAULT = (
    WORKERS_ARG_HELP + " Defaults to each run's workers.json."
)


COMMON_COMMANDS = [
    "start",
    "add",
    "del",
    "stop",
    "resume",
    "retry",
    "worker",
    "status",
    "update",
]
if cmd_web is not None:
    COMMON_COMMANDS.insert(6, "web")


COMMAND_GROUPS = [
    (
        "Common",
        COMMON_COMMANDS,
    ),
    (
        "Advanced",
        [
            "plan",
            "add-input",
            "add-repo",
            "add-command",
            "deploy",
            "doctor",
            "sync-repos",
            "work",
            "run-task",
            "claim",
            "release",
            "heartbeat",
            "drain",
            "wait",
            "verify",
            "merge",
            "self-update",
            "version",
        ],
    ),
]


class CrucibleHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


class CrucibleArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.command_groups = kwargs.pop("command_groups", [])
        self.command_help: dict[str, str] = {}
        super().__init__(*args, **kwargs)

    def format_help(self) -> str:
        if not self.command_groups:
            return super().format_help()

        formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions, self._mutually_exclusive_groups)
        formatter.add_text(self.description)
        for title, commands in self.command_groups:
            formatter.start_section(title)
            formatter.add_text(self._format_command_group(commands))
            formatter.end_section()
        for group in self._action_groups:
            actions = [
                action
                for action in group._group_actions
                if not isinstance(action, argparse._SubParsersAction)
                and action.help is not argparse.SUPPRESS
            ]
            if actions:
                formatter.start_section(group.title)
                formatter.add_arguments(actions)
                formatter.end_section()
        return formatter.format_help()

    def _format_command_group(self, commands: list[str]) -> str:
        width = max(len(command) for command in commands)
        return "\n".join(
            f"  {command:<{width}}  {self.command_help.get(command, '')}"
            for command in commands
        )


def add_worker_add_options(parser: argparse.ArgumentParser, *, worker_positional: bool) -> None:
    if worker_positional:
        parser.add_argument("worker", help="Inline worker spec, e.g. worker-a or worker-a:5.")
    else:
        parser.add_argument("--worker", required=True, help="Inline worker spec, e.g. worker-a or worker-a:5.")
    parser.add_argument("run", nargs="?", help="Run name or run directory. Omit to use the single active run.")
    parser.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    parser.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    parser.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    parser.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Poll the materialized run after workers exit and fail on incomplete or failed tasks.",
    )
    parser.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    parser.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    parser.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True, help="Verify task completion and outputs before returning success.")
    parser.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    parser.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    parser.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    parser.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with CRUCIBLE_EVENT_JSON and JSON on stdin.",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    parser.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")


def cmd_del_or_worker(args: argparse.Namespace) -> int:
    if getattr(args, "worker", None):
        if bool(getattr(args, "done", False)):
            raise SystemExit("del --worker cannot be used with --done")
        if bool(getattr(args, "force", False)):
            raise SystemExit("del --worker cannot be used with --force")
        return cmd_worker_del(args)
    if bool(getattr(args, "keep_claims", False)):
        raise SystemExit("del --keep-claims requires --worker")
    return cmd_del(args)


def build_parser() -> argparse.ArgumentParser:
    parser = CrucibleArgumentParser(
        description=(
            "Crucible distributes manifest-defined work across workers. "
            "Start with start, add, del, stop, resume, retry, status, and update."
        ),
        formatter_class=CrucibleHelpFormatter,
        command_groups=COMMAND_GROUPS,
    )
    parser.add_argument("--version", action="store_true", help="Print crucible version and exit.")
    parser.add_argument("--version-json", action="store_true", help="Print crucible version as JSON and exit.")
    sub = parser.add_subparsers(dest="subcommand", metavar="command")

    plan = sub.add_parser(
        "plan",
        help="Create a numbered task manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    plan.add_argument("--name", required=True, help="Run name stored in the manifest.")
    plan.add_argument("--kind", help="Short user-defined run type shown in status.")
    plan.add_argument("--description", help="Short single-line run description shown in status.")
    plan.add_argument(
        "--task-label",
        help="Short per-task label shown in detailed status. Supports templates.",
    )
    plan.add_argument(
        "--tasks", "--shards",
        dest="shards",
        metavar="N",
        type=int,
        required=True,
        help="Number of numbered tasks to create.",
    )
    plan.add_argument("--work-dir", required=True, help="Working directory for task commands.")
    plan.add_argument("--out", "--manifest", dest="out", required=True, help="Manifest path to write.")
    plan.add_argument(
        "--command-template", "--command",
        dest="command_template",
        required=True,
        help="Shell command template. Supports {index}, {shard}, {task_id}, {output}, and --var keys.",
    )
    plan.add_argument(
        "--output-template",
        action="append",
        required=True,
        help="Output path template for each task. Repeat for multiple outputs.",
    )
    plan.add_argument("--output-dir", help="Value for {output_dir}; defaults to the materialized run outputs directory.")
    plan.add_argument("--var", action="append", default=[], help="Template variable KEY=VALUE. Repeatable.")
    plan.add_argument(
        "--path-map",
        action="append",
        default=[],
        help="Host path mapping, PROFILE:/source=/dest. Repeatable.",
    )
    plan.add_argument("--task-prefix", default="task", help="Prefix for generated task ids.")
    plan.add_argument("--state-dir", help="Directory for .done/.fail/.running state files.")
    plan.add_argument("--log-dir", help="Directory for task logs.")
    plan.add_argument("--python-min", default="3.11", help="Minimum Python version required on workers.")
    plan.add_argument("--git-min", default="2.38", help="Minimum Git version required on workers.")
    plan.add_argument("--git-repo", help="Git checkout to pin; defaults to --work-dir.")
    plan.add_argument("--git-commit", help="Required Git commit; defaults to current HEAD.")
    plan.add_argument("--no-git-requirement", action="store_true", help="Do not pin a git checkout in the manifest.")
    plan.add_argument("--require-clean-git", action="store_true", help="Require a clean pinned Git checkout.")
    plan.add_argument("--progress-unit", help="Unit name for declared task progress, for example rows or games.")
    plan.add_argument("--progress-total", help="Per-task total for declared progress. Supports templates.")
    plan.add_argument(
        "--progress-total-lines",
        help="Count lines in this file and split the total across generated tasks.",
    )
    plan.add_argument(
        "--progress-log-regex", "--progress-regex",
        dest="progress_log_regex",
        help="Regex read from task logs. Use named group (?P<done>...), optional total and output.",
    )
    plan.add_argument("--progress-output-unit", help="Label for optional (?P<output>...) progress value.")
    plan.set_defaults(func=cmd_plan)

    add_input = sub.add_parser(
        "add-input",
        help="Record input file checksums.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_input.add_argument("--manifest", required=True, help="Manifest to update.")
    add_input.add_argument("--name", help="Human-readable input label.")
    add_input.add_argument("--path", action="append", required=True, help="Input file to checksum. Repeatable.")
    add_input.set_defaults(func=cmd_add_input)

    add_repo = sub.add_parser(
        "add-repo",
        help="Record a git checkout requirement.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_repo.add_argument("--manifest", required=True, help="Manifest to update.")
    add_repo.add_argument("--path", required=True, help="Worker-local checkout path.")
    add_repo.add_argument("--name", help="Human-readable repository label.")
    add_repo.add_argument("--url", help="Clone URL used when the checkout is missing.")
    add_repo.add_argument("--commit", help="Required commit, short or full SHA.")
    add_repo.add_argument("--branch", help="Branch to checkout before optional commit pinning.")
    add_repo.add_argument("--remote", help="Remote to fetch/pull before checkout.")
    add_repo.add_argument("--require-clean", action="store_true", help="Require a clean worktree.")
    add_repo.add_argument("--allow-dirty", action="store_true", help="Allow dirty worktree during sync-repos.")
    add_repo.add_argument("--build", action="append", help="Build command run after checkout. Repeatable.")
    add_repo.add_argument("--install", action="append", help="Install command run after build. Repeatable.")
    add_repo.add_argument("--probe", action="append", help="Probe command that must pass after install. Repeatable.")
    add_repo.add_argument("--build-timeout-s", type=float, help="Timeout for each build command.")
    add_repo.add_argument("--install-timeout-s", type=float, help="Timeout for each install command.")
    add_repo.add_argument("--probe-timeout-s", type=float, help="Timeout for each probe command.")
    add_repo.set_defaults(func=cmd_add_repo)

    add_command = sub.add_parser(
        "add-command",
        help="Record a command or executable requirement.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_command.add_argument("--manifest", required=True, help="Manifest to update.")
    add_command.add_argument("--name", help="Human-readable command label.")
    add_command.add_argument("--path", help="Executable path or PATH lookup name.")
    add_command.add_argument("--command", help="Probe command to run.")
    add_command.add_argument("--contains", help="Required substring in probe output.")
    add_command.add_argument("--regex", help="Required regex in probe output.")
    add_command.add_argument("--rc", type=int, default=0, help="Required probe exit code.")
    add_command.add_argument("--timeout-s", type=float, help="Probe timeout.")
    add_command.add_argument("--profile", action="append", help="Limit requirement to a worker profile. Repeatable.")
    add_command.set_defaults(func=cmd_add_command)

    sync_repos = sub.add_parser(
        "sync-repos",
        help="Fetch and checkout declared git requirements.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sync_repos.add_argument("--manifest", required=True, help="Manifest containing repo requirements.")
    sync_repos.add_argument("--profile", help="Path-map/profile name for this worker.")
    sync_repos.set_defaults(func=cmd_sync_repos)

    run_task = sub.add_parser(
        "run-task",
        help="Run one task by id or index.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    run_task.add_argument("--manifest", required=True, help="Manifest containing the task.")
    run_task.add_argument("--task", help="Task id to run.")
    run_task.add_argument("--index", type=int, help="Task index to run.")
    run_task.add_argument("--profile", help="Path-map/profile name for this worker.")
    run_task.add_argument("--force", action="store_true", help="Re-run even if the task is complete.")
    run_task.set_defaults(func=cmd_run)

    claim = sub.add_parser(
        "claim",
        help="Atomically claim the next coordinator task.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    claim.add_argument("--manifest", required=True, help="Coordinator manifest.")
    claim.add_argument("--worker", required=True, help="Worker name for the claim.")
    claim.add_argument("--profile", help="Profile name recorded with the claim.")
    claim.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    claim.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    claim.add_argument("--max-attempts", type=int, default=0, help="Maximum total attempts for failed-task retry; 0 means no cap.")
    claim.add_argument("--json", action="store_true", help="Print claim payload as JSON.")
    claim.set_defaults(func=cmd_claim)

    release = sub.add_parser(
        "release",
        help="Release coordinator task claims.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    release.add_argument("--manifest", required=True, help="Coordinator manifest.")
    release.add_argument("--task", help="Task id whose claim should be released.")
    release.add_argument("--worker", help="Expected worker owner, or worker whose reserved claims should be released.")
    release.add_argument("--claim-id", help="Expected claim id.")
    release.add_argument("--count", type=int, default=0, help="Maximum worker claims to release; 0 releases all reserved claims.")
    release.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    release.set_defaults(func=cmd_release)

    heartbeat = sub.add_parser(
        "heartbeat",
        help="Refresh a coordinator task claim heartbeat.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    heartbeat.add_argument("--manifest", required=True, help="Coordinator manifest.")
    heartbeat.add_argument("--task", required=True, help="Task id to refresh.")
    heartbeat.add_argument("--worker", required=True, help="Expected worker owner.")
    heartbeat.add_argument("--claim-id", help="Expected claim id.")
    heartbeat.add_argument("--progress-json", help="Optional JSON progress object to store with the heartbeat.")
    heartbeat.add_argument("--running", action="store_true", help="Mark the task as running on the coordinator.")
    heartbeat.set_defaults(func=cmd_heartbeat)

    work = sub.add_parser(
        "work",
        help="Claim, run, sync, and repeat worker tasks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    work.add_argument("--manifest", required=True, help="Worker-local manifest copy.")
    work.add_argument(
        "--coordinator",
        required=True,
        help="Coordinator manifest path, either /path/manifest.json or host:/path/manifest.json.",
    )
    work.add_argument("--profile", help="Path-map/profile name for this worker.")
    work.add_argument("--worker", default=socket.gethostname(), help="Worker name recorded in claims.")
    work.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    work.add_argument("--max-tasks", type=int, default=0, help="Maximum tasks to run; 0 means until no work remains.")
    work.add_argument("--idle-seconds", type=float, default=0.0, help="Wait this long for more coordinator work after a no-claim result.")
    work.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    work.add_argument("--task-attempts", type=int, default=1, help="Total attempts per task before final failure.")
    work.add_argument("--force", action="store_true", help="Re-run tasks even if local state says complete.")
    work.add_argument("--fail-fast", action="store_true", help="Stop after the first task failure.")
    work.set_defaults(func=cmd_work)

    doctor = sub.add_parser(
        "doctor",
        help="Check host prerequisites for a manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    doctor.add_argument("--manifest", required=True, help="Manifest to validate.")
    doctor.add_argument("--profile", help="Path-map/profile name for this host.")
    doctor.add_argument("--role", choices=["worker", "coordinator"], default="worker", help="Host role to validate.")
    doctor.add_argument("--notify-command", help="Coordinator notification command to check.")
    doctor.add_argument(
        "--coordinator",
        help="Coordinator manifest path or host:path to verify worker callback access.",
    )
    doctor.set_defaults(func=cmd_doctor)

    status = sub.add_parser(
        "status",
        help="Show run or task status.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    status.add_argument(
        "run",
        nargs="?",
        help="Run name or run directory. Omit to show active runs.",
    )
    status.add_argument("-v", "--verbose", action="store_true", help="Show detailed task sections and diagnostics.")
    status.add_argument("--show-done", action="store_true", help="Show completed task breakdown for one run.")
    status.add_argument("--all", action="store_true", help="Show all runs, including completed runs.")
    status.add_argument("--done", action="store_true", help="Show completed runs only.")
    status.add_argument("--failed", action="store_true", help="Show runs with failed tasks only.")
    status.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    status.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    status.add_argument(
        "--lease-seconds",
        type=int,
        default=600,
        help="Seconds before a claimed task is considered stale. Default: 600.",
    )
    status.set_defaults(func=cmd_status)

    if cmd_web is not None:
        web = sub.add_parser(
            "web",
            help="Serve a live status dashboard in the browser.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        web.add_argument("--port", type=int, default=8080, help="Port to listen on.")
        web.add_argument("--host", default="127.0.0.1", help="Host/address to bind.")
        web.add_argument("--foreground", "-f", action="store_true", help="Run in foreground instead of daemonizing.")
        web.set_defaults(func=cmd_web)

    wait = sub.add_parser(
        "wait",
        help="Wait until coordinator tasks finish.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    wait.add_argument("--manifest", required=True, help="Coordinator manifest.")
    wait.add_argument("--profile", help="Path-map/profile name for this host.")
    wait.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    wait.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval.")
    wait.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall timeout; 0 means no timeout.")
    wait.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when any task fails.")
    wait.add_argument("--notify-command", help="Command run on done/fail/timeout with CRUCIBLE_EVENT_JSON and JSON on stdin.")
    wait.set_defaults(func=cmd_wait)

    verify = sub.add_parser(
        "verify",
        help="Verify all run tasks are complete.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    verify.add_argument("run", help="Run name or run directory.")
    verify.set_defaults(func=cmd_verify)

    merge = sub.add_parser(
        "merge",
        help="Concatenate first output from each task.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    merge.add_argument("run", help="Run name or run directory.")
    merge.add_argument("--output", required=True, help="Merged output path.")
    merge.add_argument("--force", action="store_true", help="Overwrite an existing merged output.")
    merge.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    merge.set_defaults(func=cmd_merge)

    stop = sub.add_parser(
        "stop",
        help="Stop new claims and release active task claims.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    stop.add_argument("run", nargs="?", help="Run name or run directory to stop.")
    stop.add_argument("--all", action="store_true", help="Request stop for all current runs.")
    stop.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    stop.set_defaults(func=cmd_stop)

    drain = sub.add_parser(
        "drain",
        help="Stop a worker from claiming more tasks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    drain.add_argument("run", nargs="?", help="Run name or run directory to drain.")
    drain.add_argument("--all", action="store_true", help="Drain worker from all current runs.")
    drain.add_argument("--worker", required=True, help="Worker name to drain.")
    drain.add_argument("--clear", action="store_true", help="Clear the drain marker and allow new claims.")
    drain.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    drain.set_defaults(func=cmd_drain)

    add = sub.add_parser(
        "add",
        help="Start extra worker slots for a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_worker_add_options(add, worker_positional=False)
    add.set_defaults(func=cmd_worker_add)

    worker = sub.add_parser(
        "worker",
        help="Add or drain worker slots for a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    worker_sub = worker.add_subparsers(dest="worker_command", metavar="command", required=True)

    worker_add = worker_sub.add_parser(
        "add",
        help="Start extra worker slots for a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_worker_add_options(worker_add, worker_positional=True)
    worker_add.set_defaults(func=cmd_worker_add)

    worker_del = worker_sub.add_parser(
        "del",
        help="Drain a worker from a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    worker_del.add_argument("worker", help="Worker host to drain.")
    worker_del.add_argument("run", nargs="?", help="Run name or run directory. Omit to use the single active run.")
    worker_del.add_argument("--keep-claims", action="store_true", help="Do not release this worker's reserved claims.")
    worker_del.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    worker_del.set_defaults(func=cmd_worker_del)

    delete = sub.add_parser(
        "del",
        help="Delete run directories or drain a worker with --worker.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    delete.add_argument("run", nargs="?", help="Run name or run directory. With --worker, drain from this run; otherwise delete it.")
    delete.add_argument("--worker", help="Worker host to drain instead of deleting a run.")
    delete.add_argument("--keep-claims", action="store_true", help="With --worker, do not release this worker's reserved claims.")
    delete.add_argument("--done", action="store_true", help="Delete all completed runs.")
    delete.add_argument("--force", action="store_true", help="Do not prompt before deleting.")
    delete.add_argument(
        "--lease-seconds",
        type=int,
        default=600,
        help="Seconds before a claimed task is considered stale. Default: 600.",
    )
    delete.set_defaults(func=cmd_del_or_worker)

    self_update = sub.add_parser(
        "self-update",
        help="Fast-forward the crucible checkout itself.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    self_update.add_argument("--remote", default="origin", help="Git remote to pull from.")
    self_update.add_argument("--branch", help="Branch to update; defaults to current branch.")
    self_update.add_argument("--require-clean", action=argparse.BooleanOptionalAction, default=True, help="Require clean worktree before updating.")
    self_update.add_argument("--json", action="store_true", help="Print version payload as JSON after updating.")
    self_update.set_defaults(func=cmd_self_update)

    update = sub.add_parser(
        "update",
        help="Update workers to the coordinator HEAD.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "Update Crucible on workers to the exact commit currently checked "
            "out by the coordinator. With --all, first fast-forward the "
            "coordinator from the selected remote/branch, then update workers "
            "to that new HEAD."
        ),
    )
    update.add_argument("workers", nargs="?", help=WORKERS_ARG_HELP_WITH_DEFAULT)
    update.add_argument(
        "--all",
        action="store_true",
        help=(
            "Fast-forward the coordinator first, then update workers to the "
            "coordinator's new HEAD."
        ),
    )
    update.add_argument("--remote", default="origin", help="Git remote to pull from.")
    update.add_argument("--branch", default="main", help="Branch to update.")
    update.add_argument("--require-clean", action=argparse.BooleanOptionalAction, default=True, help="Require clean worktrees before updating.")
    update.add_argument("--timeout-seconds", type=float, default=300.0, help="Timeout per worker update.")
    update.set_defaults(func=cmd_update)

    version = sub.add_parser(
        "version",
        help="Print crucible version.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    version.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    version.set_defaults(func=cmd_version)

    deploy = sub.add_parser(
        "deploy",
        help="Verify workers and run manifest tasks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    deploy.add_argument("workers", help=WORKERS_ARG_HELP)
    deploy.add_argument("manifest", help="Manifest template to materialize into runs/<name>.")
    deploy.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    deploy.add_argument("--run", help="Run directory name under runs/. Defaults to manifest name.")
    deploy.add_argument("--resume", action="store_true", help="Resume an existing materialized run.")
    deploy.add_argument("--replace", action="store_true", help="Delete and recreate an existing run.")
    deploy.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    deploy.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    deploy.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    deploy.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    deploy.add_argument(
        "--notify-command",
        help="Command run on completion with CRUCIBLE_EVENT_JSON and JSON on stdin.",
    )
    deploy.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    deploy.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    deploy.set_defaults(func=cmd_deploy)

    run_job = sub.add_parser(
        "start",
        help="Start a manifest on workers, verify, and optionally merge outputs.",
        formatter_class=CrucibleHelpFormatter,
        description=(
            "Start a manifest on workers.\n\n"
            "Examples:\n"
            "  crucible start manifest.json --workers worker-a,worker-b:4\n"
            "  crucible start manifest.json --workers workers.json --wait\n"
            "  crucible start manifest.json --workers worker-a,worker-b:4 --merge merged.txt"
        ),
    )
    run_job.add_argument("manifest", help="Manifest template to materialize into runs/<name>.")
    run_job.add_argument("--workers", required=True, help=WORKERS_ARG_HELP)
    run_job.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    run_job.add_argument("--run", help="Run directory name under runs/. Defaults to manifest name.")
    run_job.add_argument("--resume", action="store_true", help="Resume an existing materialized run.")
    run_job.add_argument("--replace", action="store_true", help="Delete and recreate an existing run.")
    run_job.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    run_job.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    run_job.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    run_job.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    run_job.add_argument(
        "--wait",
        action="store_true",
        help="Poll the materialized run after workers exit and fail on incomplete or failed tasks.",
    )
    run_job.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    run_job.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    run_job.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    run_job.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    run_job.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True, help="Verify task completion and outputs before returning success.")
    run_job.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    run_job.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    run_job.add_argument("--newline-between", action="store_true", help="Insert a newline between merged task outputs.")
    run_job.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with CRUCIBLE_EVENT_JSON and JSON on stdin.",
    )
    run_job.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    run_job.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    run_job.set_defaults(func=cmd_run_job, command_label="start")

    resume = sub.add_parser(
        "resume",
        help="Resume stopped or incomplete work in a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    resume.add_argument("run", nargs="?", help="Run name or run directory. Omit to resume the single current incomplete run.")
    resume.add_argument("--workers", help=WORKERS_ARG_HELP_RUN_DEFAULT)
    resume.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    resume.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    resume.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    resume.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    resume.add_argument(
        "--wait",
        action="store_true",
        help="Poll the materialized run after workers exit and fail on incomplete or failed tasks.",
    )
    resume.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    resume.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    resume.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    resume.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    resume.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True, help="Verify task completion and outputs before returning success.")
    resume.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    resume.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    resume.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    resume.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with CRUCIBLE_EVENT_JSON and JSON on stdin.",
    )
    resume.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    resume.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    resume.set_defaults(func=cmd_resume)

    retry = sub.add_parser(
        "retry",
        help="Retry failed tasks in a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    retry.add_argument(
        "run",
        nargs="?",
        help="Run name or run directory. Omit to retry all failed incomplete runs.",
    )
    retry.add_argument("--workers", help=WORKERS_ARG_HELP_RETRY_DEFAULT)
    retry.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare/run concurrently.")
    retry.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    retry.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    retry.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    retry.add_argument(
        "--wait",
        action="store_true",
        help="Poll the materialized run after workers exit and fail on incomplete or failed tasks.",
    )
    retry.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    retry.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    retry.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    retry.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    retry.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True, help="Verify task completion and outputs before returning success.")
    retry.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    retry.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    retry.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    retry.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with CRUCIBLE_EVENT_JSON and JSON on stdin.",
    )
    retry.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    retry.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    retry.set_defaults(func=cmd_retry)

    parser.command_help = {
        action.dest: str(action.help)
        for action in sub._choices_actions
    }
    return parser

def main() -> int:
    parser = build_parser()
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    args = parser.parse_args()
    if args.version or args.version_json:
        args.json = bool(args.version_json)
        return cmd_version(args)
    if getattr(args, "subcommand", None) is None:
        parser.error("a command is required")
    if getattr(args, "subcommand", None) == "run-task" and args.task is None and args.index is None:
        parser.error("run-task requires --task or --index")
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130
