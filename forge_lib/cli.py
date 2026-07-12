from __future__ import annotations

from typing import Any

from .core import *
from .config import *
from .manifest import *
from .tasks import *
from .status import *
from .workers import *
from .templates import *
from .shell import *
from .spsa import *

try:
    from .web import cmd_web
except ModuleNotFoundError as exc:
    if exc.name != "forge_lib.web":
        raise
    cmd_web = None


WORKERS_ARG_HELP = (
    "workers.json file or comma-separated workers, e.g. worker-a,worker-b:4,worker-c:0."
)
WORKERS_ARG_HELP_WITH_DEFAULT = (
    WORKERS_ARG_HELP + " Defaults to ~/.config/forge/workers.json."
)
WORKERS_ARG_HELP_RUN_DEFAULT = (
    WORKERS_ARG_HELP + " Defaults to the run's workers.json."
)
WORKERS_ARG_HELP_RETRY_DEFAULT = (
    WORKERS_ARG_HELP + " If omitted, retry only requeues failed tasks and does not launch workers."
)


COMMON_COMMANDS = [
    "run",
    "label",
    "sprt",
    "spsa",
    "sync",
    "sh",
    "start",
    "del",
    "stop",
    "resume",
    "retry",
    "worker",
    "status",
    "update",
    "template",
]
if cmd_web is not None:
    COMMON_COMMANDS.insert(5, "web")


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

RESERVED_COMMANDS = {
    "add",
    "list",
    "spsa-next",
    "spsa-submit",
    "spsa-worker",
    "web",
}


def home_relative_argument(value: str) -> str:
    home = str(Path.home())
    if value == home or value.startswith(home + os.sep):
        return "~" + value[len(home):]
    return value


def shell_command_from_arguments(values: list[str]) -> str:
    if len(values) == 1:
        return values[0]
    words = [
        "~" if value == "~" else "~/" + shlex.quote(value[2:]) if value.startswith("~/") else shlex.quote(value)
        for value in values
    ]
    return " ".join(words)


def expand_template_command_aliases(args: list[str]) -> list[str]:
    if args and args[0] == "sprt":
        return ["run", "sprt", *args[1:]]
    if args and args[0] == "spsa":
        return ["run", "spsa", *args[1:]]
    if args and args[0] == "label":
        return ["run", "label", *args[1:]]
    return args


def expand_implicit_command(args: list[str]) -> list[str]:
    expanded = expand_template_command_aliases(args)
    if not expanded or expanded[0].startswith("-"):
        return expanded
    command = expanded[0]
    known_commands = {
        name
        for _title, names in COMMAND_GROUPS
        for name in names
    }
    if command in known_commands or command in RESERVED_COMMANDS:
        return expanded
    template_names = {
        path.name.removesuffix(".template.json")
        for path in template_files()
    }
    if command in template_names:
        return ["run", *expanded]
    return ["sh", *expanded]


class ForgeHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


class ForgeArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.command_groups = kwargs.pop("command_groups", [])
        self.command_help: dict[str, str] = {}
        super().__init__(*args, **kwargs)

    def parse_args(self, args: list[str] | None = None, namespace: Any = None) -> argparse.Namespace:
        if args is None:
            args = sys.argv[1:]
        normalized = [home_relative_argument(value) for value in args]
        parsed = super().parse_args(expand_implicit_command(normalized), namespace)
        if getattr(parsed, "subcommand", None) == "sh" and isinstance(parsed.command, list):
            parsed.command = shell_command_from_arguments(parsed.command)
        return parsed

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


def add_worker_set_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("worker", help="Inline worker spec; worker-a:5 sets the total slot count to five.")
    parser.add_argument("run", nargs="?", help="Run name or run directory. Omit to use the single active run.")
    parser.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare concurrently.")
    parser.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    parser.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    parser.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    parser.add_argument("--wait", action="store_true", help="Wait for completion after launching workers.")
    parser.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    parser.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    parser.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=None, help="Wait for completion and verify outputs.")
    parser.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    parser.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    parser.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    parser.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with FORGE_EVENT_JSON and JSON on stdin.",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    parser.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")


def cmd_del_or_worker(args: argparse.Namespace) -> int:
    if getattr(args, "worker", None):
        if bool(getattr(args, "done", False)) or bool(getattr(args, "all", False)):
            raise SystemExit("del --worker cannot be used with --done or --all")
        if bool(getattr(args, "force", False)):
            raise SystemExit("del --worker cannot be used with --force")
        return cmd_worker_del(args)
    if bool(getattr(args, "keep_claims", False)):
        raise SystemExit("del --keep-claims requires --worker")
    return cmd_del(args)


def build_parser() -> argparse.ArgumentParser:
    parser = ForgeArgumentParser(
        description=(
            "Forge distributes manifest-defined work across workers. "
            "Start with start, add, del, stop, resume, retry, status, and update."
        ),
        formatter_class=ForgeHelpFormatter,
        command_groups=COMMAND_GROUPS,
    )
    parser.add_argument("--version", action="store_true", help="Print forge version and exit.")
    parser.add_argument("--version-json", action="store_true", help="Print forge version as JSON and exit.")
    sub = parser.add_subparsers(dest="subcommand", metavar="command")

    run = sub.add_parser(
        "run",
        help="Run a named template.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "Run a named Forge template. Run options such as --workers may be "
            "mixed with template parameters after the template name."
        ),
    )
    run.add_argument("template", help="Template name, e.g. sprt.")
    run.add_argument("template_args", nargs=argparse.REMAINDER, help="Run options and template parameters.")
    run.set_defaults(func=cmd_template_run)

    sprt = sub.add_parser(
        "sprt",
        help="Run the SPRT template.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "Run the built-in SPRT template. This is equivalent to "
            "`forge run sprt ...`. Forge consumes only orchestration "
            "flags; other flags are passed to the SPRT template."
        ),
    )
    sprt.add_argument("template_args", nargs=argparse.REMAINDER, help="Run options and SPRT arguments.")
    sprt.set_defaults(func=cmd_template_run, template="sprt")

    selfplay = sub.add_parser(
        "selfplay",
        help="Run the Enyo self-play template.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "Run the built-in Enyo self-play template. This is equivalent to "
            "`forge run selfplay ...`."
        ),
    )
    selfplay.add_argument("template_args", nargs=argparse.REMAINDER, help="Run options and self-play arguments.")
    selfplay.set_defaults(func=cmd_template_run, template="selfplay")

    spsa = sub.add_parser(
        "spsa",
        help="Run distributed SPSA tuning.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            "Run the built-in adaptive SPSA template. Forge keeps one long-lived "
            "task per match shard and synchronizes theta updates between iterations."
        ),
    )
    spsa.add_argument("template_args", nargs=argparse.REMAINDER, help="Run options and SPSA arguments.")
    spsa.set_defaults(func=cmd_template_run, template="spsa")

    spsa_next = sub.add_parser("spsa-next", help=argparse.SUPPRESS)
    spsa_next.add_argument("--manifest", required=True)
    spsa_next.add_argument("--shard-index", required=True, type=int)
    spsa_next.set_defaults(func=cmd_spsa_next)

    spsa_submit = sub.add_parser("spsa-submit", help=argparse.SUPPRESS)
    spsa_submit.add_argument("--manifest", required=True)
    spsa_submit.add_argument("--shard-index", required=True, type=int)
    spsa_submit.add_argument("--iteration", required=True, type=int)
    spsa_submit.add_argument("--wins", required=True, type=int)
    spsa_submit.add_argument("--losses", required=True, type=int)
    spsa_submit.add_argument("--draws", required=True, type=int)
    spsa_submit.add_argument("--games", required=True, type=int)
    spsa_submit.set_defaults(func=cmd_spsa_submit)

    spsa_worker = sub.add_parser("spsa-worker", help=argparse.SUPPRESS)
    spsa_worker.add_argument("--coordinator", required=True)
    spsa_worker.add_argument("--shard-index", required=True, type=int)
    spsa_worker.add_argument("--games", required=True, type=int)
    spsa_worker.add_argument("--engine", required=True)
    spsa_worker.add_argument("--book", required=True)
    spsa_worker.add_argument("--fastchess", required=True)
    spsa_worker.add_argument("--tc", required=True)
    spsa_worker.add_argument("--hash", required=True, type=int)
    spsa_worker.add_argument("--threads", required=True, type=int)
    spsa_worker.add_argument("--concurrency", required=True, type=int)
    spsa_worker.add_argument("--restart", choices=("on", "off"), required=True)
    spsa_worker.add_argument("--match-timeout", required=True, type=float)
    spsa_worker.add_argument("--poll-seconds", required=True, type=float)
    spsa_worker.add_argument("--output", required=True)
    spsa_worker.add_argument("--uci", action="append", default=[])
    spsa_worker.set_defaults(func=cmd_spsa_worker)

    shell = sub.add_parser(
        "sh",
        help="Run a shell command once on each worker host.",
        formatter_class=ForgeHelpFormatter,
        description=(
            "Run a shell command once on each selected worker host and print "
            "grouped per-host output.\n\n"
            "Examples:\n"
            "  forge \"pwd\"\n"
            "  forge sh \"cd ~/repo; git pull; cmake --build build\"\n"
            "  forge sh --workers pwa-5090,pwa-mbp \"hostname && git --version\""
        ),
    )
    shell.add_argument("command", nargs="+", help="Shell command and arguments run on each host.")
    shell.add_argument("--workers", help=WORKERS_ARG_HELP_WITH_DEFAULT)
    shell.add_argument("--jobs", type=int, default=4, help="Maximum hosts to run concurrently.")
    shell.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for each host command.")
    shell.set_defaults(func=cmd_sh)

    sync = sub.add_parser(
        "sync",
        help="Synchronize a file or directory to each worker host.",
        formatter_class=ForgeHelpFormatter,
        description=(
            "Synchronize a local file or directory to the same destination on each "
            "selected worker. The destination defaults to the equivalent source path. "
            "Matching size and SHA-256 content is skipped."
        ),
    )
    sync.add_argument("source", help="Local source file or directory.")
    sync.add_argument("destination", nargs="?", help="Destination path on each worker; defaults to the source path.")
    sync.add_argument("--workers", help=WORKERS_ARG_HELP_WITH_DEFAULT)
    sync.add_argument("--jobs", type=int, default=4, help="Maximum hosts to synchronize concurrently.")
    sync.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for each host.")
    sync.set_defaults(func=cmd_sync)

    template = sub.add_parser(
        "template",
        help="List or show named templates.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    template_sub = template.add_subparsers(dest="action", required=True, metavar="action")
    template_list = template_sub.add_parser("list", help="List available templates.")
    template_list.set_defaults(func=cmd_template)
    template_show = template_sub.add_parser("show", help="Show a template JSON document.")
    template_show.add_argument("name", help="Template name.")
    template_show.set_defaults(func=cmd_template)

    plan = sub.add_parser(
        "plan",
        help="Create a numbered task manifest.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    plan.add_argument("--name", required=True, help="Run name stored in the manifest.")
    plan.add_argument("--kind", help="Short user-defined run type shown in status.")
    plan.add_argument("--description", help="Short single-line run description shown in status.")
    plan.add_argument("--comment", help="Short single-line run comment shown in status and web.")
    plan.add_argument(
        "--task-label",
        help="Short per-task label shown in detailed status. Supports templates.",
    )
    plan.add_argument("--shards", metavar="N", type=int, required=True, help="Number of numbered shards to create.")
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
    add_repo.add_argument("--allow-untracked", action="store_true", help="Ignore untracked files during sync-repos.")
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
    heartbeat.add_argument("--json", action="store_true", help="Print heartbeat state as JSON.")
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
    doctor.add_argument("--skip-requirements", action="store_true", help="Skip manifest requirement checks.")
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
    wait.add_argument("--notify-command", help="Command run on done/fail/timeout with FORGE_EVENT_JSON and JSON on stdin.")
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

    worker = sub.add_parser(
        "worker",
        help="Set or drain worker slots for a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    worker_sub = worker.add_subparsers(dest="worker_command", metavar="command", required=True)

    worker_set = worker_sub.add_parser(
        "set",
        help="Set and enforce total worker slots for a materialized run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_worker_set_options(worker_set)
    worker_set.set_defaults(func=cmd_worker_set)

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
    delete_scope = delete.add_mutually_exclusive_group()
    delete_scope.add_argument("--done", action="store_true", help="Delete all completed runs.")
    delete_scope.add_argument("--all", action="store_true", help="Delete all inactive runs.")
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
        help="Fast-forward the forge checkout itself.",
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
            "Update Forge on workers to the exact commit currently checked "
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
        help="Print forge version.",
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
    deploy.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare concurrently.")
    deploy.add_argument("--run", help="Run directory name under runs/. Defaults to manifest name.")
    deploy.add_argument("--comment", help="Short single-line run comment shown in status and web.")
    deploy.add_argument("--resume", action="store_true", help="Resume an existing materialized run.")
    deploy.add_argument("--replace", action="store_true", help="Delete and recreate an existing run.")
    deploy.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    deploy.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    deploy.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    deploy.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    deploy.add_argument(
        "--notify-command",
        help="Command run on completion with FORGE_EVENT_JSON and JSON on stdin.",
    )
    deploy.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    deploy.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    deploy.set_defaults(func=cmd_deploy, start_wait_monitor=True)

    run_job = sub.add_parser(
        "start",
        help="Start a manifest on workers, verify, and optionally merge outputs.",
        formatter_class=ForgeHelpFormatter,
        description=(
            "Start a manifest on workers.\n\n"
            "Examples:\n"
            "  forge start manifest.json --workers worker-a,worker-b:4\n"
            "  forge start manifest.json --workers workers.json --wait\n"
            "  forge start manifest.json --workers worker-a,worker-b:4 --merge merged.txt"
        ),
    )
    run_job.add_argument("manifest", help="Manifest template to materialize into runs/<name>.")
    run_job.add_argument("--workers", required=True, help=WORKERS_ARG_HELP)
    run_job.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare concurrently.")
    run_job.add_argument("--run", help="Run directory name under runs/. Defaults to manifest name.")
    run_job.add_argument("--comment", help="Short single-line run comment shown in status and web.")
    run_job.add_argument("--resume", action="store_true", help="Resume an existing materialized run.")
    run_job.add_argument("--replace", action="store_true", help="Delete and recreate an existing run.")
    run_job.add_argument("--retry-failed", action="store_true", help="Allow failed tasks to be claimed again.")
    run_job.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    run_job.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    run_job.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    run_job.add_argument("--wait", action="store_true", help="Wait for completion after launching workers.")
    run_job.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    run_job.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    run_job.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    run_job.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    run_job.add_argument("--verify", action=argparse.BooleanOptionalAction, default=None, help="Wait for completion and verify outputs.")
    run_job.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    run_job.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    run_job.add_argument("--newline-between", action="store_true", help="Insert a newline between merged task outputs.")
    run_job.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with FORGE_EVENT_JSON and JSON on stdin.",
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
    resume.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare concurrently.")
    resume.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    resume.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    resume.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    resume.add_argument("--wait", action="store_true", help="Wait for completion after launching workers.")
    resume.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    resume.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    resume.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    resume.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    resume.add_argument("--verify", action=argparse.BooleanOptionalAction, default=None, help="Wait for completion and verify outputs.")
    resume.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    resume.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    resume.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    resume.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with FORGE_EVENT_JSON and JSON on stdin.",
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
    retry.add_argument("--jobs", type=int, default=4, help="Maximum workers to prepare concurrently.")
    retry.add_argument("--task-attempts", type=int, default=2, help="Total attempts per task before final failure.")
    retry.add_argument("--coordinator-host", help="Coordinator hostname workers should SSH back to.")
    retry.add_argument("--remote-timeout-seconds", type=float, default=1800.0, help="Timeout for remote setup/work commands.")
    retry.add_argument("--wait", action="store_true", help="Wait for completion after launching workers.")
    retry.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration before stale.")
    retry.add_argument("--interval-seconds", type=float, default=30.0, help="Polling interval for --wait.")
    retry.add_argument("--timeout-seconds", type=float, default=0.0, help="Overall --wait timeout; 0 means no timeout.")
    retry.add_argument("--fail-on-fail", action=argparse.BooleanOptionalAction, default=True, help="Return failure when --wait sees failed tasks.")
    retry.add_argument("--verify", action=argparse.BooleanOptionalAction, default=None, help="Wait for completion and verify outputs.")
    retry.add_argument("--merge", metavar="OUTPUT", help="Merge first task outputs into this file after verification.")
    retry.add_argument("--merge-force", action="store_true", help="Overwrite an existing merged output.")
    retry.add_argument("--newline-between", action="store_true", help="Insert a newline between task outputs.")
    retry.add_argument(
        "--notify-command",
        help="Command run on deploy/wait completion with FORGE_EVENT_JSON and JSON on stdin.",
    )
    retry.add_argument("--no-color", action="store_true", help="Disable ANSI color output.")
    retry.add_argument("--verbose", action="store_true", help="Print setup and worker phase details.")
    retry.set_defaults(func=cmd_retry)

    parser.command_help = {
        action.dest: str(action.help)
        for action in sub._choices_actions
    }
    parser.command_help["label"] = "Run the labeling template."
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
