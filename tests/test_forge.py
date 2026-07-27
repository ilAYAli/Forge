from __future__ import annotations

import contextlib
import copy
import io
import inspect
import json
import os
import argparse
import re
import signal
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "forge.py"
sys.path.insert(0, str(ROOT))
import forge  # noqa: E402


class ForgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._isolated_env_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._isolated_env_dir.cleanup)
        isolated_config = Path(self._isolated_env_dir.name) / "config.yml"
        old_coordinator_runs = os.environ.pop("FORGE_COORDINATOR_RUNS", None)

        def restore_coordinator_runs_env() -> None:
            if old_coordinator_runs is not None:
                os.environ["FORGE_COORDINATOR_RUNS"] = old_coordinator_runs
            else:
                os.environ.pop("FORGE_COORDINATOR_RUNS", None)

        self.addCleanup(restore_coordinator_runs_env)
        self._env_patcher = mock.patch.dict(
            os.environ,
            {
                "FORGE_CONFIG": str(isolated_config),
                "FORGE_NOTIFY_COMMAND": "",
                "FORGE_COORDINATOR_HOST": "",
                "FORGE_WORKERS": "",
            },
            clear=False,
        )
        self._env_patcher.start()
        self.addCleanup(self._env_patcher.stop)

    def run_cli(
        self,
        *args: str,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        process_env = os.environ.copy()
        if env:
            process_env.update(env)
        return subprocess.run(
            [sys.executable, str(CLI), *args],
            cwd=cwd or ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=process_env,
        )

    def test_label_command_expands_to_label_template(self) -> None:
        self.assertEqual(
            ["run", "label", "--depth", "12"],
            forge.expand_template_command_aliases(["label", "--depth", "12"]),
        )
        self.assertEqual(
            ["unknown", "--depth", "12"],
            forge.expand_template_command_aliases(["unknown", "--depth", "12"]),
        )

    def test_worker_runtime_paths_avoid_new_zip_strict_keyword(self) -> None:
        for rel_path in ("forge_lib/tasks.py", "forge_lib/workers.py"):
            text = (ROOT / rel_path).read_text(encoding="utf-8")
            self.assertNotIn("strict=", text, rel_path)

    def test_status_numeric_parsers_accept_manifest_numbers(self) -> None:
        self.assertEqual(100, forge.parse_int(100))
        self.assertEqual(100, forge.parse_int(100.0))
        self.assertIsNone(forge.parse_int(100.5))
        self.assertEqual(100.5, forge.parse_float(100.5))
        self.assertIsNone(forge.parse_int(True))
        self.assertIsNone(forge.parse_float(False))

    def test_command_requirement_can_create_missing_executable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            executable = Path(tmp) / "generated-tool"
            command = [
                sys.executable,
                "-c",
                "import os,sys; open(sys.argv[1], 'w').close(); os.chmod(sys.argv[1], 0o755); print('ready')",
                str(executable),
            ]

            forge.check_command_requirement({
                "name": "generated",
                "path": str(executable),
                "command": command,
                "contains": "ready",
            })

            self.assertTrue(executable.is_file())
            self.assertTrue(os.access(executable, os.X_OK))

    def test_schema_matching_accepts_prior_namespaces(self) -> None:
        self.assertTrue(forge.schema_matches("forge.task.v1", "task"))
        self.assertTrue(forge.schema_matches("previous.task.v1", "task"))
        self.assertTrue(forge.schema_matches("previous.workers.v1", "workers"))
        self.assertFalse(forge.schema_matches("previous.task.v2", "task"))
        self.assertFalse(forge.schema_matches("previous.template.v1", "task"))

    def test_event_ntfy_dry_run_handles_failed_sprt_event(self) -> None:
        payload = {
            "event": "deploy_fail",
            "kind": "sprt",
            "state": "failed",
            "run": "syntax-check",
            "run_dir": "/tmp/does-not-exist",
            "tasks": 1,
            "counts": {"done": 0, "running": 0, "claimed": 0, "pending": 1, "fail": 1, "stale": 0},
            "worker_results": [{"host": "pwa-mbp", "stage": "setup", "rc": 1, "ok": False}],
            "elapsed_seconds": 1.25,
        }
        result = subprocess.run(
            ["bash", str(ROOT / "scripts/forge_event_ntfy.sh")],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env={
                **os.environ,
                "FORGE_NTFY_DRY_RUN": "1",
                "FORGE_EVENT_JSON": json.dumps(payload),
            },
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Forge sprt failed", result.stdout)
        self.assertIn("pwa-mbp:setup:rc=1", result.stdout)
        self.assertNotIn("<output>", result.stdout)
        self.assertNotIn("</output>", result.stdout)

    @contextlib.contextmanager
    def patch_runs_dir(self, path: Path):
        with (
            mock.patch("forge_lib.core.runs_dir", return_value=path),
            mock.patch("forge_lib.status.runs_dir", return_value=path),
            mock.patch("forge_lib.workers.runs_dir", return_value=path),
            mock.patch("forge_lib.web.runs_dir", return_value=path),
            mock.patch("forge_lib.shell.runs_dir", return_value=path),
        ):
            yield

    def write_retry_run(self, runs: Path, name: str, *, failed: bool | None) -> Path:
        run = runs / name
        task = {"id": "task_0", "index": 0, "command": "true", "outputs": []}
        manifest = {
            "schema": "forge.task.v1",
            "name": name,
            "work_dir": str(run),
            "state_dir": str(run / "state"),
            "log_dir": str(run / "logs"),
            "claim_dir": str(run / "claims"),
            "tasks": [task],
        }
        run.mkdir(parents=True)
        forge.write_json(run / "manifest.json", manifest)
        forge.write_json(run / "workers.json", {
            "schema": "forge.workers.v1",
            "workers": ["worker-a"],
        })
        if failed is True:
            forge.write_json(forge.task_fail_path(manifest, task), {
                "task": "task_0",
                "index": 0,
                "worker": "worker-a",
                "rc": 1,
            })
        elif failed is False:
            forge.write_json(forge.task_done_path(manifest, task), {
                "task": "task_0",
                "index": 0,
                "worker": "worker-a",
                "rc": 0,
            })
        return run

    def write_resumable_run(
        self,
        runs: Path,
        name: str,
        *,
        done: int,
        history: int | None = None,
    ) -> Path:
        run = runs / name
        output = run / "outputs" / "sprt_0000" / "logs" / "sprt_0000.log"
        progress = copy.deepcopy(
            forge.load_template(ROOT / "templates" / "sprt.template.json")["task"]["progress"]
        )
        progress["total"] = 10
        task = {
            "id": "sprt_0000",
            "index": 0,
            "label": "sprt:10",
            "command": (
                "sprt --games 10 --log-dir {output_dir}/sprt_0000/logs "
                "--name sprt_0000"
            ),
            "outputs": [str(output)],
            "progress": progress,
            "resume": {"strategy": "split-progress", "count_arg": "--games"},
            "claim_worker": "worker-a",
        }
        manifest = {
            "schema": "forge.task.v1",
            "name": name,
            "kind": "sprt",
            "work_dir": str(run),
            "state_dir": str(run / "state"),
            "log_dir": str(run / "logs"),
            "claim_dir": str(run / "claims"),
            "vars": {
                "run_dir": str(run),
                "output_dir": str(run / "outputs"),
            },
            "tasks": [task],
        }
        run.mkdir(parents=True)
        forge.write_json(run / "manifest.json", manifest)
        forge.write_json(run / "workers.json", {
            "schema": "forge.workers.v1",
            "workers": ["worker-a"],
        })
        if done > 0:
            output.parent.mkdir(parents=True)
            output.write_text("raw fastchess checkpoint\n", encoding="utf-8")
            forge.task_log_path(manifest, task).parent.mkdir(parents=True, exist_ok=True)
            forge.task_log_path(manifest, task).write_text(
                f"[{done:2d}/10] Elo   5.0 +/-  20.0 | LLR  0.10/2.20 (  5%) "
                "| LOS 60.0% | draw  40.0%\n",
                encoding="utf-8",
            )
        if history is not None:
            history_path = forge.progress_history_path(manifest)
            history_path.parent.mkdir(parents=True, exist_ok=True)
            history_path.write_text(
                json.dumps({
                    "epoch": 1.0,
                    "x": float(history),
                    "x_total": 10,
                    "x_unit": "games",
                    "series": {"elo": 5.0, "llr": 0.1, "ci": 20.0},
                }) + "\n",
                encoding="utf-8",
            )
        forge.write_json(forge.stop_request_path(manifest), {
            "schema": "forge.task.v1",
            "run": name,
        })
        return run

    def delegated_status_payload(self) -> str:
        summary = {
            "run": "remote-run",
            "manifest": "/runs/remote-run/manifest.json",
            "tasks": 2,
            "counts": {"done": 0, "fail": 0, "running": 1, "claimed": 0, "stale": 0, "pending": 1},
            "groups": {"done": {}, "fail": {}, "stale": {}},
            "active": [{
                "worker": "remote-worker",
                "state": "running",
                "index": 0,
                "label": "score:uci",
                "progress": "",
                "progress_data": {},
            }],
            "active_count": 1,
            "pending": [1],
            "pending_groups": {"score:uci": [1]},
            "rows": [],
            "metadata": {"description": "Remote run"},
            "progress_totals": {},
            "progress_done": {},
            "progress_outputs": {},
            "worker_outputs": {"remote-worker": {"output": 100, "output_seen": True}},
            "worker_rates": {"remote-worker": 1.0},
            "diagnostics": [],
            "display": {
                "title": 'run="remote-run"',
                "description": 'desc="Remote run"',
                "active_summary_text": 'output=100 min="remote-worker":100 max="remote-worker":100',
            },
        }
        payload = forge.run_status_payload(summary)
        payload["display"]["title"] = 'run="remote-run"'
        payload["display"]["description"] = 'desc="Remote run"'
        payload["display"]["active_summary_text"] = 'output=100 min="remote-worker":100 max="remote-worker":100'
        return json.dumps({
            "runs": [payload],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
        }) + "\n"

    def test_run_status_payload_includes_template_command(self) -> None:
        summary = {
            "run": "command-run",
            "manifest": "/runs/command-run/manifest.json",
            "tasks": 3,
            "counts": {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 3},
            "groups": {"done": {}, "fail": {}, "stale": {}},
            "active": [],
            "active_count": 0,
            "pending": [0, 1, 2],
            "pending_groups": {"sprt:8": [0, 1], "sprt:6": [2]},
            "rows": [],
            "metadata": {"description": "Command run", "comment": "quick smoke"},
            "progress_totals": {},
            "progress_done": {},
            "progress_outputs": {},
            "worker_outputs": {},
            "worker_rates": {},
            "diagnostics": [],
            "manifest_payload": {
                "created_at": "2026-06-13T17:13:02+0200",
                "template": {
                    "name": "sprt",
                    "parameters": {
                        "candidate": "~/assets/engines/enyo_417eecb",
                        "games": 1000,
                        "reference": "~/assets/engines/enyo_637c4be",
                        "candidate_uci": ["lmr_base=130"],
                        "shards": 136,
                        "sprt_extra_args": ["--opening-order", "random"],
                    },
                },
                "tasks": [
                    {"id": "sprt_0000", "index": 0, "label": "sprt:8", "command": "sprt --candidate c --games 8 --name sprt_0000"},
                    {"id": "sprt_0001", "index": 1, "label": "sprt:8", "command": "sprt --candidate c --games 8 --name sprt_0001"},
                    {"id": "sprt_0002", "index": 2, "label": "sprt:6", "command": "sprt --candidate c --games 6 --name sprt_0002"},
                ],
            },
        }

        payload = forge.run_status_payload(summary)

        self.assertEqual("quick smoke", payload["comment"])
        self.assertEqual('"quick smoke"', payload["display"]["comment"])
        self.assertEqual([
            {
                "label": "forge sprt",
                "count": 1,
                "command": (
                    "forge sprt --candidate ~/assets/engines/enyo_417eecb --games 1000 "
                    "--reference ~/assets/engines/enyo_637c4be --candidate-uci lmr_base=130 "
                    "--shards 136 --opening-order random"
                ),
            },
        ], payload["commands"])
        self.assertEqual("2026-06-13T17:13:02+0200", payload["created_at"])
        self.assertNotIn("manifest_payload", payload)

    def test_run_status_payload_collapses_shard_task_commands(self) -> None:
        summary = {
            "run": "command-run",
            "manifest": "/runs/command-run/manifest.json",
            "tasks": 3,
            "counts": {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 3},
            "groups": {"done": {}, "fail": {}, "stale": {}},
            "active": [],
            "active_count": 0,
            "pending": [0, 1, 2],
            "pending_groups": {"sprt:8": [0, 1], "sprt:6": [2]},
            "rows": [],
            "metadata": {"description": "Command run"},
            "progress_totals": {},
            "progress_done": {},
            "progress_outputs": {},
            "worker_outputs": {},
            "worker_rates": {},
            "diagnostics": [],
            "manifest_payload": {
                "tasks": [
                    {"id": "sprt_0000", "index": 0, "label": "sprt:8", "command": "sprt --candidate c --games 8 --log-dir {output_dir}/sprt_0000/logs --name sprt_0000"},
                    {"id": "sprt_0001", "index": 1, "label": "sprt:8", "command": "sprt --candidate c --games 8 --log-dir {output_dir}/sprt_0001/logs --name sprt_0001"},
                    {"id": "sprt_0002", "index": 2, "label": "sprt:6", "command": "sprt --candidate c --games 6 --log-dir {output_dir}/sprt_0002/logs --name sprt_0002"},
                ],
            },
        }

        payload = forge.run_status_payload(summary)

        self.assertEqual([
            {"label": "sprt", "count": 3, "command": "sprt --candidate c"},
        ], payload["commands"])

    def test_run_status_payload_extracts_sprt_from_shell_wrapper(self) -> None:
        command = (
            'set -euo pipefail; book="$HOME/.cache/forge/books/book.epd"; '
            'if [ ! -f "$book" ]; then book="$HOME/Library/Caches/forge/books/book.epd"; fi; '
            'test -f "$book"; out={output}; logdir="$(dirname "$out")/logs"; '
            'name=warning-selfcheck-{task_id}; for attempt in 1 2; do rm -rf "$logdir"; mkdir -p "$logdir"; '
            '$HOME/.local/bin/sprt --candidate $HOME/assets/engines/enyo_09ae1a4 '
            '--reference $HOME/assets/engines/enyo_09ae1a4 --candidate-uci use_syzygy=false '
            '--reference-uci use_syzygy=false --games 25 --concurrency 1 --threads 1 --tc 2+0.02 '
            '--fastchess $HOME/.local/bin/fastchess --book "$book" --log-dir "$logdir" '
            '--name "$name" --order random --srand {seed} --no-ntfy --no-eta; '
            'if grep -q "Score of candidate vs reference:" "$logdir/$name.log"; then cp "$logdir/$name.log" "$out"; exit 0; fi; '
            'echo missing >&2; done; exit 1'
        )

        self.assertEqual(
            (
                "$HOME/.local/bin/sprt --candidate $HOME/assets/engines/enyo_09ae1a4 "
                "--reference $HOME/assets/engines/enyo_09ae1a4 --candidate-uci use_syzygy=false "
                "--reference-uci use_syzygy=false --concurrency 1 --threads 1 --tc 2+0.02 "
                "--fastchess $HOME/.local/bin/fastchess --book $book --order random "
                "--srand {seed} --no-ntfy --no-eta"
            ),
            forge.display_task_command(command),
        )

    def test_display_task_command_normalizes_sprt_seed(self) -> None:
        command = (
            "$HOME/.local/bin/sprt --candidate c --reference r --games 50 "
            "--log-dir logs --name sprt_0000 --srand 2026060800 --no-eta"
        )

        self.assertEqual(
            "$HOME/.local/bin/sprt --candidate c --reference r --srand {seed} --no-eta",
            forge.display_task_command(command),
        )

    def retry_args(self, **overrides: Any) -> argparse.Namespace:
        args = argparse.Namespace(
            run=None,
            workers=None,
            jobs=4,
            coordinator_host=None,
            remote_timeout_seconds=1800.0,
            wait=False,
            lease_seconds=600,
            interval_seconds=30.0,
            timeout_seconds=0.0,
            fail_on_fail=True,
            verify=None,
            merge=None,
            merge_force=False,
            newline_between=False,
            notify_command=None,
            no_color=True,
            verbose=False,
        )
        for key, value in overrides.items():
            setattr(args, key, value)
        return args

    def worker_set_args(self, **overrides: Any) -> argparse.Namespace:
        args = self.retry_args(worker="worker-b:6")
        for key, value in overrides.items():
            setattr(args, key, value)
        return args

    def worker_del_args(self, **overrides: Any) -> argparse.Namespace:
        args = argparse.Namespace(
            worker="worker-b",
            run=None,
            keep_claims=False,
            lease_seconds=600,
        )
        for key, value in overrides.items():
            setattr(args, key, value)
        return args

    def write_test_template(self, root: Path, *, name: str = "demo") -> Path:
        templates = root / "templates"
        templates.mkdir(parents=True)
        path = templates / f"{name}.template.json"
        path.write_text(
            json.dumps({
                "schema": "forge.template.v1",
                "name": name,
                "kind": "demo",
                "description": "demo template",
                "run_name": "demo-{item_stem}-{total}",
                "work_dir": "{work_dir}",
                "parameters": {
                    "item": {"required": True, "worker_path": True},
                    "total": {"type": "int", "required": True},
                    "shard": {"type": "int", "default": 3, "aliases": ["chunk"]},
                    "work_dir": {"default": "."},
                },
                "task": {
                    "split": {"total": "total", "shard": "shard", "unit": "rows"},
                    "command": "printf {shard} > {output_dir}/{task_name}.txt",
                    "output": "{output_dir}/{task_name}.txt",
                    "progress": {
                        "unit": "rows",
                        "log_regex": "rows=(?P<done>\\d+)/(?P<total>\\d+) eta=(?P<eta>\\S+)",
                    },
                },
            }),
            encoding="utf-8",
        )
        return path

    def test_remote_script_sets_common_user_bin_path(self) -> None:
        script = forge.managed_remote_script("forge version")
        self.assertIn("PATH=\"$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH\"; export PATH; ", script)
        self.assertIn("forge version", script)

    def test_worker_stop_waits_for_checkpoint_sync_before_kill(self) -> None:
        script = forge.cleanup_run_pid_files_command("/tmp/forge-cache", "resume-test")

        self.assertIn("kill -TERM", script)
        self.assertIn("remaining=200", script)
        self.assertIn("kill -0", script)
        self.assertLess(script.index("kill -TERM"), script.index("kill -KILL"))

    def test_no_arguments_prints_help(self) -> None:
        result = self.run_cli()
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)
        self.assertIn("usage:", result.stdout)
        self.assertIn("status", result.stdout)

    def test_top_level_help_groups_common_and_advanced_commands(self) -> None:
        result = self.run_cli("--help")
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)
        stdout = result.stdout
        common = stdout.index("Common:")
        advanced = stdout.index("Advanced:")
        common_positions = {
            command: stdout.index(f"    {command}")
            for command in ("run", "sprt", "start", "del", "stop", "resume", "retry", "worker", "status", "update", "template")
        }
        for position in common_positions.values():
            self.assertLess(common, position)
            self.assertLess(position, advanced)
        self.assertLess(common_positions["run"], common_positions["start"])
        self.assertLess(common_positions["run"], common_positions["sprt"])
        self.assertLess(common_positions["sprt"], common_positions["start"])
        self.assertLess(common_positions["start"], common_positions["del"])
        self.assertLess(common_positions["del"], common_positions["stop"])
        self.assertLess(common_positions["stop"], common_positions["resume"])
        self.assertLess(common_positions["resume"], common_positions["retry"])
        self.assertLess(common_positions["retry"], common_positions["worker"])
        self.assertLess(common_positions["worker"], common_positions["status"])
        self.assertLess(common_positions["status"], common_positions["update"])
        self.assertLess(common_positions["update"], common_positions["template"])
        self.assertNotIn("run-job", stdout)
        self.assertLess(advanced, stdout.index("    plan"))
        self.assertLess(advanced, stdout.index("    run-task"))
        self.assertLess(advanced, stdout.index("    verify"))
        self.assertLess(advanced, stdout.index("    version"))
        self.assertLess(advanced, stdout.index("    claim"))
        self.assertLess(advanced, stdout.index("    heartbeat"))
        self.assertLess(advanced, stdout.index("\n    work "))
        self.assertNotIn("positional arguments:", stdout)

    def test_empty_delegated_status_uses_coordinator_runs_label(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.print_run_status(
                [],
                color_enabled=False,
                as_json=False,
                filter_name=None,
                runs_label="pwa-5090:/runs",
            )

        self.assertEqual(0, rc)
        self.assertIn("no active Forge runs found in pwa-5090:/runs", stdout.getvalue())

    def test_web_command_is_optional_when_module_is_missing(self) -> None:
        script = f"""
import argparse
import contextlib
import importlib.abc
import io
import sys
sys.path.insert(0, {str(ROOT)!r})

class BlockWeb(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "forge_lib.web":
            raise ModuleNotFoundError("No module named 'forge_lib.web'", name=fullname)
        return None

sys.meta_path.insert(0, BlockWeb())
import forge_lib.cli as cli

parser = cli.build_parser()
help_text = parser.format_help()
if "\\n    web " in help_text:
    raise SystemExit("web command should be hidden when forge_lib.web is missing")
try:
    with contextlib.redirect_stderr(io.StringIO()):
        parser.parse_args(["web", "--help"])
except SystemExit as exc:
    raise SystemExit(0 if exc.code != 0 else "web command unexpectedly parsed")
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual("", result.stdout)
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)

    def test_web_renders_progress_fields(self) -> None:
        import forge_lib.web as web

        self.assertIn('<link rel="icon" type="image/png" href="/favicon.png">', web._HTML)
        self.assertIn("FIELD_TASK_COLS", web._HTML)
        self.assertIn("t.progress_data?.output != null", web._HTML)
        self.assertIn("firstProgressField(t.progress_data", web._HTML)
        self.assertIn("{name: 'llr'", web._HTML)
        self.assertIn("display.active_fields || active.fields || []", web._HTML)
        self.assertIn("function taskProgressText", web._HTML)
        self.assertNotIn("pd.global_done", web._HTML)
        self.assertIn("'progress'", web._HTML)
        self.assertIn("['worker','label','shard','progress']", web._HTML)
        self.assertNotIn("['worker','label','task','progress','output','rate']", web._HTML)
        self.assertIn("taskSort.col === 'task'", web._HTML)
        self.assertIn("toggleSound()", web._HTML)
        self.assertIn("notifyStartedJobs(data)", web._HTML)
        self.assertIn("playDing()", web._HTML)
        self.assertIn("localStorage.getItem('forge.sound')", web._HTML)
        self.assertIn("notify on", web._HTML)
        self.assertNotIn("ding on", web._HTML)
        self.assertIn("active > 0) keys.add(String(run.run || ''))", web._HTML)
        self.assertNotIn("task.id ?? task.index", web._HTML)
        self.assertIn("loading Forge status...", web._HTML)
        self.assertIn(".elo-pos", web._HTML)
        self.assertIn("function eloFieldClass", web._HTML)
        self.assertIn("nonEtaFields.map(renderField)", web._HTML)
        self.assertIn("class=\"games-current\"", web._HTML)
        self.assertIn(".games-current { color: var(--yel)", web._HTML)
        self.assertIn("const elo = String(field).match(/^(elo~?=)(.*)$/i)", web._HTML)
        self.assertIn("const llr = String(field).match(/^(llr=)([^/]+)(.*)$/i)", web._HTML)
        self.assertIn("function mobileScoreStrip", web._HTML)
        self.assertIn("mobileScoreStrip(nonEtaFields, activeFields)", web._HTML)
        self.assertIn(".mobile-score + .run-summary { display: none; }", web._HTML)
        self.assertIn("const llrCurrent = llr.split('/', 1)[0].trim()", web._HTML)
        self.assertIn("grid-template-columns: repeat(5, max-content)", web._HTML)
        self.assertIn('<span class="mobile-score-label">Elo</span>', web._HTML)
        self.assertIn('<span class="mobile-score-label">CI</span>', web._HTML)
        self.assertIn('<span class="mobile-score-label">LLR</span>', web._HTML)
        self.assertIn('<span class="mobile-score-label">LOS</span>', web._HTML)
        self.assertIn('<span class="mobile-score-label">Games</span>', web._HTML)
        self.assertIn("const eloValue = elo.replace(/^\\+/, '')", web._HTML)
        self.assertIn("const ciValue = ci ? `±${ci}` : ''", web._HTML)
        self.assertIn("const losValue = los.replace(/%$/, '')", web._HTML)
        self.assertNotIn("mobile-llr-track", web._HTML)
        self.assertIn("ws + renderTasks(active.tasks), false", web._HTML)
        self.assertIn("function progressEtaFromRate", web._HTML)
        self.assertIn("(pd.total - pd.done) / pd.rate", web._HTML)
        self.assertIn("function hasTextSelection", web._HTML)
        self.assertIn("if (hasTextSelection()) return;", web._HTML)
        self.assertIn("function shouldDeferRefreshForSelection", web._HTML)
        self.assertIn("selectionRefreshDeferredAt", web._HTML)
        self.assertIn("if (shouldDeferRefreshForSelection()) return;", web._HTML)
        self.assertIn("pd?.eta_estimate ? '~' : ''", web._HTML)
        self.assertIn("if (col === 'rate') return esc(fmtRate(pd?.rate));", web._HTML)
        self.assertNotIn("pd?.fallback_estimate ? '' : esc(fmtRate(pd?.rate))", web._HTML)
        self.assertIn("data-eta-estimate", web._HTML)
        self.assertIn("return r.toFixed(4) + '/s'", web._HTML)
        self.assertIn("function graphDomain", web._HTML)
        self.assertIn("function graphDisplayPoints", web._HTML)
        self.assertIn("const w = 3 - Math.abs(j - i)", web._HTML)
        self.assertNotIn("GRAPH_LEAD_IN", web._HTML)
        self.assertIn("function graphConfidenceAlignedPoints", web._HTML)
        self.assertIn("eloSeries.rawPoints = graphConfidenceAlignedPoints", web._HTML)
        self.assertIn("function graphNumberOption", web._HTML)
        self.assertIn("function graphScaleMode", web._HTML)
        self.assertIn("function graphXTotal", web._HTML)
        self.assertIn("function graphEarlyStop", web._HTML)
        self.assertIn("const earlyStop = graphEarlyStop(run, allSeries, configuredTotal)", web._HTML)
        self.assertIn("stopped at ${fmtGraphNumber(earlyStop.observed)} / ${fmtGraphNumber(earlyStop.target)}", web._HTML)
        self.assertIn("function graphYDomain", web._HTML)
        self.assertIn("if (graphSeriesName(name) === 'llr' && extras.length >= 2)", web._HTML)
        self.assertIn("if (Number.isFinite(bound) && bound > 0) return [-bound, bound]", web._HTML)
        self.assertIn("function graphYExtras", web._HTML)
        self.assertIn("function interpolatedGraphY", web._HTML)
        self.assertIn("function graphValueAt", web._HTML)
        self.assertIn("function graphConfidenceBandPoints", web._HTML)
        self.assertIn("function confidenceBandPath", web._HTML)
        self.assertIn("function graphAxisHtml", web._HTML)
        self.assertNotIn("function toggleGraphLlr", web._HTML)
        self.assertIn("p.y > 0", web._HTML)
        self.assertIn("const firstRadius = scaledRadius(firstCi.y)", web._HTML)
        self.assertIn("radius = firstRadius * fraction", web._HTML)
        self.assertIn("let radius = 0", web._HTML)
        self.assertIn("return hasWidth ? band : []", web._HTML)
        self.assertIn("return scaled.slice(1).map((current, index)", web._HTML)
        self.assertIn("const previous = scaled[index]", web._HTML)
        self.assertNotIn("maxCi", web._HTML)
        self.assertNotIn("graphBandYValues", web._HTML)
        self.assertIn("const clampY = y => Math.max(0, Math.min(h, y))", web._HTML)
        self.assertIn("overflow: hidden", web._HTML)
        self.assertIn("<clipPath id=", web._HTML)
        self.assertIn('clip-path="url(#${clipId})"', web._HTML)
        self.assertIn("function zeroLine", web._HTML)
        self.assertIn("function graphBoundLines", web._HTML)
        self.assertIn("function graphHoverZones", web._HTML)
        self.assertIn("function setGraphPointReadout", web._HTML)
        self.assertIn("function setGraphReadoutText", web._HTML)
        self.assertIn("function clearGraphReadout", web._HTML)
        self.assertIn("function graphDataPoints", web._HTML)
        self.assertIn("const [minY, maxY] = graphYDomain(name, values, extras)", web._HTML)
        self.assertIn("const yExtras = graphYExtras(s.name, options, s.points)", web._HTML)
        self.assertIn("const bandMaxFraction = graphNumberOption(s, ['band_max_fraction']) ?? 0.30", web._HTML)
        self.assertIn("const maxBandRadius = bandSeries ? (maxY - minY) * bandMaxFraction : null", web._HTML)
        self.assertIn("const band = bandSeries ? graphConfidenceBandPoints(s, s.points, bandSeries.points || [], maxBandRadius, rawPoints) : []", web._HTML)
        self.assertIn("const path = sparkPath(s.name, s.points, scaleExtras, GRAPH_W, h, xTotal)", web._HTML)
        self.assertIn("const zero = zeroLine(s.name, s.points, scaleExtras, GRAPH_W, h)", web._HTML)
        self.assertIn("graphHoverZones(s.name, s.points, scaleExtras", web._HTML)
        self.assertIn("const rawPoints = s.rawPoints || s.points", web._HTML)
        self.assertIn("const rawPoints = graphDataPoints(el, 'data-raw-points')", web._HTML)
        self.assertIn("const rawBest = rawPoints[bestIndex] || (rawPoints.length ? nearestGraphPoint(rawPoints, best.x) : best)", web._HTML)
        self.assertIn("const overlayPoints = overlay?.rawPoints || overlay?.points || []", web._HTML)
        self.assertIn("const ciValue = graphValueAt(graphDataPoints(el, 'data-ci-points'), rawBest.x)", web._HTML)
        self.assertIn("` ci=${fmtGraphNumber(ciValue)}`", web._HTML)
        self.assertIn("const minX = useTotal ? 0 : dataMinX", web._HTML)
        self.assertIn("function scaledLinePath", web._HTML)
        self.assertIn("path += ` L${p.x.toFixed(1)},${p.y.toFixed(1)}`", web._HTML)
        self.assertNotIn("function smoothScaledPath", web._HTML)
        self.assertIn("function llrBoundFromRun", web._HTML)
        self.assertIn("function signedGraphSeries", web._HTML)
        self.assertIn(".graph-zero", web._HTML)
        self.assertIn(".graph-grid", web._HTML)
        self.assertIn(".graph-band", web._HTML)
        self.assertIn(".graph-bound", web._HTML)
        self.assertIn(".graph-crosshair", web._HTML)
        self.assertIn(".graph-crosshair-readout", web._HTML)
        self.assertIn(".graph-value", web._HTML)
        self.assertIn("grid-template-columns: 58px minmax(240px, 1fr)", web._HTML)
        self.assertIn(".graph-x-axis", web._HTML)
        self.assertIn(".graph-point", web._HTML)
        self.assertIn("function sizeGraphPointMarkers", web._HTML)
        self.assertIn('<ellipse class="graph-point"', web._HTML)
        self.assertIn("const viewHeight = svg.viewBox?.baseVal?.height || GRAPH_H", web._HTML)
        self.assertIn("const radiusX = 2.2 * (rect.height / viewHeight) / (rect.width / GRAPH_W)", web._HTML)
        self.assertIn("function graphTicks", web._HTML)
        self.assertIn("function graphXAxisHtml", web._HTML)
        self.assertIn("function graphPointMarkers", web._HTML)
        self.assertIn("const scaleExtras = yExtras.concat(band.flatMap", web._HTML)
        self.assertIn("font-variant-numeric: tabular-nums", web._HTML)
        self.assertIn("label.textContent = text", web._HTML)
        self.assertIn("label.style.left = `${left}px`", web._HTML)
        self.assertIn(".graph-live-hover:hover .graph-crosshair", web._HTML)
        self.assertIn(".graph-plot:hover .graph-crosshair-readout", web._HTML)
        self.assertNotIn("points.slice(-48)", web._HTML)
        self.assertIn('x2="${w}"', web._HTML)
        self.assertIn("stroke-dasharray: 4 6", web._HTML)
        self.assertIn("stroke-linecap: round", web._HTML)
        self.assertIn("return [-bound, bound]", web._HTML)
        self.assertIn("data-points=\"${payload}\"", web._HTML)
        self.assertIn("data-raw-points=\"${rawPayload}\"", web._HTML)
        self.assertIn("data-ci-points=\"${ciPayload}\"", web._HTML)
        self.assertIn("onmousemove=\"setGraphPointReadout(this,event)\"", web._HTML)
        self.assertNotIn("stroke-dasharray: 2 3", web._HTML)
        self.assertNotIn(".graph-dot", web._HTML)
        self.assertNotIn(".graph-crosshair-point", web._HTML)
        self.assertNotIn("function endpointDot", web._HTML)
        self.assertIn(".sort((a, b) => a.x - b.x)", web._HTML)
        self.assertIn("viewBox=\"0 0 ${GRAPH_W} ${h}\"", web._HTML)
        self.assertIn("vector-effect: non-scaling-stroke", web._HTML)
        self.assertNotIn("localStorage.getItem('forge.graph.llr')", web._HTML)
        self.assertNotIn("localStorage.setItem('forge.graph.llr'", web._HTML)
        self.assertNotIn("graph-toggle", web._HTML)
        self.assertNotIn("show-llr", web._HTML)
        self.assertNotIn(".graph-line.llr-overlay", web._HTML)
        self.assertNotIn("stroke-dasharray: 5 5", web._HTML)
        self.assertNotIn(".graph-axis.right", web._HTML)
        self.assertNotIn("function graphOverlayPath", web._HTML)
        self.assertIn("function nearestGraphPoint", web._HTML)
        self.assertIn("data-overlay-points", web._HTML)
        self.assertIn("data-overlay-name", web._HTML)
        self.assertIn("${overlaySeries.name} ${overlayValue}${overlayBound}", web._HTML)
        self.assertIn("`${name}=${fmtGraphValue(name, rawBest.y)}${ciText}${overlayText} @ ${fmtGraphNumber(rawBest.x)} ${xUnit}`", web._HTML)
        self.assertIn("const bandName = primary.band_series || (graphSeriesName(primary) === 'elo' ? 'ci' : '')", web._HTML)
        self.assertIn("const bandSeries = bandName ? graphSeriesByName(allSeries, bandName) : null", web._HTML)
        self.assertIn("const genericSeries = !eloSeries && !llrSeries", web._HTML)
        self.assertIn("genericSeries.map", web._HTML)
        self.assertIn("graphRowHtml(s, options, graph, xTotal, bandSeries, overlay)", web._HTML)
        self.assertIn("function graphCombinedRowHtml", web._HTML)
        self.assertIn("function combinedGraphSeriesOrder", web._HTML)
        self.assertIn("function combinedGraphLegendHtml", web._HTML)
        self.assertIn("function setCombinedGraphPointReadout", web._HTML)
        self.assertIn("const isLabelingGraph = Boolean(graphSeriesByName(allSeries, 'total'))", web._HTML)
        self.assertIn("const isSelfplayGraph = String(run?.kind || '').toLowerCase() === 'selfplay'", web._HTML)
        self.assertIn("graphCombinedRowHtml(labelingSeries, graph, xTotal)", web._HTML)
        self.assertIn("selfplaySummaryHtml(graph) + graphRowHtml(balanceSeries, options, graph, xTotal)", web._HTML)
        self.assertIn("WDL=${wins}/${draws}/${losses}", web._HTML)
        self.assertIn("draw=${fmtGraphNumber(drawPercent)}%", web._HTML)
        self.assertIn("const graphTitle = isSelfplayGraph ? 'selfplay' : isLabelingGraph ? 'labeling' : title", web._HTML)
        self.assertIn("aggregate records/s over records scanned", web._HTML)
        self.assertIn("const primary = graphSeriesByName(orderedSeries, 'total') || orderedSeries[0]", web._HTML)
        self.assertIn("const plotted = options.plotAll ? orderedSeries : [primary]", web._HTML)
        self.assertIn("if (key === 'total') return 'var(--blu)'", web._HTML)
        self.assertIn("if (key === 'balance') return 'var(--grn)'", web._HTML)
        self.assertIn("if (graphSeriesName(name) === 'balance') return [-1, 1]", web._HTML)
        self.assertIn("return [1, 0.75, 0.5, 0.25, 0, -0.25, -0.5, -0.75, -1]", web._HTML)
        self.assertIn("if (key === 'wins') return 'var(--grn)'", web._HTML)
        self.assertIn("if (key === 'losses') return 'var(--red)'", web._HTML)
        self.assertIn("if (key === 'draws') return 'var(--blu)'", web._HTML)
        self.assertIn("if (key === 'wins') return 1", web._HTML)
        self.assertIn("if (key === 'draws') return 2", web._HTML)
        self.assertIn("if (key === 'losses') return 3", web._HTML)
        self.assertIn(
            "const markers = plotted.map(s => graphSeriesName(s.name) === 'games/s' ? '' : graphPointMarkers(",
            web._HTML,
        )
        self.assertIn('`<g clip-path="url(#${clipId})">${paths}${markers}</g>`', web._HTML)
        self.assertIn(".graph-row.combined .graph-axis { height: 72px; }", web._HTML)
        self.assertIn(".graph-row.combined .graph-value { display: none; }", web._HTML)
        self.assertNotIn("GRAPH_SMALL_H", web._HTML)
        self.assertNotIn("function renderTaskGraphs", web._HTML)
        self.assertIn("function graphCard", web._HTML)
        self.assertIn("graph?.series", web._HTML)
        self.assertIn("graphCard(r.progress_graph, 'run trend', r)", web._HTML)
        self.assertIn("function renderRunGraph", web._HTML)
        self.assertNotIn("renderTaskGraphs(active.tasks)", web._HTML)
        self.assertIn("r.progress_graph", web._HTML)
        self.assertIn(".run-tree", web._HTML)
        self.assertIn(".section::before", web._HTML)
        self.assertIn('<div class="run-tree">${sections.join', web._HTML)
        self.assertIn("function renderCommands", web._HTML)
        self.assertIn("function shellWords", web._HTML)
        self.assertIn("function commandDisplayLines", web._HTML)
        self.assertIn("function commandDisplayBlock", web._HTML)
        self.assertIn("word.startsWith('--')", web._HTML)
        self.assertIn("lines.join('\\n')", web._HTML)
        self.assertIn("commandDisplayBlock(c.command)", web._HTML)
        self.assertIn("function commandDisplayText", web._HTML)
        self.assertIn("const commentText = display.comment || ''", web._HTML)
        self.assertIn("function runDescriptionText", web._HTML)
        self.assertIn("const descText = runDescriptionText(display.description, Boolean(commentText))", web._HTML)
        self.assertIn("run-comment", web._HTML)
        self.assertIn(".replace(/\\s+/g, ' ')", web._HTML)
        self.assertIn("white-space: pre-wrap", web._HTML)
        self.assertIn("overflow-wrap: normal", web._HTML)
        self.assertIn("word-break: normal", web._HTML)
        self.assertIn("r.commands", web._HTML)
        self.assertIn("cmd-line", web._HTML)
        self.assertIn("'commands'", web._HTML)
        self.assertIn("function attrEsc", web._HTML)
        self.assertNotIn("function statePri", web._HTML)
        self.assertIn("failCount > 0 && activeCount === 0", web._HTML)
        self.assertIn("runCmd('${runArg}','retry',this)", web._HTML)
        self.assertIn("runCmd('${runArg}','del',this)", web._HTML)
        self.assertIn(".act.del", web._HTML)
        self.assertIn("function runStoppedEarly", web._HTML)

    def test_web_starts_sprt_graph_at_first_valid_ci(self) -> None:
        import forge_lib.web as web

        names_start = web._HTML.index("    function signedGraphSeries")
        names_end = web._HTML.index("    function graphNumberOption", names_start)
        option_start = names_end
        option_end = web._HTML.index("    function graphScaleMode", option_start)
        constants_start = web._HTML.index("    const GRAPH_W")
        constants_end = web._HTML.index("    function graphPoints", constants_start)
        display_start = web._HTML.index("    function graphDisplayPoints")
        display_end = web._HTML.index("    function graphYDomain", display_start)
        interpolate_start = web._HTML.index("    function interpolatedGraphY")
        interpolate_end = web._HTML.index("    function graphValueAt", interpolate_start)
        align_start = web._HTML.index("    function graphConfidenceAlignedPoints")
        align_end = web._HTML.index("    function graphConfidenceBandPoints", align_start)
        band_start = web._HTML.index("    function graphConfidenceBandPoints")
        band_end = web._HTML.index("    function graphXTotal", band_start)
        probe = (
            web._HTML[constants_start:constants_end]
            + web._HTML[names_start:names_end]
            + web._HTML[option_start:option_end]
            + web._HTML[display_start:display_end]
            + web._HTML[interpolate_start:interpolate_end]
            + web._HTML[align_start:align_end]
            + web._HTML[band_start:band_end]
            + """
const raw = [
  {x: 0, y: 0},
  {x: 2, y: 0},
  {x: 10, y: 35},
  {x: 14, y: 50},
  {x: 22, y: 48},
  {x: 40, y: 35},
  {x: 80, y: 10},
  {x: 120, y: 5},
  {x: 160, y: 0},
  {x: 200, y: -5},
];
const ci = [{x: 2, y: 0}, {x: 10, y: 40}, {x: 40, y: 10}, {x: 200, y: 8}];
const aligned = graphConfidenceAlignedPoints(raw, ci);
const elo = graphDisplayPoints({name: 'elo', smooth: true}, aligned);
const loss = graphDisplayPoints({name: 'loss', smooth: false}, raw);
const band = graphConfidenceBandPoints(
  {name: 'elo'},
  elo,
  ci,
  null,
  aligned,
);
const conePoints = [0, 20, 40, 60, 80, 100, 200].map(x => ({x, y: x === 0 ? 0 : 10}));
const cone = graphConfidenceBandPoints(
  {name: 'elo'},
  conePoints,
  [{x: 20, y: 0}, {x: 40, y: 0}, {x: 100, y: 50}, {x: 200, y: 20}],
);
const alignedZeroPrefix = graphConfidenceAlignedPoints(
  conePoints,
  [{x: 20, y: 0}, {x: 40, y: 0}, {x: 100, y: 50}, {x: 200, y: 20}],
);
console.log(JSON.stringify({
  raw: raw.map(point => point.x),
  elo: elo.map(point => Number(point.x.toFixed(3))),
  loss: loss.map(point => point.x),
  ordered: elo.every((point, index) => index === 0 || point.x > elo[index - 1].x),
  firstBandX: band[1].x,
  firstBandRadius: Number(((band[1].hi - band[1].lo) / 2).toFixed(1)),
  coneRadii: cone.map(point => Number(((point.hi - point.lo) / 2).toFixed(1))),
  alignedZeroPrefix: alignedZeroPrefix.map(point => point.x),
}));
"""
        )
        result = subprocess.run(
            ["node", "-e", probe],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            {
                "raw": [0, 2, 10, 14, 22, 40, 80, 120, 160, 200],
                "elo": [0, 10, 14, 22, 40, 80, 120, 160, 200],
                "loss": [0, 2, 10, 14, 22, 40, 80, 120, 160, 200],
                "ordered": True,
                "firstBandX": 10,
                "firstBandRadius": 40,
                "coneRadii": [0, 10, 20, 30, 40, 50, 20],
                "alignedZeroPrefix": [0, 100, 200],
            },
            json.loads(result.stdout),
        )

    def test_web_selfplay_balance_uses_fixed_domain_and_ticks(self) -> None:
        import forge_lib.web as web

        names_start = web._HTML.index("    function graphSeriesName")
        names_end = web._HTML.index("    function graphNumberOption", names_start)
        domain_start = web._HTML.index("    function graphYDomain")
        domain_end = web._HTML.index("    function graphYExtras", domain_start)
        y_start = web._HTML.index("    function graphY(name")
        y_end = web._HTML.index("    function zeroLine", y_start)
        ticks_start = web._HTML.index("    function graphTicks")
        ticks_end = web._HTML.index("    function graphAxisHtml", ticks_start)
        probe = (
            "const GRAPH_H = 280;\n"
            + web._HTML[names_start:names_end]
            + web._HTML[domain_start:domain_end]
            + web._HTML[y_start:y_end]
            + web._HTML[ticks_start:ticks_end]
            + """
const points = [{x: 4, y: 0}, {x: 30, y: -0.27}];
console.log(JSON.stringify({
  domain: graphYDomain('balance', points.map(point => point.y)),
  y: graphY('balance', points.map(point => point.y), -0.27),
  ticks: graphTicks('balance', points),
}));
"""
        )
        result = subprocess.run(
            ["node", "-e", probe],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        rendered = json.loads(result.stdout)
        self.assertEqual([-1, 1], rendered["domain"])
        self.assertAlmostEqual(175.64, rendered["y"], places=2)
        self.assertEqual([1, 0.75, 0.5, 0.25, 0, -0.25, -0.5, -0.75, -1], rendered["ticks"])

    def test_web_graph_ci_outliers_do_not_flatten_elo_scale(self) -> None:
        import forge_lib.web as web

        probe = r"""
function graphSeriesName(name) {
  return String(name || '').toLowerCase();
}
function signedGraphSeries(name) {
  return ['elo', 'llr'].includes(String(name || '').toLowerCase());
}
function graphYDomain(name, values, extras = []) {
  const all = values.concat(extras).filter(Number.isFinite);
  if (!all.length) return [0, 1];
  if (signedGraphSeries(name)) {
    const minBound = graphSeriesName(name) === 'elo' ? 8 : 0.15;
    const bound = Math.max(minBound, ...all.map(v => Math.abs(v)));
    return [-bound * 1.08, bound * 1.08];
  }
  return [0, 1];
}
function graphY(name, values, y, extras = [], h = 220) {
  const pad = 8;
  const [minY, maxY] = graphYDomain(name, values, extras);
  return maxY === minY
    ? h / 2
    : h - pad - ((y - minY) / (maxY - minY)) * (h - pad * 2);
}
function interpolatedGraphY(points, x) {
  if (!points || !points.length) return null;
  if (x <= points[0].x) return points[0].y;
  const last = points[points.length - 1];
  if (x >= last.x) return last.y;
  for (let i = 0; i < points.length - 1; i++) {
    const a = points[i], b = points[i + 1];
    if (x < a.x || x > b.x) continue;
    if (b.x === a.x) return b.y;
    const t = (x - a.x) / (b.x - a.x);
    return a.y + (b.y - a.y) * t;
  }
  return null;
}
function graphValueAt(points, x) {
  if (!points || !points.length) return null;
  const sorted = points
    .map(p => ({x: Number(p.x), y: Number(p.y)}))
    .filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
  if (!sorted.length || x < sorted[0].x) return null;
  return interpolatedGraphY(sorted, x);
}
function graphConfidenceBandPoints(name, points, ciPoints, maxBandRadius = null) {
  if (graphSeriesName(name) !== 'elo' || !points || !points.length || !ciPoints || !ciPoints.length) return [];
  const sortedCi = ciPoints
    .map(p => ({x: Number(p.x), y: Number(p.y)}))
    .filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
  if (!sortedCi.length) return [];
  const ciByX = new Map(sortedCi.map(p => [String(p.x), p.y]));
  let radius = 0;
  let hasWidth = false;
  const band = points.map((p, index) => {
    if (index > 0 && p.x >= sortedCi[0].x) {
      const exact = ciByX.get(String(p.x));
      const ci = Number(exact === undefined ? interpolatedGraphY(sortedCi, p.x) : exact);
      if (Number.isFinite(ci) && ci > 0) {
        radius = Number.isFinite(maxBandRadius) && maxBandRadius > 0
          ? Math.min(ci, maxBandRadius)
          : ci;
        hasWidth = radius > 0;
      }
    }
    return {x: p.x, lo: p.y - radius, hi: p.y + radius};
  });
  return hasWidth ? band : [];
}
const points = [
  {x: 0, y: 0},
  {x: 38, y: 34.85},
  {x: 142, y: 36.84},
  {x: 496, y: 43.67},
  {x: 500, y: 43.33},
];
const ci = [
  {x: 54, y: 564.34},
  {x: 60, y: 313.4},
  {x: 142, y: 64.85},
  {x: 496, y: 27.25},
  {x: 500, y: 27.09},
];
const values = points.map(p => p.y);
const domain = graphYDomain('elo', values, []);
const maxBandRadius = (domain[1] - domain[0]) * 0.30;
const band = graphConfidenceBandPoints('elo', points, ci, maxBandRadius);
const zero = graphY('elo', values, 0, [], 220);
const last = graphY('elo', values, 43.33, [], 220);
const byX = new Map(points.map(p => [p.x, p.y]));
const misses = band.filter(p => byX.has(p.x) && (byX.get(p.x) < p.lo || byX.get(p.x) > p.hi));
const hoverCi = graphValueAt(ci, 496);
console.log(JSON.stringify({band, domain, maxBandRadius, zero, last, delta: zero - last, misses, hoverCi}));
"""
        result = subprocess.run(
            ["node", "-e", probe],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        payload = json.loads(result.stdout)
        self.assertGreater(payload["delta"], 35)
        self.assertLess(abs(payload["domain"][1]), 140)
        self.assertGreater(payload["maxBandRadius"], 0)
        self.assertEqual([], payload["misses"])
        self.assertEqual([0, 38, 142, 496, 500], [point["x"] for point in payload["band"]])
        self.assertEqual(payload["band"][1]["lo"], payload["band"][1]["hi"])
        self.assertAlmostEqual(34.85, payload["band"][1]["lo"])
        self.assertAlmostEqual(27.25, payload["hoverCi"])

    def test_web_sorts_named_fallback_in_local_time(self) -> None:
        import forge_lib.web as web

        start = web._HTML.index("    function fmtAge")
        end = web._HTML.index("    let soundEnabled", start)
        probe = web._HTML[start:end] + """
const runs = [
  {run: 'native-4.20.0-rc1-sprt-1500-20260630-141436'},
  {run: 'sprt-enyo_f908b4b-1000-20260630-152429', created_at: '2026-06-30T15:24:29+0200'},
];
console.log(runs.sort(compareRunsByCreated).map(run => run.run).join('\\n'));
const done = {state:'done', created_at:'2026-07-02T21:16:00+0200', completed_at:'2026-07-03T00:42:00+0200'};
console.log(JSON.stringify(runTiming(done, false)));
console.log(JSON.stringify(runTiming({created_at:done.created_at}, true, Date.parse(done.completed_at) / 1000)));
"""
        result = subprocess.run(
            ["node", "-e", probe],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "TZ": "Europe/Oslo"},
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual(
            [
                "sprt-enyo_f908b4b-1000-20260630-152429",
                "native-4.20.0-rc1-sprt-1500-20260630-141436",
                '{"start":"02 Jul 21:16","elapsed":"3h26m"}',
                '{"start":"21:16","elapsed":"3h26m"}',
            ],
            result.stdout.splitlines(),
        )

    def test_web_favicon_asset_is_available(self) -> None:
        import forge_lib.web as web

        self.assertTrue(web._FAVICON_PATH.is_file())
        self.assertEqual("forge.png", web._FAVICON_PATH.name)
        self.assertGreater(len(web._favicon_bytes() or b""), 0)
        self.assertIn(".run.stopped-early", web._HTML)
        self.assertIn("stopped</span>", web._HTML)
        self.assertIn("state === 'done' ? 'completed'", web._HTML)
        self.assertNotIn("stopped early", web._HTML)
        self.assertIn("pendingCount <= 0", web._HTML)
        self.assertNotIn("state=stopped", web._HTML)
        self.assertIn("id=\"inactive\"", web._HTML)
        self.assertIn("let showInactiveRuns = localStorage.getItem('forge.showInactive') !== '0'", web._HTML)
        self.assertIn("function updateInactiveButton", web._HTML)
        self.assertIn("function toggleInactiveRuns", web._HTML)
        self.assertIn("function runIsActive", web._HTML)
        self.assertIn("state === 'current' || state === 'queued' || state === 'stalled'", web._HTML)
        self.assertIn(".filter(r => showInactiveRuns || runIsActive(r))", web._HTML)
        self.assertIn("updateInactiveButton()", web._HTML)
        self.assertIn("const HIDDEN_RUNS_KEY = 'forge.hiddenRuns.v1'", web._HTML)
        self.assertIn("const hiddenRunNames = loadHiddenRunNames()", web._HTML)
        self.assertIn("function saveHiddenRunNames", web._HTML)
        self.assertIn("function removeRunCard", web._HTML)
        self.assertIn("const hidden = (cmd === 'del' || cmd === 'hide') ? removeRunCard(run) : null", web._HTML)
        self.assertIn("function restoreRunCard", web._HTML)
        self.assertIn("hiddenRunNames.add(String(run))", web._HTML)
        self.assertIn("saveHiddenRunNames()", web._HTML)
        self.assertIn("const serverRunNames = new Set((data.runs||[]).map(r => String(r.run || '')))", web._HTML)
        self.assertIn("!hiddenRunNames.has(String(r.run || ''))", web._HTML)
        self.assertIn("const card = body?.closest?.('.run')", web._HTML)
        self.assertIn("const inactiveFailed = state === 'failed' && activeCount === 0", web._HTML)
        self.assertIn("(failCount > 0 && activeCount === 0) || inactiveFailed", web._HTML)
        self.assertIn("user-select: text", web._HTML)
        self.assertIn("onclick=\"event.stopPropagation();toggleId('${rId}')\"", web._HTML)
        self.assertIn("function toggleRunHeader", web._HTML)
        self.assertIn("event.target.closest('button,a,input,select,textarea,.arrow')", web._HTML)
        self.assertIn("window.getSelection", web._HTML)
        self.assertIn('<div class="run-hdr" onclick="toggleRunHeader(event,', web._HTML)
        self.assertIn("function runTiming", web._HTML)
        self.assertIn(".run-start", web._HTML)
        self.assertIn(".run-elapsed", web._HTML)
        self.assertIn("r?.created_at", web._HTML)
        self.assertIn("r?.completed_at", web._HTML)
        self.assertIn("function runNameEpoch", web._HTML)
        self.assertIn("function runSortEpoch", web._HTML)
        self.assertIn("text.replace(/([+-]\\d{2})(\\d{2})$/, '$1:$2')", web._HTML)
        self.assertIn("function compareRunsByCreated", web._HTML)
        self.assertIn("if (ea !== eb) return eb - ea", web._HTML)
        self.assertIn(".sort(compareRunsByCreated)", web._HTML)
        self.assertIn('<span class="run-name">${esc(r.run)}</span>', web._HTML)
        self.assertIn('<span class="run-start" title="started">${esc(timing.start)}</span>', web._HTML)
        self.assertIn('<span class="run-time-separator">·</span>', web._HTML)
        self.assertIn('<span class="run-elapsed" title="elapsed">${esc(timing.elapsed)}</span>', web._HTML)
        self.assertIn('<span class="run-spacer"></span>', web._HTML)
        self.assertIn("restartWeb(this)", web._HTML)
        self.assertIn("cmd: 'restart-web'", web._HTML)
        self.assertIn("await waitForRestart()", web._HTML)
        self.assertIn("/api/status?restart=", web._HTML)
        self.assertIn("restart requested but server did not come back", web._HTML)
        self.assertIn("id=\"restart\"", web._HTML)
        self.assertIn("const UI_STATE_KEY = 'forge.ui.v3'", web._HTML)
        self.assertIn("localStorage.setItem(UI_STATE_KEY", web._HTML)
        self.assertIn("const bodyOpen = resolveOpen(rId, isCurrent)", web._HTML)
        self.assertIn("openState[id] = nowOpen", web._HTML)
        self.assertNotIn("openState[el.id] = el.classList.contains('open')", web._HTML)
        self.assertIn("runGraph, true", web._HTML)
        self.assertIn("commandRows, false", web._HTML)
        self.assertIn("ws + renderTasks(active.tasks), false", web._HTML)
        self.assertIn("renderGroups(pending.groups), false", web._HTML)
        self.assertIn("renderGroups(fail.groups), false", web._HTML)
        self.assertIn("clearOldHashState()", web._HTML)
        self.assertNotIn("location.replace('#'", web._HTML)
        self.assertIn('path = self.path.split("?", 1)[0]', inspect.getsource(web._Handler.do_GET))
        self.assertIn("ThreadingHTTPServer", web.ThreadingHTTPServer.__name__)

    def test_web_restart_command_schedules_server_restart(self) -> None:
        import forge_lib.web as web

        server = object()
        with mock.patch("forge_lib.web._schedule_web_restart") as schedule:
            result = web._run_api_command("", "restart-web", server=server)

        schedule.assert_called_once_with(server)
        self.assertEqual(0, result.returncode)
        self.assertEqual("web restart requested\n", result.stdout)

    def test_web_restart_starts_replacement_before_shutdown(self) -> None:
        import forge_lib.web as web

        events = []

        class FakeThread:
            def __init__(self, *, target: object, args: object, daemon: bool) -> None:
                events.append("thread")
                self.daemon = daemon

            def start(self) -> None:
                events.append("shutdown")

        with (
            mock.patch("forge_lib.web._start_web_restarter", side_effect=lambda: events.append("restarter")),
            mock.patch("forge_lib.web.threading.Thread", side_effect=FakeThread),
        ):
            web._schedule_web_restart(object())

        self.assertEqual(["restarter", "thread", "shutdown"], events)

    def test_web_restart_spawn_failure_does_not_shutdown(self) -> None:
        import forge_lib.web as web

        with (
            mock.patch("forge_lib.web._start_web_restarter", side_effect=OSError("spawn failed")),
            mock.patch("forge_lib.web.threading.Thread") as thread,
        ):
            with self.assertRaisesRegex(OSError, "spawn failed"):
                web._run_api_command("", "restart-web", server=object())

        thread.assert_not_called()

    def test_web_restarter_command_preserves_restart_payload(self) -> None:
        import forge_lib.web as web

        with (
            mock.patch("forge_lib.web.os.getpid", return_value=1234),
            mock.patch("forge_lib.web._WEB_HOST", "0.0.0.0"),
            mock.patch("forge_lib.web._WEB_PORT", 9000),
        ):
            command = web._web_restarter_command()

        self.assertEqual([sys.executable, "-c", web._WEB_RESTARTER_CODE], command[:3])
        payload = json.loads(command[3])
        self.assertEqual(1234, payload["parent"])
        self.assertEqual([sys.executable, str(web.script_path()), "web", "--host", "0.0.0.0", "--port", "9000", "--foreground"], payload["argv"])

    def test_web_restart_argv_preserves_host_and_port(self) -> None:
        import forge_lib.web as web

        with (
            mock.patch("forge_lib.web._WEB_HOST", "0.0.0.0"),
            mock.patch("forge_lib.web._WEB_PORT", 9000),
        ):
            argv = web._web_restart_argv()

        self.assertEqual(sys.executable, argv[0])
        self.assertEqual([str(web.script_path()), "web", "--host", "0.0.0.0", "--port", "9000", "--foreground"], argv[1:])

    def test_web_delete_command_uses_force_for_failed_inactive_run(self) -> None:
        import forge_lib.web as web

        payload = {
            "runs": [
                {
                    "run": "failed-run",
                    "counts": {"done": 0, "fail": 1, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                    "sections": {"active": {"count": 0}, "fail": {"count": 1}},
                },
            ],
            "coordinator": None,
        }
        completed = subprocess.CompletedProcess(["forge"], 0, stdout="deleted: failed-run\n", stderr="")
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=payload),
            mock.patch("forge_lib.web.subprocess.run", return_value=completed) as run_mock,
            mock.patch("forge_lib.web._delete_web_run"),
        ):
            result = web._run_api_command("failed-run", "del")

        self.assertIs(completed, result)
        self.assertEqual(
            [sys.executable, str(web.script_path()), "del", "failed-run", "--force"],
            run_mock.call_args.args[0],
        )

    def test_web_delete_command_targets_visible_coordinator(self) -> None:
        import forge_lib.web as web

        payload = {
            "runs": [
                {
                    "run": "failed-run",
                    "counts": {"done": 0, "fail": 1, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                    "sections": {"active": {"count": 0}, "fail": {"count": 1}},
                },
            ],
            "coordinator": "pwa-5090:/runs",
        }
        completed = subprocess.CompletedProcess(["ssh"], 0, stdout="deleted: failed-run\n", stderr="")
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=payload),
            mock.patch("forge_lib.web.subprocess.run", return_value=completed) as run_mock,
            mock.patch("forge_lib.web._delete_web_run"),
        ):
            result = web._run_api_command("failed-run", "del")

        self.assertIs(completed, result)
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("pwa-5090", command)
        self.assertIn("FORGE_RUNS_DIR=/runs", command[-1])
        self.assertIn("forge del failed-run --force", command[-1])

    def test_web_delete_command_refuses_active_failed_run(self) -> None:
        import forge_lib.web as web

        payload = {
            "runs": [
                {
                    "run": "active-failed",
                    "counts": {"done": 0, "fail": 1, "running": 1, "claimed": 0, "stale": 0, "pending": 8},
                    "sections": {"active": {"count": 1}, "fail": {"count": 1}},
                },
            ],
            "coordinator": None,
        }
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=payload),
            mock.patch("forge_lib.web.subprocess.run") as run_mock,
            self.assertRaisesRegex(RuntimeError, "active tasks"),
        ):
            web._run_api_command("active-failed", "del")

        run_mock.assert_not_called()

    def test_progress_fields_drop_non_finite_values(self) -> None:
        data = forge.normalize_progress_data({
            "kind": "metadata",
            "done": 2,
            "total": 50,
            "unit": "games",
            "fields": {
                "elo": "-0.0",
                "ci": "nan",
                "los": "50.0%",
                "bad": "-inf",
            },
        })

        self.assertIsNotNone(data)
        self.assertEqual({"elo": "-0.0", "los": "50.0%"}, data["fields"])

    def test_fallback_progress_marks_estimated_eta(self) -> None:
        data = forge.fallback_progress_data("games", 2, 0.5, elapsed_s=1.0)

        self.assertIsNotNone(data)
        assert data is not None
        self.assertTrue(data["eta_estimate"])
        self.assertTrue(data["fallback_estimate"])
        self.assertEqual("games=0/2, eta~=4s", data["text"])

    def test_web_status_api_returns_cached_payload_while_refreshing(self) -> None:
        import forge_lib.web as web

        with web._STATUS_CACHE_LOCK:
            previous = (
                web._STATUS_CACHE,
                web._STATUS_CACHE_EPOCH,
                web._STATUS_CACHE_ERROR,
                web._STATUS_REFRESHING,
            )
            web._STATUS_CACHE = {
                "runs": [{"run": "cached"}],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "timestamp": 1.0,
            }
            web._STATUS_CACHE_EPOCH = 1.0
            web._STATUS_CACHE_ERROR = None
            web._STATUS_REFRESHING = False
        try:
            with (
                mock.patch("forge_lib.web.time.time", return_value=10.0),
                mock.patch("forge_lib.web._start_status_refresh") as start_refresh,
            ):
                payload = json.loads(web._api_status().decode())
            start_refresh.assert_called_once_with()
            self.assertEqual([{"run": "cached"}], payload["runs"])
            self.assertTrue(payload["refreshing"])
        finally:
            with web._STATUS_CACHE_LOCK:
                (
                    web._STATUS_CACHE,
                    web._STATUS_CACHE_EPOCH,
                    web._STATUS_CACHE_ERROR,
                    web._STATUS_REFRESHING,
                ) = previous

    def test_web_status_api_uses_coordinator_payload_before_first_refresh(self) -> None:
        import forge_lib.web as web

        with web._STATUS_CACHE_LOCK:
            previous = (
                web._STATUS_CACHE,
                web._STATUS_CACHE_EPOCH,
                web._STATUS_CACHE_ERROR,
                web._STATUS_REFRESHING,
            )
            web._STATUS_CACHE = None
            web._STATUS_CACHE_EPOCH = 0.0
            web._STATUS_CACHE_ERROR = None
            web._STATUS_REFRESHING = False
        try:
            coordinator_payload = {
                "runs": [{"run": "coordinator"}],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": "pwa-5090:/runs",
            }
            with tempfile.TemporaryDirectory() as tmp:
                with (
                    mock.patch("forge_lib.web.runs_dir", return_value=Path(tmp) / "runs"),
                    mock.patch("forge_lib.web.time.time", return_value=10.0),
                    mock.patch("forge_lib.web._collect_status_payload", return_value=coordinator_payload) as collect_status,
                    mock.patch("forge_lib.web._start_status_refresh") as start_refresh,
                ):
                    payload = json.loads(web._api_status().decode())
            collect_status.assert_called_once_with()
            start_refresh.assert_called_once_with()
            self.assertEqual([{"run": "coordinator"}], payload["runs"])
            self.assertTrue(payload["refreshing"])
            self.assertNotIn("loading", payload)
        finally:
            with web._STATUS_CACHE_LOCK:
                (
                    web._STATUS_CACHE,
                    web._STATUS_CACHE_EPOCH,
                    web._STATUS_CACHE_ERROR,
                    web._STATUS_REFRESHING,
                ) = previous

    def test_web_invalidate_status_cache_drops_cached_payload(self) -> None:
        import forge_lib.web as web

        with web._STATUS_CACHE_LOCK:
            previous = (
                web._STATUS_CACHE,
                web._STATUS_CACHE_EPOCH,
                web._STATUS_CACHE_ERROR,
                web._STATUS_REFRESHING,
            )
            web._STATUS_CACHE = {
                "runs": [{"run": "stale"}],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "timestamp": 1.0,
            }
            web._STATUS_CACHE_EPOCH = 1.0
            web._STATUS_CACHE_ERROR = "old"
            web._STATUS_REFRESHING = False
        try:
            web._invalidate_status_cache()
            with web._STATUS_CACHE_LOCK:
                self.assertIsNone(web._STATUS_CACHE)
                self.assertEqual(0.0, web._STATUS_CACHE_EPOCH)
                self.assertIsNone(web._STATUS_CACHE_ERROR)
        finally:
            with web._STATUS_CACHE_LOCK:
                (
                    web._STATUS_CACHE,
                    web._STATUS_CACHE_EPOCH,
                    web._STATUS_CACHE_ERROR,
                    web._STATUS_REFRESHING,
                ) = previous

    def test_web_status_payload_prefers_local_active_runs(self) -> None:
        import forge_lib.web as web

        coordinator = "pwa-5090:/runs"
        local_payload = {
            "runs": [
                {
                    "run": "local-active",
                    "counts": {"done": 0, "fail": 0, "running": 1, "claimed": 0, "stale": 0, "pending": 0},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": None,
        }
        coordinator_payload = {
            "runs": [
                {
                    "run": "remote-active",
                    "counts": {"done": 0, "fail": 0, "running": 1, "claimed": 0, "stale": 0, "pending": 0},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": coordinator,
        }
        remote_done = {
            "run": "remote-done",
            "run_dir": "/runs/remote-done",
            "manifest": "/runs/remote-done/manifest.json",
            "state": "done",
            "tasks": 1,
            "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
        }
        with tempfile.TemporaryDirectory() as tmp:
            with (
                mock.patch("forge_lib.web.runs_dir", return_value=Path(tmp) / "runs"),
                mock.patch("forge_lib.web.configured_coordinator_runs", return_value=coordinator),
                mock.patch("forge_lib.web._collect_local_status_payload", return_value=local_payload),
                mock.patch("forge_lib.web._collect_coordinator_status_payload", return_value=coordinator_payload) as collect_coordinator,
                mock.patch("forge_lib.web._read_coordinator_web_state", return_value={
                    "seen_runs": {"remote-done": {"source": "remember"}},
                    "lingering_runs": {"remote-done": remote_done},
                }),
                mock.patch("forge_lib.web._collect_named_run_payload", return_value=remote_done) as named,
            ):
                payload = web._collect_status_payload()

        collect_coordinator.assert_not_called()
        named.assert_called_once_with("remote-done", coordinator)
        self.assertEqual(["local-active", "remote-done"], [run["run"] for run in payload["runs"]])
        self.assertEqual("done", payload["runs"][1]["state"])
        self.assertIsNone(payload["coordinator"])

    def test_web_lingers_completed_seen_runs_without_all_status(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            active_payload = {
                "runs": [
                    {
                        "run": "short-sprt",
                        "state": "current",
                        "tasks": 1,
                        "counts": {"done": 0, "fail": 0, "running": 1, "claimed": 0, "stale": 0, "pending": 0},
                    },
                ],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }
            done_payload = {
                "run": "short-sprt",
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._merge_lingering_runs(copy.deepcopy(active_payload))
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=done_payload) as named:
                    payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    again = web._merge_lingering_runs(copy.deepcopy(empty_payload))

            named.assert_called_once_with("short-sprt", None)
            self.assertEqual(["short-sprt"], [run["run"] for run in payload["runs"]])
            self.assertEqual("done", payload["runs"][0]["state"])
            self.assertEqual(["short-sprt"], [run["run"] for run in again["runs"]])

    def test_web_lingers_remembered_run_that_finished_between_polls(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            done_payload = {
                "run": "fast-sprt",
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web.remember_web_run("fast-sprt")
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=done_payload) as named:
                    payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))

            named.assert_called_once_with("fast-sprt", None)
            self.assertEqual(["fast-sprt"], [run["run"] for run in payload["runs"]])
            self.assertEqual("done", payload["runs"][0]["state"])

    def test_web_remember_does_not_clear_deleted_tombstone(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            done_payload = {
                "run": "deleted-sprt",
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._write_web_state({"deleted_runs": ["deleted-sprt"]})
                web.remember_web_run("deleted-sprt")
                web.remember_web_run_payload(done_payload)
                state = web._read_web_state()

            self.assertEqual(["deleted-sprt"], state["deleted_runs"])
            self.assertEqual({}, state.get("seen_runs", {}))
            self.assertEqual({}, state.get("lingering_runs", {}))

    def test_web_recreated_run_clears_deleted_tombstone(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            payload = {
                "runs": [{
                    "run": "recreated",
                    "state": "stopped",
                    "tasks": 2,
                    "counts": {
                        "done": 0,
                        "fail": 0,
                        "running": 0,
                        "claimed": 0,
                        "stale": 0,
                        "pending": 2,
                    },
                }],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._write_web_state({"deleted_runs": ["recreated"]})
                merged = web._merge_lingering_runs(copy.deepcopy(payload))
                state = web._read_web_state()

            self.assertEqual(["recreated"], [run["run"] for run in merged["runs"]])
            self.assertEqual([], state["deleted_runs"])

    def test_notify_resolves_repo_relative_executable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            script = repo / "scripts" / "notify.sh"
            script.parent.mkdir(parents=True)
            script.write_text("#!/bin/sh\n", encoding="utf-8")
            with mock.patch("forge_lib.status.script_repo", return_value=repo), mock.patch(
                "forge_lib.status.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0, "", ""),
            ) as run:
                rc = forge.run_notify_command("scripts/notify.sh --flag", {"event": "done"})

            self.assertEqual(0, rc)
            self.assertEqual([str(script), "--flag"], run.call_args.args[0])

    def test_notify_expands_tilde_executable(self) -> None:
        home = Path.home()
        script = home / "code" / "cpp" / "chess" / "forge" / "scripts" / "forge_event_ntfy.sh"
        with mock.patch("forge_lib.status.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            rc = forge.run_notify_command("~/code/cpp/chess/forge/scripts/forge_event_ntfy.sh", {"event": "done"})

        self.assertEqual(0, rc)
        self.assertEqual([str(script)], run.call_args.args[0])

    def test_web_remembered_completion_normalizes_ok_state(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "ok-finished"
            payload = {
                "run": "ok-finished",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                forge.write_json(run_dir / "manifest.json", {
                    "schema": "forge.task.v1",
                    "name": "ok-finished",
                    "work_dir": str(run_dir),
                    "state_dir": str(run_dir / "state"),
                    "tasks": [],
                })
                web.remember_web_run_payload(payload)
                merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))

            self.assertEqual(["ok-finished"], [run["run"] for run in merged["runs"]])
            self.assertEqual("done", merged["runs"][0]["state"])

    def test_web_ignores_completion_outside_runs_root(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            outside = root / "outside" / "tmp-finished"
            payload = {
                "run": "tmp-finished",
                "run_dir": str(outside),
                "manifest": str(outside / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web.remember_web_run_payload(payload)
                state = web._read_web_state()

            self.assertEqual({}, state)

    def test_web_ignores_completion_outside_coordinator_runs_root(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            outside = root / "outside" / "tmp-finished"
            payload = {
                "run": "tmp-finished",
                "run_dir": str(outside),
                "manifest": str(outside / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web.remember_web_run_payload(payload, coordinator_runs="pwa-5090:/home/petter/code/cpp/chess/forge/runs")
                state = web._read_web_state()

            self.assertEqual({}, state)

    def test_web_collects_coordinator_lingering_state(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator-runs"
            coordinator.mkdir()
            run_dir = coordinator / "remote-done"
            done_payload = {
                "run": "remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            forge.write_json(coordinator / ".web-state.json", {
                "hidden_runs": ["hidden-done"],
                "deleted_runs": ["deleted-done"],
                "seen_runs": {"remote-done": {"source": "remember"}},
                "lingering_runs": {"remote-done": done_payload},
            })
            status = subprocess.CompletedProcess(
                ["forge"],
                0,
                stdout=json.dumps({"runs": [], "collapsed_failures": [], "collapsed_failures_count": 0}),
                stderr="",
            )

            with mock.patch("forge_lib.web.subprocess.run", return_value=status):
                payload = web._collect_coordinator_status_payload(str(coordinator))

            self.assertEqual(done_payload, payload["remote_lingering_runs"]["remote-done"])
            self.assertEqual({"source": "remember"}, payload["remote_seen_runs"]["remote-done"])
            self.assertEqual(["hidden-done"], payload["remote_hidden_runs"])
            self.assertEqual(["deleted-done"], payload["remote_deleted_runs"])

    def test_web_ignores_unseen_coordinator_lingering_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            run_dir = coordinator / "remote-done"
            done_payload = {
                "run": "remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
                "remote_lingering_runs": {"remote-done": done_payload},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                with mock.patch("forge_lib.web._collect_named_run_payload") as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_not_called()
            self.assertEqual([], merged["runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_imports_seen_coordinator_lingering_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            run_dir = coordinator / "remote-done"
            done_payload = {
                "run": "remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            full_payload = {
                **done_payload,
                "state": "done",
                "display": {"fields": ["tasks=1/1", "elo=+8.0", "ci=20.6"]},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
                "remote_lingering_runs": {"remote-done": done_payload},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                web._write_web_state({
                    "seen_runs": {"remote-done": {"coordinator": str(coordinator), "last_seen": 1.0, "source": "status"}},
                })
                forge.write_json(run_dir / "manifest.json", {
                    "schema": "forge.task.v1",
                    "name": "remote-done",
                    "work_dir": str(run_dir),
                    "state_dir": str(run_dir / "state"),
                    "tasks": [],
                })
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=full_payload) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_called_once_with("remote-done", str(coordinator))
            self.assertEqual(["remote-done"], [run["run"] for run in merged["runs"]])
            self.assertEqual("done", merged["runs"][0]["state"])
            self.assertEqual(["tasks=1/1", "elo=+8.0", "ci=20.6"], merged["runs"][0]["display"]["fields"])
            self.assertEqual("done", state["lingering_runs"]["remote-done"]["state"])

    def test_web_ignores_coordinator_lingering_run_outside_runs_root(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            outside = root / "outside" / "tmp-finished"
            done_payload = {
                "run": "tmp-finished",
                "run_dir": str(outside),
                "manifest": str(outside / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
                "remote_lingering_runs": {"tmp-finished": done_payload},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                state = web._read_web_state()

            self.assertEqual([], merged["runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_prunes_legacy_imported_coordinator_lingering_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            run_dir = coordinator / "old-remote-done"
            stale_payload = {
                "run": "old-remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                web._write_web_state({
                    "seen_runs": {"old-remote-done": {"coordinator": str(coordinator), "last_seen": 1.0}},
                    "lingering_runs": {"old-remote-done": stale_payload},
                })
                merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                state = web._read_web_state()

            self.assertEqual([], merged["runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_does_not_reimport_deleted_coordinator_lingering_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            run_dir = coordinator / "deleted-done"
            stale_payload = {
                "run": "deleted-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
                "remote_lingering_runs": {"deleted-done": stale_payload},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                web._write_web_state({
                    "seen_runs": {"deleted-done": {"coordinator": str(coordinator), "last_seen": 1.0, "source": "status"}},
                    "deleted_runs": ["deleted-done"],
                })
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=stale_payload) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_not_called()
            self.assertEqual([], merged["runs"])
            self.assertEqual({}, state["lingering_runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual(["deleted-done"], state["deleted_runs"])

    def test_web_imports_verified_coordinator_lingering_run_after_local_state_reset(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator_root = root / "coordinator-runs"
            coordinator = f"pwa-5090:{coordinator_root}"
            run_dir = coordinator_root / "remote-done"
            stale_payload = {
                "run": "remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": coordinator,
                "remote_lingering_runs": {"remote-done": stale_payload},
                "remote_seen_runs": {"remote-done": {"source": "remember"}},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=stale_payload) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

        named.assert_called_once_with("remote-done", coordinator)
        self.assertEqual(["remote-done"], [run["run"] for run in merged["runs"]])
        self.assertIn("remote-done", state["lingering_runs"])
        self.assertEqual(coordinator, state["seen_runs"]["remote-done"]["coordinator"])

    def test_web_prunes_lingering_runs_outside_runs_root(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            outside = root / "outside" / "tmp-finished"
            stale = {
                "run": "tmp-finished",
                "run_dir": str(outside),
                "manifest": str(outside / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                forge.write_json(runs / ".web-state.json", {
                    "seen_runs": {"tmp-finished": {"coordinator": None, "last_seen": 1.0}},
                    "lingering_runs": {"tmp-finished": stale},
                })
                merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                state = web._read_web_state()

            self.assertEqual([], merged["runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_enriches_lingering_completed_run_commands(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run_dir = runs / "done-run"
            stale = {
                "run": "done-run",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                "commands": [
                    {
                        "label": "sprt:8",
                        "count": 2,
                        "command": "sprt --candidate c --games 8 --log-dir {output_dir}/sprt_0000/logs --name sprt_0000",
                    },
                ],
            }
            fresh = {
                **stale,
                "commands": [{"label": "forge sprt", "count": 1, "command": "forge sprt --candidate c --games 16"}],
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                forge.write_json(run_dir / "manifest.json", {
                    "schema": "forge.task.v1",
                    "name": "done-run",
                    "work_dir": str(run_dir),
                    "state_dir": str(run_dir / "state"),
                    "tasks": [],
                })
                forge.write_json(runs / ".web-state.json", {
                    "lingering_runs": {"done-run": stale},
                    "command_checked_runs": ["done-run"],
                })
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=fresh) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_called_once_with("done-run", None)
            self.assertEqual(fresh["commands"], merged["runs"][0]["commands"])
            self.assertEqual(fresh["commands"], state["lingering_runs"]["done-run"]["commands"])

    def test_web_enriches_lingering_completed_run_graph_metadata(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run_dir = runs / "done-run"
            stale = {
                "run": "done-run",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                "commands": [{"label": "forge sprt", "count": 1, "command": "forge sprt --candidate c --games 16"}],
                "progress_graph": {
                    "series": [
                        {"name": "ci", "points": [{"x": 1, "y": 50}, {"x": 2, "y": 40}]},
                        {"name": "elo", "points": [{"x": 1, "y": 5}, {"x": 2, "y": 8}]},
                    ],
                },
            }
            fresh = copy.deepcopy(stale)
            fresh["progress_graph"]["series"][1].update({
                "band_max_fraction": 0.24,
                "band_scale": 0.35,
                "band_series": "ci",
                "scale": "symmetric",
            })
            fresh["progress_graph"]["version"] = forge.PROGRESS_GRAPH_VERSION
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                forge.write_json(run_dir / "manifest.json", {
                    "schema": "forge.task.v1",
                    "name": "done-run",
                    "work_dir": str(run_dir),
                    "state_dir": str(run_dir / "state"),
                    "tasks": [],
                })
                forge.write_json(runs / ".web-state.json", {
                    "lingering_runs": {"done-run": stale},
                })
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=fresh) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_called_once_with("done-run", None)
            series = {item["name"]: item for item in merged["runs"][0]["progress_graph"]["series"]}
            self.assertEqual("ci", series["elo"]["band_series"])
            self.assertEqual(0.35, series["elo"]["band_scale"])
            self.assertEqual(0.24, series["elo"]["band_max_fraction"])
            self.assertEqual(fresh["progress_graph"], state["lingering_runs"]["done-run"]["progress_graph"])
            self.assertEqual(web._WEB_RUN_SNAPSHOT_VERSION, state["lingering_runs"]["done-run"]["web_snapshot_version"])

    def test_web_refreshes_lingering_completed_selfplay_without_graph(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run_dir = runs / "selfplay-done"
            stale = {
                "run": "selfplay-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "done",
                "kind": "selfplay",
                "tasks": 2,
                "counts": {"done": 2, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                "commands": [{"label": "forge selfplay", "count": 1, "command": "forge selfplay --games 100"}],
                "web_snapshot_version": web._WEB_RUN_SNAPSHOT_VERSION,
            }
            fresh = {
                **stale,
                "progress_graph": {
                    "version": forge.PROGRESS_GRAPH_VERSION,
                    "x_unit": "games",
                    "x_total": 100,
                    "series": [
                        {"name": "wins", "points": [{"x": 4, "y": 1}, {"x": 100, "y": 29}]},
                        {"name": "losses", "points": [{"x": 4, "y": 1}, {"x": 100, "y": 30}]},
                    ],
                },
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                forge.write_json(run_dir / "manifest.json", {
                    "schema": "forge.task.v1",
                    "name": "selfplay-done",
                    "kind": "selfplay",
                    "work_dir": str(run_dir),
                    "state_dir": str(run_dir / "state"),
                    "tasks": [],
                })
                forge.write_json(runs / ".web-state.json", {
                    "lingering_runs": {"selfplay-done": stale},
                })
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=fresh) as named:
                    merged = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_called_once_with("selfplay-done", None)
            self.assertEqual(fresh["progress_graph"], merged["runs"][0]["progress_graph"])
            self.assertEqual(fresh["progress_graph"], state["lingering_runs"]["selfplay-done"]["progress_graph"])

    def test_web_refreshes_seed_specific_legacy_commands(self) -> None:
        import forge_lib.web as web

        self.assertTrue(web._commands_need_refresh({
            "commands": [{"label": "sprt", "count": 80, "command": "sprt --candidate c --srand 2026060800"}],
        }))
        self.assertTrue(web._commands_need_refresh({
            "commands": [{"label": "sprt", "count": 80, "command": "sprt --candidate c --srand=2026060800"}],
        }))
        self.assertFalse(web._commands_need_refresh({
            "commands": [{"label": "sprt", "count": 80, "command": "sprt --candidate c --srand {seed}"}],
        }))

    def test_web_drops_remembered_run_that_never_started(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            empty_run_payload = {
                "run": "failed-setup",
                "state": "empty",
                "tasks": 1,
                "counts": {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 1},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web.remember_web_run("failed-setup")
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=empty_run_payload) as named:
                    payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    again = web._merge_lingering_runs(copy.deepcopy(empty_payload))

            named.assert_called_once_with("failed-setup", None)
            self.assertEqual([], payload["runs"])
            self.assertEqual([], again["runs"])

    def test_web_hide_removes_lingering_completed_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            done_payload = {
                "runs": [
                    {
                        "run": "short-sprt",
                        "state": "done",
                        "tasks": 1,
                        "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                    },
                ],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._merge_lingering_runs(copy.deepcopy(done_payload))
                web._hide_web_run("short-sprt")
                payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))

            self.assertEqual([], payload["runs"])

    def test_web_prunes_deleted_lingering_completed_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            deleted = runs / "deleted-sprt"
            stale = {
                "run": "deleted-sprt",
                "run_dir": str(deleted),
                "manifest": str(deleted / "manifest.json"),
                "state": "done",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                "commands": [{"label": "forge sprt", "count": 1, "command": "forge sprt --games 1"}],
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                state = {
                    "seen_runs": {"deleted-sprt": {"coordinator": None, "last_seen": 1.0}},
                    "lingering_runs": {"deleted-sprt": stale},
                    "hidden_runs": [],
                }
                web._write_web_state(state)
                payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                state = web._read_web_state()

            self.assertEqual([], payload["runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_delete_prevents_coordinator_lingering_reimport(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_runs = root / "local-runs"
            coordinator = root / "coordinator-runs"
            run_dir = coordinator / "remote-done"
            done_payload = {
                "run": "remote-done",
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "state": "ok",
                "tasks": 1,
                "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
            }
            empty_payload = {
                "runs": [],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": str(coordinator),
                "remote_lingering_runs": {"remote-done": done_payload},
                "remote_seen_runs": {"remote-done": {"source": "remember"}},
            }

            with self.patch_runs_dir(local_runs), mock.patch("forge_lib.web.runs_dir", return_value=local_runs):
                web._delete_web_run("remote-done")
                with mock.patch("forge_lib.web._collect_named_run_payload", return_value=done_payload) as named:
                    payload = web._merge_lingering_runs(copy.deepcopy(empty_payload))
                    state = web._read_web_state()

            named.assert_not_called()
            self.assertEqual([], payload["runs"])
            self.assertEqual([], state["hidden_runs"])
            self.assertEqual(["remote-done"], state["deleted_runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_delete_tombstone_filters_visible_status_run(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            status_payload = {
                "runs": [
                    {
                        "run": "remote-done",
                        "state": "done",
                        "tasks": 1,
                        "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                    },
                ],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": "pwa-5090:/runs",
            }

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._write_web_state({"deleted_runs": ["remote-done"]})
                payload = web._merge_lingering_runs(copy.deepcopy(status_payload))
                state = web._read_web_state()

            self.assertEqual([], payload["runs"])
            self.assertEqual(["remote-done"], state["deleted_runs"])
            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_web_delete_purges_all_local_run_metadata(self) -> None:
        import forge_lib.web as web

        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"

            with self.patch_runs_dir(runs), mock.patch("forge_lib.web.runs_dir", return_value=runs):
                web._write_web_state({
                    "seen_runs": {"dead-run": {"source": "status"}},
                    "lingering_runs": {"dead-run": {"run": "dead-run", "state": "done"}},
                    "hidden_runs": ["dead-run", "other-run"],
                    "command_checked_runs": ["dead-run", "other-run"],
                })
                web._delete_web_run("dead-run")
                state = web._read_web_state()

            self.assertEqual({}, state["seen_runs"])
            self.assertEqual({}, state["lingering_runs"])
            self.assertEqual(["other-run"], state["hidden_runs"])
            self.assertEqual(["dead-run"], state["deleted_runs"])
            self.assertEqual(["other-run"], state["command_checked_runs"])

    def test_web_delete_tombstone_is_written_before_delete_command(self) -> None:
        import forge_lib.web as web

        done_payload = {
            "runs": [
                {
                    "run": "short-sprt",
                    "state": "done",
                    "tasks": 1,
                    "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": None,
        }
        completed = subprocess.CompletedProcess(["del"], 0, stdout="deleted\n", stderr="")
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"

            def run_cli(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
                state = web._read_web_state()
                self.assertEqual(["short-sprt"], state["deleted_runs"])
                return completed

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.web.runs_dir", return_value=runs),
                mock.patch("forge_lib.web._collect_status_payload", return_value=done_payload),
                mock.patch("forge_lib.web._run_web_cli", side_effect=run_cli),
            ):
                result = web._run_api_command("short-sprt", "del")

        self.assertEqual(0, result.returncode)

    def test_web_delete_rolls_back_tombstone_when_delete_command_fails(self) -> None:
        import forge_lib.web as web

        done_payload = {
            "runs": [
                {
                    "run": "short-sprt",
                    "state": "done",
                    "tasks": 1,
                    "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": None,
        }
        failed = subprocess.CompletedProcess(["del"], 1, stdout="", stderr="delete failed\n")
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.web.runs_dir", return_value=runs),
                mock.patch("forge_lib.web._collect_status_payload", return_value=done_payload),
                mock.patch("forge_lib.web._run_web_cli", return_value=failed),
            ):
                result = web._run_api_command("short-sprt", "del")
                state = web._read_web_state()

        self.assertEqual(1, result.returncode)
        self.assertEqual([], state["deleted_runs"])
        self.assertEqual({}, state["seen_runs"])
        self.assertEqual({}, state["lingering_runs"])

    def test_web_delete_allows_completed_runs(self) -> None:
        import forge_lib.web as web

        done_payload = {
            "runs": [
                {
                    "run": "short-sprt",
                    "state": "done",
                    "tasks": 1,
                    "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": None,
        }
        completed = subprocess.CompletedProcess(["del"], 0, stdout="deleted\n", stderr="")
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=done_payload),
            mock.patch("forge_lib.web._run_web_cli", return_value=completed) as run_cli,
            mock.patch("forge_lib.web._delete_web_run") as delete,
        ):
            result = web._run_api_command("short-sprt", "del")

        self.assertEqual(0, result.returncode)
        run_cli.assert_called_once_with(["del", "short-sprt", "--force"], coordinator_runs=None)
        delete.assert_called_once_with("short-sprt")

    def test_web_delete_allows_stopped_incomplete_runs(self) -> None:
        import forge_lib.web as web

        stopped_payload = {
            "runs": [
                {
                    "run": "stopped-sprt",
                    "state": "stopped",
                    "tasks": 2,
                    "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 1},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": "pwa-5090:/runs",
        }
        completed = subprocess.CompletedProcess(["del"], 0, stdout="deleted\n", stderr="")
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=stopped_payload),
            mock.patch("forge_lib.web._run_web_cli", return_value=completed) as run_cli,
            mock.patch("forge_lib.web._delete_web_run") as delete,
        ):
            result = web._run_api_command("stopped-sprt", "del")

        self.assertEqual(0, result.returncode)
        run_cli.assert_called_once_with(["del", "stopped-sprt", "--force"], coordinator_runs="pwa-5090:/runs")
        delete.assert_called_once_with("stopped-sprt")

    def test_web_delete_allows_failed_launch_without_failed_shards(self) -> None:
        import forge_lib.web as web

        failed_payload = {
            "runs": [
                {
                    "run": "launch-failed-sprt",
                    "state": "failed",
                    "tasks": 20,
                    "launch_state": "stale",
                    "counts": {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 20},
                },
            ],
            "collapsed_failures": [],
            "collapsed_failures_count": 0,
            "coordinator": None,
        }
        completed = subprocess.CompletedProcess(["del"], 0, stdout="deleted\n", stderr="")
        with (
            mock.patch("forge_lib.web._collect_status_payload", return_value=failed_payload),
            mock.patch("forge_lib.web._run_web_cli", return_value=completed) as run_cli,
            mock.patch("forge_lib.web._delete_web_run") as delete,
        ):
            result = web._run_api_command("launch-failed-sprt", "del")

        self.assertEqual(0, result.returncode)
        run_cli.assert_called_once_with(["del", "launch-failed-sprt", "--force"], coordinator_runs=None)
        delete.assert_called_once_with("launch-failed-sprt")

    def test_web_delete_refuses_empty_and_queued_runs(self) -> None:
        import forge_lib.web as web

        for state in ("empty", "queued"):
            payload = {
                "runs": [
                    {
                        "run": f"{state}-sprt",
                        "state": state,
                        "tasks": 136,
                        "counts": {"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 136},
                    },
                ],
                "collapsed_failures": [],
                "collapsed_failures_count": 0,
                "coordinator": None,
            }
            with (
                mock.patch("forge_lib.web._collect_status_payload", return_value=payload),
                mock.patch("forge_lib.web._run_web_cli") as run_cli,
                mock.patch("forge_lib.web._delete_web_run") as delete,
                self.assertRaisesRegex(RuntimeError, "delete is only available for inactive runs"),
            ):
                web._run_api_command(f"{state}-sprt", "del")

            run_cli.assert_not_called()
            delete.assert_not_called()

    def test_web_current_detection_ignores_stopped_runs(self) -> None:
        import forge_lib.web as web

        payload = {
            "runs": [
                {
                    "run": "stopped-sprt",
                    "state": "stopped",
                    "counts": {"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 1},
                },
            ],
        }

        self.assertFalse(web._status_payload_has_current_run(payload))

    def test_web_local_status_does_not_use_all(self) -> None:
        import forge_lib.web as web

        result = subprocess.CompletedProcess(
            ["forge"],
            0,
            stdout=json.dumps({"runs": [], "collapsed_failures": [], "collapsed_failures_count": 0}),
            stderr="",
        )
        with mock.patch("forge_lib.web.subprocess.run", return_value=result) as run_mock:
            web._collect_local_status_payload()

        command = run_mock.call_args.args[0]
        self.assertIn("status", command)
        self.assertIn("--json", command)
        self.assertNotIn("--all", command)

    def test_config_yml_supplies_default_workers_and_template_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.yml"
            config.write_text(
                f'default_workers: "worker-a:2,worker-b:3"\n'
                f'coordinator_host: "coordinator-a"\n'
                f'notify_command: "notify-me"\n'
                f'speedup.single_host_workers: 7\n'
                f"template_dirs:\n"
                f"  - \"{root / 'templates'}\"\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config), "FORGE_NOTIFY_COMMAND": ""}, clear=False):
                payload = forge.load_config()
                self.assertEqual("worker-a:2,worker-b:3", forge.default_workers_arg(payload))
                self.assertEqual("coordinator-a", forge.default_coordinator_host(payload))
                self.assertEqual("notify-me", forge.default_notify_command(payload))
                self.assertEqual(7, forge.speedup_single_host_workers(payload))
                self.assertEqual(root / "templates", forge.config_template_dirs(payload)[0])

    def test_empty_coordinator_env_does_not_hide_file_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            runs.mkdir()
            (runs / "coordinator").write_text("pwa-5090:/runs\n", encoding="utf-8")
            with (
                mock.patch("forge_lib.core.script_repo", return_value=root),
                mock.patch.dict(os.environ, {"FORGE_COORDINATOR_RUNS": ""}, clear=True),
            ):
                self.assertEqual("pwa-5090:/runs", forge.configured_coordinator_runs())

            with mock.patch.dict(
                os.environ,
                {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(runs)},
                clear=False,
            ):
                self.assertIsNone(forge.configured_coordinator_runs())

    def test_template_expands_without_exposing_task_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            with mock.patch.dict(os.environ, {"FORGE_CONFIG": str(root / "config.yml")}, clear=False):
                (root / "config.yml").write_text(
                    f'default_workers: "worker-a:2"\n'
                    f"template_dirs:\n"
                    f"  - \"{root / 'templates'}\"\n",
                    encoding="utf-8",
                )
                template = forge.load_template(template_path)
                params = forge.parse_template_parameters(
                    "demo",
                    template,
                    ["--item", "/tmp/input.dat", "--total", "10", "--shard", "4"],
                )
                manifest = forge.expand_template("demo", template_path, template, params)

            self.assertEqual("forge.task.v1", manifest["schema"])
            self.assertEqual("demo-input.dat-10", manifest["name"])
            self.assertEqual(3, len(manifest["tasks"]))
            self.assertEqual(["demo_0000", "demo_0001", "demo_0002"], [task["id"] for task in manifest["tasks"]])
            self.assertEqual([4, 4, 2], [task["progress"]["total"] for task in manifest["tasks"]])
            self.assertEqual("printf 4 > {output_dir}/demo_0000.txt", manifest["tasks"][0]["command"])
            self.assertIn("{output_dir}/demo_0000.txt", manifest["tasks"][0]["outputs"])

    def test_template_accepts_legacy_chunk_split(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            with mock.patch.dict(os.environ, {"FORGE_CONFIG": str(root / "config.yml")}, clear=False):
                (root / "config.yml").write_text(
                    f'default_workers: "worker-a:2"\n'
                    f"template_dirs:\n"
                    f"  - \"{root / 'templates'}\"\n",
                    encoding="utf-8",
                )
                template = forge.load_template(template_path)
                template["parameters"]["chunk"] = {"type": "int", "default": 3}
                template["parameters"].pop("shard")
                template["task"]["split"] = {"total": "total", "chunk": "chunk", "unit": "rows"}
                template["task"]["command"] = "printf {chunk} > {output_dir}/{task_name}.txt"
                params = forge.parse_template_parameters(
                    "demo",
                    template,
                    ["--item", "/tmp/input.dat", "--total", "10", "--chunk", "4"],
                )
                manifest = forge.expand_template("demo", template_path, template, params)

            self.assertEqual([4, 4, 2], [task["progress"]["total"] for task in manifest["tasks"]])
            self.assertEqual("printf 4 > {output_dir}/demo_0000.txt", manifest["tasks"][0]["command"])

    def test_template_accepts_legacy_chunk_parameter_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            template = forge.load_template(template_path)
            params = forge.parse_template_parameters(
                "demo",
                template,
                ["--item", "/tmp/input.dat", "--total", "10", "--chunk", "4"],
            )
            manifest = forge.expand_template("demo", template_path, template, params)

            self.assertEqual([4, 4, 2], [task["progress"]["total"] for task in manifest["tasks"]])

    def test_template_splits_total_by_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            template = forge.load_template(template_path)
            template["parameters"]["shards"] = {"type": "int", "default": 3}
            template["task"]["split"] = {"total": "total", "count": "shards", "unit": "rows"}
            template["task"]["command"] = "printf {shard} > {output_dir}/{task_name}.txt"
            params = forge.parse_template_parameters(
                "demo",
                template,
                ["--item", "/tmp/input.dat", "--total", "10", "--shards", "3"],
            )
            manifest = forge.expand_template("demo", template_path, template, params)

            self.assertEqual([4, 3, 3], [task["progress"]["total"] for task in manifest["tasks"]])
            self.assertEqual("printf 4 > {output_dir}/demo_0000.txt", manifest["tasks"][0]["command"])

    def test_template_worker_path_parameters_preserve_tilde(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            template = forge.load_template(template_path)

            params = forge.parse_template_parameters(
                "demo",
                template,
                ["--item", str(Path.home() / "input.dat"), "--total", "10"],
            )

        self.assertEqual("~/input.dat", params["item"])

    def test_template_worker_path_parameters_expand_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            template = forge.load_template(template_path)
            with mock.patch.dict(os.environ, {"PWD": str(Path.home() / "project")}, clear=False):
                params = forge.parse_template_parameters(
                    "demo",
                    template,
                    ["--item", "./input.dat", "--total", "10"],
                )

        self.assertEqual("~/project/input.dat", params["item"])

    def test_template_worker_path_parameter_accepts_directory_executable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project = Path.home() / "project"
            template_path = self.write_test_template(root)
            template = forge.load_template(template_path)
            template["parameters"]["item"]["directory_executable"] = "build/enyo"
            with (
                mock.patch.dict(os.environ, {"PWD": str(project)}, clear=False),
                mock.patch("pathlib.Path.is_dir", lambda path: str(path) == str(project)),
            ):
                params = forge.parse_template_parameters(
                    "demo",
                    template,
                    ["--item", ".", "--total", "10"],
                )

        self.assertEqual("~/project/build/enyo", params["item"])

    def test_template_repeatable_parameters_render_shell_args(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            templates = root / "templates"
            templates.mkdir(parents=True)
            template_path = templates / "repeat.template.json"
            template_path.write_text(
                json.dumps({
                    "schema": "forge.template.v1",
                    "name": "repeat",
                    "parameters": {
                        "opt": {
                            "repeatable": True,
                            "arg": "--engine-option",
                        },
                    },
                    "task": {
                        "split": {"count": 1},
                        "command": "run {opt_args}",
                    },
                }),
                encoding="utf-8",
            )
            template = forge.load_template(template_path)
            params = forge.parse_template_parameters(
                "repeat",
                template,
                ["--opt", "nnue_file=/tmp/a.nn", "--opt", "Debug Log File=/tmp/a b.log"],
            )
            manifest = forge.expand_template("repeat", template_path, template, params)

        self.assertEqual(
            "run --engine-option nnue_file=/tmp/a.nn --engine-option 'Debug Log File=/tmp/a b.log'",
            manifest["tasks"][0]["command"],
        )

    def test_template_derives_commit_from_hashed_path_stem(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            templates = root / "templates"
            templates.mkdir(parents=True)
            template_path = templates / "engine.template.json"
            template_path.write_text(
                json.dumps({
                    "schema": "forge.template.v1",
                    "name": "engine",
                    "run_name": "engine-{candidate_hash}",
                    "parameters": {
                        "candidate": {"required": True, "worker_path": True},
                    },
                    "requirements": {
                        "repos": [{
                            "name": "engine",
                            "path": "~/code/engine",
                            "commit": "{candidate_commit}",
                        }],
                    },
                    "task": {
                        "split": {"count": 1},
                        "command": "printf ok",
                    },
                }),
                encoding="utf-8",
            )
            template = forge.load_template(template_path)
            params = forge.parse_template_parameters(
                "engine",
                template,
                ["--candidate", str(Path.home() / "assets/engines/enyo_9696dac")],
            )
            manifest = forge.expand_template("engine", template_path, template, params)

        self.assertEqual("~/assets/engines/enyo_9696dac", manifest["template"]["parameters"]["candidate"])
        self.assertEqual("engine-9696dac", manifest["name"])
        self.assertEqual("9696dac", manifest["requirements"]["repos"][0]["commit"])

    def test_training_template_wraps_nnue_build_create(self) -> None:
        template_path = ROOT / "templates" / "training.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_template_parameters("training", template, [])
        manifest = forge.expand_template("training", template_path, template, params)

        self.assertEqual("~/code/cpp/chess/nnue", manifest["work_dir"])
        self.assertEqual(1, len(manifest["tasks"]))
        task = manifest["tasks"][0]
        self.assertEqual("training_0000", task["id"])
        self.assertEqual("training", task["label"])
        self.assertEqual("./build.py create -c build.json", task["command"])
        self.assertNotIn("--shard", task["command"])
        self.assertNotIn("--output-dir", task["command"])
        self.assertEqual("steps", task["progress"]["unit"])
        self.assertEqual(["loss", "lr"], [spec["name"] for spec in task["progress"]["series"]])

    def test_template_commands_list_and_show(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_test_template(root)
            config = root / "config.yml"
            config.write_text(
                f"template_dirs:\n"
                f"  - \"{root / 'templates'}\"\n",
                encoding="utf-8",
            )
            env = {"FORGE_CONFIG": str(config)}

            listing = self.run_cli("template", "list", env=env)
            self.assertEqual(0, listing.returncode, listing.stderr)
            self.assertIn("demo - demo template", listing.stdout)

            shown = self.run_cli("template", "show", "demo", env=env)
            self.assertEqual(0, shown.returncode, shown.stderr)
            self.assertEqual("forge.template.v1", json.loads(shown.stdout)["schema"])

            shown_by_path = self.run_cli("template", "show", str(root / "templates" / "demo.template.json"), env=env)
            self.assertEqual(0, shown_by_path.returncode, shown_by_path.stderr)
            self.assertEqual("demo", json.loads(shown_by_path.stdout)["name"])

    def test_run_template_accepts_explicit_template_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            config = root / "config.yml"
            config.write_text(
                f"template_dirs:\n"
                f"  - \"{root / 'templates'}\"\n",
                encoding="utf-8",
            )
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{"host": "worker-a", "slots": 2}],
            })
            captured = {}

            def fake_run_job(args):
                captured["args"] = args
                return 0

            with (
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config), "FORGE_NOTIFY_COMMAND": ""}, clear=False),
                mock.patch("forge_lib.workers.cmd_run_job", side_effect=fake_run_job),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_template_run(argparse.Namespace(
                        template=str(template_path),
                        template_args=["--item", "/tmp/input.dat", "--total", "10"],
                    ))

            self.assertEqual(0, rc)
            self.assertEqual(str(workers), captured["args"].workers)
            self.assertEqual("run: id=demo-input.dat-10\n", stdout.getvalue())

    def test_run_template_immediately_prints_explicit_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            template_path = self.write_test_template(root)
            config = root / "config.yml"
            config.write_text("", encoding="utf-8")
            forge.write_json(root / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{"host": "worker-a", "slots": 2}],
            })

            with (
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config)}, clear=False),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0),
                mock.patch("builtins.print") as print_mock,
            ):
                rc = forge.cmd_template_run(argparse.Namespace(
                    template=str(template_path),
                    template_args=[
                        "--run", "explicit-run",
                        "--item", "/tmp/input.dat",
                        "--total", "10",
                    ],
                ))

            self.assertEqual(0, rc)
            print_mock.assert_called_once_with("run: id=explicit-run", flush=True)

    def test_sprt_command_alias_runs_sprt_template(self) -> None:
        args = forge.build_parser().parse_args([
            "sprt",
            "--reference", "~/assets/engines/reference",
            "--candidate", "~/assets/engines/candidate",
            "--games", "256",
        ])

        self.assertIs(args.func, forge.cmd_template_run)
        self.assertEqual("sprt", args.template)
        self.assertEqual([
            "--reference", "~/assets/engines/reference",
            "--candidate", "~/assets/engines/candidate",
            "--games", "256",
        ], args.template_args)

    def test_selfplay_command_alias_runs_selfplay_template(self) -> None:
        args = forge.build_parser().parse_args([
            "selfplay",
            "--engine", "~/assets/engines/enyo_9696dac",
            "--games", "256",
        ])

        self.assertIs(args.func, forge.cmd_template_run)
        self.assertEqual("selfplay", args.template)
        self.assertEqual([
            "--engine", "~/assets/engines/enyo_9696dac",
            "--games", "256",
        ], args.template_args)

    def test_sprt_command_alias_accepts_dashdash_passthrough(self) -> None:
        run_options, template_args = forge.parse_run_options([
            "--workers", "pwa-mbp:4",
            "--comment", "quick smoke",
            "--",
            "--candidate", "~/assets/engines/candidate",
            "--games", "256",
        ])

        self.assertEqual("pwa-mbp:4", run_options.workers)
        self.assertEqual("quick smoke", run_options.comment)
        self.assertEqual([
            "--candidate", "~/assets/engines/candidate",
            "--games", "256",
        ], template_args)

    def test_sprt_command_auto_sizes_shards_to_worker_multiple(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="sprt",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-mbp:4,pwa-mbp0:5,pwa-5090:6",
                    "--comment", "quick smoke",
                    "--candidate", "~/assets/engines/enyo_9696dac",
                    "--games", "256",
                ],
            ))
        manifest = json.loads(stdout.getvalue())

        self.assertEqual(0, rc)
        self.assertEqual("quick smoke", manifest["comment"])
        self.assertEqual(30, manifest["template"]["parameters"]["shards"])
        self.assertEqual(30, len(manifest["tasks"]))
        totals = [task["progress"]["total"] for task in manifest["tasks"]]
        self.assertEqual(256, sum(totals))
        self.assertEqual(8, min(totals))
        self.assertEqual(10, max(totals))

    def test_sprt_command_rounds_fifty_target_shards_to_two_worker_waves(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="sprt",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-5090:11,pwa-mbp:5,pwa-mbp0:5,pwa-wrk:16,pwa-wsl:7",
                    "--candidate", "~/assets/engines/enyo_91ede5f",
                    "--games", "1250",
                ],
            ))
        manifest = json.loads(stdout.getvalue())
        totals = [task["progress"]["total"] for task in manifest["tasks"]]

        self.assertEqual(0, rc)
        self.assertEqual(88, manifest["template"]["parameters"]["shards"])
        self.assertEqual(88, len(manifest["tasks"]))
        self.assertEqual(0, len(manifest["tasks"]) % 44)
        self.assertEqual(1250, sum(totals))
        self.assertEqual(14, min(totals))
        self.assertEqual(16, max(totals))

    def test_sprt_command_preserves_explicit_shards(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="sprt",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-mbp:4,pwa-mbp0:5,pwa-5090:6",
                    "--candidate", "~/assets/engines/enyo_9696dac",
                    "--games", "256",
                    "--shards", "8",
                ],
            ))
        manifest = json.loads(stdout.getvalue())

        self.assertEqual(0, rc)
        self.assertEqual(8, manifest["template"]["parameters"]["shards"])
        self.assertEqual(8, len(manifest["tasks"]))

    def test_sprt_command_auto_shards_avoid_short_game_shards(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="sprt",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-5090:7,pwa-mbp0:5,pwa-mbp:4",
                    "--candidate", "~/assets/engines/enyo_91ede5f",
                    "--games", "400",
                ],
            ))
        manifest = json.loads(stdout.getvalue())
        totals = [task["progress"]["total"] for task in manifest["tasks"]]

        self.assertEqual(0, rc)
        self.assertEqual(32, manifest["template"]["parameters"]["shards"])
        self.assertEqual(32, len(manifest["tasks"]))
        self.assertEqual(400, sum(totals))
        self.assertEqual(12, min(totals))
        self.assertEqual(14, max(totals))

    def test_sprt_command_auto_shards_large_runs_by_target_games(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="sprt",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-5090:7,pwa-mbp0:5,pwa-mbp:4",
                    "--candidate", "~/assets/engines/enyo_91ede5f",
                    "--games", "8000",
                ],
            ))
        manifest = json.loads(stdout.getvalue())
        totals = [task["progress"]["total"] for task in manifest["tasks"]]

        self.assertEqual(0, rc)
        self.assertEqual(320, manifest["template"]["parameters"]["shards"])
        self.assertEqual(320, len(manifest["tasks"]))
        self.assertEqual(8000, sum(totals))
        self.assertEqual(24, min(totals))
        self.assertEqual(26, max(totals))

    def test_run_template_uses_default_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_test_template(root)
            config = root / "config.yml"
            config.write_text(
                f'coordinator_host: "coordinator-a"\n'
                f'notify_command: "notify-me"\n'
                f"template_dirs:\n"
                f"  - \"{root / 'templates'}\"\n",
                encoding="utf-8",
            )
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{"host": "worker-a", "slots": 2}],
            })
            captured = {}

            def fake_run_job(args):
                captured["args"] = args
                return 0

            with (
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config), "FORGE_NOTIFY_COMMAND": ""}, clear=False),
                mock.patch("forge_lib.workers.cmd_run_job", side_effect=fake_run_job),
            ):
                rc = forge.cmd_template_run(argparse.Namespace(
                    template="demo",
                    template_args=["--item", "/tmp/input.dat", "--total", "10"],
                ))

            self.assertEqual(0, rc)
            self.assertEqual(str(workers), captured["args"].workers)
            self.assertEqual("coordinator-a", captured["args"].coordinator_host)
            self.assertIsNone(captured["args"].notify_command)

    def test_run_template_threads_task_complete_options_to_deploy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_test_template(root)
            config = root / "config.yml"
            config.write_text(
                f'coordinator_host: "coordinator-a"\n'
                f"template_dirs:\n"
                f"  - \"{root / 'templates'}\"\n",
                encoding="utf-8",
            )
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{"host": "worker-a", "slots": 2}],
            })
            captured = {}

            def fake_run_job(args):
                captured["args"] = args
                return 0

            with (
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config), "FORGE_NOTIFY_COMMAND": ""}, clear=False),
                mock.patch("forge_lib.workers.cmd_run_job", side_effect=fake_run_job),
            ):
                rc = forge.cmd_template_run(argparse.Namespace(
                    template="demo",
                    template_args=[
                        "--item", "/tmp/input.dat",
                        "--total", "10",
                        "--task-complete-command", "~/scripts/hook.sh",
                        "--task-complete-sync",
                    ],
                ))

            self.assertEqual(0, rc)
            self.assertEqual("~/scripts/hook.sh", captured["args"].task_complete_command)
            self.assertTrue(captured["args"].task_complete_sync)

    def test_run_template_prefers_environment_over_default_workers_file(self) -> None:
        run_options = argparse.Namespace(workers=None)
        workers_path = Path("/tmp/forge-runs/workers.json")

        with (
            mock.patch.dict(os.environ, {"FORGE_WORKERS": "environment-worker:2"}),
            mock.patch("forge_lib.workers.default_workers_path", return_value=workers_path),
        ):
            workers = forge.resolved_workers_arg(run_options, {})

        self.assertEqual("environment-worker:2", workers)

    def test_run_template_falls_back_to_workers_file_without_config_default(self) -> None:
        run_options = argparse.Namespace(workers=None)
        workers_path = Path("/tmp/forge-runs/workers.json")

        with mock.patch("forge_lib.workers.default_workers_path", return_value=workers_path):
            workers = forge.resolved_workers_arg(run_options, {})

        self.assertEqual(str(workers_path), workers)

    def test_sprt_template_uses_candidate_executable_path(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        project = Path.home() / "code/cpp/chess/enyo"
        with (
            mock.patch.dict(os.environ, {"PWD": str(project)}, clear=False),
            mock.patch("pathlib.Path.is_dir", lambda path: str(path) == str(project)),
        ):
            params = forge.parse_template_parameters(
                "sprt",
                template,
                ["--candidate", ".", "--reference", "~/assets/engines/reference", "--games", "50", "--shards", "1"],
            )
        manifest = forge.expand_template("sprt", template_path, template, params)

        self.assertEqual("~/code/cpp/chess/enyo/build/enyo", manifest["template"]["parameters"]["candidate"])
        self.assertEqual(["sprt", "fastchess"], [repo["name"] for repo in manifest["requirements"]["repos"]])
        self.assertIn("--candidate ~/code/cpp/chess/enyo/build/enyo", manifest["tasks"][0]["command"])

    def test_sprt_template_resolves_candidate_symlink_for_run_name(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.home()) as tmp:
            root = Path(tmp)
            engines = root / "assets" / "engines"
            engines.mkdir(parents=True)
            target = engines / "enyo_637c4be"
            target.write_text("#!/bin/sh\n", encoding="utf-8")
            link = engines / "reference"
            link.symlink_to(target)
            link_arg = "~/" + str(link.relative_to(Path.home()))
            target_arg = "~/" + str(target.relative_to(Path.home()))
            template_path = ROOT / "templates" / "sprt.template.json"
            template = forge.load_template(template_path)

            params = forge.parse_sprt_template_parameters(
                "sprt",
                template,
                ["--candidate", link_arg, "--games", "256", "--shards", "1"],
            )
            manifest = forge.expand_template("sprt", template_path, template, params)

        self.assertEqual(target_arg, manifest["template"]["parameters"]["candidate"])
        self.assertTrue(manifest["name"].startswith("sprt-enyo_637c4be-256-"))
        self.assertIn(f"--candidate {target_arg}", manifest["tasks"][0]["command"])

    def test_sprt_template_passes_unknown_sprt_args_through(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_sprt_template_parameters(
            "sprt",
            template,
            [
                "--candidate", "~/assets/engines/enyo_9696dac",
                "--reference", "~/assets/engines/reference-static",
                "--games", "125",
                "--shards", "3",
                "--opening-order", "random",
                "--custom-flag",
            ],
        )
        manifest = forge.expand_template("sprt", template_path, template, params)

        self.assertEqual(["--opening-order", "random", "--custom-flag"], manifest["template"]["parameters"]["sprt_extra_args"])
        self.assertIn("--reference ~/assets/engines/reference-static", manifest["tasks"][0]["command"])
        self.assertIn("--opening-order random --custom-flag", manifest["tasks"][0]["command"])
        self.assertIn("--games 42", manifest["tasks"][0]["command"])
        self.assertIn("--games 41", manifest["tasks"][2]["command"])
        self.assertNotIn("--games 125", manifest["tasks"][0]["command"])

    def test_sprt_template_splits_games_evenly_by_shards(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_sprt_template_parameters(
            "sprt",
            template,
            ["--candidate", "~/assets/engines/enyo_9696dac", "--games", "256", "--shards", "28"],
        )
        manifest = forge.expand_template("sprt", template_path, template, params)

        self.assertEqual(28, len(manifest["tasks"]))
        totals = [task["progress"]["total"] for task in manifest["tasks"]]
        self.assertEqual(16, totals.count(10))
        self.assertEqual(12, totals.count(8))
        self.assertEqual(256, sum(totals))
        self.assertEqual(["llr", "elo", "ci"], [spec["name"] for spec in manifest["tasks"][0]["progress"]["series"]])
        self.assertIn("?P<llr>", manifest["tasks"][0]["progress"]["log_regex"])
        self.assertEqual(
            {"strategy": "split-progress", "count_arg": "--games"},
            manifest["tasks"][0]["resume"],
        )

    def test_template_commit_placeholder_accepts_checkout_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "engine"
            repo.mkdir()
            subprocess.run(["git", "-C", str(repo), "init"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (repo / "engine.cpp").write_text("int main() { return 0; }\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "engine.cpp"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "-c",
                    "user.name=Petter Wahlman",
                    "-c",
                    "user.email=petter@wahlman.no",
                    "commit",
                    "-m",
                    "init",
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            template_path = root / "legacy.template.json"
            template_path.write_text(
                json.dumps({
                    "schema": "forge.template.v1",
                    "name": "legacy",
                    "run_name": "legacy-{candidate_stem}",
                    "parameters": {
                        "candidate": {"required": True, "worker_path": True},
                    },
                    "requirements": {
                        "repos": [{
                            "name": "engine",
                            "path": "~/code/engine",
                            "commit": "{candidate_commit}",
                        }],
                    },
                    "task": {
                        "split": {"count": 1},
                        "command": "run {candidate}",
                    },
                }),
                encoding="utf-8",
            )
            template = forge.load_template(template_path)
            params = forge.parse_template_parameters("legacy", template, ["--candidate", str(repo)])
            manifest = forge.expand_template("legacy", template_path, template, params)

        self.assertEqual(commit, manifest["requirements"]["repos"][0]["commit"])
        self.assertEqual(f"~/assets/engines/engine_{commit[:7]}", manifest["template"]["parameters"]["candidate"])
        self.assertEqual(f"run ~/assets/engines/engine_{commit[:7]}", manifest["tasks"][0]["command"])

    def test_sprt_template_uses_inline_worker_slots_literally(self) -> None:
        with mock.patch("forge_lib.workers.default_workers_path", side_effect=SystemExit("no workers")):
            payload = forge.sprt_worker_slots_payload("pwa-mbp:4,pwa-mbp0:5,pwa-5090:6,pwa-rce:0")

        self.assertEqual(
            [
                {"host": "pwa-mbp", "profile": "pwa-mbp", "local": False, "slots": 4},
                {"host": "pwa-mbp0", "profile": "pwa-mbp0", "local": False, "slots": 5},
                {"host": "pwa-5090", "profile": "pwa-5090", "local": False, "slots": 6},
            ],
            payload["workers"],
        )
        self.assertNotIn("sprt_thread_budget", payload)

    def test_sprt_template_does_not_scale_slots_by_engine_settings(self) -> None:
        with mock.patch("forge_lib.workers.default_workers_path", side_effect=SystemExit("no workers")):
            payload = forge.sprt_worker_slots_payload("pwa-mbp:16")

        self.assertEqual(
            [{"host": "pwa-mbp", "profile": "pwa-mbp", "local": False, "slots": 16}],
            payload["workers"],
        )

    def test_sprt_template_accepts_one_worker_slot(self) -> None:
        with mock.patch("forge_lib.workers.default_workers_path", side_effect=SystemExit("no workers")):
            payload = forge.sprt_worker_slots_payload("pwa-mbp:1")

        self.assertEqual(
            [{"host": "pwa-mbp", "profile": "pwa-mbp", "local": False}],
            payload["workers"],
        )

    def test_sprt_template_uses_worker_file_slots_literally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "pwa-mbp", "profile": "mac", "slots": 8},
                        {"host": "pwa-5090", "slots": 10},
                        {"host": "pwa-rce", "slots": 0},
                    ],
                }),
                encoding="utf-8",
            )

            payload = forge.sprt_worker_slots_payload(str(workers_path))

        self.assertEqual(
            [
                {"host": "pwa-mbp", "profile": "mac", "local": False, "slots": 8},
                {"host": "pwa-5090", "profile": "pwa-5090", "local": False, "slots": 10},
                {"host": "pwa-rce", "profile": "pwa-rce", "local": False, "slots": 0},
            ],
            payload["workers"],
        )

    def test_sprt_template_passes_candidate_and_reference_uci_options(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_template_parameters(
            "sprt",
            template,
            [
                "--candidate", "~/assets/engines/enyo_9696dac",
                "--reference", "~/assets/engines/reference",
                "--games", "50",
                "--shards", "1",
                "--candidate-uci", "nnue_file=~/code/cpp/chess/enyo/net/candidate.nn",
                "--candidate-uci", "Hash=256",
                "--reference-uci", "nnue_file=~/code/cpp/chess/enyo/net/reference.nn",
            ],
        )
        manifest = forge.expand_template("sprt", template_path, template, params)
        command = manifest["tasks"][0]["command"]

        self.assertIn("--candidate-uci nnue_file=~/code/cpp/chess/enyo/net/candidate.nn", command)
        self.assertIn("--candidate-uci Hash=128", command)
        self.assertIn("--reference-uci Hash=128", command)
        self.assertIn("--candidate-uci Hash=256", command)
        self.assertLess(command.index("--candidate-uci Hash=128"), command.index("--candidate-uci Hash=256"))
        self.assertIn("--reference-uci nnue_file=~/code/cpp/chess/enyo/net/reference.nn", command)

    def test_sprt_template_hash_override_applies_to_both_engines(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_template_parameters(
            "sprt",
            template,
            [
                "--candidate", "~/assets/engines/enyo_9696dac",
                "--games", "50",
                "--shards", "1",
                "--hash", "64",
            ],
        )
        manifest = forge.expand_template("sprt", template_path, template, params)
        command = manifest["tasks"][0]["command"]

        self.assertIn("--candidate-uci Hash=64", command)
        self.assertIn("--reference-uci Hash=64", command)
        self.assertNotIn("--hash", command)

    def test_sprt_template_uses_commit_scoped_tool_paths(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)
        repos = {item["name"]: item for item in template["requirements"]["repos"]}

        for repo in repos.values():
            self.assertIn(repo["commit"], repo["path"])

        params = forge.parse_template_parameters(
            "sprt",
            template,
            ["--candidate", "~/assets/engines/enyo_9696dac", "--games", "2", "--shards", "1"],
        )
        manifest = forge.expand_template("sprt", template_path, template, params)
        command = manifest["tasks"][0]["command"]

        self.assertIn(repos["sprt"]["path"] + "/sprt", command)
        self.assertIn(repos["fastchess"]["path"] + "/fastchess", command)
        self.assertNotIn("~/.local/bin", command)

    def test_sprt_template_rejects_empty_uci_option_values(self) -> None:
        template_path = ROOT / "templates" / "sprt.template.json"
        template = forge.load_template(template_path)

        with self.assertRaisesRegex(SystemExit, "--reference-uci nnue_file="):
            forge.parse_sprt_template_parameters(
                "sprt",
                template,
                [
                    "--candidate", "~/assets/engines/enyo_9696dac",
                    "--reference", "~/assets/engines/reference",
                    "--games", "50",
                    "--reference-uci", "nnue_file=",
                ],
            )

    def test_sprt_template_syncs_data_but_not_engine_executables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                name: root / name
                for name in ("candidate", "reference", "book.epd", "candidate.nn", "reference.nn")
            }
            for name, path in paths.items():
                path.write_bytes(name.encode())
            template_path = ROOT / "templates" / "sprt.template.json"
            template = forge.load_template(template_path)
            params = forge.parse_sprt_template_parameters(
                "sprt",
                template,
                [
                    "--candidate", str(paths["candidate"]),
                    "--reference", str(paths["reference"]),
                    "--book", str(paths["book.epd"]),
                    "--games", "50",
                    "--shards", "2",
                    "--candidate-uci", f"nnue_file={paths['candidate.nn']}",
                    "--reference-uci", f"nnue_file={paths['reference.nn']}",
                ],
            )
            manifest = forge.expand_template("sprt", template_path, template, params)

            forge.materialize_template_inputs(template, manifest, params)

            inputs = manifest["requirements"]["inputs"]
            self.assertEqual(3, len(inputs))
            self.assertEqual(
                {"book", "candidate_uci.nnue_file", "reference_uci.nnue_file"},
                {item["name"] for item in inputs},
            )
            self.assertEqual(
                {paths["book.epd"], paths["candidate.nn"], paths["reference.nn"]},
                {Path(item["path"]) for item in inputs},
            )

    def test_selfplay_template_generates_pgn_shards(self) -> None:
        template_path = ROOT / "templates" / "selfplay.template.json"
        template = forge.load_template(template_path)
        params = forge.parse_selfplay_template_parameters(
            "selfplay",
            template,
            [
                "--engine", "~/assets/engines/enyo_9696dac",
                "--book", "~/assets/books/openings.epd",
                "--games", "64",
                "--shards", "4",
                "--uci", "nnue_file=~/code/cpp/chess/enyo/net/default.nn",
                "--hash", "64",
                "--threads", "2",
                "--tc", "5+0.05",
            ],
        )
        manifest = forge.expand_template("selfplay", template_path, template, params)
        command = manifest["tasks"][0]["command"]

        self.assertEqual("selfplay", manifest["kind"])
        self.assertEqual(4, len(manifest["tasks"]))
        self.assertEqual([16, 16, 16, 16], [task["progress"]["total"] for task in manifest["tasks"]])
        self.assertEqual(["{output_dir}/selfplay_0000.pgn"], manifest["tasks"][0]["outputs"])
        self.assertIn("-rounds 8 -games 2", command)
        self.assertIn("option.Hash=64", command)
        self.assertIn("option.Threads=2", command)
        self.assertIn('option.nnue_file="$HOME/code/cpp/chess/enyo/net/default.nn"', command)
        self.assertIn("-pgnout file=\"{output_dir}/selfplay_0000.pgn\" notation=uci", command)
        self.assertIn("Games:", manifest["tasks"][0]["progress"]["log_regex"])

    def test_selfplay_live_progress_prefers_completed_pgn_games_over_zero_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "logs"
            output = root / "output"
            logs.mkdir()
            output.mkdir()
            task = {
                "id": "selfplay_0000",
                "outputs": [str(output / "selfplay_0000.pgn")],
                "progress": {
                    "unit": "games",
                    "total": 16,
                    "log_regex": (
                        r"Games:\s+(?P<done>\d+),\s+Wins:\s+(?P<wins>\d+),"
                        r"\s+Losses:\s+(?P<losses>\d+),\s+Draws:\s+(?P<draws>\d+)"
                    ),
                },
            }
            manifest = {
                "kind": "selfplay",
                "work_dir": str(root),
                "log_dir": str(logs),
                "tasks": [task],
            }
            (logs / "selfplay_0000.log").write_text(
                "Games: 0, Wins: 0, Losses: 0, Draws: 0\n",
                encoding="utf-8",
            )
            (output / "selfplay_0000.pgn").write_text(
                '[Event "game 1"]\n[Result "1-0"]\n\n1. e4 1-0\n\n'
                '[Event "game 2"]\n[Result "1/2-1/2"]\n\n1. d4 1/2-1/2\n\n'
                '[Event "game 3"]\n[Result "0-1"]\n\n1. c4 0-1\n',
                encoding="utf-8",
            )

            data = forge.task_log_progress_data(manifest, task)

            self.assertIsNotNone(data)
            assert data is not None
            self.assertEqual(3, data["done"])
            self.assertEqual(16, data["total"])
            self.assertEqual("games=3/16", data["text"])

    def test_selfplay_template_assigns_distinct_deterministic_seed_per_shard(self) -> None:
        template_path = ROOT / "templates" / "selfplay.template.json"
        template = forge.load_template(template_path)

        def seeds_for(base_seed: str) -> list[int]:
            params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                [
                    "--engine", "~/assets/engines/enyo_9696dac",
                    "--book", "~/assets/books/openings.epd",
                    "--games", "300",
                    "--shards", "3",
                    "--seed", base_seed,
                ],
            )
            manifest = forge.expand_template("selfplay", template_path, template, params)
            found = []
            for task in manifest["tasks"]:
                match = re.search(r"-srand (\d+)", task["command"])
                self.assertIsNotNone(match, task["command"])
                found.append(int(match.group(1)))
            return found

        seeds = seeds_for("5")
        self.assertEqual([5, 6, 7], seeds)
        self.assertEqual(len(seeds), len(set(seeds)), "shard seeds must be distinct")

        # Same base seed reproduces the exact same per-shard seeds.
        self.assertEqual(seeds, seeds_for("5"))

        # Default base seed (1) shifts the same way.
        default_params = forge.parse_selfplay_template_parameters(
            "selfplay",
            template,
            [
                "--engine", "~/assets/engines/enyo_9696dac",
                "--book", "~/assets/books/openings.epd",
                "--games", "300",
                "--shards", "3",
            ],
        )
        default_manifest = forge.expand_template("selfplay", template_path, template, default_params)
        default_seeds = [
            int(re.search(r"-srand (\d+)", task["command"]).group(1))
            for task in default_manifest["tasks"]
        ]
        self.assertEqual([1, 2, 3], default_seeds)

    def test_selfplay_command_auto_sizes_shards_for_queue_depth(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            rc = forge.cmd_template_run(argparse.Namespace(
                template="selfplay",
                template_args=[
                    "--dry-run",
                    "--workers", "pwa-mbp:4,pwa-mbp0:5,pwa-5090:6",
                    "--engine", "~/assets/engines/enyo_9696dac",
                    "--games", "64",
                    "--uci", "nnue_file=~/code/cpp/chess/enyo/net/default.nn",
                ],
            ))
        manifest = json.loads(stdout.getvalue())
        totals = [task["progress"]["total"] for task in manifest["tasks"]]

        self.assertEqual(0, rc)
        self.assertEqual(15, manifest["template"]["parameters"]["shards"])
        self.assertEqual(15, len(manifest["tasks"]))
        self.assertEqual(64, sum(totals))
        self.assertEqual(4, min(totals))
        self.assertEqual(6, max(totals))

    def test_selfplay_command_auto_sizes_shards_from_chunk_history(self) -> None:
        from forge_lib import selfplay_chunks

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_path = root / "selfplay-chunk-history.json"
            template = forge.load_template(forge.template_path("selfplay"))
            parsed_params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                [
                    "--engine", "~/assets/engines/enyo_9696dac",
                    "--games", "600",
                    "--chunk-time-seconds", "120",
                    "--uci", "nnue_file=~/code/cpp/chess/enyo/net/default.nn",
                ],
            )
            signature = selfplay_chunks.selfplay_chunk_signature({
                **parsed_params,
                "__forge_template_name": "selfplay",
                "__forge_worker": "worker-a",
            })
            key = selfplay_chunks.selfplay_chunk_signature_key(signature)
            forge.write_json(cache_path, {
                "schema": selfplay_chunks.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": {
                    key: {
                        "label": selfplay_chunks.selfplay_chunk_signature_label(signature),
                        "rate": 0.05,
                        "samples": 3,
                        "updated_epoch": 1.0,
                    }
                },
            })

            stdout = io.StringIO()
            with mock.patch("forge_lib.chunking.selfplay_chunk_cache_path", return_value=cache_path):
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_template_run(argparse.Namespace(
                        template="selfplay",
                        template_args=[
                            "--dry-run",
                            "--workers", "worker-a:1",
                            "--engine", "~/assets/engines/enyo_9696dac",
                            "--games", "600",
                            "--chunk-time-seconds", "120",
                            "--uci", "nnue_file=~/code/cpp/chess/enyo/net/default.nn",
                        ],
                    ))
            manifest = json.loads(stdout.getvalue())
            totals = [task["progress"]["total"] for task in manifest["tasks"]]

        self.assertEqual(0, rc)
        self.assertEqual(100, manifest["template"]["parameters"]["shards"])
        self.assertEqual(100, len(manifest["tasks"]))
        self.assertEqual(6, min(totals))
        self.assertEqual(6, max(totals))

    def test_selfplay_default_chunk_games_is_15_for_5_minutes(self) -> None:
        from forge_lib import selfplay_chunks

        self.assertEqual(15, selfplay_chunks.selfplay_default_chunk_games({"chunk_time_seconds": 300}))

    def test_adaptive_chunk_history_sizes_every_split_template(self) -> None:
        from forge_lib import chunking

        cases = {
            "selfplay": ("games", 600, 6),
            "sprt": ("games", 1000, 10),
            "spsa": ("games", 1200, 12),
            "label": ("max_records", 900_000, 9000),
        }
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "chunk-history.json"
            records = {}
            for name, (_total_key, _total, target_units) in cases.items():
                signature = chunking.selfplay_chunk_signature({
                    "__forge_template_name": name,
                    "__forge_worker": "worker-a",
                })
                records[chunking.selfplay_chunk_signature_key(signature)] = {
                    "label": name,
                    "rate": target_units / 300.0,
                    "samples": 3,
                    "updated_epoch": 1.0,
                }
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": records,
            })

            for name, (total_key, total, target_units) in cases.items():
                with self.subTest(template=name):
                    template = forge.load_template(forge.template_path(name))
                    shards = chunking.adaptive_autosized_shards(
                        name,
                        template,
                        {total_key: total, "chunk_time_seconds": 300},
                        {"workers": [{"host": "worker-a", "slots": 1}]},
                        cold_units=1,
                        cache_path=cache_path,
                    )
                    self.assertEqual((total + target_units - 1) // target_units, shards)

    def test_adaptive_initial_size_ignores_aggregate_history_for_uncalibrated_worker(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "chunk-history.json"
            signature = chunking.selfplay_chunk_signature({
                "__forge_template_name": "selfplay",
            })
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": {
                    chunking.selfplay_chunk_signature_key(signature): {
                        "label": "selfplay",
                        "rate": 10.0,
                        "samples": 20,
                        "updated_epoch": 1.0,
                    },
                },
            })
            template = forge.load_template(forge.template_path("selfplay"))

            shards = chunking.adaptive_autosized_shards(
                "selfplay",
                template,
                {"games": 600, "chunk_time_seconds": 300},
                {"workers": [{"host": "new-worker"}]},
                cold_units=15,
                cache_path=cache_path,
            )

        self.assertEqual(40, shards)

    def test_adaptive_chunk_units_support_spsa_split_games(self) -> None:
        from forge_lib import chunking

        task = {
            "progress": {
                "unit": "iterations",
                "total": 20,
                "games": 125,
            },
        }

        self.assertEqual(125, chunking.adaptive_task_units(task, "games"))

    def test_adaptive_initial_size_uses_slowest_known_worker_for_template(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "chunk-history.json"
            records = {}
            for worker, rate in (("fast", 1.0), ("slow", 0.1)):
                signature = chunking.selfplay_chunk_signature({
                    "__forge_template_name": "selfplay",
                    "__forge_worker": worker,
                })
                records[chunking.selfplay_chunk_signature_key(signature)] = {
                    "label": "selfplay",
                    "rate": rate,
                    "samples": 2,
                    "updated_epoch": 1.0,
                }
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": records,
            })
            template = forge.load_template(forge.template_path("selfplay"))

            shards = chunking.adaptive_autosized_shards(
                "selfplay",
                template,
                {"games": 600, "chunk_time_seconds": 300},
                {"workers": [{"host": "fast"}, {"host": "slow"}]},
                cold_units=15,
                cache_path=cache_path,
            )

        self.assertEqual(20, shards)

    def test_adaptive_completion_records_template_and_worker_rates(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            state.mkdir()
            task = {
                "id": "sprt_0000",
                "progress": {"unit": "games", "total": 20},
            }
            manifest = {
                "kind": "sprt",
                "state_dir": str(state),
                "template": {
                    "name": "sprt",
                    "path": str(forge.template_path("sprt")),
                    "parameters": {"games": 20, "chunk_time_seconds": 300},
                },
                "tasks": [task],
            }
            forge.write_json(state / "sprt_0000.done.json", {
                "rc": 0,
                "elapsed_s": 100.0,
                "worker": "slow-worker",
            })
            cache_path = root / "chunk-history.json"

            chunking.record_selfplay_chunk_task_result(
                manifest,
                task,
                cache_path=cache_path,
            )
            history = chunking.load_selfplay_chunk_history(cache_path=cache_path)
            aggregate_key = chunking.selfplay_chunk_signature_key(
                chunking.selfplay_chunk_signature({"__forge_template_name": "sprt"})
            )
            worker_key = chunking.selfplay_chunk_signature_key(
                chunking.selfplay_chunk_signature({
                    "__forge_template_name": "sprt",
                    "__forge_worker": "slow-worker",
                })
            )

            self.assertAlmostEqual(0.2, history["records"][aggregate_key]["rate"])
            self.assertAlmostEqual(0.2, history["records"][worker_key]["rate"])

    def test_adaptive_completion_extracts_worker_name_from_result_metadata(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_path = root / "chunk-history.json"
            task = {
                "id": "selfplay_0000",
                "progress": {"unit": "games", "total": 10},
            }
            manifest = {
                "kind": "selfplay",
                "template": {"name": "selfplay", "parameters": {}},
                "state_dir": str(root / "state"),
                "tasks": [task],
            }
            Path(manifest["state_dir"]).mkdir()
            forge.write_json(forge.task_done_path(manifest, task), {
                "rc": 0,
                "elapsed_s": 100,
                "host": "actual-hostname",
                "worker": {"name": "pwa-wrk", "host": "actual-hostname"},
            })

            chunking.record_selfplay_chunk_task_result(
                manifest,
                task,
                cache_path=cache_path,
            )

            self.assertAlmostEqual(
                0.1,
                chunking.selfplay_cached_throughput(
                    {"__forge_template_name": "selfplay", "__forge_worker": "pwa-wrk"},
                    cache_path=cache_path,
                ),
            )

    def test_selfplay_chunk_history_records_throughput_from_completed_tasks(self) -> None:
        from forge_lib import selfplay_chunks

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            state.mkdir()
            cache_path = root / "selfplay-chunk-history.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay-test",
                "kind": "selfplay",
                "state_dir": str(state),
                "template": {
                    "parameters": {
                        "engine": "~/assets/engines/enyo_9696dac",
                        "games": 40,
                        "threads": 1,
                        "hash": 128,
                        "concurrency": 1,
                        "nodes": 5000,
                        "restart": "off",
                        "tc": "10+0.1",
                    }
                },
                "tasks": [
                    {
                        "id": "selfplay_0000",
                        "index": 0,
                        "progress": {"unit": "games", "total": 20},
                    },
                    {
                        "id": "selfplay_0001",
                        "index": 1,
                        "progress": {"unit": "games", "total": 20},
                    },
                ],
            }
            forge.write_json(state / "selfplay_0000.done.json", {
                "schema": "forge.task.v1",
                "rc": 0,
                "elapsed_s": 100.0,
            })
            forge.write_json(state / "selfplay_0001.done.json", {
                "schema": "forge.task.v1",
                "rc": 0,
                "elapsed_s": 200.0,
            })

            record = selfplay_chunks.record_selfplay_chunk_history(manifest, cache_path=cache_path)
            payload = forge.read_json(cache_path)

        self.assertIsNotNone(record)
        self.assertEqual("forge.selfplay.chunk-history.v1", payload["schema"])
        signature = selfplay_chunks.selfplay_chunk_signature(manifest["template"]["parameters"])
        key = selfplay_chunks.selfplay_chunk_signature_key(signature)
        stored = payload["records"][key]
        self.assertEqual(1, stored["samples"])
        self.assertAlmostEqual(40 / 300.0, stored["rate"], places=6)

    def test_selfplay_chunk_history_updates_after_each_completed_chunk(self) -> None:
        from forge_lib import selfplay_chunks

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            state.mkdir()
            cache_path = root / "selfplay-chunk-history.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay-test",
                "kind": "selfplay",
                "state_dir": str(state),
                "template": {
                    "parameters": {
                        "engine": "~/assets/engines/enyo_9696dac",
                        "games": 40,
                        "threads": 1,
                        "hash": 128,
                        "concurrency": 1,
                        "nodes": 5000,
                        "restart": "off",
                        "tc": "10+0.1",
                    }
                },
                "tasks": [
                    {
                        "id": "selfplay_0000",
                        "index": 0,
                        "progress": {"unit": "games", "total": 20},
                    },
                    {
                        "id": "selfplay_0001",
                        "index": 1,
                        "progress": {"unit": "games", "total": 20},
                    },
                ],
            }
            forge.write_json(state / "selfplay_0000.done.json", {
                "schema": "forge.task.v1",
                "rc": 0,
                "elapsed_s": 100.0,
            })
            forge.write_json(state / "selfplay_0001.done.json", {
                "schema": "forge.task.v1",
                "rc": 0,
                "elapsed_s": 200.0,
            })

            first = selfplay_chunks.record_selfplay_chunk_task_result(
                manifest,
                manifest["tasks"][0],
                cache_path=cache_path,
            )
            second = selfplay_chunks.record_selfplay_chunk_task_result(
                manifest,
                manifest["tasks"][1],
                cache_path=cache_path,
            )
            payload = forge.read_json(cache_path)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        signature = selfplay_chunks.selfplay_chunk_signature(manifest["template"]["parameters"])
        key = selfplay_chunks.selfplay_chunk_signature_key(signature)
        stored = payload["records"][key]
        self.assertEqual(2, stored["samples"])
        self.assertAlmostEqual(0.15, stored["rate"], places=6)

    def test_selfplay_claim_rechunks_pending_tasks_from_throughput(self) -> None:
        from forge_lib import selfplay_chunks

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            claims = root / "claims"
            state.mkdir()
            claims.mkdir()
            template_path = forge.template_path("selfplay")
            template = forge.load_template(template_path)
            params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                [
                    "--engine", "~/assets/engines/enyo_9696dac",
                    "--games", "52",
                    "--shards", "2",
                ],
            )
            manifest = forge.expand_template("selfplay", template_path, template, params)
            manifest["state_dir"] = str(state)
            manifest["claim_dir"] = str(claims)
            manifest_path = root / "manifest.json"
            forge.write_json(manifest_path, manifest)
            cache_path = root / "selfplay-chunk-history.json"
            signature = selfplay_chunks.selfplay_chunk_signature(params)
            key = selfplay_chunks.selfplay_chunk_signature_key(signature)
            forge.write_json(cache_path, {
                "schema": selfplay_chunks.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": {
                    key: {
                        "label": selfplay_chunks.selfplay_chunk_signature_label(signature),
                        "rate": 16 / 300.0,
                        "samples": 3,
                        "updated_epoch": 1.0,
                    }
                },
            })

            with mock.patch("forge_lib.chunking.selfplay_chunk_cache_path", return_value=cache_path):
                claimed = forge.claim_task(
                    manifest,
                    manifest_path=manifest_path,
                    worker="worker-a",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )

            updated = forge.load_manifest(manifest_path)
            totals = [task["progress"]["total"] for task in updated["tasks"]]

        self.assertIsNotNone(claimed)
        self.assertEqual(3, len(updated["tasks"]))
        self.assertLessEqual(next(
            task["progress"]["total"]
            for task in updated["tasks"]
            if task["id"] == claimed["task"]
        ), 16)
        self.assertGreater(max(totals), min(totals))

    def test_sprt_claim_uses_same_live_rechunker(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            claims = root / "claims"
            state.mkdir()
            claims.mkdir()
            template_path = forge.template_path("sprt")
            template = forge.load_template(template_path)
            params = forge.parse_sprt_template_parameters(
                "sprt",
                template,
                ["--candidate", "~/assets/engines/candidate", "--games", "100", "--shards", "2"],
            )
            manifest = forge.expand_template("sprt", template_path, template, params)
            manifest["state_dir"] = str(state)
            manifest["claim_dir"] = str(claims)
            manifest_path = root / "manifest.json"
            forge.write_json(manifest_path, manifest)
            cache_path = root / "chunk-history.json"
            signature = chunking.selfplay_chunk_signature({
                "__forge_template_name": "sprt",
                "__forge_worker": "worker-a",
            })
            key = chunking.selfplay_chunk_signature_key(signature)
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": {
                    key: {
                        "label": "sprt",
                        "rate": 10 / 300.0,
                        "samples": 3,
                        "updated_epoch": 1.0,
                    },
                },
            })

            with mock.patch("forge_lib.chunking.selfplay_chunk_cache_path", return_value=cache_path):
                claimed = forge.claim_task(
                    manifest,
                    manifest_path=manifest_path,
                    worker="worker-a",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )

            updated = forge.load_manifest(manifest_path)
            totals = [task["progress"]["total"] for task in updated["tasks"]]

        self.assertIsNotNone(claimed)
        self.assertEqual(6, len(updated["tasks"]))
        self.assertEqual(10, next(
            task["progress"]["total"]
            for task in updated["tasks"]
            if task["id"] == claimed["task"]
        ))
        self.assertEqual(50, max(totals))

    def test_live_rechunk_assigns_each_worker_its_calibrated_chunk_size(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "state").mkdir()
            (root / "claims").mkdir()
            template_path = forge.template_path("selfplay")
            template = forge.load_template(template_path)
            params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                ["--engine", "~/assets/engines/enyo_9696dac", "--games", "640", "--shards", "40"],
            )
            manifest = forge.expand_template("selfplay", template_path, template, params)
            manifest["state_dir"] = str(root / "state")
            manifest["claim_dir"] = str(root / "claims")
            manifest_path = root / "manifest.json"
            forge.write_json(manifest_path, manifest)
            cache_path = root / "chunk-history.json"
            records = {}
            for worker, target in (("slow", 24), ("fast", 64)):
                signature = chunking.selfplay_chunk_signature({
                    "__forge_template_name": "selfplay",
                    "__forge_worker": worker,
                })
                records[chunking.selfplay_chunk_signature_key(signature)] = {
                    "label": "selfplay",
                    "rate": target / 300,
                    "samples": 3,
                    "updated_epoch": 1.0,
                }
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": records,
            })

            with mock.patch("forge_lib.chunking.selfplay_chunk_cache_path", return_value=cache_path):
                slow_claim = forge.claim_task(
                    manifest,
                    manifest_path=manifest_path,
                    worker="slow",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )
                fast_claim = forge.claim_task(
                    forge.load_manifest(manifest_path),
                    manifest_path=manifest_path,
                    worker="fast",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )

            updated = forge.load_manifest(manifest_path)
            tasks = {task["id"]: task for task in updated["tasks"]}

        self.assertEqual(24, tasks[slow_claim["task"]]["progress"]["total"])
        self.assertEqual(64, tasks[fast_claim["task"]]["progress"]["total"])

    def test_live_rechunk_uses_current_running_throughput_before_chunk_finishes(self) -> None:
        from forge_lib import chunking

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "state").mkdir()
            (root / "claims").mkdir()
            template_path = forge.template_path("selfplay")
            template = forge.load_template(template_path)
            params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                ["--engine", "~/assets/engines/enyo_9696dac", "--games", "64", "--shards", "4"],
            )
            manifest = forge.expand_template("selfplay", template_path, template, params)
            manifest["state_dir"] = str(root / "state")
            manifest["claim_dir"] = str(root / "claims")
            manifest_path = root / "manifest.json"
            running_task = manifest["tasks"][0]
            forge.write_json(forge.task_running_path(manifest, running_task), {
                "task": running_task["id"],
                "worker": "slow",
                "started_epoch": 700.0,
            })
            forge.task_claim_dir(manifest, running_task).mkdir()
            forge.write_json(forge.task_heartbeat_path(manifest, running_task), {
                "task": running_task["id"],
                "worker": "slow",
                "progress_data": {"done": 2, "total": 16, "unit": "games"},
            })
            forge.write_json(manifest_path, manifest)
            cache_path = root / "chunk-history.json"
            signature = chunking.selfplay_chunk_signature({
                "__forge_template_name": "selfplay",
                "__forge_worker": "slow",
            })
            forge.write_json(cache_path, {
                "schema": chunking.SELFPLAY_CHUNK_CACHE_SCHEMA,
                "records": {
                    chunking.selfplay_chunk_signature_key(signature): {
                        "label": "selfplay",
                        "rate": 0.1,
                        "samples": 3,
                        "updated_epoch": 1.0,
                    },
                },
            })

            with (
                mock.patch("forge_lib.chunking.selfplay_chunk_cache_path", return_value=cache_path),
                mock.patch("forge_lib.chunking.time.time", return_value=1000.0),
            ):
                claimed = forge.claim_task(
                    manifest,
                    manifest_path=manifest_path,
                    worker="slow",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )

            updated = forge.load_manifest(manifest_path)
            claimed_task = next(task for task in updated["tasks"] if task["id"] == claimed["task"])

        self.assertEqual(2, claimed_task["progress"]["total"])

    def test_selfplay_status_graph_aggregates_completed_fastchess_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            state.mkdir()
            logs.mkdir()
            progress = {
                "unit": "games",
                "total": 4,
                "log_regex": r"Score of .*?:\s+(?P<wins>\d+)\s+-\s+(?P<losses>\d+)\s+-\s+(?P<draws>\d+)\s+\[[^\]]+\]\s+(?P<done>\d+)\s+Rate:\s+(?P<rate>[0-9.]+)",
                "series": [
                    {"name": "wins", "field": "wins", "x": "done", "scale": "linear"},
                    {"name": "losses", "field": "losses", "x": "done", "scale": "linear"},
                    {"name": "draws", "field": "draws", "x": "done", "scale": "linear"},
                    {"name": "rate", "field": "rate", "x": "done", "scale": "linear"},
                ],
            }
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay",
                "kind": "selfplay",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {"id": "selfplay_0000", "index": 0, "command": "true", "outputs": [], "progress": dict(progress)},
                    {"id": "selfplay_0001", "index": 1, "command": "true", "outputs": [], "progress": dict(progress)},
                ],
            }
            (state / "selfplay_0000.done.json").write_text(json.dumps({"task": "selfplay_0000"}), encoding="utf-8")
            (state / "selfplay_0001.done.json").write_text(json.dumps({"task": "selfplay_0001"}), encoding="utf-8")
            (logs / "selfplay_0000.log").write_text(
                "Score of selfplay: 1 - 1 - 2 [..] 4 Rate: 2.0\n",
                encoding="utf-8",
            )
            (logs / "selfplay_0001.log").write_text(
                "Score of selfplay: 2 - 1 - 1 [..] 4 Rate: 3.0\n",
                encoding="utf-8",
            )

            summary = forge.status_summary(manifest, lease_seconds=600)

            graph = summary["progress_graph"]
            self.assertEqual("games", graph["x_unit"])
            self.assertEqual(8, graph["x_total"])
            series = {item["name"]: item["points"] for item in graph["series"]}
            self.assertEqual([
                {"x": 8.0, "y": 5.0},
            ], series["games/s"])
            self.assertEqual({
                "wins": 3,
                "draws": 3,
                "losses": 2,
                "draw_percent": 37.5,
            }, graph["selfplay"])

    def test_selfplay_history_rate_uses_completed_games_over_wall_clock(self) -> None:
        fallback = {
            "version": forge.PROGRESS_GRAPH_VERSION,
            "x_unit": "games",
            "x_total": 200000,
            "selfplay": {"wins": 1, "draws": 1, "losses": 1, "draw_percent": 100 / 3},
            "series": [{
                "name": "games/s",
                "points": [{"x": 1536.0, "y": 0.01}],
            }],
        }
        history = [
            {"epoch": 100.0, "x": 1136.0, "series": {"games/s": 0.44}},
            {"epoch": 131.0, "x": 1376.0, "series": {"games/s": 0.068}},
            {"epoch": 151.0, "x": 1536.0, "series": {"games/s": 0.01}},
        ]

        graph = forge.progress_graph_from_history(history, fallback)

        self.assertIsNotNone(graph)
        series = {item["name"]: item["points"] for item in graph["series"]}
        self.assertEqual([
            {"x": 1376.0, "y": 240 / 31},
            {"x": 1536.0, "y": 8.0},
        ], series["games/s"])

    def test_selfplay_history_ignores_projected_endpoints(self) -> None:
        current_graph = {
            "version": forge.PROGRESS_GRAPH_VERSION,
            "x_unit": "games",
            "x_total": 1000,
            "selfplay": {
                "wins": 29,
                "draws": 41,
                "losses": 30,
                "draw_percent": 41.0,
            },
            "series": [
                {
                    "name": "balance",
                    "points": [
                        {"x": 0.0, "y": 0.0},
                        {"x": 350.0, "y": -0.01},
                    ],
                }
            ],
        }
        history = [
            {
                "epoch": 1.0,
                "x": 100.0,
                "x_unit": "games",
                "x_total": 1000,
                "series": {"balance": -0.02},
            },
            {
                "epoch": 2.0,
                "x": 350.0,
                "x_unit": "games",
                "x_total": 1000,
                "series": {"balance": -0.01},
            },
            {
                "epoch": 3.0,
                "x": 1000.0,
                "x_unit": "games",
                "x_total": 1000,
                "series": {"balance": -0.27},
            },
        ]

        graph = forge.progress_graph_from_history(history, current_graph)

        self.assertIsNotNone(graph)
        balance = next(item for item in graph["series"] if item["name"] == "balance")
        self.assertEqual([
            {"x": 0.0, "y": 0.0},
            {"x": 100.0, "y": -0.02},
            {"x": 350.0, "y": -0.01},
        ], balance["points"])
        self.assertNotIn({"x": 1000.0, "y": -0.27}, balance["points"])

    def test_selfplay_template_rounds_odd_games_up(self) -> None:
        template_path = ROOT / "templates" / "selfplay.template.json"
        template = forge.load_template(template_path)

        params = forge.parse_selfplay_template_parameters(
            "selfplay",
            template,
            ["--engine", "~/assets/engines/enyo_9696dac", "--games", "15"],
        )

        self.assertEqual(16, params["games"])

    def test_selfplay_template_rejects_empty_uci_option_values(self) -> None:
        template_path = ROOT / "templates" / "selfplay.template.json"
        template = forge.load_template(template_path)

        with self.assertRaisesRegex(SystemExit, "--uci nnue_file="):
            forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                [
                    "--engine", "~/assets/engines/enyo_9696dac",
                    "--games", "16",
                    "--uci", "nnue_file=",
                ],
            )

    def test_selfplay_template_syncs_book_and_net_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                name: root / name
                for name in ("engine", "book.epd", "default.nn")
            }
            for name, path in paths.items():
                path.write_bytes(name.encode())
            template_path = ROOT / "templates" / "selfplay.template.json"
            template = forge.load_template(template_path)
            params = forge.parse_selfplay_template_parameters(
                "selfplay",
                template,
                [
                    "--engine", str(paths["engine"]),
                    "--book", str(paths["book.epd"]),
                    "--games", "16",
                    "--shards", "2",
                    "--uci", f"nnue_file={paths['default.nn']}",
                ],
            )
            manifest = forge.expand_template("selfplay", template_path, template, params)

            forge.materialize_template_inputs(template, manifest, params)

            inputs = manifest["requirements"]["inputs"]
            self.assertEqual(2, len(inputs))
            self.assertEqual({"book", "uci.nnue_file"}, {item["name"] for item in inputs})
            self.assertEqual(
                {paths["book.epd"], paths["default.nn"]},
                {Path(item["path"]) for item in inputs},
            )
            self.assertTrue(all(item["bytes"] > 0 and len(item["sha256"]) == 64 for item in inputs))

            commands = {item["name"]: item for item in manifest["requirements"]["commands"]}
            self.assertIn("./scripts/deploy.sh --candidate", commands["engine"]["command"])
            self.assertEqual(900, commands["engine"]["timeout_s"])

    def test_enyo_engine_fallback_uses_isolated_worktree(self) -> None:
        for template_name in ("sprt", "selfplay", "spsa"):
            with self.subTest(template=template_name):
                template = forge.load_template(ROOT / "templates" / f"{template_name}.template.json")
                commands = [
                    item["command"]
                    for item in template["requirements"]["commands"]
                    if "./scripts/deploy.sh --candidate" in str(item.get("command") or "")
                ]
                self.assertTrue(commands)
                for command in commands:
                    self.assertIn("$HOME/.cache/forge/enyo-worktrees/$commit-$$", command)
                    self.assertIn('git -C "$repo" worktree add --detach', command)
                    self.assertIn('git -C "$repo" worktree remove --force', command)
                    self.assertNotIn("checkout -f", command)
                    self.assertNotIn("cd ~/code/cpp/chess/enyo", command)
                    syntax = subprocess.run(
                        ["sh", "-n", "-c", command],
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    self.assertEqual(0, syntax.returncode, syntax.stderr)

    def test_enyo_engine_fallback_does_not_switch_shared_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            origin = home / "origin.git"
            seed = home / "seed"
            repo = home / "code" / "cpp" / "chess" / "enyo"
            subprocess.run(["git", "init", "--bare", str(origin)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "init", "-b", "main", str(seed)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "-C", str(seed), "config", "user.email", "forge@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(seed), "config", "user.name", "Forge Test"], check=True)
            deploy = seed / "scripts" / "deploy.sh"
            deploy.parent.mkdir()
            deploy.write_text(
                "#!/bin/sh\nset -eu\n"
                "hash=$(git rev-parse --short HEAD)\n"
                "mkdir -p \"$HOME/assets/engines\"\n"
                "printf '#!/bin/sh\\necho uciok\\n' >\"$HOME/assets/engines/enyo_$hash\"\n"
                "chmod +x \"$HOME/assets/engines/enyo_$hash\"\n",
                encoding="utf-8",
            )
            deploy.chmod(0o755)
            subprocess.run(["git", "-C", str(seed), "add", "."], check=True)
            subprocess.run(["git", "-C", str(seed), "commit", "-m", "base"], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "-C", str(seed), "remote", "add", "origin", str(origin)], check=True)
            subprocess.run(["git", "-C", str(seed), "push", "-u", "origin", "main"], check=True, stdout=subprocess.DEVNULL)
            (seed / "revision.txt").write_text("target\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(seed), "add", "revision.txt"], check=True)
            subprocess.run(["git", "-C", str(seed), "commit", "-m", "target"], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "-C", str(seed), "push"], check=True, stdout=subprocess.DEVNULL)
            repo.parent.mkdir(parents=True)
            subprocess.run(["git", "clone", str(origin), str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "-C", str(repo), "switch", "-c", "workspace", "HEAD~1"], check=True, stdout=subprocess.DEVNULL)
            (repo / "keep.me").write_text("untracked\n", encoding="utf-8")
            before = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            target = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "origin/main"], text=True).strip()[:7]
            template = forge.load_template(ROOT / "templates" / "selfplay.template.json")
            raw = next(
                item["command"]
                for item in template["requirements"]["commands"]
                if item["name"] == "engine"
            )
            command = raw.format(engine=f"~/assets/engines/enyo_{target}")

            result = subprocess.run(
                ["sh", "-c", command],
                env={**os.environ, "HOME": str(home)},
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("workspace", subprocess.check_output(
                ["git", "-C", str(repo), "branch", "--show-current"], text=True
            ).strip())
            self.assertEqual(before, subprocess.check_output(
                ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
            ).strip())
            self.assertTrue((repo / "keep.me").is_file())
            self.assertEqual(1, subprocess.check_output(
                ["git", "-C", str(repo), "worktree", "list", "--porcelain"], text=True
            ).count("worktree "))

    def test_subcommand_help_is_available(self) -> None:
        commands = [
            "run", "sprt", "selfplay", "sync", "sh", "template", "plan", "add-input", "add-repo", "add-command", "sync-repos", "start", "run-task", "claim",
            "release", "heartbeat", "work", "doctor", "status",
            "wait", "verify", "merge", "stop", "drain", "del", "self-update",
            "update", "version", "deploy", "resume", "retry", "worker",
        ]
        for command in commands:
            with self.subTest(command=command):
                result = self.run_cli(command, "--help")
                self.assertEqual("", result.stderr)
                self.assertEqual(0, result.returncode)
                self.assertIn("usage:", result.stdout)
        self.assertIn("--progress-log-regex", self.run_cli("plan", "--help").stdout)
        self.assertIn("Run a shell command once", self.run_cli("sh", "--help").stdout)
        self.assertIn("Coordinator manifest", self.run_cli("claim", "--help").stdout)
        self.assertIn("comma-separated workers", self.run_cli("deploy", "--help").stdout)
        self.assertIn("--merge", self.run_cli("start", "--help").stdout)
        self.assertIn("--merge", self.run_cli("resume", "--help").stdout)
        self.assertIn("--worker", self.run_cli("del", "--help").stdout)
        self.assertIn("worker-a:5", self.run_cli("worker", "set", "--help").stdout)
        self.assertIn("--keep-claims", self.run_cli("worker", "del", "--help").stdout)
        self.assertIn("--index", self.run_cli("run-task", "--help").stdout)
        self.assertIn("--coordinator", self.run_cli("doctor", "--help").stdout)
        self.assertIn("comma-separated workers", self.run_cli("update", "--help").stdout)
        self.assertNotEqual(0, self.run_cli("worker", "run-job", "--help").returncode)

    def test_unknown_command_is_an_implicit_shell_command(self) -> None:
        parser = forge.build_parser()
        with mock.patch(
            "forge_lib.cli.template_files",
            return_value=[Path("/tmp/custom.template.json")],
        ):
            shell = parser.parse_args(["pwd"])
            quoted_shell = parser.parse_args(["sprt foo bar"])
            copied_path = str(Path.home() / "code/cpp/chess/enyo/net/berserk-9b84c340af7e.nn")
            copy_shell = parser.parse_args(["cp", copied_path])
            explicit_copy_shell = parser.parse_args(["sh", "cp", copied_path])
            builtin_template = parser.parse_args(["sprt", "--games", "10"])
            custom_template = parser.parse_args(["custom", "--value", "x"])

        self.assertEqual("sh", shell.subcommand)
        self.assertEqual("pwd", shell.command)
        self.assertEqual("sh", quoted_shell.subcommand)
        self.assertEqual("sprt foo bar", quoted_shell.command)
        self.assertEqual("cp ~/code/cpp/chess/enyo/net/berserk-9b84c340af7e.nn", copy_shell.command)
        self.assertEqual(copy_shell.command, explicit_copy_shell.command)
        self.assertEqual("run", builtin_template.subcommand)
        self.assertEqual("sprt", builtin_template.template)
        self.assertEqual(["--games", "10"], builtin_template.template_args)
        self.assertEqual("run", custom_template.subcommand)
        self.assertEqual("custom", custom_template.template)
        self.assertEqual(["--value", "x"], custom_template.template_args)

    def test_additive_worker_commands_are_removed(self) -> None:
        parser = forge.build_parser()
        for argv in (["add", "--worker", "worker-a:2"], ["worker", "add", "worker-a:2"]):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(argv)

    def test_sh_caps_default_workers_to_one_slot_per_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            forge.write_json(runs / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "slots": 4, "cache_dir": "/tmp/a"},
                    {"host": "worker-b", "slots": 2, "python": "/usr/bin/python3"},
                    {"host": "worker-a", "slots": 1, "cache_dir": "/tmp/duplicate"},
                ],
            })
            seen: list[tuple[dict[str, Any], str]] = []

            def fake_run_worker_shell(
                worker: dict[str, Any],
                command: str,
                **_kwargs: Any,
            ) -> subprocess.CompletedProcess[str]:
                seen.append((worker, command))
                return subprocess.CompletedProcess(["sh"], 0, stdout=f"{worker['host']} output\n", stderr="")

            with (
                mock.patch("forge_lib.workers.config_workers_path", return_value=runs / "workers.json"),
                mock.patch("forge_lib.shell.run_worker_shell", side_effect=fake_run_worker_shell),
            ):
                rc = forge.cmd_sh(argparse.Namespace(
                    command=f"rm {Path.home()}/code/cpp/chess/enyo/net/berserk-9b84c340af7e.nn",
                    workers=None,
                    jobs=4,
                    run="sh-test",
                    coordinator_host=None,
                    remote_timeout_seconds=1800.0,
                    lease_seconds=600,
                    interval_seconds=2.0,
                    timeout_seconds=0.0,
                    notify_command=None,
                    no_color=True,
                    verbose=False,
                ))

            self.assertEqual(0, rc)
            seen_sorted = sorted(seen, key=lambda item: item[0]["host"])
            self.assertEqual([
                {"cache_dir": "/tmp/a", "host": "worker-a", "local": False, "profile": "worker-a"},
                {"host": "worker-b", "local": False, "profile": "worker-b", "python": "/usr/bin/python3"},
            ], [worker for worker, _command in seen_sorted])
            self.assertEqual(
                ["rm ~/code/cpp/chess/enyo/net/berserk-9b84c340af7e.nn"] * 2,
                [command for _worker, command in seen_sorted],
            )
            self.assertFalse((runs / ".sh-tmp").exists())
            self.assertFalse((runs / "sh-test").exists())

    def test_sync_copies_once_and_skips_matching_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "foo"
            destination = root / "worker" / "bar"
            source.write_text("content\n", encoding="utf-8")
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{"host": "localhost", "local": True, "slots": 8}],
            })
            args = argparse.Namespace(
                source=str(source),
                destination=str(destination),
                workers=str(workers),
                jobs=4,
                remote_timeout_seconds=1800.0,
            )

            with contextlib.redirect_stdout(io.StringIO()) as first_stdout:
                self.assertEqual(0, forge.cmd_sync(args))
            with contextlib.redirect_stdout(io.StringIO()) as second_stdout:
                self.assertEqual(0, forge.cmd_sync(args))

            self.assertEqual("content\n", destination.read_text(encoding="utf-8"))
            self.assertIn(f"syncing foo from ", first_stdout.getvalue())
            self.assertIn(f" to {destination}\n", first_stdout.getvalue())
            self.assertIn("localhost: synced", first_stdout.getvalue())
            self.assertIn("localhost: current", second_stdout.getvalue())
            self.assertIsNone(forge.build_parser().parse_args(["sync", str(source)]).destination)
            args.destination = None
            with contextlib.redirect_stdout(io.StringIO()) as default_stdout:
                self.assertEqual(0, forge.cmd_sync(args))
            self.assertIn(f" to {source}\n", default_stdout.getvalue())
            self.assertIn("localhost: current", default_stdout.getvalue())

    def test_sync_directory_preserves_layout_and_skips_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "agents"
            (source / "nested").mkdir(parents=True)
            (source / "one.toml").write_text("one\n", encoding="utf-8")
            (source / "nested" / "two.toml").write_text("two\n", encoding="utf-8")
            destination = root / "worker"
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{"host": "localhost", "local": True, "slots": 8}],
            })
            args = argparse.Namespace(
                source=str(source),
                destination=str(destination) + "/",
                workers=str(workers),
                jobs=4,
                remote_timeout_seconds=1800.0,
            )

            with contextlib.redirect_stdout(io.StringIO()) as first_stdout:
                self.assertEqual(0, forge.cmd_sync(args))
            with contextlib.redirect_stdout(io.StringIO()) as second_stdout:
                self.assertEqual(0, forge.cmd_sync(args))

            copied = destination / "agents"
            self.assertEqual("one\n", (copied / "one.toml").read_text(encoding="utf-8"))
            self.assertEqual("two\n", (copied / "nested" / "two.toml").read_text(encoding="utf-8"))
            self.assertIn(f"syncing agents from ", first_stdout.getvalue())
            self.assertIn(f" to {copied}\n", first_stdout.getvalue())
            self.assertIn("localhost: synced", first_stdout.getvalue())
            self.assertIn("localhost: current", second_stdout.getvalue())

    def test_sh_prints_grouped_output_and_preserves_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            seen: list[str] = []

            def fake_run_worker_shell(
                _worker: dict[str, Any],
                command: str,
                **_kwargs: Any,
            ) -> subprocess.CompletedProcess[str]:
                seen.append(command)
                return subprocess.CompletedProcess(["sh"], 0, stdout="{ok}\n", stderr="")

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.shell.run_worker_shell", side_effect=fake_run_worker_shell),
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                rc = forge.cmd_sh(argparse.Namespace(
                    command="printf '{ok}\\n'",
                    workers="localhost:4",
                    jobs=4,
                    run="sh-braces",
                    coordinator_host=None,
                    remote_timeout_seconds=1800.0,
                    lease_seconds=600,
                    interval_seconds=2.0,
                    timeout_seconds=0.0,
                    notify_command=None,
                    no_color=True,
                    verbose=False,
                ))

            self.assertEqual(0, rc)
            self.assertEqual(["printf '{ok}\\n'"], seen)
            output = stdout.getvalue()
            self.assertIn("localhost:\n", output)
            self.assertNotIn("localhost: OK", output)
            self.assertIn("  {ok}", output)

    def test_sh_reports_failed_hosts(self) -> None:
        def fake_run_worker_shell(
            worker: dict[str, Any],
            _command: str,
            **_kwargs: Any,
        ) -> subprocess.CompletedProcess[str]:
            if worker["host"] == "worker-b":
                return subprocess.CompletedProcess(["ssh"], 7, stdout="", stderr="boom\n")
            return subprocess.CompletedProcess(["ssh"], 0, stdout="fine\n", stderr="")

        with (
            mock.patch("forge_lib.shell.run_worker_shell", side_effect=fake_run_worker_shell),
            contextlib.redirect_stdout(io.StringIO()) as stdout,
            contextlib.redirect_stderr(io.StringIO()) as stderr,
        ):
            rc = forge.cmd_sh(argparse.Namespace(
                command="do thing",
                workers="worker-a,worker-b",
                jobs=4,
                run="sh-fail",
                coordinator_host=None,
                remote_timeout_seconds=1800.0,
                lease_seconds=600,
                interval_seconds=2.0,
                timeout_seconds=0.0,
                notify_command=None,
                no_color=True,
                verbose=False,
            ))

        self.assertEqual(1, rc)
        output = stdout.getvalue()
        self.assertIn("worker-a:\n", output)
        self.assertNotIn("worker-a: OK", output)
        self.assertIn("worker-b: FAILED rc=7", output)
        self.assertIn("  boom", output)
        self.assertEqual("sh: failed on 1/2 host(s):\n  worker-b:\n      boom\n", stderr.getvalue())

    def test_shell_stream_text_uses_crlf_for_tty_output(self) -> None:
        class TtyStream(io.StringIO):
            def isatty(self) -> bool:
                return True

        self.assertEqual(
            "a\r\nb\r\n",
            forge.shell_stream_text(TtyStream(), "a\nb\n"),
        )
        self.assertEqual(
            "a\r\nb\r\n",
            forge.shell_stream_text(TtyStream(), "a\r\nb\r"),
        )
        self.assertEqual(
            "a\nb\n",
            forge.shell_stream_text(io.StringIO(), "a\nb\n"),
        )

    def test_terminal_color_enabled_detects_watch_parent(self) -> None:
        calls = iter([
            "111 sh\n",
            "1 watch\n",
        ])
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch("forge_lib.core.sys.stdout.isatty", return_value=False),
            mock.patch("forge_lib.core.os.getppid", return_value=222),
            mock.patch("forge_lib.core.subprocess.check_output", side_effect=lambda *args, **kwargs: next(calls)),
        ):
            self.assertTrue(forge.terminal_color_enabled())

    def test_terminal_color_no_color_overrides_watch_parent(self) -> None:
        with (
            mock.patch("forge_lib.core.sys.stdout.isatty", return_value=False),
            mock.patch("forge_lib.core.running_under_watch", return_value=True),
        ):
            self.assertFalse(forge.terminal_color_enabled(no_color=True))

    def test_list_subcommand_is_not_supported(self) -> None:
        result = self.run_cli("list", "--help")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("invalid choice", result.stderr)

    @unittest.skipIf(not hasattr(os, "symlink"), "symlink support required")
    def test_expand_path_preserves_symlink_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            real = root / "real"
            link = root / "link"
            real.mkdir()
            link.symlink_to(real, target_is_directory=True)

            path = forge.expand_path(link / "manifest.json")

            self.assertEqual(link / "manifest.json", path)

    def test_plan_run_status_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            run_dir = root / "runs" / "test"
            manifest = run_dir / "manifest.json"
            out_template = str(root / "out" / "shard.{index}.txt")
            result = self.run_cli(
                "plan",
                "--name", "test",
                "--kind", "sprt",
                "--description", "native check",
                "--comment", "quick smoke",
                "--task-label", "sprt-game",
                "--shards", "3",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf '{index}\\n' > {output}",
                "--output-template", out_template,
            )
            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual("sprt", manifest_payload["kind"])
            self.assertEqual("native check", manifest_payload["description"])
            self.assertEqual("quick smoke", manifest_payload["comment"])
            self.assertEqual("sprt-game", manifest_payload["tasks"][0]["label"])

            add = self.run_cli(
                "add-input",
                "--manifest", str(manifest),
                "--name", "source-input",
                "--path", str(root / "input.txt"),
            )
            self.assertEqual(0, add.returncode)
            manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual("source-input", manifest_payload["requirements"]["inputs"][0]["name"])

            for index in range(3):
                run = self.run_cli("run-task", "--manifest", str(manifest), "--index", str(index))
                self.assertEqual("", run.stderr)
                self.assertEqual(0, run.returncode)

            status = self.run_cli("status", str(run_dir))
            self.assertEqual(0, status.returncode)
            self.assertIn("tasks=3/3", status.stdout)
            self.assertIn("run=test\n  desc=native check", status.stdout)
            self.assertIn('  "quick smoke"', status.stdout)
            self.assertNotIn("kind=", status.stdout)
            self.assertTrue(any(line.startswith("  tasks=3/3") for line in status.stdout.splitlines()))

            merged = root / "merged.txt"
            merge = self.run_cli(
                "merge", str(run_dir),
                "--output", str(merged),
                "--newline-between",
            )
            self.assertEqual(0, merge.returncode)
            self.assertEqual("0\n\n1\n\n2\n\n", merged.read_text(encoding="utf-8"))

    def test_start_local_wait_verify_and_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            (repo / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "template.json"
            plan = self.run_cli(
                "plan",
                "--name", "run-pass",
                "--shards", "4",
                "--work-dir", str(repo),
                "--out", str(manifest),
                "--command-template", "mkdir -p \"$(dirname {output})\" && printf '{index}\\n' > {output}",
                "--output-template", str(repo / "out" / "task.{index}.txt"),
            )
            self.assertEqual("", plan.stderr)
            self.assertEqual(0, plan.returncode)

            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(root / "worker-cache"),
                    "python": sys.executable,
                }],
            })

            merged = root / "merged.txt"
            result = self.run_cli(
                "start", str(manifest),
                "--workers", str(workers),
                "--jobs", "1",
                "--wait",
                "--interval-seconds", "0.01",
                "--timeout-seconds", "5",
                "--merge", str(merged),
                "--newline-between",
                env={"FORGE_RUNS_DIR": str(root / "runs")},
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            self.assertIn("deploy: tasks complete", result.stdout)
            self.assertIn("tasks=4/4", result.stdout)
            self.assertIn("ok", result.stdout)
            self.assertIn("start: ok", result.stdout)
            self.assertEqual("0\n\n1\n\n2\n\n3\n\n", merged.read_text(encoding="utf-8"))

    def test_start_launches_and_returns_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-default",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            captured = {}

            def fake_deploy(args):
                captured["deploy"] = args
                return 0

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_deploy", side_effect=fake_deploy),
                mock.patch("forge_lib.workers.cmd_wait") as wait,
                mock.patch("forge_lib.workers.cmd_verify") as verify,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(argparse.Namespace(
                        manifest=str(manifest),
                        workers="worker-a",
                        jobs=1,
                        run=None,
                        resume=False,
                        replace=False,
                        retry_failed=False,
                        task_attempts=2,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        wait=False,
                        lease_seconds=600,
                        interval_seconds=30.0,
                        timeout_seconds=0.0,
                        fail_on_fail=True,
                        verify=None,
                        merge=None,
                        merge_force=False,
                        newline_between=False,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                        command_label="start",
                    ))

            self.assertEqual(0, rc)
            self.assertTrue(captured["deploy"].launch_only)
            wait.assert_not_called()
            verify.assert_not_called()
            self.assertIn("start: ok; status: forge status launch-default --verbose", stdout.getvalue())

    def test_selfplay_template_defaults_retry_failed_without_fail_fast(self) -> None:
        options = argparse.Namespace(
            resume=False,
            retry_failed=False,
            fail_on_fail=True,
        )

        result = forge.apply_template_run_defaults("selfplay", options)

        self.assertIs(result, options)
        self.assertTrue(options.retry_failed)
        self.assertFalse(options.fail_on_fail)

    def test_spsa_resume_defaults_retry_failed(self) -> None:
        options = argparse.Namespace(
            resume=True,
            retry_failed=False,
            fail_on_fail=True,
        )

        result = forge.apply_template_run_defaults("spsa", options)

        self.assertIs(result, options)
        self.assertTrue(options.retry_failed)
        self.assertTrue(options.fail_on_fail)

    def test_start_ctrl_c_cleans_and_resumes_without_duplicate_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            cache = root / "worker-cache"
            template = root / "template.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "ctrl-c-cleanup",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{
                        "id": "task",
                        "index": 0,
                        "command": (
                            "if test -f gate; then test ! -e {output} && printf complete > {output}; "
                            "else printf partial > {output}; sleep 30; fi"
                        ),
                        "outputs": ["{output_dir}/task.txt"],
                    }],
                }),
                encoding="utf-8",
            )
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(cache),
                    "python": sys.executable,
                }],
            })
            env = os.environ.copy()
            env.update({
                "FORGE_RUNS_DIR": str(runs),
                "FORGE_COORDINATOR_RUNS": "",
                "FORGE_NOTIFY_COMMAND": "",
            })
            command = [
                sys.executable,
                str(CLI),
                "start",
                str(template),
                "--workers",
                str(workers),
                "--wait",
                "--interval-seconds",
                "0.02",
                "--timeout-seconds",
                "10",
            ]
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            try:
                running = runs / "ctrl-c-cleanup" / "state" / "task.running.json"
                deadline = time.monotonic() + 10
                while not running.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(running.exists(), "task never reached running state")

                process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=15)
                self.assertEqual(130, process.returncode, stdout + stderr)
                self.assertIn("interrupt cleanup complete", stderr)

                run = runs / "ctrl-c-cleanup"
                manifest = forge.load_manifest(run / "manifest.json")
                task = manifest["tasks"][0]
                self.assertTrue(forge.stop_request_path(manifest).exists())
                self.assertEqual(
                    "pending",
                    forge.task_state(manifest, task, lease_seconds=600),
                    stdout + stderr + "\n" + "\n".join(str(path) for path in (run / "state").iterdir()),
                )
                self.assertFalse(Path(task["outputs"][0]).exists())
                self.assertEqual([], list(cache.glob("ctrl-c-cleanup-*.pid")))

                (root / "gate").write_text("resume\n", encoding="utf-8")
                resumed = subprocess.run(
                    [
                        sys.executable,
                        str(CLI),
                        "resume",
                        "ctrl-c-cleanup",
                        "--wait",
                        "--interval-seconds",
                        "0.02",
                        "--timeout-seconds",
                        "10",
                    ],
                    cwd=ROOT,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=20,
                    env=env,
                )
                self.assertEqual(0, resumed.returncode, resumed.stdout + resumed.stderr)
                self.assertEqual("complete", Path(task["outputs"][0]).read_text(encoding="utf-8"))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                for pid_file in cache.glob("ctrl-c-cleanup-*.pid"):
                    try:
                        pid = int(pid_file.read_text(encoding="utf-8").splitlines()[0])
                        os.killpg(pid, signal.SIGKILL)
                    except (OSError, ValueError, IndexError):
                        pass

    def test_interrupted_split_progress_run_resumes_from_preserved_games(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            cache = root / "worker-cache"
            runner = root / "progress-runner.py"
            runner.write_text(
                "#!/usr/bin/env python3\n"
                "import argparse, pathlib, time\n"
                "parser = argparse.ArgumentParser()\n"
                "parser.add_argument('--games', type=int, required=True)\n"
                "parser.add_argument('--log-dir', required=True)\n"
                "parser.add_argument('--name', required=True)\n"
                "args = parser.parse_args()\n"
                "log_dir = pathlib.Path(args.log_dir)\n"
                "log_dir.mkdir(parents=True, exist_ok=True)\n"
                "with (log_dir / f'{args.name}.log').open('w') as raw:\n"
                "    for done in range(2, args.games + 1, 2):\n"
                "        line = (f'[{done:2d}/{args.games}] Elo   5.0 +/-  20.0 | '"
                "                'LLR  0.10/2.20 (  5%) | LOS 60.0% | draw  40.0%')\n"
                "        print(line, flush=True)\n"
                "        raw.write(line + '\\n')\n"
                "        raw.flush()\n"
                "        time.sleep(1.0)\n",
                encoding="utf-8",
            )
            runner.chmod(0o755)
            progress = copy.deepcopy(
                forge.load_template(ROOT / "templates" / "sprt.template.json")["task"]["progress"]
            )
            template = root / "template.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "resume-progress",
                    "kind": "sprt",
                    "work_dir": str(root),
                    "tasks": [{
                        "id": "sprt_0000",
                        "index": 0,
                        "label": "sprt:10",
                        "command": (
                            f"{runner} --games 10 --log-dir "
                            "{output_dir}/sprt_0000/logs --name sprt_0000"
                        ),
                        "outputs": ["{output_dir}/sprt_0000/logs/sprt_0000.log"],
                        "progress": {**progress, "total": 10},
                        "resume": {"strategy": "split-progress", "count_arg": "--games"},
                    }],
                }),
                encoding="utf-8",
            )
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(cache),
                    "python": sys.executable,
                }],
            })
            env = {
                **os.environ,
                "FORGE_RUNS_DIR": str(runs),
                "FORGE_COORDINATOR_RUNS": "",
                "FORGE_NOTIFY_COMMAND": "",
            }
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CLI),
                    "start",
                    str(template),
                    "--workers",
                    str(workers),
                    "--wait",
                    "--interval-seconds",
                    "0.02",
                    "--timeout-seconds",
                    "20",
                ],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            try:
                running = runs / "resume-progress" / "state" / "sprt_0000.running.json"
                deadline = time.monotonic() + 10
                while not running.is_file() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(running.is_file())
                time.sleep(1.3)

                process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=40)
                self.assertEqual(130, process.returncode, stdout + stderr)

                stopped = forge.load_manifest(runs / "resume-progress" / "manifest.json")
                stopped_summary = forge.status_summary(stopped, lease_seconds=600)
                preserved = stopped_summary["progress_done"]["games"]
                self.assertGreaterEqual(preserved, 2)
                self.assertLess(preserved, 10)

                resumed = subprocess.run(
                    [
                        sys.executable,
                        str(CLI),
                        "resume",
                        "resume-progress",
                        "--wait",
                        "--interval-seconds",
                        "0.02",
                        "--timeout-seconds",
                        "20",
                    ],
                    cwd=ROOT,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=40,
                    env=env,
                )
                self.assertEqual(0, resumed.returncode, resumed.stdout + resumed.stderr)
                self.assertIn(f"preserved {preserved} completed game(s)", resumed.stdout)
                finished = forge.load_manifest(runs / "resume-progress" / "manifest.json")
                summary = forge.status_summary(finished, lease_seconds=600)
                self.assertEqual(10, summary["progress_done"]["games"])
                self.assertEqual(10, summary["progress_totals"]["games"])
                self.assertEqual(len(finished["tasks"]), summary["counts"]["done"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                for pid_file in cache.glob("resume-progress-*.pid"):
                    try:
                        pid = int(pid_file.read_text(encoding="utf-8").splitlines()[0])
                        os.killpg(pid, signal.SIGKILL)
                    except (OSError, ValueError, IndexError):
                        pass

    def test_start_queues_new_run_when_another_run_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            active_run = self.write_retry_run(runs, "active", failed=None)
            active_manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                active_manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "queued-new",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.start_queue_monitor", return_value=4242) as queue_monitor,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(argparse.Namespace(
                        manifest=str(manifest),
                        workers="worker-b",
                        jobs=1,
                        run=None,
                        resume=False,
                        replace=False,
                        retry_failed=False,
                        task_attempts=2,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        wait=False,
                        lease_seconds=600,
                        interval_seconds=30.0,
                        timeout_seconds=0.0,
                        fail_on_fail=True,
                        verify=None,
                        merge=None,
                        merge_force=False,
                        newline_between=False,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                        command_label="start",
                    ))

            self.assertEqual(0, rc)
            queue_monitor.assert_called_once()
            queued_manifest = forge.load_manifest(runs / "queued-new" / "manifest.json")
            self.assertTrue(forge.queue_request_path(queued_manifest).exists())
            output = stdout.getvalue()
            self.assertIn("start: queued queued-new; waiting for active", output)
            self.assertIn("start: queue monitor pid=4242", output)

            status = self.run_cli(
                "status",
                cwd=root,
                env={"FORGE_RUNS_DIR": str(runs), "FORGE_COORDINATOR_RUNS": ""},
            )
            self.assertIn("run=queued-new", status.stdout)
            payload = json.loads((runs / ".web-state.json").read_text(encoding="utf-8"))
            self.assertIn("queued-new", payload["seen_runs"])

    def test_concurrent_deploys_admit_only_one_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"

            def write_template(name: str) -> Path:
                path = root / f"{name}.json"
                path.write_text(
                    json.dumps({
                        "schema": "forge.task.v1",
                        "name": name,
                        "work_dir": str(root),
                        "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                    }),
                    encoding="utf-8",
                )
                return path

            def deploy_args(path: Path, worker: str) -> argparse.Namespace:
                return argparse.Namespace(
                    workers=worker,
                    worker_defaults=None,
                    worker_snapshot_payload=None,
                    preserve_workers_snapshot=False,
                    worker_snapshot_commit_payload=None,
                    launch_workers_payload=None,
                    launch_only=False,
                    manifest=str(path),
                    jobs=1,
                    run=None,
                    comment=None,
                    resume=False,
                    replace=False,
                    retry_failed=False,
                    task_attempts=2,
                    coordinator_host=None,
                    remote_timeout_seconds=30.0,
                    notify_command=None,
                    start_wait_monitor=False,
                    no_color=True,
                    verbose=False,
                )

            first_setup = threading.Event()
            release_setup = threading.Event()
            shell_calls = 0
            shell_lock = threading.Lock()

            def fake_worker_shell(*_args, **_kwargs):
                nonlocal shell_calls
                with shell_lock:
                    shell_calls += 1
                    call = shell_calls
                if call == 1:
                    first_setup.set()
                    self.assertTrue(release_setup.wait(5))
                return subprocess.CompletedProcess(["worker"], 0, stdout="ok", stderr="")

            results: dict[str, int] = {}
            errors: list[BaseException] = []

            def deploy(name: str, args: argparse.Namespace) -> None:
                try:
                    results[name] = forge.cmd_deploy(args)
                except BaseException as exc:
                    errors.append(exc)

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.bootstrap_worker_runner"),
                mock.patch("forge_lib.workers.sync_inputs_to_worker", return_value=0),
                mock.patch("forge_lib.workers.run_worker_shell", side_effect=fake_worker_shell),
                mock.patch("forge_lib.workers.start_queue_monitor", return_value=4343),
            ):
                first = threading.Thread(target=deploy, args=("first", deploy_args(write_template("first"), "worker-a")))
                first.start()
                self.assertTrue(first_setup.wait(5))

                first_manifest = forge.load_manifest(runs / "first" / "manifest.json")
                self.assertTrue(forge.launch_request_path(first_manifest).exists())

                second = threading.Thread(target=deploy, args=("second", deploy_args(write_template("second"), "worker-b")))
                second.start()
                second.join(5)
                self.assertFalse(second.is_alive())

                second_manifest = forge.load_manifest(runs / "second" / "manifest.json")
                self.assertTrue(forge.queue_request_path(second_manifest).exists())
                self.assertFalse(forge.launch_request_path(second_manifest).exists())

                release_setup.set()
                first.join(5)

            self.assertFalse(errors)
            self.assertEqual({"first": 0, "second": 0}, results)

    def test_started_run_between_claims_blocks_the_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "between-claims", failed=None)
            manifest_path = run / "manifest.json"
            manifest = forge.load_manifest(manifest_path)
            manifest["tasks"].append({"id": "task_1", "index": 1, "command": "true", "outputs": []})
            forge.write_json(manifest_path, manifest)
            forge.write_json(run / "state" / "task_0.done.json", {"task": "task_0"})

            with self.patch_runs_dir(runs):
                blockers = forge.active_or_queued_blockers("new-run", lease_seconds=600)

        self.assertEqual(["between-claims"], [summary["run"] for summary in blockers])

    def test_active_run_with_stale_launch_marker_still_blocks_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            forge.write_json(forge.launch_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "active",
                "requested_epoch": 1.0,
                "lease_seconds": 1,
            })

            with self.patch_runs_dir(runs):
                blockers = forge.active_or_queued_blockers("new-run", lease_seconds=600)

        self.assertEqual(["active"], [summary["run"] for summary in blockers])

    def test_queue_launcher_claims_runs_in_fifo_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            for name, requested_epoch in (("first", 100.0), ("second", 200.0), ("third", 300.0)):
                run = self.write_retry_run(runs, name, failed=None)
                manifest = forge.load_manifest(run / "manifest.json")
                forge.write_json(forge.queue_request_path(manifest), {
                    "schema": "forge.task.v1",
                    "run": name,
                    "requested_epoch": requested_epoch,
                    "command": ["true"],
                })

            def payload(name: str) -> dict[str, Any]:
                return {
                    "run": name,
                    "runs_dir": str(runs),
                    "command": ["true"],
                    "lease_seconds": 600,
                    "poll_seconds": 0.5,
                }

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.subprocess.run", return_value=subprocess.CompletedProcess(["true"], 0)),
            ):
                self.assertEqual(0, forge.run_queue_launcher(payload("first")))
                first_manifest = forge.load_manifest(runs / "first" / "manifest.json")
                forge.queue_request_path(first_manifest).unlink()
                summaries = forge.load_run_summaries(lease_seconds=600)
                self.assertEqual(["first"], [item["run"] for item in forge.queue_blockers_for_run("second", summaries)])

                forge.clear_launch_request(first_manifest)
                self.assertEqual(0, forge.run_queue_launcher(payload("second")))

            second_manifest = forge.load_manifest(runs / "second" / "manifest.json")
            third_manifest = forge.load_manifest(runs / "third" / "manifest.json")
            self.assertTrue(forge.queue_request_path(second_manifest).exists())
            self.assertTrue(forge.launch_request_path(second_manifest).exists())
            self.assertTrue(forge.queue_request_path(third_manifest).exists())

    def test_queue_recovery_restarts_all_dead_monitors_in_fifo_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            for name, requested_epoch in (("later", 200.0), ("earlier", 100.0)):
                run = self.write_retry_run(runs, name, failed=None)
                manifest = forge.load_manifest(run / "manifest.json")
                forge.write_json(forge.queue_request_path(manifest), {
                    "schema": "forge.task.v1",
                    "run": name,
                    "requested_epoch": requested_epoch,
                    "command": ["forge", "resume", name],
                })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.start_queue_monitor", side_effect=(111, 222)) as start,
            ):
                monitors = forge.recover_queue_monitors(lease_seconds=600)

        self.assertEqual({"earlier": 111, "later": 222}, monitors)
        self.assertEqual(
            [runs / "earlier", runs / "later"],
            [call.kwargs["run_dir"] for call in start.call_args_list],
        )

    def test_queued_local_run_starts_after_active_run_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            cache = root / "worker-cache"
            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(cache),
                    "python": sys.executable,
                }],
            })

            def write_template(name: str, command: str) -> Path:
                path = root / f"{name}.json"
                path.write_text(
                    json.dumps({
                        "schema": "forge.task.v1",
                        "name": name,
                        "work_dir": str(root),
                        "requirements": {"python_min": "3.11", "git_min": "2.38"},
                        "tasks": [{"id": "task_0000", "index": 0, "command": command, "outputs": []}],
                    }),
                    encoding="utf-8",
                )
                return path

            env = {
                **os.environ,
                "FORGE_RUNS_DIR": str(runs),
                "FORGE_COORDINATOR_RUNS": "",
                "FORGE_NOTIFY_COMMAND": "",
            }
            first = self.run_cli(
                "start",
                str(write_template("queue-first", "sleep 1.5")),
                "--workers",
                str(workers),
                "--jobs",
                "1",
                cwd=ROOT,
                env=env,
            )
            second = self.run_cli(
                "start",
                str(write_template("queue-second", "true")),
                "--workers",
                str(workers),
                "--jobs",
                "1",
                cwd=ROOT,
                env=env,
            )

            try:
                self.assertEqual(0, first.returncode, first.stdout + first.stderr)
                self.assertEqual(0, second.returncode, second.stdout + second.stderr)
                self.assertIn("start: queued queue-second; waiting for queue-first", second.stdout)

                overlap = False
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    first_manifest = forge.load_manifest(runs / "queue-first" / "manifest.json")
                    second_manifest = forge.load_manifest(runs / "queue-second" / "manifest.json")
                    first_summary = forge.status_summary(first_manifest, lease_seconds=600)
                    second_summary = forge.status_summary(second_manifest, lease_seconds=600)
                    overlap |= first_summary["active_count"] > 0 and second_summary["active_count"] > 0
                    if first_summary["counts"]["done"] == 1 and second_summary["counts"]["done"] == 1:
                        break
                    time.sleep(0.02)
                else:
                    queue_log = runs / "queue-second" / "queue.log"
                    wait_log = runs / "queue-first" / "wait.log"
                    self.fail(
                        "queued run did not finish: "
                        f"first={first_summary['counts']} second={second_summary['counts']} "
                        f"first_launch={first_summary.get('launch')} "
                        f"second_monitor={forge.queue_monitor_state(runs / 'queue-second' / 'manifest.json')} "
                        f"first_stdout={first.stdout!r} first_stderr={first.stderr!r} "
                        f"wait_log={wait_log.read_text(encoding='utf-8') if wait_log.exists() else '<missing>'} "
                        f"queue_log={queue_log.read_text(encoding='utf-8') if queue_log.exists() else '<missing>'}"
                    )

                self.assertFalse(overlap)
                self.assertFalse(forge.queue_request_path(second_manifest).exists())
            finally:
                for pid_file in cache.glob("queue-*.pid"):
                    try:
                        pid = int(pid_file.read_text(encoding="utf-8").splitlines()[0])
                        os.killpg(pid, signal.SIGKILL)
                    except (OSError, ValueError, IndexError):
                        pass
                for monitor_file in runs.glob("queue-*/**/*-monitor.json"):
                    try:
                        pid = int(forge.read_json(monitor_file)["pid"])
                        os.killpg(pid, signal.SIGKILL)
                    except (OSError, SystemExit, TypeError, ValueError, KeyError):
                        pass

    def test_start_verify_waits_for_queued_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            active_run = self.write_retry_run(runs, "active", failed=None)
            active_manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                active_manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "queued-verify",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.start_queue_monitor") as queue_monitor,
                mock.patch("forge_lib.workers.run_queue_launcher_foreground", return_value=23) as queue_launcher,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(argparse.Namespace(
                        manifest=str(manifest),
                        workers="worker-b",
                        jobs=1,
                        run=None,
                        resume=False,
                        replace=False,
                        retry_failed=False,
                        task_attempts=2,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        wait=False,
                        lease_seconds=600,
                        interval_seconds=30.0,
                        timeout_seconds=0.0,
                        fail_on_fail=True,
                        verify=True,
                        merge=None,
                        merge_force=False,
                        newline_between=False,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                        command_label="start",
                    ))

            self.assertEqual(23, rc)
            queue_monitor.assert_not_called()
            queue_launcher.assert_called_once()
            command = queue_launcher.call_args.kwargs["command"]
            self.assertIn("resume", command)
            self.assertIn("--verify", command)
            queued_manifest = forge.load_manifest(runs / "queued-verify" / "manifest.json")
            self.assertTrue(forge.queue_request_path(queued_manifest).exists())
            output = stdout.getvalue()
            self.assertIn("start: queued queued-verify; waiting for active", output)
            self.assertIn("start: waiting for queued run to finish", output)
            self.assertNotIn("start: queue monitor pid=", output)

    def test_queue_launcher_can_fail_when_queue_marker_is_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "cleared", failed=None)
            payload = {
                "run": "cleared",
                "runs_dir": str(runs),
                "command": ["true"],
                "lease_seconds": 600,
                "poll_seconds": 0.5,
                "fail_if_unqueued": True,
            }

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.run_queue_launcher(payload)

        self.assertEqual(1, rc)
        self.assertIn("queue: cleared is no longer queued", stdout.getvalue())

    def test_queue_launcher_failure_keeps_run_queued_for_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "retry-queue", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.write_json(forge.queue_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "retry-queue",
                "requested_epoch": 100.0,
                "command": ["false"],
            })
            payload = {
                "run": "retry-queue",
                "runs_dir": str(runs),
                "command": ["false"],
                "lease_seconds": 600,
                "poll_seconds": 0.5,
            }

            with (
                self.patch_runs_dir(runs),
                mock.patch(
                    "forge_lib.workers.subprocess.run",
                    return_value=subprocess.CompletedProcess(["false"], 1),
                ),
            ):
                self.assertEqual(1, forge.run_queue_launcher(payload))

            self.assertTrue(forge.queue_request_path(manifest).exists())
            self.assertTrue(forge.launch_request_path(manifest).exists())

    def test_deploy_resume_queues_when_another_run_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            active_run = self.write_retry_run(runs, "active", failed=None)
            active_manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                active_manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            paused_run = self.write_retry_run(runs, "paused", failed=None)

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_worker_shell") as run_worker_shell,
                mock.patch("forge_lib.workers.start_queue_monitor", return_value=4245) as queue_monitor,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(argparse.Namespace(
                        workers="worker-b:2",
                        worker_defaults=None,
                        worker_snapshot_payload=None,
                        preserve_workers_snapshot=False,
                        launch_workers_payload=None,
                        manifest=str(paused_run / "manifest.json"),
                        jobs=2,
                        run="paused",
                        resume=True,
                        replace=False,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        retry_failed=False,
                        task_attempts=2,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                    ))

            self.assertEqual(0, rc)
            run_worker_shell.assert_not_called()
            queue_monitor.assert_called_once()
            queued_manifest = forge.load_manifest(paused_run / "manifest.json")
            self.assertTrue(forge.queue_request_path(queued_manifest).exists())
            output = stdout.getvalue()
            self.assertIn("deploy: queued paused; waiting for active", output)
            self.assertIn("deploy: queue monitor pid=4245", output)

    def test_start_queues_new_run_behind_existing_queued_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            queued_run = self.write_retry_run(runs, "already-queued", failed=None)
            queued_manifest = forge.load_manifest(queued_run / "manifest.json")
            forge.write_json(forge.queue_request_path(queued_manifest), {
                "schema": "forge.task.v1",
                "run": "already-queued",
                "requested_epoch": 100.0,
            })
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "queued-behind",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.start_queue_monitor", return_value=4243),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(argparse.Namespace(
                        manifest=str(manifest),
                        workers="worker-b",
                        jobs=1,
                        run=None,
                        resume=False,
                        replace=False,
                        retry_failed=False,
                        task_attempts=2,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        wait=False,
                        lease_seconds=600,
                        interval_seconds=30.0,
                        timeout_seconds=0.0,
                        fail_on_fail=True,
                        verify=None,
                        merge=None,
                        merge_force=False,
                        newline_between=False,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                        command_label="start",
                    ))

            self.assertEqual(0, rc)
            self.assertIn("start: queued queued-behind; waiting for already-queued", stdout.getvalue())

    def test_start_wait_verifies_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "wait-default",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            captured = {}

            def fake_deploy(args):
                captured["deploy"] = args
                return 0

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_deploy", side_effect=fake_deploy),
                mock.patch("forge_lib.workers.cmd_wait", return_value=0) as wait,
                mock.patch("forge_lib.workers.cmd_verify", return_value=0) as verify,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(argparse.Namespace(
                        manifest=str(manifest),
                        workers="worker-a",
                        jobs=1,
                        run=None,
                        resume=False,
                        replace=False,
                        retry_failed=False,
                        task_attempts=2,
                        coordinator_host=None,
                        remote_timeout_seconds=1800.0,
                        wait=True,
                        lease_seconds=600,
                        interval_seconds=30.0,
                        timeout_seconds=0.0,
                        fail_on_fail=True,
                        verify=None,
                        merge=None,
                        merge_force=False,
                        newline_between=False,
                        notify_command=None,
                        no_color=True,
                        verbose=False,
                        command_label="start",
                    ))

            self.assertEqual(0, rc)
            self.assertTrue(captured["deploy"].launch_only)
            wait.assert_called_once()
            verify.assert_called_once()
            self.assertIn("start: ok; status: forge status wait-default --verbose", stdout.getvalue())

    def test_start_wait_no_verify_skips_verify(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            manifest = root / "template.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "wait-no-verify",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_deploy", return_value=0),
                mock.patch("forge_lib.workers.cmd_wait", return_value=0) as wait,
                mock.patch("forge_lib.workers.cmd_verify", return_value=0) as verify,
            ):
                rc = forge.cmd_run_job(argparse.Namespace(
                    manifest=str(manifest),
                    workers="worker-a",
                    jobs=1,
                    run=None,
                    resume=False,
                    replace=False,
                    retry_failed=False,
                    task_attempts=2,
                    coordinator_host=None,
                    remote_timeout_seconds=1800.0,
                    wait=True,
                    lease_seconds=600,
                    interval_seconds=30.0,
                    timeout_seconds=0.0,
                    fail_on_fail=True,
                    verify=False,
                    merge=None,
                    merge_force=False,
                    newline_between=False,
                    notify_command=None,
                    no_color=True,
                    verbose=False,
                    command_label="start",
                ))

            self.assertEqual(0, rc)
            wait.assert_called_once()
            verify.assert_not_called()

    def test_start_does_not_merge_failed_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            (repo / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "template.json"
            plan = self.run_cli(
                "plan",
                "--name", "run-fail",
                "--shards", "2",
                "--work-dir", str(repo),
                "--out", str(manifest),
                "--command-template", "if [ {index} -eq 0 ]; then printf ok > {output}; else false; fi",
                "--output-template", str(repo / "out" / "task.{index}.txt"),
            )
            self.assertEqual(0, plan.returncode)

            workers = root / "workers.json"
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(root / "worker-cache"),
                    "python": sys.executable,
                }],
            })

            merged = root / "merged.txt"
            result = self.run_cli(
                "start", str(manifest),
                "--workers", str(workers),
                "--jobs", "1",
                "--wait",
                "--merge", str(merged),
                env={"FORGE_RUNS_DIR": str(root / "runs")},
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("deploy: tasks failed", result.stdout)
            self.assertIn("start: deploy failed", result.stderr)
            self.assertFalse(merged.exists())

    def test_retry_without_run_requeues_failed_tasks_without_launching_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run_a = self.write_retry_run(runs, "failed-a", failed=True)
            run_b = self.write_retry_run(runs, "failed-b", failed=True)
            self.write_retry_run(runs, "done-run", failed=False)

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_retry(self.retry_args())

            self.assertEqual(0, rc)
            run_job.assert_not_called()
            self.assertFalse((run_a / "state" / "task_0.fail.json").exists())
            self.assertFalse((run_b / "state" / "task_0.fail.json").exists())
            output = stdout.getvalue()
            self.assertIn("retry: failed-a (1/2)", output)
            self.assertIn("retry: failed-b (2/2)", output)
            self.assertIn("retry: failed-a requeued=1; no workers launched", output)
            self.assertIn("retry: failed-b requeued=1; no workers launched", output)

    def test_retry_can_use_alternate_workers_for_named_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            self.write_retry_run(runs, "failed-a", failed=True)
            alternate_workers = root / "alternate-workers.json"
            forge.write_json(alternate_workers, {
                "schema": "forge.workers.v1",
                "workers": ["worker-b"],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                rc = forge.cmd_retry(self.retry_args(run="failed-a", workers=str(alternate_workers)))

            self.assertEqual(0, rc)
            retry_args = run_job.call_args.args[0]
            self.assertEqual("failed-a", retry_args.run)
            self.assertEqual(str(alternate_workers), retry_args.workers)
            self.assertTrue(retry_args.resume)
            self.assertTrue(retry_args.retry_failed)

    def test_retry_clears_stop_and_queue_before_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "failed-a", failed=True)
            manifest = forge.load_manifest(run / "manifest.json")
            task = manifest["tasks"][0]
            fail_path = forge.task_fail_path(manifest, task)
            forge.write_json(forge.stop_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "failed-a",
            })
            forge.write_json(forge.queue_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "failed-a",
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_retry(self.retry_args(run="failed-a"))

            self.assertEqual(0, rc)
            run_job.assert_not_called()
            self.assertFalse(forge.stop_request_path(manifest).exists())
            self.assertFalse(forge.queue_request_path(manifest).exists())
            self.assertFalse(fail_path.exists())
            archived = list((forge.state_dir(manifest) / "retry-failures").glob("*/task_0.fail.json"))
            self.assertEqual(1, len(archived))
            output = stdout.getvalue()
            self.assertIn("retry: cleared stop request for failed-a", output)
            self.assertIn("retry: cleared queue request for failed-a", output)
            self.assertIn("retry: archived 1 failed task marker(s) for failed-a", output)
            self.assertIn("retry: failed-a requeued=1; no workers launched", output)

    def test_retry_with_workers_refuses_to_double_launch_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "failed-a", failed=True)
            manifest = forge.load_manifest(run / "manifest.json")
            active_task = {"id": "task_1", "index": 1, "command": "true", "outputs": []}
            manifest["tasks"].append(active_task)
            forge.write_json(run / "manifest.json", manifest)
            forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
                self.assertRaises(SystemExit) as raised,
            ):
                forge.cmd_retry(self.retry_args(run="failed-a", workers="worker-b:2"))

            run_job.assert_not_called()
            self.assertTrue((run / "state" / "task_0.fail.json").exists())
            self.assertIn("already has 1 active/claimed/stale task", str(raised.exception))

    def test_queued_retry_command_stays_retry(self) -> None:
        args = self.retry_args(run="failed-a")
        args.retry_failed = True

        command = forge.queue_resume_command(args, "failed-a")

        self.assertEqual("retry", Path(command[2]).name if "/" in command[2] else command[2])
        self.assertEqual("failed-a", command[3])
        self.assertEqual("--workers", command[4])
        self.assertTrue(command[5].endswith("/failed-a/workers.json"))

    def test_retry_with_workers_preserves_live_queue_launch_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "failed-a", failed=True)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.write_json(forge.launch_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "failed-a",
                "reason": "queue",
                "pid": os.getpid(),
                "requested_epoch": time.time(),
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                self.assertEqual(0, forge.cmd_retry(self.retry_args(run="failed-a", workers="worker-b")))

            self.assertTrue(forge.launch_request_path(manifest).exists())
            run_job.assert_called_once()

    def test_retry_named_run_delegates_to_coordinator(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote retry\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch(
            "forge_lib.workers.run_interruptible_coordinator_process",
            return_value=remote_result,
        ) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_retry(self.retry_args(run="failed-a"))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote retry\n",
            stdout.getvalue(),
        )
        host, remote, pid_file = run_mock.call_args.args
        self.assertEqual("pwa-5090", host)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", remote)
        self.assertIn("FORGE_COORDINATOR_RUNS=", remote)
        self.assertIn("forge retry failed-a", remote)
        self.assertIn(pid_file, remote)

    def test_retry_without_failed_runs_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "done-run", failed=False)

            with self.patch_runs_dir(runs), self.assertRaises(SystemExit) as raised:
                forge.cmd_retry(self.retry_args())

            self.assertIn("no failed Forge run found", str(raised.exception))

    def test_resume_clears_stop_and_restarts_incomplete_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "paused", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.write_json(forge.stop_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "paused",
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_resume(self.retry_args(run="paused"))

            self.assertEqual(0, rc)
            self.assertFalse(forge.stop_request_path(manifest).exists())
            self.assertIn("resume: cleared stop request for paused", stdout.getvalue())
            resume_args = run_job.call_args.args[0]
            self.assertEqual("paused", resume_args.run)
            self.assertEqual(str(run / "workers.json"), resume_args.workers)
            self.assertTrue(resume_args.resume)
            self.assertFalse(resume_args.retry_failed)
            self.assertEqual("resume", resume_args.command_label)

    def test_stop_preserves_split_progress_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = self.write_resumable_run(Path(tmp) / "runs", "paused-sprt", done=8, history=8)
            manifest = forge.load_manifest(run / "manifest.json")
            task = manifest["tasks"][0]
            output = Path(task["outputs"][0])
            forge.stop_request_path(manifest).unlink()
            forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            heartbeat = forge.read_json(forge.task_heartbeat_path(manifest, task))
            heartbeat["progress_data"] = {"done": 8, "total": 10, "unit": "games"}
            forge.write_json(forge.task_heartbeat_path(manifest, task), heartbeat)
            forge.write_json(forge.task_running_path(manifest, task), {"task": task["id"]})

            with mock.patch("forge_lib.workers.kill_run_workers", return_value=1):
                forge.request_stop_for_run(
                    run / "manifest.json",
                    manifest,
                    lease_seconds=600,
                    preserve_active_tasks=True,
                )

            self.assertTrue(output.is_file())
            self.assertEqual("running", forge.task_state(manifest, task, lease_seconds=600))
            self.assertEqual(
                {task["id"]: 8},
                forge.read_json(forge.stop_request_path(manifest))["task_progress"],
            )
            resumed = forge.prepare_run_resume(run / "manifest.json", manifest)
            self.assertEqual({"preserved": 8, "continued": 1}, resumed)

    def test_resume_seals_partial_progress_and_runs_only_remainder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = self.write_resumable_run(Path(tmp) / "runs", "paused-sprt", done=8, history=8)
            manifest_path = run / "manifest.json"
            manifest = forge.load_manifest(manifest_path)

            result = forge.prepare_run_resume(manifest_path, manifest)
            resumed = forge.load_manifest(manifest_path)

            self.assertEqual({"preserved": 8, "continued": 1}, result)
            self.assertEqual(2, len(resumed["tasks"]))
            original, remainder = resumed["tasks"]
            self.assertEqual(8, original["progress"]["total"])
            self.assertEqual(2, remainder["progress"]["total"])
            self.assertIn("--games 2", remainder["command"])
            self.assertIn(remainder["id"], remainder["command"])
            self.assertNotEqual(original["outputs"], remainder["outputs"])
            self.assertTrue(forge.task_complete(resumed, original))
            self.assertFalse(forge.task_complete(resumed, remainder))
            summary = forge.status_summary(resumed, lease_seconds=600)
            self.assertEqual(10, summary["progress_totals"]["games"])
            self.assertEqual(8, summary["progress_done"]["games"])

            repeated = forge.prepare_run_resume(manifest_path, resumed)
            self.assertEqual({"preserved": 0, "continued": 0}, repeated)
            self.assertEqual(2, len(forge.load_manifest(manifest_path)["tasks"]))

    def test_resume_refuses_to_discard_recorded_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = self.write_resumable_run(Path(tmp) / "runs", "damaged-sprt", done=6, history=8)
            manifest_path = run / "manifest.json"

            with self.assertRaisesRegex(
                SystemExit,
                "only 6 of 8 recorded games are recoverable; restart refused",
            ):
                forge.prepare_run_resume(manifest_path, forge.load_manifest(manifest_path))

            unchanged = forge.load_manifest(manifest_path)
            self.assertEqual(1, len(unchanged["tasks"]))
            self.assertTrue(forge.stop_request_path(unchanged).exists())

    def test_resume_refuses_legacy_sprt_with_partial_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = self.write_resumable_run(Path(tmp) / "runs", "legacy-sprt", done=8, history=8)
            manifest_path = run / "manifest.json"
            manifest = forge.load_manifest(manifest_path)
            manifest["tasks"][0].pop("resume")
            forge.write_json(manifest_path, manifest)

            with self.assertRaisesRegex(
                SystemExit,
                "8 recorded games have no resumable shard state; restart refused",
            ):
                forge.prepare_run_resume(manifest_path, manifest)

    def test_resume_uses_configured_workers_when_run_snapshot_is_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = self.write_retry_run(runs, "paused", failed=None)
            (run / "workers.json").unlink()
            defaults = root / "workers.json"
            payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "linux", "slots": 4},
                    {"host": "worker-b", "profile": "mac", "slots": 2},
                ],
            }
            forge.write_json(defaults, payload)

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.default_workers_path", return_value=defaults),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                self.assertEqual(0, forge.cmd_resume(self.retry_args(run="paused")))

            self.assertEqual(payload, forge.read_json(run / "workers.json"))
            self.assertEqual(str(run / "workers.json"), run_job.call_args.args[0].workers)

    def test_resume_clears_stale_launch_and_restarts_incomplete_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "launch-stale", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.write_json(forge.launch_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "launch-stale",
                "requested_epoch": 1000.0,
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_resume(self.retry_args(run="launch-stale"))

            self.assertEqual(0, rc)
            self.assertFalse(forge.launch_request_path(manifest).exists())
            self.assertIn("resume: cleared launch request for launch-stale", stdout.getvalue())
            run_job.assert_called_once()

    def test_resume_does_not_launch_workers_for_already_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            forge.write_json(forge.stop_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "active",
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_resume(self.retry_args(run="active"))

            self.assertEqual(0, rc)
            run_job.assert_not_called()
            self.assertFalse(forge.stop_request_path(manifest).exists())
            self.assertIn("resume: active already active (1 task(s)); no workers launched", stdout.getvalue())

    def test_resume_queues_stopped_run_when_another_run_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            active_run = self.write_retry_run(runs, "active", failed=None)
            active_manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                active_manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            paused_run = self.write_retry_run(runs, "paused", failed=None)
            paused_manifest = forge.load_manifest(paused_run / "manifest.json")
            forge.write_json(forge.stop_request_path(paused_manifest), {
                "schema": "forge.task.v1",
                "run": "paused",
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_worker_shell") as run_worker_shell,
                mock.patch("forge_lib.workers.start_queue_monitor", return_value=4244) as queue_monitor,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_resume(self.retry_args(run="paused"))

            self.assertEqual(0, rc)
            run_worker_shell.assert_not_called()
            queue_monitor.assert_called_once()
            self.assertFalse(forge.stop_request_path(paused_manifest).exists())
            queue = forge.read_json(forge.queue_request_path(paused_manifest))
            self.assertEqual("paused", queue["run"])
            self.assertEqual(["active"], queue["blocked_by"])
            self.assertIn("resume", queue["command"])
            self.assertIn("paused", queue["command"])
            output = stdout.getvalue()
            self.assertIn("resume: cleared stop request for paused", output)
            self.assertIn("resume: queued paused; waiting for active", output)
            self.assertIn("resume: queue monitor pid=4244", output)

    def test_resume_verify_waits_for_queued_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            active_run = self.write_retry_run(runs, "active", failed=None)
            active_manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                active_manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            paused_run = self.write_retry_run(runs, "paused", failed=None)

            args = self.retry_args(run="paused")
            args.verify = True
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_worker_shell") as run_worker_shell,
                mock.patch("forge_lib.workers.start_queue_monitor") as queue_monitor,
                mock.patch("forge_lib.workers.run_queue_launcher_foreground", return_value=5) as queue_launcher,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_resume(args)

            self.assertEqual(1, rc)
            run_worker_shell.assert_not_called()
            queue_monitor.assert_not_called()
            queue_launcher.assert_called_once()
            command = queue_launcher.call_args.kwargs["command"]
            self.assertIn("resume", command)
            self.assertIn("--verify", command)
            output = stdout.getvalue()
            self.assertIn("resume: queued paused; waiting for active", output)
            self.assertIn("resume: waiting for queued run to finish", output)
            self.assertNotIn("resume: queue monitor pid=", output)

    def test_resume_named_run_delegates_to_coordinator(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote resume\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch(
            "forge_lib.workers.run_interruptible_coordinator_process",
            return_value=remote_result,
        ) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_resume(self.retry_args(run="paused"))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote resume\n",
            stdout.getvalue(),
        )
        host, remote, pid_file = run_mock.call_args.args
        self.assertEqual("pwa-5090", host)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", remote)
        self.assertIn("FORGE_COORDINATOR_RUNS=", remote)
        self.assertIn("forge resume paused", remote)
        self.assertIn(pid_file, remote)

    def test_resume_without_run_requires_single_incomplete_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "paused-a", failed=None)
            self.write_retry_run(runs, "paused-b", failed=None)

            with self.patch_runs_dir(runs), self.assertRaises(SystemExit) as raised:
                forge.cmd_resume(self.retry_args())

            message = str(raised.exception)
            self.assertIn("multiple incomplete Forge runs found", message)
            self.assertIn("paused-a", message)
            self.assertIn("paused-b", message)


    def test_worker_set_launches_only_delta_slots_and_commits_total(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            cache = Path(tmp) / "cache"
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "pwa-mbp",
                    "profile": "mac",
                    "local": True,
                    "cache_dir": str(cache),
                    "slots": 4,
                }],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_active_worker_task_count", return_value=4),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                rc = forge.cmd_worker_set(self.worker_set_args(run="active", worker="pwa-mbp:6"))

            self.assertEqual(0, rc)
            set_args = run_job.call_args.args[0]
            self.assertEqual("active", set_args.run)
            self.assertEqual("pwa-mbp=mac:2", set_args.workers)
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "pwa-mbp",
                        "profile": "mac",
                        "local": True,
                        "cache_dir": str(cache),
                        "slots": 2,
                    }],
                },
                set_args.launch_workers_payload,
            )
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "pwa-mbp",
                        "profile": "mac",
                        "local": True,
                        "cache_dir": str(cache),
                        "slots": 6,
                    }],
                },
                set_args.worker_snapshot_commit_payload,
            )
            self.assertEqual("worker set", set_args.command_label)
            self.assertTrue(set_args.launch_only)

    def test_worker_set_seeds_defaults_then_merges_repeated_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            (run / "workers.json").unlink()
            defaults = root / "config-workers.json"
            forge.write_json(defaults, {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "linux", "slots": 4},
                    {"host": "worker-b", "profile": "mac", "slots": 3},
                ],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.default_workers_path", return_value=defaults),
                mock.patch("forge_lib.workers.run_pending_task_count", return_value=0),
            ):
                self.assertEqual(0, forge.cmd_worker_set(self.worker_set_args(run="active", worker="worker-c:1")))
                self.assertEqual(0, forge.cmd_worker_set(self.worker_set_args(run="active", worker="worker-c:2")))

            payload = forge.read_json(run / "workers.json")
            self.assertEqual(
                [("worker-a", 4), ("worker-b", 3), ("worker-c", 2)],
                [(item["host"], int(item.get("slots", 1))) for item in payload["workers"]],
            )

    def test_worker_set_refreshes_missing_metadata_from_current_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{"host": "pwa-mbp", "profile": "pwa-mbp", "slots": 0}],
            })
            defaults = root / "config-workers.json"
            forge.write_json(defaults, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "pwa-mbp",
                    "profile": "pwa-mbp",
                    "python": "/opt/homebrew/bin/python3",
                    "slots": 5,
                }],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.default_workers_path", return_value=defaults),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                self.assertEqual(0, forge.cmd_worker_set(self.worker_set_args(run="active", worker="pwa-mbp:5")))

            set_args = run_job.call_args.args[0]
            self.assertEqual("/opt/homebrew/bin/python3", set_args.launch_workers_payload["workers"][0]["python"])
            self.assertEqual("/opt/homebrew/bin/python3", set_args.worker_snapshot_commit_payload["workers"][0]["python"])

    def test_worker_set_launch_preserves_all_default_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            (run / "workers.json").unlink()
            defaults = root / "config-workers.json"
            forge.write_json(defaults, {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "slots": 4},
                    {"host": "worker-b", "slots": 3},
                ],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.default_workers_path", return_value=defaults),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                self.assertEqual(0, forge.cmd_worker_set(self.worker_set_args(run="active", worker="worker-c:2")))

            payload = run_job.call_args.args[0].worker_snapshot_commit_payload
            self.assertEqual(
                [("worker-a", 4), ("worker-b", 3), ("worker-c", 2)],
                [(item["host"], int(item.get("slots", 1))) for item in payload["workers"]],
            )

    def test_worker_set_same_total_restarts_missing_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "pwa-mbp",
                    "profile": "mac",
                    "local": True,
                    "slots": 6,
                }],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                rc = forge.cmd_worker_set(self.worker_set_args(run="active", worker="pwa-mbp:6"))

            self.assertEqual(0, rc)
            set_args = run_job.call_args.args[0]
            self.assertEqual("pwa-mbp=mac:6", set_args.workers)
            self.assertEqual(6, set_args.launch_workers_payload["workers"][0]["slots"])
            self.assertEqual(6, set_args.worker_snapshot_commit_payload["workers"][0]["slots"])

    def test_worker_set_same_total_with_active_slots_does_not_launch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{"host": "pwa-mbp", "profile": "mac", "local": True, "slots": 6}],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_active_worker_task_count", return_value=6),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_worker_set(self.worker_set_args(run="active", worker="pwa-mbp:6"))

            self.assertEqual(0, rc)
            run_job.assert_not_called()
            self.assertIn("worker set: active pwa-mbp slots=6 unchanged", stdout.getvalue())

    def test_worker_set_reduces_recorded_total_without_launching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "pwa-mbp",
                    "profile": "mac",
                    "local": True,
                    "slots": 6,
                }],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_active_worker_task_count", return_value=6),
                mock.patch("forge_lib.workers.cmd_run_job") as run_job,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_worker_set(self.worker_set_args(run="active", worker="pwa-mbp:4"))

            self.assertEqual(0, rc)
            run_job.assert_not_called()
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "pwa-mbp",
                        "profile": "mac",
                        "local": True,
                        "slots": 4,
                    }],
                },
                forge.read_json(run / "workers.json"),
            )
            self.assertIn("worker set: active pwa-mbp slots=6->4 released=0", stdout.getvalue())

    def test_worker_path_map_applies_to_worker_manifest_without_coordinator_profile(self) -> None:
        manifest = {
            "schema": forge.SCHEMA,
            "path_maps": {
                "pwa-mbp0": {
                    "/home/petter/code/cpp/chess": "/Users/petter/code/cpp/chess",
                },
            },
            "work_dir": "/home/petter/code/cpp/chess/nnue",
            "requirements": {
                "inputs": [{
                    "path": "/home/petter/code/cpp/chess/nnue/runs/source.jsonl",
                }],
            },
            "tasks": [],
        }
        worker = {
            "host": "pwa-mbp",
            "profile": "pwa-mbp",
            "path_map": {
                "/home/petter/code/cpp/chess": "/Users/pwahlman/code/cpp/chess",
            },
        }

        local_manifest = forge.local_manifest_for_worker(manifest, worker)
        worker_manifest = forge.worker_manifest(manifest, worker)

        self.assertNotIn("pwa-mbp", manifest["path_maps"])
        self.assertEqual(
            "/Users/pwahlman/code/cpp/chess/nnue",
            worker_manifest["work_dir"],
        )
        self.assertEqual(
            "/Users/pwahlman/code/cpp/chess/nnue/runs/source.jsonl",
            worker_manifest["requirements"]["inputs"][0]["path"],
        )
        self.assertEqual("pwa-mbp", local_manifest["_active_profile"])
        self.assertEqual(
            "/Users/pwahlman/code/cpp/chess",
            local_manifest["path_maps"]["pwa-mbp"]["/home/petter/code/cpp/chess"],
        )
        self.assertEqual(["--profile", "pwa-mbp"], forge.worker_profile_args(manifest, worker))


    def test_worker_set_command_sets_total_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "paused", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "worker-b",
                    "profile": "worker-b",
                    "local": False,
                    "slots": 4,
                }],
            })

            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.run_active_worker_task_count", return_value=4),
                mock.patch("forge_lib.workers.cmd_run_job", return_value=0) as run_job,
            ):
                args = forge.build_parser().parse_args(["worker", "set", "worker-b:6", "paused"])
                rc = args.func(args)

            self.assertEqual(0, rc)
            set_args = run_job.call_args.args[0]
            self.assertEqual("paused", set_args.run)
            self.assertEqual("worker-b:2", set_args.workers)
            self.assertEqual(2, set_args.launch_workers_payload["workers"][0]["slots"])
            self.assertEqual(6, set_args.worker_snapshot_commit_payload["workers"][0]["slots"])


    def test_worker_set_named_run_delegates_to_coordinator(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote worker set\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch(
            "forge_lib.workers.status_can_delegate", return_value=True,
        ), mock.patch(
            "forge_lib.workers.run_interruptible_coordinator_process",
            return_value=remote_result,
        ) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_worker_set(self.worker_set_args(run="paused", worker="worker-b:6"))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote worker set\n",
            stdout.getvalue(),
        )
        host, remote, pid_file = run_mock.call_args.args
        self.assertEqual("pwa-5090", host)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", remote)
        self.assertIn("FORGE_COORDINATOR_RUNS=", remote)
        self.assertIn("forge worker set worker-b:6 paused", remote)
        self.assertIn("signal.SIG_DFL", remote)
        self.assertIn("kill -INT", remote)
        self.assertIn(pid_file, remote)

    def test_worker_set_interrupt_cancels_remote_coordinator_process(self) -> None:
        proc = mock.Mock()
        proc.communicate.side_effect = [KeyboardInterrupt, ("", "")]
        with (
            mock.patch("forge_lib.workers.subprocess.Popen", return_value=proc),
            mock.patch("forge_lib.workers.cancel_remote_coordinator_process") as cancel,
        ):
            with self.assertRaises(KeyboardInterrupt):
                forge.run_interruptible_coordinator_process(
                    "pwa-5090",
                    "remote command",
                    "/tmp/control.pid",
                )

        cancel.assert_called_once_with("pwa-5090", "/tmp/control.pid")
        proc.communicate.assert_has_calls([mock.call(), mock.call(timeout=forge.REMOTE_PROCESS_STOP_GRACE_SECONDS)])

    def test_worker_del_drains_worker_and_releases_reserved_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": ["worker-a", "worker-b"],
            })
            manifest = forge.load_manifest(run / "manifest.json")
            claimed = forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            self.assertEqual("task_0", claimed["task"])
            self.assertTrue(forge.task_claim_path(manifest, manifest["tasks"][0]).exists())

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_worker_del(self.worker_del_args(run="active", worker="worker-b"))

            self.assertEqual(0, rc)
            self.assertTrue(forge.worker_drain_path(manifest, "worker-b").exists())
            self.assertFalse(forge.task_claim_path(manifest, manifest["tasks"][0]).exists())
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-a", "profile": "worker-a", "local": False}],
                },
                forge.read_json(run / "workers.json"),
            )
            self.assertIn("worker del: active worker-b released=1", stdout.getvalue())

    def test_worker_del_can_keep_reserved_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": ["worker-a", "worker-b"],
            })
            manifest = forge.load_manifest(run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_worker_del(
                        self.worker_del_args(run="active", worker="worker-b", keep_claims=True)
                    )

            self.assertEqual(0, rc)
            self.assertTrue(forge.worker_drain_path(manifest, "worker-b").exists())
            self.assertTrue(forge.task_claim_path(manifest, manifest["tasks"][0]).exists())
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-a", "profile": "worker-a", "local": False}],
                },
                forge.read_json(run / "workers.json"),
            )

    def test_worker_del_without_run_uses_single_active_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "paused", failed=None)
            active_run = self.write_retry_run(runs, "active", failed=None)
            manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_worker_del(self.worker_del_args(run=None, worker="worker-b"))

            self.assertEqual(0, rc)
            self.assertTrue(forge.worker_drain_path(manifest, "worker-b").exists())
            self.assertFalse(forge.task_claim_path(manifest, manifest["tasks"][0]).exists())
            self.assertIn("worker del: active worker-b released=1", stdout.getvalue())

    def test_worker_del_without_run_delegates_explicit_active_run(self) -> None:
        status_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout=self.delegated_status_payload(),
            stderr="",
        )
        del_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote worker del\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch("forge.subprocess.run", return_value=status_result) as run_mock, mock.patch(
            "forge_lib.workers.run_interruptible_coordinator_process",
            return_value=del_result,
        ) as control_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_worker_del(self.worker_del_args(run=None, worker="worker-b"))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote worker del\n",
            stdout.getvalue(),
        )
        status_command = run_mock.call_args_list[0].args[0][-1]
        del_command = control_mock.call_args.args[1]
        self.assertIn("forge status --json", status_command)
        self.assertIn("forge worker del worker-b remote-run", del_command)

    def test_del_command_with_worker_drains_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "active", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    args = forge.build_parser().parse_args(["del", "--worker", "worker-b", "active"])
                    rc = args.func(args)

            self.assertEqual(0, rc)
            self.assertTrue(forge.worker_drain_path(manifest, "worker-b").exists())
            self.assertFalse(forge.task_claim_path(manifest, manifest["tasks"][0]).exists())
            self.assertIn("worker del: active worker-b released=1", stdout.getvalue())

    def test_del_command_worker_rejects_delete_options(self) -> None:
        for option in ("--done", "--all", "--force"):
            with self.subTest(option=option):
                args = forge.build_parser().parse_args(["del", "--worker", "worker-b", option])
                with self.assertRaises(SystemExit):
                    args.func(args)

    def test_plan_rejects_duplicate_output_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            result = self.run_cli(
                "plan",
                "--name", "duplicate-output",
                "--shards", "2",
                "--work-dir", str(root),
                "--out", str(root / "manifest.json"),
                "--command-template", "printf '{index}\\n' > {output}",
                "--output-template", str(root / "out.txt"),
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("duplicate output path", result.stderr)

    def test_plan_can_skip_git_requirement_for_ad_hoc_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"

            result = self.run_cli(
                "plan",
                "--name", "ad-hoc",
                "--shards", "1",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf ok > {output}",
                "--output-template", str(root / "out.txt"),
                "--no-git-requirement",
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertNotIn("git", payload["requirements"])

    def test_plan_accepts_user_facing_shards_option(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"

            result = self.run_cli(
                "plan",
                "--name", "simple-plan",
                "--shards", "1",
                "--work-dir", str(root),
                "--manifest", str(manifest),
                "--command", "printf ok > {output_dir}/out.txt",
                "--output-dir", str(root / "outputs"),
                "--output-template", "{output_dir}/out.txt",
                "--progress-regex", r"done=(?P<done>\d+)",
                "--no-git-requirement",
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual("printf ok > {output_dir}/out.txt", payload["tasks"][0]["command"])
            self.assertEqual(str(root / "outputs"), payload["vars"]["output_dir"])
            self.assertEqual(str(root / "outputs" / "out.txt"), payload["tasks"][0]["outputs"][0])
            self.assertEqual(r"done=(?P<done>\d+)", payload["tasks"][0]["progress"]["log_regex"])

    def test_plan_allows_deferred_output_dir_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"

            result = self.run_cli(
                "plan",
                "--name", "deferred-output",
                "--shards", "2",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf ok > {output}",
                "--output-template", "{output_dir}/{task_id}.txt",
                "--no-git-requirement",
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(
                ["{output_dir}/task_0000.txt", "{output_dir}/task_0001.txt"],
                [task["outputs"][0] for task in payload["tasks"]],
            )
            materialized = forge.materialize_manifest(
                payload,
                run_name="deferred-output-run",
                run_dir=root / "runs" / "deferred-output-run",
            )
            self.assertEqual(
                str(root / "runs" / "deferred-output-run" / "outputs" / "task_0000.txt"),
                materialized["tasks"][0]["outputs"][0],
            )

    def test_write_json_replaces_existing_file_without_temp_leftover(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            forge.write_json(path, {"old": True})

            forge.write_json(path, {"new": True})

            self.assertEqual({"new": True}, json.loads(path.read_text(encoding="utf-8")))
            self.assertEqual([], list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_plan_declares_progress_metadata_from_line_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            source = root / "source.txt"
            source.write_text("a\nb\nc\nd\ne\n", encoding="utf-8")
            subprocess.run(["git", "add", "source.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "manifest.json"
            result = self.run_cli(
                "plan",
                "--name", "progress",
                "--shards", "2",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf ok > {output}",
                "--output-template", str(root / "out" / "task.{index}.txt"),
                "--progress-unit", "rows",
                "--progress-total-lines", str(source),
                "--progress-log-regex", r"processed=(?P<done>\d+)",
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            tasks = json.loads(manifest.read_text(encoding="utf-8"))["tasks"]
            self.assertEqual([3, 2], [task["progress"]["total"] for task in tasks])
            self.assertEqual("rows", tasks[0]["progress"]["unit"])

    def test_negative_timeout_is_not_passed_to_subprocess(self) -> None:
        self.assertIsNone(forge.bounded_timeout(-1))
        self.assertIsNone(forge.bounded_timeout(0))
        self.assertEqual(15.0, forge.bounded_timeout(None, 15.0))

    def test_run_task_removes_stale_running_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "manifest.json"
            plan = self.run_cli(
                "plan",
                "--name", "stale-running",
                "--shards", "1",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf ok > {output}",
                "--output-template", str(root / "out.txt"),
            )
            self.assertEqual(0, plan.returncode)

            state = root / "state"
            state.mkdir(exist_ok=True)
            running = state / "task_0000.running.json"
            running.write_text(json.dumps({"task": "task_0000"}), encoding="utf-8")

            run = self.run_cli("run-task", "--manifest", str(manifest), "--index", "0")

            self.assertEqual("", run.stderr)
            self.assertEqual(0, run.returncode)
            self.assertFalse(running.exists())

    def test_wrong_git_commit_rejects_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            manifest = {
                "schema": "forge.task.v1",
                "name": "bad",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "python_min": "3.11",
                    "git_min": "2.38",
                    "git": {
                        "repo": str(root),
                        "commit": "0" * 40,
                        "require_clean": False,
                    },
                    "inputs": [],
                },
                "tasks": [
                    {
                        "id": "task_0000",
                        "index": 0,
                        "command": "true",
                        "outputs": [str(root / "out.txt")],
                    }
                ],
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            result = self.run_cli("run-task", "--manifest", str(path), "--index", "0")
            self.assertNotEqual(0, result.returncode)
            self.assertIn("git commit", result.stderr)

    def test_sync_git_requirement_honors_require_clean_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            (root / "dirty.txt").write_text("dirty\n", encoding="utf-8")

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                forge.sync_git_requirement({
                    "name": "repo",
                    "path": str(root),
                    "commit": commit,
                    "require_clean": False,
                })

            self.assertIn(commit, stdout.getvalue())

    def test_profile_path_map_translates_manifest_strings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            worker = root / "worker"
            worker.mkdir()
            subprocess.run(["git", "init", "-b", "main"], cwd=worker, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=worker, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=worker, check=True)
            (worker / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=worker, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=worker, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(
                ["git", "-C", str(worker), "rev-parse", "HEAD"],
                text=True,
            ).strip()

            coordinator_root = "/coordinator/project"
            manifest = {
                "schema": "forge.task.v1",
                "name": "mapped",
                "work_dir": coordinator_root,
                "state_dir": f"{coordinator_root}/state",
                "log_dir": f"{coordinator_root}/logs",
                "path_maps": {
                    "worker": {
                        coordinator_root: str(worker),
                    }
                },
                "requirements": {
                    "python_min": "3.11",
                    "git_min": "2.38",
                    "git": {
                        "repo": coordinator_root,
                        "commit": commit,
                        "require_clean": False,
                    },
                    "inputs": [
                        {
                            "path": f"{coordinator_root}/input.txt",
                            "sha256": "",
                        }
                    ],
                },
                "tasks": [
                    {
                        "id": "task_0000",
                        "index": 0,
                        "command": "printf mapped > {output}",
                        "outputs": [f"{coordinator_root}/out.txt"],
                    }
                ],
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            result = self.run_cli(
                "run-task",
                "--manifest", str(path),
                "--profile", "worker",
                "--index", "0",
            )
            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            self.assertEqual("mapped", (worker / "out.txt").read_text(encoding="utf-8"))

    def test_profile_specific_inputs_are_checked_only_for_active_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common = root / "common.txt"
            common.write_text("common\n", encoding="utf-8")
            mac_input = root / "mac.bin"
            manifest = {
                "schema": "forge.task.v1",
                "name": "profile-inputs",
                "work_dir": str(root),
                "requirements": {
                    "inputs": [
                        {
                            "path": str(common),
                            "sha256": forge.sha256_file(common),
                        },
                        {
                            "path": str(mac_input),
                            "sha256": "0" * 64,
                            "profile": "mac",
                        },
                    ],
                },
                "tasks": [],
            }

            forge.check_requirements(manifest)
            self.assertEqual(["--profile", "mac"], forge.worker_profile_args(manifest, {"profile": "mac"}))

            with self.assertRaises(SystemExit) as missing:
                forge.check_requirements(forge.apply_profile(manifest, "mac"))
            self.assertIn("required input missing", str(missing.exception))

            mac_input.write_text("mac\n", encoding="utf-8")
            with self.assertRaises(SystemExit) as mismatch:
                forge.check_requirements(forge.apply_profile(manifest, "mac"))
            self.assertIn("checksum mismatch", str(mismatch.exception))

            manifest["requirements"]["inputs"][1]["sha256"] = forge.sha256_file(mac_input)
            forge.check_requirements(forge.apply_profile(manifest, "mac"))

    def test_add_command_accepts_path_only_requirement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tool = root / "tool"
            tool.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            tool.chmod(0o755)
            manifest = root / "manifest.json"
            forge.write_json(manifest, {
                "schema": "forge.task.v1",
                "name": "path-only-command",
                "work_dir": str(root),
                "requirements": {"inputs": []},
                "tasks": [],
            })

            result = self.run_cli(
                "add-command",
                "--manifest", str(manifest),
                "--name", "tool",
                "--path", str(tool),
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertEqual(str(tool), payload["requirements"]["commands"][0]["path"])

    def test_worker_claims_runs_and_syncs_to_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            subprocess.run(["git", "init", "-b", "main", str(coordinator)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=coordinator, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=coordinator, check=True)
            (coordinator / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=coordinator, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=coordinator, check=True, stdout=subprocess.DEVNULL)

            worker = root / "worker"
            subprocess.run(["git", "clone", str(coordinator), str(worker)], check=True, stdout=subprocess.DEVNULL)

            manifest = coordinator / "manifest.json"
            result = self.run_cli(
                "plan",
                "--name", "sync",
                "--shards", "2",
                "--work-dir", str(coordinator),
                "--out", str(manifest),
                "--command-template", "printf '{index}\\n' > {output}",
                "--output-template", str(coordinator / "out" / "shard.{index}.txt"),
                "--path-map", f"worker:{coordinator}={worker}",
            )
            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)

            work = self.run_cli(
                "work",
                "--manifest", str(manifest),
                "--coordinator", str(manifest),
                "--profile", "worker",
                "--worker", "test-worker",
                "--max-tasks", "2",
            )
            self.assertEqual("", work.stderr)
            self.assertEqual(0, work.returncode)

            status = self.run_cli("status", str(coordinator))
            self.assertEqual(0, status.returncode)
            self.assertIn("tasks=2/2", status.stdout)

            verify = self.run_cli("verify", str(coordinator))
            self.assertEqual("", verify.stderr)
            self.assertEqual(0, verify.returncode)
            self.assertEqual("0\n", (coordinator / "out" / "shard.0.txt").read_text(encoding="utf-8"))
            self.assertEqual("1\n", (coordinator / "out" / "shard.1.txt").read_text(encoding="utf-8"))

    def test_worker_execution_manifest_keeps_state_local(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "cache" / "manifest.json"
            manifest_path.parent.mkdir()
            manifest = {
                "schema": "forge.task.v1",
                "name": "worker-paths",
                "work_dir": "/home/petter/code/cpp/chess/nnue",
                "state_dir": "/home/petter/code/cpp/chess/forge/runs/job/state",
                "log_dir": "/home/petter/code/cpp/chess/forge/runs/job/logs",
                "claim_dir": "/home/petter/code/cpp/chess/forge/runs/job/claims",
                "path_maps": {
                    "mac": {
                        "/home/petter/code/cpp/chess/nnue": "/Users/pwahlman/code/cpp/chess/nnue_worker",
                    },
                },
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "command": "printf x > {output}",
                    "outputs": ["/home/petter/code/cpp/chess/nnue/runs/job/out.txt"],
                }],
            }

            local = forge.worker_execution_manifest(manifest, profile="mac", manifest_path=manifest_path)

            self.assertEqual("/Users/pwahlman/code/cpp/chess/nnue_worker", local["work_dir"])
            self.assertEqual(
                "/Users/pwahlman/code/cpp/chess/nnue_worker/runs/job/out.txt",
                local["tasks"][0]["outputs"][0],
            )
            worker_run = manifest_path.parent / "worker-state" / "worker-paths"
            self.assertEqual(str(worker_run / "state"), local["state_dir"])
            self.assertEqual(str(worker_run / "logs"), local["log_dir"])
            self.assertEqual(str(worker_run / "claims"), local["claim_dir"])

    def test_worker_execution_manifest_keeps_outputs_local(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "cache" / "manifest.json"
            manifest_path.parent.mkdir()
            coordinator_run = Path("/Users/pwahlman/Cloud-Drive/code/cpp/chess/forge/runs/job")
            coordinator_output = coordinator_run / "outputs" / "task_0000" / "out.txt"
            manifest = {
                "schema": "forge.task.v1",
                "name": "worker-output-paths",
                "work_dir": "~",
                "state_dir": str(coordinator_run / "state"),
                "log_dir": str(coordinator_run / "logs"),
                "claim_dir": str(coordinator_run / "claims"),
                "vars": {
                    "run_dir": str(coordinator_run),
                    "output_dir": str(coordinator_run / "outputs"),
                },
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "command": f"tool --out {coordinator_output}",
                    "outputs": [str(coordinator_output)],
                }],
            }

            local = forge.worker_execution_manifest(manifest, profile=None, manifest_path=manifest_path)

            worker_run = manifest_path.parent / "worker-state" / "worker-output-paths"
            worker_output = worker_run / "outputs" / "task_0000" / "out.txt"
            self.assertEqual(str(worker_run), local["vars"]["run_dir"])
            self.assertEqual(str(worker_run / "outputs"), local["vars"]["output_dir"])
            self.assertEqual(str(worker_output), local["tasks"][0]["outputs"][0])
            self.assertIn(str(worker_output), local["tasks"][0]["command"])
            self.assertNotIn(str(coordinator_run), local["tasks"][0]["command"])

    def test_worker_execution_manifest_isolates_state_by_run_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "cache" / "manifest.json"
            manifest_path.parent.mkdir()
            base = {
                "schema": "forge.task.v1",
                "work_dir": "~",
                "state_dir": "/coordinator/run/state",
                "log_dir": "/coordinator/run/logs",
                "claim_dir": "/coordinator/run/claims",
                "tasks": [{"id": "sprt_0064", "index": 64, "command": "true", "outputs": []}],
            }

            first = forge.worker_execution_manifest(
                {**base, "name": "first-run"},
                profile=None,
                manifest_path=manifest_path,
            )
            second = forge.worker_execution_manifest(
                {**base, "name": "second-run"},
                profile=None,
                manifest_path=manifest_path,
            )

            self.assertEqual(
                str(manifest_path.parent / "worker-state" / "first-run" / "state"),
                first["state_dir"],
            )
            self.assertEqual(
                str(manifest_path.parent / "worker-state" / "second-run" / "state"),
                second["state_dir"],
            )
            self.assertNotEqual(first["state_dir"], second["state_dir"])

    def test_worker_waits_when_no_claim_but_coordinator_has_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "idle-wait",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "claim_dir": str(root / "claims"),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                manifest=str(manifest_path),
                coordinator=str(manifest_path),
                profile=None,
                worker="worker-a",
                lease_seconds=600,
                max_tasks=1,
                idle_seconds=10.0,
                retry_failed=False,
                force=False,
                fail_fast=False,
            )
            claim = {"task": "task_0000", "claim": {"claim_id": "claim-1"}}

            with (
                mock.patch("forge_lib.tasks.check_coordinator_cli"),
                mock.patch("forge_lib.tasks.check_requirements"),
                mock.patch("forge_lib.tasks.claim_from_coordinator", side_effect=[None, claim]) as claim_mock,
                mock.patch("forge_lib.tasks.coordinator_has_claimable_or_active_work", return_value=True),
                mock.patch("forge_lib.tasks.time.sleep") as sleep_mock,
                mock.patch("forge_lib.tasks.heartbeat_on_coordinator"),
                mock.patch("forge_lib.tasks.run_task", return_value=0) as run_task_mock,
                mock.patch("forge_lib.tasks.sync_task_result"),
                mock.patch("forge_lib.tasks.release_on_coordinator"),
            ):
                rc = forge.cmd_work(args)

            self.assertEqual(0, rc)
            self.assertEqual(2, claim_mock.call_count)
            sleep_mock.assert_called_once()
            run_task_mock.assert_called_once()

    def test_run_task_terminates_child_group_on_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "signal-cleanup",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "sleep 60", "outputs": []}],
            }
            task = manifest["tasks"][0]

            class FakeProc:
                pid = 12345

                def __init__(self) -> None:
                    self.wait_calls = 0

                def wait(self, timeout=None):
                    self.wait_calls += 1
                    if timeout is None and self.wait_calls == 1:
                        raise forge.TaskSignalInterrupt(signal.SIGTERM)
                    return 0

            fake_proc = FakeProc()
            with (
                mock.patch("forge.subprocess.Popen", return_value=fake_proc),
                mock.patch("forge.os.getpgid", return_value=fake_proc.pid),
                mock.patch("forge.os.killpg") as killpg_mock,
                mock.patch("forge.platform.platform", return_value="test-platform"),
            ):
                rc = forge.run_task(manifest, task, force=False)

            self.assertEqual(128 + signal.SIGTERM, rc)
            killpg_mock.assert_called_with(fake_proc.pid, signal.SIGTERM)
            self.assertTrue(forge.task_fail_path(manifest, task).exists())
            self.assertFalse(forge.task_running_path(manifest, task).exists())
            log = forge.task_log_path(manifest, task).read_text(encoding="utf-8")
            self.assertIn("terminate signal", log)

    def test_run_task_soft_cancel_writes_done_without_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "soft-cancel",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "tasks": [
                    {
                        "id": "task_0000",
                        "index": 0,
                        "command": "sleep 60",
                        "outputs": [str(root / "missing.txt")],
                    }
                ],
            }
            task = manifest["tasks"][0]
            cancel = threading.Event()

            def cancel_soon() -> None:
                cancel.wait(0.1)
                cancel.set()

            thread = threading.Thread(target=cancel_soon)
            thread.start()
            try:
                rc = forge.run_task(manifest, task, force=False, cancel_event=cancel)
            finally:
                cancel.set()
                thread.join(timeout=1)

            self.assertEqual(0, rc)
            self.assertTrue(forge.task_done_path(manifest, task).exists())
            self.assertFalse(forge.task_fail_path(manifest, task).exists())
            self.assertTrue(forge.task_complete(manifest, task))
            payload = json.loads(forge.task_done_path(manifest, task).read_text(encoding="utf-8"))
            self.assertTrue(payload["soft_cancel"])
            self.assertTrue(payload["skipped"])
            self.assertEqual([], payload["outputs"])

    def test_wait_reports_timeout_until_tasks_are_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "manifest.json"
            plan = self.run_cli(
                "plan",
                "--name", "wait",
                "--shards", "2",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf '{index}\\n' > {output}",
                "--output-template", str(root / "out" / "shard.{index}.txt"),
            )
            self.assertEqual(0, plan.returncode)

            first = self.run_cli("run-task", "--manifest", str(manifest), "--index", "0")
            self.assertEqual(0, first.returncode)

            waiting = self.run_cli(
                "wait",
                "--manifest", str(manifest),
                "--interval-seconds", "0.01",
                "--timeout-seconds", "0.02",
            )
            self.assertEqual(124, waiting.returncode)
            self.assertIn("tasks=1/2", waiting.stdout)
            self.assertIn("pending=1", waiting.stdout)

            second = self.run_cli("run-task", "--manifest", str(manifest), "--index", "1")
            self.assertEqual(0, second.returncode)

            done = self.run_cli(
                "wait",
                "--manifest", str(manifest),
                "--interval-seconds", "0.01",
                "--timeout-seconds", "1",
            )
            self.assertEqual("", done.stderr)
            self.assertEqual(0, done.returncode)
            self.assertIn("tasks=2/2", done.stdout)

    def test_wait_fails_on_stale_launch_without_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "launch-stale"
            manifest_path = run / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "launch-stale",
                "work_dir": str(root),
                "state_dir": str(run / "state"),
                "log_dir": str(run / "logs"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(manifest_path, manifest)
            forge.write_json(forge.launch_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "launch-stale",
                "requested_epoch": 1000.0,
                "lease_seconds": 10,
            })
            args = argparse.Namespace(
                manifest=str(manifest_path),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command=None,
            )

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch("forge_lib.status.epoch", return_value=2000.0):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_wait(args)

            self.assertEqual(1, rc)
            self.assertIn("tasks=0/1", stdout.getvalue())
            self.assertIn("launch failed", stderr.getvalue())
            payload = json.loads((run / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual("run_launch_failed", payload["event"])
            self.assertEqual("failed", payload["state"])

    def test_wait_exits_cleanly_when_run_is_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "deleted-run"
            manifest_path = run / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "deleted-run",
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(manifest_path, manifest)
            args = argparse.Namespace(
                manifest=str(manifest_path),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=0.0,
                fail_on_fail=True,
                notify_command=None,
            )

            def delete_run(_seconds: float) -> None:
                shutil.rmtree(run)

            stderr = io.StringIO()
            with (
                mock.patch("forge_lib.status.time.sleep", side_effect=delete_run),
                contextlib.redirect_stderr(stderr),
            ):
                rc = forge.cmd_wait(args)

        self.assertEqual(130, rc)
        self.assertEqual("run deleted while waiting: deleted-run\n", stderr.getvalue())

    def test_wait_interrupt_stops_run_and_resets_active_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "wait-interrupt"
            output = run / "outputs" / "task.txt"
            task = {"id": "task", "index": 0, "command": "true", "outputs": [str(output)]}
            manifest = {
                "schema": "forge.task.v1",
                "name": run.name,
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "log_dir": str(run / "logs"),
                "claim_dir": str(run / "claims"),
                "tasks": [task],
            }
            run.mkdir(parents=True)
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{"host": "localhost", "local": True}],
            })
            forge.claim_task(
                manifest,
                worker="localhost",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            forge.write_json(forge.task_running_path(manifest, task), {"task": "task"})
            output.parent.mkdir(parents=True)
            output.write_text("partial", encoding="utf-8")
            args = argparse.Namespace(
                manifest=str(run / "manifest.json"),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.1,
                timeout_seconds=0.0,
                fail_on_fail=True,
                notify_command=None,
            )

            with (
                mock.patch("forge_lib.status.time.sleep", side_effect=KeyboardInterrupt),
                mock.patch("forge_lib.workers.kill_run_workers", return_value=1),
                self.assertRaises(KeyboardInterrupt),
            ):
                forge.cmd_wait(args)

            self.assertTrue(forge.stop_request_path(manifest).exists())
            self.assertFalse(output.exists())
            self.assertFalse(forge.task_claim_dir(manifest, task).exists())
            self.assertEqual("pending", forge.task_state(manifest, task, lease_seconds=600))

    def test_wait_sends_done_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "wait-done" / "manifest.json"
            manifest_payload = {
                "schema": "forge.task.v1",
                "name": "wait-done",
                "kind": "smoke",
                "description": "wait done check",
                "work_dir": str(root),
                "state_dir": str(root / "wait-done" / "state"),
                "log_dir": str(root / "wait-done" / "logs"),
                "claim_dir": str(root / "wait-done" / "claims"),
                "tasks": [],
            }
            forge.write_json(manifest, manifest_payload)
            forge.write_json(forge.launch_request_path(manifest_payload), {
                "schema": "forge.task.v1",
                "run": "wait-done",
                "pid": os.getpid(),
            })
            payloads: list[dict[str, object]] = []

            def capture_notify(command: str | None, payload: dict[str, object]) -> int:
                self.assertEqual("notify-me", command)
                payloads.append(payload)
                return 0

            args = argparse.Namespace(
                manifest=str(manifest),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command="notify-me",
            )
            with mock.patch("forge_lib.status.run_notify_command", capture_notify):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_wait(args)

            self.assertEqual(0, rc)
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("run_done", payload["event"])
            self.assertEqual("wait-done", payload["run"])
            self.assertEqual("ok", payload["state"])
            self.assertEqual(0, payload["rc"])
            self.assertEqual("smoke", payload["kind"])
            self.assertEqual("wait done check", payload["description"])
            self.assertEqual({"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}, payload["counts"])
            summary = json.loads((manifest.parent / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual("run_done", summary["event"])
            self.assertEqual("tasks=0/0", summary["status_line"])
            self.assertEqual(["tasks=0/0"], summary["display"]["fields"])
            self.assertFalse(forge.launch_request_path(manifest_payload).exists())

    def test_wait_completion_payload_keeps_progress_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-done"
            manifest_path = run / "manifest.json"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir()
            tasks = []
            for index in range(2):
                task_id = f"task_{index}"
                tasks.append({
                    "id": task_id,
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 2,
                        "log_regex": r"\[\s*(?P<done>\d+)\/(?P<total>\d+)\]",
                    },
                })
                forge.write_json(state / f"{task_id}.done.json", {"task": task_id, "host": "worker"})
                (logs / f"{task_id}.log").write_text(
                    f"[2/2] Elo  {10 - index * 20}.0 +/-  80.0 | LLR {0.1 - index * 0.2:.1f}/2.94 (  3%) | LOS 50.0% | draw   0.0%\n",
                    encoding="utf-8",
                )
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-done",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }

            payload = forge.run_completion_event_payload(
                manifest_path=manifest_path,
                manifest=manifest,
                event="run_done",
                state="ok",
                rc=0,
                elapsed_seconds=1.0,
                lease_seconds=600,
            )

            graph = payload["progress_graph"]
            self.assertEqual(4, graph["x_total"])
            series = {item["name"]: item["points"] for item in graph["series"]}
            self.assertEqual(
                [{"x": 0.0, "y": 0.0}, {"x": 2.0, "y": 0.1}, {"x": 4.0, "y": 0.0}],
                series["llr"],
            )
            self.assertAlmostEqual(0.0, series["elo"][0]["y"], places=1)
            self.assertAlmostEqual(0.0, series["elo"][-1]["y"], places=1)
            self.assertEqual([{"label": "sprt", "count": 2, "command": "true"}], payload["commands"])

    def test_wait_completes_sprt_when_aggregate_h1_is_reached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-h1"
            manifest_path = run / "manifest.json"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir()
            tasks = []
            for index in range(3):
                tasks.append({
                    "id": f"task_{index}",
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {"unit": "games", "total": 10},
                })
            manifest_payload = {
                "schema": "forge.task.v1",
                "name": "sprt-h1",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "claim_dir": str(run / "claims"),
                "tasks": tasks,
            }
            forge.write_json(manifest_path, manifest_payload)
            for index, llr in enumerate((1.2, 1.1)):
                task_id = f"task_{index}"
                forge.write_json(state / f"{task_id}.done.json", {"task": task_id, "host": "worker"})
                (logs / f"{task_id}.log").write_text(
                    f"[10/10] Elo  300.0 +/-  50.0 | LLR {llr:.1f}/2.20 (  50%) | LOS 99.0% | draw   1.0%\n",
                    encoding="utf-8",
                )

            args = argparse.Namespace(
                manifest=str(manifest_path),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command=None,
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_wait(args)

            self.assertEqual(0, rc)
            self.assertIn("aggregate H1 reached", stdout.getvalue())
            summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual("run_done", summary["event"])
            self.assertEqual("ok", summary["state"])
            self.assertEqual({"done": 3, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 0}, summary["counts"])
            self.assertIn("games=20/30", summary["status_line"])
            stop = json.loads((state / "stop.json").read_text(encoding="utf-8"))
            self.assertEqual("aggregate_sprt_decision", stop["reason"])
            self.assertEqual("H1", stop["decision"]["side"])
            skipped = json.loads((state / "task_2.done.json").read_text(encoding="utf-8"))
            self.assertTrue(skipped["soft_cancel"])
            self.assertEqual("aggregate_sprt_decision", skipped["skip_reason"])
            self.assertEqual("H1", skipped["decision"]["side"])

    def test_wait_sends_fail_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "wait-fail" / "manifest.json"
            out = root / "wait-fail" / "out.txt"
            forge.write_json(manifest, {
                "schema": "forge.task.v1",
                "name": "wait-fail",
                "work_dir": str(root),
                "state_dir": str(root / "wait-fail" / "state"),
                "log_dir": str(root / "wait-fail" / "logs"),
                "claim_dir": str(root / "wait-fail" / "claims"),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "false",
                        "outputs": [str(out)],
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "sleep 60",
                        "outputs": [],
                    },
                ],
            })
            loaded = forge.load_manifest(manifest)
            forge.write_json(forge.task_fail_path(loaded, loaded["tasks"][0]), {
                "worker": {"host": "worker-a"},
                "rc": 1,
            })
            running_task = loaded["tasks"][1]
            claim_payload = {
                "task": "task_1",
                "claim_id": "claim-1",
                "worker": "worker-b",
                "claimed_epoch": forge.epoch(),
                "heartbeat_epoch": forge.epoch(),
                "lease_seconds": 600,
            }
            forge.write_json(forge.task_claim_path(loaded, running_task), claim_payload)
            forge.write_json(forge.task_heartbeat_path(loaded, running_task), claim_payload)
            forge.write_json(forge.task_running_path(loaded, running_task), {
                "task": "task_1",
                "worker": "worker-b",
            })
            payloads: list[dict[str, object]] = []

            def capture_notify(command: str | None, payload: dict[str, object]) -> int:
                self.assertEqual("notify-me", command)
                payloads.append(payload)
                return 0

            args = argparse.Namespace(
                manifest=str(manifest),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command="notify-me",
            )
            with (
                mock.patch("forge_lib.status.run_notify_command", capture_notify),
                mock.patch("forge_lib.workers.kill_run_workers", return_value=2) as kill_workers,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_wait(args)

            self.assertEqual(1, rc)
            kill_workers.assert_called_once_with(manifest.parent)
            self.assertTrue(forge.stop_request_path(loaded).exists())
            self.assertFalse(forge.task_claim_dir(loaded, running_task).exists())
            self.assertFalse(forge.task_running_path(loaded, running_task).exists())
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("run_fail", payload["event"])
            self.assertEqual("wait-fail", payload["run"])
            self.assertEqual("failed", payload["state"])
            self.assertEqual(1, payload["rc"])
            self.assertEqual(1, payload["counts"]["fail"])
            self.assertEqual(1, payload["counts"]["pending"])
            self.assertEqual(0, payload["active_count"])
            summary = json.loads((manifest.parent / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual("run_fail", summary["event"])
            self.assertEqual("failed", summary["state"])
            self.assertIn("fail=1", summary["status_line"])

    def test_wait_sends_stopped_notification_after_drain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "wait-stopped" / "manifest.json"
            forge.write_json(manifest, {
                "schema": "forge.task.v1",
                "name": "wait-stopped",
                "work_dir": str(root),
                "state_dir": str(root / "wait-stopped" / "state"),
                "log_dir": str(root / "wait-stopped" / "logs"),
                "claim_dir": str(root / "wait-stopped" / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": []},
                ],
            })
            loaded = forge.load_manifest(manifest)
            forge.write_json(forge.task_done_path(loaded, loaded["tasks"][0]), {"task": "task_0"})
            forge.write_json(forge.stop_request_path(loaded), {"run": "wait-stopped"})
            payloads: list[dict[str, object]] = []

            def capture_notify(command: str | None, payload: dict[str, object]) -> int:
                self.assertEqual("notify-me", command)
                payloads.append(payload)
                return 0

            args = argparse.Namespace(
                manifest=str(manifest),
                profile=None,
                lease_seconds=600,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command="notify-me",
            )
            with mock.patch("forge_lib.status.run_notify_command", capture_notify):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_wait(args)

            self.assertEqual(130, rc)
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("run_stopped", payload["event"])
            self.assertEqual("wait-stopped", payload["run"])
            self.assertEqual("stopped", payload["state"])
            self.assertEqual(130, payload["rc"])
            self.assertEqual(1, payload["counts"]["done"])
            self.assertEqual(1, payload["counts"]["pending"])
            summary = json.loads((manifest.parent / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual("run_stopped", summary["event"])
            self.assertEqual("stopped", summary["state"])

    def test_claim_is_atomic_status_visible_and_released(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)

            manifest = root / "manifest.json"
            plan = self.run_cli(
                "plan",
                "--name", "claim",
                "--shards", "1",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf x > {output}",
                "--output-template", str(root / "out.txt"),
            )
            self.assertEqual(0, plan.returncode)

            claim = self.run_cli(
                "claim",
                "--manifest", str(manifest),
                "--worker", "worker-a",
                "--json",
            )
            self.assertEqual("", claim.stderr)
            self.assertEqual(0, claim.returncode)
            claim_payload = json.loads(claim.stdout)
            self.assertEqual("task_0000", claim_payload["task"])
            claim_id = claim_payload["claim"]["claim_id"]

            second = self.run_cli(
                "claim",
                "--manifest", str(manifest),
                "--worker", "worker-b",
                "--json",
            )
            self.assertEqual(0, second.returncode)
            self.assertIsNone(json.loads(second.stdout)["task"])

            status = self.run_cli("status", str(root))
            self.assertEqual(0, status.returncode)
            self.assertIn("active: 1", status.stdout)
            self.assertIn("worker-a [claimed] shard=0", status.stdout)

            heartbeat = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--progress-json", '{"done":42,"total":1000,"rate":2}',
            )
            self.assertEqual("", heartbeat.stderr)
            self.assertEqual(0, heartbeat.returncode)

            status_with_progress = self.run_cli("status", str(root))
            self.assertEqual(0, status_with_progress.returncode)
            self.assertIn("worker-a [claimed] shard=0", status_with_progress.stdout)
            self.assertNotIn("worker-a [claimed] shard=0 [subtask=42/1000", status_with_progress.stdout)

            unknown_total = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--progress-json", '{"done":43}',
            )
            self.assertEqual("", unknown_total.stderr)
            self.assertEqual(0, unknown_total.returncode)
            status_unknown_total = self.run_cli("status", str(root))
            self.assertEqual(0, status_unknown_total.returncode)
            self.assertIn("worker-a [claimed] shard=0", status_unknown_total.stdout)

            wrong_claim = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--claim-id", "not-the-claim",
            )
            self.assertNotEqual(0, wrong_claim.returncode)
            self.assertIn("claim id mismatch", wrong_claim.stderr)

            wrong_worker = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-b",
            )
            self.assertNotEqual(0, wrong_worker.returncode)
            self.assertIn("claimed by worker-a", wrong_worker.stderr)

            wrong_release = self.run_cli(
                "release",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--claim-id", "not-the-claim",
            )
            self.assertNotEqual(0, wrong_release.returncode)
            status = self.run_cli("status", str(root))
            self.assertIn("worker-a [claimed] shard=0", status.stdout)

            release = self.run_cli(
                "release",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--claim-id", claim_id,
            )
            self.assertEqual(0, release.returncode)
            status = self.run_cli("status", str(root))
            self.assertIn("pending: 1", status.stdout)

    def test_active_retry_status_wins_over_stale_fail_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "retry",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            run = root / "runs" / "retry"
            run.mkdir(parents=True)
            forge.write_json(run / "manifest.json", manifest)
            claim_epoch = forge.epoch()
            fail_payload = {"schema": "forge.task.v1", "task": "task_0", "index": 0, "worker": "old", "rc": 1}
            running_payload = {"schema": "forge.task.v1", "task": "task_0", "index": 0, "worker": "new"}
            claim_payload = {
                "schema": "forge.task.v1",
                "task": "task_0",
                "index": 0,
                "claim_id": "claim",
                "worker": "new",
                "claimed_epoch": claim_epoch,
                "heartbeat_epoch": claim_epoch,
                "lease_seconds": 600,
            }
            forge.write_json(forge.task_fail_path(manifest, task), fail_payload)
            forge.write_json(forge.task_running_path(manifest, task), running_payload)
            forge.task_claim_dir(manifest, task).mkdir(parents=True)
            forge.write_json(forge.task_claim_path(manifest, task), claim_payload)
            forge.write_json(forge.task_heartbeat_path(manifest, task), claim_payload)

            status = self.run_cli("status", str(run), "--verbose")

            self.assertEqual(0, status.returncode)
            self.assertIn("active: 1", status.stdout)
            self.assertIn("new shard=0", status.stdout)
            self.assertIn("command: true", status.stdout)
            self.assertNotIn("pending: 0", status.stdout)
            self.assertNotIn("fail: 0", status.stdout)

    def test_newer_fail_marker_wins_over_old_running_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "masked-fail",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            run = root / "runs" / "masked-fail"
            run.mkdir(parents=True)
            forge.write_json(run / "manifest.json", manifest)
            claim_payload = {
                "schema": "forge.task.v1",
                "task": "task_0",
                "index": 0,
                "claim_id": "claim",
                "worker": "worker-a",
                "claimed_epoch": forge.epoch(),
                "heartbeat_epoch": forge.epoch(),
                "lease_seconds": 600,
                "attempt": 1,
            }
            running_payload = {
                "schema": "forge.task.v1",
                "task": "task_0",
                "index": 0,
                "worker": "worker-a",
                "attempt": 1,
            }
            fail_payload = {
                "schema": "forge.task.v1",
                "task": "task_0",
                "index": 0,
                "worker": "worker-a",
                "rc": 143,
                "attempt": 1,
            }
            forge.task_claim_dir(manifest, task).mkdir(parents=True)
            forge.write_json(forge.task_claim_path(manifest, task), claim_payload)
            forge.write_json(forge.task_heartbeat_path(manifest, task), claim_payload)
            forge.write_json(forge.task_running_path(manifest, task), running_payload)
            forge.write_json(forge.task_fail_path(manifest, task), fail_payload)

            status = self.run_cli("status", str(run), "--verbose")

            self.assertEqual(1, status.returncode)
            self.assertIn("tasks=0/1 fail=1", status.stdout)
            self.assertNotIn("active: 1", status.stdout)
            self.assertIn("fail: 1", status.stdout)
            self.assertIn("worker-a [1]: 0", status.stdout)

    def test_verbose_status_splits_stale_rows_from_active_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "stale-active",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": []},
                    {"id": "task_2", "index": 2, "command": "true", "outputs": []},
                ],
            }
            run = root / "runs" / "stale-active"
            run.mkdir(parents=True)
            forge.write_json(run / "manifest.json", manifest)
            current = forge.epoch()
            old = current - 1000.0
            for index, task in enumerate(manifest["tasks"]):
                worker = "worker-live" if index == 0 else "worker-stale"
                timestamp = current if index == 0 else old
                claim_payload = {
                    "schema": "forge.task.v1",
                    "task": task["id"],
                    "index": index,
                    "claim_id": f"claim-{index}",
                    "worker": worker,
                    "claimed_epoch": timestamp,
                    "heartbeat_epoch": timestamp,
                    "lease_seconds": 600,
                    "attempt": 1,
                }
                forge.task_claim_dir(manifest, task).mkdir(parents=True)
                forge.write_json(forge.task_claim_path(manifest, task), claim_payload)
                forge.write_json(forge.task_heartbeat_path(manifest, task), claim_payload)
                forge.write_json(forge.task_running_path(manifest, task), {
                    "schema": "forge.task.v1",
                    "task": task["id"],
                    "index": index,
                    "worker": worker,
                    "attempt": 1,
                })

            status = self.run_cli("status", str(run), "--verbose")

            self.assertEqual(0, status.returncode)
            self.assertIn("tasks=0/3 stale=2", status.stdout)
            self.assertIn("active: 1", status.stdout)
            self.assertNotIn("active: 3", status.stdout)
            self.assertIn("worker-live shard=0", status.stdout)
            self.assertIn("stale: 2", status.stdout)
            self.assertIn("worker-stale [stale] shard=1", status.stdout)
            self.assertIn("worker-stale [stale] shard=2", status.stdout)

    def test_failed_active_run_state_is_current(self) -> None:
        summary = {
            "tasks": 3,
            "counts": {
                "done": 0,
                "fail": 1,
                "running": 1,
                "claimed": 0,
                "stale": 1,
                "pending": 0,
            },
        }
        self.assertEqual("current", forge.run_state(summary))

        summary["counts"] = {
            "done": 0,
            "fail": 1,
            "running": 0,
            "claimed": 0,
            "stale": 0,
            "pending": 0,
        }
        self.assertEqual("failed", forge.run_state(summary))

    def test_malformed_claim_is_claimed_until_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "bad-claim",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = forge.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            forge.task_claim_path(manifest, task).write_text("{broken", encoding="utf-8")

            forge.clear_state_diagnostics()
            self.assertEqual("claimed", forge.task_state(manifest, task, lease_seconds=600))
            self.assertIn("claim.json", "\n".join(forge.state_diagnostics()))

            os.utime(claim, (100.0, 100.0))
            with mock.patch("forge_lib.tasks.epoch", return_value=1000.0):
                self.assertEqual("stale", forge.task_state(manifest, task, lease_seconds=1))

    def test_malformed_heartbeat_is_diagnostic_not_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "bad-heartbeat",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = forge.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            heartbeat_epoch = forge.epoch()
            forge.write_json(forge.task_claim_path(manifest, task), {
                "task": "task_0",
                "worker": "worker-a",
                "claimed_epoch": heartbeat_epoch,
                "heartbeat_epoch": heartbeat_epoch,
            })
            forge.task_heartbeat_path(manifest, task).write_text("{broken", encoding="utf-8")

            forge.clear_state_diagnostics()
            self.assertEqual("claimed", forge.task_state(manifest, task, lease_seconds=600))
            self.assertIn("heartbeat.json", "\n".join(forge.state_diagnostics()))

    def test_verbose_status_shows_state_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "bad-claim"
            manifest = {
                "schema": "forge.task.v1",
                "name": "bad-claim",
                "work_dir": str(root),
                "state_dir": str(run / "state"),
                "log_dir": str(run / "logs"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            run.mkdir(parents=True)
            forge.write_json(run / "manifest.json", manifest)
            task = manifest["tasks"][0]
            claim = forge.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            forge.task_claim_path(manifest, task).write_text("{broken", encoding="utf-8")

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_status(
                    argparse.Namespace(
                        run=str(run),
                        all=False,
                        done=False,
                        failed=False,
                        json=False,
                        no_color=True,
                        lease_seconds=600,
                        verbose=True,
                        show_done=False,
                    )
                )

            self.assertEqual(0, rc)
            text = stdout.getvalue()
            self.assertIn("diagnostics:", text)
            self.assertIn("claim.json", text)

    def test_zombie_claim_expires_and_can_be_reclaimed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "zombie-claim",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = forge.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            old = forge.epoch() - 30
            os.utime(claim, (old, old))

            self.assertEqual("stale", forge.task_state(manifest, task, lease_seconds=1))
            reclaimed = forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=1,
                retry_failed=False,
            )

            self.assertIsNotNone(reclaimed)
            self.assertEqual("task_0000", reclaimed["task"])
            payload = forge.read_json(forge.task_claim_path(manifest, task))
            self.assertEqual("worker-b", payload["worker"])

    def test_stale_running_marker_does_not_block_reclaim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "stale-running",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            running = forge.task_running_path(manifest, task)
            running.parent.mkdir(parents=True)
            running.write_text(json.dumps({"task": "task_0000"}), encoding="utf-8")
            old = forge.epoch() - 30
            os.utime(running, (old, old))

            self.assertEqual("stale", forge.task_state(manifest, task, lease_seconds=1))
            reclaimed = forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=1,
                retry_failed=False,
            )

            self.assertIsNotNone(reclaimed)
            self.assertFalse(running.exists())

    def test_worker_claims_newest_preferred_recalibrated_task_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "adaptive-order",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [
                    {"id": "task_0001", "index": 1, "claim_worker": "worker-a", "command": "true", "outputs": []},
                    {"id": "task_0100", "index": 100, "claim_worker": "worker-a", "command": "true", "outputs": []},
                ],
            }

            claimed = forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

        self.assertEqual("task_0100", claimed["task"])

    def test_task_execution_checksum_excludes_routing_but_tracks_execution(self) -> None:
        manifest = {
            "schema": "forge.task.v1",
            "kind": "label",
            "name": "checksum-test",
            "created_at": "2026-01-01T00:00:00+0000",
            "work_dir": "/work",
            "vars": {"depth": 12},
            "requirements": {"repos": [{"commit": "abc123"}]},
            "tasks": [
                {
                    "id": "task_0000",
                    "index": 0,
                    "claim_worker": "worker-a",
                    "command": "label --depth {depth}",
                    "outputs": ["/output/0.bullet"],
                },
                {"id": "task_0001", "index": 1, "command": "other"},
            ],
        }
        task = manifest["tasks"][0]
        expected = forge.task_execution_checksum(manifest, task)

        routed = copy.deepcopy(manifest)
        routed["created_at"] = "2026-02-01T00:00:00+0000"
        routed["tasks"][0]["claim_worker"] = "worker-b"
        routed["tasks"][1]["command"] = "changed other task"
        self.assertEqual(
            expected,
            forge.task_execution_checksum(routed, routed["tasks"][0]),
        )

        changed_command = copy.deepcopy(manifest)
        changed_command["tasks"][0]["command"] = "label --depth 13"
        self.assertNotEqual(
            expected,
            forge.task_execution_checksum(changed_command, changed_command["tasks"][0]),
        )
        changed_requirement = copy.deepcopy(manifest)
        changed_requirement["requirements"]["repos"][0]["commit"] = "def456"
        self.assertNotEqual(
            expected,
            forge.task_execution_checksum(changed_requirement, changed_requirement["tasks"][0]),
        )

    def test_verify_prefers_task_execution_checksum_over_manifest_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "task.out"
            output.write_text("valid output", encoding="utf-8")
            manifest = {
                "schema": "forge.task.v1",
                "kind": "test",
                "name": "task-checksum",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "requirements": {},
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "claim_worker": "worker-b",
                    "command": "true",
                    "outputs": [str(output)],
                }],
            }
            task = manifest["tasks"][0]
            done_path = forge.task_done_path(manifest, task)
            done_path.parent.mkdir()
            payload = {
                "task": task["id"],
                "manifest_sha256": "obsolete-manifest-checksum",
                "task_execution_sha256": forge.task_execution_checksum(manifest, task),
                "outputs": [forge.file_record(output)],
            }
            forge.write_json(done_path, payload)

            forge.verify_manifest(manifest, verify_requirements=False)

            payload["task_execution_sha256"] = "wrong-task-checksum"
            forge.write_json(done_path, payload)
            with self.assertRaisesRegex(SystemExit, "task execution checksum mismatch"):
                forge.verify_manifest(manifest, verify_requirements=False)

    def test_coordinator_result_payload_stamps_task_execution_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator_output = root / "coordinator" / "task.out"
            local_output = root / "worker" / "task.out"
            local_output.parent.mkdir(parents=True)
            local_output.write_text("result", encoding="utf-8")
            coordinator_task = {
                "id": "task_0000",
                "index": 0,
                "claim_worker": "worker-a",
                "command": "true",
                "outputs": [str(coordinator_output)],
            }
            local_task = dict(coordinator_task, outputs=[str(local_output)])
            coordinator_manifest = {
                "schema": "forge.task.v1",
                "work_dir": str(root / "coordinator"),
                "state_dir": str(root / "coordinator" / "state"),
                "log_dir": str(root / "coordinator" / "logs"),
                "requirements": {},
                "tasks": [coordinator_task],
            }
            local_manifest = {
                **coordinator_manifest,
                "work_dir": str(root / "worker"),
                "state_dir": str(root / "worker" / "state"),
                "log_dir": str(root / "worker" / "logs"),
                "tasks": [local_task],
            }

            payload, _ = forge.coordinator_result_payload(
                coordinator_manifest=coordinator_manifest,
                local_payload={"host": "worker-a"},
                local_manifest=local_manifest,
                coordinator_task=coordinator_task,
                local_task=local_task,
                worker="worker-a",
                state="done",
            )

            self.assertEqual(
                forge.task_execution_checksum(coordinator_manifest, coordinator_task),
                payload["task_execution_sha256"],
            )
            self.assertEqual(
                forge.task_execution_checksum(local_manifest, local_task),
                payload["worker"]["profile_task_execution_sha256"],
            )

    def test_verify_rejects_done_task_without_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
            manifest = {
                "schema": "forge.task.v1",
                "name": "no-output",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "python_min": "3.11",
                    "git_min": "2.38",
                    "git": {
                        "repo": str(root),
                        "commit": commit,
                        "require_clean": False,
                    },
                    "inputs": [],
                },
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                }],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            run = self.run_cli("run-task", "--manifest", str(manifest_path), "--index", "0")
            self.assertEqual(0, run.returncode)
            verify = self.run_cli("verify", str(root))
            self.assertNotEqual(0, verify.returncode)
            self.assertIn("no outputs declared", verify.stderr)

    def test_post_deploy_verify_skips_worker_local_requirements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "worker-local-requirements",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "requirements": {
                    "repos": [{
                        "name": "worker-repo",
                        "path": str(root / "only-exists-on-worker"),
                        "commit": "abc1234",
                    }],
                },
                "tasks": [],
            }
            (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaises(SystemExit):
                forge.verify_manifest(manifest)

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_verify(argparse.Namespace(run=str(root), skip_requirements=True))

            self.assertEqual(0, rc)
            self.assertEqual("ok\n", stdout.getvalue())

    def test_worker_releases_claim_after_sync_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            subprocess.run(["git", "init", "-b", "main", str(coordinator)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=coordinator, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=coordinator, check=True)
            (coordinator / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=coordinator, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=coordinator, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=coordinator, text=True).strip()

            worker = root / "worker"
            subprocess.run(["git", "clone", str(coordinator), str(worker)], check=True, stdout=subprocess.DEVNULL)
            (coordinator / "blocked").write_text("not a directory\n", encoding="utf-8")

            manifest = {
                "schema": "forge.task.v1",
                "name": "sync-fail",
                "work_dir": str(coordinator),
                "state_dir": str(coordinator / "state"),
                "log_dir": str(coordinator / "logs"),
                "claim_dir": str(coordinator / "claims"),
                "path_maps": {
                    "worker": {
                        str(coordinator): str(worker),
                    },
                },
                "requirements": {
                    "python_min": "3.11",
                    "git_min": "2.38",
                    "git": {
                        "repo": str(coordinator),
                        "commit": commit,
                        "require_clean": False,
                    },
                    "inputs": [],
                },
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "command": "mkdir -p \"$(dirname {output})\" && printf x > {output}",
                    "outputs": [str(coordinator / "blocked" / "out.txt")],
                }],
            }
            manifest_path = coordinator / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            work = self.run_cli(
                "work",
                "--manifest", str(manifest_path),
                "--coordinator", str(manifest_path),
                "--profile", "worker",
                "--worker", "test-worker",
                "--max-tasks", "1",
            )
            self.assertNotEqual(0, work.returncode)
            self.assertIn("sync failed", work.stderr)

            status = self.run_cli("status", str(coordinator))
            self.assertEqual(0, status.returncode)
            self.assertIn("  tasks=0/1", status.stdout)
            self.assertIn("  pending: 1", status.stdout)

    def test_worker_loop_continues_after_sync_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "sync-retry",
                "work_dir": str(root),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": []},
                ],
            }
            forge.write_json(manifest_path, manifest)
            claims = [
                {"task": "task_0", "claim": {"claim_id": "claim-0", "attempt": 1}},
                {"task": "task_1", "claim": {"claim_id": "claim-1", "attempt": 1}},
                None,
            ]
            args = argparse.Namespace(
                manifest=str(manifest_path),
                coordinator=str(manifest_path),
                profile=None,
                worker="worker-a",
                max_tasks=0,
                idle_seconds=0.0,
                lease_seconds=600,
                task_attempts=1,
                retry_failed=False,
                force=False,
                fail_fast=False,
            )
            with (
                mock.patch("forge_lib.tasks.load_coordinator_manifest", return_value=manifest),
                mock.patch("forge_lib.tasks.check_coordinator_cli"),
                mock.patch("forge_lib.tasks.check_requirements"),
                mock.patch("forge_lib.tasks.claim_from_coordinator", side_effect=claims),
                mock.patch("forge_lib.tasks.heartbeat_on_coordinator"),
                mock.patch("forge_lib.tasks.release_on_coordinator"),
                mock.patch("forge_lib.tasks.run_task", return_value=0) as run_task,
                mock.patch("forge_lib.tasks.sync_task_result", side_effect=[RuntimeError("temporary sync"), None]) as sync_result,
                mock.patch("forge_lib.tasks.WORK_IDLE_POLL_SECONDS", 0.0),
            ):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_work(args)

            self.assertEqual(1, rc)
            self.assertIn("task_0: sync failed: temporary sync", stderr.getvalue())
            self.assertEqual(2, run_task.call_count)
            self.assertEqual(2, sync_result.call_count)

    def test_worker_progress_heartbeat_interval_is_capped(self) -> None:
        self.assertEqual(10.0, forge.heartbeat_interval_seconds(600))
        self.assertEqual(10.0, forge.heartbeat_interval_seconds(0))
        self.assertEqual(5.0, forge.heartbeat_interval_seconds(15))

    def test_worker_doctor_checks_coordinator_callback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "doctor-callback",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            with mock.patch("forge_lib.tasks.check_coordinator_cli") as check:
                rc = forge.cmd_doctor(argparse.Namespace(
                    manifest=str(manifest),
                    profile=None,
                    role="worker",
                    notify_command=None,
                    coordinator="pwa-5090:/tmp/manifest.json",
                ))
            self.assertEqual(0, rc)
            check.assert_called_once_with("pwa-5090:/tmp/manifest.json")

    def test_coordinator_doctor_requires_notify_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            manifest = root / "manifest.json"
            self.run_cli(
                "plan",
                "--name", "doctor",
                "--shards", "1",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "true",
                "--output-template", str(root / "out.txt"),
            )

            worker = self.run_cli("doctor", "--manifest", str(manifest), "--role", "worker")
            self.assertEqual(0, worker.returncode)

            coordinator = self.run_cli(
                "doctor",
                "--manifest", str(manifest),
                "--role", "coordinator",
                "--notify-command", "__definitely_missing_notifai__",
            )
            self.assertNotEqual(0, coordinator.returncode)
            self.assertIn("notify command not found", coordinator.stderr)

    def test_active_status_sort_prefers_shorter_eta(self) -> None:
        rows = [
            {"index": 5, "state": "stale"},
            {"index": 6, "state": "claimed"},
            {
                "index": 1,
                "state": "running",
                "progress_data": forge.normalize_progress_data({
                    "kind": "metadata",
                    "done": 20,
                    "total": 31,
                    "unit": "rows",
                    "rate": 1.0,
                }),
            },
            {
                "index": 2,
                "state": "running",
                "progress_data": forge.normalize_progress_data({
                    "kind": "metadata",
                    "done": 20,
                    "total": 31,
                    "unit": "rows",
                    "rate": 2.0,
                }),
            },
            {
                "index": 3,
                "state": "running",
                "progress_data": forge.normalize_progress_data({
                    "kind": "metadata",
                    "done": 10,
                    "total": 31,
                    "unit": "rows",
                }),
            },
            {"index": 4, "state": "running"},
        ]

        ordered = sorted(rows, key=forge.active_status_sort_key)
        self.assertEqual([5, 6, 2, 1, 3, 4], [row["index"] for row in ordered])

    def test_status_verbose_shows_host_and_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "test",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": [str(root / "out0.txt")]},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": [str(root / "out1.txt")]},
                    {"id": "task_2", "index": 2, "command": "true", "outputs": [str(root / "out2.txt")]},
                    {
                        "id": "task_3",
                        "index": 3,
                        "command": "/venv/bin/python /repo/tools/score/score.py uci --depth 16",
                        "outputs": [str(root / "out3.txt")],
                    },
                ]
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            state_dir = root / "state"
            state_dir.mkdir()
            log_dir = root / "logs"
            log_dir.mkdir()
            (root / "out0.txt").write_text("done")
            done_payload = {
                "task": "task_0",
                "host": "host-done",
                "worker": {"host": "host-done"},
                "manifest_sha256": "dummy",
                "outputs": [{"path": str(root / "out0.txt"), "bytes": 4, "sha256": "dummy"}]
            }
            (state_dir / "task_0.done.json").write_text(json.dumps(done_payload))

            claim_dir = root / "claims" / "task_1.claim"
            claim_dir.mkdir(parents=True)
            claim_payload = {
                "task": "task_1",
                "worker": "worker-claimed",
                "claimed_at": "2023-01-01T00:00:00Z"
            }
            (claim_dir / "claim.json").write_text(json.dumps(claim_payload))
            running_payload = {
                "task": "task_1",
                "host": "host-running",
                "started_at": "2023-01-01T00:00:00Z",
            }
            (state_dir / "task_1.running.json").write_text(json.dumps(running_payload))
            (log_dir / "task_1.log").write_text(
                "[1/1   42/1000] selfplay | score 0.500 | draw 30.0%\n",
                encoding="utf-8",
            )

            fail_payload = {
                "task": "task_2",
                "host": "host-fail",
                "rc": 1
            }
            (state_dir / "task_2.fail.json").write_text(json.dumps(fail_payload))

            compact = self.run_cli("status", str(root))
            self.assertEqual(1, compact.returncode)
            self.assertIn("run=", compact.stdout)
            self.assertIn("  tasks=1/4 fail=1", compact.stdout)
            self.assertIn("  active: 1 subtask=42/1000", compact.stdout)
            self.assertNotIn("host-done [1]: 0", compact.stdout)
            self.assertIn("    worker-claimed shard=1 [subtask=42/1000]", compact.stdout)
            self.assertIn("    host-fail [1]: 2", compact.stdout)
            self.assertIn("  pending: 1\n    score:uci [1]: 3", compact.stdout)

            result = self.run_cli("status", str(root), "--verbose")
            self.assertEqual(1, result.returncode)
            self.assertNotIn("host-done [1]: 0", result.stdout)
            self.assertNotIn("task_0\tdone\thost-done", result.stdout)
            self.assertIn("    worker-claimed shard=1 [subtask=42/1000]", result.stdout)
            self.assertIn("    host-fail [1]: 2", result.stdout)
            self.assertIn("    score:uci [1]: 3", result.stdout)

            show_done = self.run_cli("status", str(root), "--show-done")
            self.assertEqual(1, show_done.returncode)
            self.assertIn("host-done [1]: 0", show_done.stdout)

            result_json = self.run_cli("status", str(root), "--json")
            self.assertEqual(1, result_json.returncode)
            payload = json.loads(result_json.stdout)
            self.assertEqual(4, payload["tasks"])
            self.assertEqual(1, payload["counts"]["done"])
            self.assertEqual(1, payload["active_count"])
            self.assertEqual([0], payload["groups"]["done"]["host-done"])
            self.assertEqual("subtask=42/1000", payload["active"][0]["progress"])
            self.assertEqual("", payload["active"][0]["label"])
            self.assertEqual(1, payload["sections"]["active"]["count"])
            self.assertEqual([3], payload["sections"]["pending"]["tasks"])
            self.assertEqual([2], payload["sections"]["fail"]["groups"]["host-fail"])
            self.assertEqual(["tasks=1/4", "fail=1"], payload["display"]["fields"])

    def test_status_sorts_active_tasks_by_completeness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            state = root / "state"
            logs = root / "logs"
            claims = root / "claims"
            state.mkdir()
            logs.mkdir()
            manifest = {
                "schema": "forge.task.v1",
                "name": "test",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "claim_dir": str(claims),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 100,
                            "log_regex": r"done=(?P<done>\d+)/(?P<total>\d+)",
                        },
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 100,
                            "log_regex": r"done=(?P<done>\d+)/(?P<total>\d+)",
                        },
                    },
                    {"id": "task_2", "index": 2, "command": "true", "outputs": []},
                ],
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            for task_id, worker in [
                ("task_0", "worker-low"),
                ("task_1", "worker-high"),
                ("task_2", "worker-none"),
            ]:
                claim_dir = claims / f"{task_id}.claim"
                claim_dir.mkdir(parents=True)
                (claim_dir / "claim.json").write_text(json.dumps({
                    "task": task_id,
                    "worker": worker,
                    "claimed_at": "2023-01-01T00:00:00Z",
                }))
                (state / f"{task_id}.running.json").write_text(json.dumps({
                    "task": task_id,
                    "host": worker,
                    "started_at": "2023-01-01T00:00:00Z",
                }))
            (logs / "task_0.log").write_text("done=10/100\n", encoding="utf-8")
            (logs / "task_1.log").write_text("done=80/100\n", encoding="utf-8")

            status = self.run_cli("status", str(root))
            self.assertEqual(0, status.returncode)
            high = status.stdout.index("worker-high shard=1")
            low = status.stdout.index("worker-low shard=0")
            none = status.stdout.index("worker-none shard=2")
            self.assertLess(high, low)
            self.assertLess(low, none)

    def test_status_infers_python_subcommand_task_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "test",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "/venv/bin/python /repo/tools/score/score.py uci --depth 16",
                        "outputs": [str(root / "out0.txt")],
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "/venv/bin/python /repo/tools/score/score.py uci --depth 16",
                        "outputs": [str(root / "out1.txt")],
                    },
                    {
                        "id": "task_2",
                        "index": 2,
                        "command": "/venv/bin/python /repo/tools/score/score.py uci --depth 16",
                        "outputs": [str(root / "out2.txt")],
                    },
                ],
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            (root / "state").mkdir()
            old = forge.epoch() - 1000
            for task_id, worker in [("task_0", "worker-a"), ("task_1", "worker-b"), ("task_2", "worker-c")]:
                claim_dir = root / "claims" / f"{task_id}.claim"
                claim_dir.mkdir(parents=True)
                claim_epoch = old if task_id == "task_2" else forge.epoch()
                (claim_dir / "claim.json").write_text(json.dumps({
                    "task": task_id,
                    "worker": worker,
                    "claimed_at": "2023-01-01T00:00:00Z",
                    "claimed_epoch": claim_epoch,
                    "heartbeat_epoch": claim_epoch,
                }))
            (root / "state" / "task_0.running.json").write_text(json.dumps({
                "task": "task_0",
                "host": "host-a",
                "started_at": "2023-01-01T00:00:00Z",
            }))
            os.utime(root / "claims" / "task_2.claim", (old, old))

            status = self.run_cli("status", str(root))
            self.assertEqual(0, status.returncode)
            self.assertIn("worker-a score:uci shard=0", status.stdout)
            self.assertIn("worker-b [claimed] score:uci shard=1", status.stdout)
            self.assertIn("worker-c [stale] score:uci shard=2", status.stdout)

    def test_pending_status_groups_by_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "forge.task.v1",
                "name": "pending-labels",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "tasks": [
                    {"id": "task_0", "index": 0, "label": "score:uci", "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "label": "label:uci", "command": "true", "outputs": []},
                    {"id": "task_2", "index": 2, "command": "true", "outputs": []},
                    {"id": "task_3", "index": 3, "command": "true", "outputs": []},
                ],
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            status = self.run_cli("status", str(root))
            self.assertEqual(0, status.returncode)
            self.assertIn("  pending: 4\n    label:uci [1]: 1\n    score:uci [1]: 0\n    unlabeled [2]: 2-3", status.stdout)

    def test_status_uses_runs_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            for name in ("main", "bonus"):
                run = runs / name
                manifest = run / "manifest.json"
                manifest.parent.mkdir(parents=True)
                manifest.write_text(
                    json.dumps({
                        "schema": "forge.task.v1",
                        "name": name,
                        "work_dir": str(run),
                        "state_dir": str(run / "state"),
                        "log_dir": str(run / "logs"),
                        "tasks": [{"id": f"{name}_0", "index": 0, "command": "true", "outputs": []}],
                    }),
                    encoding="utf-8",
                )
            bonus_state = runs / "bonus" / "state"
            bonus_state.mkdir(parents=True)
            (bonus_state / "bonus_0.done.json").write_text(
                json.dumps({"task": "bonus_0", "host": "host-done"}),
                encoding="utf-8",
            )
            main_state = runs / "main" / "state"
            main_state.mkdir(parents=True)
            main_claim = runs / "main" / "claims" / "main_0.claim"
            main_claim.mkdir(parents=True)
            (main_claim / "claim.json").write_text(
                json.dumps({"task": "main_0", "worker": "worker-main"}),
                encoding="utf-8",
            )
            (main_state / "main_0.running.json").write_text(
                json.dumps({"task": "main_0", "host": "host-main"}),
                encoding="utf-8",
            )
            setup_manifest = root / "examples" / "setup.manifest.json"
            setup_manifest.parent.mkdir(parents=True)
            setup_manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "setup",
                    "work_dir": str(root / "examples"),
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            example_task_manifest = root / "examples" / "nnue-selfplay.manifest.json"
            example_task_manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "nnue-selfplay",
                    "work_dir": str(root / "examples"),
                    "state_dir": str(root / "examples" / "state"),
                    "tasks": [{"id": "example_0", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )
                self.assertEqual(0, rc)
                status_text = stdout.getvalue()
                self.assertIn("main", status_text)
                self.assertRegex(status_text, r"run=main\s+tasks=0/1")
                self.assertNotIn("bonus", status_text)
                self.assertNotIn("setup", status_text)
                self.assertNotIn("nnue-selfplay", status_text)
                self.assertIn("worker-main shard=0", status_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                        )
                    )
                self.assertEqual(0, rc)
                active_text = stdout.getvalue()
                self.assertIn("main", active_text)
                self.assertNotIn("bonus", active_text)
                self.assertIn("  active: 1", active_text)
                self.assertIn("worker-main shard=0", active_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=True,
                            done=False,
                            failed=False,
                            json=True,
                            no_color=True,
                            lease_seconds=600,
                        )
                    )
                self.assertEqual(0, rc)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(["bonus", "main"], sorted(item["run"] for item in payload["runs"]))
                self.assertEqual(0, payload["collapsed_failures_count"])
                bonus_payload = next(item for item in payload["runs"] if item["run"] == "bonus")
                main_payload = next(item for item in payload["runs"] if item["run"] == "main")
                self.assertIn("groups", bonus_payload)
                self.assertIn("sections", bonus_payload)
                self.assertIn("state", bonus_payload)
                self.assertEqual("done", bonus_payload["state"])
                self.assertEqual(1, main_payload["sections"]["active"]["count"])
                self.assertEqual("worker-main", main_payload["sections"]["active"]["tasks"][0]["worker"])

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=True,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )
                self.assertEqual(0, rc)
                all_text = stdout.getvalue()
                self.assertIn("main", all_text)
                self.assertIn("bonus", all_text)
                self.assertIn("worker-main shard=0", all_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=True,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                        )
                    )
                self.assertEqual(0, rc)
                done_text = stdout.getvalue()
                self.assertIn("bonus", done_text)
                self.assertNotIn("main", done_text)

                one_run = self.run_cli("status", str(runs / "main"), cwd=root)
                self.assertEqual(0, one_run.returncode)
                self.assertIn("tasks=0/1", one_run.stdout)

    def test_status_sorts_done_runs_by_completion_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"

            def write_done_run(name: str, completed_epoch: float) -> None:
                run = runs / name
                manifest = {
                    "schema": "forge.task.v1",
                    "name": name,
                    "work_dir": str(run),
                    "state_dir": str(run / "state"),
                    "claim_dir": str(run / "claims"),
                    "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
                }
                forge.write_json(run / "manifest.json", manifest)
                forge.write_json(run / "state" / "task_0.done.json", {"task": "task_0", "host": "worker"})
                summary_path = run / "summary.json"
                forge.write_json(summary_path, {"event": "run_done", "state": "ok"})
                os.utime(summary_path, (completed_epoch, completed_epoch))

            write_done_run("aaa-older-done", 100.0)
            write_done_run("zzz-newer-done", 200.0)
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(runs)}

            result = self.run_cli("status", "--done", "--json", cwd=root, env=env)

        self.assertEqual(0, result.returncode)
        payload = json.loads(result.stdout)
        self.assertEqual(["zzz-newer-done", "aaa-older-done"], [run["run"] for run in payload["runs"]])
        self.assertEqual([200.0, 100.0], [run["completed_epoch"] for run in payload["runs"]])

    def test_status_hides_never_started_pending_runs_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def write_run(name: str, task_count: int = 1) -> Path:
                run = root / "runs" / name
                manifest = run / "manifest.json"
                manifest.parent.mkdir(parents=True)
                manifest.write_text(
                    json.dumps({
                        "schema": "forge.task.v1",
                        "name": name,
                        "work_dir": str(run),
                        "state_dir": str(run / "state"),
                        "tasks": [
                            {"id": f"task_{index}", "index": index, "command": "true", "outputs": []}
                            for index in range(task_count)
                        ],
                    }),
                    encoding="utf-8",
                )
                return run

            write_run("queued")
            waiting = write_run("waiting", task_count=2)
            waiting_state = waiting / "state"
            waiting_state.mkdir()
            (waiting_state / "task_0.done.json").write_text(
                json.dumps({"task": "task_0", "host": "worker-waiting"}),
                encoding="utf-8",
            )
            active = write_run("zzz-active")
            state = active / "state"
            state.mkdir()
            (state / "task_0.running.json").write_text(
                json.dumps({"task": "task_0", "host": "worker-a"}),
                encoding="utf-8",
            )

            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}
            active_status = self.run_cli("status", cwd=root, env=env)
            self.assertEqual(0, active_status.returncode)
            self.assertIn("active", active_status.stdout)
            self.assertIn("    worker-a shard=0", active_status.stdout)
            self.assertNotIn("queued", active_status.stdout)
            self.assertNotIn("waiting", active_status.stdout)

            verbose_status = self.run_cli("status", "--verbose", cwd=root, env=env)
            self.assertEqual(0, verbose_status.returncode)
            self.assertIn("active", verbose_status.stdout)
            self.assertIn("waiting", verbose_status.stdout)
            self.assertNotIn("queued", verbose_status.stdout)
            self.assertIn("command: true", verbose_status.stdout)
            self.assertNotIn("active: 0", verbose_status.stdout)
            self.assertNotIn("fail: 0", verbose_status.stdout)

            failed = write_run("aaa-failed")
            failed_state = failed / "state"
            failed_state.mkdir()
            (failed_state / "task_0.fail.json").write_text(
                json.dumps({"task": "task_0", "host": "worker-fail", "rc": 1}),
                encoding="utf-8",
            )

            failed_status = self.run_cli("status", cwd=root, env=env)
            self.assertEqual(1, failed_status.returncode)
            self.assertIn("zzz-active", failed_status.stdout)
            self.assertIn("failed: 1 run (use forge status --failed)", failed_status.stdout)
            self.assertLess(
                failed_status.stdout.index("failed: 1 run (use forge status --failed)"),
                failed_status.stdout.index("zzz-active"),
            )
            self.assertNotIn("aaa-failed", failed_status.stdout)
            self.assertNotIn("waiting", failed_status.stdout)

            failed_json = self.run_cli("status", "--json", cwd=root, env=env)
            self.assertEqual(1, failed_json.returncode)
            failed_payload = json.loads(failed_json.stdout)
            self.assertEqual(["aaa-failed"], failed_payload["collapsed_failures"])
            failed_run = next(item for item in failed_payload["runs"] if item["run"] == "aaa-failed")
            self.assertEqual([0], failed_run["sections"]["fail"]["groups"]["worker-fail"])

            detailed_failed = self.run_cli("status", "--failed", cwd=root, env=env)
            self.assertEqual(1, detailed_failed.returncode)
            self.assertIn("aaa-failed", detailed_failed.stdout)
            self.assertIn("  fail: 1", detailed_failed.stdout)
            self.assertIn("worker-fail [1]: 0", detailed_failed.stdout)

            all_status = self.run_cli("status", "--all", cwd=root, env=env)
            self.assertEqual(1, all_status.returncode)
            self.assertIn("active", all_status.stdout)
            self.assertIn("queued", all_status.stdout)
            self.assertRegex(all_status.stdout, r"run=queued\n  tasks=0/1")

            all_json = self.run_cli("status", "--all", "--json", cwd=root, env=env)
            self.assertEqual(1, all_json.returncode)
            all_payload = json.loads(all_json.stdout)
            queued_run = next(item for item in all_payload["runs"] if item["run"] == "queued")
            self.assertEqual("empty", queued_run["state"])

    def test_status_shows_marked_queued_runs_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "marked-queued"
            active_run = root / "runs" / "active-run"
            manifest = {
                "schema": "forge.task.v1",
                "name": "marked-queued",
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(forge.queue_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "marked-queued",
                "requested_epoch": 100.0,
            })
            active_manifest = {
                "schema": "forge.task.v1",
                "name": "active-run",
                "work_dir": str(active_run),
                "state_dir": str(active_run / "state"),
                "claim_dir": str(active_run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(active_run / "manifest.json", active_manifest)
            forge.write_json(active_run / "state" / "task_0.running.json", {"task": "task_0", "worker": "worker-a"})
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}

            status = self.run_cli("status", cwd=root, env=env)
            payload_result = self.run_cli("status", "--json", cwd=root, env=env)

        self.assertEqual(0, status.returncode)
        self.assertIn("run=marked-queued", status.stdout)
        self.assertLess(status.stdout.index("run=marked-queued"), status.stdout.index("run=active-run"))
        payload = json.loads(payload_result.stdout)
        self.assertEqual("queued", payload["runs"][0]["state"])
        self.assertEqual("marked-queued", payload["runs"][0]["run"])

    def test_load_run_summaries_skips_manifest_that_disappears(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            missing = runs / "deleted-run" / "manifest.json"
            live = runs / "live-run"
            manifest = {
                "schema": "forge.task.v1",
                "name": "live-run",
                "work_dir": str(live),
                "state_dir": str(live / "state"),
                "claim_dir": str(live / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(live / "manifest.json", manifest)

            with mock.patch("forge_lib.status.run_manifest_paths", return_value=[missing, live / "manifest.json"]):
                summaries = forge.load_run_summaries(lease_seconds=600)

        self.assertEqual(["live-run"], [summary["run"] for summary in summaries])

    def test_status_marks_queued_run_with_dead_monitor_as_stalled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "marked-queued"
            manifest = {
                "schema": "forge.task.v1",
                "name": "marked-queued",
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(forge.queue_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "marked-queued",
                "requested_epoch": 100.0,
            })
            forge.write_json(run / "queue-monitor.json", {
                "schema": "forge.task.v1",
                "pid": 999999999,
            })
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}

            status = self.run_cli("status", cwd=root, env=env)
            payload_result = self.run_cli("status", "--json", cwd=root, env=env)

        self.assertEqual(0, status.returncode)
        self.assertIn("state=stalled", status.stdout)
        payload = json.loads(payload_result.stdout)
        self.assertEqual("stalled", payload["runs"][0]["state"])
        self.assertEqual("dead", payload["runs"][0]["queue_monitor_state"])
        self.assertIn("queue monitor dead", payload["runs"][0]["diagnostics"])

    def test_status_reports_stale_launch_as_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "launch-stale"
            manifest = {
                "schema": "forge.task.v1",
                "name": "launch-stale",
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(forge.launch_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "launch-stale",
                "requested_epoch": 1.0,
                "lease_seconds": 1,
            })
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}

            status = self.run_cli("status", "launch-stale", "--verbose", cwd=root, env=env)
            failed_status = self.run_cli("status", "--failed", "--json", cwd=root, env=env)

        self.assertEqual(1, status.returncode)
        self.assertIn("state=launch-stale", status.stdout)
        self.assertIn("launch request is stale", status.stdout)
        payload = json.loads(failed_status.stdout)
        self.assertEqual(1, failed_status.returncode)
        self.assertEqual("failed", payload["runs"][0]["state"])
        self.assertEqual("stale", payload["runs"][0]["launch_state"])

    def test_status_sorts_queued_runs_fifo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"

            def write_queued_run(name: str, requested_epoch: float) -> None:
                run = runs / name
                manifest = {
                    "schema": "forge.task.v1",
                    "name": name,
                    "work_dir": str(run),
                    "state_dir": str(run / "state"),
                    "claim_dir": str(run / "claims"),
                    "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
                }
                forge.write_json(run / "manifest.json", manifest)
                forge.write_json(forge.queue_request_path(manifest), {
                    "schema": "forge.task.v1",
                    "run": name,
                    "requested_epoch": requested_epoch,
                })

            write_queued_run("aaa-newer-queued", 200.0)
            write_queued_run("zzz-older-queued", 100.0)
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(runs)}

            result = self.run_cli("status", "--json", cwd=root, env=env)

        self.assertEqual(0, result.returncode)
        payload = json.loads(result.stdout)
        self.assertEqual(["zzz-older-queued", "aaa-newer-queued"], [run["run"] for run in payload["runs"]])
        self.assertEqual([100.0, 200.0], [run["queued_epoch"] for run in payload["runs"]])

    def test_status_reports_partially_done_queued_run_as_queued(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "queued-resume"
            manifest = {
                "schema": "forge.task.v1",
                "name": "queued-resume",
                "work_dir": str(run),
                "state_dir": str(run / "state"),
                "claim_dir": str(run / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": []},
                ],
            }
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(run / "state" / "task_0.done.json", {"task": "task_0"})
            forge.write_json(forge.queue_request_path(manifest), {
                "schema": "forge.task.v1",
                "run": "queued-resume",
                "requested_epoch": 100.0,
            })
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}

            status = self.run_cli("status", cwd=root, env=env)
            named_status = self.run_cli("status", "queued-resume", "--verbose", cwd=root, env=env)
            payload_result = self.run_cli("status", "--json", cwd=root, env=env)

        self.assertIn("state=queued", status.stdout)
        self.assertIn("state=queued", named_status.stdout)
        self.assertIn("command: true", named_status.stdout)
        self.assertNotIn("active: 0", named_status.stdout)
        self.assertNotIn("fail: 0", named_status.stdout)
        payload = json.loads(payload_result.stdout)
        self.assertEqual("queued", payload["runs"][0]["state"])
        self.assertIn("state=queued", payload["runs"][0]["display"]["fields"])
        self.assertEqual("queued-resume", payload["runs"][0]["run"])

    def test_status_reports_stopped_incomplete_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "stopped-run"
            state = run / "state"
            manifest = {
                "schema": "forge.task.v1",
                "name": "stopped-run",
                "work_dir": str(run),
                "state_dir": str(state),
                "claim_dir": str(run / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": []},
                ],
            }
            forge.write_json(run / "manifest.json", manifest)
            forge.write_json(state / "task_0.done.json", {"task": "task_0", "host": "worker"})
            forge.write_json(forge.stop_request_path(manifest), {"run": "stopped-run"})
            env = {"FORGE_COORDINATOR_RUNS": "", "FORGE_RUNS_DIR": str(root / "runs")}

            status = self.run_cli("status", "stopped-run", "--json", cwd=root, env=env)
            default_status = self.run_cli("status", cwd=root, env=env)
            default_payload = self.run_cli("status", "--json", cwd=root, env=env)

        self.assertEqual(0, status.returncode)
        payload = json.loads(status.stdout)
        self.assertEqual("stopped", payload["state"])
        self.assertEqual({"done": 1, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 1}, payload["counts"])
        self.assertEqual(0, default_status.returncode)
        self.assertIn("run=stopped-run", default_status.stdout)
        self.assertIn("tasks=1/2", default_status.stdout)
        self.assertEqual(0, default_payload.returncode)
        listed = json.loads(default_payload.stdout)
        self.assertEqual(["stopped-run"], [run["run"] for run in listed["runs"]])
        self.assertEqual("stopped", listed["runs"][0]["state"])

    def test_default_status_prefilters_inactive_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"

            def write_run(name: str, state: str) -> None:
                run = runs / name
                state_dir = run / "state"
                run.mkdir(parents=True)
                manifest = {
                    "schema": "forge.task.v1",
                    "name": name,
                    "work_dir": str(run),
                    "state_dir": str(state_dir),
                    "claim_dir": str(run / "claims"),
                    "tasks": [
                        {"id": "task_0", "index": 0, "command": "true", "outputs": []},
                    ],
                }
                forge.write_json(run / "manifest.json", manifest)
                if state == "queued":
                    return
                state_dir.mkdir()
                if state == "done":
                    forge.write_json(state_dir / "task_0.done.json", {"task": "task_0", "host": "worker-done"})
                elif state == "failed":
                    forge.write_json(state_dir / "task_0.fail.json", {"task": "task_0", "host": "worker-fail", "rc": 1})
                elif state == "running":
                    forge.write_json(state_dir / "task_0.running.json", {"task": "task_0", "host": "worker-run"})

            write_run("aaa-done", "done")
            write_run("bbb-queued", "queued")
            write_run("ccc-failed", "failed")
            write_run("ddd-running", "running")
            write_run("eee-stopped", "running")
            stopped_manifest = forge.load_manifest(runs / "eee-stopped" / "manifest.json")
            forge.write_json(forge.stop_request_path(stopped_manifest), {"run": "eee-stopped"})
            calls: list[str] = []
            original = forge.status_summary

            def wrapped(manifest: dict[str, Any], *, lease_seconds: int) -> dict[str, Any]:
                calls.append(str(manifest["name"]))
                return original(manifest, lease_seconds=lease_seconds)

            args = argparse.Namespace(
                run=None,
                all=False,
                done=False,
                failed=False,
                json=False,
                no_color=True,
                lease_seconds=600,
                show_done=False,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.status.status_summary", side_effect=wrapped),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                rc = forge.cmd_status(args)
            self.assertEqual(1, rc)
            self.assertEqual(["ccc-failed", "ddd-running", "eee-stopped"], calls)

    def test_status_empty_active_message_is_specific(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "done-run"
            manifest = run / "manifest.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "done-run",
                    "work_dir": str(run),
                    "state_dir": str(run / "state"),
                    "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            state = run / "state"
            state.mkdir(parents=True)
            (state / "task_0.done.json").write_text(
                json.dumps({"task": "task_0", "host": "worker"}),
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )
                self.assertEqual(0, rc)
                self.assertIn("no active Forge runs found", stdout.getvalue())

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=True,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )
                self.assertEqual(0, rc)
                self.assertIn("done-run", stdout.getvalue())

    def test_status_missing_named_run_reports_clean_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = {
                "FORGE_COORDINATOR_RUNS": "",
                "FORGE_RUNS_DIR": str(root / "runs"),
            }
            result = self.run_cli("status", "missing-run", cwd=root, env=env)
            self.assertNotEqual(0, result.returncode)
            self.assertEqual("", result.stdout)
            self.assertIn("run not found: missing-run", result.stderr)
            self.assertNotIn("Traceback", result.stderr)

    def test_status_uses_runs_dir_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "custom-runs"
            run = runs / "main"
            manifest = run / "manifest.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "main",
                    "work_dir": str(run),
                    "state_dir": str(run / "state"),
                    "log_dir": str(run / "logs"),
                    "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            state = run / "state"
            state.mkdir()
            (state / "task_0.running.json").write_text(
                json.dumps({"task": "task_0", "host": "worker-a"}),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {"FORGE_RUNS_DIR": str(runs)}):
                result = self.run_cli("status", cwd=root)
            self.assertEqual(0, result.returncode)
            self.assertIn("main", result.stdout)

    def test_status_colors_human_output_when_forced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "color", failed=None)
            manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
            manifest["description"] = "Run description"
            manifest["comment"] = "quick smoke"
            forge.write_json(run / "manifest.json", manifest)
            stdout = io.StringIO()
            with mock.patch.dict(os.environ, {"FORGE_COLOR": "always"}), contextlib.redirect_stdout(stdout):
                rc = forge.cmd_status(
                    argparse.Namespace(
                        run=str(run),
                        all=False,
                        done=False,
                        failed=False,
                        json=False,
                        no_color=False,
                        lease_seconds=600,
                        show_done=False,
                        verbose=False,
                    )
                )

            self.assertEqual(0, rc)
            text = stdout.getvalue()
            blue = forge.STATUS_BLUE_SGR
            self.assertIn(f"run=\033[{blue}mcolor\033[0m", text)
            self.assertIn("desc=Run description", text)
            self.assertIn('"quick smoke"', text)
            self.assertNotIn("desc=\033[", text)
            self.assertIn("tasks=\033[33m0/1", text)
            self.assertIn("pending: \033[33m1", text)
            self.assertEqual(
                f"eta=\033[{blue}m55m38s\033[0m",
                forge.color_status_field("eta=55m38s", color_enabled=True),
            )
            self.assertEqual(
                "eta=\033[33munknown\033[0m",
                forge.color_status_field("eta=unknown", color_enabled=True),
            )
            self.assertEqual(
                "rows=1/2, eta=\033[33munknown\033[0m",
                forge.color_progress_text("rows=1/2, eta=unknown", color_enabled=True),
            )
            self.assertEqual(
                f"speedup=\033[{blue}m1.5x\033[0m",
                forge.color_status_field("speedup=1.5x", color_enabled=True),
            )
            self.assertEqual(
                "elo=\033[32m+1.8\033[0m",
                forge.color_status_field("elo=+1.8", color_enabled=True),
            )
            self.assertEqual(
                "elo=\033[33m0.0\033[0m",
                forge.color_status_field("elo=0.0", color_enabled=True),
            )
            self.assertEqual(
                "elo=\033[31m-1.8\033[0m",
                forge.color_status_field("elo=-1.8", color_enabled=True),
            )
            self.assertEqual(
                "rows=1/2, elo=\033[31m-1.8\033[0m, eta=\033[33munknown\033[0m",
                forge.color_progress_text("rows=1/2, elo=-1.8, eta=unknown", color_enabled=True),
            )
            self.assertEqual(
                f"run=\033[{blue}mname\033[0m",
                forge.color_status_field("run=name", color_enabled=True),
            )

    def test_status_no_color_overrides_forced_color(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "color", failed=None)
            stdout = io.StringIO()
            with mock.patch.dict(os.environ, {"FORGE_COLOR": "always"}), contextlib.redirect_stdout(stdout):
                rc = forge.cmd_status(
                    argparse.Namespace(
                        run=str(run),
                        all=False,
                        done=False,
                        failed=False,
                        json=False,
                        no_color=True,
                        lease_seconds=600,
                        show_done=False,
                        verbose=False,
                    )
                )

            self.assertEqual(0, rc)
            self.assertNotIn("\033[", stdout.getvalue())

    def test_status_delegates_to_coordinator_runs_env(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout=self.delegated_status_payload(),
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch("forge.subprocess.run", return_value=remote_result) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_status(
                    argparse.Namespace(
                        run=None,
                        all=False,
                        done=False,
                        failed=False,
                        json=False,
                        no_color=True,
                        lease_seconds=600,
                        show_done=False,
                        verbose=False,
                    )
                )

        self.assertEqual(0, rc)
        output = stdout.getvalue()
        self.assertIn("coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\n", output)
        self.assertIn("run=remote-run", output)
        self.assertIn("desc=Remote run", output)
        self.assertIn("remote-worker score:uci shard=0", output)
        self.assertNotIn('"remote-run"', output)
        self.assertNotIn('"score:uci"', output)
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("pwa-5090", command)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", command[-1])
        self.assertIn("FORGE_COORDINATOR_RUNS=", command[-1])
        self.assertIn("FORGE_COLOR=never", command[-1])
        self.assertIn("--json", command[-1])

    def test_status_prefers_existing_local_named_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "local-run", failed=False)
            with (
                self.patch_runs_dir(runs),
                mock.patch.dict(
                    os.environ,
                    {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
                ),
                mock.patch("forge.subprocess.run") as run_mock,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run="local-run",
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertIn("run=local-run", stdout.getvalue())
            run_mock.assert_not_called()

    def test_status_prefers_local_active_runs_before_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            active_run = self.write_retry_run(runs, "local-active", failed=None)
            manifest = forge.load_manifest(active_run / "manifest.json")
            forge.claim_task(
                manifest,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch.dict(
                    os.environ,
                    {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
                ),
                mock.patch("forge.subprocess.run") as run_mock,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertIn("run=local-active", stdout.getvalue())
            run_mock.assert_not_called()

    def test_status_failed_prefers_local_failures_before_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            self.write_retry_run(runs, "local-failed", failed=True)
            with (
                self.patch_runs_dir(runs),
                mock.patch.dict(
                    os.environ,
                    {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
                ),
                mock.patch("forge.subprocess.run") as run_mock,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=True,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )

            self.assertEqual(1, rc)
            self.assertIn("run=local-failed", stdout.getvalue())
            run_mock.assert_not_called()

    def test_status_delegation_forces_color_when_local_output_is_colored(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout=self.delegated_status_payload(),
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {
                "FORGE_COLOR": "always",
                "FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs",
            },
        ), mock.patch("forge.subprocess.run", return_value=remote_result) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_status(
                    argparse.Namespace(
                        run=None,
                        all=False,
                        done=False,
                        failed=False,
                        json=False,
                        no_color=False,
                        lease_seconds=600,
                        show_done=False,
                        verbose=False,
                    )
                )

        self.assertEqual(0, rc)
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("FORGE_COLOR=always", command[-1])

    def test_status_delegates_to_runs_coordinator_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            runs.mkdir()
            (runs / "coordinator").write_text(
                "pwa-5090:/home/petter/code/cpp/chess/forge/runs\n",
                encoding="utf-8",
            )
            remote_result = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout=self.delegated_status_payload(),
                stderr="",
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.subprocess.run", return_value=remote_result) as run_mock,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            show_done=False,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            output = stdout.getvalue()
            self.assertIn("coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\n", output)
            self.assertIn("run=remote-run", output)
            self.assertIn("remote-worker score:uci shard=0", output)
            self.assertNotIn('"remote-run"', output)
            command = run_mock.call_args.args[0]
            self.assertEqual("ssh", command[0])
            self.assertIn("pwa-5090", command)
            self.assertIn("--json", command[-1])

    def test_status_shows_declared_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "rows",
                        "total": 125000,
                        "log_regex": (
                            r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+"
                            r"rate=(?P<rate>[0-9.]+)/s\s+elo=(?P<elo>[+-]?[0-9.]+)"
                        ),
                        "output_unit": "output",
                        "display_fields": {"elo": "elo~"},
                    },
                }],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker"}))
            (logs / "task_0.log").write_text(
                "shard 0/24: selected=50000 written=48010 rate=6.7/s elo=-12.3\n",
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                        )
                )
                self.assertEqual(0, rc)
                self.assertIn("    worker shard=0 [rows=50k/125k, output=48k, elo~=-12.3, eta=3h06m]", stdout.getvalue())

                named_stdout = io.StringIO()
                with contextlib.redirect_stdout(named_stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run="score",
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=False,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                self.assertIn("active: 1 rows=50k/125k output=48k", named_stdout.getvalue())
                self.assertNotIn("active: 1 rows=50k/125k output=48k eta=3h06m", named_stdout.getvalue())
                self.assertNotIn("workers=1", named_stdout.getvalue())
                self.assertNotIn("speedup=1.0x", named_stdout.getvalue())
                self.assertIn("worker shard=0 [rows=50k/125k, output=48k, elo~=-12.3, eta=3h06m]", named_stdout.getvalue())

    def test_status_extracts_declared_progress_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "rows",
                        "total": 100,
                        "log_regex": r"step=(?P<done>\d+)/(?P<total>\d+)\s+loss=(?P<loss>[0-9.]+)\s+lr=(?P<lr>[0-9.e-]+)",
                        "series": [
                            {"name": "loss", "field": "loss", "x": "done", "scale": "linear", "min": 0.0},
                            {"name": "lr", "field": "lr", "x": "done", "scale": "linear", "max": 0.01},
                        ],
                    },
                }],
            }
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker"}), encoding="utf-8")
            (logs / "task_0.log").write_text(
                "step=10/100 loss=1.50 lr=1e-3\n"
                "step=20/100 loss=1.25 lr=5e-4\n",
                encoding="utf-8",
            )

            summary = forge.status_summary(manifest, lease_seconds=600)

            graph = summary["active"][0]["progress_graph"]
            self.assertEqual("rows", graph["x_unit"])
            self.assertEqual(100, graph["x_total"])
            series = {item["name"]: item["points"] for item in graph["series"]}
            meta = {item["name"]: item for item in graph["series"]}
            self.assertEqual([{"x": 10.0, "y": 1.5}, {"x": 20.0, "y": 1.25}], series["loss"])
            self.assertEqual([{"x": 10.0, "y": 0.001}, {"x": 20.0, "y": 0.0005}], series["lr"])
            self.assertEqual("linear", meta["loss"]["scale"])
            self.assertEqual(0.0, meta["loss"]["min"])
            self.assertEqual("linear", meta["lr"]["scale"])
            self.assertEqual(0.01, meta["lr"]["max"])

    def test_status_builds_sprt_run_graph_without_declared_series(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            tasks = []
            for index in range(2):
                tasks.append({
                    "id": f"task_{index}",
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 2,
                        "log_regex": (
                            r"\[\s*(?P<done>\d+)\/(?P<total>\d+)\].*?"
                            r"Elo\s+(?P<elo>[-+]?\d+(?:\.\d+)?|[-+]?inf).*?"
                            r"\+/-\s+(?P<ci>\d+(?:\.\d+)?).*?"
                            r"LOS\s+(?P<los>\d+(?:\.\d+)?)%.*?"
                            r"draw\s+(?P<draw>\d+(?:\.\d+)?)%"
                        ),
                    },
                })
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }
            for index, (elo, llr) in enumerate((("10.0", "0.01"), ("-20.0", "-0.02"))):
                task_id = f"task_{index}"
                (state / f"{task_id}.done.json").write_text(
                    json.dumps({"task": task_id, "host": "worker"}),
                    encoding="utf-8",
                )
                (logs / f"{task_id}.log").write_text(
                    f"[2/2] Elo  {elo} +/-  80.0 | LLR {llr}/2.94 (  1%) | LOS 50.0% | draw   0.0%\n",
                    encoding="utf-8",
                )

            summary = forge.status_summary(manifest, lease_seconds=600)

            graph = summary["progress_graph"]
            self.assertIsNotNone(graph)
            self.assertEqual(4, graph["x_total"])
            series = {item["name"]: item["points"] for item in graph["series"]}
            meta = {item["name"]: item for item in graph["series"]}
            self.assertEqual(
                [{"x": 0.0, "y": 0.0}, {"x": 2.0, "y": 0.01}, {"x": 4.0, "y": -0.01}],
                series["llr"],
            )
            self.assertAlmostEqual(0.0, series["elo"][0]["y"], places=1)
            self.assertAlmostEqual(-5.0, series["elo"][-1]["y"], places=1)
            self.assertEqual(-2.94, meta["llr"]["min"])
            self.assertEqual(2.94, meta["llr"]["max"])
            self.assertEqual("symmetric", meta["llr"]["scale"])
            self.assertEqual("ci", meta["elo"]["band_series"])
            self.assertEqual(0.35, meta["elo"]["band_scale"])
            self.assertEqual(0.24, meta["elo"]["band_max_fraction"])
            self.assertTrue(meta["elo"]["smooth"])

    def test_status_sprt_run_graph_uses_processed_game_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            tasks = []
            for index in range(3):
                tasks.append({
                    "id": f"task_{index}",
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 2,
                        "log_regex": (
                            r"\[\s*(?P<done>\d+)\/(?P<total>\d+)\].*?"
                            r"Elo\s+(?P<elo>[-+]?\d+(?:\.\d+)?|[-+]?inf).*?"
                            r"\+/-\s+(?P<ci>\d+(?:\.\d+)?).*?"
                            r"LOS\s+(?P<los>\d+(?:\.\d+)?)%.*?"
                            r"draw\s+(?P<draw>\d+(?:\.\d+)?)%"
                        ),
                    },
                })
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }
            for index, (elo, llr) in ((1, ("20.0", "0.10")), (2, ("-20.0", "-0.10"))):
                task_id = f"task_{index}"
                (state / f"{task_id}.done.json").write_text(
                    json.dumps({"task": task_id, "host": "worker"}),
                    encoding="utf-8",
                )
                (logs / f"{task_id}.log").write_text(
                    f"[2/2] Elo  {elo} +/-  80.0 | LLR {llr}/2.94 (  3%) | LOS 50.0% | draw   0.0%\n",
                    encoding="utf-8",
                )

            summary = forge.status_summary(manifest, lease_seconds=600)

            graph = summary["progress_graph"]
            self.assertIsNotNone(graph)
            self.assertEqual(6, graph["x_total"])
            series = {item["name"]: item["points"] for item in graph["series"]}
            self.assertEqual([0.0, 4.0], [point["x"] for point in series["llr"]])
            self.assertEqual([0.0, 4.0], [point["x"] for point in series["elo"]])

    def test_status_run_graph_history_keeps_old_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            tasks = []
            for index in range(2):
                tasks.append({
                    "id": f"task_{index}",
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 2,
                        "log_regex": (
                            r"\[\s*(?P<done>\d+)\/(?P<total>\d+)\].*?"
                            r"Elo\s+(?P<elo>[-+]?\d+(?:\.\d+)?|[-+]?inf).*?"
                            r"\+/-\s+(?P<ci>\d+(?:\.\d+)?).*?"
                            r"LOS\s+(?P<los>\d+(?:\.\d+)?)%.*?"
                            r"draw\s+(?P<draw>\d+(?:\.\d+)?)%"
                        ),
                    },
                })
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }
            (state / "task_0.done.json").write_text(
                json.dumps({"task": "task_0", "host": "worker"}),
                encoding="utf-8",
            )
            (logs / "task_0.log").write_text(
                "[2/2] Elo  -50.0 +/-  80.0 | LLR -0.20/2.94 ( -7%) | LOS 40.0% | draw   0.0%\n",
                encoding="utf-8",
            )

            first = forge.status_summary(manifest, lease_seconds=600)
            first_series = {item["name"]: item["points"] for item in first["progress_graph"]["series"]}
            self.assertEqual([{"x": 0.0, "y": 0.0}, {"x": 2.0, "y": -0.2}], first_series["llr"])

            (logs / "task_0.log").write_text(
                "[2/2] Elo  200.0 +/-  80.0 | LLR 0.40/2.94 ( 14%) | LOS 90.0% | draw   0.0%\n",
                encoding="utf-8",
            )
            (state / "task_1.done.json").write_text(
                json.dumps({"task": "task_1", "host": "worker"}),
                encoding="utf-8",
            )
            (logs / "task_1.log").write_text(
                "[2/2] Elo  10.0 +/-  80.0 | LLR 0.10/2.94 (  3%) | LOS 55.0% | draw   0.0%\n",
                encoding="utf-8",
            )

            second = forge.status_summary(manifest, lease_seconds=600)
            series = {item["name"]: item["points"] for item in second["progress_graph"]["series"]}
            self.assertEqual(
                [{"x": 0.0, "y": 0.0}, {"x": 2.0, "y": 0.4}, {"x": 4.0, "y": 0.5}],
                series["llr"],
            )
            self.assertEqual([2.0, 4.0], [point["x"] for point in series["ci"]])
            self.assertAlmostEqual(80.0, series["ci"][0]["y"], places=1)
            self.assertAlmostEqual(56.6, series["ci"][1]["y"], places=1)

            third = forge.status_summary(manifest, lease_seconds=600)
            series = {item["name"]: item["points"] for item in third["progress_graph"]["series"]}
            self.assertEqual([0.0, 2.0, 4.0], [point["x"] for point in series["llr"]])
            self.assertEqual(2, len((state / "progress-history.jsonl").read_text(encoding="utf-8").splitlines()))

    def test_sprt_run_graph_shows_elo_from_first_valid_aggregate(self) -> None:
        points = forge.sprt_aggregate_graph_points(
            [
                {"x": 26.0, "done": 26, "fields": {"elo": "-70.0", "llr": "-0.05/2.20"}},
                {"x": 52.0, "done": 26, "fields": {"elo": "0.0", "llr": "0.00/2.20"}},
                {"x": 78.0, "done": 26, "fields": {"elo": "30.0", "llr": "0.03/2.20"}},
                {"x": 104.0, "done": 26, "fields": {"elo": "50.0", "llr": "0.05/2.20"}},
            ],
            {"elo": "elo", "llr": "llr"},
            x_total=3000,
        )

        self.assertEqual([0.0, 26.0, 52.0, 78.0, 104.0], [point["x"] for point in points["llr"]])
        self.assertEqual([0.0, 26.0, 52.0, 78.0, 104.0], [point["x"] for point in points["elo"]])

    def test_status_sprt_header_shows_aggregated_elo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            tasks = []
            for index in range(2):
                tasks.append({
                    "id": f"task_{index}",
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 1000,
                        "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                    },
                })
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker-a"}), encoding="utf-8")
            (state / "task_1.running.json").write_text(json.dumps({"task": "task_1", "host": "worker-b"}), encoding="utf-8")
            (logs / "task_0.log").write_text(
                "[100/1000] Elo  10.0 +/-  20.0 | LLR  0.12/2.94 (  4%) | LOS 70.0% | draw  40.0% | ETA 10m\n",
                encoding="utf-8",
            )
            (logs / "task_1.log").write_text(
                "[300/1000] Elo  -2.0 +/-  10.0 | LLR -0.24/2.94 ( -8%) | LOS 55.0% | draw  45.0% | ETA 5m\n",
                encoding="utf-8",
            )

            status = self.run_cli("status", str(run))

            self.assertEqual(0, status.returncode, status.stderr)
            # los is pooled from aggregate elo/ci (Phi(elo/sigma)), not a
            # weighted mean of per-shard LOS values.
            self.assertIn("elo=+1.0 llr=-0.12/2.94 (-4%) los=58.6% ci=9.0 draw=43.8% games=400/2000 tasks=0/2", status.stdout)
            self.assertIn("active: 2", status.stdout)
            self.assertNotIn("active: 2 games=400/2000", status.stdout)
            self.assertIn("worker-a sprt shard=0 [games=100/1000, elo=10.0, ci=20.0, llr=0.12/2.94 (4%), los=70.0, draw=40.0]", status.stdout)
            self.assertIn("worker-b sprt shard=1 [games=300/1000, elo=-2.0, ci=10.0, llr=-0.24/2.94 (-8%), los=55.0, draw=45.0]", status.stdout)

    def test_pooled_los_percent_tracks_pooled_evidence(self) -> None:
        from forge_lib.status import pooled_los_percent

        # Regression: sprt-tag-3855cbf-vs-be211e3 showed elo=+7.4 ci=14.8
        # but los=54.6% because per-shard LOS values (each ~55% at 26
        # games/shard) were averaged. The pooled LOS must reflect the
        # pooled evidence instead.
        self.assertAlmostEqual(83.6, pooled_los_percent(7.4, 14.8), places=1)
        self.assertAlmostEqual(50.0, pooled_los_percent(0.0, 14.8), places=1)
        self.assertAlmostEqual(16.4, pooled_los_percent(-7.4, 14.8), places=1)
        self.assertAlmostEqual(
            100.0 - pooled_los_percent(5.0, 10.0),
            pooled_los_percent(-5.0, 10.0),
            places=9,
        )

    def test_status_sprt_progress_does_not_require_eta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 50,
                        "log_regex": (
                            r"\[\s*(?P<done>\d+)\/(?P<total>\d+)\].*?"
                            r"Elo\s+(?P<elo>[-+]?\d+(?:\.\d+)?).*?"
                            r"ETA\s+(?P<eta>\S+)"
                        ),
                    },
                }],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker-a"}), encoding="utf-8")
            (logs / "task_0.log").write_text(
                "[38/50] Elo -18.3 +/- 89.8 | LLR -0.09/2.94 ( -3%) | LOS 34.1% | draw 36.8%\n",
                encoding="utf-8",
            )

            status = self.run_cli("status", str(run))

            self.assertEqual(0, status.returncode, status.stderr)
            # Aggregate los is derived from pooled elo/ci, so it can differ
            # slightly from the shard's own W/L-based LOS (34.1%).
            self.assertIn("elo=-18.3 llr=-0.09/2.94 (-3%) los=34.5% ci=89.8 draw=36.8% games=38/50 tasks=0/1", status.stdout)
            self.assertIn("worker-a sprt shard=0 [games=38/50, elo=-18.3, ci=89.8, llr=-0.09/2.94 (-3%), los=34.1, draw=36.8]", status.stdout)

    def test_status_header_shows_games_before_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "sprt-run"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt-run",
                "kind": "sprt",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {"unit": "games", "total": 128},
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {"unit": "games", "total": 128},
                    },
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            status = self.run_cli("status", str(run))

            self.assertEqual(0, status.returncode, status.stderr)
            self.assertIn("games=0/256 tasks=0/2", status.stdout)

    def test_status_eta_counts_down_from_log_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "rows",
                        "total": 100,
                        "log_regex": r"selected=(?P<done>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                    },
                }],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker"}))
            log = logs / "task_0.log"
            log.write_text("selected=50 rate=10/s\n", encoding="utf-8")
            os.utime(log, (1000.0, 1000.0))

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1002.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run="score",
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=False,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                self.assertIn("eta=3s", stdout.getvalue())

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1006.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run="score",
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=False,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                self.assertIn("eta=unknown", stdout.getvalue())

    def test_status_marks_complete_running_progress_as_finalizing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 50,
                        "log_regex": r"games=(?P<done>\d+)/(?P<total>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                    },
                }],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker"}))
            (logs / "task_0.log").write_text("games=50/50 rate=10/s\n", encoding="utf-8")

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run="score",
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=False,
                            show_done=False,
                        )
                    )
                self.assertEqual(0, rc)
                text = stdout.getvalue()
                self.assertIn("worker shard=0 [games=50/50, finalizing]", text)
                self.assertNotIn("eta=0s", text)

    def test_status_shows_active_worker_throughput(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 125000,
                            "log_regex": r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                            "output_unit": "output",
                        },
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 125000,
                            "log_regex": r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                            "output_unit": "output",
                        },
                    },
                    {
                        "id": "task_2",
                        "index": 2,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 125000,
                            "log_regex": r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                            "output_unit": "output",
                        },
                    },
                    {
                        "id": "task_3",
                        "index": 3,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "rows",
                            "total": 125000,
                            "log_regex": r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                            "output_unit": "output",
                        },
                    },
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker-a", "started_epoch": 1000.0}))
            (state / "task_1.running.json").write_text(json.dumps({"task": "task_1", "host": "worker-a-slot2", "started_epoch": 1000.0}))
            (state / "task_2.running.json").write_text(json.dumps({"task": "task_2", "host": "worker-b", "started_epoch": 1000.0}))
            (state / "task_3.done.json").write_text(json.dumps({"task": "task_3", "host": "worker-b", "elapsed_s": 100.0}))
            (logs / "task_0.log").write_text(
                "shard 0/2: selected=50000 written=48010 rate=6.7/s\n",
                encoding="utf-8",
            )
            (logs / "task_1.log").write_text(
                "shard 1/2: selected=1000 written=120 rate=2.0/s\n",
                encoding="utf-8",
            )
            (logs / "task_2.log").write_text(
                "shard 2/2: selected=1000 written=777 rate=1.0/s\n",
                encoding="utf-8",
            )
            (logs / "task_3.log").write_text(
                "shard 3/3: selected=20000 written=20000 rate=4.0/s\n",
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1100.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=True,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                text = stdout.getvalue()
                self.assertIn("tasks=1/4 speedup=1.1x", text)
                self.assertIn("active: 3 rows=177k/500k workers=2 output=68k min=worker-b:20k max=worker-a:48k", text)
                self.assertNotIn("max=worker-a:48k speedup=", text)
                self.assertNotIn("active workers:", text)
                self.assertNotIn("tasks=2 output=48k", text)
                self.assertNotIn("tasks=1 output=20k", text)
                worker_section = text.split("active: 3", 1)[1].split("  worker-a shard=0", 1)[0]
                self.assertNotIn("output=68k output=68k", worker_section)
                self.assertNotIn("rate", worker_section)
                self.assertNotIn("avg", worker_section)

                color_stdout = io.StringIO()
                with mock.patch.dict(os.environ, {"FORGE_COLOR": "always"}), contextlib.redirect_stdout(color_stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=False,
                            lease_seconds=600,
                            verbose=True,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                color_line = next(
                    line for line in color_stdout.getvalue().splitlines()
                    if "active:" in line and "workers=" in line
                )
                self.assertIn("active: \033[33m3\033[0m", color_line)
                self.assertIn("workers=2 output=68k min=worker-b:20k max=worker-a:48k", color_line)
                self.assertIn(f"speedup=\033[{forge.STATUS_BLUE_SGR}m1.1x\033[0m", color_stdout.getvalue())
                self.assertNotIn("workers=\033[", color_line)
                self.assertNotIn("output=\033[", color_line)
                self.assertNotIn("min=\033[", color_line)
                self.assertNotIn("max=\033[", color_line)

    def test_summary_eta_includes_pending_work(self) -> None:
        summary = {
            "active": [
                {
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 5.0,
                    },
                },
                {
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 5.0,
                    },
                },
            ],
            "active_count": 2,
            "counts": {"pending": 1},
            "progress_done": {"rows": 100},
            "progress_totals": {"rows": 400},
        }
        self.assertEqual("progress=rows=200/400, eta=10s", forge.summary_progress(summary))

    def test_summary_eta_uses_cumulative_run_rate_over_noisy_active_rate(self) -> None:
        created_at_str = time.strftime(
            "%Y-%m-%dT%H:%M:%S+0000", time.gmtime(forge.epoch() - 1000.0)
        )
        summary = {
            "created_at": created_at_str,
            "active": [
                {
                    "progress_data": {
                        "kind": "metadata",
                        "done": 10,
                        "total": 1000,
                        "unit": "games",
                        # Deliberately absurd instantaneous rate a single fresh
                        # shard could report; must not dominate the aggregate ETA.
                        "rate": 5000.0,
                    },
                },
            ],
            "counts": {"pending": 5},
            "progress_done": {"games": 990},
            "progress_totals": {"games": 2000000},
        }
        # cumulative rate = 1000 games done / 1000s elapsed = 1.0 games/s
        # remaining = 2000000 - 1000 = 1999000 -> eta ~= 1999000s, not the
        # ~400s the noisy 5000.0 active rate would have produced.
        progress = forge.summary_progress(summary)
        self.assertIn("games=1000/2m", progress)
        self.assertIn("eta=23d", progress)

    def test_summary_eta_uses_aggregate_rate_while_work_is_pending(self) -> None:
        summary = {
            "active": [
                {
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 5.0,
                    },
                },
                {
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 30.0,
                    },
                },
            ],
            "active_count": 2,
            "counts": {"pending": 1},
            "progress_done": {"rows": 100},
            "progress_totals": {"rows": 400},
        }
        self.assertEqual("progress=rows=200/400, eta=10s", forge.summary_progress(summary))

    def test_summary_eta_counts_claimed_work_without_claimed_rate(self) -> None:
        summary = {
            "active": [
                {
                    "state": "running",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 5.0,
                    },
                },
                {
                    "state": "claimed",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 0,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 10.0,
                    },
                },
            ],
            "active_count": 2,
            "counts": {"running": 1, "claimed": 1, "stale": 0, "pending": 0},
            "progress_done": {"rows": 0},
            "progress_totals": {"rows": 200},
        }
        self.assertEqual("progress=rows=50/200, eta=15s", forge.summary_progress(summary))

    def test_summary_eta_ignores_stale_rate_but_counts_stale_work(self) -> None:
        summary = {
            "active": [
                {
                    "state": "running",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 50,
                        "total": 100,
                        "unit": "rows",
                        "rate": 10.0,
                        "eta_s": 5.0,
                    },
                },
                {
                    "state": "stale",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 0,
                        "total": 100,
                        "unit": "rows",
                        "rate": 0.1,
                        "eta_s": 1000.0,
                    },
                },
            ],
            "active_count": 2,
            "counts": {"running": 1, "claimed": 0, "stale": 1, "pending": 0},
            "progress_done": {"rows": 0},
            "progress_totals": {"rows": 200},
        }
        self.assertEqual("progress=rows=50/200, eta=15s", forge.summary_progress(summary))

    def test_status_speedup_requires_task_rates(self) -> None:
        summary = {
            "run": "run-a",
            "manifest": "runs/run-a/manifest.json",
            "tasks": 2,
            "counts": {"done": 0, "fail": 0, "running": 2, "claimed": 0, "stale": 0, "pending": 0},
            "active_count": 2,
            "active": [{"worker": "fast"}, {"worker": "slow"}],
            "groups": {"done": {}, "fail": {}, "stale": {}},
            "pending": [],
            "pending_groups": {},
            "worker_outputs": {
                "fast": {"output": 80000, "output_seen": True},
                "slow": {"output": 10000, "output_seen": True},
            },
            "worker_rates": {
                "fast": 10.0,
                "slow": 10.0,
            },
            "progress_totals": {},
            "progress_done": {},
            "progress_outputs": {},
            "rows": [],
            "diagnostics": [],
            "metadata": {},
        }
        self.assertEqual(
            "workers=2 output=90k min=slow:10k max=fast:80k",
            forge.active_worker_summary(summary),
        )
        self.assertEqual("", forge.run_speedup_field(summary))
        payload = forge.run_status_payload(summary)
        self.assertNotIn("speedup", payload["display"]["active_summary"])
        self.assertNotIn("speedup_text", payload["display"]["active_summary"])
        self.assertEqual("90k", payload["display"]["active_summary"]["output_text"])
        self.assertEqual("slow", payload["sections"]["active"]["summary"]["min"]["worker"])

    def test_status_speedup_uses_progress_rates_without_output(self) -> None:
        summary = {
            "run": "run-a",
            "manifest": "runs/run-a/manifest.json",
            "tasks": 3,
            "counts": {"done": 0, "fail": 0, "running": 3, "claimed": 0, "stale": 0, "pending": 0},
            "active_count": 3,
            "active": [
                {
                    "worker": "fast",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 8,
                        "total": 50,
                        "unit": "games",
                        "rate": 2.0,
                    },
                },
                {
                    "worker": "fast",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 6,
                        "total": 50,
                        "unit": "games",
                        "rate": 1.0,
                    },
                },
                {
                    "worker": "slow",
                    "progress_data": {
                        "kind": "metadata",
                        "done": 4,
                        "total": 50,
                        "unit": "games",
                        "rate": 1.0,
                    },
                },
            ],
            "groups": {"done": {}, "fail": {}, "stale": {}},
            "pending": [],
            "pending_groups": {},
            "worker_outputs": {},
            "worker_rates": {},
            "progress_totals": {},
            "progress_done": {},
            "progress_outputs": {},
            "rows": [],
            "diagnostics": [],
            "metadata": {},
        }
        self.assertEqual(
            "workers=2 rate=4/s min=slow:1/s max=fast:3/s",
            forge.active_worker_summary(summary),
        )
        self.assertEqual("speedup=1.3x", forge.run_speedup_field(summary))
        payload = forge.run_status_payload(summary)
        self.assertEqual(1.333, payload["display"]["active_summary"]["speedup"])
        self.assertEqual("1.3x", payload["display"]["active_summary"]["speedup_text"])
        self.assertEqual("rate", payload["display"]["active_summary"]["rate_name"])
        self.assertEqual("4/s", payload["display"]["active_summary"]["rate_text"])
        self.assertNotIn("speedup=", payload["display"]["active_summary_text"])
        self.assertIn("speedup=1.3x", payload["display"]["fields"])

    def test_status_speedup_uses_fastest_host_baseline(self) -> None:
        summary = {
            "active": [
                {
                    "worker": f"worker-{index % 5}",
                    "progress_data": {"kind": "metadata", "rate": 1.0},
                }
                for index in range(44)
            ],
            "worker_outputs": {},
            "worker_rates": {},
            "metadata": {},
        }

        self.assertEqual(44 / 9, forge.run_speedup_value(summary))
        self.assertEqual("speedup=4.9x", forge.run_speedup_field(summary))

    def test_status_speedup_uses_fastest_host_rate(self) -> None:
        summary = {
            "active": [
                {"worker": "host-a", "progress_data": {"kind": "metadata", "rate": 2.0}},
                {"worker": "host-b", "progress_data": {"kind": "metadata", "rate": 1.0}},
                {"worker": "host-c", "progress_data": {"kind": "metadata", "rate": 2.0}},
            ],
            "worker_outputs": {},
            "worker_rates": {},
            "metadata": {},
        }
        self.assertEqual(2.5, forge.run_speedup_value(summary))

    def test_status_single_worker_summary_omits_redundant_fields(self) -> None:
        summary = {
            "active": [{"worker": "solo"}],
            "worker_outputs": {
                "solo": {"output": 67000, "output_seen": True},
            },
            "worker_rates": {
                "solo": 10.0,
            },
        }
        self.assertEqual("output=67k", forge.active_worker_summary(summary))

    def test_status_normalizes_localhost_worker_name(self) -> None:
        with mock.patch("forge_lib.status.socket.gethostname", return_value="coordinator"):
            self.assertEqual("coordinator", forge.worker_group_name("localhost"))
            self.assertEqual("coordinator", forge.worker_group_name("127.0.0.1"))
            self.assertEqual("coordinator", forge.worker_group_name("::1"))

    def test_status_infers_eta_from_elapsed_declared_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "selfplay"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.done.json").write_text(json.dumps({"task": "task_0"}), encoding="utf-8")
            (state / "task_1.running.json").write_text(
                json.dumps({"task": "task_1", "worker": "worker-a", "started_epoch": 1000.0}),
                encoding="utf-8",
            )
            (logs / "task_1.log").write_text("[1/2 500/1000]\n", encoding="utf-8")

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1100.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=True,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                text = stdout.getvalue()
                self.assertIn("  games=1500/2000 tasks=1/2 eta=1m40s", text)
                self.assertIn("  active: 1", text)
                self.assertNotIn("  active: 1 games=1500/2000", text)
                self.assertIn("worker-a shard=1 [games=500/1000, eta=1m40s]", text)

    def test_status_prefers_active_heartbeat_progress_over_stale_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            claims = root / "claims"
            state.mkdir()
            logs.mkdir()
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "claim_dir": str(claims),
                "tasks": [{
                    "id": "sprt_0049",
                    "index": 49,
                    "command": "true",
                    "outputs": [],
                    "progress": {
                        "unit": "games",
                        "total": 40,
                        "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                    },
                }],
            }
            task = manifest["tasks"][0]
            forge.write_json(
                forge.task_running_path(manifest, task),
                {"task": "sprt_0049", "worker": "pwa-5090", "started_epoch": 1000.0},
            )
            claim_payload = {
                "task": "sprt_0049",
                "claim_id": "claim-49",
                "worker": "pwa-5090",
                "claimed_epoch": 1000.0,
                "heartbeat_epoch": 1100.0,
                "lease_seconds": 600,
            }
            forge.task_claim_dir(manifest, task).mkdir(parents=True)
            forge.write_json(forge.task_claim_path(manifest, task), claim_payload)
            forge.write_json(
                forge.task_heartbeat_path(manifest, task),
                {
                    **claim_payload,
                    "progress_data": {
                        "kind": "metadata",
                        "done": 28,
                        "total": 40,
                        "unit": "games",
                        "rate": 0.2,
                    },
                },
            )
            (logs / "sprt_0049.log").write_text("[1/1 2/40]\n", encoding="utf-8")

            with mock.patch("forge_lib.tasks.epoch", return_value=1100.0):
                summary = forge.status_summary(manifest, lease_seconds=600)

            self.assertEqual("games=28/40, eta=1m00s", summary["active"][0]["progress"])

    def test_status_counts_failed_task_log_progress_as_observed_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            state.mkdir()
            logs.mkdir()
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "sprt_0000",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 64,
                            "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                    {
                        "id": "sprt_0001",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 64,
                            "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                ],
            }
            (state / "sprt_0000.fail.json").write_text(
                json.dumps({"task": "sprt_0000", "host": "worker"}),
                encoding="utf-8",
            )
            (logs / "sprt_0000.log").write_text("[20/64]\n", encoding="utf-8")

            summary = forge.status_summary(manifest, lease_seconds=600)

            self.assertEqual({"done": 0, "fail": 1, "running": 0, "claimed": 0, "stale": 0, "pending": 1}, summary["counts"])
            self.assertEqual({"games": 20}, summary["progress_done"])
            self.assertEqual("progress=games=20/128", forge.summary_progress(summary))
            failed_row = next(row for row in summary["rows"] if row["id"] == "sprt_0000")
            self.assertEqual("fail", failed_row["state"])
            self.assertEqual("games=20/64", failed_row["progress"])

    def test_status_counts_pending_output_log_progress_as_observed_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            outputs = root / "outputs"
            state.mkdir()
            logs.mkdir()
            outputs.mkdir()
            output_log = outputs / "sprt_0001.log"
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "sprt_0000",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 64,
                            "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                    {
                        "id": "sprt_0001",
                        "index": 1,
                        "command": "true",
                        "outputs": [str(output_log)],
                        "progress": {
                            "unit": "games",
                            "total": 64,
                            "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                ],
            }
            output_log.write_text("[22/64]\n", encoding="utf-8")

            summary = forge.status_summary(manifest, lease_seconds=600)

            self.assertEqual({"done": 0, "fail": 0, "running": 0, "claimed": 0, "stale": 0, "pending": 2}, summary["counts"])
            self.assertEqual({"games": 22}, summary["progress_done"])
            self.assertEqual("progress=games=22/128", forge.summary_progress(summary))
            pending_row = next(row for row in summary["rows"] if row["id"] == "sprt_0001")
            self.assertEqual("pending", pending_row["state"])
            self.assertEqual("games=22/64", pending_row["progress"])

    def test_status_counts_only_observed_progress_for_soft_cancelled_done_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            state.mkdir()
            logs.mkdir()
            manifest = {
                "schema": "forge.task.v1",
                "name": "sprt",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": f"sprt_{index:04d}",
                        "index": index,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 25,
                            "log_regex": r"\[\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    }
                    for index in range(2)
                ],
            }
            for index in range(2):
                (state / f"sprt_{index:04d}.done.json").write_text(
                    json.dumps({"task": f"sprt_{index:04d}", "soft_cancel": True, "skipped": True}),
                    encoding="utf-8",
                )
            (logs / "sprt_0000.log").write_text("[10/25]\n", encoding="utf-8")

            summary = forge.status_summary(manifest, lease_seconds=600)

            self.assertEqual(2, summary["counts"]["done"])
            self.assertEqual({"games": 10}, summary["progress_done"])
            self.assertEqual("progress=games=10/50", forge.summary_progress(summary))

    def test_status_uses_completed_rate_when_running_task_has_no_progress_yet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "selfplay"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.done.json").write_text(
                json.dumps({"task": "task_0", "elapsed_s": 100.0}),
                encoding="utf-8",
            )
            (state / "task_1.running.json").write_text(
                json.dumps({"task": "task_1", "worker": "worker-a", "started_epoch": 1000.0}),
                encoding="utf-8",
            )
            task = manifest["tasks"][1]
            forge.task_claim_dir(manifest, task).mkdir(parents=True)
            forge.write_json(
                forge.task_heartbeat_path(manifest, task),
                {
                    "task": "task_1",
                    "worker": "worker-a",
                    "heartbeat_epoch": 1001.0,
                    "progress_data": {
                        "kind": "metadata",
                        "done": 0,
                        "total": 1000,
                        "unit": "games",
                    },
                },
            )
            (logs / "task_1.log").write_text("", encoding="utf-8")

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1001.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=True,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                text = stdout.getvalue()
                self.assertIn("  games=1000/2000 tasks=1/2 eta=1m40s", text)
                self.assertIn("  active: 1", text)
                self.assertNotIn("  active: 1 games=1000/2000", text)
                self.assertIn("worker-a shard=1 [games=0/1000, eta~=1m40s]", text)

    def test_status_labels_overdue_fallback_eta_as_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "selfplay"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "selfplay",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [
                    {
                        "id": "task_0",
                        "index": 0,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                    {
                        "id": "task_1",
                        "index": 1,
                        "command": "true",
                        "outputs": [],
                        "progress": {
                            "unit": "games",
                            "total": 1000,
                            "log_regex": r"\[\s*\d+/\d+\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\]",
                        },
                    },
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.done.json").write_text(
                json.dumps({"task": "task_0", "elapsed_s": 100.0}),
                encoding="utf-8",
            )
            (state / "task_1.running.json").write_text(
                json.dumps({"task": "task_1", "worker": "worker-a", "started_epoch": 1000.0}),
                encoding="utf-8",
            )
            (logs / "task_1.log").write_text("", encoding="utf-8")

            with self.patch_runs_dir(runs), mock.patch("forge_lib.status.epoch", return_value=1110.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                            verbose=True,
                            show_done=False,
                        )
                )
                self.assertEqual(0, rc)
                text = stdout.getvalue()
                self.assertIn("  games=1000/2000 tasks=1/2 eta=1m40s", text)
                self.assertIn("  active: 1", text)
                self.assertNotIn("  active: 1 games=1000/2000", text)
                self.assertIn("worker-a shard=1 [games=0/1000, eta~=1m40s]", text)

    def test_status_ignores_undeclared_domain_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "score"
            state = run / "state"
            logs = run / "logs"
            state.mkdir(parents=True)
            logs.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "score",
                "work_dir": str(run),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker"}))
            (logs / "task_0.log").write_text(
                "shard 0/24: selected=50000 written=48010 rate=6.7/s\n",
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_status(
                        argparse.Namespace(
                            run=None,
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                        )
                    )
                self.assertEqual(0, rc)
                self.assertNotIn("progress=", stdout.getvalue())

    def test_stop_prevents_new_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": [str(run_dir / "outputs/0.txt")]},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": [str(run_dir / "outputs/1.txt")]},
                ],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                loaded = forge.load_manifest(run_dir / "manifest.json")
                claimed = forge.claim_task(
                    loaded,
                    worker="worker",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )
                self.assertEqual("task_0", claimed["task"])
                task = loaded["tasks"][0]
                forge.write_json(forge.task_running_path(loaded, task), {
                    "task": "task_0",
                    "worker": "worker",
                })

                stdout = io.StringIO()
                with mock.patch("forge_lib.status.status_summary", return_value={"counts": {"running": 0, "claimed": 0, "stale": 0}}):
                    with contextlib.redirect_stdout(stdout):
                        stop = forge.cmd_stop(argparse.Namespace(run="active", all=False, lease_seconds=600, interval_seconds=0.0))
                self.assertEqual(0, stop)
                self.assertIn("stop requested: active waiting", stdout.getvalue())
                self.assertIn("stop complete: active", stdout.getvalue())
                self.assertTrue(forge.task_claim_dir(loaded, task).exists())
                self.assertTrue(forge.task_running_path(loaded, task).exists())
                result = forge.claim_task(
                    forge.load_manifest(run_dir / "manifest.json"),
                    worker="worker",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )
                self.assertIsNone(result)

    def test_claim_prefers_shards_assigned_to_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "runs" / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "claim_worker": "worker-a", "command": "true", "outputs": []},
                    {"id": "task_1", "index": 1, "claim_worker": "worker-b", "command": "true", "outputs": []},
                    {"id": "task_2", "index": 2, "claim_worker": "worker-a", "command": "true", "outputs": []},
                ],
            }
            forge.state_dir(manifest).mkdir(parents=True)
            forge.log_dir(manifest).mkdir(parents=True)

            claimed = forge.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )

            self.assertEqual("task_1", claimed["task"])

    def test_materialized_run_assigns_claim_workers_by_expanded_slot_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "slot-order",
                    "work_dir": str(root),
                    "tasks": [
                        {"id": f"task_{index}", "index": index, "command": "true", "outputs": []}
                        for index in range(7)
                    ],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                _run_dir, manifest_path, _workers_path = forge.materialize_run(
                    template_path=template,
                    workers_arg="worker-a:2,worker-b:1",
                    comment="ordered smoke",
                    run_name_arg=None,
                    resume=False,
                    replace=False,
                )

            materialized = forge.read_json(manifest_path)
            self.assertEqual("ordered smoke", materialized["comment"])
            self.assertEqual(
                ["worker-a", "worker-a", "worker-b", "worker-a", "worker-a", "worker-b", "worker-a"],
                [task.get("claim_worker") for task in materialized["tasks"]],
            )

    def test_stop_named_run_delegates_to_coordinator(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote stop\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
        ), mock.patch("forge.subprocess.run", return_value=remote_result) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = forge.cmd_stop(argparse.Namespace(run="active", all=False, lease_seconds=600))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote stop\n",
            stdout.getvalue(),
        )
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("pwa-5090", command)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", command[-1])
        self.assertIn("FORGE_COORDINATOR_RUNS=", command[-1])
        self.assertIn("forge stop active", command[-1])

    def test_stop_clears_queued_launch_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            run = self.write_retry_run(runs, "queued", failed=None)
            manifest = forge.load_manifest(run / "manifest.json")
            forge.write_json(forge.queue_request_path(manifest), {"run": "queued"})
            forge.write_json(forge.launch_request_path(manifest), {"run": "queued"})

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_stop(argparse.Namespace(run="queued", all=False, lease_seconds=600, interval_seconds=0.0))

            self.assertEqual(0, rc)
            self.assertTrue(forge.stop_request_path(manifest).exists())
            self.assertFalse(forge.queue_request_path(manifest).exists())
            self.assertFalse(forge.launch_request_path(manifest).exists())
            self.assertIn("stop requested: queued waiting; cleared=queue,launch", stdout.getvalue())
            self.assertIn("stop complete: queued", stdout.getvalue())

    def test_stop_force_kills_active_workers_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": [str(run_dir / "outputs/0.txt")]},
                ],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                loaded = forge.load_manifest(run_dir / "manifest.json")
                claimed = forge.claim_task(
                    loaded,
                    worker="worker",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )
                self.assertEqual("task_0", claimed["task"])
                task = loaded["tasks"][0]
                forge.write_json(forge.task_running_path(loaded, task), {
                    "task": "task_0",
                    "worker": "worker",
                })

                stdout = io.StringIO()
                with mock.patch("forge_lib.workers.kill_run_workers", return_value=2) as kill_workers:
                    with contextlib.redirect_stdout(stdout):
                        rc = forge.cmd_stop(argparse.Namespace(run="active", all=False, lease_seconds=600, force=True))

                self.assertEqual(0, rc)
                kill_workers.assert_called_once()
                self.assertIn("stop requested: active", stdout.getvalue())
                self.assertIn("killed=2", stdout.getvalue())
                self.assertFalse(forge.task_claim_dir(loaded, task).exists())
                self.assertFalse(forge.task_running_path(loaded, task).exists())

    def test_drain_prevents_worker_claims_until_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": [str(run_dir / "outputs/0.txt")]},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": [str(run_dir / "outputs/1.txt")]},
                ],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                drain = forge.cmd_drain(
                    argparse.Namespace(run="active", all=False, worker="worker-a", clear=False, lease_seconds=600)
                )
                self.assertEqual(0, drain)
                loaded = forge.load_manifest(run_dir / "manifest.json")
                self.assertIsNone(forge.claim_task(
                    loaded,
                    worker="worker-a",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                ))
                self.assertEqual("task_0", forge.claim_task(
                    loaded,
                    worker="worker-b",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )["task"])

                clear = forge.cmd_drain(
                    argparse.Namespace(run="active", all=False, worker="worker-a", clear=True, lease_seconds=600)
                )
                self.assertEqual(0, clear)
                self.assertEqual("task_1", forge.claim_task(
                    loaded,
                    worker="worker-a",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )["task"])

    def test_release_worker_claims_skips_running_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-b", "main"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=root, check=True)
            (root / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=root, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=root, check=True, stdout=subprocess.DEVNULL)
            manifest = root / "manifest.json"
            plan = self.run_cli(
                "plan",
                "--name", "release-worker",
                "--shards", "3",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf x > {output}",
                "--output-template", str(root / "out.{index}.txt"),
            )
            self.assertEqual(0, plan.returncode)
            loaded = forge.load_manifest(manifest)

            first = forge.claim_task(
                loaded,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            second = forge.claim_task(
                loaded,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            third = forge.claim_task(
                loaded,
                worker="worker-b",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            self.assertEqual("task_0000", first["task"])
            self.assertEqual("task_0001", second["task"])
            self.assertEqual("task_0002", third["task"])

            running = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0001",
                "--worker", "worker-a",
                "--claim-id", second["claim"]["claim_id"],
                "--running",
            )
            self.assertEqual("", running.stderr)
            self.assertEqual(0, running.returncode)

            release = self.run_cli(
                "release",
                "--manifest", str(manifest),
                "--worker", "worker-a",
            )
            self.assertEqual("", release.stderr)
            self.assertEqual(0, release.returncode)
            self.assertEqual("task_0000\n", release.stdout)

            status = self.run_cli("status", str(root))
            self.assertEqual(0, status.returncode)
            self.assertIn("active: 2", status.stdout)
            self.assertIn("worker-a shard=1", status.stdout)
            self.assertIn("worker-b [claimed] shard=2", status.stdout)
            self.assertIn("pending: 1", status.stdout)

    def test_delete_done_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "finished"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "finished",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                rc = forge.cmd_del(argparse.Namespace(run=None, done=True, force=True, lease_seconds=600))
                state = json.loads((runs / ".web-state.json").read_text(encoding="utf-8"))

            self.assertEqual(0, rc)
            self.assertFalse(run_dir.exists())
            self.assertEqual([], state["hidden_runs"])
            self.assertEqual(["finished"], state["deleted_runs"])
            self.assertEqual({}, state["lingering_runs"])

    def test_delete_all_keeps_active_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            states = ["done", "failed", "stopped", "empty", "current", "queued", "stalled"]
            summaries = []
            for state in states:
                run_dir = root / state
                run_dir.mkdir()
                summaries.append({"manifest": str(run_dir / "manifest.json"), "test_state": state})
            args = forge.build_parser().parse_args(["del", "--all", "--force"])
            self.assertIn("--all", forge.state_control_command_args("del", args))

            with (
                mock.patch.dict(os.environ, {"FORGE_COORDINATOR_RUNS": ""}),
                mock.patch("forge_lib.status.configured_coordinator_runs", return_value=None),
                mock.patch("forge_lib.status.load_run_summaries", return_value=summaries),
                mock.patch("forge_lib.status.run_state", side_effect=lambda item: item["test_state"]),
                mock.patch("forge_lib.status.mark_deleted_run_for_web") as mark_deleted,
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                self.assertEqual(0, forge.cmd_del(args))

            for state in states[:4]:
                self.assertFalse((root / state).exists())
            for state in states[4:]:
                self.assertTrue((root / state).exists())
            self.assertEqual(states[:4], [call.args[0] for call in mark_deleted.call_args_list])
            self.assertIn("kept active: current, queued, stalled", stdout.getvalue())

    def test_delete_named_run_delegates_to_coordinator(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote delete\n",
            stderr="",
        )
        with tempfile.TemporaryDirectory() as tmp:
            runs = Path(tmp) / "runs"
            with self.patch_runs_dir(runs), mock.patch.dict(
                os.environ,
                {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
            ), mock.patch("forge.subprocess.run", return_value=remote_result) as run_mock:
                forge.write_json(runs / ".web-state.json", {
                    "seen_runs": {"queued": {"source": "status"}},
                    "lingering_runs": {"queued": {"run": "queued", "state": "done"}},
                    "hidden_runs": ["queued"],
                    "command_checked_runs": ["queued"],
                })
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_del(argparse.Namespace(run="queued", done=False, force=True, lease_seconds=600))
                state = json.loads((runs / ".web-state.json").read_text(encoding="utf-8"))

        self.assertEqual(0, rc)
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs\nremote delete\n",
            stdout.getvalue(),
        )
        self.assertEqual({}, state["seen_runs"])
        self.assertEqual({}, state["lingering_runs"])
        self.assertEqual([], state["hidden_runs"])
        self.assertEqual(["queued"], state["deleted_runs"])
        self.assertEqual([], state["command_checked_runs"])
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("pwa-5090", command)
        self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", command[-1])
        self.assertIn("FORGE_COORDINATOR_RUNS=", command[-1])
        self.assertIn("forge del queued --force", command[-1])

    def test_multi_repo_and_command_requirements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo_a = root / "repo-a"
            repo_b = root / "repo-b"
            for repo in (repo_a, repo_b):
                subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
                subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
                subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
                (repo / "file.txt").write_text(repo.name, encoding="utf-8")
                subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
                subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            commit_a = subprocess.check_output(["git", "-C", str(repo_a), "rev-parse", "HEAD"], text=True).strip()
            commit_b = subprocess.check_output(["git", "-C", str(repo_b), "rev-parse", "HEAD"], text=True).strip()

            manifest = {
                "schema": "forge.task.v1",
                "name": "multi-req",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "python_min": "3.11",
                    "repos": [
                        {"name": "repo-a", "path": str(repo_a), "commit": commit_a},
                        {"name": "repo-b", "path": str(repo_b), "commit": commit_b},
                    ],
                    "commands": [
                        {"name": "python", "path": sys.executable},
                        {
                            "name": "project-version-probe",
                            "command": "printf 'Project Tool v.test\\n'",
                            "contains": "Project Tool",
                        },
                    ],
                },
                "tasks": [],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            doctor = self.run_cli("doctor", "--manifest", str(manifest_path))
            self.assertEqual("", doctor.stderr)
            self.assertEqual(0, doctor.returncode)

            (repo_b / "file.txt").write_text("changed", encoding="utf-8")
            subprocess.run(["git", "commit", "-am", "change"], cwd=repo_b, check=True, stdout=subprocess.DEVNULL)
            rejected = self.run_cli("doctor", "--manifest", str(manifest_path))
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("repo-b: git commit", rejected.stderr)

    def test_add_repo_records_setup_hooks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            (repo / "tool.sh").write_text("#!/bin/sh\nprintf 'tool ok\\n'\n", encoding="utf-8")
            subprocess.run(["chmod", "+x", "tool.sh"], cwd=repo, check=True)
            subprocess.run(["git", "add", "tool.sh"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "tool"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()

            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "repo-hooks",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {},
                    "tasks": [],
                }),
                encoding="utf-8",
            )

            add = self.run_cli(
                "add-repo",
                "--manifest", str(manifest),
                "--name", "tool",
                "--path", str(repo),
                "--commit", commit[:8],
                "--install", "mkdir -p install && cp tool.sh install/tool",
                "--probe", "install/tool | grep 'tool ok'",
                "--probe-timeout-s", "5",
            )
            self.assertEqual("", add.stderr)
            self.assertEqual(0, add.returncode)

            sync = self.run_cli("sync-repos", "--manifest", str(manifest))
            self.assertEqual("", sync.stderr)
            self.assertEqual(0, sync.returncode)
            self.assertTrue((repo / "install/tool").exists())

    def test_add_command_records_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "command-probe",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {},
                    "tasks": [],
                }),
                encoding="utf-8",
            )

            add = self.run_cli(
                "add-command",
                "--manifest", str(manifest),
                "--name", "python-version",
                "--path", sys.executable,
                "--command", f"{sys.executable} --version",
                "--contains", "Python",
                "--timeout-s", "5",
            )
            self.assertEqual("", add.stderr)
            self.assertEqual(0, add.returncode)

            doctor = self.run_cli("doctor", "--manifest", str(manifest))
            self.assertEqual("", doctor.stderr)
            self.assertEqual(0, doctor.returncode)

    def test_sync_repos_checks_out_declared_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            (repo / "file.txt").write_text("one", encoding="utf-8")
            subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            first = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            (repo / "file.txt").write_text("two", encoding="utf-8")
            subprocess.run(["git", "commit", "-am", "two"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            manifest = {
                "schema": "forge.task.v1",
                "name": "sync-repos",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "repos": [{"name": "repo", "path": str(repo), "commit": first}],
                },
                "tasks": [],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            sync = self.run_cli("sync-repos", "--manifest", str(manifest_path))
            self.assertEqual("", sync.stderr)
            self.assertEqual(0, sync.returncode)
            head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(first, head)

    def test_sync_repos_accepts_short_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            (repo / "file.txt").write_text("one", encoding="utf-8")
            subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            first = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            (repo / "file.txt").write_text("two", encoding="utf-8")
            subprocess.run(["git", "commit", "-am", "two"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            manifest = {
                "schema": "forge.task.v1",
                "name": "sync-short",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "repos": [{"name": "repo", "path": str(repo), "commit": first[:7]}],
                },
                "tasks": [],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            sync = self.run_cli("sync-repos", "--manifest", str(manifest_path))
            self.assertEqual("", sync.stderr)
            self.assertEqual(0, sync.returncode)

            doctor = self.run_cli("doctor", "--manifest", str(manifest_path))
            self.assertEqual("", doctor.stderr)
            self.assertEqual(0, doctor.returncode)
            head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(first, head)

    def test_sync_repos_can_ignore_untracked_files_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo"
            subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            tracked = repo / "file.txt"
            tracked.write_text("one", encoding="utf-8")
            subprocess.run(["git", "add", "file.txt"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
            (repo / "build.log").write_text("generated\n", encoding="utf-8")

            manifest = {
                "schema": "forge.task.v1",
                "name": "sync-untracked",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "repos": [{
                        "name": "repo",
                        "path": str(repo),
                        "commit": commit,
                        "allow_untracked": True,
                    }],
                },
                "tasks": [],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            sync = self.run_cli("sync-repos", "--manifest", str(manifest_path))
            self.assertEqual("", sync.stderr)
            self.assertEqual(0, sync.returncode)

            tracked.write_text("tracked edit\n", encoding="utf-8")
            rejected = self.run_cli("sync-repos", "--manifest", str(manifest_path))
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("repo: git worktree is not clean", rejected.stderr)

    def test_sync_repos_clones_and_runs_setup_hooks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            subprocess.run(["git", "init", "-b", "main", str(source)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=source, check=True)
            (source / "tool.sh").write_text("#!/bin/sh\nprintf 'tool ok\\n'\n", encoding="utf-8")
            subprocess.run(["chmod", "+x", "tool.sh"], cwd=source, check=True)
            subprocess.run(["git", "add", "tool.sh"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "tool"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()

            checkout = root / "checkout"
            install_dir = root / "bin"
            manifest = {
                "schema": "forge.task.v1",
                "name": "clone-setup",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "requirements": {
                    "repos": [{
                        "name": "tool-repo",
                        "url": str(source),
                        "path": str(checkout),
                        "commit": commit,
                        "build": "mkdir -p build && cp tool.sh build/tool",
                        "install": f"mkdir -p {install_dir} && cp build/tool {install_dir}/tool",
                        "probe": f"{install_dir}/tool | grep 'tool ok'",
                    }],
                },
                "tasks": [],
            }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            sync = self.run_cli("sync-repos", "--manifest", str(manifest_path))
            self.assertEqual("", sync.stderr)
            self.assertEqual(0, sync.returncode)
            self.assertTrue((install_dir / "tool").exists())

            doctor = self.run_cli("doctor", "--manifest", str(manifest_path))
            self.assertEqual("", doctor.stderr)
            self.assertEqual(0, doctor.returncode)

    def test_worker_preflight_rejects_before_claiming(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            subprocess.run(["git", "init", "-b", "main", str(coordinator)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=coordinator, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=coordinator, check=True)
            (coordinator / "input.txt").write_text("fixed\n", encoding="utf-8")
            subprocess.run(["git", "add", "input.txt"], cwd=coordinator, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=coordinator, check=True, stdout=subprocess.DEVNULL)

            worker = root / "worker"
            subprocess.run(["git", "clone", str(coordinator), str(worker)], check=True, stdout=subprocess.DEVNULL)
            wrong_commit = "0" * 40
            manifest = {
                "schema": "forge.task.v1",
                "name": "preflight",
                "work_dir": str(coordinator),
                "state_dir": str(coordinator / "state"),
                "log_dir": str(coordinator / "logs"),
                "claim_dir": str(coordinator / "claims"),
                "path_maps": {
                    "worker": {
                        str(coordinator): str(worker),
                    },
                },
                "requirements": {
                    "repos": [
                        {"name": "project", "path": str(coordinator), "commit": wrong_commit},
                    ],
                },
                "tasks": [{
                    "id": "task_0000",
                    "index": 0,
                    "command": "printf x > {output}",
                    "outputs": [str(coordinator / "out.txt")],
                }],
            }
            manifest_path = coordinator / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            work = self.run_cli(
                "work",
                "--manifest", str(manifest_path),
                "--coordinator", str(manifest_path),
                "--profile", "worker",
                "--worker", "test-worker",
                "--max-tasks", "1",
            )
            self.assertNotEqual(0, work.returncode)
            self.assertIn("project: git commit", work.stderr)

            status = self.run_cli("status", str(coordinator))
            self.assertEqual(0, status.returncode)
            self.assertIn("  tasks=0/1", status.stdout)
            self.assertIn("  pending: 1", status.stdout)

    def test_version_reports_current_checkout(self) -> None:
        result = self.run_cli("version", "--json")
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)
        payload = json.loads(result.stdout)
        self.assertEqual("forge", payload["tool"])
        self.assertIn("commit", payload)
        self.assertIn("repo", payload)

    def test_version_payload_uses_current_branch_remote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            subprocess.run(["git", "init", "-b", "develop", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            subprocess.run(["git", "config", "branch.develop.remote", "origin"], cwd=repo, check=True)
            (repo / "forge.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "forge.py"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            import forge_lib.core as forge_core

            original_file = forge_core.__file__
            try:
                forge_core.__file__ = str(repo / "forge.py")
                payload = forge.forge_version_payload()
            finally:
                forge_core.__file__ = original_file

            self.assertEqual("develop", payload["branch"])
            self.assertEqual("origin", payload["remote"])

    def test_self_update_fast_forwards_forge_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            clone = root / "clone"
            subprocess.run(["git", "init", "-b", "main", str(source)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=source, check=True)
            (source / "forge.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "forge.py"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "clone", str(source), str(clone)], check=True, stdout=subprocess.DEVNULL)
            (source / "marker.txt").write_text("two", encoding="utf-8")
            subprocess.run(["git", "add", "marker.txt"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "two"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            expected = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()

            import forge_lib.core as forge_core

            original_file = forge_core.__file__
            try:
                forge_core.__file__ = str(clone / "forge.py")
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_self_update(
                        argparse.Namespace(
                            remote="origin",
                            branch=None,
                            require_clean=True,
                            json=True,
                        )
                    )
            finally:
                forge_core.__file__ = original_file
            self.assertEqual(0, rc)
            head = subprocess.check_output(["git", "-C", str(clone), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(expected, head)

    def test_update_matches_workers_to_coordinator_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "localhost", "local": True, "name": "local-a"},
                        {"host": "remote-a"},
                    ],
                }),
                encoding="utf-8",
            )
            result = subprocess.CompletedProcess(
                args=["sh"],
                returncode=0,
                stdout="forge abc main\n",
                stderr="",
            )
            with mock.patch("forge_lib.workers.run_worker_shell", return_value=result) as run_mock:
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_update(
                        argparse.Namespace(
                            workers=str(workers),
                            all=False,
                            remote="origin",
                            branch="main",
                            require_clean=True,
                            timeout_seconds=30.0,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertEqual(2, run_mock.call_count)
            first_script = run_mock.call_args_list[0].args[1]
            self.assertIn("git -C \"$repo\" fetch origin main", first_script)
            self.assertIn("git -C \"$repo\" merge --ff-only", first_script)
            self.assertIn("test \"$(git -C \"$repo\" rev-parse HEAD)\" =", first_script)
            self.assertIn("localhost: OK forge abc main", stdout.getvalue())
            self.assertIn("remote-a: OK forge abc main", stdout.getvalue())

    def test_update_uses_default_workers_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            runs.mkdir()
            workers = runs / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True}],
                }),
                encoding="utf-8",
            )
            result = subprocess.CompletedProcess(
                args=["sh"],
                returncode=0,
                stdout="forge abc main\n",
                stderr="",
            )
            with (
                mock.patch("forge_lib.workers.config_workers_path", return_value=workers),
                mock.patch("forge_lib.workers.run_worker_shell", return_value=result) as run_mock,
            ):
                rc = forge.cmd_update(
                    argparse.Namespace(
                        workers=None,
                        all=False,
                        remote="origin",
                        branch="main",
                        require_clean=True,
                        timeout_seconds=30.0,
                    )
                )

            self.assertEqual(0, rc)
            self.assertEqual(1, run_mock.call_count)

    def test_update_deduplicates_worker_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "pwa-5090", "profile": "linux", "slots": 8},
                        {"host": "pwa-rce", "profile": "linux", "slots": 2},
                    ],
                }),
                encoding="utf-8",
            )
            result = subprocess.CompletedProcess(
                args=["sh"],
                returncode=0,
                stdout="forge abc main\n",
                stderr="",
            )
            with mock.patch("forge_lib.workers.run_worker_shell", return_value=result) as run_mock:
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_update(
                        argparse.Namespace(
                            workers=str(workers),
                            all=False,
                            remote="origin",
                            branch="main",
                            require_clean=True,
                            timeout_seconds=30.0,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertEqual(2, run_mock.call_count)
            self.assertIn("pwa-5090: OK forge abc main", stdout.getvalue())
            self.assertIn("pwa-rce: OK forge abc main", stdout.getvalue())

    def test_remote_forge_command_uses_path(self) -> None:
        command = forge.remote_forge_command(["status", "run name"])

        self.assertIn("command -v forge", command)
        self.assertIn("exec forge status 'run name'", command)

    def test_check_coordinator_cli_fails_early(self) -> None:
        result = subprocess.CompletedProcess(["ssh"], 127, stdout="", stderr="forge not found")

        with mock.patch("forge_lib.tasks.run_coordinator_cli", return_value=result):
            with self.assertRaises(SystemExit) as raised:
                forge.check_coordinator_cli("pwa-5090:/tmp/manifest.json")

        self.assertIn("forge not found", str(raised.exception))

    def test_run_coordinator_cli_retries_transient_ssh_failure(self) -> None:
        calls = []

        def fake_run(command, **kwargs):
            calls.append(command)
            if len(calls) == 1:
                return subprocess.CompletedProcess(
                    command,
                    255,
                    stdout="ok\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

        with (
            mock.patch("forge_lib.core.run_process_command", side_effect=fake_run),
            mock.patch("forge_lib.core.SSH_TRANSIENT_RETRY_DELAY_SECONDS", 0.0),
        ):
            result = forge.run_coordinator_cli(
                "pwa-mbp:/tmp/manifest.json",
                ["heartbeat", "--manifest", "{manifest}", "--task", "task_0", "--worker", "worker-a"],
            )

        self.assertEqual(0, result.returncode)
        self.assertEqual(2, len(calls))
        self.assertEqual("ssh", calls[0][0])


    def test_load_workers_reads_workers_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        "pwa-5090",
                        {"host": "pwa-wsl", "profile": "pwa-wsl"},
                        {
                            "host": "pwa-mbp",
                            "profile": "mac",
                            "python": "/opt/homebrew/bin/python3",
                            "path_map": {
                                "/home/petter/code/cpp/chess": "/Users/pwahlman/code/cpp/chess",
                            },
                        },
                        {"host": "localhost", "local": True},
                        {"host": "disabled", "enabled": False},
                    ],
                }),
                encoding="utf-8",
            )

            workers = forge.load_workers(workers_path)

            self.assertEqual(
                [
                    {"host": "pwa-5090", "profile": "pwa-5090", "local": False},
                    {"host": "pwa-wsl", "profile": "pwa-wsl", "local": False},
                    {
                        "host": "pwa-mbp",
                        "profile": "mac",
                        "local": False,
                        "python": "/opt/homebrew/bin/python3",
                        "path_map": {
                            "/home/petter/code/cpp/chess": "/Users/pwahlman/code/cpp/chess",
                        },
                    },
                    {"host": "localhost", "profile": "localhost", "local": True},
                ],
                workers,
            )

    def test_load_workers_expands_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cache"
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {
                            "host": "pwa-5090",
                            "profile": "linux",
                            "cache_dir": str(cache_dir),
                            "slots": 2,
                        },
                    ],
                }),
                encoding="utf-8",
            )

            workers = forge.load_workers(workers_path)

            self.assertEqual(
                [
                    {
                        "host": "pwa-5090",
                        "profile": "linux",
                        "local": False,
                        "cache_dir": str(cache_dir),
                        "_slot": 1,
                        "_slots": 2,
                    },
                    {
                        "host": "pwa-5090",
                        "profile": "linux",
                        "local": False,
                        "cache_dir": str(cache_dir),
                        "_slot": 2,
                        "_slots": 2,
                    },
                ],
                workers,
            )

    def test_load_workers_arg_inline_csv_without_defaults_uses_eight_slots(self) -> None:
        with mock.patch("forge_lib.workers.default_workers_path", side_effect=SystemExit("no workers")):
            workers = forge.load_workers_arg("pwa-mbp,pwa-5090,localhost")

        self.assertEqual(24, len(workers))
        self.assertEqual({"pwa-mbp", "pwa-5090", "localhost"}, {worker["host"] for worker in workers})
        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], [worker["_slot"] for worker in workers if worker["host"] == "pwa-mbp"])
        self.assertTrue(all(worker["_slots"] == 8 for worker in workers))
        self.assertTrue(all(worker["local"] for worker in workers if worker["host"] == "localhost"))

    def test_load_workers_arg_inline_inherits_default_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {
                            "host": "pwa-mbp",
                            "profile": "mac",
                            "python": "/opt/homebrew/bin/python3",
                            "cache_dir": str(Path(tmp) / "cache"),
                            "slots": 2,
                        },
                        {"host": "pwa-5090", "profile": "linux", "slots": 4},
                    ],
                }),
                encoding="utf-8",
            )

            with mock.patch("forge_lib.workers.default_workers_path", return_value=workers_path):
                workers = forge.load_workers_arg("pwa-mbp,pwa-5090:1")

        self.assertEqual(3, len(workers))
        self.assertEqual(["pwa-mbp", "pwa-mbp", "pwa-5090"], [worker["host"] for worker in workers])
        self.assertEqual(["mac", "mac", "linux"], [worker["profile"] for worker in workers])
        self.assertEqual([1, 2], [worker["_slot"] for worker in workers if worker["host"] == "pwa-mbp"])
        self.assertTrue(all(worker.get("python") == "/opt/homebrew/bin/python3" for worker in workers[:2]))
        self.assertNotIn("_slot", workers[2])

    def test_load_workers_arg_inline_overrides_slots_and_disables_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "pwa-mbp", "profile": "mac", "slots": 8},
                        {"host": "pwa-5090", "profile": "linux", "slots": 24},
                    ],
                }),
                encoding="utf-8",
            )

            with mock.patch("forge_lib.workers.default_workers_path", return_value=workers_path):
                workers = forge.load_workers_arg("pwa-mbp:3,pwa-5090:0,unknown:2")

        self.assertEqual(5, len(workers))
        self.assertEqual(["pwa-mbp", "pwa-mbp", "pwa-mbp", "unknown", "unknown"], [worker["host"] for worker in workers])
        self.assertEqual(["mac", "mac", "mac", "unknown", "unknown"], [worker["profile"] for worker in workers])
        self.assertNotIn("pwa-5090", [worker["host"] for worker in workers])

    def test_load_workers_arg_inline_overrides_profile_and_keeps_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {
                            "host": "pwa-mbp",
                            "profile": "mac",
                            "python": "/opt/homebrew/bin/python3",
                            "slots": 8,
                        },
                    ],
                }),
                encoding="utf-8",
            )

            with mock.patch("forge_lib.workers.default_workers_path", return_value=workers_path):
                workers = forge.load_workers_arg("pwa-mbp=linux:2")

        self.assertEqual(2, len(workers))
        self.assertTrue(all(worker["profile"] == "linux" for worker in workers))
        self.assertTrue(all(worker["python"] == "/opt/homebrew/bin/python3" for worker in workers))

    def test_workers_payload_from_inline_omits_disabled_workers(self) -> None:
        with mock.patch("forge_lib.workers.default_workers_path", side_effect=SystemExit("no workers")):
            payload = forge.workers_payload_from_arg("pwa-mbp:0,pwa-5090:2", default_ok=False)

        self.assertEqual(
            {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "pwa-5090", "profile": "pwa-5090", "local": False, "slots": 2},
                ],
            },
            payload,
        )

    def test_load_workers_arg_rejects_invalid_inline_csv(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            forge.load_workers_arg("pwa-mbp,,pwa-5090")

        self.assertIn("comma-separated host list", str(raised.exception))

    def test_load_workers_arg_rejects_invalid_inline_slots(self) -> None:
        for value in ("pwa-mbp:-1", "pwa-mbp:x"):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit) as raised:
                    forge.load_workers_arg(value)

                self.assertIn("worker slots must be a non-negative integer", str(raised.exception))

    def test_load_workers_arg_treats_json_name_as_missing_file(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            forge.load_workers_arg("missing-workers.json")

        self.assertIn("workers file not found", str(raised.exception))

    def test_expand_worker_record_does_not_mutate_input(self) -> None:
        worker = {"host": "pwa-5090", "profile": "linux", "local": False, "slots": 2}

        workers = forge.expand_worker_record(worker)

        self.assertEqual({"host": "pwa-5090", "profile": "linux", "local": False, "slots": 2}, worker)
        self.assertEqual([1, 2], [item["_slot"] for item in workers])
        self.assertTrue(all("slots" not in item for item in workers))

    def test_load_workers_rejects_invalid_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "pwa-5090", "slots": -1}],
                }),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                forge.load_workers(workers_path)

            self.assertIn("pwa-5090: worker slots", str(raised.exception))

    def test_load_workers_omits_zero_slot_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "pwa-mbp", "slots": 0},
                        {"host": "pwa-5090", "profile": "linux"},
                    ],
                }),
                encoding="utf-8",
            )

            workers = forge.load_workers(workers_path)

        self.assertEqual([{"host": "pwa-5090", "profile": "linux", "local": False}], workers)

    def test_load_workers_rejects_all_zero_slot_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "pwa-mbp", "slots": 0},
                        {"host": "pwa-5090", "slots": 0},
                    ],
                }),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                forge.load_workers(workers_path)

        self.assertIn("no enabled workers", str(raised.exception))

    def test_deploy_commands_use_worker_python(self) -> None:
        worker = {
            "host": "pwa-mbp",
            "profile": "pwa-mbp",
            "local": False,
            "python": "/opt/homebrew/bin/python3",
        }
        manifest = {
            "requirements": {"repos": [{"name": "repo", "path": "~/repo"}]},
            "tasks": [{"id": "task_0000"}],
        }

        prereq = forge.deploy_prereq_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )
        work = forge.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )
        retry_work = forge.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
            retry_failed=True,
        )

        self.assertIn("/opt/homebrew/bin/python3 /tmp/forge-cache/forge.py doctor", prereq)
        self.assertIn("--skip-requirements", prereq)
        self.assertIn("--coordinator pwa-5090:/tmp/manifest.json", prereq)
        self.assertLess(prereq.index("--skip-requirements"), prereq.index("sync-repos"))
        self.assertLess(prereq.index("--coordinator pwa-5090:/tmp/manifest.json"), prereq.index("sync-repos"))
        self.assertIn("/opt/homebrew/bin/python3 /tmp/forge-cache/forge.py work", work)
        self.assertIn("--idle-seconds 120.0", work)
        self.assertNotIn("--retry-failed", work)
        self.assertIn("--retry-failed", retry_work)

    def test_deploy_work_command_includes_task_complete_options(self) -> None:
        worker = {"host": "pwa-mbp", "profile": "pwa-mbp", "local": False}
        manifest = {"tasks": [{"id": "task_0000"}]}

        plain = forge.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )
        hooked = forge.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
            task_complete_command="~/scripts/hook.sh",
            task_complete_sync=True,
        )

        self.assertNotIn("--task-complete-command", plain)
        self.assertNotIn("--task-complete-sync", plain)
        self.assertIn("--task-complete-command '~/scripts/hook.sh'", hooked)
        self.assertIn("--task-complete-sync", hooked)

    def test_coordinator_host_override_is_used_for_remote_workers(self) -> None:
        manifest = Path("/tmp/run/manifest.json")

        remote = forge.coordinator_for_worker(
            {"host": "pwa-rce", "profile": "pwa-rce", "local": False},
            manifest,
            "pwa-5090.local",
        )
        local = forge.coordinator_for_worker(
            {"host": "localhost", "profile": "localhost", "local": True},
            manifest,
            "pwa-5090.local",
            force_remote=True,
        )

        self.assertEqual("pwa-5090.local:/tmp/run/manifest.json", remote)
        self.assertEqual("pwa-5090.local:/tmp/run/manifest.json", local)

    def test_coordinator_host_defaults_to_current_worker_alias(self) -> None:
        manifest = Path("/tmp/run/manifest.json")
        coordinator = {"host": "pwa-mbp", "profile": "pwa-mbp", "local": False}
        remote = {"host": "pwa-5090", "profile": "pwa-5090", "local": False}

        def fake_ssh_config_hostname(host: str) -> str | None:
            return "localhost" if host == "pwa-mbp" else host

        with (
            mock.patch("forge_lib.workers.socket.gethostname", return_value="raw-host"),
            mock.patch("forge_lib.workers.socket.getfqdn", return_value="raw-host"),
            mock.patch("forge_lib.workers.ssh_config_hostname", side_effect=fake_ssh_config_hostname),
        ):
            host = forge.coordinator_host_for_workers([coordinator, remote])
            local_coordinator = forge.coordinator_for_worker(coordinator, manifest, host)
            remote_coordinator = forge.coordinator_for_worker(remote, manifest, host)
            forced_local_coordinator = forge.coordinator_for_worker(
                coordinator,
                manifest,
                host,
                force_remote=True,
            )

        self.assertEqual("pwa-mbp", host)
        self.assertEqual("/tmp/run/manifest.json", local_coordinator)
        self.assertEqual("pwa-mbp:/tmp/run/manifest.json", forced_local_coordinator)
        self.assertEqual("pwa-mbp:/tmp/run/manifest.json", remote_coordinator)

    def test_deploy_uses_configured_coordinator_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            config = root / "config.yml"
            config.write_text('coordinator_host: "pwa-mbp"\n', encoding="utf-8")
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "configured-coordinator",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "pwa-5090", "profile": "pwa-5090", "local": False}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with (
                self.patch_runs_dir(runs),
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config)}, clear=False),
                mock.patch("forge.ProcessTracker.run", fake_run),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="configured-coordinator",
                            resume=False,
                            replace=True,
                            coordinator_host=None,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            work_scripts = [command[-1] for command in commands if command[0] == "ssh" and " work " in command[-1]]
            self.assertEqual(1, len(work_scripts))
            self.assertIn("--coordinator pwa-mbp:", work_scripts[0])
            self.assertNotIn("pwahlman-m-7jfq", work_scripts[0])

    def test_start_delegates_materialization_to_configured_coordinator_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "delegated-start",
                    "work_dir": str(root),
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "pwa-5090", "profile": "pwa-5090", "local": False}],
                }),
                encoding="utf-8",
            )
            ssh_results = [
                subprocess.CompletedProcess(["ssh"], 0, stdout="/tmp/forge.manifest\n", stderr=""),
                subprocess.CompletedProcess(["ssh"], 0, stdout="/tmp/forge.workers\n", stderr=""),
                subprocess.CompletedProcess(["ssh"], 0, stdout="", stderr=""),
            ]
            remote_result = subprocess.CompletedProcess(
                ["ssh"], 0, stdout="remote start\n", stderr="",
            )

            with (
                mock.patch.dict(
                    os.environ,
                    {"FORGE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/forge/runs"},
                ),
                mock.patch("forge.subprocess.run", side_effect=ssh_results) as run_mock,
                mock.patch(
                    "forge_lib.workers.run_process_command_with_retries",
                    return_value=subprocess.CompletedProcess(["rsync"], 0, stdout="", stderr=""),
                ) as rsync_mock,
                mock.patch(
                    "forge_lib.workers.run_interruptible_coordinator_process",
                    return_value=remote_result,
                ) as launch_mock,
                mock.patch("forge_lib.workers.remember_run_for_web") as remember_web,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_run_job(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=4,
                            run=None,
                            resume=False,
                            replace=False,
                            retry_failed=False,
                            task_attempts=2,
                            coordinator_host=None,
                            remote_timeout_seconds=1800.0,
                            wait=False,
                            lease_seconds=600,
                            interval_seconds=30.0,
                            timeout_seconds=0.0,
                            fail_on_fail=True,
                            verify=None,
                            merge=None,
                            merge_force=False,
                            newline_between=False,
                            notify_command=None,
                            no_color=True,
                            verbose=False,
                            command_label="start",
                        )
                    )

            self.assertEqual(0, rc)
            remember_web.assert_called_once_with(
                "delegated-start",
                coordinator_runs="pwa-5090:/home/petter/code/cpp/chess/forge/runs",
            )
            self.assertIn("coordinator: pwa-5090:/home/petter/code/cpp/chess/forge/runs", stdout.getvalue())
            self.assertEqual(2, rsync_mock.call_count)
            host, remote, pid_file = launch_mock.call_args.args
            self.assertEqual("pwa-5090", host)
            self.assertIn("FORGE_COORDINATOR_RUNS=", remote)
            self.assertIn("FORGE_RUNS_DIR=/home/petter/code/cpp/chess/forge/runs", remote)
            self.assertIn("forge start /tmp/forge.manifest --workers /tmp/forge.workers", remote)
            self.assertIn("--coordinator-host pwa-5090", remote)
            self.assertIn("signal.SIG_DFL", remote)
            self.assertIn("kill -INT", remote)
            self.assertIn(pid_file, remote)
            self.assertNotIn(str(root), remote)
            cleanup_command = run_mock.call_args_list[-1].args[0]
            self.assertIn("rm -f -- /tmp/forge.manifest /tmp/forge.workers", cleanup_command[-1])

    def test_coordinator_copy_interrupt_removes_allocated_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "manifest.json"
            source.write_text("{}\n", encoding="utf-8")
            with (
                mock.patch("forge_lib.workers.coordinator_temp_file", return_value="/tmp/forge.abcd"),
                mock.patch(
                    "forge_lib.workers.run_process_command_with_retries",
                    side_effect=KeyboardInterrupt,
                ),
                mock.patch("forge_lib.workers.remove_coordinator_temp_files") as remove_temps,
                self.assertRaises(KeyboardInterrupt),
            ):
                forge.copy_file_to_coordinator("pwa-5090", source)

            remove_temps.assert_called_once_with("pwa-5090", ["/tmp/forge.abcd"])

    def test_deploy_materializes_contained_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "template",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{
                        "id": "task_0000",
                        "index": 0,
                        "command": "printf x > {output}",
                        "outputs": ["{output_dir}/out.txt"],
                    }],
                }),
                encoding="utf-8",
            )
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers),
                jobs=1,
                run="contained",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)
                self.assertEqual(0, rc)

                run_dir = runs / "contained"
                materialized = forge.load_manifest(run_dir / "manifest.json")
                self.assertTrue((run_dir / "workers.json").exists())
                self.assertTrue((run_dir / "state").is_dir())
                self.assertTrue((run_dir / "logs").is_dir())
                self.assertTrue((run_dir / "claims").is_dir())
                self.assertTrue((run_dir / "outputs").is_dir())
                self.assertEqual(str(run_dir / "state"), materialized["state_dir"])
                self.assertEqual(str(run_dir / "logs"), materialized["log_dir"])
                self.assertEqual(str(run_dir / "claims"), materialized["claim_dir"])
                self.assertEqual(str(run_dir), materialized["vars"]["run_dir"])
                self.assertEqual(str(run_dir / "outputs"), materialized["vars"]["output_dir"])
                self.assertEqual([str(run_dir / "outputs" / "out.txt")], materialized["tasks"][0]["outputs"])

                with self.assertRaises(SystemExit) as raised:
                    forge.cmd_deploy(args)
                self.assertIn("run already exists", str(raised.exception))

    def test_deploy_materializes_inline_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "template",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                manifest=str(template),
                workers="worker-a:2,worker-b:0",
                jobs=1,
                run="inline-workers",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["ssh"], 0, stdout="", stderr="")),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            payload = forge.read_json(runs / "inline-workers" / "workers.json")
            self.assertEqual(
                {
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "worker-a", "profile": "worker-a", "local": False, "slots": 2},
                    ],
                },
                payload,
            )

    def test_deploy_sets_up_unique_workers_not_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "unique-setup",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            worker_b_started = threading.Event()
            worker_a_saw_worker_b = False
            worker_a_waited = False
            bootstrap_timeouts = []
            input_sync_timeouts = []

            def targets(command: list[str], host: str) -> bool:
                return any(part == host or part.startswith(f"{host}:") for part in command)

            def fake_run(self, command, **kwargs):
                nonlocal worker_a_saw_worker_b, worker_a_waited
                if command[0] == "rsync" or "mkdir -p /tmp/forge-cache" in command[-1]:
                    bootstrap_timeouts.append(kwargs.get("timeout"))
                if targets(command, "worker-b"):
                    worker_b_started.set()
                if targets(command, "worker-a") and not worker_a_waited:
                    worker_a_waited = True
                    worker_a_saw_worker_b = worker_b_started.wait(0.2)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            def fake_sync_inputs(*args, **kwargs):
                input_sync_timeouts.append(kwargs.get("timeout"))
                return 0

            args = argparse.Namespace(
                manifest=str(template),
                workers="worker-a:2,worker-b:1",
                jobs=2,
                run="unique-setup",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with self.patch_runs_dir(runs), mock.patch(
                "forge.ProcessTracker.run", fake_run
            ), mock.patch("forge_lib.workers.sync_inputs_to_worker", side_effect=fake_sync_inputs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertTrue(worker_a_saw_worker_b)
            self.assertTrue(bootstrap_timeouts)
            self.assertTrue(all(timeout == 60.0 for timeout in bootstrap_timeouts))
            self.assertEqual([1800.0, 1800.0], sorted(input_sync_timeouts))
            self.assertIn("  SETUP  worker-a [2 slots]: setup", stdout.getvalue())
            self.assertIn("  SETUP  worker-b: setup", stdout.getvalue())
            self.assertNotIn("  OK     worker-a#1: setup", stdout.getvalue())
            self.assertNotIn("  OK     worker-a#2: setup", stdout.getvalue())
            self.assertNotIn("  OK     worker-b: setup", stdout.getvalue())

    def test_deploy_retries_transient_setup_ssh_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "transient-setup",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            calls = []
            failed_once = False

            def fake_run(self, command, **kwargs):
                nonlocal failed_once
                calls.append(command)
                if not failed_once and command[0] == "ssh":
                    failed_once = True
                    return subprocess.CompletedProcess(
                        command,
                        255,
                        stdout="",
                        stderr="Connection timed out during banner exchange",
                    )
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers="worker-a:1",
                jobs=1,
                run="transient-setup",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.SSH_TRANSIENT_RETRY_DELAY_SECONDS", 0.0),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertTrue(failed_once)
            self.assertGreaterEqual(len(calls), 2)
            self.assertNotIn("  OK     worker-a: setup", stdout.getvalue())

    def test_deploy_can_write_worker_snapshot_and_launch_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "subset-workers",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            snapshot_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "worker-a", "local": True, "cache_dir": str(root / "cache-a")},
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            }
            launch_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            }
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers="worker-b",
                worker_snapshot_payload=snapshot_payload,
                launch_workers_payload=launch_payload,
                jobs=1,
                run="subset-workers",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with self.patch_runs_dir(runs), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertIn("deploy: run=subset-workers workers=1 jobs=1", stdout.getvalue())
            self.assertEqual(snapshot_payload, forge.read_json(runs / "subset-workers" / "workers.json"))
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertTrue(any("--worker worker-b" in script for script in scripts))
            self.assertFalse(any("--worker worker-a" in script for script in scripts))

    def test_deploy_launch_only_detaches_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-only-workers",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                if " work " in command[-1]:
                    manifest = forge.load_manifest(runs / "launch-only-workers" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_running_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-b",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="launch-only-workers",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with self.patch_runs_dir(runs), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue()
            self.assertIn("deploy: worker-b ready; launching now", output)
            self.assertNotIn("  OK     worker-b: launch", output)
            self.assertIn("deploy: workers active", output)
            self.assertIn("deploy: launch complete", output)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            launch_scripts = [script for script in scripts if " work " in script]
            self.assertEqual(1, len(launch_scripts))
            self.assertIn("start_new_session=True", launch_scripts[0])
            self.assertIn("--worker worker-b", launch_scripts[0])

    def test_deploy_resume_launch_only_does_not_kill_existing_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "resume-launch"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "resume-launch",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "requirements": {"python_min": "3.11", "git_min": "2.38"},
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            forge.write_json(run_dir / "manifest.json", manifest)
            forge.write_json(run_dir / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "worker-a", "local": True, "cache_dir": str(root / "cache-a")},
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            })
            launch_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            }

            def fake_run(self, command, **kwargs):
                if " work " in command[-1]:
                    loaded = forge.load_manifest(run_dir / "manifest.json")
                    task = loaded["tasks"][0]
                    forge.write_json(forge.task_running_path(loaded, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-b",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(run_dir / "manifest.json"),
                workers="worker-b",
                worker_snapshot_payload=None,
                preserve_workers_snapshot=True,
                worker_snapshot_commit_payload=None,
                launch_workers_payload=launch_payload,
                jobs=1,
                run="resume-launch",
                resume=True,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.kill_run_workers") as kill_workers,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            kill_workers.assert_not_called()
            output = stdout.getvalue()
            self.assertIn("deploy: workers active", output)
            self.assertIn("deploy: launch complete", output)

    def test_deploy_launch_only_accepts_post_launch_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            tasks = [
                {"id": f"task_{index:04d}", "index": index, "command": "true", "outputs": []}
                for index in range(10)
            ]
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-progress",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": tasks,
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": f"worker-{index}", "profile": f"worker-{index}", "local": True, "cache_dir": str(root / f"cache-{index}")}
                        for index in range(4)
                    ],
                }),
                encoding="utf-8",
            )
            commands = []
            seeded_done = False
            launched_done = 0
            launch_lock = threading.Lock()

            def write_done(manifest, index: int) -> None:
                task = manifest["tasks"][index]
                forge.write_json(forge.task_done_path(manifest, task), {
                    "task": task["id"],
                    "index": task["index"],
                    "worker": "worker-b",
                    "state": "done",
                })

            def fake_run(self, command, **kwargs):
                nonlocal seeded_done, launched_done
                commands.append(command)
                manifest_path = runs / "launch-progress" / "manifest.json"
                if manifest_path.exists():
                    manifest = forge.load_manifest(manifest_path)
                    if not seeded_done and " work " not in command[-1]:
                        for index in range(8):
                            write_done(manifest, index)
                        seeded_done = True
                    if " work " in command[-1]:
                        with launch_lock:
                            if launched_done < 2:
                                write_done(manifest, 8 + launched_done)
                                launched_done += 1
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="launch-progress",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue()
            self.assertIn("deploy: workers active", output)
            self.assertIn("deploy: launch complete", output)
            manifest = forge.load_manifest(runs / "launch-progress" / "manifest.json")
            self.assertFalse(forge.stop_request_path(manifest).exists())
            self.assertEqual(2, launched_done)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertEqual(4, len([script for script in scripts if " work " in script]))

    def test_deploy_launch_only_finalizes_when_setup_lags_completed_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "setup-lags-complete",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "worker-fast", "profile": "worker-fast", "local": True, "cache_dir": str(root / "cache-fast")},
                        {"host": "worker-slow", "profile": "worker-slow", "local": True, "cache_dir": str(root / "cache-slow")},
                    ],
                }),
                encoding="utf-8",
            )
            setup_released = threading.Event()
            terminated = threading.Event()

            def fake_bootstrap(worker, manifest_path, **kwargs):
                if worker["host"] == "worker-slow":
                    setup_released.wait(timeout=5)

            def fake_run_worker_shell(worker, script, **kwargs):
                if " work " in script:
                    manifest = forge.load_manifest(runs / "setup-lags-complete" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_done_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": worker["host"],
                        "state": "done",
                    })
                return subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")

            def fake_terminate_all(self):
                terminated.set()
                setup_released.set()

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=2,
                run="setup-lags-complete",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge_lib.workers.bootstrap_worker_runner", side_effect=fake_bootstrap),
                mock.patch("forge_lib.workers.sync_inputs_to_worker", return_value=0),
                mock.patch("forge_lib.workers.run_worker_shell", side_effect=fake_run_worker_shell),
                mock.patch.object(forge.ProcessTracker, "terminate_all", fake_terminate_all),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertTrue(terminated.is_set())
            self.assertIn("deploy: tasks completed while setup was still pending; stopping setup", stderr.getvalue())
            self.assertIn("deploy: launch complete", stdout.getvalue())
            manifest = forge.load_manifest(runs / "setup-lags-complete" / "manifest.json")
            self.assertEqual("done", forge.task_state(manifest, manifest["tasks"][0], lease_seconds=600))

    def test_deploy_launch_only_keeps_partial_worker_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            tasks = [
                {"id": f"task_{index:04d}", "index": index, "command": "true", "outputs": []}
                for index in range(17)
            ]
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "partial-launch-start",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": tasks,
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "worker-b",
                        "profile": "worker-b",
                        "local": True,
                        "cache_dir": str(root / "cache-b"),
                        "slots": 17,
                    }],
                }),
                encoding="utf-8",
            )
            commands = []
            launched = 0
            launch_lock = threading.Lock()

            def fake_run(self, command, **kwargs):
                nonlocal launched
                commands.append(command)
                manifest_path = runs / "partial-launch-start" / "manifest.json"
                if manifest_path.exists() and " work " in command[-1]:
                    with launch_lock:
                        if launched < 16:
                            manifest = forge.load_manifest(manifest_path)
                            task = manifest["tasks"][launched]
                            forge.write_json(forge.task_running_path(manifest, task), {
                                "task": task["id"],
                                "index": task["index"],
                                "worker": "worker-b",
                                "state": "running",
                            })
                        launched += 1
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=4,
                run="partial-launch-start",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue() + stderr.getvalue()
            self.assertIn("deploy: workers partially active; 16/17 slots started", output)
            self.assertIn("deploy: launch complete", output)
            self.assertNotIn("deploy: launch failed", output)
            manifest = forge.load_manifest(runs / "partial-launch-start" / "manifest.json")
            self.assertFalse(forge.stop_request_path(manifest).exists())
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertEqual(17, len([script for script in scripts if " work " in script]))
            self.assertFalse(any("kill -TERM" in script for script in scripts))

    def test_deploy_launch_only_counts_failed_task_as_worker_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            tasks = [
                {"id": f"task_{index:04d}", "index": index, "command": "true", "outputs": []}
                for index in range(17)
            ]
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-start-with-task-fail",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": tasks,
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "worker-b",
                        "profile": "worker-b",
                        "local": True,
                        "cache_dir": str(root / "cache-b"),
                        "slots": 17,
                    }],
                }),
                encoding="utf-8",
            )
            commands = []
            launched = 0
            launch_lock = threading.Lock()

            def fake_run(self, command, **kwargs):
                nonlocal launched
                commands.append(command)
                manifest_path = runs / "launch-start-with-task-fail" / "manifest.json"
                if manifest_path.exists() and " work " in command[-1]:
                    with launch_lock:
                        manifest = forge.load_manifest(manifest_path)
                        task = manifest["tasks"][launched]
                        payload = {
                            "task": task["id"],
                            "index": task["index"],
                            "worker": "worker-b",
                            "state": "fail" if launched == 16 else "running",
                        }
                        path = (
                            forge.task_fail_path(manifest, task)
                            if launched == 16
                            else forge.task_running_path(manifest, task)
                        )
                        forge.write_json(path, payload)
                        launched += 1
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=4,
                run="launch-start-with-task-fail",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue() + stderr.getvalue()
            self.assertIn("deploy: workers active", output)
            self.assertIn("deploy: launch complete", output)
            self.assertNotIn("deploy: workers did not become active", output)
            manifest = forge.load_manifest(runs / "launch-start-with-task-fail" / "manifest.json")
            self.assertFalse(forge.stop_request_path(manifest).exists())
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertEqual(17, len([script for script in scripts if " work " in script]))
            self.assertFalse(any("kill -TERM" in script for script in scripts))

    def test_deploy_launch_only_skips_workers_that_fail_setup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "partial-launch",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {
                            "host": "worker-a",
                            "profile": "worker-a",
                            "local": True,
                            "cache_dir": str(root / "cache-a"),
                            "path_map": {str(root): str(root)},
                        },
                        {
                            "host": "worker-b",
                            "profile": "worker-b",
                            "local": True,
                            "cache_dir": str(root / "cache-b"),
                            "path_map": {str(root): str(root)},
                            "slots": 3,
                        },
                    ],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                script = command[-1]
                if "--profile worker-b" in script and " doctor " in script:
                    return subprocess.CompletedProcess(command, 1, stdout="", stderr="missing worker-b engine")
                if "--worker worker-a" in script and " work " in script:
                    manifest = forge.load_manifest(runs / "partial-launch" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_running_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-a",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=2,
                run="partial-launch",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with self.patch_runs_dir(runs), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue()
            self.assertIn("FAILED worker-b [3 slots]: setup rc=1", output)
            self.assertEqual(1, output.count("missing worker-b engine"))
            self.assertNotIn("FAILED worker-b#1: setup", output)
            self.assertIn("deploy: setup partial; 1/4 slots ready", output)
            self.assertIn("deploy: worker-a ready; launching now", output)
            self.assertNotIn("  OK     worker-a: launch", output)
            self.assertIn("deploy: launch complete", output)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertEqual(1, len([script for script in scripts if " work " in script and "--worker worker-a" in script]))
            self.assertEqual(0, len([script for script in scripts if " work " in script and "--worker worker-b" in script]))

    def test_deploy_launches_ready_worker_before_slow_setup_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "slow-setup",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "worker-a", "profile": "worker-a", "local": True, "cache_dir": str(root / "cache-a")},
                        {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                    ],
                }),
                encoding="utf-8",
            )
            commands = []
            slow_setup_started = threading.Event()
            slow_setup_finished = threading.Event()
            fast_worker_launched = threading.Event()
            slow_setup_saw_fast_launch = False

            def fake_run(self, command, **kwargs):
                nonlocal slow_setup_saw_fast_launch
                commands.append(command)
                script = command[-1]
                if str(root / "cache-b") in script and " doctor " in script:
                    slow_setup_started.set()
                    slow_setup_saw_fast_launch = fast_worker_launched.wait(1.0)
                    slow_setup_finished.set()
                    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                if "--worker worker-a" in script and " work " in script:
                    fast_worker_launched.set()
                    manifest = forge.load_manifest(runs / "slow-setup" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_running_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-a",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="slow-setup",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertTrue(slow_setup_started.is_set())
            self.assertTrue(slow_setup_finished.wait(1.0))
            self.assertTrue(slow_setup_saw_fast_launch)
            output = stdout.getvalue()
            self.assertNotIn("FAILED worker-b", output)
            self.assertIn("deploy: worker-a ready; launching now", output)
            self.assertIn("deploy: worker-b ready; launching now", output)
            self.assertIn("deploy: workers active", output)
            self.assertIn("deploy: launch complete", output)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertEqual(1, len([script for script in scripts if " work " in script and "--worker worker-a" in script]))
            self.assertEqual(1, len([script for script in scripts if " work " in script and "--worker worker-b" in script]))

    def test_deploy_launch_only_cleans_up_when_workers_do_not_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "failed-launch",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="failed-launch",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_TIMEOUT_SECONDS", 0.01),
                mock.patch("forge_lib.workers.DEPLOY_STARTUP_POLL_SECONDS", 0.01),
                mock.patch("forge_lib.workers.kill_run_workers", return_value=1) as kill_workers,
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            output = stdout.getvalue() + stderr.getvalue()
            self.assertIn("deploy: workers launched; task claims are still pending", output)
            self.assertIn("deploy: launch complete", output)
            manifest = forge.load_manifest(runs / "failed-launch" / "manifest.json")
            self.assertFalse(forge.stop_request_path(manifest).exists())
            kill_workers.assert_not_called()

    def test_kill_run_workers_cleans_slot_pid_glob(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "slot-stop"
            run.mkdir(parents=True)
            cache = root / "cache"
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [
                    {
                        "host": "worker-a",
                        "profile": "worker-a",
                        "local": False,
                        "cache_dir": str(cache),
                        "slots": 3,
                    },
                ],
            })
            commands = []

            def fake_run(command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout="\n".join([
                        str(cache / "slot-stop-0-worker-a_1.pid"),
                        str(cache / "slot-stop-1-worker-a_2.pid"),
                        str(cache / "slot-stop-2-worker-a_3.pid"),
                    ]) + "\n",
                    stderr="",
                )

            with mock.patch("forge_lib.workers.subprocess.run", fake_run):
                killed = forge.kill_run_workers(run)

            self.assertEqual(3, killed)
            self.assertEqual(1, len(commands))
            script = commands[0][-1]
            self.assertIn("slot-stop-*.pid", script)

    def test_kill_run_workers_cleans_local_pid_glob(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "local-stop"
            run.mkdir(parents=True)
            cache = root / "cache"
            forge.write_json(run / "workers.json", {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "cache_dir": str(cache),
                    "slots": 2,
                }],
            })
            pid_path = cache / "local-stop-0-localhost_1.pid"

            with mock.patch(
                "forge_lib.workers.subprocess.run",
                return_value=subprocess.CompletedProcess(["sh"], 0, stdout=f"{pid_path}\n", stderr=""),
            ) as run_mock:
                killed = forge.kill_run_workers(run)

            self.assertEqual(1, killed)
            command = run_mock.call_args.args[0]
            self.assertEqual(["sh", "-lc"], command[:2])
            self.assertIn("local-stop-*.pid", command[-1])
            self.assertEqual(1, command[-1].count("sleep 0.1"))
            self.assertIn("kill -TERM", command[-1])

    def test_deploy_launch_only_starts_wait_monitor_for_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-monitor",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")}],
                }),
                encoding="utf-8",
            )

            def fake_run(self, command, **kwargs):
                if " work " in command[-1]:
                    manifest = forge.load_manifest(runs / "launch-monitor" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_running_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-b",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            class FakePopen:
                def __init__(self, command, **kwargs):
                    self.pid = 4242
                    self.command = command

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="launch-monitor",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                notify_command="notify-me",
                lease_seconds=600,
                interval_seconds=7.0,
                timeout_seconds=0.0,
                fail_on_fail=True,
                start_wait_monitor=True,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.run_notify_command", return_value=0) as notify,
                mock.patch("forge_lib.workers.subprocess.Popen", side_effect=FakePopen) as popen,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertIn("deploy: wait monitor pid=4242", stdout.getvalue())
            monitor = forge.read_json(runs / "launch-monitor" / "wait-monitor.json")
            self.assertEqual(4242, monitor["pid"])
            launch = forge.read_json(runs / "launch-monitor" / "state" / "launch.json")
            self.assertEqual(4242, launch["pid"])
            self.assertEqual("wait-monitor", launch["reason"])
            self.assertIn("--notify-command", monitor["command"])
            self.assertIn("notify-me", monitor["command"])
            self.assertIn("--interval-seconds", monitor["command"])
            self.assertIn("7.0", monitor["command"])
            popen.assert_called_once()
            notify.assert_not_called()

    def test_deploy_launch_only_starts_wait_monitor_without_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "launch-monitor-no-notify",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")}],
                }),
                encoding="utf-8",
            )

            def fake_run(self, command, **kwargs):
                if " work " in command[-1]:
                    manifest = forge.load_manifest(runs / "launch-monitor-no-notify" / "manifest.json")
                    task = manifest["tasks"][0]
                    forge.write_json(forge.task_running_path(manifest, task), {
                        "task": task["id"],
                        "index": task["index"],
                        "worker": "worker-b",
                        "state": "running",
                    })
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            class FakePopen:
                def __init__(self, command, **kwargs):
                    self.pid = 4343
                    self.command = command

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers_path),
                jobs=1,
                run="launch-monitor-no-notify",
                resume=False,
                replace=False,
                launch_only=True,
                remote_timeout_seconds=1800.0,
                notify_command=None,
                lease_seconds=600,
                interval_seconds=7.0,
                timeout_seconds=0.0,
                fail_on_fail=True,
                start_wait_monitor=True,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", fake_run),
                mock.patch("forge_lib.workers.subprocess.Popen", side_effect=FakePopen) as popen,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertIn("deploy: wait monitor pid=4343", stdout.getvalue())
            monitor = forge.read_json(runs / "launch-monitor-no-notify" / "wait-monitor.json")
            self.assertEqual(4343, monitor["pid"])
            launch = forge.read_json(runs / "launch-monitor-no-notify" / "state" / "launch.json")
            self.assertEqual(4343, launch["pid"])
            self.assertNotIn("--notify-command", monitor["command"])
            popen.assert_called_once()

    def test_deploy_preserves_worker_snapshot_until_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "forge.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "requirements": {"python_min": "3.11", "git_min": "2.38"},
                "tasks": [],
            }
            forge.write_json(run_dir / "manifest.json", manifest)
            original_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "worker-a", "local": True, "cache_dir": str(root / "cache-a")},
                ],
            }
            commit_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-a", "profile": "worker-a", "local": True, "cache_dir": str(root / "cache-a")},
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            }
            launch_payload = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "worker-b", "profile": "worker-b", "local": True, "cache_dir": str(root / "cache-b")},
                ],
            }
            forge.write_json(run_dir / "workers.json", original_payload)

            def fail_doctor(self, command, **kwargs):
                script = command[-1]
                rc = 1 if " doctor " in script else 0
                return subprocess.CompletedProcess(command, rc, stdout="", stderr="")

            args = argparse.Namespace(
                manifest=str(run_dir / "manifest.json"),
                workers="worker-b",
                worker_snapshot_payload=None,
                preserve_workers_snapshot=True,
                worker_snapshot_commit_payload=commit_payload,
                launch_workers_payload=launch_payload,
                jobs=1,
                run="active",
                resume=True,
                replace=False,
                remote_timeout_seconds=1800.0,
                no_color=True,
                verbose=False,
            )
            with self.patch_runs_dir(runs), mock.patch("forge.ProcessTracker.run", fail_doctor):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(1, rc)
            self.assertIn("deploy: setup failed", stdout.getvalue())
            self.assertEqual(original_payload, forge.read_json(run_dir / "workers.json"))

    def test_deploy_sends_completion_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "notify-template",
                    "kind": "smoke",
                    "description": "notify check",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            config = root / "config.yml"
            config.write_text('notify_command: "notify-me"\n', encoding="utf-8")
            payloads: list[dict[str, object]] = []

            def capture_notify(command: str | None, payload: dict[str, object]) -> int:
                self.assertEqual("notify-me", command)
                payloads.append(payload)
                return 0

            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers),
                jobs=1,
                run="notify-run",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                notify_command=None,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch.dict(os.environ, {"FORGE_CONFIG": str(config), "FORGE_NOTIFY_COMMAND": ""}, clear=False),
                mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
                mock.patch("forge_lib.workers.run_notify_command", capture_notify),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("deploy_done", payload["event"])
            self.assertEqual("notify-run", payload["run"])
            self.assertEqual("ok", payload["state"])
            self.assertEqual("smoke", payload["kind"])
            self.assertEqual("notify check", payload["description"])
            self.assertEqual(0, payload["tasks"])

    def test_deploy_does_not_use_user_notify_config_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "no-notify-template",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            args = argparse.Namespace(
                manifest=str(template),
                workers=str(workers),
                jobs=1,
                run="no-notify-run",
                resume=False,
                replace=False,
                remote_timeout_seconds=1800.0,
                notify_command=None,
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
                mock.patch("forge_lib.workers.run_notify_command", return_value=0) as notify,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(args)

            self.assertEqual(0, rc)
            notify.assert_not_called()

    def test_example_manifests_parse_and_format_tasks(self) -> None:
        for path in sorted((ROOT / "examples").glob("*.manifest.json")):
            with self.subTest(path=path.name):
                manifest = forge.load_manifest(path)
                for task in manifest["tasks"]:
                    self.assertTrue(forge.format_command(manifest, task))

    def test_worker_label_omits_duplicate_profile(self) -> None:
        self.assertEqual("pwa-5090", forge.worker_label("pwa-5090", "pwa-5090"))
        self.assertEqual("pwa-wsl", forge.worker_label("pwa-wsl", "pwa-wsl"))
        self.assertEqual("pwa-5090#2", forge.worker_label("pwa-5090", "pwa-5090", 2, 4))

    def test_run_worker_shell_retries_transient_ssh_transport_failure(self) -> None:
        worker = {"host": "pwa-5090", "profile": "pwa-5090", "local": False}
        calls = []

        def fake_run(self, command, **kwargs):
            calls.append(command)
            if len(calls) == 1:
                return subprocess.CompletedProcess(
                    command,
                    255,
                    stdout="ok\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

        with (
            mock.patch("forge.ProcessTracker.run", fake_run),
            mock.patch("forge_lib.workers.SSH_TRANSIENT_RETRY_DELAY_SECONDS", 0.0),
        ):
            result = forge.run_worker_shell(
                worker,
                "true",
                tracker=forge.ProcessTracker(),
                retries=1,
            )

        self.assertEqual(0, result.returncode)
        self.assertEqual(2, len(calls))

    def test_worker_process_retries_rsync_code_255_without_ssh_detail(self) -> None:
        from forge_lib import workers as worker_module

        calls = []

        def fake_run(command, **kwargs):
            calls.append(command)
            if len(calls) == 1:
                return subprocess.CompletedProcess(
                    command,
                    255,
                    stdout="",
                    stderr="rsync error: unexplained error (code 255) at rsync.c(716) [sender=3.4.1]",
                )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with (
            mock.patch("forge_lib.workers.run_process_command", side_effect=fake_run),
            mock.patch("forge_lib.workers.SSH_TRANSIENT_RETRY_DELAY_SECONDS", 0.0),
        ):
            result = worker_module.run_process_command_with_retries(
                ["rsync", "source", "pwa-rce:destination"],
                retries=1,
            )

        self.assertEqual(0, result.returncode)
        self.assertEqual(2, len(calls))

    def test_run_worker_shell_does_not_retry_regular_failures(self) -> None:
        worker = {"host": "pwa-5090", "profile": "pwa-5090", "local": False}
        calls = []

        def fake_run(self, command, **kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="candidate missing")

        with (
            mock.patch("forge.ProcessTracker.run", fake_run),
            mock.patch("forge_lib.workers.SSH_TRANSIENT_RETRY_DELAY_SECONDS", 0.0),
        ):
            result = forge.run_worker_shell(
                worker,
                "false",
                tracker=forge.ProcessTracker(),
                retries=3,
            )

        self.assertEqual(1, result.returncode)
        self.assertEqual(1, len(calls))

    def test_bootstrap_worker_runner_syncs_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            cache = root / "cache"
            manifest.write_text("{}", encoding="utf-8")
            worker = {"host": "localhost", "local": True, "cache_dir": str(cache)}

            forge.bootstrap_worker_runner(worker, manifest)

            self.assertTrue((cache / "forge.py").exists())
            self.assertTrue((cache / "forge_lib" / "__init__.py").exists())
            self.assertTrue((cache / "manifest.json").exists())

    def test_managed_remote_script_cleans_child_process_group(self) -> None:
        script = forge.managed_remote_script("sleep 60")

        self.assertIn("setsid sh -lc", script)
        self.assertIn("kill -TERM -$pid", script)
        self.assertIn("kill -KILL -$pid", script)

    def test_deploy_uses_workers_json_without_unknown_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        "pwa-5090",
                        {"host": "localhost", "local": True, "cache_dir": str(Path(tmp) / "cache")},
                    ],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(Path(tmp) / "runs"), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(ROOT / "examples/enyo-candidate-deploy.manifest.json"),
                            workers=str(workers_path),
                            jobs=1,
                            run="enyo-candidate-deploy-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            deploy_output = stdout.getvalue()
            self.assertIn("deploy: run=enyo-candidate-deploy-test workers=2 jobs=1", deploy_output)
            self.assertNotIn("  OK     pwa-5090: setup", deploy_output)
            self.assertNotIn("  OK     localhost: setup", deploy_output)
            self.assertNotIn("syncing runner", deploy_output)
            self.assertNotIn("checking prerequisites", deploy_output)
            self.assertIn("deploy: setup complete", deploy_output)
            self.assertTrue(
                any(command[0] == "ssh" and command[-2] == "pwa-5090" and "mkdir -p /tmp/forge-cache" in command[-1] for command in commands)
            )
            self.assertNotIn(["sh", "-lc", "mkdir -p " + str(Path(tmp) / "cache")], commands)
            self.assertTrue((Path(tmp) / "cache/forge.py").exists())
            self.assertTrue((Path(tmp) / "cache/manifest.json").exists())
            self.assertTrue(any(command[:4] == ["rsync", "-a", "-e", forge.RSYNC_SSH] for command in commands))
            scripts = [command[-1] for command in commands if command[0] in {"ssh", "sh"}]
            self.assertFalse(any("--profile pwa-5090" in script for script in scripts))

    def test_deploy_verbose_shows_phase_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-verbose",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {
                        "python_min": "3.11",
                        "git_min": "2.38",
                    },
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-verbose-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=True,
                        )
                    )

            self.assertEqual(0, rc)
            deploy_output = stdout.getvalue()
            self.assertIn("localhost: syncing runner and manifest", deploy_output)
            self.assertIn("localhost: checking prerequisites", deploy_output)
            self.assertIn("localhost: prerequisites OK", deploy_output)
            self.assertIn("  OK     localhost: setup", deploy_output)

    def test_deploy_syncs_declared_inputs_to_worker_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            worker_root = root / "worker"
            coordinator.mkdir()
            worker_root.mkdir()
            source = coordinator / "source.jsonl"
            source.write_text("{\"x\":1}\n", encoding="utf-8")
            manifest = coordinator / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-input-sync",
                    "work_dir": str(coordinator),
                    "state_dir": str(coordinator / "state"),
                    "log_dir": str(coordinator / "logs"),
                    "path_maps": {"worker": {str(coordinator): str(worker_root)}},
                    "requirements": {
                        "python_min": "3.11",
                        "git_min": "2.38",
                        "inputs": [forge.file_record(source)],
                    },
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "localhost",
                        "profile": "worker",
                        "local": True,
                        "cache_dir": str(root / "cache"),
                    }],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-input-sync-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=True,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertEqual(source.read_text(encoding="utf-8"), (worker_root / "source.jsonl").read_text(encoding="utf-8"))
            self.assertIn("localhost/worker: synced 1 input file(s)", stdout.getvalue())

    def test_sync_inputs_skips_matching_remote_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            worker_root = root / "worker"
            coordinator.mkdir()
            worker_root.mkdir()
            source = coordinator / "source.jsonl"
            source.write_text("{\"x\":1}\n", encoding="utf-8")
            manifest = {
                "schema": "forge.task.v1",
                "name": "remote-input-skip",
                "work_dir": str(coordinator),
                "state_dir": str(coordinator / "state"),
                "log_dir": str(coordinator / "logs"),
                "path_maps": {"worker": {str(coordinator): str(worker_root)}},
                "requirements": {"inputs": [forge.file_record(source)]},
                "tasks": [],
            }
            worker = {"host": "remote", "profile": "worker", "local": False}

            with (
                mock.patch(
                    "forge_lib.workers.run_worker_shell",
                    return_value=subprocess.CompletedProcess(["check"], 0, stdout="", stderr=""),
                ) as run_shell,
                mock.patch("forge_lib.workers.copy_to_worker") as copy_to_worker,
            ):
                synced = forge.sync_inputs_to_worker(worker, manifest)

            self.assertEqual(0, synced)
            self.assertEqual(1, run_shell.call_count)
            self.assertIn(str(worker_root / "source.jsonl"), run_shell.call_args.args[1])
            copy_to_worker.assert_not_called()

    def test_sync_inputs_copies_missing_remote_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator = root / "coordinator"
            worker_root = root / "worker"
            coordinator.mkdir()
            worker_root.mkdir()
            source = coordinator / "source.jsonl"
            source.write_text("{\"x\":1}\n", encoding="utf-8")
            manifest = {
                "schema": "forge.task.v1",
                "name": "remote-input-copy",
                "work_dir": str(coordinator),
                "state_dir": str(coordinator / "state"),
                "log_dir": str(coordinator / "logs"),
                "path_maps": {"worker": {str(coordinator): str(worker_root)}},
                "requirements": {"inputs": [forge.file_record(source)]},
                "tasks": [],
            }
            worker = {"host": "remote", "profile": "worker", "local": False}

            with (
                mock.patch(
                    "forge_lib.workers.run_worker_shell",
                    side_effect=[
                        subprocess.CompletedProcess(["check"], 1, stdout="", stderr=""),
                        subprocess.CompletedProcess(["mkdir"], 0, stdout="", stderr=""),
                    ],
                ),
                mock.patch("forge_lib.workers.copy_to_worker") as copy_to_worker,
            ):
                synced = forge.sync_inputs_to_worker(worker, manifest)

            self.assertEqual(1, synced)
            copy_to_worker.assert_called_once_with(
                worker,
                source,
                str(worker_root / "source.jsonl"),
                timeout=None,
                tracker=None,
                retries=0,
            )

    def test_sync_inputs_checks_tunneled_worker_with_matching_local_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source.jsonl"
            source.write_text("{\"x\":1}\n", encoding="utf-8")
            manifest = {
                "schema": "forge.task.v1",
                "name": "tunneled-worker-input",
                "requirements": {"inputs": [forge.file_record(source)]},
                "tasks": [],
            }
            worker = {"host": "remote", "profile": "remote", "local": False}

            with (
                mock.patch("forge_lib.workers.worker_is_current_host", return_value=True),
                mock.patch(
                    "forge_lib.workers.run_worker_shell",
                    side_effect=[
                        subprocess.CompletedProcess(["check"], 1, stdout="", stderr=""),
                        subprocess.CompletedProcess(["mkdir"], 0, stdout="", stderr=""),
                    ],
                ),
                mock.patch("forge_lib.workers.copy_to_worker") as copy_to_worker,
            ):
                synced = forge.sync_inputs_to_worker(worker, manifest)

            self.assertEqual(1, synced)
            copy_to_worker.assert_called_once_with(
                worker,
                source,
                str(source),
                timeout=None,
                tracker=None,
                retries=0,
            )

    def test_deploy_passes_matching_profile_and_runs_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-test",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "path_maps": {"worker": {}},
                    "requirements": {
                        "python_min": "3.11",
                        "git_min": "2.38",
                        "git": {"repo": str(root), "commit": ""},
                    },
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "profile": "worker", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-profile-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertTrue(any("sync-repos" in script and "--profile worker" in script for script in scripts))
            self.assertTrue(any(" work " in script and "--profile worker" in script for script in scripts))

    def test_deploy_runs_one_worker_loop_per_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-slots",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{
                        "host": "localhost",
                        "local": True,
                        "cache_dir": str(root / "cache"),
                        "slots": 2,
                    }],
                }),
                encoding="utf-8",
            )
            commands = []
            events: list[str] = []
            event_lock = threading.Lock()
            second_work_started = threading.Event()

            def fake_run(self, command, **kwargs):
                commands.append(command)
                script = command[-1]
                if command[0] == "sh" and " work " in script:
                    with event_lock:
                        work_index = sum(1 for event in events if event.startswith("work_start"))
                        events.append(f"work_start_{work_index}")
                    if work_index == 0:
                        if second_work_started.wait(1):
                            events.append("first_work_saw_second")
                        else:
                            events.append("first_work_timed_out")
                    else:
                        second_work_started.set()
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-slots-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            output = stdout.getvalue()
            self.assertIn("deploy: run=deploy-slots-test workers=2 jobs=1", output)
            self.assertNotIn("  OK     localhost#1: tasks", output)
            self.assertNotIn("  OK     localhost#2: tasks", output)
            work_scripts = [command[-1] for command in commands if command[0] == "sh" and " work " in command[-1]]
            doctor_scripts = [command[-1] for command in commands if command[0] == "sh" and " doctor " in command[-1]]
            self.assertEqual(1, len(doctor_scripts))
            self.assertEqual(2, len(work_scripts))
            self.assertTrue(all("--worker localhost" in script for script in work_scripts))
            self.assertIn("first_work_saw_second", events)
            self.assertNotIn("first_work_timed_out", events)

    def test_deploy_waits_for_all_prereqs_before_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-pipeline",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "path_maps": {"fast": {}, "slow": {}},
                    "requirements": {
                        "python_min": "3.11",
                        "git_min": "2.38",
                    },
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [
                        {"host": "fast", "profile": "fast", "local": True, "cache_dir": str(root / "cache-fast")},
                        {"host": "slow", "profile": "slow", "local": True, "cache_dir": str(root / "cache-slow")},
                    ],
                }),
                encoding="utf-8",
            )
            events: list[str] = []
            allow_slow = threading.Event()

            def fake_run(self, command, **kwargs):
                script = command[-1]
                if "doctor" in script and "--profile slow" in script:
                    events.append("slow_prereq_start")
                    allow_slow.wait(2)
                    events.append("slow_prereq_done")
                    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                if "doctor" in script and "--profile fast" in script:
                    events.append("fast_prereq_done")
                    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                if " work " in script and "--profile fast" in script:
                    events.append("fast_work_start")
                    allow_slow.set()
                    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                if " work " in script and "--profile slow" in script:
                    events.append("slow_work_start")
                    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=2,
                            run="deploy-pipeline-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertLess(events.index("slow_prereq_done"), events.index("fast_work_start"))
            self.assertLess(events.index("slow_prereq_done"), events.index("slow_work_start"))

    def test_deploy_stops_when_prerequisites_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-fail",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

            with self.patch_runs_dir(root / "runs"), mock.patch("forge.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-fail-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(1, rc)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertFalse(any(" work " in script for script in scripts))

    def test_deploy_command_probe_failure_blocks_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-command-probe",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {
                        "commands": [{
                            "name": "sprt-feature",
                            "command": "printf 'old sprt\\n'; exit 1",
                            "contains": "--no-ntfy",
                        }],
                    },
                    "tasks": [{
                        "id": "task_0000",
                        "index": 0,
                        "command": "printf task > {output}",
                        "outputs": ["{output_dir}/out.txt"],
                    }],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-command-probe-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(1, rc)
            self.assertIn("deploy: setup failed; no tasks started", stdout.getvalue())
            self.assertFalse((runs / "deploy-command-probe-test/outputs/out.txt").exists())

    def test_deploy_interrupt_terminates_active_worker_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "forge.task.v1",
                    "name": "deploy-interrupt",
                    "work_dir": str(root),
                    "state_dir": str(root / "state"),
                    "log_dir": str(root / "logs"),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "forge.workers.v1",
                    "workers": ["pwa-5090"],
                }),
                encoding="utf-8",
            )
            real_wait = forge.concurrent.futures.wait
            wait_calls = 0

            def interrupt_first_wait(*args, **kwargs):
                nonlocal wait_calls
                wait_calls += 1
                if wait_calls == 1:
                    raise KeyboardInterrupt
                return real_wait(*args, **kwargs)

            with (
                self.patch_runs_dir(root / "runs"),
                mock.patch("forge.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
                mock.patch.object(forge.ProcessTracker, "terminate_all") as terminate_all,
                mock.patch("forge_lib.workers.kill_run_workers", return_value=0) as kill_workers,
                mock.patch("forge.concurrent.futures.wait", side_effect=interrupt_first_wait),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = forge.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            run="deploy-interrupt-test",
                            resume=False,
                            replace=True,
                            remote_timeout_seconds=1800.0,
                            no_color=True,
                            verbose=False,
                        )
                    )

            self.assertEqual(130, rc)
            terminate_all.assert_called_once()
            kill_workers.assert_called_once()
            self.assertIn("deploy: interrupted; stopping active worker commands", stderr.getvalue())
            self.assertIn("deploy: interrupt cleanup complete", stderr.getvalue())
            materialized = forge.load_manifest(root / "runs" / "deploy-interrupt-test" / "manifest.json")
            self.assertTrue(forge.stop_request_path(materialized).exists())

    def test_prepare_command_is_removed(self) -> None:
        result = self.run_cli("prepare", "manifest.json")
        self.assertNotEqual(0, result.returncode)

    def test_deploy_cli_takes_workers_then_manifest(self) -> None:
        result = self.run_cli("deploy", "--help")
        self.assertEqual(0, result.returncode)
        self.assertIn("workers", result.stdout)
        self.assertIn("manifest", result.stdout)
        self.assertIn("--no-color", result.stdout)
        self.assertNotIn("--color,", result.stdout)
        self.assertNotIn("--workers", result.stdout)


if __name__ == "__main__":
    unittest.main()
