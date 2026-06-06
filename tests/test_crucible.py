from __future__ import annotations

import contextlib
import io
import json
import os
import argparse
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "crucible.py"
sys.path.insert(0, str(ROOT))
import crucible  # noqa: E402


class CrucibleTests(unittest.TestCase):
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

    @contextlib.contextmanager
    def patch_runs_dir(self, path: Path):
        with (
            mock.patch("crucible_lib.core.runs_dir", return_value=path),
            mock.patch("crucible_lib.status.runs_dir", return_value=path),
            mock.patch("crucible_lib.workers.runs_dir", return_value=path),
        ):
            yield

    def test_remote_script_sets_common_user_bin_path(self) -> None:
        script = crucible.managed_remote_script("crucible version")
        self.assertIn("PATH=\"$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH\"; export PATH; ", script)
        self.assertIn("crucible version", script)

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
        common = stdout.index("Common workflow:")
        results = stdout.index("Result handling:")
        maintenance = stdout.index("Maintenance:")
        advanced = stdout.index("Advanced/internal:")

        self.assertLess(common, stdout.index("    run-job"))
        self.assertLess(stdout.index("    run-job"), results)
        self.assertLess(results, stdout.index("    verify"))
        self.assertLess(stdout.index("    merge"), maintenance)
        self.assertLess(maintenance, stdout.index("    update"))
        self.assertLess(stdout.index("    version"), advanced)
        self.assertLess(advanced, stdout.index("    claim"))
        self.assertLess(advanced, stdout.index("    heartbeat"))
        self.assertLess(advanced, stdout.index("    work"))
        self.assertNotIn("positional arguments:", stdout)

    def test_subcommand_help_is_available(self) -> None:
        commands = [
            "plan", "add-input", "add-repo", "add-command", "sync-repos", "run", "claim",
            "release", "heartbeat", "work", "doctor", "status",
            "wait", "verify", "merge", "stop", "drain", "del", "self-update",
            "update", "version", "deploy", "run-job",
        ]
        for command in commands:
            with self.subTest(command=command):
                result = self.run_cli(command, "--help")
                self.assertEqual("", result.stderr)
                self.assertEqual(0, result.returncode)
                self.assertIn("usage:", result.stdout)
        self.assertIn("--progress-log-regex", self.run_cli("plan", "--help").stdout)
        self.assertIn("Coordinator manifest", self.run_cli("claim", "--help").stdout)
        self.assertIn("workers.json", self.run_cli("deploy", "--help").stdout)
        self.assertIn("--merge", self.run_cli("run-job", "--help").stdout)
        self.assertIn("--coordinator", self.run_cli("doctor", "--help").stdout)
        self.assertIn("workers.json", self.run_cli("update", "--help").stdout)

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

            path = crucible.expand_path(link / "manifest.json")

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
                run = self.run_cli("run", "--manifest", str(manifest), "--index", str(index))
                self.assertEqual("", run.stderr)
                self.assertEqual(0, run.returncode)

            status = self.run_cli("status", str(run_dir))
            self.assertEqual(0, status.returncode)
            self.assertIn("tasks=3/3", status.stdout)
            self.assertIn("kind=sprt", status.stdout)
            self.assertIn("desc=native check", status.stdout)
            self.assertRegex(status.stdout.splitlines()[1], r"^tasks=3/3 kind=sprt desc=native check")

            merged = root / "merged.txt"
            merge = self.run_cli(
                "merge", str(run_dir),
                "--output", str(merged),
                "--newline-between",
            )
            self.assertEqual(0, merge.returncode)
            self.assertEqual("0\n\n1\n\n2\n\n", merged.read_text(encoding="utf-8"))

    def test_run_job_local_wait_verify_and_merge(self) -> None:
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
                "--name", "run-job-pass",
                "--shards", "4",
                "--work-dir", str(repo),
                "--out", str(manifest),
                "--command-template", "mkdir -p \"$(dirname {output})\" && printf '{index}\\n' > {output}",
                "--output-template", str(repo / "out" / "task.{index}.txt"),
            )
            self.assertEqual("", plan.stderr)
            self.assertEqual(0, plan.returncode)

            workers = root / "workers.json"
            crucible.write_json(workers, {
                "schema": "crucible.workers.v1",
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
                "run-job", str(manifest),
                "--workers", str(workers),
                "--jobs", "1",
                "--wait",
                "--interval-seconds", "0.01",
                "--timeout-seconds", "5",
                "--merge", str(merged),
                "--newline-between",
                env={"CRUCIBLE_RUNS_DIR": str(root / "runs")},
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            self.assertIn("deploy: tasks complete", result.stdout)
            self.assertIn("tasks=4/4", result.stdout)
            self.assertIn("ok", result.stdout)
            self.assertIn("run-job: ok", result.stdout)
            self.assertEqual("0\n\n1\n\n2\n\n3\n\n", merged.read_text(encoding="utf-8"))

    def test_run_job_does_not_merge_failed_tasks(self) -> None:
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
                "--name", "run-job-fail",
                "--shards", "2",
                "--work-dir", str(repo),
                "--out", str(manifest),
                "--command-template", "if [ {index} -eq 0 ]; then printf ok > {output}; else false; fi",
                "--output-template", str(repo / "out" / "task.{index}.txt"),
            )
            self.assertEqual(0, plan.returncode)

            workers = root / "workers.json"
            crucible.write_json(workers, {
                "schema": "crucible.workers.v1",
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
                "run-job", str(manifest),
                "--workers", str(workers),
                "--jobs", "1",
                "--wait",
                "--merge", str(merged),
                env={"CRUCIBLE_RUNS_DIR": str(root / "runs")},
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("deploy: tasks failed", result.stdout)
            self.assertIn("run-job: deploy failed", result.stderr)
            self.assertFalse(merged.exists())

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

    def test_plan_accepts_user_facing_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"

            result = self.run_cli(
                "plan",
                "--name", "simple-plan",
                "--tasks", "1",
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
            materialized = crucible.materialize_manifest(
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
            crucible.write_json(path, {"old": True})

            crucible.write_json(path, {"new": True})

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
        self.assertIsNone(crucible.bounded_timeout(-1))
        self.assertIsNone(crucible.bounded_timeout(0))
        self.assertEqual(15.0, crucible.bounded_timeout(None, 15.0))

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

            run = self.run_cli("run", "--manifest", str(manifest), "--index", "0")

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
                "schema": "crucible.task.v1",
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

            result = self.run_cli("run", "--manifest", str(path), "--index", "0")
            self.assertNotEqual(0, result.returncode)
            self.assertIn("git commit", result.stderr)

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
                "schema": "crucible.task.v1",
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
                "run",
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
                "schema": "crucible.task.v1",
                "name": "profile-inputs",
                "work_dir": str(root),
                "requirements": {
                    "inputs": [
                        {
                            "path": str(common),
                            "sha256": crucible.sha256_file(common),
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

            crucible.check_requirements(manifest)
            self.assertEqual(["--profile", "mac"], crucible.worker_profile_args(manifest, {"profile": "mac"}))

            with self.assertRaises(SystemExit) as missing:
                crucible.check_requirements(crucible.apply_profile(manifest, "mac"))
            self.assertIn("required input missing", str(missing.exception))

            mac_input.write_text("mac\n", encoding="utf-8")
            with self.assertRaises(SystemExit) as mismatch:
                crucible.check_requirements(crucible.apply_profile(manifest, "mac"))
            self.assertIn("checksum mismatch", str(mismatch.exception))

            manifest["requirements"]["inputs"][1]["sha256"] = crucible.sha256_file(mac_input)
            crucible.check_requirements(crucible.apply_profile(manifest, "mac"))

    def test_add_command_accepts_path_only_requirement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tool = root / "tool"
            tool.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            tool.chmod(0o755)
            manifest = root / "manifest.json"
            crucible.write_json(manifest, {
                "schema": "crucible.task.v1",
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
                "schema": "crucible.task.v1",
                "name": "worker-paths",
                "work_dir": "/home/petter/code/cpp/chess/nnue",
                "state_dir": "/home/petter/code/cpp/chess/crucible/runs/job/state",
                "log_dir": "/home/petter/code/cpp/chess/crucible/runs/job/logs",
                "claim_dir": "/home/petter/code/cpp/chess/crucible/runs/job/claims",
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

            local = crucible.worker_execution_manifest(manifest, profile="mac", manifest_path=manifest_path)

            self.assertEqual("/Users/pwahlman/code/cpp/chess/nnue_worker", local["work_dir"])
            self.assertEqual(
                "/Users/pwahlman/code/cpp/chess/nnue_worker/runs/job/out.txt",
                local["tasks"][0]["outputs"][0],
            )
            self.assertEqual(str(manifest_path.parent / "worker-state" / "state"), local["state_dir"])
            self.assertEqual(str(manifest_path.parent / "worker-state" / "logs"), local["log_dir"])
            self.assertEqual(str(manifest_path.parent / "worker-state" / "claims"), local["claim_dir"])

    def test_worker_waits_when_no_claim_but_coordinator_has_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                mock.patch("crucible_lib.tasks.check_coordinator_cli"),
                mock.patch("crucible_lib.tasks.check_requirements"),
                mock.patch("crucible_lib.tasks.claim_from_coordinator", side_effect=[None, claim]) as claim_mock,
                mock.patch("crucible_lib.tasks.coordinator_has_claimable_or_active_work", return_value=True),
                mock.patch("crucible_lib.tasks.time.sleep") as sleep_mock,
                mock.patch("crucible_lib.tasks.heartbeat_on_coordinator"),
                mock.patch("crucible_lib.tasks.run_task", return_value=0) as run_task_mock,
                mock.patch("crucible_lib.tasks.sync_task_result"),
                mock.patch("crucible_lib.tasks.release_on_coordinator"),
            ):
                rc = crucible.cmd_work(args)

            self.assertEqual(0, rc)
            self.assertEqual(2, claim_mock.call_count)
            sleep_mock.assert_called_once()
            run_task_mock.assert_called_once()

    def test_run_task_terminates_child_group_on_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "crucible.task.v1",
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
                        raise crucible.TaskSignalInterrupt(signal.SIGTERM)
                    return 0

            fake_proc = FakeProc()
            with (
                mock.patch("crucible.subprocess.Popen", return_value=fake_proc),
                mock.patch("crucible.os.getpgid", return_value=fake_proc.pid),
                mock.patch("crucible.os.killpg") as killpg_mock,
                mock.patch("crucible.platform.platform", return_value="test-platform"),
            ):
                rc = crucible.run_task(manifest, task, force=False)

            self.assertEqual(128 + signal.SIGTERM, rc)
            killpg_mock.assert_called_with(fake_proc.pid, signal.SIGTERM)
            self.assertTrue(crucible.task_fail_path(manifest, task).exists())
            self.assertFalse(crucible.task_running_path(manifest, task).exists())
            log = crucible.task_log_path(manifest, task).read_text(encoding="utf-8")
            self.assertIn("terminate signal", log)

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

            first = self.run_cli("run", "--manifest", str(manifest), "--index", "0")
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

            second = self.run_cli("run", "--manifest", str(manifest), "--index", "1")
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

    def test_wait_sends_done_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "wait-done" / "manifest.json"
            crucible.write_json(manifest, {
                "schema": "crucible.task.v1",
                "name": "wait-done",
                "kind": "smoke",
                "description": "wait done check",
                "work_dir": str(root),
                "state_dir": str(root / "wait-done" / "state"),
                "log_dir": str(root / "wait-done" / "logs"),
                "claim_dir": str(root / "wait-done" / "claims"),
                "tasks": [],
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
            with mock.patch("crucible_lib.status.run_notify_command", capture_notify):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_wait(args)

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

    def test_wait_sends_fail_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "wait-fail" / "manifest.json"
            out = root / "wait-fail" / "out.txt"
            crucible.write_json(manifest, {
                "schema": "crucible.task.v1",
                "name": "wait-fail",
                "work_dir": str(root),
                "state_dir": str(root / "wait-fail" / "state"),
                "log_dir": str(root / "wait-fail" / "logs"),
                "claim_dir": str(root / "wait-fail" / "claims"),
                "tasks": [{
                    "id": "task_0",
                    "index": 0,
                    "command": "false",
                    "outputs": [str(out)],
                }],
            })
            loaded = crucible.load_manifest(manifest)
            crucible.write_json(crucible.task_fail_path(loaded, loaded["tasks"][0]), {
                "worker": {"host": "worker-a"},
                "rc": 1,
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
            with mock.patch("crucible_lib.status.run_notify_command", capture_notify):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_wait(args)

            self.assertEqual(1, rc)
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("run_fail", payload["event"])
            self.assertEqual("wait-fail", payload["run"])
            self.assertEqual("failed", payload["state"])
            self.assertEqual(1, payload["rc"])
            self.assertEqual(1, payload["counts"]["fail"])

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
            self.assertIn("claimed=1", status.stdout)

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
            self.assertIn("worker-a [claimed] task=0", status_with_progress.stdout)
            self.assertNotIn("worker-a [claimed] task=0 [subtask=42/1000", status_with_progress.stdout)

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
            self.assertIn("worker-a [claimed] task=0", status_unknown_total.stdout)

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
            self.assertIn("claimed=1", status.stdout)

            release = self.run_cli(
                "release",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--claim-id", claim_id,
            )
            self.assertEqual(0, release.returncode)
            status = self.run_cli("status", str(root))
            self.assertIn("pending=1", status.stdout)

    def test_malformed_claim_is_claimed_until_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "crucible.task.v1",
                "name": "bad-claim",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = crucible.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            crucible.task_claim_path(manifest, task).write_text("{broken", encoding="utf-8")

            crucible.clear_state_diagnostics()
            self.assertEqual("claimed", crucible.task_state(manifest, task, lease_seconds=600))
            self.assertIn("claim.json", "\n".join(crucible.state_diagnostics()))

            os.utime(claim, (100.0, 100.0))
            with mock.patch("crucible_lib.tasks.epoch", return_value=1000.0):
                self.assertEqual("stale", crucible.task_state(manifest, task, lease_seconds=1))

    def test_malformed_heartbeat_is_diagnostic_not_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "crucible.task.v1",
                "name": "bad-heartbeat",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = crucible.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            heartbeat_epoch = crucible.epoch()
            crucible.write_json(crucible.task_claim_path(manifest, task), {
                "task": "task_0",
                "worker": "worker-a",
                "claimed_epoch": heartbeat_epoch,
                "heartbeat_epoch": heartbeat_epoch,
            })
            crucible.task_heartbeat_path(manifest, task).write_text("{broken", encoding="utf-8")

            crucible.clear_state_diagnostics()
            self.assertEqual("claimed", crucible.task_state(manifest, task, lease_seconds=600))
            self.assertIn("heartbeat.json", "\n".join(crucible.state_diagnostics()))

    def test_verbose_status_shows_state_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs" / "bad-claim"
            manifest = {
                "schema": "crucible.task.v1",
                "name": "bad-claim",
                "work_dir": str(root),
                "state_dir": str(run / "state"),
                "log_dir": str(run / "logs"),
                "claim_dir": str(run / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
            }
            run.mkdir(parents=True)
            crucible.write_json(run / "manifest.json", manifest)
            task = manifest["tasks"][0]
            claim = crucible.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            crucible.task_claim_path(manifest, task).write_text("{broken", encoding="utf-8")

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = crucible.cmd_status(
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
                "schema": "crucible.task.v1",
                "name": "zombie-claim",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            claim = crucible.task_claim_dir(manifest, task)
            claim.mkdir(parents=True)
            old = crucible.epoch() - 30
            os.utime(claim, (old, old))

            self.assertEqual("stale", crucible.task_state(manifest, task, lease_seconds=1))
            reclaimed = crucible.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=1,
                retry_failed=False,
            )

            self.assertIsNotNone(reclaimed)
            self.assertEqual("task_0000", reclaimed["task"])
            payload = crucible.read_json(crucible.task_claim_path(manifest, task))
            self.assertEqual("worker-b", payload["worker"])

    def test_stale_running_marker_does_not_block_reclaim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "crucible.task.v1",
                "name": "stale-running",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{"id": "task_0000", "index": 0, "command": "true", "outputs": []}],
            }
            task = manifest["tasks"][0]
            running = crucible.task_running_path(manifest, task)
            running.parent.mkdir(parents=True)
            running.write_text(json.dumps({"task": "task_0000"}), encoding="utf-8")
            old = crucible.epoch() - 30
            os.utime(running, (old, old))

            self.assertEqual("stale", crucible.task_state(manifest, task, lease_seconds=1))
            reclaimed = crucible.claim_task(
                manifest,
                worker="worker-b",
                profile=None,
                lease_seconds=1,
                retry_failed=False,
            )

            self.assertIsNotNone(reclaimed)
            self.assertFalse(running.exists())

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
                "schema": "crucible.task.v1",
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

            run = self.run_cli("run", "--manifest", str(manifest_path), "--index", "0")
            self.assertEqual(0, run.returncode)
            verify = self.run_cli("verify", str(root))
            self.assertNotEqual(0, verify.returncode)
            self.assertIn("no outputs declared", verify.stderr)

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
                "schema": "crucible.task.v1",
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
            self.assertIn("claimed=0", status.stdout)
            self.assertIn("pending=1", status.stdout)

    def test_worker_doctor_checks_coordinator_callback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
                    "name": "doctor-callback",
                    "work_dir": str(root),
                    "requirements": {"python_min": "3.11", "git_min": "2.38"},
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            with mock.patch("crucible_lib.tasks.check_coordinator_cli") as check:
                rc = crucible.cmd_doctor(argparse.Namespace(
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
                "progress_data": crucible.normalize_progress_data({
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
                "progress_data": crucible.normalize_progress_data({
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
                "progress_data": crucible.normalize_progress_data({
                    "kind": "metadata",
                    "done": 10,
                    "total": 31,
                    "unit": "rows",
                }),
            },
            {"index": 4, "state": "running"},
        ]

        ordered = sorted(rows, key=crucible.active_status_sort_key)
        self.assertEqual([5, 6, 2, 1, 3, 4], [row["index"] for row in ordered])

    def test_status_verbose_shows_host_and_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "crucible.task.v1",
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
            self.assertIn(
                "tasks=1/4 active=1 running=1 claimed=0 progress=subtask=42/1000 fail=1 pending=1 stale=0",
                compact.stdout,
            )
            self.assertNotIn("host-done [1]: 0", compact.stdout)
            self.assertIn("worker-claimed task=1 [subtask=42/1000]", compact.stdout)
            self.assertIn("host-fail [1]: 2", compact.stdout)
            self.assertIn('pending: 1\n  "score:uci" [1]: 3', compact.stdout)

            result = self.run_cli("status", str(root), "--verbose")
            self.assertEqual(1, result.returncode)
            self.assertIn("host-done [1]: 0", result.stdout)
            self.assertIn("task_0\tdone\thost-done", result.stdout)
            self.assertIn("task_1\trunning\tworker-claimed", result.stdout)
            self.assertIn("task_2\tfail\thost-fail", result.stdout)
            self.assertIn("task_3\tpending\t", result.stdout)

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
                "schema": "crucible.task.v1",
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
            high = status.stdout.index("worker-high task=1")
            low = status.stdout.index("worker-low task=0")
            none = status.stdout.index("worker-none task=2")
            self.assertLess(high, low)
            self.assertLess(low, none)

    def test_status_infers_python_subcommand_task_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "crucible.task.v1",
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
            old = crucible.epoch() - 1000
            for task_id, worker in [("task_0", "worker-a"), ("task_1", "worker-b"), ("task_2", "worker-c")]:
                claim_dir = root / "claims" / f"{task_id}.claim"
                claim_dir.mkdir(parents=True)
                claim_epoch = old if task_id == "task_2" else crucible.epoch()
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
            self.assertIn('worker-a "score:uci" task=0', status.stdout)
            self.assertIn('worker-b [claimed] "score:uci" task=1', status.stdout)
            self.assertIn('worker-c [stale] "score:uci" task=2', status.stdout)

    def test_pending_status_groups_by_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            manifest = {
                "schema": "crucible.task.v1",
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
            self.assertIn('pending: 4\n  "label:uci" [1]: 1\n  "score:uci" [1]: 0\n  unlabeled [2]: 2-3', status.stdout)

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
                        "schema": "crucible.task.v1",
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
                    "schema": "crucible.task.v1",
                    "name": "setup",
                    "work_dir": str(root / "examples"),
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            example_task_manifest = root / "examples" / "nnue-selfplay.manifest.json"
            example_task_manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    rc = crucible.cmd_status(
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
                self.assertRegex(status_text, r"main\s+current tasks=0/1")
                self.assertNotIn("bonus", status_text)
                self.assertNotIn("setup", status_text)
                self.assertNotIn("nnue-selfplay", status_text)
                self.assertNotIn("worker-main task=0", status_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("running=1 claimed=0", active_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertNotIn("rows", payload["runs"][0])
                self.assertNotIn("groups", payload["runs"][0])
                self.assertIn("state", payload["runs"][0])

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("worker-main task=0", all_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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

    def test_status_hides_never_started_pending_runs_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def write_run(name: str) -> Path:
                run = root / "runs" / name
                manifest = run / "manifest.json"
                manifest.parent.mkdir(parents=True)
                manifest.write_text(
                    json.dumps({
                        "schema": "crucible.task.v1",
                        "name": name,
                        "work_dir": str(run),
                        "state_dir": str(run / "state"),
                        "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": []}],
                    }),
                    encoding="utf-8",
                )
                return run

            write_run("queued")
            active = write_run("active")
            state = active / "state"
            state.mkdir()
            (state / "task_0.running.json").write_text(
                json.dumps({"task": "task_0", "host": "worker-a"}),
                encoding="utf-8",
            )

            env = {"CRUCIBLE_COORDINATOR_RUNS": "", "CRUCIBLE_RUNS_DIR": str(root / "runs")}
            active_status = self.run_cli("status", cwd=root, env=env)
            self.assertEqual(0, active_status.returncode)
            self.assertIn("active", active_status.stdout)
            self.assertNotIn("queued", active_status.stdout)

            all_status = self.run_cli("status", "--all", cwd=root, env=env)
            self.assertEqual(0, all_status.returncode)
            self.assertIn("active", all_status.stdout)
            self.assertIn("queued", all_status.stdout)
            self.assertRegex(all_status.stdout, r"queued\s+queued tasks=0/1")

    def test_status_empty_active_message_is_specific(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run = runs / "done-run"
            manifest = run / "manifest.json"
            manifest.parent.mkdir(parents=True)
            manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    rc = crucible.cmd_status(
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
                self.assertIn("no active Crucible runs found", stdout.getvalue())

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                "CRUCIBLE_COORDINATOR_RUNS": "",
                "CRUCIBLE_RUNS_DIR": str(root / "runs"),
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
                    "schema": "crucible.task.v1",
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

            with mock.patch.dict(os.environ, {"CRUCIBLE_RUNS_DIR": str(runs)}):
                result = self.run_cli("status", cwd=root)
            self.assertEqual(0, result.returncode)
            self.assertIn("main", result.stdout)

    def test_status_delegates_to_coordinator_runs_env(self) -> None:
        remote_result = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="remote status\n",
            stderr="",
        )
        with mock.patch.dict(
            os.environ,
            {"CRUCIBLE_COORDINATOR_RUNS": "pwa-5090:/home/petter/code/cpp/chess/crucible/runs"},
        ), mock.patch("crucible.subprocess.run", return_value=remote_result) as run_mock:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = crucible.cmd_status(
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
        self.assertEqual(
            "coordinator: pwa-5090:/home/petter/code/cpp/chess/crucible/runs\nremote status\n",
            stdout.getvalue(),
        )
        command = run_mock.call_args.args[0]
        self.assertEqual("ssh", command[0])
        self.assertIn("pwa-5090", command)
        self.assertIn("CRUCIBLE_RUNS_DIR=/home/petter/code/cpp/chess/crucible/runs", command[-1])
        self.assertIn("CRUCIBLE_COORDINATOR_RUNS=", command[-1])

    def test_status_delegates_to_runs_coordinator_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            runs.mkdir()
            (runs / "coordinator").write_text(
                "pwa-5090:/home/petter/code/cpp/chess/crucible/runs\n",
                encoding="utf-8",
            )
            remote_result = subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout="remote status\n",
                stderr="",
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("crucible.subprocess.run", return_value=remote_result) as run_mock,
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
            self.assertEqual(
                "coordinator: pwa-5090:/home/petter/code/cpp/chess/crucible/runs\nremote status\n",
                stdout.getvalue(),
            )
            command = run_mock.call_args.args[0]
            self.assertEqual("ssh", command[0])
            self.assertIn("pwa-5090", command)

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
                "schema": "crucible.task.v1",
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
                        "log_regex": r"selected=(?P<done>\d+)\s+written=(?P<output>\d+)\s+rate=(?P<rate>[0-9.]+)/s",
                        "output_unit": "output",
                    },
                }],
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
                    rc = crucible.cmd_status(
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
                self.assertIn("progress=50k/125k rows, output=48k, eta=3h06m", stdout.getvalue())

                named_stdout = io.StringIO()
                with contextlib.redirect_stdout(named_stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("progress=50k/125k rows, output=48k, eta=3h06m", named_stdout.getvalue())

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
                "schema": "crucible.task.v1",
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
                ],
            }
            (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (state / "task_0.running.json").write_text(json.dumps({"task": "task_0", "host": "worker-a"}))
            (state / "task_1.running.json").write_text(json.dumps({"task": "task_1", "host": "worker-a-slot2"}))
            (state / "task_2.running.json").write_text(json.dumps({"task": "task_2", "host": "worker-b"}))
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

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn(
                    "active: 3 (workers=2, max=worker-a avg_rows/s=4.35, min=worker-b avg_rows/s=1)",
                    text,
                )
                self.assertIn("active workers:", text)
                self.assertIn(
                    "worker-a: slots=2, rows=51k/250k, avg_rows/s=4.35",
                    text,
                )
                self.assertIn(
                    "worker-b: slots=1, rows=1000/125k, avg_rows/s=1",
                    text,
                )
                self.assertNotIn("avg_output", text)

    def test_status_normalizes_localhost_worker_name(self) -> None:
        with mock.patch("crucible_lib.status.socket.gethostname", return_value="coordinator"):
            self.assertEqual("coordinator", crucible.worker_group_name("localhost"))
            self.assertEqual("coordinator", crucible.worker_group_name("127.0.0.1"))
            self.assertEqual("coordinator", crucible.worker_group_name("::1"))

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
                "schema": "crucible.task.v1",
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

            with self.patch_runs_dir(runs), mock.patch("crucible_lib.status.epoch", return_value=1100.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("progress=1500/2000 games, eta=1m40s", text)
                self.assertIn("worker-a task=1 [500/1000 games, eta=1m40s]", text)

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
                "schema": "crucible.task.v1",
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

            with self.patch_runs_dir(runs), mock.patch("crucible_lib.status.epoch", return_value=1001.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("progress=1000/2000 games, eta=1m39s", text)
                self.assertIn("worker-a task=1 [0/1000 games, eta=1m39s]", text)

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
                "schema": "crucible.task.v1",
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

            with self.patch_runs_dir(runs), mock.patch("crucible_lib.status.epoch", return_value=1110.0):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
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
                self.assertIn("progress=1000/2000 games, eta=unknown", text)
                self.assertIn("worker-a task=1 [0/1000 games, eta=unknown]", text)

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
                "schema": "crucible.task.v1",
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
                    rc = crucible.cmd_status(
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
                "schema": "crucible.task.v1",
                "name": "active",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [{"id": "task_0", "index": 0, "command": "true", "outputs": [str(run_dir / "outputs/out.txt")]}],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                stop = crucible.cmd_stop(argparse.Namespace(run="active", all=False, lease_seconds=600))
                self.assertEqual(0, stop)
                result = crucible.claim_task(
                    crucible.load_manifest(run_dir / "manifest.json"),
                    worker="worker",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )
                self.assertIsNone(result)

    def test_drain_prevents_worker_claims_until_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "active"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "crucible.task.v1",
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
                drain = crucible.cmd_drain(
                    argparse.Namespace(run="active", all=False, worker="worker-a", clear=False, lease_seconds=600)
                )
                self.assertEqual(0, drain)
                loaded = crucible.load_manifest(run_dir / "manifest.json")
                self.assertIsNone(crucible.claim_task(
                    loaded,
                    worker="worker-a",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                ))
                self.assertEqual("task_0", crucible.claim_task(
                    loaded,
                    worker="worker-b",
                    profile=None,
                    lease_seconds=600,
                    retry_failed=False,
                )["task"])

                clear = crucible.cmd_drain(
                    argparse.Namespace(run="active", all=False, worker="worker-a", clear=True, lease_seconds=600)
                )
                self.assertEqual(0, clear)
                self.assertEqual("task_1", crucible.claim_task(
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
                "--tasks", "3",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf x > {output}",
                "--output-template", str(root / "out.{index}.txt"),
            )
            self.assertEqual(0, plan.returncode)
            loaded = crucible.load_manifest(manifest)

            first = crucible.claim_task(
                loaded,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            second = crucible.claim_task(
                loaded,
                worker="worker-a",
                profile=None,
                lease_seconds=600,
                retry_failed=False,
            )
            third = crucible.claim_task(
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
            self.assertIn("running=1", status.stdout)
            self.assertIn("claimed=1", status.stdout)
            self.assertIn("pending=1", status.stdout)

    def test_delete_done_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            run_dir = runs / "finished"
            run_dir.mkdir(parents=True)
            manifest = {
                "schema": "crucible.task.v1",
                "name": "finished",
                "work_dir": str(root),
                "state_dir": str(run_dir / "state"),
                "log_dir": str(run_dir / "logs"),
                "claim_dir": str(run_dir / "claims"),
                "tasks": [],
            }
            (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            with self.patch_runs_dir(runs):
                rc = crucible.cmd_del(argparse.Namespace(run=None, done=True, force=True, lease_seconds=600))

            self.assertEqual(0, rc)
            self.assertFalse(run_dir.exists())

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
                "schema": "crucible.task.v1",
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
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.task.v1",
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
                "schema": "crucible.task.v1",
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
                "schema": "crucible.task.v1",
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
                "schema": "crucible.task.v1",
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
                "schema": "crucible.task.v1",
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
            self.assertIn("claimed=0", status.stdout)
            self.assertIn("pending=1", status.stdout)

    def test_version_reports_current_checkout(self) -> None:
        result = self.run_cli("version", "--json")
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)
        payload = json.loads(result.stdout)
        self.assertEqual("crucible", payload["tool"])
        self.assertIn("commit", payload)
        self.assertIn("repo", payload)

    def test_version_payload_uses_current_branch_remote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            subprocess.run(["git", "init", "-b", "develop", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            subprocess.run(["git", "config", "branch.develop.remote", "origin"], cwd=repo, check=True)
            (repo / "crucible.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "crucible.py"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            import crucible_lib.core as crucible_core

            original_file = crucible_core.__file__
            try:
                crucible_core.__file__ = str(repo / "crucible.py")
                payload = crucible.crucible_version_payload()
            finally:
                crucible_core.__file__ = original_file

            self.assertEqual("develop", payload["branch"])
            self.assertEqual("origin", payload["remote"])

    def test_self_update_fast_forwards_crucible_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            clone = root / "clone"
            subprocess.run(["git", "init", "-b", "main", str(source)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=source, check=True)
            (source / "crucible.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "crucible.py"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "clone", str(source), str(clone)], check=True, stdout=subprocess.DEVNULL)
            (source / "marker.txt").write_text("two", encoding="utf-8")
            subprocess.run(["git", "add", "marker.txt"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "two"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            expected = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()

            import crucible_lib.core as crucible_core

            original_file = crucible_core.__file__
            try:
                crucible_core.__file__ = str(clone / "crucible.py")
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_self_update(
                        argparse.Namespace(
                            remote="origin",
                            branch=None,
                            require_clean=True,
                            json=True,
                        )
                    )
            finally:
                crucible_core.__file__ = original_file
            self.assertEqual(0, rc)
            head = subprocess.check_output(["git", "-C", str(clone), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(expected, head)

    def test_update_matches_workers_to_coordinator_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workers = root / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
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
                stdout="crucible abc main\n",
                stderr="",
            )
            with mock.patch("crucible_lib.workers.run_worker_shell", return_value=result) as run_mock:
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_update(
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
            self.assertIn("localhost: OK crucible abc main", stdout.getvalue())
            self.assertIn("remote-a: OK crucible abc main", stdout.getvalue())

    def test_update_uses_default_workers_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            runs.mkdir()
            workers = runs / "workers.json"
            workers.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "local": True}],
                }),
                encoding="utf-8",
            )
            result = subprocess.CompletedProcess(
                args=["sh"],
                returncode=0,
                stdout="crucible abc main\n",
                stderr="",
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("crucible_lib.workers.run_worker_shell", return_value=result) as run_mock,
            ):
                rc = crucible.cmd_update(
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

    def test_remote_crucible_command_uses_path(self) -> None:
        command = crucible.remote_crucible_command(["status", "run name"])

        self.assertIn("command -v crucible", command)
        self.assertIn("exec crucible status 'run name'", command)

    def test_check_coordinator_cli_fails_early(self) -> None:
        result = subprocess.CompletedProcess(["ssh"], 127, stdout="", stderr="crucible not found")

        with mock.patch("crucible_lib.tasks.run_coordinator_cli", return_value=result):
            with self.assertRaises(SystemExit) as raised:
                crucible.check_coordinator_cli("pwa-5090:/tmp/manifest.json")

        self.assertIn("crucible not found", str(raised.exception))


    def test_load_workers_reads_workers_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
                    "workers": [
                        "pwa-5090",
                        {"host": "pwa-wsl", "profile": "pwa-wsl"},
                        {"host": "pwa-mbp", "profile": "mac", "python": "/opt/homebrew/bin/python3"},
                        {"host": "localhost", "local": True},
                        {"host": "disabled", "enabled": False},
                    ],
                }),
                encoding="utf-8",
            )

            workers = crucible.load_workers(workers_path)

            self.assertEqual(
                [
                    {"host": "pwa-5090", "profile": "pwa-5090", "local": False},
                    {"host": "pwa-wsl", "profile": "pwa-wsl", "local": False},
                    {
                        "host": "pwa-mbp",
                        "profile": "mac",
                        "local": False,
                        "python": "/opt/homebrew/bin/python3",
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
                    "schema": "crucible.workers.v1",
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

            workers = crucible.load_workers(workers_path)

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

    def test_expand_worker_record_does_not_mutate_input(self) -> None:
        worker = {"host": "pwa-5090", "profile": "linux", "local": False, "slots": 2}

        workers = crucible.expand_worker_record(worker)

        self.assertEqual({"host": "pwa-5090", "profile": "linux", "local": False, "slots": 2}, worker)
        self.assertEqual([1, 2], [item["_slot"] for item in workers])
        self.assertTrue(all("slots" not in item for item in workers))

    def test_load_workers_rejects_invalid_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "pwa-5090", "slots": 0}],
                }),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit) as raised:
                crucible.load_workers(workers_path)

            self.assertIn("pwa-5090: worker slots", str(raised.exception))

    def test_deploy_commands_use_worker_python(self) -> None:
        worker = {
            "host": "pwa-mbp",
            "profile": "pwa-mbp",
            "local": False,
            "python": "/opt/homebrew/bin/python3",
        }
        manifest = {"tasks": [{"id": "task_0000"}]}

        prereq = crucible.deploy_prereq_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )
        work = crucible.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )

        self.assertIn("/opt/homebrew/bin/python3 .cache/crucible/crucible.py doctor", prereq)
        self.assertIn("--coordinator pwa-5090:/tmp/manifest.json", prereq)
        self.assertIn("/opt/homebrew/bin/python3 .cache/crucible/crucible.py work", work)
        self.assertIn("--idle-seconds 120.0", work)

    def test_coordinator_host_override_is_used_for_remote_workers(self) -> None:
        manifest = Path("/tmp/run/manifest.json")

        remote = crucible.coordinator_for_worker(
            {"host": "pwa-rce", "profile": "pwa-rce", "local": False},
            manifest,
            "pwa-5090.local",
        )
        local = crucible.coordinator_for_worker(
            {"host": "localhost", "profile": "localhost", "local": True},
            manifest,
            "pwa-5090.local",
        )

        self.assertEqual("pwa-5090.local:/tmp/run/manifest.json", remote)
        self.assertEqual("/tmp/run/manifest.json", local)

    def test_deploy_materializes_contained_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
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
                mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(args)
                self.assertEqual(0, rc)

                run_dir = runs / "contained"
                materialized = crucible.load_manifest(run_dir / "manifest.json")
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
                    crucible.cmd_deploy(args)
                self.assertIn("run already exists", str(raised.exception))

    def test_deploy_sends_completion_notification_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs = root / "runs"
            template = root / "template.manifest.json"
            template.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            payloads: list[dict[str, object]] = []

            def capture_notify(command: str | None, payload: dict[str, object]) -> int:
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
                notify_command="notify-me",
                no_color=True,
                verbose=False,
            )
            with (
                self.patch_runs_dir(runs),
                mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
                mock.patch("crucible_lib.workers.run_notify_command", capture_notify),
            ):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(args)

            self.assertEqual(0, rc)
            self.assertEqual(1, len(payloads))
            payload = payloads[0]
            self.assertEqual("deploy_done", payload["event"])
            self.assertEqual("notify-run", payload["run"])
            self.assertEqual("ok", payload["state"])
            self.assertEqual("smoke", payload["kind"])
            self.assertEqual("notify check", payload["description"])
            self.assertEqual(0, payload["tasks"])

    def test_example_manifests_parse_and_format_tasks(self) -> None:
        for path in sorted((ROOT / "examples").glob("*.manifest.json")):
            with self.subTest(path=path.name):
                manifest = crucible.load_manifest(path)
                for task in manifest["tasks"]:
                    self.assertTrue(crucible.format_command(manifest, task))

    def test_worker_label_omits_duplicate_profile(self) -> None:
        self.assertEqual("pwa-5090", crucible.worker_label("pwa-5090", "pwa-5090"))
        self.assertEqual("pwa-wsl", crucible.worker_label("pwa-wsl", "pwa-wsl"))
        self.assertEqual("pwa-5090#2", crucible.worker_label("pwa-5090", "pwa-5090", 2, 4))

    def test_bootstrap_worker_runner_syncs_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            cache = root / "cache"
            manifest.write_text("{}", encoding="utf-8")
            worker = {"host": "localhost", "local": True, "cache_dir": str(cache)}

            crucible.bootstrap_worker_runner(worker, manifest)

            self.assertTrue((cache / "crucible.py").exists())
            self.assertTrue((cache / "crucible_lib" / "__init__.py").exists())
            self.assertTrue((cache / "manifest.json").exists())

    def test_managed_remote_script_cleans_child_process_group(self) -> None:
        script = crucible.managed_remote_script("sleep 60")

        self.assertIn("setsid sh -lc", script)
        self.assertIn("kill -TERM -$pid", script)
        self.assertIn("kill -KILL -$pid", script)

    def test_deploy_uses_workers_json_without_unknown_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
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

            with self.patch_runs_dir(Path(tmp) / "runs"), mock.patch("crucible.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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
            self.assertIn("  OK     pwa-5090: setup", deploy_output)
            self.assertIn("  OK     localhost: setup", deploy_output)
            self.assertNotIn("syncing runner", deploy_output)
            self.assertNotIn("checking prerequisites", deploy_output)
            self.assertIn("deploy: setup complete", deploy_output)
            self.assertTrue(
                any(command[0] == "ssh" and command[-2] == "pwa-5090" and "mkdir -p .cache/crucible" in command[-1] for command in commands)
            )
            self.assertNotIn(["sh", "-lc", "mkdir -p " + str(Path(tmp) / "cache")], commands)
            self.assertTrue((Path(tmp) / "cache/crucible.py").exists())
            self.assertTrue((Path(tmp) / "cache/manifest.json").exists())
            self.assertTrue(any(command[:4] == ["rsync", "-a", "-e", crucible.RSYNC_SSH] for command in commands))
            scripts = [command[-1] for command in commands if command[0] in {"ssh", "sh"}]
            self.assertFalse(any("--profile pwa-5090" in script for script in scripts))

    def test_deploy_verbose_shows_phase_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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
                    "schema": "crucible.task.v1",
                    "name": "deploy-input-sync",
                    "work_dir": str(coordinator),
                    "state_dir": str(coordinator / "state"),
                    "log_dir": str(coordinator / "logs"),
                    "path_maps": {"worker": {str(coordinator): str(worker_root)}},
                    "requirements": {
                        "python_min": "3.11",
                        "git_min": "2.38",
                        "inputs": [crucible.file_record(source)],
                    },
                    "tasks": [],
                }),
                encoding="utf-8",
            )
            workers_path = root / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "crucible.workers.v1",
                    "workers": [{
                        "host": "localhost",
                        "profile": "worker",
                        "local": True,
                        "cache_dir": str(root / "cache"),
                    }],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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

    def test_deploy_passes_matching_profile_and_runs_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "profile": "worker", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
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

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=2,
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
            self.assertIn("deploy: run=deploy-slots-test workers=2 jobs=2", output)
            self.assertIn("  OK     localhost#1: tasks", output)
            self.assertIn("  OK     localhost#2: tasks", output)
            work_scripts = [command[-1] for command in commands if command[0] == "sh" and " work " in command[-1]]
            doctor_scripts = [command[-1] for command in commands if command[0] == "sh" and " doctor " in command[-1]]
            self.assertEqual(1, len(doctor_scripts))
            self.assertEqual(2, len(work_scripts))
            self.assertTrue(all("--worker localhost" in script for script in work_scripts))

    def test_deploy_waits_for_all_prereqs_before_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
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

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(self, command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

            with self.patch_runs_dir(root / "runs"), mock.patch("crucible.ProcessTracker.run", fake_run):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = crucible.cmd_deploy(
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
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )

            with self.patch_runs_dir(runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_deploy(
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
                    "schema": "crucible.task.v1",
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
                    "schema": "crucible.workers.v1",
                    "workers": ["pwa-5090"],
                }),
                encoding="utf-8",
            )

            with (
                self.patch_runs_dir(root / "runs"),
                mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")),
                mock.patch.object(crucible.ProcessTracker, "terminate_all") as terminate_all,
                mock.patch("crucible.concurrent.futures.as_completed", side_effect=KeyboardInterrupt),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    rc = crucible.cmd_deploy(
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
            self.assertIn("deploy: interrupted; stopping active worker commands", stderr.getvalue())

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
