from __future__ import annotations

import contextlib
import io
import json
import os
import argparse
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
    def run_cli(self, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(CLI), *args],
            cwd=cwd or ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_subcommand_help_is_available(self) -> None:
        commands = [
            "plan", "add-input", "add-repo", "sync-repos", "run", "claim",
            "release", "heartbeat", "work", "doctor", "status", "list",
            "wait", "verify", "merge", "stop", "del", "self-update",
            "version", "deploy",
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
                "--shards", "3",
                "--work-dir", str(root),
                "--out", str(manifest),
                "--command-template", "printf '{index}\\n' > {output}",
                "--output-template", out_template,
            )
            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)

            add = self.run_cli("add-input", "--manifest", str(manifest), "--path", str(root / "input.txt"))
            self.assertEqual(0, add.returncode)

            for index in range(3):
                run = self.run_cli("run", "--manifest", str(manifest), "--index", str(index))
                self.assertEqual("", run.stderr)
                self.assertEqual(0, run.returncode)

            status = self.run_cli("status", str(run_dir))
            self.assertEqual(0, status.returncode)
            self.assertIn("tasks=3/3", status.stdout)

            merged = root / "merged.txt"
            merge = self.run_cli(
                "merge", str(run_dir),
                "--output", str(merged),
                "--newline-between",
            )
            self.assertEqual(0, merge.returncode)
            self.assertEqual("0\n\n1\n\n2\n\n", merged.read_text(encoding="utf-8"))

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
            self.assertIn("worker-a claimed: 0 [subtask=42/1000, eta=7m59s]", status_with_progress.stdout)

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
            self.assertIn("worker-a claimed: 0 [subtask=43/?]", status_unknown_total.stdout)

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
                    {"id": "task_3", "index": 3, "command": "true", "outputs": [str(root / "out3.txt")]},
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
                "tasks=1/4 active=1 running=1 claimed=0 pending=1 fail=1 stale=0",
                compact.stdout,
            )
            self.assertNotIn("host-done [1]: 0", compact.stdout)
            self.assertIn("worker-claimed running: 1 [subtask=42/1000]", compact.stdout)
            self.assertIn("host-fail [1]: 2", compact.stdout)
            self.assertIn("pending:\n  3", compact.stdout)

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

            with mock.patch("crucible.runs_dir", return_value=runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_status(
                        argparse.Namespace(
                            run=None,
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
                self.assertIn("bonus", status_text)
                self.assertNotIn("setup", status_text)
                self.assertNotIn("nnue-selfplay", status_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_list(
                        argparse.Namespace(
                            all=False,
                            done=False,
                            failed=False,
                            json=False,
                            no_color=True,
                            lease_seconds=600,
                        )
                    )
                self.assertEqual(0, rc)
                list_text = stdout.getvalue()
                self.assertIn("main", list_text)
                self.assertNotIn("bonus", list_text)
                self.assertIn("running=0 claimed=0", list_text)

                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_list(
                        argparse.Namespace(
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
                    rc = crucible.cmd_list(
                        argparse.Namespace(
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

    def test_list_shows_declared_progress(self) -> None:
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

            with mock.patch("crucible.runs_dir", return_value=runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_list(
                        argparse.Namespace(
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

    def test_list_ignores_undeclared_domain_progress(self) -> None:
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

            with mock.patch("crucible.runs_dir", return_value=runs):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = crucible.cmd_list(
                        argparse.Namespace(
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

            with mock.patch("crucible.runs_dir", return_value=runs):
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

            with mock.patch("crucible.runs_dir", return_value=runs):
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

            original_file = crucible.__file__
            try:
                crucible.__file__ = str(repo / "crucible.py")
                payload = crucible.crucible_version_payload()
            finally:
                crucible.__file__ = original_file

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

            original_file = crucible.__file__
            try:
                crucible.__file__ = str(clone / "crucible.py")
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
                crucible.__file__ = original_file
            self.assertEqual(0, rc)
            head = subprocess.check_output(["git", "-C", str(clone), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(expected, head)

    def test_remote_crucible_command_uses_path(self) -> None:
        command = crucible.remote_crucible_command(["status", "run name"])

        self.assertIn("command -v crucible", command)
        self.assertIn("exec crucible status 'run name'", command)

    def test_check_coordinator_cli_fails_early(self) -> None:
        result = subprocess.CompletedProcess(["ssh"], 127, stdout="", stderr="crucible not found")

        with mock.patch("crucible.run_coordinator_cli", return_value=result):
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

        prereq = crucible.deploy_prereq_command(worker=worker, manifest=manifest)
        work = crucible.deploy_work_command(
            worker=worker,
            manifest=manifest,
            coordinator="pwa-5090:/tmp/manifest.json",
        )

        self.assertIn("/opt/homebrew/bin/python3 .cache/crucible/crucible.py doctor", prereq)
        self.assertIn("/opt/homebrew/bin/python3 .cache/crucible/crucible.py work", work)

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
                mock.patch("crucible.runs_dir", return_value=runs),
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

            with mock.patch("crucible.ProcessTracker.run", fake_run):
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

            with mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
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

            with mock.patch("crucible.ProcessTracker.run", return_value=subprocess.CompletedProcess(["sh"], 0, stdout="", stderr="")):
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

            with mock.patch("crucible.ProcessTracker.run", fake_run):
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

            with mock.patch("crucible.ProcessTracker.run", fake_run):
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

    def test_deploy_starts_ready_worker_before_all_prereqs_finish(self) -> None:
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
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with mock.patch("crucible.ProcessTracker.run", fake_run):
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
            self.assertLess(events.index("fast_work_start"), events.index("slow_prereq_done"))

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

            with mock.patch("crucible.ProcessTracker.run", fake_run):
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
