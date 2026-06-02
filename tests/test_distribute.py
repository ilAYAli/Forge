from __future__ import annotations

import contextlib
import io
import json
import os
import argparse
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "distribute.py"
sys.path.insert(0, str(ROOT))
import distribute  # noqa: E402


class DistributeTests(unittest.TestCase):
    def run_cli(self, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(CLI), *args],
            cwd=cwd or ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    @unittest.skipIf(not hasattr(os, "symlink"), "symlink support required")
    def test_expand_path_preserves_symlink_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            real = root / "real"
            link = root / "link"
            real.mkdir()
            link.symlink_to(real, target_is_directory=True)

            path = distribute.expand_path(link / "manifest.json")

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

            manifest = root / "manifest.json"
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

            status = self.run_cli("status", "--manifest", str(manifest))
            self.assertEqual(0, status.returncode)
            self.assertIn("done=3", status.stdout)

            merged = root / "merged.txt"
            merge = self.run_cli(
                "merge",
                "--manifest", str(manifest),
                "--output", str(merged),
                "--newline-between",
            )
            self.assertEqual(0, merge.returncode)
            self.assertEqual("0\n\n1\n\n2\n\n", merged.read_text(encoding="utf-8"))

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
                "schema": "distribute.task.v1",
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
                "schema": "distribute.task.v1",
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

            status = self.run_cli("status", "--manifest", str(manifest))
            self.assertEqual(0, status.returncode)
            self.assertIn("done=2", status.stdout)

            verify = self.run_cli("verify", "--manifest", str(manifest))
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
            self.assertIn("done=1", waiting.stdout)
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
            self.assertIn("done=2", done.stdout)

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

            status = self.run_cli("status", "--manifest", str(manifest))
            self.assertEqual(0, status.returncode)
            self.assertIn("claimed=1", status.stdout)

            heartbeat = self.run_cli(
                "heartbeat",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
            )
            self.assertEqual("", heartbeat.stderr)
            self.assertEqual(0, heartbeat.returncode)

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
            status = self.run_cli("status", "--manifest", str(manifest))
            self.assertIn("claimed=1", status.stdout)

            release = self.run_cli(
                "release",
                "--manifest", str(manifest),
                "--task", "task_0000",
                "--worker", "worker-a",
                "--claim-id", claim_id,
            )
            self.assertEqual(0, release.returncode)
            status = self.run_cli("status", "--manifest", str(manifest))
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
                "schema": "distribute.task.v1",
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
            verify = self.run_cli("verify", "--manifest", str(manifest_path))
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
                "schema": "distribute.task.v1",
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

            status = self.run_cli("status", "--manifest", str(manifest_path))
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
                "schema": "distribute.task.v1",
                "name": "test",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [
                    {"id": "task_0", "index": 0, "command": "true", "outputs": [str(root / "out0.txt")]},
                    {"id": "task_1", "index": 1, "command": "true", "outputs": [str(root / "out1.txt")]},
                    {"id": "task_2", "index": 2, "command": "true", "outputs": [str(root / "out2.txt")]},
                ]
            }
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            state_dir = root / "state"
            state_dir.mkdir()
            (root / "out0.txt").write_text("done")
            done_payload = {
                "task": "task_0",
                "host": "host-done",
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

            fail_payload = {
                "task": "task_2",
                "host": "host-fail",
                "rc": 1
            }
            (state_dir / "task_2.fail.json").write_text(json.dumps(fail_payload))

            result = self.run_cli("status", "--manifest", str(manifest_path), "--verbose")
            self.assertEqual(1, result.returncode)
            self.assertIn("task_0\tdone\thost-done", result.stdout)
            self.assertIn("task_1\tclaimed\tworker-claimed", result.stdout)
            self.assertIn("task_2\tfail\thost-fail", result.stdout)

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
                "schema": "distribute.task.v1",
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
                "schema": "distribute.task.v1",
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
                "schema": "distribute.task.v1",
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
                "schema": "distribute.task.v1",
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

            status = self.run_cli("status", "--manifest", str(manifest_path))
            self.assertEqual(0, status.returncode)
            self.assertIn("claimed=0", status.stdout)
            self.assertIn("pending=1", status.stdout)

    def test_version_reports_current_checkout(self) -> None:
        result = self.run_cli("version", "--json")
        self.assertEqual("", result.stderr)
        self.assertEqual(0, result.returncode)
        payload = json.loads(result.stdout)
        self.assertEqual("distrib", payload["tool"])
        self.assertIn("commit", payload)
        self.assertIn("repo", payload)

    def test_version_payload_uses_current_branch_remote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            subprocess.run(["git", "init", "-b", "develop", str(repo)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=repo, check=True)
            subprocess.run(["git", "config", "branch.develop.remote", "origin"], cwd=repo, check=True)
            (repo / "distribute.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "distribute.py"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "test"], cwd=repo, check=True, stdout=subprocess.DEVNULL)

            original_file = distribute.__file__
            try:
                distribute.__file__ = str(repo / "distribute.py")
                payload = distribute.distrib_version_payload()
            finally:
                distribute.__file__ = original_file

            self.assertEqual("develop", payload["branch"])
            self.assertEqual("origin", payload["remote"])

    def test_self_update_fast_forwards_distrib_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            clone = root / "clone"
            subprocess.run(["git", "init", "-b", "main", str(source)], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.name", "Petter Wahlman"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "petter@wahlman.no"], cwd=source, check=True)
            (source / "distribute.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "add", "distribute.py"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "one"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "clone", str(source), str(clone)], check=True, stdout=subprocess.DEVNULL)
            (source / "marker.txt").write_text("two", encoding="utf-8")
            subprocess.run(["git", "add", "marker.txt"], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "two"], cwd=source, check=True, stdout=subprocess.DEVNULL)
            expected = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()

            original_file = distribute.__file__
            try:
                distribute.__file__ = str(clone / "distribute.py")
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = distribute.cmd_self_update(
                        argparse.Namespace(
                            remote="origin",
                            branch=None,
                            require_clean=True,
                            json=True,
                        )
                    )
            finally:
                distribute.__file__ = original_file
            self.assertEqual(0, rc)
            head = subprocess.check_output(["git", "-C", str(clone), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(expected, head)

    def test_remote_distrib_command_uses_path(self) -> None:
        command = distribute.remote_distrib_command(["status", "--manifest", "/tmp/run manifest.json"])

        self.assertIn("command -v distrib", command)
        self.assertIn("exec distrib status --manifest '/tmp/run manifest.json'", command)

    def test_check_coordinator_cli_fails_early(self) -> None:
        result = subprocess.CompletedProcess(["ssh"], 127, stdout="", stderr="distrib not found")

        with mock.patch("distribute.run_coordinator_cli", return_value=result):
            with self.assertRaises(SystemExit) as raised:
                distribute.check_coordinator_cli("pwa-5090:/tmp/manifest.json")

        self.assertIn("distrib not found", str(raised.exception))


    def test_load_workers_reads_workers_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "distribute.workers.v1",
                    "workers": [
                        "pwa-5090",
                        {"host": "pwa-win", "profile": "pwa-wsl"},
                        {"host": "localhost", "local": True},
                        {"host": "disabled", "enabled": False},
                    ],
                }),
                encoding="utf-8",
            )

            workers = distribute.load_workers(workers_path)

            self.assertEqual(
                [
                    {"host": "pwa-5090", "profile": "pwa-5090", "local": False},
                    {"host": "pwa-win", "profile": "pwa-wsl", "local": False},
                    {"host": "localhost", "profile": "localhost", "local": True},
                ],
                workers,
            )

    def test_deploy_uses_workers_json_without_unknown_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workers_path = Path(tmp) / "workers.json"
            workers_path.write_text(
                json.dumps({
                    "schema": "distribute.workers.v1",
                    "workers": [
                        "pwa-5090",
                        {"host": "localhost", "local": True, "cache_dir": str(Path(tmp) / "cache")},
                    ],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with mock.patch("distribute.subprocess.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = distribute.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(ROOT / "examples/enyo-candidate-deploy.manifest.json"),
                            workers=str(workers_path),
                            jobs=1,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            self.assertTrue(
                any(command[0] == "ssh" and command[-2:] == ["pwa-5090", "mkdir -p .cache/distrib"] for command in commands)
            )
            self.assertNotIn(["sh", "-lc", "mkdir -p " + str(Path(tmp) / "cache")], commands)
            self.assertTrue((Path(tmp) / "cache/distribute.py").exists())
            self.assertTrue((Path(tmp) / "cache/manifest.json").exists())
            self.assertTrue(any(command[:4] == ["rsync", "-a", "-e", distribute.RSYNC_SSH] for command in commands))
            scripts = [command[-1] for command in commands if command[0] in {"ssh", "sh"}]
            self.assertFalse(any("--profile pwa-5090" in script for script in scripts))

    def test_deploy_passes_matching_profile_and_runs_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "distribute.task.v1",
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
                    "schema": "distribute.workers.v1",
                    "workers": [{"host": "localhost", "profile": "worker", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with mock.patch("distribute.subprocess.run", fake_run):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    rc = distribute.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            verbose=False,
                        )
                    )

            self.assertEqual(0, rc)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertTrue(any("sync-repos" in script and "--profile worker" in script for script in scripts))
            self.assertTrue(any(" work " in script and "--profile worker" in script for script in scripts))

    def test_deploy_stops_when_prerequisites_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps({
                    "schema": "distribute.task.v1",
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
                    "schema": "distribute.workers.v1",
                    "workers": [{"host": "localhost", "local": True, "cache_dir": str(root / "cache")}],
                }),
                encoding="utf-8",
            )
            commands = []

            def fake_run(command, **kwargs):
                commands.append(command)
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

            with mock.patch("distribute.subprocess.run", fake_run):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    rc = distribute.cmd_deploy(
                        argparse.Namespace(
                            manifest=str(manifest),
                            workers=str(workers_path),
                            jobs=1,
                            verbose=False,
                        )
                    )

            self.assertEqual(1, rc)
            scripts = [command[-1] for command in commands if command[0] == "sh"]
            self.assertFalse(any(" work " in script for script in scripts))

    def test_prepare_command_is_removed(self) -> None:
        result = self.run_cli("prepare", "manifest.json")
        self.assertNotEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main()
