from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "distribute.py"


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


if __name__ == "__main__":
    unittest.main()
