from __future__ import annotations

import json
import argparse
import copy
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from crucible_lib.lc0 import (
    INVENTORY_SCHEMA,
    build_inventory,
    canonical_inventory_bytes,
    finalize_lc0,
    inventory_digest,
    marker_matches,
    verify_inventory_tree,
)
from crucible_lib.core import file_record, write_json
from crucible_lib.status import cmd_wait
from crucible_lib.status import active_worker_summary, status_summary
from crucible_lib.tasks import task_done_path


class Lc0InventoryTests(unittest.TestCase):
    def test_digest_excludes_host_paths(self) -> None:
        files = [
            {"path": "a/training.1.gz", "size": 3, "sha256": "1" * 64},
            {"path": "b/training.2.gz", "size": 7, "sha256": "2" * 64},
        ]
        self.assertEqual(inventory_digest(files), inventory_digest(list(files)))
        self.assertNotIn(b"/Users/", canonical_inventory_bytes(files))

    def test_reuses_hashes_for_unchanged_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            cache = root / "cache"
            source.mkdir()
            (source / "training.1.gz").write_bytes(b"one")
            (source / "training.2.gz").write_bytes(b"two")
            first = build_inventory(source, cache_home=cache)
            with mock.patch("crucible_lib.lc0.sha256_file", side_effect=AssertionError("rehash")):
                second = build_inventory(source, cache_home=cache)
            self.assertEqual(first["digest"], second["digest"])
            self.assertTrue(marker_matches(Path(second["cache_root"]), str(second["digest"])))

    def test_verifies_cache_before_writing_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "training.1.gz"
            data.write_bytes(b"abc")
            files = [{
                "path": data.name,
                "size": 3,
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            }]
            digest = inventory_digest(files)
            (root / "inventory.json").write_bytes(canonical_inventory_bytes(files) + b"\n")
            marker = verify_inventory_tree(root, digest)
            self.assertEqual(3, marker["bytes"])
            data.write_bytes(b"bad")
            (root / ".verified.json").unlink()
            with self.assertRaisesRegex(ValueError, "checksum"):
                verify_inventory_tree(root, digest)
            self.assertFalse((root / ".verified.json").exists())


class Lc0FinalizationTests(unittest.TestCase):
    def make_run(self, root: Path, *, replace: bool = False) -> tuple[Path, dict]:
        outputs = root / "outputs"
        outputs.mkdir()
        tasks = []
        for index, marker in ((0, b"a"), (1, b"b"), (2, b"c")):
            bullet = outputs / f"shard-{index}.bullet"
            stats = outputs / f"shard-{index}.stats.json"
            bullet.write_bytes(marker * 32)
            stats.write_text(json.dumps({
                "schema": "enyo.lc0-label-stats.v1",
                "shard_index": index,
                "read": index + 2,
                "selected": 1,
                "written": 1,
                "skipped_min_ply": index,
            }), encoding="utf-8")
            tasks.append({
                "id": f"lc0_{index:04d}",
                "index": index,
                "outputs": [str(bullet), str(stats)],
            })
        final = root / "dataset.bullet"
        manifest = {
            "schema": "crucible.task.v1",
            "tasks": tasks,
            "finalize": {
                "type": "lc0-bullet",
                "output": str(final),
                "inventory_digest": "d" * 64,
                "replace": replace,
            },
        }
        return root / "manifest.json", manifest

    def test_merges_in_numeric_order_and_aggregates_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            summary = finalize_lc0(manifest_path, manifest)
            assert summary is not None
            output = Path(str(summary["output"]))
            self.assertEqual(b"a" * 32 + b"b" * 32 + b"c" * 32, output.read_bytes())
            self.assertEqual(96, summary["merged_bytes"])
            self.assertEqual(3, summary["positions"])
            self.assertEqual(9, summary["task_totals"]["read"])
            self.assertEqual(summary, json.loads((Path(tmp) / "dataset-summary.json").read_text()))
            self.assertEqual([], list(Path(tmp).glob(".dataset.bullet.partial.*")))

    def test_refuses_existing_output_without_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            Path(manifest["finalize"]["output"]).write_bytes(b"old")
            with self.assertRaisesRegex(ValueError, "--replace"):
                finalize_lc0(manifest_path, manifest)

    def test_rejects_invalid_shard_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            Path(manifest["tasks"][1]["outputs"][0]).write_bytes(b"x")
            with self.assertRaisesRegex(ValueError, "invalid Bullet shard"):
                finalize_lc0(manifest_path, manifest)

    def test_detached_wait_finalizes_before_success_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path, manifest = self.make_run(root)
            manifest.update({
                "name": "lc0-test",
                "created_at": "2026-01-01T00:00:00Z",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "vars": {},
                "requirements": {},
            })
            write_json(manifest_path, manifest)
            for task in manifest["tasks"]:
                done = task_done_path(manifest, task)
                done.parent.mkdir(parents=True, exist_ok=True)
                write_json(done, {
                    "task": task["id"],
                    "rc": 0,
                    "outputs": [file_record(Path(path)) for path in task["outputs"]],
                })
            rc = cmd_wait(argparse.Namespace(
                manifest=str(manifest_path),
                profile=None,
                lease_seconds=60,
                interval_seconds=0.01,
                timeout_seconds=1.0,
                fail_on_fail=True,
                notify_command=None,
            ))
            self.assertEqual(0, rc)
            completion = json.loads((root / "summary.json").read_text())
            self.assertEqual("run_done", completion["event"])
            self.assertEqual(96, completion["merged_bytes"])
            self.assertEqual(3, completion["positions"])
            self.assertTrue((root / "dataset.bullet").is_file())


class Lc0ProgressTests(unittest.TestCase):
    def test_template_graphs_counters_and_reports_speedup(self) -> None:
        template_path = Path(__file__).resolve().parents[1] / "templates" / "lc0-label.template.json"
        progress = json.loads(template_path.read_text(encoding="utf-8"))["task"]["progress"]
        progress.pop("series")
        progress["log_regex"] = (
            r"read=(?P<done>\d+) selected=(?P<selected>\d+) written=(?P<output>\d+)"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            logs = root / "logs"
            state.mkdir()
            logs.mkdir()
            tasks = []
            for index, host in enumerate(("worker-a", "worker-b")):
                task_progress = copy.deepcopy(progress)
                task_progress["total"] = 200
                task_id = f"lc0_{index}"
                tasks.append({
                    "id": task_id,
                    "index": index,
                    "command": "true",
                    "outputs": [],
                    "progress": task_progress,
                })
                (state / f"{task_id}.running.json").write_text(
                    json.dumps({"task": task_id, "host": host}),
                    encoding="utf-8",
                )
                (logs / f"{task_id}.log").write_text(
                    "shard 0/2 read=100 selected=80 written=60\n"
                    "shard 0/2 read=200 selected=150 written=120\n",
                    encoding="utf-8",
                )
            manifest = {
                "schema": "crucible.task.v1",
                "name": "lc0-progress",
                "kind": "lc0-label",
                "work_dir": str(root),
                "state_dir": str(state),
                "log_dir": str(logs),
                "tasks": tasks,
            }

            summary = status_summary(manifest, lease_seconds=600)

            graph = summary["active"][0]["progress_graph"]
            series = {item["name"]: item["points"] for item in graph["series"]}
            self.assertEqual(["read", "selected", "written"], list(series))
            self.assertEqual([{"x": 100.0, "y": 80.0}, {"x": 200.0, "y": 150.0}], series["selected"])
            self.assertEqual([{"x": 100.0, "y": 60.0}, {"x": 200.0, "y": 120.0}], series["written"])
            self.assertIn("workers=2", active_worker_summary(summary))
            self.assertIn("output=240", active_worker_summary(summary))
            self.assertIn("speedup=2.0x", active_worker_summary(summary))

            for index in range(2):
                with (logs / f"lc0_{index}.log").open("a", encoding="utf-8") as file:
                    file.write("shard 0/2 read=201 selected=151 written=121\n")
            summary = status_summary(manifest, lease_seconds=600)
            run_series = {
                item["name"]: item["points"]
                for item in summary["progress_graph"]["series"]
            }
            self.assertEqual(
                [{"x": 400.0, "y": 400.0}, {"x": 402.0, "y": 402.0}],
                run_series["read"],
            )
            self.assertEqual(
                [{"x": 400.0, "y": 300.0}, {"x": 402.0, "y": 302.0}],
                run_series["selected"],
            )
            self.assertEqual(
                [{"x": 400.0, "y": 240.0}, {"x": 402.0, "y": 242.0}],
                run_series["written"],
            )

    def test_speedup_uses_current_rates_instead_of_accumulated_output(self) -> None:
        summary = {
            "metadata": {"kind": "lc0-label"},
            "active": [
                {"worker": "early", "progress_data": {"kind": "metadata", "rate": 10.0}},
                {"worker": "late", "progress_data": {"kind": "metadata", "rate": 8.0}},
            ],
            "worker_outputs": {
                "early": {"output": 1_000_000, "output_seen": True},
                "late": {"output": 10_000, "output_seen": True},
            },
            "worker_rates": {},
        }

        text = active_worker_summary(summary)

        self.assertIn("output=1m", text)
        self.assertIn("rate=18.0/s", text)
        self.assertIn("min=late:8/s", text)
        self.assertIn("max=early:10.0/s", text)
        self.assertIn("speedup=1.8x", text)


if __name__ == "__main__":
    unittest.main()
