from __future__ import annotations

import json
import argparse
import copy
import gzip
import io
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from forge_lib.labeling import (
    INVENTORY_SCHEMA,
    build_inventory,
    canonical_inventory_bytes,
    finalize_labeling,
    inventory_digest,
    marker_matches,
    unpack_lc0_archives,
    verify_inventory_tree,
)
from forge_lib.core import file_record, write_json
from forge_lib.status import cmd_wait
from forge_lib.status import active_worker_summary, run_speedup_field, status_summary
from forge_lib.tasks import task_done_path


class Lc0InventoryTests(unittest.TestCase):
    def test_unpacks_tar_training_members_for_balanced_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            cache = root / "cache"
            source.mkdir()
            archive = source / "training.2-test91.tar"
            with tarfile.open(archive, "w") as handle:
                for name, size in (("training.small.gz", 8), ("training.large.gz", 32)):
                    payload = gzip.compress(b"x" * size)
                    member = tarfile.TarInfo(f"batch/{name}")
                    member.size = len(payload)
                    handle.addfile(member, io.BytesIO(payload))

            unpacked = unpack_lc0_archives(source, cache_home=cache)

            self.assertNotEqual(source, unpacked)
            self.assertTrue((unpacked / "training.2-test91" / "batch" / "training.small.gz").is_file())
            self.assertTrue((unpacked / "training.2-test91" / "batch" / "training.large.gz").is_file())
            self.assertEqual(unpacked, unpack_lc0_archives(source, cache_home=cache))

    def test_template_uses_source_neutral_labeling_name(self) -> None:
        template_path = Path(__file__).resolve().parents[1] / "templates" / "label-search.template.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))

        self.assertEqual("label-search", template["name"])
        self.assertEqual("label", template["kind"])
        self.assertTrue(template["run_name"].startswith("label-"))
        self.assertEqual("label:{shard}", template["task"]["label"])
        self.assertIn("tools/score/label.py", template["task"]["command"])
        self.assertEqual("bullet-labels", template["finalize"]["type"])

    def test_template_cache_mapping_is_os_specific(self) -> None:
        template_path = Path(__file__).resolve().parents[1] / "templates" / "label-search.template.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))

        self.assertEqual(
            "~/Library/Caches/forge",
            template["path_maps"]["macos"]["~/.cache/forge"],
        )
        self.assertEqual({"macos"}, set(template["path_maps"]))

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
            with mock.patch("forge_lib.labeling.sha256_file", side_effect=AssertionError("rehash")):
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


class LabelingFinalizationTests(unittest.TestCase):
    def make_run(self, root: Path, *, replace: bool = False) -> tuple[Path, dict]:
        outputs = root / "outputs"
        outputs.mkdir()
        tasks = []
        for index, marker in ((0, b"a"), (1, b"b"), (2, b"c")):
            bullet = outputs / f"shard-{index}.bullet"
            stats = outputs / f"shard-{index}.stats.json"
            bullet.write_bytes(marker * 32)
            stats.write_text(json.dumps({
                "schema": "enyo.label-stats.v1",
                "shard_index": index,
                "read": index + 2,
                "selected": 1,
                "written": 1,
                "skipped_min_ply": index,
            }), encoding="utf-8")
            tasks.append({
                "id": f"label_{index:04d}",
                "index": index,
                "outputs": [str(bullet), str(stats)],
            })
        final = root / "dataset.bullet"
        manifest = {
            "schema": "forge.task.v1",
            "tasks": tasks,
            "finalize": {
                "type": "bullet-labels",
                "output": str(final),
                "inventory_digest": "d" * 64,
                "replace": replace,
            },
        }
        return root / "manifest.json", manifest

    def test_merges_in_numeric_order_and_aggregates_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            summary = finalize_labeling(manifest_path, manifest)
            assert summary is not None
            output = Path(str(summary["output"]))
            self.assertEqual(b"a" * 32 + b"b" * 32 + b"c" * 32, output.read_bytes())
            self.assertEqual(96, summary["merged_bytes"])
            self.assertEqual("forge.label-summary.v1", summary["schema"])
            self.assertEqual(3, summary["positions"])
            self.assertEqual(9, summary["task_totals"]["read"])
            self.assertEqual(summary, json.loads((Path(tmp) / "dataset-summary.json").read_text()))
            self.assertEqual([], list(Path(tmp).glob(".dataset.bullet.partial.*")))

    def test_task_scoped_inputs_use_local_shard_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            for task in manifest["tasks"]:
                task["inputs"] = [{"tree": "lc0-inventory", "path": f"/input/{task['index']}"}]
                stats_path = Path(task["outputs"][1])
                stats = json.loads(stats_path.read_text(encoding="utf-8"))
                stats["shard_index"] = 0
                stats["shard_count"] = 1
                stats_path.write_text(json.dumps(stats), encoding="utf-8")

            summary = finalize_labeling(manifest_path, manifest)

            assert summary is not None
            self.assertEqual(3, summary["shard_count"])

    def test_merges_adaptive_ranges_in_source_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            tasks = manifest["tasks"]
            tasks[0]["index"] = 20
            tasks[0]["adaptive_range"] = {"unit": "pgns", "start": 0, "count": 2}
            tasks[1]["index"] = 7
            tasks[1]["adaptive_range"] = {"unit": "pgns", "start": 2, "count": 3}
            tasks[2]["index"] = 99
            tasks[2]["adaptive_range"] = {"unit": "pgns", "start": 5, "count": 1}
            for task in tasks:
                stats_path = Path(task["outputs"][1])
                stats = json.loads(stats_path.read_text(encoding="utf-8"))
                stats["shard_index"] = 0
                stats_path.write_text(json.dumps(stats), encoding="utf-8")

            summary = finalize_labeling(manifest_path, manifest)

            assert summary is not None
            self.assertEqual(b"a" * 32 + b"b" * 32 + b"c" * 32, Path(summary["output"]).read_bytes())
            self.assertEqual(
                [(0, 2), (2, 3), (5, 1)],
                [(shard["start"], shard["count"]) for shard in summary["shards"]],
            )

    def test_refuses_existing_output_without_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            Path(manifest["finalize"]["output"]).write_bytes(b"old")
            with self.assertRaisesRegex(ValueError, "--replace"):
                finalize_labeling(manifest_path, manifest)

    def test_rejects_invalid_shard_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            Path(manifest["tasks"][1]["outputs"][0]).write_bytes(b"x")
            with self.assertRaisesRegex(ValueError, "invalid Bullet shard"):
                finalize_labeling(manifest_path, manifest)

    def test_accepts_empty_shard_when_stats_report_zero_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, manifest = self.make_run(Path(tmp))
            task = manifest["tasks"][1]
            Path(task["outputs"][0]).write_bytes(b"")
            stats_path = Path(task["outputs"][1])
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
            stats["written"] = 0
            stats_path.write_text(json.dumps(stats), encoding="utf-8")

            summary = finalize_labeling(manifest_path, manifest)

            assert summary is not None
            self.assertEqual(64, summary["merged_bytes"])
            self.assertEqual(2, summary["positions"])

    def test_detached_wait_finalizes_before_success_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest_path, manifest = self.make_run(root)
            manifest.update({
                "name": "label-test",
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


class LabelingProgressTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch("forge_lib.status.speedup_single_host_workers", return_value=5)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_template_graphs_counters_and_reports_speedup(self) -> None:
        template_path = Path(__file__).resolve().parents[1] / "templates" / "label-search.template.json"
        progress = json.loads(template_path.read_text(encoding="utf-8"))["task"]["progress"]
        progress.pop("series")
        progress["log_regex"] = (
            r"read=(?P<done>\d+) selected=(?P<selected>\d+) written=(?P<output>\d+) "
            r"rate=(?P<rate>[0-9.]+)/s"
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
                task_id = f"label_{index}"
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
                    f"shard 0/2 read=100 selected=80 written=60 rate={(index + 1) * 8}/s\n"
                    f"shard 0/2 read=200 selected=150 written=120 rate={(index + 1) * 10}/s\n",
                    encoding="utf-8",
                )
            manifest = {
                "schema": "forge.task.v1",
                "name": "label-progress",
                "kind": "label",
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
            self.assertNotIn("speedup=", active_worker_summary(summary))
            self.assertEqual("speedup=1.5x", run_speedup_field(summary))

            for index in range(2):
                with (logs / f"label_{index}.log").open("a", encoding="utf-8") as file:
                    file.write(
                        f"shard 0/2 read=201 selected=151 written=121 rate={(index + 1) * 12}/s\n"
                    )
            summary = status_summary(manifest, lease_seconds=600)
            run_series = {
                item["name"]: item["points"]
                for item in summary["progress_graph"]["series"]
            }
            self.assertEqual(
                [{"x": 400.0, "y": 30.0}, {"x": 402.0, "y": 36.0}],
                run_series["total"],
            )
            self.assertEqual(
                [{"x": 400.0, "y": 10.0}, {"x": 402.0, "y": 12.0}],
                run_series["worker-a"],
            )
            self.assertEqual(
                [{"x": 400.0, "y": 20.0}, {"x": 402.0, "y": 24.0}],
                run_series["worker-b"],
            )

    def test_speedup_uses_current_rates_instead_of_accumulated_output(self) -> None:
        summary = {
            "metadata": {"kind": "label"},
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
        self.assertNotIn("speedup=", text)
        self.assertEqual("speedup=1.8x", run_speedup_field(summary))


if __name__ == "__main__":
    unittest.main()
