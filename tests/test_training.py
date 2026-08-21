from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from forge_lib.core import write_json
from forge_lib.manifest import input_requirement_items
from forge_lib.tasks import claim_task
from forge_lib.training import (
    attach_distributed_train_inputs,
    distributed_train_workers_payload,
    finalize_distributed_train,
)
from forge_lib.workers import parse_worker_record, run_worker_shell, worker_resources


class DistributedTrainingTests(unittest.TestCase):
    def test_worker_resources_default_to_cpu_and_train_requires_gpu(self) -> None:
        self.assertEqual({"cpu"}, worker_resources(parse_worker_record({"host": "cpu"})))
        with self.assertRaisesRegex(SystemExit, "requires gpu resource"):
            distributed_train_workers_payload({
                "schema": "forge.workers.v1",
                "workers": [{"host": "cpu-a"}, {"host": "gpu-b", "resources": ["gpu"]}],
            })

    def test_worker_environment_is_exported_for_worker_commands(self) -> None:
        worker = parse_worker_record({
            "host": "localhost",
            "local": True,
            "env": {"CUDA_HOME": "/usr/lib/cuda", "CUDA_PATH": "/usr/lib/cuda"},
        })
        self.assertEqual("/usr/lib/cuda", worker["env"]["CUDA_PATH"])
        result = run_worker_shell(
            worker,
            'test "$CUDA_PATH" = /usr/lib/cuda && test "$CUDA_HOME" = /usr/lib/cuda',
        )
        self.assertEqual(0, result.returncode)
        with self.assertRaisesRegex(SystemExit, "env variable names"):
            parse_worker_record({"host": "localhost", "env": {"CUDA-PATH": "/usr/lib/cuda"}})

    def test_stages_one_shard_per_host_and_pins_each_rank(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shards = root / "shards"
            shards.mkdir()
            workers = {
                "schema": "forge.workers.v1",
                "workers": [
                    {"host": "pwa-llm", "resources": ["cpu", "gpu"]},
                    {"host": "pwa-5090", "resources": ["gpu"]},
                    {"host": "pwa-hak", "resources": ["cpu", "gpu"]},
                ],
            }
            for host in ("pwa-llm", "pwa-5090", "pwa-hak"):
                (shards / f"{host}.bullet").write_bytes(host.encode())
            manifest = {
                "name": "train-demo",
                "requirements": {"inputs": []},
                "template": {"parameters": {"shard_dir": str(shards)}},
                "tasks": [
                    {"id": f"train_{index:04d}", "outputs": [str(root / f"{host}.sha256")], "command": "train {train_shard}"}
                    for index, host in enumerate(("pwa-llm", "pwa-5090", "pwa-hak"))
                ],
            }
            payload = distributed_train_workers_payload(workers)
            attach_distributed_train_inputs(manifest, payload)

            self.assertEqual(
                ["pwa-llm", "pwa-5090", "pwa-hak"],
                [task["claim_worker"] for task in manifest["tasks"]],
            )
            self.assertTrue(all(task["strict_claim_worker"] for task in manifest["tasks"]))
            self.assertEqual(3, len(manifest["requirements"]["inputs"]))
            for task, item in zip(manifest["tasks"], manifest["requirements"]["inputs"]):
                host = task["claim_worker"]
                self.assertEqual([host], item["hosts"])
                self.assertIn(f"/{host}.bullet", task["command"])

            manifest["_active_worker_host"] = "pwa-5090"
            self.assertEqual(
                ["training shard pwa-5090"],
                [item["name"] for item in input_requirement_items(manifest)],
            )

    def test_finalizer_requires_matching_quantised_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest = "a" * 64
            manifest = {
                "name": "train-demo",
                "finalize": {"type": "distributed-train"},
                "tasks": [
                    {
                        "id": "train_0000",
                        "claim_worker": "pwa-llm",
                        "strict_claim_worker": True,
                        "outputs": [str(root / "llm.sha256")],
                    },
                    {
                        "id": "train_0001",
                        "claim_worker": "pwa-5090",
                        "strict_claim_worker": True,
                        "outputs": [str(root / "gpu.sha256")],
                    },
                ],
            }
            for task in manifest["tasks"]:
                Path(task["outputs"][0]).write_text(digest + "\n", encoding="utf-8")
            manifest_path = root / "manifest.json"
            write_json(manifest_path, manifest)
            summary = finalize_distributed_train(manifest_path, manifest)
            self.assertIsNotNone(summary)
            assert summary is not None
            self.assertEqual(digest, summary["quantised_sha256"])
            self.assertTrue((root / "training-summary.json").is_file())

            Path(manifest["tasks"][1]["outputs"][0]).write_text("b" * 64 + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hashes differ"):
                finalize_distributed_train(manifest_path, manifest)

    def test_strict_rank_cannot_be_claimed_by_another_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "train-demo",
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "claim_dir": str(root / "claims"),
                "tasks": [{
                    "id": "train_0000",
                    "index": 0,
                    "command": "true",
                    "outputs": [str(root / "digest.sha256")],
                    "claim_worker": "pwa-llm",
                    "strict_claim_worker": True,
                }],
            }
            self.assertIsNone(claim_task(
                manifest, worker="pwa-5090", profile=None,
                lease_seconds=60, retry_failed=False,
            ))
            self.assertIsNotNone(claim_task(
                manifest, worker="pwa-llm", profile=None,
                lease_seconds=60, retry_failed=False,
            ))
