from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import forge


ROOT = Path(__file__).resolve().parents[1]


def parameter(name: str = "alpha") -> dict:
    return {
        "name": name,
        "default": 5,
        "min": 0.0,
        "max": 10.0,
        "c_end": 1.0,
        "r_end": 0.03,
    }


class SpsaCoordinatorTests(unittest.TestCase):
    def make_manifest(
        self,
        root: Path,
        *,
        iterations: int = 1,
        start_iteration: int = 0,
    ) -> Path:
        manifest_path = root / "manifest.json"
        forge.write_json(manifest_path, {
            "schema": "forge.task.v1",
            "name": "spsa-test",
            "kind": "spsa",
            "created_at": forge.now(),
            "work_dir": str(root),
            "vars": {},
            "requirements": {},
            "tasks": [
                {"id": "spsa_0000", "index": 0, "command": "true", "outputs": []},
                {"id": "spsa_0001", "index": 1, "command": "true", "outputs": []},
            ],
            "spsa": {
                "schema": forge.CONFIG_SCHEMA,
                "iterations": iterations,
                "start_iteration": start_iteration,
                "target_iteration": start_iteration + iterations,
                "rounds": 2,
                "shards": 2,
                "shard_games": [2, 2],
                "seed": 7,
                "state": str(root / "exported-state.json"),
                "parameters": [parameter()],
                "parameter_source": {"path": "params.txt", "sha256": "0" * 64},
                "initial_state": {
                    "k": start_iteration,
                    "names": ["alpha"],
                    "theta": [5.0],
                },
            },
        })
        return manifest_path

    def test_barrier_aggregates_results_and_exports_final_theta(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-") as tmp:
            root = Path(tmp)
            manifest_path = self.make_manifest(root)

            first = forge.spsa_next_payload(manifest_path, 0)
            second = forge.spsa_next_payload(manifest_path, 1)
            self.assertEqual("work", first["status"])
            self.assertEqual(first["iteration"], second["iteration"])
            self.assertEqual(first["plus"], second["plus"])
            accepted = forge.spsa_submit_payload(
                manifest_path, 0, 1, wins=2, losses=0, draws=0, games=2
            )
            self.assertEqual("accepted", accepted["status"])
            duplicate = forge.spsa_submit_payload(
                manifest_path, 0, 1, wins=2, losses=0, draws=0, games=2
            )
            self.assertEqual("duplicate", duplicate["status"])
            self.assertEqual("wait", forge.spsa_next_payload(manifest_path, 0)["status"])

            advanced = forge.spsa_submit_payload(
                manifest_path, 1, 1, wins=2, losses=0, draws=0, games=2
            )
            self.assertEqual("advanced", advanced["status"])
            self.assertEqual(4, advanced["games"])
            self.assertEqual(1.0, advanced["result"])
            done = forge.spsa_next_payload(manifest_path, 0)
            self.assertEqual("done", done["status"])
            self.assertEqual(1, done["k"])
            self.assertNotEqual([5.0], done["theta"])

            exported = json.loads((root / "exported-state.json").read_text())
            self.assertEqual(1, exported["k"])
            self.assertEqual(["alpha"], exported["names"])
            self.assertTrue((root / "spsa.csv").is_file())

    def test_rejects_wrong_shard_game_count(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-") as tmp:
            manifest_path = self.make_manifest(Path(tmp))
            forge.spsa_next_payload(manifest_path, 0)
            with self.assertRaisesRegex(SystemExit, "expected 2 valid games"):
                forge.spsa_submit_payload(
                    manifest_path, 0, 1, wins=1, losses=0, draws=0, games=1
                )

    def test_iterations_are_added_to_loaded_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-") as tmp:
            manifest_path = self.make_manifest(
                Path(tmp), iterations=2, start_iteration=1500
            )

            first = forge.spsa_next_payload(manifest_path, 0)

            self.assertEqual(1501, first["iteration"])
            self.assertEqual(1, first["batch_iteration"])
            self.assertEqual(2, first["iterations"])

    def test_iteration_timing_uses_exponential_average(self) -> None:
        state: dict = {}
        with mock.patch("forge_lib.spsa.time.time", return_value=116.0):
            forge.update_iteration_timing(state, {"created_epoch": 100.0})
        with mock.patch("forge_lib.spsa.time.time", return_value=220.0):
            forge.update_iteration_timing(state, {"created_epoch": 200.0})

        self.assertAlmostEqual(16.8, state["iteration_seconds_ema"])
        self.assertEqual(2, state["iteration_samples"])

    def test_status_uses_coordinator_progress_and_eta(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-status-") as tmp:
            root = Path(tmp)
            manifest_path = root / "manifest.json"
            forge.write_json(root / "spsa-state.json", {
                "run": "spsa-status",
                "k": 2340,
                "start_iteration": 2331,
                "target_iteration": 2500,
                "iterations": 169,
                "iteration_seconds_ema": 16.5,
            })
            summary = {
                "run": "spsa-status",
                "manifest": str(manifest_path),
                "metadata": {"kind": "spsa"},
                "manifest_payload": {
                    "name": "spsa-status",
                    "kind": "spsa",
                    "spsa": {
                        "iterations": 169,
                        "start_iteration": 2331,
                        "target_iteration": 2500,
                    },
                },
                "active": [],
            }

            self.assertEqual(
                "progress=iterations=9/169, eta=44m00s",
                forge.summary_progress(summary),
            )
            self.assertEqual(
                ["iterations=9/169", "eta=44m00s"],
                forge.run_progress_fields(summary),
            )
            self.assertIsNone(forge.run_speedup_value(summary))

    def test_delegated_status_preserves_coordinator_spsa_eta(self) -> None:
        summary = {
            "metadata": {"kind": "spsa"},
            "progress": "progress=iterations=41/169, eta=52m47s",
            "active": [],
        }

        self.assertEqual(
            "progress=iterations=41/169, eta=52m47s",
            forge.summary_progress(summary),
        )


class SpsaTemplateTests(unittest.TestCase):
    def test_spsa_shared_template_requires_installation_paths(self) -> None:
        template = json.loads(
            (ROOT / "templates" / "spsa.template.json").read_text(encoding="utf-8")
        )

        self.assertEqual(1000, template["parameters"]["iterations"]["default"])
        for name in ("engine", "params", "state", "settings", "book"):
            self.assertTrue(template["parameters"][name]["required"])
            self.assertTrue(template["parameters"][name]["local_default"])
            self.assertNotIn("default", template["parameters"][name])
        self.assertNotIn("default", template["parameters"]["uci"])

    def test_uci_defaults_are_read_from_enyo_settings(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-settings-") as tmp:
            settings = Path(tmp) / "settings.json"
            settings.write_text(json.dumps({
                "uci_options": {
                    "nnue_file": "/tmp/current.nn",
                    "use_syzygy": False,
                },
            }))

            self.assertEqual([
                "nnue_file=/tmp/current.nn",
                "use_syzygy=false",
            ], forge.spsa_uci_defaults(settings))

    def test_explicit_uci_options_override_defaults(self) -> None:
        self.assertEqual([
            "nnue_file=/tmp/custom.net",
            "use_syzygy=true",
        ], forge.normalize_common_uci([
            "nnue_file=~/code/cpp/chess/enyo/net/default.net",
            "use_syzygy=false",
            "nnue_file=/tmp/custom.net",
            "use_syzygy=true",
        ]))

    def test_worker_slots_are_capped_and_spread_across_hosts(self) -> None:
        payload = {
            "schema": "forge.workers.v1",
            "workers": [
                {"host": "a", "slots": 11},
                {"host": "b", "slots": 5},
                {"host": "c", "slots": 5},
                {"host": "d", "slots": 16},
                {"host": "e", "slots": 4},
                {"host": "f", "slots": 8},
            ],
        }

        capped = forge.cap_spsa_worker_slots(payload, 14)

        self.assertEqual(14, sum(worker["slots"] for worker in capped["workers"]))
        self.assertEqual([3, 3, 2, 2, 2, 2], [
            worker["slots"] for worker in capped["workers"]
        ])

    def test_template_creates_long_lived_exact_game_shards(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-template-") as tmp:
            root = Path(tmp)
            params_path = root / "params.txt"
            state_path = root / "state.json"
            settings_path = root / "settings.json"
            params_path.write_text("alpha, 5, 0, 10, 1, 0.03\n")
            state_path.write_text(json.dumps({
                "k": 7,
                "names": ["alpha"],
                "theta": [5.0],
            }))
            settings_path.write_text(json.dumps({
                "uci_options": {
                    "nnue_file": "/tmp/current.nn",
                    "use_syzygy": False,
                },
            }))
            template_path = ROOT / "templates" / "spsa.template.json"
            template = forge.load_template(template_path)
            params = forge.parse_template_parameters("spsa", template, [
                "--engine", str(root / "engine"),
                "--params", str(params_path),
                "--state", str(state_path),
                "--settings", str(settings_path),
                "--book", str(root / "book.epd"),
                "--iterations", "3",
                "--rounds", "3",
                "--shards", "2",
            ])
            params = forge.prepare_spsa_template_parameters(params)
            manifest = forge.expand_template("spsa", template_path, template, params)

            self.assertEqual("spsa", manifest["kind"])
            self.assertEqual(2, len(manifest["tasks"]))
            self.assertEqual([4, 2], manifest["spsa"]["shard_games"])
            self.assertEqual(3, manifest["spsa"]["iterations"])
            self.assertEqual(7, manifest["spsa"]["start_iteration"])
            self.assertEqual(10, manifest["spsa"]["target_iteration"])
            self.assertIn("spsa-worker", manifest["tasks"][0]["command"])
            self.assertIn("$FORGE_COORDINATOR", manifest["tasks"][0]["command"])
            self.assertEqual("3", manifest["tasks"][0]["progress"]["total"])

    def test_cli_alias_selects_spsa_template(self) -> None:
        self.assertEqual(
            ["run", "spsa", "--rounds", "14"],
            forge.expand_template_command_aliases(["spsa", "--rounds", "14"]),
        )

    def test_full_local_run_advances_two_distributed_iterations(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-run-") as tmp:
            root = Path(tmp)
            params = root / "params.txt"
            state = root / "state.json"
            book = root / "book.epd"
            engine = root / "engine"
            fastchess = root / "fastchess"
            workers = root / "workers.json"
            config = root / "config.yml"
            settings = root / "settings.json"
            network = root / "current.nn"
            template_path = root / "spsa.template.json"
            params.write_text("alpha, 5, 0, 10, 1, 0.03\n")
            state.write_text(json.dumps({
                "k": 5,
                "names": ["alpha"],
                "theta": [5.0],
            }))
            network.write_text("network\n")
            settings.write_text(json.dumps({
                "uci_options": {
                    "nnue_file": str(network),
                    "use_syzygy": False,
                },
            }))
            book.write_text("startpos\n")
            engine.write_text("#!/bin/sh\nexit 0\n")
            fastchess.write_text(
                "#!/bin/sh\n"
                "echo 'Games: 2, Wins: 2, Losses: 0, Draws: 0'\n"
            )
            engine.chmod(engine.stat().st_mode | stat.S_IXUSR)
            fastchess.chmod(fastchess.stat().st_mode | stat.S_IXUSR)
            config.write_text("")
            forge.write_json(workers, {
                "schema": "forge.workers.v1",
                "workers": [{
                    "host": "localhost",
                    "profile": "localhost",
                    "local": True,
                    "slots": 2,
                    "cache_dir": str(root / "worker-cache"),
                    "python": sys.executable,
                }],
            })
            template = json.loads(
                (ROOT / "templates" / "spsa.template.json").read_text()
            )
            template["requirements"] = {
                "python_min": "3.11",
                "commands": [
                    {"name": "fastchess", "path": "{fastchess}"},
                    {"name": "engine", "path": "{engine}"},
                ],
                "inputs": [{"name": "book", "path": "{book}"}],
            }
            forge.write_json(template_path, template)
            environment = os.environ.copy()
            environment.update({
                "FORGE_CONFIG": str(config),
                "FORGE_COORDINATOR_RUNS": "",
                "FORGE_NOTIFY_COMMAND": "",
                "FORGE_RUNS_DIR": str(root / "runs"),
            })

            result = subprocess.run([
                sys.executable,
                str(ROOT / "forge.py"),
                "run", str(template_path),
                "--workers", str(workers),
                "--run", "spsa-integration",
                "--wait",
                "--interval-seconds", "0.05",
                "--remote-timeout-seconds", "20",
                "--engine", str(engine),
                "--fastchess", str(fastchess),
                "--params", str(params),
                "--state", str(state),
                "--settings", str(settings),
                "--book", str(book),
                "--iterations", "2",
                "--rounds", "2",
                "--shards", "2",
                "--poll-seconds", "0.01",
            ], env=environment, text=True, capture_output=True, timeout=30)

            run = root / "runs" / "spsa-integration"
            logs = "\n".join(
                path.read_text(errors="replace")
                for path in sorted((run / "logs").glob("*"))
            )
            self.assertEqual(
                0,
                result.returncode,
                result.stdout + result.stderr + "\n" + logs,
            )
            final_state = json.loads(state.read_text())
            self.assertEqual(7, final_state["k"])
            self.assertEqual(5, final_state["start_iteration"])
            self.assertEqual(7, final_state["target_iteration"])
            self.assertEqual(["alpha"], final_state["names"])
            self.assertTrue((run / "summary.json").is_file())
            self.assertEqual(3, len((run / "spsa.csv").read_text().splitlines()))
            self.assertIn("SPSA iteration 1/2 total=6", logs)
            self.assertIn("SPSA iteration 2/2 total=7", logs)

    def test_spsa_cancel_is_retryable_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-cancel-") as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "spsa-cancel",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "tasks": [{
                    "id": "spsa_0000",
                    "index": 0,
                    "command": "sleep 60",
                    "outputs": [],
                    "cancel_rc": 130,
                }],
            }
            task = manifest["tasks"][0]
            cancel = threading.Event()
            timer = threading.Timer(0.1, cancel.set)
            timer.start()
            try:
                rc = forge.run_task(manifest, task, force=False, cancel_event=cancel)
            finally:
                timer.cancel()

            self.assertEqual(130, rc)
            self.assertTrue(forge.task_fail_path(manifest, task).exists())
            self.assertFalse(forge.task_done_path(manifest, task).exists())


class SpsaWorkerTests(unittest.TestCase):
    def test_fastchess_match_requires_and_returns_exact_score(self) -> None:
        with tempfile.TemporaryDirectory(prefix="forge-spsa-match-") as tmp:
            root = Path(tmp)
            fastchess = root / "fastchess"
            fastchess.write_text(
                "#!/bin/sh\n"
                "echo 'Games: 4, Wins: 2, Losses: 1, Draws: 1'\n"
            )
            fastchess.chmod(fastchess.stat().st_mode | stat.S_IXUSR)
            args = argparse.Namespace(
                games=4,
                engine=str(root / "engine"),
                book=str(root / "book.epd"),
                fastchess=str(fastchess),
                uci=["nnue_file=~/network.nn"],
                tc="1+0.01",
                restart="on",
                threads=1,
                hash=16,
                concurrency=1,
                match_timeout=10,
            )

            result = forge.run_spsa_match(args, {
                "plus": {"alpha": 6},
                "minus": {"alpha": 4},
                "seed": 9,
            })

            self.assertEqual({"games": 4, "wins": 2, "losses": 1, "draws": 1}, result)


if __name__ == "__main__":
    unittest.main()
