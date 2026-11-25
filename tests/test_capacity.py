from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Deterministic worker hardware for capacity resolution; never probe real hosts.
os.environ["FORGE_HARDWARE"] = '{"threads": 64, "cores": 64, "memory_mb": 262144}'
os.environ["FORGE_CPU_GATE"] = "0"
# Never read or write the user's real ~/.config/forge (worker set saves utilization there).
import tempfile as _tempfile  # noqa: E402
os.environ["FORGE_CONFIG"] = os.path.join(_tempfile.mkdtemp(prefix="forge-test-config-"), "config.yml")

from forge_lib import capacity  # noqa: E402

LLM = {"threads": 24, "cores": 12, "perf_cores": 12, "memory_mb": 65536}
MBP = {"threads": 14, "cores": 14, "perf_cores": 10, "memory_mb": 24576}
FIXED = {"timing": "fixed", "threads": 1, "memory_mb": 0}
TIMED = {"timing": "timed", "threads": 1, "memory_mb": 0}


class UtilizationParsingTests(unittest.TestCase):
    def test_parses_percent_threads_and_max(self) -> None:
        self.assertEqual(("percent", 50.0), capacity.parse_utilization("50%"))
        self.assertEqual(("threads", 4.0), capacity.parse_utilization("4"))
        self.assertEqual(("threads", 4.0), capacity.parse_utilization(4))
        self.assertEqual(("percent", 100.0), capacity.parse_utilization("max"))
        self.assertEqual("12.5%", capacity.normalize_utilization(" 12.5 % ".replace(" %", "%")))
        self.assertTrue(capacity.utilization_is_zero("0%"))
        self.assertTrue(capacity.utilization_is_zero("0"))

    def test_rejects_invalid_values(self) -> None:
        for value in ("-1", "x", "150%", "-5%", True):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                capacity.parse_utilization(value)


class SlotResolutionTests(unittest.TestCase):
    def test_fixed_work_uses_every_hardware_thread(self) -> None:
        self.assertEqual(24, capacity.resolve_slots(LLM, "100%", FIXED)[0])
        self.assertEqual(12, capacity.resolve_slots(LLM, "50%", FIXED)[0])
        self.assertEqual(14, capacity.resolve_slots(MBP, "max", FIXED)[0])

    def test_timed_work_needs_a_physical_core_per_engine_thread(self) -> None:
        self.assertEqual(12, capacity.resolve_slots(LLM, "100%", TIMED)[0])
        self.assertEqual(6, capacity.resolve_slots(LLM, "50%", TIMED)[0])
        self.assertEqual(3, capacity.resolve_slots(LLM, "100%", {**TIMED, "threads": 4})[0])
        # A thread budget on an SMT host buys half as many timed cores.
        self.assertEqual(4, capacity.resolve_slots(LLM, "8", TIMED)[0])

    def test_timed_work_only_uses_performance_cores(self) -> None:
        self.assertEqual(10, capacity.resolve_slots(MBP, "100%", TIMED)[0])
        self.assertEqual(5, capacity.resolve_slots(MBP, "50%", TIMED)[0])
        self.assertEqual(10, capacity.resolve_slots(MBP, "14", TIMED)[0])

    def test_memory_caps_slots(self) -> None:
        heavy = {**FIXED, "memory_mb": 8192}
        slots, notes = capacity.resolve_slots(LLM, "100%", heavy)
        self.assertEqual(7, slots)  # 65536 * 0.9 // 8192
        self.assertTrue(any("memory limits" in note for note in notes))
        self.assertEqual(3, capacity.resolve_slots(LLM, "50%", heavy)[0])

    def test_any_positive_share_runs_at_least_one_task(self) -> None:
        self.assertEqual(1, capacity.resolve_slots(MBP, "1%", TIMED)[0])
        self.assertEqual(1, capacity.resolve_slots(LLM, "1", {**TIMED, "threads": 4})[0])
        self.assertEqual(0, capacity.resolve_slots(LLM, "0%", FIXED)[0])

    def test_thread_budget_above_hardware_is_capped_unless_forced(self) -> None:
        slots, notes = capacity.resolve_slots(LLM, "99", FIXED)
        self.assertEqual(24, slots)
        self.assertTrue(any("capped" in note for note in notes))
        self.assertEqual(99, capacity.resolve_slots(LLM, "99", FIXED, allow_oversubscribe=True)[0])

    def test_resolution_keeps_already_resolved_records_unless_forced(self) -> None:
        record = {"host": "a", "profile": "a", "utilization": "4", "slots": 2, "capacity": {"task": FIXED}}
        kept = capacity.resolve_worker_records([record], FIXED, quiet=True)
        self.assertEqual(2, kept[0]["slots"])
        forced = capacity.resolve_worker_records([record], FIXED, force=True, quiet=True)
        self.assertEqual(4, forced[0]["slots"])

    def test_unknown_hardware_falls_back_to_thread_budget_or_one(self) -> None:
        with mock.patch.object(capacity, "probe_worker_hardware", return_value=None):
            records = capacity.resolve_worker_records(
                [{"host": "a", "utilization": "6"}, {"host": "b"}],
                FIXED,
                quiet=True,
            )
        self.assertEqual([6, 1], [record["slots"] for record in records])


class HardwareProbeTests(unittest.TestCase):
    def test_parses_linux_smt_topology(self) -> None:
        hardware = capacity.parse_hardware_probe(
            "os=Linux\nthreads=24\nusable_threads=24\ncores=12\nmem_kb=65863368\n"
        )
        self.assertEqual((24, 12, 12, 64319), (
            hardware["threads"], hardware["cores"], hardware["perf_cores"], hardware["memory_mb"],
        ))

    def test_applies_cgroup_cpu_and_memory_limits(self) -> None:
        hardware = capacity.parse_hardware_probe(
            "os=Linux\nthreads=32\ncores=16\nmem_kb=134217728\n"
            "cgroup_cpu=800000 100000\ncgroup_mem=8589934592\n"
        )
        self.assertEqual(8, hardware["threads"])
        self.assertEqual(4, hardware["cores"])
        self.assertEqual(8192, hardware["memory_mb"])

    def test_parses_macos_performance_cores(self) -> None:
        hardware = capacity.parse_hardware_probe(
            "os=Darwin\nthreads=14\ncores=14\nperf_cores=10\nmem_bytes=25769803776\n"
        )
        self.assertEqual((14, 14, 10, 24576), (
            hardware["threads"], hardware["cores"], hardware["perf_cores"], hardware["memory_mb"],
        ))

    def test_parses_intel_hybrid_performance_cores(self) -> None:
        # Core Ultra 9 275HX: 8 P-cores + 16 E-cores, no SMT.
        hardware = capacity.parse_hardware_probe(
            "os=Linux\nthreads=24\nusable_threads=24\ncores=24\nmem_kb=31981252\nperf_cores=8\n"
        )
        self.assertEqual((24, 24, 8), (hardware["threads"], hardware["cores"], hardware["perf_cores"]))
        self.assertEqual(8, capacity.resolve_slots(hardware, "100%", TIMED)[0])
        self.assertEqual(24, capacity.resolve_slots(hardware, "100%", FIXED)[0])

    def test_missing_lscpu_assumes_no_smt(self) -> None:
        hardware = capacity.parse_hardware_probe("os=Linux\nthreads=8\nmem_kb=1048576\n")
        self.assertEqual((8, 8), (hardware["threads"], hardware["cores"]))

    def test_rejects_output_without_threads(self) -> None:
        with self.assertRaises(ValueError):
            capacity.parse_hardware_probe("os=Linux\n")

    def test_override_env_is_used_instead_of_probing(self) -> None:
        with mock.patch.dict(os.environ, {"FORGE_HARDWARE": json.dumps(MBP)}):
            hardware = capacity.probe_worker_hardware({"host": "pwa-mbp"})
        self.assertEqual(10, hardware["perf_cores"])
        self.assertEqual("pwa-mbp", hardware["host"])


class ResourceDeclarationTests(unittest.TestCase):
    def test_evaluates_expressions_from_values(self) -> None:
        resources = capacity.evaluate_resources(
            {"timing": "timed", "threads": "{threads}*{concurrency}", "memory_mb": "2*({hash}+64)*{concurrency}+64"},
            {"threads": "2", "concurrency": 3, "hash": 128},
            label="t",
        )
        self.assertEqual({"timing": "timed", "threads": 6, "memory_mb": 1216}, resources)

    def test_timed_unless_a_limit_is_set(self) -> None:
        spec = {"timing": "fixed", "timed_unless": ["nodes", "depth"], "threads": "1"}
        self.assertEqual("fixed", capacity.evaluate_resources(spec, {"nodes": 5000, "depth": None}, label="t")["timing"])
        self.assertEqual("timed", capacity.evaluate_resources(spec, {"nodes": None, "depth": ""}, label="t")["timing"])

    def test_rejects_code_and_unknown_variables(self) -> None:
        for spec in ({"threads": "__import__('os')"}, {"threads": "{missing}"}, {"timing": "fast"}):
            with self.subTest(spec=spec), self.assertRaises(SystemExit):
                capacity.evaluate_resources(spec, {}, label="t")

    def test_manifest_without_resources_costs_one_fixed_thread(self) -> None:
        self.assertEqual(capacity.default_task_resources(), capacity.manifest_task_resources({"tasks": []}))

    def test_manifest_resources_use_manifest_vars(self) -> None:
        manifest = {"vars": {"threads": "3"}, "resources": {"timing": "timed", "threads": "{threads}"}}
        self.assertEqual(3, capacity.manifest_task_resources(manifest)["threads"])

    def test_shipped_templates_declare_resources(self) -> None:
        from forge_lib.templates import load_template, template_resources

        sprt = load_template(ROOT / "templates" / "sprt.template.json")
        self.assertEqual(
            {"timing": "timed", "threads": 2, "memory_mb": 576},
            template_resources(sprt, {"threads": 1, "concurrency": 2, "hash": 64}),
        )
        label = load_template(ROOT / "templates" / "label-search.template.json")
        self.assertEqual(
            {"timing": "fixed", "threads": 1, "memory_mb": 448},
            template_resources(label, {"threads": 1, "hash": 128}),
        )


class WorkerRuntimeTests(unittest.TestCase):
    def test_memory_gate_waits_when_too_little_is_free(self) -> None:
        resources = {**FIXED, "memory_mb": 1024}
        with (
            mock.patch.object(capacity, "available_memory_mb", return_value=1200),
            mock.patch.object(capacity, "total_memory_mb", return_value=16384),
        ):
            self.assertEqual((1200, 1024 + 819), capacity.memory_gate_shortfall(resources))
        with (
            mock.patch.object(capacity, "available_memory_mb", return_value=8000),
            mock.patch.object(capacity, "total_memory_mb", return_value=16384),
        ):
            self.assertIsNone(capacity.memory_gate_shortfall(resources))
        with mock.patch.object(capacity, "available_memory_mb", return_value=None):
            self.assertIsNone(capacity.memory_gate_shortfall(resources))
        self.assertIsNone(capacity.memory_gate_shortfall(FIXED))

    def test_available_memory_is_readable_on_this_host(self) -> None:
        available = capacity.available_memory_mb()
        if available is not None:
            self.assertGreater(available, 0)

    def test_oom_score_prefers_youngest_task(self) -> None:
        self.assertEqual(1000, capacity.oom_score_adj_for_age(0))
        self.assertEqual(1000, capacity.oom_score_adj_for_age(59))
        self.assertEqual(990, capacity.oom_score_adj_for_age(60))
        self.assertEqual(500, capacity.oom_score_adj_for_age(24 * 3600))
        self.assertGreater(capacity.oom_score_adj_for_age(300), capacity.oom_score_adj_for_age(600))

    def test_fixed_tasks_run_at_low_priority(self) -> None:
        import subprocess

        for resources, expected in ((FIXED, "19"), (TIMED, str(os.getpriority(os.PRIO_PROCESS, 0)))):
            with self.subTest(timing=resources["timing"]):
                result = subprocess.run(
                    [sys.executable, "-c", "import os; print(os.getpriority(os.PRIO_PROCESS, 0))"],
                    capture_output=True,
                    text=True,
                    preexec_fn=capacity.task_preexec(resources),
                    check=True,
                )
                self.assertEqual(expected, result.stdout.strip())


if __name__ == "__main__":
    unittest.main()


class RunTaskPriorityTests(unittest.TestCase):
    def test_run_task_starts_fixed_work_at_low_priority(self) -> None:
        import tempfile

        from forge_lib.tasks import run_task, task_log_path

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = {
                "schema": "forge.task.v1",
                "name": "priority",
                "work_dir": str(root),
                "state_dir": str(root / "state"),
                "log_dir": str(root / "logs"),
                "resources": {"timing": "fixed", "threads": "1"},
                "tasks": [{
                    "id": "t0",
                    "index": 0,
                    "command": "python3 -c 'import os; print(\"nice=%d\" % os.getpriority(os.PRIO_PROCESS, 0))'",
                    "outputs": [],
                }],
            }
            task = manifest["tasks"][0]
            with mock.patch("forge_lib.tasks.check_requirements"):
                self.assertEqual(0, run_task(manifest, task, force=True))
            self.assertIn("nice=19", task_log_path(manifest, task).read_text(encoding="utf-8"))


class ProbeScriptTests(unittest.TestCase):
    def test_probe_script_succeeds_and_parses_on_this_host(self) -> None:
        import subprocess

        result = subprocess.run(["sh", "-c", capacity.HARDWARE_PROBE_SCRIPT], capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        hardware = capacity.parse_hardware_probe(result.stdout)
        self.assertGreaterEqual(hardware["threads"], hardware["cores"])
        self.assertGreater(hardware["memory_mb"], 0)


class WorkerIdLockTests(unittest.TestCase):
    def test_second_process_with_same_worker_id_is_refused(self) -> None:
        import subprocess
        import tempfile

        from forge_lib.tasks import acquire_worker_id_lock

        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            held = acquire_worker_id_lock(manifest, "run-a", "pwa-mbp#1")
            self.assertIsNotNone(held)
            # flock is per open file description, so a second process is refused...
            probe = (
                "import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]);"
                "from forge_lib.tasks import acquire_worker_id_lock as a;"
                "print(a(Path(sys.argv[2]), sys.argv[3], sys.argv[4]) is not None)"
            )
            def other(run: str, worker_id: str) -> str:
                return subprocess.run(
                    [sys.executable, "-c", probe, str(ROOT), str(manifest), run, worker_id],
                    capture_output=True, text=True, check=True,
                ).stdout.strip()
            self.assertEqual("False", other("run-a", "pwa-mbp#1"))
            # ...while other IDs and other runs are independent.
            self.assertEqual("True", other("run-a", "pwa-mbp#2"))
            self.assertEqual("True", other("run-b", "pwa-mbp#1"))
            held.close()
            self.assertEqual("True", other("run-a", "pwa-mbp#1"))


class CheckpointTransferTests(unittest.TestCase):
    def test_checkpoint_paths_follow_outputs(self) -> None:
        from forge_lib.tasks import task_checkpoint_paths

        task = {"id": "t", "outputs": ["/out/a.bullet"], "resume": {"strategy": "checkpoint"}}
        self.assertEqual(
            [Path("/out/a.bullet.partial"), Path("/out/a.bullet.partial.json")],
            task_checkpoint_paths(task),
        )
        self.assertEqual([], task_checkpoint_paths({"id": "t", "outputs": ["/out/a"]}))

    def test_failed_attempt_uploads_and_retry_downloads_checkpoint(self) -> None:
        import tempfile

        from forge_lib import tasks

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            coordinator_root, host_a, host_b = root / "coord", root / "a", root / "b"

            def manifest(base: Path) -> dict:
                return {
                    "schema": "forge.task.v1",
                    "name": "ckpt",
                    "state_dir": str(base / "state"),
                    "log_dir": str(base / "logs"),
                    "tasks": [{
                        "id": "label_0000",
                        "index": 0,
                        "command": "true",
                        "outputs": [str(base / "out" / "label_0000.bullet")],
                        "resume": {"strategy": "checkpoint"},
                    }],
                }

            coordinator, local_a, local_b = manifest(coordinator_root), manifest(host_a), manifest(host_b)
            pairs_a = tasks.task_checkpoint_pairs(local_a["tasks"][0], local_a, coordinator["tasks"][0], coordinator)
            pairs_b = tasks.task_checkpoint_pairs(local_b["tasks"][0], local_b, coordinator["tasks"][0], coordinator)

            # Host A was killed mid-shard and left a checkpoint behind.
            for local, _ in pairs_a:
                local.parent.mkdir(parents=True, exist_ok=True)
                local.write_text(local.name)
            for path in (tasks.task_fail_path(local_a, local_a["tasks"][0]), tasks.task_log_path(local_a, local_a["tasks"][0])):
                path.parent.mkdir(parents=True, exist_ok=True)
            tasks.write_json(tasks.task_fail_path(local_a, local_a["tasks"][0]), {"task": "label_0000", "rc": 137})
            tasks.task_log_path(local_a, local_a["tasks"][0]).write_text("killed\n")
            with mock.patch.object(tasks, "heartbeat_on_coordinator"):
                tasks.sync_task_result(
                    coordinator="",
                    coordinator_manifest=coordinator,
                    local_manifest=local_a,
                    coordinator_task=coordinator["tasks"][0],
                    local_task=local_a["tasks"][0],
                    worker="a",
                    claim_id="c1",
                )
            self.assertTrue(all(remote.is_file() for _, remote in pairs_a))

            # Host B picks up the retry and receives the checkpoint first.
            self.assertEqual(2, tasks.fetch_task_checkpoint("", pairs_b))
            self.assertEqual(
                ["label_0000.bullet.partial", "label_0000.bullet.partial.json"],
                [local.read_text() for local, _ in pairs_b],
            )

            # Success on B clears the coordinator copy.
            output = Path(local_b["tasks"][0]["outputs"][0])
            output.write_text("done")
            for path in (tasks.task_done_path(local_b, local_b["tasks"][0]), tasks.task_log_path(local_b, local_b["tasks"][0])):
                path.parent.mkdir(parents=True, exist_ok=True)
            tasks.write_json(tasks.task_done_path(local_b, local_b["tasks"][0]), {"task": "label_0000", "rc": 0})
            tasks.task_log_path(local_b, local_b["tasks"][0]).write_text("ok\n")
            with mock.patch.object(tasks, "heartbeat_on_coordinator"):
                tasks.sync_task_result(
                    coordinator="",
                    coordinator_manifest=coordinator,
                    local_manifest=local_b,
                    coordinator_task=coordinator["tasks"][0],
                    local_task=local_b["tasks"][0],
                    worker="b",
                    claim_id="c2",
                )
            self.assertFalse(any(remote.exists() for _, remote in pairs_b))
            self.assertFalse(any(local.exists() for local, _ in pairs_b))
            self.assertEqual("done", Path(coordinator["tasks"][0]["outputs"][0]).read_text())

    def test_explicit_checkpoint_files_resolve_like_outputs(self) -> None:
        from forge_lib.tasks import task_checkpoint_paths

        manifest = {"vars": {"output_dir": "/runs/r/outputs"}, "tasks": []}
        task = {
            "id": "sprt_0003",
            "outputs": ["{output_dir}/sprt_0003/logs/sprt_0003.log"],
            "resume": {
                "strategy": "split-progress",
                "checkpoint_files": ["{output_dir}/sprt_0003/logs/sprt_0003.resume.json"],
            },
        }
        manifest["tasks"] = [task]
        self.assertEqual(
            [Path("/runs/r/outputs/sprt_0003/logs/sprt_0003.resume.json")],
            task_checkpoint_paths(task, manifest),
        )

    def test_worker_id_locks_live_in_the_run_worker_state(self) -> None:
        import tempfile

        from forge_lib.tasks import acquire_worker_id_lock

        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            handle = acquire_worker_id_lock(manifest, "run-a", "w#1")
            self.assertIsNotNone(handle)
            handle.close()
            self.assertEqual(
                [Path(tmp) / "worker-state" / "run-a" / "locks" / "run-a--w_1.lock"],
                list((Path(tmp) / "worker-state").rglob("*.lock")),
            )


class CpuGateTests(unittest.TestCase):
    """Timed tasks wait while non-Forge work occupies the performance cores."""

    TIMED = {"timing": "timed", "threads": 1}

    def setUp(self) -> None:
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patches = mock.patch.dict(os.environ, {
            "FORGE_CPU_GATE": "1",
            "FORGE_CPU_GATE_DIR": self.tmp.name,
            "FORGE_HARDWARE": json.dumps(MBP),
        })
        patches.start()
        self.addCleanup(patches.stop)
        self.addCleanup(capacity.cpu_gate_release)

    def reserve_for(self, pid: int, threads: int = 1) -> None:
        directory = Path(self.tmp.name) / "reservations"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{pid}.json").write_text(json.dumps({"threads": threads}))

    def test_fixed_tasks_are_never_gated(self) -> None:
        with mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=14.0) as measure:
            self.assertIsNone(capacity.cpu_gate_acquire({"timing": "fixed", "threads": 1}))
        measure.assert_not_called()

    def test_idle_host_reserves_a_core(self) -> None:
        with mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=0.3):
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))
        self.assertTrue((Path(self.tmp.name) / "reservations" / f"{os.getpid()}.json").exists())
        capacity.cpu_gate_release()
        self.assertFalse((Path(self.tmp.name) / "reservations" / f"{os.getpid()}.json").exists())

    def test_waits_when_other_load_and_forge_fill_perf_cores(self) -> None:
        # A lichess engine on 2 cores plus 8 running Forge games fills 10 P-cores.
        directory = Path(self.tmp.name) / "reservations"
        directory.mkdir(parents=True, exist_ok=True)
        with mock.patch.object(capacity, "_pid_alive", return_value=True):
            for index in range(8):
                (directory / f"{900000 + index}.json").write_text(json.dumps({"threads": 1}))
            with mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=2.0):
                busy = capacity.cpu_gate_acquire(self.TIMED)
            self.assertEqual({"foreign": 2.0, "reserved": 8, "needed": 1, "capacity": 10}, busy)
            (directory / "900007.json").unlink()
            # Seven Forge games + 2 cores of other load leave one P-core.
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))

    def test_peak_load_in_window_counts(self) -> None:
        # The engine idles on the opponent's move; the earlier busy sample counts.
        history = Path(self.tmp.name) / "foreign.json"
        Path(self.tmp.name).mkdir(parents=True, exist_ok=True)
        import time

        history.write_text(json.dumps({"samples": [[time.time() - 30, 10.0], [time.time() - 10, 0.1]]}))
        with mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=0.0):
            busy = capacity.cpu_gate_acquire(self.TIMED)
        self.assertIsNotNone(busy)
        self.assertEqual(10.0, busy["foreign"])

    def test_dead_worker_reservations_are_dropped(self) -> None:
        self.reserve_for(999999, 10)
        with mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=0.0), \
                mock.patch.object(capacity, "_pid_alive", return_value=False):
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))
        self.assertFalse((Path(self.tmp.name) / "reservations" / "999999.json").exists())

    def test_disabled_gate_allows(self) -> None:
        with mock.patch.dict(os.environ, {"FORGE_CPU_GATE": "0"}), \
                mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=14.0):
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))

    def test_forge_process_trees_are_excluded(self) -> None:
        snapshot = {
            10: (1, 5.0, "/usr/bin/python3 /home/u/.cache/forge/run/forge.py work --manifest m.json"),
            11: (10, 5.0, "bash -c sprt ..."),
            12: (11, 5.0, "fastchess -engine ..."),
            20: (1, 5.0, "python3 lichess-bot.py"),
            21: (20, 5.0, "enyo"),
            30: (1, 5.0, "python3 /x/forge.py web --port 9000"),
        }
        self.assertEqual({10, 11, 12}, capacity.forge_process_ids(snapshot))

    def test_ps_time_parsing(self) -> None:
        self.assertAlmostEqual(1.5, capacity._parse_ps_time("0:01.50"))
        self.assertAlmostEqual(3723.0, capacity._parse_ps_time("01:02:03"))
        self.assertAlmostEqual(90061.0, capacity._parse_ps_time("1-01:01:01"))

    def test_real_snapshot_measures(self) -> None:
        self.assertGreater(len(capacity.process_cpu_snapshot()), 1)
        self.assertGreaterEqual(capacity.measure_foreign_cpu_cores(0.2), 0.0)


class CpuReserveTests(CpuGateTests):
    """A declared program holds its cores while it runs, even when idle."""

    SNAPSHOT = {
        500: (1, 0.0, "python3 lichess-bot.py -v"),
        600: (1, 0.0, "/usr/bin/zsh"),
    }

    def test_idle_bot_still_holds_its_cores(self) -> None:
        directory = Path(self.tmp.name) / "reservations"
        directory.mkdir(parents=True, exist_ok=True)
        for index in range(6):
            (directory / f"{900000 + index}.json").write_text(json.dumps({"threads": 1}))
        with mock.patch.dict(os.environ, {"FORGE_CPU_RESERVE": json.dumps({"lichess-bot": 4})}), \
                mock.patch.object(capacity, "_pid_alive", return_value=True), \
                mock.patch.object(capacity, "process_cpu_snapshot", return_value=self.SNAPSHOT), \
                mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=0.0):
            # 6 Forge games + 4 held cores = 10 P-cores: no room.
            busy = capacity.cpu_gate_acquire(self.TIMED)
            self.assertEqual(4.0, busy["foreign"])
            (directory / "900005.json").unlink()
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))

    def test_cores_are_free_when_the_program_is_not_running(self) -> None:
        with mock.patch.dict(os.environ, {"FORGE_CPU_RESERVE": json.dumps({"lichess-bot": 10})}), \
                mock.patch.object(capacity, "process_cpu_snapshot", return_value={600: (1, 0.0, "/usr/bin/zsh")}), \
                mock.patch.object(capacity, "measure_foreign_cpu_cores", return_value=0.0):
            self.assertIsNone(capacity.cpu_gate_acquire(self.TIMED))

    def test_reserve_config_is_validated_and_exported(self) -> None:
        from forge_lib import workers

        record = workers.parse_worker_record({"host": "pwa-llm", "reserve": {"lichess-bot": 4}})
        self.assertEqual({"lichess-bot": 4}, record["reserve"])
        for bad in ({"lichess-bot": -1}, {"": 2}, ["lichess-bot"], {"x": True}):
            with self.assertRaises(SystemExit):
                workers.parse_worker_record({"host": "pwa-llm", "reserve": bad})
        with mock.patch.object(workers, "run_process_command_with_retries") as run:
            workers.run_worker_shell({"host": "pwa-llm", "reserve": {"lichess-bot": 4}}, "true")
        script = " ".join(run.call_args.args[0])
        self.assertIn("FORGE_CPU_RESERVE=", script)
        self.assertIn("lichess-bot", script)
