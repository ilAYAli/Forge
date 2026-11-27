from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ["FORGE_CONFIG"] = os.path.join(tempfile.mkdtemp(prefix="forge-test-config-"), "config.yml")

from forge_lib import cloud, config, workers  # noqa: E402

C7A = {
    "c7a.large": {"vcpus": 2, "cores": 2},
    "c7a.xlarge": {"vcpus": 4, "cores": 4},
    "c7a.4xlarge": {"vcpus": 16, "cores": 16},
    "c7a.16xlarge": {"vcpus": 64, "cores": 64},
    "c7a.48xlarge": {"vcpus": 192, "cores": 192},
}
POOLS = {"aws": cloud.Pool(name="aws")}


class CloudConfigTests(unittest.TestCase):
    def write_workers(self, entries: list[object]) -> Path:
        path = config.config_workers_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema": "forge.workers.v1", "workers": entries}), encoding="utf-8")
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return path

    def test_no_cloud_entry_means_no_pools(self) -> None:
        self.write_workers(["pwa-llm", {"host": "pwa-5090", "profile": "linux"}])
        self.assertEqual(cloud.cloud_pools(), {})

    def test_reads_the_pool_from_workers_json(self) -> None:
        self.write_workers(["pwa-llm", {
            "host": "aws",
            "cloud": {"region": "eu-north-1", "family": "c7a", "market": "spot", "idle_minutes": 5,
                      "engines": ["~/a", "~/b"]},
            "cores": 64,
            "enabled": False,
        }])
        pool = cloud.cloud_pools()["aws"]
        self.assertEqual((pool.region, pool.family, pool.market, pool.idle_minutes), ("eu-north-1", "c7a", "spot", 5))
        self.assertEqual((pool.engines, pool.cores, pool.enabled), (["~/a", "~/b"], 64, False))

    def test_rejects_unknown_market(self) -> None:
        self.write_workers([{"host": "aws", "cloud": {"market": "reserved"}}])
        with self.assertRaises(SystemExit):
            cloud.cloud_pools()

    def test_cloud_entries_are_not_hosts(self) -> None:
        path = self.write_workers(["pwa-llm", {"host": "aws", "cloud": {}, "cores": 64}])
        payload = workers.read_config_workers(path)
        self.assertEqual(payload["workers"], ["pwa-llm"])

    def test_default_workers_bring_enabled_pools_only(self) -> None:
        self.write_workers(["pwa-llm", {"host": "aws", "cloud": {}, "cores": 64, "enabled": False}])
        pools = cloud.cloud_pools()
        self.assertEqual(cloud.cloud_requests(None, pools), [])
        self.assertEqual(cloud.cloud_requests("aws", pools), [("aws", 64)])
        self.assertEqual(cloud.cloud_requests("pwa-llm,aws:16", pools), [("aws", 16)])
        self.write_workers(["pwa-llm", {"host": "aws", "cloud": {}, "cores": 32}])
        self.assertEqual(cloud.cloud_requests(None, cloud.cloud_pools()), [("aws", 32)])

    def test_bare_pool_without_cores_is_an_error(self) -> None:
        with self.assertRaises(SystemExit):
            cloud.parse_cloud_entry("aws", {"aws": cloud.Pool(name="aws")})


class CloudEntryTests(unittest.TestCase):
    def test_parses_cores_and_max(self) -> None:
        self.assertEqual(cloud.parse_cloud_entry("aws:64", POOLS), ("aws", 64))
        self.assertEqual(cloud.parse_cloud_entry("aws:max", POOLS), ("aws", None))
        self.assertEqual(cloud.parse_cloud_entry("aws:0", POOLS), ("aws", 0))

    def test_bare_pool_uses_its_cores(self) -> None:
        self.assertEqual(cloud.parse_cloud_entry("aws", {"aws": cloud.Pool(name="aws", cores=64)}), ("aws", 64))

    def test_hosts_and_unknown_pools_are_not_cloud_entries(self) -> None:
        for entry in ("pwa-llm", "pwa-llm:8", "aws:50%", "gcp:64", "aws=linux:4"):
            self.assertIsNone(cloud.parse_cloud_entry(entry, POOLS), entry)

    def test_splits_cloud_entries_from_hosts(self) -> None:
        self.assertEqual(
            cloud.split_cloud_entries("pwa-llm:8,aws:64,pwa-5090", POOLS),
            ("pwa-llm:8,pwa-5090", [("aws", 64)]),
        )

    def test_cloud_only_means_default_workers(self) -> None:
        self.assertEqual(cloud.split_cloud_entries("aws:32", POOLS), (None, [("aws", 32)]))

    def test_without_pools_nothing_is_split(self) -> None:
        self.assertEqual(cloud.split_cloud_entries("aws:32", {}), ("aws:32", []))

    def test_workers_arg_drops_cloud_entries(self) -> None:
        with mock.patch.object(cloud, "cloud_pools", return_value=POOLS):
            payload = workers.workers_payload_from_arg("pwa-llm:4,aws:64", default_ok=False)
        self.assertEqual([item["host"] for item in payload["workers"]], ["pwa-llm"])

    def test_workers_arg_with_only_cloud_uses_default_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "workers.json"
            path.write_text(json.dumps({"schema": "forge.workers.v1", "workers": ["pwa-llm", "pwa-5090"]}))
            with mock.patch.object(cloud, "cloud_pools", return_value=POOLS), \
                    mock.patch.object(workers, "default_workers_path", return_value=path):
                payload = workers.workers_payload_from_arg("aws:64", default_ok=False)
        self.assertEqual(payload["workers"], ["pwa-llm", "pwa-5090"])


class PlanTests(unittest.TestCase):
    def cores(self, plan: list[str]) -> int:
        return sum(C7A[name]["cores"] for name in plan)

    def test_uses_the_fewest_instances(self) -> None:
        self.assertEqual(cloud.plan_instances(64, C7A), ["c7a.16xlarge"])
        self.assertEqual(cloud.plan_instances(80, C7A), ["c7a.16xlarge", "c7a.4xlarge"])
        self.assertEqual(cloud.plan_instances(256, C7A), ["c7a.48xlarge", "c7a.16xlarge"])

    def test_rounds_up_to_the_smallest_size(self) -> None:
        plan = cloud.plan_instances(5, C7A)
        self.assertEqual(plan, ["c7a.xlarge", "c7a.large"])
        self.assertGreaterEqual(self.cores(plan), 5)

    def test_zero_cores_is_no_instances(self) -> None:
        self.assertEqual(cloud.plan_instances(0, C7A), [])


class UserDataTests(unittest.TestCase):
    def script(self, **overrides: object) -> str:
        pool = cloud.Pool(name="aws", user="petter", **overrides)  # type: ignore[arg-type]
        return cloud.user_data(pool, authkey="tskey-auth-x", public_key="ssh-ed25519 AAAA test")

    def test_is_valid_bash(self) -> None:
        result = subprocess.run(["bash", "-n"], input=self.script(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_has_both_safety_shutdowns(self) -> None:
        script = self.script(max_hours=3, idle_minutes=7)
        self.assertIn('shutdown -h +180 "forge-cloud lifetime reached"', script)
        self.assertIn("-ge 420 ]", script)
        self.assertIn("systemctl enable --now forge-cloud-idle.timer", script)

    def test_idle_check_only_counts_real_processes(self) -> None:
        script = self.script()
        start = script.index("<<'IDLE'\n") + len("<<'IDLE'\n")
        body = script[start:script.index("\nIDLE\n")]
        self.assertIn("ps -u petter", body)
        # The session manager alone is idle; anything else keeps the host up.
        check = "grep -qvE '^(systemd|\\(sd-pam\\))$'"
        self.assertIn(check, body)
        for comm, busy in (("systemd\n(sd-pam)\n", False), ("systemd\npython3\n", True), ("", False)):
            result = subprocess.run(["bash", "-c", check], input=comm,
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode == 0, busy, comm)


class SshConfTests(unittest.TestCase):
    def test_adds_and_removes_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conf = Path(tmp) / "forge-cloud.conf"
            with mock.patch.object(cloud, "ssh_conf_path", return_value=conf):
                cloud.set_ssh_host("aws-11111111", "100.1.1.1", "petter")
                cloud.set_ssh_host("aws-22222222", "100.2.2.2", "petter")
                cloud.set_ssh_host("aws-11111111", "100.1.1.9", "petter")
                text = conf.read_text()
                self.assertEqual(text.count("Host aws-11111111"), 1)
                self.assertIn("HostName 100.1.1.9", text)
                cloud.set_ssh_host("aws-11111111", None)
                text = conf.read_text()
            self.assertNotIn("aws-11111111", text)
            self.assertIn("Host aws-22222222", text)


class ScaleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pool = cloud.Pool(name="aws")
        patches = [
            mock.patch.object(cloud, "instance_types", return_value=C7A),
            mock.patch.object(cloud, "run_lock", return_value=mock.MagicMock()),
            mock.patch.object(cloud, "terminate"),
            mock.patch.object(cloud, "drain"),
            mock.patch.object(cloud, "set_ssh_host"),
            mock.patch.object(cloud, "bring_up", return_value=True),
        ]
        self.mocks = [patch.start() for patch in patches]
        for patch in patches:
            self.addCleanup(patch.stop)
        self.terminate, self.drain = self.mocks[2], self.mocks[3]

    def instance(self, suffix: str, instance_type: str, launched: str) -> cloud.Instance:
        return cloud.Instance(id=f"i-0000{suffix}", name=f"aws-0000{suffix}", type=instance_type, state="running",
                              run="r", lifecycle="on-demand", launched=launched)

    def test_scales_up_within_the_quota(self) -> None:
        launched = [self.instance("aaaa", "c7a.16xlarge", "t1")]
        with mock.patch.object(cloud, "list_instances", return_value=[]), \
                mock.patch.object(cloud, "free_vcpus", return_value=100), \
                mock.patch.object(cloud, "launch", return_value=launched) as launch:
            self.assertEqual(cloud.scale(self.pool, "r", 128, log=lambda _m: None), 0)
        # 128 asked, quota leaves 100: 64 + 16 + 16 + 4.
        plan = launch.call_args.args[1]
        self.assertLessEqual(sum(C7A[name]["cores"] for name in plan), 100)
        self.assertEqual(plan[0], "c7a.16xlarge")

    def test_scales_down_newest_first_and_drains(self) -> None:
        current = [self.instance("aaaa", "c7a.16xlarge", "t1"), self.instance("bbbb", "c7a.4xlarge", "t2")]
        with mock.patch.object(cloud, "list_instances", return_value=current):
            self.assertEqual(cloud.scale(self.pool, "r", 64, log=lambda _m: None), 0)
        self.drain.assert_called_once_with(current[1], "r")
        self.terminate.assert_called_once_with(self.pool, ["i-0000bbbb"])

    def test_zero_removes_everything(self) -> None:
        current = [self.instance("aaaa", "c7a.16xlarge", "t1"), self.instance("bbbb", "c7a.4xlarge", "t2")]
        with mock.patch.object(cloud, "list_instances", return_value=current):
            cloud.scale(self.pool, "r", 0, log=lambda _m: None)
        self.terminate.assert_called_once_with(self.pool, ["i-0000bbbb", "i-0000aaaa"])

    def test_forget_hosts_drops_terminated_cloud_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "r"
            run_dir.mkdir()
            path = run_dir / "workers.json"
            path.write_text(json.dumps({"schema": "forge.workers.v1", "workers": [
                {"host": "pwa-llm"}, {"host": "aws-0000aaaa", "profile": "linux"}, "aws-0000bbbb:4"]}))
            with mock.patch.object(cloud, "run_dir_for_name", return_value=run_dir):
                cloud.forget_hosts("r", {"aws-0000aaaa", "aws-0000bbbb"})
            self.assertEqual(json.loads(path.read_text())["workers"], [{"host": "pwa-llm"}])

    def test_release_run_never_raises(self) -> None:
        with mock.patch.object(cloud, "cloud_pools", return_value=POOLS), \
                mock.patch.object(cloud, "list_instances", side_effect=cloud.CloudError("offline")):
            messages: list[str] = []
            self.assertEqual(cloud.release_run("r", log=messages.append), 0)
        self.assertIn("offline", messages[0])


if __name__ == "__main__":
    unittest.main()
