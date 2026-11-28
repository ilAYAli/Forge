from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("FORGE_CONFIG", os.path.join(tempfile.mkdtemp(prefix="forge-test-config-"), "config.yml"))

from forge_lib.status import process_throughput  # noqa: E402


def waves(processes: int, cycle: float, games: int, start: float, end: float) -> list[tuple[float, str, int]]:
    """Every process finishes a shard each cycle, all at the same moment."""
    events = []
    t = start + cycle
    while t <= end:
        events.extend((t, f"p{i}", games) for i in range(processes))
        t += cycle
    return events


class ProcessThroughputTests(unittest.TestCase):
    def test_synchronized_waves_give_the_true_rate_at_any_phase(self) -> None:
        # 200 slots, 2-game shards every 67 s: 200 * 2 / 67 games/s.
        events = waves(200, 67.0, 2, 0.0, 3000.0)
        true_rate = 200 * 2 / 67.0
        for now in range(1200, 3000, 7):  # crests, troughs and everything between
            rate = process_throughput(events, [f"p{i}" for i in range(200)], float(now))
            self.assertAlmostEqual(rate, true_rate, delta=true_rate * 0.02, msg=f"now={now}")

    def test_counts_only_processes_working_now(self) -> None:
        events = waves(10, 60.0, 2, 0.0, 1200.0)
        rate = process_throughput(events, [f"p{i}" for i in range(5)], 1200.0)
        self.assertAlmostEqual(rate, 5 * 2 / 60.0, places=6)

    def test_new_process_counts_at_the_typical_rate(self) -> None:
        events = waves(4, 60.0, 2, 0.0, 1200.0)
        rate = process_throughput(events, ["p0", "p1", "p2", "p3", "new"], 1200.0)
        self.assertAlmostEqual(rate, 5 * 2 / 60.0, places=6)

    def test_long_tasks_widen_the_window(self) -> None:
        # 10-minute shards: a 300 s window would hold one completion each.
        events = waves(8, 600.0, 20, 0.0, 6000.0)
        rate = process_throughput(events, [f"p{i}" for i in range(8)], 6000.0)
        self.assertAlmostEqual(rate, 8 * 20 / 600.0, places=6)

    def test_without_two_completions_there_is_no_rate(self) -> None:
        self.assertIsNone(process_throughput([(100.0, "p0", 2)], ["p0"], 120.0))
        self.assertIsNone(process_throughput([], ["p0"], 120.0))
        self.assertIsNone(process_throughput([(1.0, "p0", 2), (60.0, "p0", 2)], [], 61.0))


class SprtFieldOrderTests(unittest.TestCase):
    def test_draw_follows_games_per_minute(self) -> None:
        from unittest import mock
        from forge_lib import status

        progress = ["elo=-14.9", "ci=14.3", "llr=-4.02/690.78", "los=2.0%", "draw=35.1%",
                    "games=1895/10k", "games/min=352", "eta=23m03s"]
        summary = {"counts": {"done": 773, "fail": 0, "stale": 0}, "tasks": 4832}
        with mock.patch.object(status, "run_progress_fields", return_value=progress), \
                mock.patch.object(status, "run_state_fields", return_value=[]), \
                mock.patch.object(status, "run_speedup_field", return_value=None):
            fields = status.run_display_fields(summary)
        self.assertEqual(fields, ["elo=-14.9", "ci=14.3", "llr=-4.02/690.78", "los=2.0%",
                                  "games=1895/10k", "games/min=352", "draw=35.1%",
                                  "tasks=773/4832", "eta=23m03s"])


if __name__ == "__main__":
    unittest.main()
