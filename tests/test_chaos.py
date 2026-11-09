import unittest

from fleetcc.chaos import _ddmin, make_cfg, run_checked, run_summary, shrink
from fleetcc.sim import RunConfig
from run_eval import STALE_COORD_SCENARIO


class ChaosTests(unittest.TestCase):
    def test_random_schedules_hold_all_invariants(self):
        for prof in ("default", "lease", "reorder"):
            for seed in range(25):
                s = run_summary(make_cfg(seed, 20, "pinned", profile=prof))
                self.assertEqual(s["violations"], {}, (prof, seed, s["first_violation"]))

    def test_fencing_blocks_the_stale_coordinator(self):
        cfg = RunConfig(n_nodes=20, seed=0, faults=STALE_COORD_SCENARIO)
        sim, viol = run_checked(cfg)
        self.assertEqual(viol, [])
        self.assertTrue(any(e["k"] == "decide_fenced" for e in sim.history))
        _, viol = run_checked(RunConfig(n_nodes=20, seed=0, mutations=("no_fencing",), faults=STALE_COORD_SCENARIO))
        self.assertIn("no_stranded_epoch", {v["inv"] for v in viol})

    def test_majority_commit_bug_is_caught_and_shrunk(self):
        bad = None
        for seed in range(100):
            cfg = make_cfg(seed, 20, "pinned", ["commit_on_majority"], max_ms=8000)
            if run_checked(cfg)[1]:
                bad = cfg
                break
        self.assertIsNotNone(bad)
        small, info = shrink(bad)
        self.assertLessEqual(len(small.faults), len(bad.faults))
        self.assertLess(small.n_nodes, bad.n_nodes)
        self.assertIn(info["invariant"], {v["inv"] for v in run_checked(small)[1]})

    def test_ddmin_finds_minimal_subset(self):
        items = list(range(10))
        self.assertEqual(_ddmin(items, lambda xs: 3 in xs and 7 in xs), [3, 7])


if __name__ == "__main__":
    unittest.main()
