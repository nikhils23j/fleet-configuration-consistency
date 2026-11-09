import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from fleetcc.chaos import make_cfg, run_checked, run_summary
from fleetcc.checker import summarize
from fleetcc.protocol import BASE, ROLLBACK, digest
from fleetcc.sim import RunConfig, Sim

ROOT = Path(__file__).resolve().parents[1]


class SimTests(unittest.TestCase):
    def test_clean_pinned_rollout_and_rollback(self):
        sim, viol = run_checked(RunConfig(n_nodes=30, seed=1))
        self.assertEqual(viol, [])
        store = sim.actors["store"]
        self.assertEqual(store.committed, 3)  # v2 = epoch 2, rollback = NEW epoch 3
        self.assertEqual(store.epochs[3]["digest"], digest(ROLLBACK))
        rolled_back = {k: v for k, v in store.epochs[3]["manifest"].items() if k != "change_id"}
        self.assertEqual(rolled_back, {k: v for k, v in BASE.items() if k != "change_id"})
        s = summarize(sim.history, sim.final_state(), viol)
        self.assertEqual(s["mixed_reads"], 0)
        self.assertGreater(s["reads"], 10)

    def test_same_seed_same_history(self):
        cfg = make_cfg(7, 40, "pinned")
        a = Sim(cfg).run().history
        b = Sim(make_cfg(7, 40, "pinned")).run().history
        self.assertEqual(a, b)

    def test_deterministic_across_hash_seeds(self):
        code = ("import json,hashlib;from fleetcc.chaos import make_cfg;from fleetcc.sim import Sim;"
                "h=Sim(make_cfg(11,30,'pinned')).run().history;"
                "print(hashlib.sha256(json.dumps(h,sort_keys=True).encode()).hexdigest())")
        outs = set()
        for hs in ("1", "2"):
            env = dict(os.environ, PYTHONHASHSEED=hs)
            outs.add(subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True,
                                    text=True, check=True).stdout.strip())
        self.assertEqual(len(outs), 1)

    def test_naive_baseline_exposes_mixed_reads(self):
        s = run_summary(make_cfg(1, 50, "naive", faulty=False))
        self.assertGreater(s["mixed_reads"], 0)
        self.assertEqual(set(s["violations"]), {"no_mixed_read"})

    def test_node_crash_mid_prepare_aborts_then_commits(self):
        cfg = RunConfig(n_nodes=20, seed=0, faults=[{"kind": "crash", "actor": "n3", "at": 2.0, "down": 300.0}])
        sim, viol = run_checked(cfg)
        self.assertEqual(viol, [])
        kinds = [e["k"] for e in sim.history]
        self.assertIn("store_abort", kinds)
        self.assertEqual(sim.actors["store"].epochs[sim.actors["store"].committed]["digest"], digest(ROLLBACK))

    def test_bitrot_repaired_only_with_anti_entropy(self):
        rot = [{"kind": "bitrot", "actor": "n2", "at": 400.0}]
        sim, viol = run_checked(RunConfig(n_nodes=10, seed=0, faults=rot))
        self.assertEqual(viol, [])
        self.assertTrue(any(e["k"] == "drift_repaired" for e in sim.history))
        _, viol = run_checked(RunConfig(n_nodes=10, seed=0, mode="naive", faults=rot, max_ms=4000))
        self.assertIn("convergence", {v["inv"] for v in viol})

    def test_corruption_in_transit_is_rejected_and_retried(self):
        cfg = RunConfig(n_nodes=20, seed=2, faults=[{"kind": "corrupt", "start": 0, "end": 300, "p": 0.3}])
        sim, viol = run_checked(cfg)
        self.assertEqual(viol, [])
        self.assertTrue(any(e["k"] == "hash_reject" for e in sim.history))

    def test_runconfig_roundtrip(self):
        cfg = make_cfg(3, 10, "pinned", ["no_fencing"])
        self.assertEqual(RunConfig.from_json(json.loads(json.dumps(cfg.to_json()))), cfg)


if __name__ == "__main__":
    unittest.main()
