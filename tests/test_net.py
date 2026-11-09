import unittest

from fleetcc.chaos import ALLOWED, make_cfg
from fleetcc.checker import check
from fleetcc.net import run_net
from fleetcc.sim import RunConfig


class NetTests(unittest.TestCase):
    def run_cfg(self, cfg):
        rt, wall = run_net(cfg)
        final = rt.final_state()
        self.assertEqual(final["errors"], [])
        self.assertEqual(rt.servers, {})  # every listener was closed
        return rt, check(rt.history, final, cfg.mode, ALLOWED)

    def test_clean_rollout_over_tcp(self):
        rt, viol = self.run_cfg(make_cfg(0, 10, "pinned", faulty=False, max_ms=8000))
        self.assertEqual(viol, [])
        self.assertGreater(rt.stats["frames"], 100)

    def test_node_crash_restart_reloads_from_disk(self):
        cfg = RunConfig(n_nodes=10, seed=0, max_ms=8000,
                        faults=[{"kind": "crash", "actor": "n1", "at": 5.0, "down": 400.0}])
        rt, viol = self.run_cfg(cfg)
        self.assertEqual(viol, [])
        self.assertIn("restart", [e["k"] for e in rt.history])


if __name__ == "__main__":
    unittest.main()
