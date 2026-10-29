import asyncio
import unittest

from fleet import Fleet, Manifest, detect_drift, naive_rollout_mixed_samples


class FleetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old = Manifest(0, {"limit": 100})
        self.new = Manifest(1, {"limit": 80})

    async def test_clean_commit(self):
        fleet = Fleet(50, self.old)
        result = await fleet.rollout(self.new)
        self.assertTrue(result.committed)
        self.assertEqual(result.mixed_samples, 0)
        self.assertEqual(fleet.served_versions(), {1})
        self.assertEqual(fleet.drifted_nodes(), [])

    async def test_failures_abort_without_switch(self):
        for mode in ("reject", "corrupt", "timeout"):
            with self.subTest(mode=mode):
                fleet = Fleet(50, self.old)
                fleet.nodes[7].failure = mode
                result = await fleet.rollout(self.new)
                self.assertFalse(result.committed)
                self.assertEqual(result.mixed_samples, 0)
                self.assertEqual(fleet.served_versions(), {0})
                self.assertTrue(all(node.staged is None for node in fleet.nodes))

    async def test_drift_by_hash_not_version_alone(self):
        fleet = Fleet(10, self.old)
        fleet.nodes[4].active = Manifest(0, {"limit": 101})
        self.assertEqual(await detect_drift(fleet), [4])

    async def test_rejects_old_target(self):
        fleet = Fleet(3, self.old)
        with self.assertRaises(ValueError):
            await fleet.rollout(self.old)

    def test_naive_baseline_exposes_mixed_versions(self):
        self.assertEqual(naive_rollout_mixed_samples(50), 49)


if __name__ == "__main__":
    unittest.main()
