import unittest

from fleetcc.checker import check

OK_FINAL = {"converged_at": 100.0, "end_ms": 100.0}


def commit(t, e, d):
    return {"k": "store_commit", "t": t, "a": "store", "epoch": e, "digest": d}


def read(t, epoch, ok=(), err=()):
    return {"k": "read", "t": t, "t0": t, "a": "router", "req": int(t), "epoch": epoch,
            "ok": [list(x) for x in ok], "err": [list(x) for x in err], "lost": []}


class CheckerTests(unittest.TestCase):
    def kinds(self, hist, final=OK_FINAL, mode="pinned", allowed=()):
        return {v["inv"] for v in check(hist, final, mode, allowed)}

    def test_clean_history_passes(self):
        h = [commit(0, 1, "A"), read(5, 1, ok=[("n0", 1, "A", 6), ("n1", 1, "A", 6)])]
        self.assertEqual(self.kinds(h), set())

    def test_mixed_read(self):
        h = [commit(0, 1, "A"), commit(1, 2, "B"), read(5, 1, ok=[("n0", 1, "A", 6), ("n1", 2, "B", 6)])]
        self.assertEqual(self.kinds(h), {"no_mixed_read"})

    def test_single_wrong_digest_counts_as_mixed(self):
        h = [commit(0, 1, "A"), read(5, 1, ok=[("n0", 1, "Z", 6)])]
        self.assertEqual(self.kinds(h), {"no_mixed_read"})

    def test_naive_mode_mixed(self):
        h = [read(5, None, ok=[("n0", 1, "A", 6), ("n1", 2, "B", 6)])]
        self.assertEqual(self.kinds(h, mode="naive", allowed=("A", "B")), {"no_mixed_read"})

    def test_resurrection(self):
        h = [commit(0, 1, "A"), commit(1, 2, "B"), commit(2, 3, "C"),
             {"k": "node_activate", "t": 3, "a": "n0", "node": "n0", "epoch": 3},
             {"k": "node_activate", "t": 4, "a": "n0", "node": "n0", "epoch": 2}]
        self.assertEqual(self.kinds(h), {"no_resurrection"})

    def test_activate_uncommitted(self):
        h = [commit(0, 1, "A"), {"k": "node_activate", "t": 3, "a": "n0", "node": "n0", "epoch": 2}]
        self.assertEqual(self.kinds(h), {"no_resurrection"})

    def test_router_going_back(self):
        h = [commit(0, 1, "A"), commit(1, 3, "C"), {"k": "router_flip", "t": 2, "a": "router", "epoch": 3},
             {"k": "router_flip", "t": 3, "a": "router", "epoch": 2}]
        self.assertIn("no_resurrection", self.kinds(h))

    def test_stranded_epoch_only_for_latest_commit(self):
        h = [commit(0, 1, "A"), commit(10, 2, "B"),
             read(5, 1, err=[("n0", "EPOCH_MISSING", 6)]),   # epoch 1 was latest at t=6 -> violation
             read(20, 1, err=[("n0", "EPOCH_MISSING", 21)])]  # router lagging behind epoch 2 -> fine
        v = check(h, OK_FINAL, "pinned")
        self.assertEqual([x["inv"] for x in v], ["no_stranded_epoch"])

    def test_no_convergence(self):
        self.assertEqual(self.kinds([commit(0, 1, "A")], final={"converged_at": None, "end_ms": 9}),
                         {"convergence"})


if __name__ == "__main__":
    unittest.main()
