import random
import unittest

from fleetcc.protocol import BASE, V2, Node, Params, Store, digest, fresh_node_state


class FakeCtx:
    def __init__(self, now=0.0):
        self.now = now
        self.sent, self.events, self.timers = [], [], {}
        self.rng = random.Random(0)

    def send(self, dst, msg):
        self.sent.append((dst, msg))

    def set_timer(self, name, delay):
        self.timers[name] = delay

    def cancel_timer(self, name):
        self.timers.pop(name, None)

    def record(self, kind, **kw):
        self.events.append((kind, kw))

    def persist(self):
        pass

    def last(self):
        return self.sent[-1][1]

    def last_reply(self):
        return next(m for _, m in reversed(self.sent) if m["type"].startswith("read"))


class DigestTests(unittest.TestCase):
    def test_canonical_and_sensitive(self):
        self.assertEqual(digest({"a": 1, "b": 2}), digest({"b": 2, "a": 1}))
        self.assertNotEqual(digest(BASE), digest({**BASE, "max_conns": 101}))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.ctx = FakeCtx()
        self.store = Store(self.ctx, Params())

    def msg(self, **kw):
        kw.setdefault("src", "coord")
        self.store.on_message(kw)
        return self.ctx.last()

    def test_lease_tokens_increase_and_stale_decide_is_fenced(self):
        g1 = self.msg(type="acquire", owner="a")
        self.assertEqual(g1["type"], "granted")
        self.assertEqual(self.msg(type="acquire", owner="b")["type"], "denied")
        e = self.msg(type="alloc", token=g1["token"], rid="r1", manifest=V2, digest=digest(V2))["epoch"]
        self.ctx.now = 1000.0  # lease expired; b takes over with a bigger token
        g2 = self.msg(type="acquire", owner="b")
        self.assertGreater(g2["token"], g1["token"])
        reply = self.msg(type="decide", token=g1["token"], epoch=e, decision="commit")
        self.assertEqual(reply["type"], "fenced")
        self.assertEqual(self.store.committed, 1)

    def test_decisions_are_write_once_and_monotonic(self):
        tok = self.msg(type="acquire", owner="a")["token"]
        e2 = self.msg(type="alloc", token=tok, rid="r1", manifest=V2, digest=digest(V2))["epoch"]
        e3 = self.msg(type="alloc", token=tok, rid="r2", manifest=BASE, digest=digest(BASE))["epoch"]
        self.assertEqual(self.msg(type="alloc", token=tok, rid="r2", manifest=BASE, digest="x")["epoch"], e3)
        self.assertTrue(self.msg(type="decide", token=tok, epoch=e3, decision="commit")["ok"])
        late = self.msg(type="decide", token=tok, epoch=e2, decision="commit")
        self.assertFalse(late["ok"])  # older epoch can never become current
        self.assertEqual(self.store.committed, e3)
        again = self.msg(type="decide", token=tok, epoch=e3, decision="abort")
        self.assertEqual(again["decision"], "commit")


class NodeTests(unittest.TestCase):
    def setUp(self):
        self.ctx = FakeCtx()
        self.node = Node(self.ctx, Params(), "n0", fresh_node_state())

    def prepare(self, epoch, manifest=V2, token=1, claimed=None, committed=1):
        self.node.on_message({"type": "prepare", "src": "coord", "token": token, "epoch": epoch,
                              "manifest": manifest, "digest": claimed or digest(manifest),
                              "committed": committed})
        return self.ctx.last()

    def test_hash_mismatch_is_rejected(self):
        bad = {**V2, "max_conns": 81}
        r = self.prepare(2, manifest=bad, claimed=digest(V2))
        self.assertEqual((r["type"], r["reason"]), ("nack", "hash_mismatch"))
        self.assertNotIn(2, self.node.d["configs"])

    def test_stale_token_is_rejected(self):
        self.prepare(2, token=5)
        r = self.prepare(3, token=4)
        self.assertEqual(r["reason"], "stale_token")

    def test_staged_epoch_serves_pinned_reads_and_old_epoch_still_served(self):
        self.prepare(2)
        for e in (1, 2):
            self.node.on_message({"type": "read", "src": "router", "req": e, "epoch": e})
            self.assertEqual(self.ctx.last_reply()["type"], "read_ok")
        self.node.on_message({"type": "read", "src": "router", "req": 9, "epoch": 7})
        self.assertEqual(self.ctx.last_reply()["reason"], "EPOCH_MISSING")

    def test_activation_never_goes_backwards(self):
        self.prepare(2)
        self.prepare(3, manifest=BASE, committed=2)  # hint promotes epoch 2 first
        self.assertEqual(self.node.d["active"], 2)
        self.node.on_message({"type": "commit", "src": "coord", "token": 1, "epoch": 3})
        self.assertEqual(self.node.d["active"], 3)
        self.node.on_message({"type": "commit", "src": "coord", "token": 1, "epoch": 2})  # late duplicate
        self.assertEqual(self.node.d["active"], 3)

    def test_corrupted_config_is_not_served(self):
        c = self.node.d["configs"][1]
        c["manifest"] = {**c["manifest"], "max_conns": 1}
        self.node.on_message({"type": "read", "src": "router", "req": 1, "epoch": 1})
        self.assertEqual(self.ctx.last_reply()["reason"], "CORRUPT")
        self.assertEqual(self.ctx.last()["type"], "get")  # and asks the store for a fresh copy


if __name__ == "__main__":
    unittest.main()
