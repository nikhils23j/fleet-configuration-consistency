"""The rollout protocol, written as I/O-free state machines.

Every actor (Store, Node, Router, Coordinator) only reacts to
``on_message(msg)`` and ``on_timer(name)``, and talks to the outside world
through a small ``ctx`` object:

    ctx.now                  current time in milliseconds
    ctx.send(dst, msg)       fire-and-forget message (may be lost/duplicated)
    ctx.set_timer(name, ms)  (re)arm a named timer; ctx.cancel_timer(name)
    ctx.record(kind, **kw)   append an event to the run history
    ctx.persist()            "fsync" this actor's durable state
    ctx.rng                  per-actor random.Random

The same classes run inside the deterministic simulator (sim.py) and on real
asyncio TCP sockets (net.py).  Messages are plain JSON-able dicts.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace

STORE, COORD, COORD_B, ROUTER = "store", "coord", "coordb", "router"

# Deliberate bugs that can be switched on to check that the chaos harness and
# invariant checker actually notice when a safety layer is missing.
MUTATIONS = {
    "no_fencing": "store accepts alloc/decide from a stale lease token",
    "commit_on_majority": "coordinator commits once a majority (not all) nodes prepared",
    "skip_hash_check": "node stages a manifest without checking its SHA-256",
    "no_epoch_monotonic": "node activates a COMMIT even if the epoch is older than its active one",
    "no_read_verify": "node serves reads without re-hashing the stored config",
    "no_anti_entropy": "nodes never poll the store (no catch-up, no drift repair)",
}


@dataclass(frozen=True)
class Params:
    lease_ms: float = 300.0
    renew_ms: float = 80.0
    lease_margin_ms: float = 30.0
    prepare_timeout_ms: float = 150.0
    retry_ms: float = 40.0
    bake_ms: float = 100.0
    abort_backoff_ms: float = 120.0
    poll_ms: float = 200.0
    router_poll_ms: float = 20.0
    read_every_ms: float = 10.0
    read_fanout: int = 5
    read_timeout_ms: float = 100.0
    keep_committed: int = 3
    canary_fraction: float = 0.05
    canary_bake_ms: float = 200.0
    wave_fraction: float = 0.1  # baselines roll out in waves of 10% of the fleet ...
    wave_ms: float = 20.0  # ... every 20 ms (a compressed rolling deploy)
    mutations: frozenset = field(default_factory=frozenset)

    def scaled(self, factor: float) -> "Params":
        """Same protocol, slower timeouts (used by the real-socket mode, where
        ~55 actors share one Python event loop and timers fire late)."""
        keys = ("lease_ms", "renew_ms", "lease_margin_ms", "prepare_timeout_ms", "retry_ms", "bake_ms",
                "abort_backoff_ms", "poll_ms", "router_poll_ms", "read_timeout_ms")
        return replace(self, **{k: getattr(self, k) * factor for k in keys})

    def with_mutations(self, *names: str) -> "Params":
        for name in names:
            if name not in MUTATIONS:
                raise ValueError(f"unknown mutation {name!r}")
        return replace(self, mutations=frozenset(names))


def canonical(manifest: dict) -> bytes:
    return json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()


_DIGEST_CACHE: dict[int, tuple] = {}


def digest(manifest: dict) -> str:
    """SHA-256 of the canonical JSON.  Memoised per dict object: manifests are
    never mutated in place (corruption always builds a new dict), so object
    identity is a safe cache key while we hold a reference to the object."""
    hit = _DIGEST_CACHE.get(id(manifest))
    if hit is not None and hit[0] is manifest:
        return hit[1]
    d = hashlib.sha256(canonical(manifest)).hexdigest()
    if len(_DIGEST_CACHE) > 4096:
        _DIGEST_CACHE.clear()
    _DIGEST_CACHE[id(manifest)] = (manifest, d)
    return d


BASE = {"change_id": "base", "max_conns": 100, "timeout_ms": 250, "feature_x": False}
V2 = {"change_id": "rollout-1", "max_conns": 80, "timeout_ms": 200, "feature_x": True}
ROLLBACK = {**BASE, "change_id": "rollback-1"}
DEFAULT_PLAN = (V2, ROLLBACK)  # roll out v2, then roll back to base content as a NEW epoch


def node_name(i: int) -> str:
    return f"n{i}"


# --------------------------------------------------------------------------- store
class Store:
    """Lease + decision log + manifest blobs.  Stand-in for an etcd-like
    linearizable store; we assume it never crashes (see README limits)."""

    def __init__(self, ctx, params: Params, base: dict = BASE):
        self.ctx, self.p = ctx, params
        self.token = 0
        self.holder: str | None = None
        self.expiry = -1.0
        d = digest(base)
        self.epochs: dict[int, dict] = {1: {"manifest": base, "digest": d, "decision": "commit"}}
        self.committed = 1
        self.next_epoch = 2
        self.alloc_seen: dict[tuple, int] = {}
        ctx.record("store_commit", epoch=1, digest=d)

    def start(self):
        pass

    def on_timer(self, name):
        pass

    def _fence_ok(self, token) -> bool:
        return "no_fencing" in self.p.mutations or token == self.token

    def on_message(self, m):
        t, src, now = m["type"], m["src"], self.ctx.now
        if t == "acquire":
            if self.holder is None or now >= self.expiry:
                self.token += 1
                self.holder, self.expiry = m["owner"], now + self.p.lease_ms
                c = self.epochs[self.committed]
                self.ctx.record("lease_granted", token=self.token, owner=m["owner"])
                self.ctx.send(src, {"type": "granted", "owner": m["owner"], "token": self.token,
                                    "committed": self.committed, "digest": c["digest"]})
            else:
                self.ctx.send(src, {"type": "denied", "owner": m["owner"],
                                    "retry_in": self.expiry - now})
        elif t == "renew":
            ok = m["token"] == self.token and now < self.expiry
            if ok:
                self.expiry = now + self.p.lease_ms
            self.ctx.send(src, {"type": "renewed" if ok else "lease_lost",
                                "token": m["token"], "rid": m["rid"]})
        elif t == "alloc":
            if not self._fence_ok(m["token"]):
                self.ctx.send(src, {"type": "fenced", "op": "alloc", "token": m["token"]})
                return
            key = (m["token"], m["rid"])
            if key not in self.alloc_seen:
                e = self.next_epoch
                self.next_epoch += 1
                self.epochs[e] = {"manifest": m["manifest"], "digest": digest(m["manifest"]),
                                  "decision": None}
                self.alloc_seen[key] = e
                self.ctx.record("store_alloc", epoch=e, token=m["token"])
            self.ctx.send(src, {"type": "allocated", "rid": m["rid"], "epoch": self.alloc_seen[key]})
        elif t == "decide":
            e, want = m["epoch"], m["decision"]
            if not self._fence_ok(m["token"]):
                self.ctx.record("decide_fenced", epoch=e, token=m["token"], current=self.token)
                self.ctx.send(src, {"type": "fenced", "op": "decide", "token": m["token"], "epoch": e})
                return
            rec = self.epochs.get(e)
            if rec is None:
                self.ctx.send(src, {"type": "decided", "epoch": e, "decision": None, "ok": False})
                return
            if rec["decision"] is None:
                if want == "commit" and e <= self.committed:
                    want = "abort"  # monotonic: an older epoch can never become current
                rec["decision"] = want
                if want == "commit":
                    self.committed = e
                    self.ctx.record("store_commit", epoch=e, digest=rec["digest"], token=m["token"])
                else:
                    self.ctx.record("store_abort", epoch=e, token=m["token"])
            self.ctx.send(src, {"type": "decided", "epoch": e, "decision": rec["decision"],
                                "ok": rec["decision"] == m["decision"]})
        elif t == "get":
            c = self.epochs[self.committed]
            decisions = [[e, self.epochs[e]["decision"] if e in self.epochs else "abort"]
                         for e in m.get("ask", ())]
            self.ctx.send(src, {"type": "state", "committed": self.committed, "digest": c["digest"],
                                "manifest": c["manifest"], "decisions": decisions})


# --------------------------------------------------------------------------- node
def fresh_node_state(base: dict = BASE) -> dict:
    """Durable node state.  Survives crashes (assumed fsync'd before replies)."""
    return {"configs": {1: {"manifest": base, "digest": digest(base), "state": "committed"}},
            "active": 1, "max_token": 0}


class Node:
    def __init__(self, ctx, params: Params, nid: str, durable: dict):
        self.ctx, self.p, self.id, self.d = ctx, params, nid, durable
        self.last_poll = -1e9

    # -- helpers
    def start(self):
        if "no_anti_entropy" not in self.p.mutations:
            self.ctx.set_timer("poll", self.p.poll_ms * (0.25 + 0.75 * self.ctx.rng.random()))

    def _fence(self, token) -> bool:
        if token < self.d["max_token"]:
            return False
        self.d["max_token"] = token
        return True

    def _poll_now(self):
        if "no_anti_entropy" in self.p.mutations or self.ctx.now - self.last_poll < 20:
            return
        self.last_poll = self.ctx.now
        staged = [e for e, c in self.d["configs"].items() if c["state"] == "staged"]
        self.ctx.send(STORE, {"type": "get", "ask": staged})

    def _activate(self, e: int):
        active = self.d["active"]
        if e == active or (e < active and "no_epoch_monotonic" not in self.p.mutations):
            return
        c = self.d["configs"][e]
        c["state"] = "committed"
        self.d["active"] = e
        self.ctx.record("node_activate", node=self.id, epoch=e, prev=active, digest=c["digest"])
        self._gc()
        self.ctx.persist()

    def _gc(self):
        cfg = self.d["configs"]
        committed = sorted(e for e, c in cfg.items() if c["state"] == "committed")
        newest = max(committed)
        keep = set(committed[-self.p.keep_committed:]) | {self.d["active"]}
        for e in list(cfg):
            c = cfg[e]
            if (c["state"] == "committed" and e not in keep) or (c["state"] == "staged" and e < newest):
                del cfg[e]

    # -- handlers
    def on_timer(self, name):
        if name == "poll":
            self.last_poll = -1e9
            self._poll_now()
            self.ctx.set_timer("poll", self.p.poll_ms)

    def on_message(self, m):
        t = m["type"]
        if t == "read":
            self._on_read(m)
        elif t == "prepare":
            self._on_prepare(m)
        elif t == "commit":
            if not self._fence(m["token"]):
                self.ctx.send(m["src"], {"type": "nack", "epoch": m["epoch"], "reason": "stale_token"})
                return
            if m["epoch"] in self.d["configs"]:
                self._activate(m["epoch"])
            else:
                self._poll_now()
            self.ctx.send(m["src"], {"type": "commit_ack", "epoch": m["epoch"]})
        elif t == "abort":
            if self._fence(m["token"]):
                c = self.d["configs"].get(m["epoch"])
                if c is not None and c["state"] == "staged":
                    del self.d["configs"][m["epoch"]]
                    self.ctx.persist()
        elif t == "state":
            self._on_state(m)

    def _on_read(self, m):
        e = m["epoch"]
        c = self.d["configs"].get(e)
        if c is None:
            self.ctx.send(m["src"], {"type": "read_err", "req": m["req"], "reason": "EPOCH_MISSING"})
            self._poll_now()
            return
        actual = digest(c["manifest"])
        if actual != c["digest"] and "no_read_verify" not in self.p.mutations:
            self.ctx.send(m["src"], {"type": "read_err", "req": m["req"], "reason": "CORRUPT"})
            self._poll_now()
            return
        self.ctx.send(m["src"], {"type": "read_ok", "req": m["req"], "epoch": e, "digest": actual})

    def _on_prepare(self, m):
        src, e = m["src"], m["epoch"]
        if not self._fence(m["token"]):
            self.ctx.send(src, {"type": "nack", "epoch": e, "reason": "stale_token"})
            return
        hint = m["committed"]
        if hint > self.d["active"]:
            if hint in self.d["configs"]:
                self._activate(hint)
            else:
                self._poll_now()
        if e <= self.d["active"]:
            c = self.d["configs"].get(e)
            if c is not None and e == self.d["active"]:
                self.ctx.send(src, {"type": "prepared", "epoch": e, "digest": c["digest"]})
            else:
                self.ctx.send(src, {"type": "nack", "epoch": e, "reason": "old_epoch"})
            return
        claimed = m["digest"]
        actual = claimed if "skip_hash_check" in self.p.mutations else digest(m["manifest"])
        if actual != claimed:
            self.ctx.record("hash_reject", node=self.id, epoch=e)
            self.ctx.send(src, {"type": "nack", "epoch": e, "reason": "hash_mismatch"})
            return
        cfg = self.d["configs"]
        for x in [x for x, c in cfg.items() if c["state"] == "staged" and x < e]:
            del cfg[x]  # superseded: a coordinator only prepares e after giving up on x
        if e not in cfg:
            cfg[e] = {"manifest": m["manifest"], "digest": claimed, "state": "staged"}
            self.ctx.persist()
        self.ctx.send(src, {"type": "prepared", "epoch": e, "digest": cfg[e]["digest"]})

    def _on_state(self, m):
        cfg, committed = self.d["configs"], m["committed"]
        changed = False
        for e, dec in m["decisions"]:
            c = cfg.get(e)
            if dec == "abort" and c is not None and c["state"] == "staged":
                del cfg[e]
                changed = True
        if committed >= self.d["active"]:
            c = cfg.get(committed)
            healthy = c is not None and c["digest"] == m["digest"] and digest(c["manifest"]) == m["digest"]
            if not healthy:
                if digest(m["manifest"]) != m["digest"]:
                    return
                drift = c is not None and committed == self.d["active"]
                cfg[committed] = {"manifest": m["manifest"], "digest": m["digest"],
                                  "state": "staged" if committed > self.d["active"] else "committed"}
                changed = True
                self.ctx.record("drift_repaired" if drift else "node_fetched",
                                node=self.id, epoch=committed)
            if committed > self.d["active"]:
                self._activate(committed)
        if changed:
            self._gc()
            self.ctx.persist()


# --------------------------------------------------------------------------- router
class Router:
    """Client-facing gateway.  Every client request fans out to `read_fanout`
    nodes; in pinned mode all of them are asked for the same epoch."""

    def __init__(self, ctx, params: Params, node_ids: list[str], pinned: bool = True):
        self.ctx, self.p, self.nodes, self.pinned = ctx, params, node_ids, pinned
        self.epoch = 1
        self.req = 0
        self.inflight: dict[int, dict] = {}

    def start(self):
        self.ctx.set_timer("read", self.p.read_every_ms)
        if self.pinned:
            self.ctx.set_timer("poll", self.p.router_poll_ms)

    def on_timer(self, name):
        if name == "read":
            self.req += 1
            k = min(self.p.read_fanout, len(self.nodes))
            targets = self.ctx.rng.sample(self.nodes, k)
            e = self.epoch if self.pinned else None
            self.inflight[self.req] = {"t0": self.ctx.now, "epoch": e, "want": set(targets),
                                       "ok": [], "err": []}
            for n in targets:
                self.ctx.send(n, {"type": "read", "req": self.req, "epoch": e})
            self.ctx.set_timer(f"rto:{self.req}", self.p.read_timeout_ms)
            self.ctx.set_timer("read", self.p.read_every_ms)
        elif name == "poll":
            self.ctx.send(STORE, {"type": "get", "ask": []})
            self.ctx.set_timer("poll", self.p.router_poll_ms)
        elif name.startswith("rto:"):
            self._finish(int(name[4:]))

    def _finish(self, req):
        r = self.inflight.pop(req, None)
        if r is None:
            return
        self.ctx.cancel_timer(f"rto:{req}")
        self.ctx.record("read", req=req, t0=r["t0"], epoch=r["epoch"], ok=r["ok"], err=r["err"],
                        lost=sorted(r["want"]))

    def on_message(self, m):
        t = m["type"]
        if t in ("read_ok", "read_err"):
            r = self.inflight.get(m["req"])
            if r is None or m["src"] not in r["want"]:
                return  # late or duplicate reply
            r["want"].discard(m["src"])
            if t == "read_ok":
                r["ok"].append([m["src"], m["epoch"], m["digest"], self.ctx.now])
            else:
                r["err"].append([m["src"], m["reason"], self.ctx.now])
            if not r["want"]:
                self._finish(m["req"])
        elif t == "state" and m["committed"] > self.epoch:
            self.ctx.record("router_flip", epoch=m["committed"], prev=self.epoch)
            self.epoch = m["committed"]


# --------------------------------------------------------------------------- coordinator
class Coordinator:
    """Lease-holding rollout driver: alloc epoch -> PREPARE all -> decide in
    store (fenced, write-once) -> COMMIT/ABORT -> bake -> next plan step."""

    def __init__(self, ctx, params: Params, node_ids: list[str], plan=DEFAULT_PLAN, inc: int = 1,
                 name: str = COORD):
        self.ctx, self.p, self.nodes, self.inc = ctx, params, node_ids, inc
        self.plan = [(m, digest(m)) for m in plan]
        self.owner = f"{name}#{inc}"
        self.phase = "acquiring"
        self.token: int | None = None
        self.lease_until = -1.0
        self.rid_n = 0
        self.renew_sent: dict[str, float] = {}
        self.acquire_sent = 0.0
        self.committed = 1
        self.step = 0
        self.epoch: int | None = None
        self.acks: set[str] = set()
        self.commit_acks: set[str] = set()
        self.attempts = 0

    def _rid(self) -> str:
        self.rid_n += 1
        return f"{self.inc}:{self.rid_n}"

    def _lease_ok(self) -> bool:
        return self.token is not None and self.ctx.now < self.lease_until

    def start(self):
        self._acquire()

    def _acquire(self):
        self.phase = "acquiring"
        self.acquire_sent = self.ctx.now
        self.ctx.send(STORE, {"type": "acquire", "owner": self.owner})
        self.ctx.set_timer("acquire", 100.0)

    def _step_down(self, why: str):
        self.ctx.record("coord_step_down", owner=self.owner, why=why, epoch=self.epoch)
        self.token = None
        for name in ("renew", "alloc", "prep_retry", "prep_timeout", "decide", "step", "commit_retry"):
            self.ctx.cancel_timer(name)
        self.phase = "acquiring"
        self.ctx.set_timer("acquire", 50.0)

    # -- plan steps
    def _begin_step(self):
        if not self._lease_ok():
            return self._step_down("lease expired before step")
        if self.step >= len(self.plan):
            if self.phase != "done":
                self.ctx.record("plan_done", owner=self.owner, committed=self.committed)
            self.phase = "done"
            return
        self.phase = "alloc"
        self.alloc_rid = self._rid()
        self._send_alloc()

    def _send_alloc(self):
        manifest, d = self.plan[self.step]
        self.ctx.send(STORE, {"type": "alloc", "token": self.token, "rid": self.alloc_rid,
                              "manifest": manifest, "digest": d})
        self.ctx.set_timer("alloc", self.p.retry_ms)

    def _send_prepare(self, targets):
        manifest, d = self.plan[self.step]
        msg = {"type": "prepare", "token": self.token, "epoch": self.epoch, "manifest": manifest,
               "digest": d, "committed": self.committed}
        for n in targets:
            self.ctx.send(n, msg)

    def _decide(self, decision):
        if not self._lease_ok():
            return self._step_down("lease expired before decide")
        self.ctx.cancel_timer("prep_retry")
        self.ctx.cancel_timer("prep_timeout")
        self.phase, self.decision = "deciding", decision
        self.ctx.send(STORE, {"type": "decide", "token": self.token, "epoch": self.epoch,
                              "decision": decision})
        self.ctx.set_timer("decide", self.p.retry_ms)

    # -- events
    def on_timer(self, name):
        if name == "acquire" and self.phase == "acquiring":
            self._acquire()
        elif name == "renew" and self.token is not None:
            if not self._lease_ok():
                return self._step_down("renew timeout")
            rid = self._rid()
            self.renew_sent[rid] = self.ctx.now
            self.ctx.send(STORE, {"type": "renew", "token": self.token, "rid": rid})
            self.ctx.set_timer("renew", self.p.renew_ms)
        elif name == "alloc" and self.phase == "alloc":
            if not self._lease_ok():
                return self._step_down("lease expired in alloc")
            self._send_alloc()
        elif name == "prep_retry" and self.phase == "prepare":
            if not self._lease_ok():
                return self._step_down("lease expired in prepare")
            self._send_prepare([n for n in self.nodes if n not in self.acks])
            self.ctx.set_timer("prep_retry", self.p.retry_ms)
        elif name == "prep_timeout" and self.phase == "prepare":
            self.ctx.record("prepare_timeout", epoch=self.epoch, acked=len(self.acks))
            self._decide("abort")
        elif name == "decide" and self.phase == "deciding":
            self._decide(self.decision)
        elif name == "commit_retry" and self.phase == "bake":
            msg = {"type": "commit", "token": self.token, "epoch": self.epoch}
            for n in self.nodes:
                if n not in self.commit_acks:
                    self.ctx.send(n, msg)
            self.ctx.set_timer("commit_retry", self.p.retry_ms)
        elif name == "step" and self.phase in ("bake", "backoff"):
            self.ctx.cancel_timer("commit_retry")
            self._begin_step()

    def on_message(self, m):
        t = m["type"]
        if t == "granted" and m["owner"] == self.owner and self.phase == "acquiring":
            self.token = m["token"]
            self.lease_until = self.acquire_sent + self.p.lease_ms - self.p.lease_margin_ms
            self.committed = m["committed"]
            idx = [d for _, d in self.plan].index(m["digest"]) if m["digest"] in [d for _, d in self.plan] else -1
            self.step = idx + 1
            self.ctx.cancel_timer("acquire")
            self.ctx.record("coord_leader", owner=self.owner, token=self.token, step=self.step)
            self.ctx.set_timer("renew", self.p.renew_ms)
            self.phase = "leader"
            self._begin_step()
        elif t == "denied" and m["owner"] == self.owner and self.phase == "acquiring":
            self.ctx.set_timer("acquire", max(10.0, m["retry_in"]) + 5 * self.ctx.rng.random())
        elif t == "renewed" and m["token"] == self.token:
            sent = self.renew_sent.pop(m["rid"], None)
            if sent is not None:
                self.lease_until = max(self.lease_until, sent + self.p.lease_ms - self.p.lease_margin_ms)
        elif t == "lease_lost" and m["token"] == self.token:
            self._step_down("lease lost")
        elif t == "fenced" and m["token"] == self.token:
            self._step_down(f"fenced on {m['op']}")
        elif t == "allocated" and self.phase == "alloc" and m["rid"] == self.alloc_rid:
            if not self._lease_ok():
                return self._step_down("lease expired after alloc")
            self.ctx.cancel_timer("alloc")
            self.epoch, self.acks, self.phase = m["epoch"], set(), "prepare"
            self.ctx.record("prepare_start", epoch=self.epoch, step=self.step, attempt=self.attempts)
            self._send_prepare(self.nodes)
            self.ctx.set_timer("prep_retry", self.p.retry_ms)
            self.ctx.set_timer("prep_timeout", self.p.prepare_timeout_ms)
        elif t == "prepared" and self.phase == "prepare" and m["epoch"] == self.epoch:
            if m["digest"] == self.plan[self.step][1]:
                self.acks.add(m["src"])
            need = len(self.nodes) // 2 + 1 if "commit_on_majority" in self.p.mutations else len(self.nodes)
            if len(self.acks) >= need:
                self._decide("commit")
        elif t == "nack" and m["epoch"] == self.epoch and self.phase == "prepare":
            if m["reason"] == "stale_token":
                self._step_down("node saw newer token")
            elif m["reason"] == "hash_mismatch":
                self._send_prepare([m["src"]])  # corrupted in transit: retransmit
        elif t == "decided" and self.phase == "deciding" and m["epoch"] == self.epoch:
            self.ctx.cancel_timer("decide")
            if m["decision"] == "commit":
                if not m["ok"]:
                    return self._step_down("someone else decided")
                self.committed = self.epoch
                self.ctx.record("coord_committed", epoch=self.epoch, step=self.step)
                self.commit_acks = set()
                msg = {"type": "commit", "token": self.token, "epoch": self.epoch}
                for n in self.nodes:
                    self.ctx.send(n, msg)
                self.phase = "bake"
                self.step += 1
                self.attempts = 0
                self.ctx.set_timer("commit_retry", self.p.retry_ms)
                self.ctx.set_timer("step", self.p.bake_ms)
            else:
                msg = {"type": "abort", "token": self.token, "epoch": self.epoch}
                for n in self.nodes:
                    self.ctx.send(n, msg)
                self.attempts += 1
                self.phase = "backoff"
                self.ctx.set_timer("step", self.p.abort_backoff_ms)
        elif t == "commit_ack" and self.phase == "bake" and m["epoch"] == self.epoch:
            self.commit_acks.add(m["src"])
