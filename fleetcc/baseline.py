"""Baselines: rolling ("naive") push and canary push.

Both still check the SHA-256 of what they receive and never go backwards in
version, so they are not strawmen.  What they lack is the prepare/commit
barrier and epoch-pinned reads: each node serves whatever it has active.
Rollouts go out in waves (Params.wave_fraction every Params.wave_ms), like a
compressed rolling deploy; canary first pushes Params.canary_fraction, bakes
for Params.canary_bake_ms, then rolls out the rest in waves.
"""
from __future__ import annotations

import math

from .protocol import BASE, DEFAULT_PLAN, Params, digest


def fresh_naive_state(base: dict = BASE) -> dict:
    return {"version": 1, "manifest": base, "digest": digest(base)}


class NaiveNode:
    def __init__(self, ctx, params: Params, nid: str, durable: dict):
        self.ctx, self.p, self.id, self.d = ctx, params, nid, durable

    def start(self):
        pass

    def on_timer(self, name):
        pass

    def on_message(self, m):
        t = m["type"]
        if t == "activate":
            if digest(m["manifest"]) != m["digest"]:
                self.ctx.record("hash_reject", node=self.id, epoch=m["version"])
                self.ctx.send(m["src"], {"type": "nack", "version": m["version"]})
                return
            if m["version"] > self.d["version"]:
                prev = self.d["version"]
                self.d.update(version=m["version"], manifest=m["manifest"], digest=m["digest"])
                self.ctx.record("node_activate", node=self.id, epoch=m["version"], prev=prev,
                                digest=m["digest"])
                self.ctx.persist()
            self.ctx.send(m["src"], {"type": "activated", "version": m["version"]})
        elif t == "read":
            self.ctx.send(m["src"], {"type": "read_ok", "req": m["req"], "epoch": self.d["version"],
                                     "digest": digest(self.d["manifest"])})


class NaiveCoordinator:
    """Push each plan step in waves, retrying until acked.  Progress (`step`)
    is durable across coordinator crashes."""

    def __init__(self, ctx, params: Params, node_ids, plan=DEFAULT_PLAN, inc: int = 1,
                 durable: dict | None = None, canary: bool = False):
        self.ctx, self.p, self.nodes, self.inc = ctx, params, list(node_ids), inc
        self.plan = [(m, digest(m)) for m in plan]
        self.d = durable if durable is not None else {"step": 0}
        self.canary = canary
        self.wave = max(1, math.ceil(len(self.nodes) * params.wave_fraction))
        self.n_targets = 0
        self.acks: set[str] = set()
        self.phase = "idle"

    @property
    def version(self) -> int:
        return self.d["step"] + 2

    def start(self):
        self._begin_step()

    def _begin_step(self):
        if self.d["step"] >= len(self.plan):
            if self.phase != "done":
                self.ctx.record("plan_done", owner=f"naive#{self.inc}")
            self.phase = "done"
            return
        self.acks = set()
        self.ctx.record("prepare_start", epoch=self.version, step=self.d["step"], attempt=0)
        if self.canary:
            self.phase = "canary"
            self._extend(max(1, int(len(self.nodes) * self.p.canary_fraction)))
        else:
            self.phase = "rolling"
            self._extend(self.wave)
            self.ctx.set_timer("wave", self.p.wave_ms)
        self.ctx.set_timer("retry", self.p.retry_ms)

    def _extend(self, k):
        old = self.n_targets if self.phase != "canary" or self.n_targets else 0
        self.n_targets = min(len(self.nodes), old + k)
        self._push(self.nodes[old:self.n_targets])

    def _push(self, targets):
        manifest, d = self.plan[self.d["step"]]
        msg = {"type": "activate", "version": self.version, "manifest": manifest, "digest": d}
        for n in targets:
            self.ctx.send(n, msg)

    def _maybe_done(self):
        if self.n_targets < len(self.nodes) and self.phase == "rolling":
            return
        if not all(n in self.acks for n in self.nodes[:self.n_targets]):
            return
        self.ctx.cancel_timer("retry")
        if self.phase == "canary":
            self.phase = "canary_bake"
            self.ctx.set_timer("bake", self.p.canary_bake_ms)
        elif self.phase == "rolling":
            self.ctx.record("coord_committed", epoch=self.version, step=self.d["step"])
            self.d["step"] += 1
            self.ctx.persist()
            self.n_targets = 0
            self.phase = "bake"
            self.ctx.set_timer("bake", self.p.bake_ms)

    def on_timer(self, name):
        if name == "retry" and self.phase in ("canary", "rolling"):
            self._push([n for n in self.nodes[:self.n_targets] if n not in self.acks])
            self.ctx.set_timer("retry", self.p.retry_ms)
        elif name == "wave" and self.phase == "rolling" and self.n_targets < len(self.nodes):
            self._extend(self.wave)
            if self.n_targets < len(self.nodes):
                self.ctx.set_timer("wave", self.p.wave_ms)
            self._maybe_done()
        elif name == "bake" and self.phase == "canary_bake":
            self.phase = "rolling"
            self._extend(self.wave)
            self.ctx.set_timer("wave", self.p.wave_ms)
            self.ctx.set_timer("retry", self.p.retry_ms)
        elif name == "bake" and self.phase == "bake":
            self._begin_step()

    def on_message(self, m):
        if m["type"] == "activated" and m["version"] == self.version:
            self.acks.add(m["src"])
            if self.phase in ("canary", "rolling"):
                self._maybe_done()
        elif m["type"] == "nack" and self.phase in ("canary", "rolling"):
            self._push([m["src"]])
