"""Deterministic discrete-event simulator (FoundationDB/TigerBeetle style, but tiny).

One heap of (virtual_time, seq, event).  Same seed + same schedule => same
history, byte for byte.  Actors are the I/O-free classes from protocol.py and
baseline.py; this file only supplies time, the network and the faults.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import asdict, dataclass, field

from . import baseline, protocol
from .faults import LinkModel, corrupt_manifest, schedule_end, uniforms
from .protocol import COORD, COORD_B, DEFAULT_PLAN, ROUTER, STORE, Params, digest, node_name

MODES = ("pinned", "naive", "canary")
MSG, TIMER, FAULT, PROBE = 0, 1, 2, 3


@dataclass
class RunConfig:
    n_nodes: int = 50
    seed: int = 0
    mode: str = "pinned"
    mutations: tuple = ()
    faults: list = field(default_factory=list)
    max_ms: float = 15000.0

    def to_json(self) -> dict:
        d = asdict(self)
        d["mutations"] = list(self.mutations)
        return d

    @classmethod
    def from_json(cls, d: dict) -> "RunConfig":
        d = dict(d)
        d["mutations"] = tuple(d.get("mutations", ()))
        return cls(**d)


def make_actor(aid, ctx, mode, p, node_ids, plan, durable, inc):
    """Build one actor; shared by the simulator and the real-socket runtime."""
    if aid == STORE:
        return protocol.Store(ctx, p)
    if aid == ROUTER:
        return protocol.Router(ctx, p, node_ids, pinned=mode == "pinned")
    if aid in (COORD, COORD_B) and mode == "pinned":
        return protocol.Coordinator(ctx, p, node_ids, plan, inc=inc, name=aid)
    if aid == COORD:
        return baseline.NaiveCoordinator(ctx, p, node_ids, plan, inc=inc, durable=durable[COORD],
                                         canary=mode == "canary")
    if mode == "pinned":
        return protocol.Node(ctx, p, aid, durable[aid])
    return baseline.NaiveNode(ctx, p, aid, durable[aid])


def initial_durable(mode, node_ids) -> dict:
    d = {n: protocol.fresh_node_state() if mode == "pinned" else baseline.fresh_naive_state() for n in node_ids}
    d[COORD] = {"step": 0}
    return d


def actor_ids(mode, node_ids) -> list[str]:
    coords = [COORD, COORD_B] if mode == "pinned" else [COORD]
    return [STORE, ROUTER] + coords + list(node_ids)


def is_converged(mode, actors, durable, node_ids, final_digest, down) -> bool:
    """Omniscient test-harness check: is the whole fleet on the final config?"""
    if down:
        return False
    if mode == "pinned":
        store = actors[STORE]
        e = store.committed
        want = store.epochs[e]["digest"]
        if want != final_digest or actors[ROUTER].epoch != e:
            return False
        if any(durable[n]["active"] != e for n in node_ids):
            return False
        return all(digest(durable[n]["configs"][e]["manifest"]) == want for n in node_ids)
    return all(digest(durable[n]["manifest"]) == final_digest for n in node_ids)


def apply_bitrot(mode, durable_node: dict) -> int:
    if mode == "pinned":
        c = durable_node["configs"][durable_node["active"]]
        c["manifest"] = corrupt_manifest(c["manifest"])
        return durable_node["active"]
    durable_node["manifest"] = corrupt_manifest(durable_node["manifest"])
    return durable_node["version"]


class SimCtx:
    __slots__ = ("sim", "aid", "rng")

    def __init__(self, sim: "Sim", aid: str, inc: int):
        self.sim, self.aid = sim, aid
        self.rng = random.Random(f"{sim.cfg.seed}:{aid}:{inc}")

    @property
    def now(self) -> float:
        """This actor's *local* clock (true time plus any injected clock jumps)."""
        jumps = self.sim.clock_jumps.get(self.aid)
        if not jumps:
            return self.sim.now
        return self.sim.now + sum(by for at, by in jumps if at <= self.sim.now)

    def send(self, dst, msg):
        self.sim._send(self.aid, dst, msg)

    def set_timer(self, name, delay):
        self.sim._set_timer(self.aid, name, delay)

    def cancel_timer(self, name):
        key = (self.aid, name)
        self.sim.timer_gen[key] = self.sim.timer_gen.get(key, 0) + 1

    def record(self, kind, **kw):
        self.sim._record(kind, self.aid, kw)

    def persist(self):
        pass  # durable dicts live in Sim.durable and survive crashes


class Sim:
    def __init__(self, cfg: RunConfig, params: Params | None = None, plan=DEFAULT_PLAN):
        if cfg.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        self.cfg = cfg
        self.p = (params or Params()).with_mutations(*cfg.mutations)
        self.plan = plan
        self.final_digest = digest(plan[-1])
        self.now = 0.0
        self.heap: list = []
        self.seq = 0
        self.history: list[dict] = []
        self.timer_gen: dict = {}
        self.link_seq: dict = {}
        self.inc: dict[str, int] = {}
        self.down: set[str] = set()
        self.paused_until: dict[str, float] = {}
        self.link = LinkModel(cfg.faults)
        self.clock_jumps: dict[str, list] = {}
        for f in cfg.faults:
            if f["kind"] == "clock_jump":
                self.clock_jumps.setdefault(f["actor"], []).append((f["at"], f["by"]))
            elif f["kind"] == "suspend":  # frozen, and the local clock skips the frozen time
                self.clock_jumps.setdefault(f["actor"], []).append((f["at"], -f["dur"]))
        self.fault_end = schedule_end(cfg.faults)
        self.plan_done = False
        self.converged_at: float | None = None
        self.stats = {"sent": 0, "dropped": 0, "duplicated": 0, "corrupted": 0, "events": 0}
        self.node_ids = [node_name(i) for i in range(cfg.n_nodes)]
        self.durable = initial_durable(cfg.mode, self.node_ids)
        self.actors: dict = {}
        # pinned mode also runs a hot-standby coordinator that keeps trying to take the lease
        ids = actor_ids(cfg.mode, self.node_ids)
        for aid in ids:
            self.inc[aid] = 1
            self.actors[aid] = self._make(aid)
        for aid in ids:
            self.actors[aid].start()
        for f in cfg.faults:
            self._schedule_fault(f)
        self._push(5.0, PROBE, None)

    # ---------------------------------------------------------------- plumbing
    def _make(self, aid):
        ctx = SimCtx(self, aid, self.inc[aid])
        return make_actor(aid, ctx, self.cfg.mode, self.p, self.node_ids, self.plan, self.durable, self.inc[aid])

    def _push(self, t, kind, payload):
        self.seq += 1
        heapq.heappush(self.heap, (t, self.seq, kind, payload))

    def _record(self, kind, aid, kw):
        ev = {"k": kind, "t": round(self.now, 3), "a": aid}
        ev.update(kw)
        self.history.append(ev)
        if kind == "plan_done":
            self.plan_done = True

    def _send(self, src, dst, msg):
        self.stats["sent"] += 1
        key = (src, dst)
        s = self.link_seq.get(key, 0) + 1
        self.link_seq[key] = s
        u = uniforms(self.cfg.seed, src, dst, s)
        delays, corrupt = self.link.fate(self.now, src, dst, msg["type"], u)
        if not delays:
            self.stats["dropped"] += 1
            return
        if corrupt:
            self.stats["corrupted"] += 1
            msg = dict(msg, manifest=corrupt_manifest(msg["manifest"]))
        msg = dict(msg, src=src)
        if len(delays) > 1:
            self.stats["duplicated"] += 1
        for d in delays:
            self._push(self.now + d, MSG, (dst, msg))

    def _set_timer(self, aid, name, delay):
        key = (aid, name)
        g = self.timer_gen.get(key, 0) + 1
        self.timer_gen[key] = g
        self._push(self.now + delay, TIMER, (aid, self.inc[aid], name, g))

    # ---------------------------------------------------------------- faults
    def _schedule_fault(self, f):
        k = f["kind"]
        if k == "crash":
            self._push(f["at"], FAULT, ("crash", f["actor"]))
            self._push(f["at"] + f["down"], FAULT, ("restart", f["actor"]))
        elif k in ("pause", "suspend"):
            self._push(f["at"], FAULT, ("pause", f["actor"], f["dur"]))
        elif k == "bitrot":
            self._push(f["at"], FAULT, ("bitrot", f["actor"]))

    def _apply_fault(self, ev):
        what, aid = ev[0], ev[1]
        if aid not in self.actors:
            return
        if what == "crash" and aid not in self.down:
            self.down.add(aid)
            self.inc[aid] += 1  # invalidates all pending timers of the old incarnation
            self._record("crash", aid, {})
        elif what == "restart" and aid in self.down:
            self.down.discard(aid)
            self.actors[aid] = self._make(aid)
            self._record("restart", aid, {})
            self.actors[aid].start()
        elif what == "pause" and aid not in self.down:
            self.paused_until[aid] = max(self.paused_until.get(aid, 0.0), self.now + ev[2])
            self._record("pause", aid, {"until": self.paused_until[aid]})
        elif what == "bitrot":
            self._record("bitrot", aid, {"epoch": apply_bitrot(self.cfg.mode, self.durable[aid])})

    # ---------------------------------------------------------------- convergence
    def converged(self) -> bool:
        return is_converged(self.cfg.mode, self.actors, self.durable, self.node_ids, self.final_digest, self.down)

    # ---------------------------------------------------------------- main loop
    def run(self) -> "Sim":
        heap, actors = self.heap, self.actors
        while heap:
            t, _, kind, payload = heapq.heappop(heap)
            if t > self.cfg.max_ms:
                break
            self.now = t
            self.stats["events"] += 1
            if kind == MSG:
                dst, msg = payload
                if dst in self.down:
                    continue
                pu = self.paused_until.get(dst)
                if pu is not None and t < pu:
                    self._push(pu, MSG, payload)
                    continue
                actors[dst].on_message(msg)
            elif kind == TIMER:
                aid, inc, name, g = payload
                if aid in self.down or inc != self.inc[aid] or self.timer_gen.get((aid, name)) != g:
                    continue
                pu = self.paused_until.get(aid)
                if pu is not None and t < pu:
                    self._push(pu, TIMER, payload)
                    continue
                actors[aid].on_timer(name)
            elif kind == FAULT:
                self._apply_fault(payload)
            elif kind == PROBE:
                if self.plan_done and t >= self.fault_end and self.converged():
                    self.converged_at = t
                    self._record("converged", "sim", {})
                    break
                self._push(t + 5.0, PROBE, None)
        return self

    def final_state(self) -> dict:
        out = {"converged_at": self.converged_at, "fault_end": self.fault_end, "end_ms": self.now}
        if self.cfg.mode == "pinned":
            st = self.actors[STORE]
            out["store_committed"] = st.committed
            out["router_epoch"] = self.actors[ROUTER].epoch
            out["lagging"] = [n for n in self.node_ids if self.durable[n]["active"] != st.committed][:10]
        else:
            out["lagging"] = [n for n in self.node_ids
                              if digest(self.durable[n]["manifest"]) != self.final_digest][:10]
        return out


def run(cfg: RunConfig, params: Params | None = None) -> Sim:
    return Sim(cfg, params).run()
