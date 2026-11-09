"""Real-socket mode: every actor is an asyncio TCP server on 127.0.0.1.

Same protocol classes as the simulator, but messages are length-prefixed JSON
frames over real loopback TCP connections, timers are real `loop.call_later`
timers, and node state is written to JSON files (atomic rename, no fsync) and
re-read from disk after a crash.  Faults are injected on the sending side
using the same schedule format and link model as the simulator.

It is still one OS process with one event loop - see README "what this isn't".
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import struct
import tempfile
import time

from .faults import LinkModel, schedule_end, uniforms
from .protocol import DEFAULT_PLAN, Params, digest, node_name
from .sim import RunConfig, actor_ids, apply_bitrot, initial_durable, is_converged, make_actor

HDR = struct.Struct(">I")
NET_PARAMS = Params().scaled(3.0)


def _load_durable(path: str) -> dict:
    with open(path) as f:
        d = json.load(f)
    if "configs" in d:  # JSON turned int epoch keys into strings
        d["configs"] = {int(k): v for k, v in d["configs"].items()}
    return d


class NetCtx:
    def __init__(self, rt: "Runtime", aid: str, inc: int):
        self.rt, self.aid, self.inc = rt, aid, inc
        self.rng = random.Random(f"{rt.cfg.seed}:{aid}:{inc}")

    @property
    def now(self) -> float:
        return self.rt.local_now(self.aid)

    def send(self, dst, msg):
        self.rt.send(self.aid, dst, msg)

    def set_timer(self, name, delay):
        self.rt.set_timer(self.aid, self.inc, name, delay)

    def cancel_timer(self, name):
        h = self.rt.timers.pop((self.aid, name), None)
        if h is not None:
            h.cancel()

    def record(self, kind, **kw):
        self.rt.record(kind, self.aid, kw)

    def persist(self):
        self.rt.persist(self.aid)


class Runtime:
    def __init__(self, cfg: RunConfig, params: Params | None = None, plan=DEFAULT_PLAN, workdir: str = "."):
        self.cfg = cfg
        self.p = (params or NET_PARAMS).with_mutations(*cfg.mutations)
        self.plan = plan
        self.final_digest = digest(plan[-1])
        self.node_ids = [node_name(i) for i in range(cfg.n_nodes)]
        self.ids = actor_ids(cfg.mode, self.node_ids)
        self.workdir = workdir
        self.link = LinkModel(cfg.faults, base_latency=(0.0, 0.0))  # real loopback latency only
        self.fault_end = schedule_end(cfg.faults)
        self.clock_jumps: dict[str, list] = {}
        for f in cfg.faults:
            if f["kind"] == "clock_jump":
                self.clock_jumps.setdefault(f["actor"], []).append((f["at"], f["by"]))
            elif f["kind"] == "suspend":
                self.clock_jumps.setdefault(f["actor"], []).append((f["at"], -f["dur"]))
        self.history: list[dict] = []
        self.timers: dict = {}
        self.link_seq: dict = {}
        self.inc = {a: 1 for a in self.ids}
        self.down: set[str] = set()
        self.paused_until: dict[str, float] = {}
        self.ports: dict[str, int] = {}
        self.servers: dict[str, asyncio.base_events.Server] = {}
        self.conns_in: dict[str, set] = {a: set() for a in self.ids}
        self.out_q: dict[tuple, asyncio.Queue] = {}
        self.tasks: list[asyncio.Task] = []
        self.connected = 0
        self.errors: list[str] = []
        self.plan_done = False
        self.converged_at = None
        self.stats = {"sent": 0, "dropped": 0, "frames": 0, "send_failures": 0}
        self.durable = initial_durable(cfg.mode, self.node_ids)
        self.actors: dict = {}

    # ---------------------------------------------------------------- clocks / history
    def true_now(self) -> float:
        return (self.loop.time() - self.t0) * 1000.0

    def local_now(self, aid) -> float:
        t = self.true_now()
        jumps = self.clock_jumps.get(aid)
        return t + sum(by for at, by in jumps if at <= t) if jumps else t

    def record(self, kind, aid, kw):
        ev = {"k": kind, "t": round(self.true_now(), 3), "a": aid}
        ev.update(kw)
        self.history.append(ev)
        if kind == "plan_done":
            self.plan_done = True

    def persist(self, aid):
        if aid not in self.durable or aid.startswith("coord"):
            return
        path = os.path.join(self.workdir, f"{aid}.json")
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.durable[aid], f)
        os.replace(tmp, path)

    # ---------------------------------------------------------------- timers
    def set_timer(self, aid, inc, name, delay):
        old = self.timers.pop((aid, name), None)
        if old is not None:
            old.cancel()
        self.timers[(aid, name)] = self.loop.call_later(delay / 1000.0, self._fire, aid, inc, name)

    def _fire(self, aid, inc, name):
        if aid in self.down or inc != self.inc[aid]:
            return
        pu = self.paused_until.get(aid)
        if pu is not None and self.true_now() < pu:
            self.timers[(aid, name)] = self.loop.call_later((pu - self.true_now()) / 1000.0, self._fire, aid, inc, name)
            return
        self.timers.pop((aid, name), None)
        self._guard(aid, lambda: self.actors[aid].on_timer(name))

    def _guard(self, aid, fn):
        try:
            fn()
        except Exception as exc:  # surface protocol bugs instead of losing them in callbacks
            self.errors.append(f"{aid}: {type(exc).__name__}: {exc}")

    # ---------------------------------------------------------------- network
    def send(self, src, dst, msg):
        self.stats["sent"] += 1
        key = (src, dst)
        s = self.link_seq.get(key, 0) + 1
        self.link_seq[key] = s
        delays, corrupt = self.link.fate(self.true_now(), src, dst, msg["type"], uniforms(self.cfg.seed, src, dst, s))
        if not delays:
            self.stats["dropped"] += 1
            return
        if corrupt:
            from .faults import corrupt_manifest
            msg = dict(msg, manifest=corrupt_manifest(msg["manifest"]))
        data = json.dumps(dict(msg, src=src), separators=(",", ":")).encode()
        frame = HDR.pack(len(data)) + data
        for d in delays:
            if d <= 0:
                self._enqueue(key, frame)
            else:
                self.loop.call_later(d / 1000.0, self._enqueue, key, frame)

    def _enqueue(self, key, frame):
        q = self.out_q.get(key)
        if q is None:
            q = self.out_q[key] = asyncio.Queue()
            self.tasks.append(asyncio.ensure_future(self._writer(key, q)))
        q.put_nowait(frame)

    async def _writer(self, key, q):
        """One outgoing TCP connection per (src, dst) link, reconnecting on failure."""
        _, dst = key
        writer, port = None, None
        try:
            while True:
                frame = await q.get()
                try:
                    if writer is None or port != self.ports.get(dst) or writer.is_closing():
                        if writer is not None:
                            writer.close()
                        port = self.ports.get(dst)
                        if port is None or dst in self.down:
                            raise ConnectionError("peer down")
                        _, writer = await asyncio.open_connection("127.0.0.1", port)
                        self.connected += 1
                    if frame is None:  # warm-up: just connect
                        continue
                    writer.write(frame)
                    await writer.drain()
                    self.stats["frames"] += 1
                except (OSError, ConnectionError):
                    self.stats["send_failures"] += 1
                    if writer is not None:
                        writer.close()
                    writer = None
        finally:
            if writer is not None:
                writer.close()

    async def _serve(self, aid, reader, writer):
        self.conns_in[aid].add(writer)
        try:
            while True:
                hdr = await reader.readexactly(HDR.size)
                body = await reader.readexactly(HDR.unpack(hdr)[0])
                self._deliver(aid, json.loads(body))
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            self.conns_in[aid].discard(writer)
            writer.close()

    def _deliver(self, aid, msg):
        if aid in self.down:
            return
        pu = self.paused_until.get(aid)
        if pu is not None and self.true_now() < pu:
            self.loop.call_later((pu - self.true_now()) / 1000.0, self._deliver, aid, msg)
            return
        self._guard(aid, lambda: self.actors[aid].on_message(msg))

    async def _warm_up(self):
        """Open the long-lived links up front, like a real fleet would already have."""
        coords = [a for a in self.ids if a.startswith("coord")]
        links = [(c, n) for c in coords for n in self.node_ids] + [("router", n) for n in self.node_ids]
        links += [(n, a) for n in self.node_ids for a in ["store", "router"] + coords]
        links += [("store", a) for a in self.ids if a != "store"]
        links += [(c, "store") for c in coords] + [("router", "store")]
        for key in links:
            self._enqueue(key, None)
        while self.connected < len(links):
            await asyncio.sleep(0.005)

    async def _listen(self, aid):
        server = await asyncio.start_server(lambda r, w: self._serve(aid, r, w), "127.0.0.1", 0)
        self.servers[aid] = server
        self.ports[aid] = server.sockets[0].getsockname()[1]

    async def _close_actor_sockets(self, aid):
        server = self.servers.pop(aid, None)
        self.ports.pop(aid, None)
        if server is not None:
            server.close()
        for w in list(self.conns_in[aid]):
            w.close()

    # ---------------------------------------------------------------- faults
    def _new_actor(self, aid):
        if aid in self.node_ids:
            path = os.path.join(self.workdir, f"{aid}.json")
            if os.path.exists(path):  # a restarted node only knows what it wrote to disk
                self.durable[aid] = _load_durable(path)
        return make_actor(aid, NetCtx(self, aid, self.inc[aid]), self.cfg.mode, self.p, self.node_ids,
                          self.plan, self.durable, self.inc[aid])

    async def _fault(self, f):
        kind, aid = f["kind"], f.get("actor")
        if kind == "crash" and aid in self.actors:
            await asyncio.sleep(max(0.0, f["at"] - self.true_now()) / 1000.0)
            if aid in self.down:
                return
            self.down.add(aid)
            self.inc[aid] += 1
            for (a, name), h in list(self.timers.items()):
                if a == aid:
                    h.cancel()
                    del self.timers[(a, name)]
            self.record("crash", aid, {})
            await self._close_actor_sockets(aid)
            await asyncio.sleep(f["down"] / 1000.0)
            await self._listen(aid)
            self.actors[aid] = self._new_actor(aid)
            self.down.discard(aid)
            self.record("restart", aid, {})
            self._guard(aid, self.actors[aid].start)
        elif kind in ("pause", "suspend") and aid in self.actors:
            await asyncio.sleep(max(0.0, f["at"] - self.true_now()) / 1000.0)
            self.paused_until[aid] = max(self.paused_until.get(aid, 0.0), self.true_now() + f["dur"])
            self.record("pause", aid, {"until": self.paused_until[aid]})
        elif kind == "bitrot" and aid in self.actors:
            await asyncio.sleep(max(0.0, f["at"] - self.true_now()) / 1000.0)
            self.record("bitrot", aid, {"epoch": apply_bitrot(self.cfg.mode, self.durable[aid])})
            self.persist(aid)

    # ---------------------------------------------------------------- main
    async def run(self) -> "Runtime":
        self.loop = asyncio.get_running_loop()
        for aid in self.ids:
            await self._listen(aid)
        for n in self.node_ids:
            self.persist(n)
        self.t0 = self.loop.time()
        await self._warm_up()
        self.t0 = self.loop.time()
        for aid in self.ids:
            self.actors[aid] = self._new_actor(aid)
        for aid in self.ids:
            self._guard(aid, self.actors[aid].start)
        fault_tasks = [asyncio.ensure_future(self._fault(f)) for f in self.cfg.faults]
        try:
            while self.true_now() < self.cfg.max_ms:
                await asyncio.sleep(0.005)
                if (self.plan_done and self.true_now() >= self.fault_end and not self.errors
                        and is_converged(self.cfg.mode, self.actors, self.durable, self.node_ids,
                                         self.final_digest, self.down)):
                    self.converged_at = self.true_now()
                    self.record("converged", "harness", {})
                    break
        finally:
            await self.shutdown(fault_tasks)
        return self

    async def shutdown(self, extra_tasks=()):
        for h in self.timers.values():
            h.cancel()
        self.timers.clear()
        self.down.update(self.ids)  # stop delivering anything
        for t in list(extra_tasks) + self.tasks:
            t.cancel()
        await asyncio.gather(*extra_tasks, *self.tasks, return_exceptions=True)
        for aid in list(self.servers):
            await self._close_actor_sockets(aid)
        for aid in self.ids:
            for w in list(self.conns_in[aid]):
                w.close()
        await asyncio.sleep(0.05)  # let the transports finish closing

    def final_state(self) -> dict:
        out = {"converged_at": self.converged_at, "fault_end": self.fault_end, "end_ms": self.true_now(),
               "errors": self.errors}
        if self.cfg.mode == "pinned":
            st = self.actors["store"]
            out["store_committed"] = st.committed
            out["lagging"] = [n for n in self.node_ids if self.durable[n]["active"] != st.committed][:10]
        return out


def raise_fd_limit(want: int = 8192):
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < want:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(want, hard), hard))
    except (ImportError, ValueError, OSError):
        pass


def run_net(cfg: RunConfig, params: Params | None = None) -> tuple[Runtime, float]:
    """Run one config on real sockets.  Returns the runtime and wall seconds."""
    raise_fd_limit()
    with tempfile.TemporaryDirectory(prefix="fleetcc-") as d:
        rt = Runtime(cfg, params, workdir=d)
        t = time.perf_counter()
        asyncio.run(rt.run())
        return rt, time.perf_counter() - t
