"""Seeded fault schedules and the per-message network model.

A schedule is a plain list of JSON dicts, so a failing run can be saved,
shrunk and replayed.  Per-message randomness is derived from
hash(seed, src, dst, per-link sequence number) instead of one shared RNG, so
deleting one fault while shrinking does not reshuffle every later coin flip.
"""
from __future__ import annotations

import hashlib
import random

from .protocol import COORD, COORD_B, ROUTER, node_name

KINDS = {  # kind: relative weight
    "crash_node": 3, "crash_coord": 1.5, "pause_coord": 1.5, "pause_node": 0.5,
    "partition": 2, "lossy": 2, "delay_spike": 1, "slow": 1, "corrupt": 1, "bitrot": 1,
    "clock_jump": 1, "suspend_coord": 1.5,
}
# Targeted profiles bias the generator toward fault combinations that hit a
# specific mechanism (same idea as FoundationDB's BUGGIFY): uniform random
# schedules rarely line up a stale coordinator with a slow prepare.
PROFILES = {
    "default": KINDS,
    "lease": {"suspend_in_prepare": 3, "suspend_coord": 1, "slow": 3, "lossy": 1, "crash_coord": 1},
    "reorder": {"delay_spike": 3, "lossy": 2, "slow": 1, "crash_node": 1},
}


def _r(x: float) -> float:
    return round(x, 1)


def gen_schedule(seed: int, n_nodes: int, max_faults: int = 5, profile: str = "default") -> list[dict]:
    rng = random.Random(f"schedule:{profile}:{seed}" if profile != "default" else f"schedule:{seed}")
    kinds, weights = zip(*PROFILES[profile].items())
    out = []
    for _ in range(rng.randint(1, max_faults)):
        k = rng.choices(kinds, weights)[0]
        node = node_name(rng.randrange(n_nodes))
        coord = COORD if rng.random() < 0.75 else COORD_B
        if k == "crash_node":
            out.append({"kind": "crash", "actor": node, "at": _r(rng.uniform(0, 600)),
                        "down": _r(rng.uniform(20, 800))})
        elif k == "crash_coord":
            out.append({"kind": "crash", "actor": coord, "at": _r(rng.uniform(0, 900)),
                        "down": _r(rng.uniform(20, 600))})
        elif k == "pause_coord":
            out.append({"kind": "pause", "actor": coord, "at": _r(rng.uniform(0, 900)),
                        "dur": _r(rng.uniform(100, 700))})
        elif k == "pause_node":
            out.append({"kind": "pause", "actor": node, "at": _r(rng.uniform(0, 900)),
                        "dur": _r(rng.uniform(50, 500))})
        elif k == "partition":
            shape = rng.random()
            if shape < 0.5:
                size = rng.randint(1, max(1, n_nodes // 10))
                group = [node_name(i) for i in sorted(rng.sample(range(n_nodes), size))]
            elif shape < 0.7:
                group = [coord]
            elif shape < 0.85:
                group = [ROUTER]
            else:  # coordinator stranded with a slice of the fleet
                size = rng.randint(1, max(1, n_nodes // 2))
                group = [COORD] + [node_name(i) for i in sorted(rng.sample(range(n_nodes), size))]
            start = rng.uniform(0, 1000)
            out.append({"kind": "partition", "group": group, "start": _r(start),
                        "end": _r(start + rng.uniform(50, 800))})
        elif k == "lossy":
            start = rng.uniform(0, 1000)
            out.append({"kind": "lossy", "start": _r(start), "end": _r(start + rng.uniform(100, 800)),
                        "drop": round(rng.uniform(0, 0.3), 3), "dup": round(rng.uniform(0, 0.2), 3),
                        "delay_max": _r(rng.uniform(0, 50))})
        elif k == "delay_spike":
            start = rng.uniform(0, 1000)
            out.append({"kind": "lossy", "start": _r(start), "end": _r(start + rng.uniform(50, 300)),
                        "drop": 0.0, "dup": round(rng.uniform(0, 0.2), 3),
                        "delay_max": _r(rng.uniform(200, 600))})
        elif k == "slow":
            start = rng.uniform(0, 1000)
            out.append({"kind": "slow", "actor": node, "start": _r(start),
                        "end": _r(start + rng.uniform(200, 1000)), "extra": _r(rng.uniform(20, 200))})
        elif k == "corrupt":
            start = rng.uniform(0, 800)
            out.append({"kind": "corrupt", "start": _r(start), "end": _r(start + rng.uniform(100, 800)),
                        "p": round(rng.uniform(0.05, 0.3), 3)})
        elif k == "suspend_coord":
            # VM suspend / live migration: the process is frozen AND its
            # monotonic clock does not advance, so on resume it still thinks
            # its lease is fresh.  This is the case fencing tokens exist for.
            out.append({"kind": "suspend", "actor": coord, "at": _r(rng.uniform(0, 900)),
                        "dur": _r(rng.uniform(200, 900))})
        elif k == "suspend_in_prepare":  # aimed at the ~15 ms windows where a prepare is in flight
            at = rng.uniform(0, 20) if rng.random() < 0.5 else rng.uniform(100, 140)
            out.append({"kind": "suspend", "actor": coord, "at": _r(at), "dur": _r(rng.uniform(300, 900))})
        elif k == "clock_jump":  # e.g. an NTP step: the coordinator's clock jumps backwards
            out.append({"kind": "clock_jump", "actor": coord, "at": _r(rng.uniform(0, 900)),
                        "by": _r(-rng.uniform(100, 600))})
        elif k == "bitrot":
            out.append({"kind": "bitrot", "actor": node, "at": _r(rng.uniform(0, 1400))})
    return out


def fault_end(f: dict) -> float:
    if "end" in f:
        return f["end"]
    return f["at"] + f.get("down", f.get("dur", 0.0))


def schedule_end(faults: list[dict]) -> float:
    return max((fault_end(f) for f in faults), default=0.0)


def uniforms(seed, src: str, dst: str, seq: int, n: int = 6) -> list[float]:
    h = hashlib.blake2b(f"{seed}|{src}|{dst}|{seq}".encode(), digest_size=4 * n).digest()
    return [int.from_bytes(h[4 * i:4 * i + 4], "big") / 2**32 for i in range(n)]


def corrupt_manifest(manifest: dict) -> dict:
    bad = dict(manifest)
    bad["max_conns"] = bad.get("max_conns", 0) + 1  # one "flipped" field
    return bad


class LinkModel:
    """Decides the fate of each message: list of delivery delays (empty =
    dropped) and whether the payload gets corrupted."""

    def __init__(self, faults: list[dict], base_latency=(0.5, 3.0)):
        self.partitions = [f for f in faults if f["kind"] == "partition"]
        self.lossy = [f for f in faults if f["kind"] == "lossy"]
        self.slow = [f for f in faults if f["kind"] == "slow"]
        self.corrupt = [f for f in faults if f["kind"] == "corrupt"]
        self.base = base_latency
        self.groups = [set(f["group"]) for f in self.partitions]

    def fate(self, t: float, src: str, dst: str, mtype: str, u: list[float]):
        for f, g in zip(self.partitions, self.groups):
            if f["start"] <= t < f["end"] and ((src in g) != (dst in g)):
                return [], False
        lo, hi = self.base
        delay = lo + (hi - lo) * u[0]
        drop = dup = 0.0
        extra = 0.0
        for f in self.lossy:
            if f["start"] <= t < f["end"]:
                drop = 1 - (1 - drop) * (1 - f["drop"])
                dup = max(dup, f["dup"])
                extra = max(extra, f["delay_max"] * u[3])
        for f in self.slow:
            if f["start"] <= t < f["end"] and f["actor"] in (src, dst):
                extra += f["extra"]
        if u[1] < drop:
            return [], False
        delays = [delay + extra]
        if u[2] < dup:
            delays.append(delay + extra + 1 + 20 * u[5])
        corrupt = False
        if mtype in ("prepare", "activate"):
            for f in self.corrupt:
                if f["start"] <= t < f["end"] and u[4] < f["p"]:
                    corrupt = True
        return delays, corrupt
