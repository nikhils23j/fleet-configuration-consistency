"""Run many seeded fault schedules, check invariants, and shrink failures."""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor

from .checker import check, summarize
from .faults import gen_schedule
from .protocol import BASE, DEFAULT_PLAN, digest, node_name
from .sim import RunConfig, Sim

ALLOWED = tuple(digest(m) for m in (BASE, *DEFAULT_PLAN))


def run_checked(cfg: RunConfig) -> tuple[Sim, list[dict]]:
    sim = Sim(cfg).run()
    final = sim.final_state()
    return sim, check(sim.history, final, cfg.mode, ALLOWED)


def run_summary(cfg: RunConfig) -> dict:
    sim, viol = run_checked(cfg)
    s = summarize(sim.history, sim.final_state(), viol)
    s.update(seed=cfg.seed, mode=cfg.mode, n_nodes=cfg.n_nodes, mutations=list(cfg.mutations),
             n_faults=len(cfg.faults), fault_kinds=sorted({f["kind"] for f in cfg.faults}),
             first_violation=viol[0] if viol else None, events=sim.stats["events"],
             msgs=sim.stats["sent"], dropped=sim.stats["dropped"])
    return s


def make_cfg(seed: int, n_nodes: int, mode: str, mutations=(), faulty: bool = True, max_ms=15000.0,
             profile: str = "default") -> RunConfig:
    faults = gen_schedule(seed, n_nodes, profile=profile) if faulty else []
    return RunConfig(n_nodes=n_nodes, seed=seed, mode=mode, mutations=tuple(mutations), faults=faults,
                     max_ms=max_ms)


def run_many(cfgs: list[RunConfig], workers: int | None = None) -> list[dict]:
    workers = workers or os.cpu_count() or 1
    if workers == 1 or len(cfgs) < 4:
        return [run_summary(c) for c in cfgs]
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(run_summary, cfgs, chunksize=max(1, len(cfgs) // (workers * 8))))


# ------------------------------------------------------------------ shrinking
def _fails(cfg: RunConfig, inv: str) -> bool:
    _, viol = run_checked(cfg)
    return any(x["inv"] == inv for x in viol)


def _ddmin(items: list, test) -> list:
    """Classic delta debugging: smallest subset (1-minimal) that still fails."""
    n = 2
    while len(items) >= 2:
        chunk = max(1, len(items) // n)
        subsets = [items[i:i + chunk] for i in range(0, len(items), chunk)]
        progressed = False
        for i in range(len(subsets)):
            complement = [x for j, s in enumerate(subsets) if j != i for x in s]
            if test(complement):
                items, n, progressed = complement, max(n - 1, 2), True
                break
        if not progressed:
            if n >= len(items):
                break
            n = min(len(items), n * 2)
    if len(items) == 1 and test([]):
        return []
    return items


def _simplify_fault(f: dict):
    """Yield simpler variants of one fault (shorter, gentler, smaller)."""
    for key in ("down", "dur", "by"):
        if key in f and abs(f[key]) > 10:
            yield dict(f, **{key: round(f[key] / 2, 1)})
    if "end" in f and f["end"] - f["start"] > 10:
        yield dict(f, end=round(f["start"] + (f["end"] - f["start"]) / 2, 1))
    if f["kind"] == "partition" and len(f["group"]) > 1:
        g = f["group"]
        yield dict(f, group=g[: len(g) // 2])
        yield dict(f, group=g[len(g) // 2:])
    if f["kind"] == "lossy":
        for key in ("drop", "dup"):
            if f[key] > 0:
                yield dict(f, **{key: 0.0})
        if f["delay_max"] > 1:
            yield dict(f, delay_max=round(f["delay_max"] / 2, 1))


def _nodes_used(faults) -> list[str]:
    used = set()
    for f in faults:
        for a in [f.get("actor")] + list(f.get("group", [])):
            if a and a.startswith("n"):
                used.add(a)
    return sorted(used, key=lambda a: int(a[1:]))


def _remap(faults, mapping):
    out = []
    for f in faults:
        g = dict(f)
        if "actor" in g:
            g["actor"] = mapping.get(g["actor"], g["actor"])
        if "group" in g:
            g["group"] = [mapping.get(a, a) for a in g["group"]]
        out.append(g)
    return out


def shrink(cfg: RunConfig, inv: str | None = None, budget: int = 300) -> tuple[RunConfig, dict]:
    """Shrink a failing config to a small repro of the same invariant."""
    if inv is None:
        _, viol = run_checked(cfg)
        if not viol:
            raise ValueError("config does not fail")
        inv = viol[0]["inv"]
    runs = [0]

    def test(c: RunConfig) -> bool:
        if runs[0] >= budget:
            return False
        runs[0] += 1
        return _fails(c, inv)

    start_faults, start_nodes = len(cfg.faults), cfg.n_nodes
    best = cfg
    faults = _ddmin(list(best.faults), lambda fs: test(RunConfig(**{**best.__dict__, "faults": fs})))
    best = RunConfig(**{**best.__dict__, "faults": faults})
    improved = True
    while improved and runs[0] < budget:
        improved = False
        for i, f in enumerate(best.faults):
            for g in _simplify_fault(f):
                cand = RunConfig(**{**best.__dict__, "faults": best.faults[:i] + [g] + best.faults[i + 1:]})
                if test(cand):
                    best, improved = cand, True
                    break
            if improved:
                break
    # shrink the fleet: renumber the nodes the faults touch to n0..nk, then try smaller fleets
    used = _nodes_used(best.faults)
    mapping = {a: node_name(i) for i, a in enumerate(used)}
    for n in sorted({max(len(used), 2), 3, 5, 8, 12, 20, 30}):
        if n >= best.n_nodes or n < len(used):
            continue
        cand = RunConfig(**{**best.__dict__, "n_nodes": n, "faults": _remap(best.faults, mapping)})
        if test(cand):
            best = cand
            break
    info = {"invariant": inv, "faults_before": start_faults, "faults_after": len(best.faults),
            "nodes_before": start_nodes, "nodes_after": best.n_nodes, "sim_runs": runs[0]}
    return best, info
