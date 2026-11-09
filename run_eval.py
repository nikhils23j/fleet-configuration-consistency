"""Full evaluation.  Every number in README.md comes from the results.json this writes.

    python3 run_eval.py                 # full run (202 s recorded on an 8-core M2)
    python3 run_eval.py --quick         # small smoke version (~1 min)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

from fleetcc.chaos import ALLOWED, make_cfg, run_checked, run_many, shrink
from fleetcc.checker import check, summarize
from fleetcc.protocol import MUTATIONS, Params
from fleetcc.sim import RunConfig

HERE = Path(__file__).resolve().parent


def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 1)


def dist(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"n": len(xs), "min": round(min(xs), 1), "p50": pct(xs, 0.5), "p95": pct(xs, 0.95),
            "p99": pct(xs, 0.99), "max": round(max(xs), 1), "mean": round(statistics.mean(xs), 1)}


def aggregate(res: list[dict]) -> dict:
    reads = sum(r["reads"] for r in res)
    mixed = sum(r["mixed_reads"] for r in res)
    per_run_mixed_frac = [r["mixed_reads"] / r["reads"] for r in res if r["reads"]]
    return {
        "runs": len(res),
        "runs_with_violation": sum(bool(r["violations"]) for r in res),
        "violations_by_invariant_runs": dict(Counter(k for r in res for k in r["violations"])),
        "reads": reads,
        "mixed_reads": mixed,
        "mixed_read_fraction": round(mixed / reads, 4) if reads else None,
        "runs_with_mixed_read": sum(r["mixed_reads"] > 0 for r in res),
        "per_run_mixed_fraction": dist([100 * x for x in per_run_mixed_frac]),
        "read_errors": dict(sum((Counter(r["read_errors"]) for r in res), Counter())),
        "read_timeouts": sum(r["read_timeouts"] for r in res),
        "converged_runs": sum(r["converge_ms"] is not None for r in res),
        "converge_ms": dist([r["converge_ms"] for r in res]),
        "converge_after_last_fault_ms": dist([r["converge_after_heal_ms"] for r in res]),
        "aborted_prepares": sum(r["aborts"] for r in res),
        "runs_with_leader_change": sum(r["leader_terms"] > 1 for r in res),
        "fenced_stale_decides": sum(r["fenced_decides"] for r in res),
        "hash_rejects": sum(r["hash_rejects"] for r in res),
        "drift_repair_ms": dist([x for r in res for x in r["drift_repair_ms"]]),
        "fault_kind_runs": dict(Counter(k for r in res for k in r["fault_kinds"])),
    }


def history_hash(cfg: RunConfig) -> str:
    sim, _ = run_checked(cfg)
    return hashlib.sha256(json.dumps(sim.history, sort_keys=True).encode()).hexdigest()


STALE_COORD_SCENARIO = [  # hand-built: whichever coordinator leads gets frozen mid-prepare
    {"kind": "suspend", "actor": "coord", "at": 8, "dur": 600},
    {"kind": "suspend", "actor": "coordb", "at": 8, "dur": 600},
    {"kind": "slow", "actor": "n0", "start": 250, "end": 1200, "extra": 200},
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--output", type=Path, default=HERE / "results.json")
    ap.add_argument("--skip-net", action="store_true")
    a = ap.parse_args()
    q = a.quick
    t_start = time.time()
    out: dict = {
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "machine": platform.machine(), "cpu_count": os.cpu_count()},
        "command": " ".join([Path(sys.executable).name] + sys.argv),
        "params_sim": {k: v for k, v in Params().__dict__.items() if k != "mutations"},
        "notes": "All times are virtual milliseconds in the simulator and wall-clock milliseconds in "
                 "real-socket mode. Each sim run rolls out v2 and then rolls back (new epoch), "
                 "while the router issues a 5-node fan-out read every 10 ms.",
    }

    def stage(name):
        print(f"[{time.time() - t_start:6.1f}s] {name}", flush=True)

    # 1. determinism
    stage("determinism")
    seeds = range(5 if q else 20)
    same = [history_hash(make_cfg(s, 50, "pinned")) == history_hash(make_cfg(s, 50, "pinned")) for s in seeds]
    out["determinism"] = {"seeds": len(same), "identical_histories": sum(same)}

    # 2. clean rollouts at 500 nodes
    stage("clean")
    n_clean = 5 if q else 20
    big = 100 if q else 500
    out["clean"] = {"nodes": big, "seeds": f"0..{n_clean - 1}"}
    for mode in ("pinned", "naive", "canary"):
        out["clean"][mode] = aggregate(run_many([make_cfg(s, big, mode, faulty=False) for s in range(n_clean)]))

    # 3. chaos at 500 nodes, same schedules for all three modes
    stage("chaos_500")
    n_chaos = 40 if q else 500
    out["chaos_main"] = {"nodes": big, "profile": "default", "seeds": f"0..{n_chaos - 1}"}
    for mode in ("pinned", "naive", "canary"):
        stage(f"  chaos_500 {mode}")
        out["chaos_main"][mode] = aggregate(run_many([make_cfg(s, big, mode) for s in range(n_chaos)]))

    # 4. wide search: many more schedules, smaller fleet, all fault profiles, correct protocol
    stage("wide search")
    n_wide = 100 if q else 2000
    out["wide_search"] = {"nodes": 50, "seeds_per_profile": f"0..{n_wide - 1}"}
    for prof in ("default", "lease", "reorder"):
        stage(f"  wide {prof}")
        out["wide_search"][prof] = aggregate(run_many([make_cfg(s, 50, "pinned", profile=prof)
                                                        for s in range(n_wide)]))

    # 5. mutation testing: switch one safety layer off at a time
    stage("mutations")
    n_mut = 50 if q else 300
    muts = {}
    repro_dir = HERE / "repros"
    repro_dir.mkdir(exist_ok=True)
    for m in MUTATIONS:
        entry = {"what": MUTATIONS[m], "nodes": 20, "seeds_per_profile": f"0..{n_mut - 1}"}
        first_bad = None
        for prof in ("default", "lease", "reorder"):
            cfgs = [make_cfg(s, 20, "pinned", [m], profile=prof, max_ms=8000.0) for s in range(n_mut)]
            res = run_many(cfgs)
            bad = [r for r in res if r["violations"]]
            entry[prof] = {"runs": len(res), "caught": len(bad),
                           "invariants": dict(Counter(k for r in bad for k in r["violations"])),
                           "read_errors": dict(sum((Counter(r["read_errors"]) for r in res), Counter()))}
            if bad and first_bad is None:
                first_bad = next(c for c in cfgs if c.seed == bad[0]["seed"])
        entry["caught_total"] = sum(entry[p]["caught"] for p in ("default", "lease", "reorder"))
        entry["runs_total"] = 3 * n_mut
        if first_bad is not None:
            small, info = shrink(first_bad)
            entry["shrunk"] = info
            path = repro_dir / f"{m}.json"
            path.write_text(json.dumps({"config": small.to_json(), "shrink": info}, indent=1) + "\n")
            entry["repro"] = f"python3 -m fleetcc replay repros/{m}.json"
        muts[m] = entry
        stage(f"  {m}: caught {entry['caught_total']}/{entry['runs_total']}")
    out["mutations"] = muts

    # 6. the hand-built stale coordinator scenario, with and without fencing
    stage("stale coordinator scenario")
    scen = {}
    for label, mm in (("fencing_on", ()), ("no_fencing", ("no_fencing",))):
        rows = []
        for seed in range(8):
            cfg = RunConfig(n_nodes=20, seed=seed, mutations=mm, faults=STALE_COORD_SCENARIO)
            sim, viol = run_checked(cfg)
            s = summarize(sim.history, sim.final_state(), viol)
            rows.append(s)
        scen[label] = {"seeds": "0..7", "runs_with_violation": sum(bool(r["violations"]) for r in rows),
                       "fenced_stale_decides": sum(r["fenced_decides"] for r in rows),
                       "violations": dict(sum((Counter(r["violations"]) for r in rows), Counter()))}
    scen["faults"] = STALE_COORD_SCENARIO
    out["stale_coordinator_scenario"] = scen

    # 7. real sockets
    if not a.skip_net:
        from fleetcc.net import NET_PARAMS, run_net
        stage("real sockets")
        n_net, n_net_naive, n_net_clean = (5, 3, 2) if q else (40, 15, 5)
        nodes = 50
        net = {"nodes": nodes, "params_scale": 3.0,
               "params": {k: v for k, v in NET_PARAMS.__dict__.items() if k != "mutations"}}
        for label, mode, seeds, faulty in (("pinned_clean", "pinned", range(n_net_clean), False),
                                           ("pinned_chaos", "pinned", range(n_net), True),
                                           ("naive_chaos", "naive", range(n_net_naive), True)):
            rows, walls, errors = [], [], []
            for s in seeds:
                cfg = make_cfg(s, nodes, mode, faulty=faulty, max_ms=8000.0)
                rt, wall = run_net(cfg)
                final = rt.final_state()
                viol = check(rt.history, final, mode, ALLOWED)
                r = summarize(rt.history, final, viol)
                r.update(seed=s, fault_kinds=sorted({f["kind"] for f in cfg.faults}))
                rows.append(r)
                walls.append(wall)
                errors += final["errors"]
            agg = aggregate(rows)
            agg["seeds"] = f"0..{len(seeds) - 1}"
            agg["wall_s_per_run"] = dist(walls)
            agg["handler_exceptions"] = len(errors)
            net[label] = agg
            stage(f"  net {label}: {agg['runs_with_violation']} runs with violations")
        out["real_sockets"] = net

    out["eval_wall_seconds"] = round(time.time() - t_start, 1)
    a.output.write_text(json.dumps(out, indent=1) + "\n")
    stage(f"wrote {a.output}")


if __name__ == "__main__":
    main()
