"""Command line: python3 -m fleetcc {sim,chaos,replay,net} ..."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter

from .chaos import ALLOWED, make_cfg, run_checked, run_many, shrink
from .checker import check, summarize
from .faults import PROFILES
from .protocol import MUTATIONS
from .sim import MODES, RunConfig

TIMELINE_KINDS = {"lease_granted", "coord_leader", "coord_step_down", "store_alloc", "prepare_start",
                  "prepare_timeout", "store_commit", "store_abort", "decide_fenced", "router_flip",
                  "crash", "restart", "pause", "bitrot", "plan_done", "hash_reject", "drift_repaired",
                  "node_fetched"}


def _print_summary(cfg, sim, viol):
    s = summarize(sim.history, sim.final_state(), viol)
    keep = ("reads", "mixed_reads", "read_errors", "commits", "aborts", "leader_terms", "fenced_decides",
            "violations", "converge_ms", "converge_after_heal_ms")
    print(json.dumps({k: s[k] for k in keep}, indent=1))


def _timeline(sim, viol, window=400.0, limit=60):
    t = viol[0]["t"] if viol else None
    rows = [e for e in sim.history if e["k"] in TIMELINE_KINDS
            and (t is None or t - window <= e["t"] <= t + 50)]
    for e in rows[-limit:]:
        extra = {k: v for k, v in e.items() if k not in ("k", "t", "a", "digest")}
        print(f"  {e['t']:9.1f}  {e['a']:<7} {e['k']:<16} {extra if extra else ''}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="fleetcc")
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--nodes", type=int, default=50)
    common.add_argument("--mode", choices=MODES, default="pinned")
    common.add_argument("--mutation", action="append", default=[], choices=sorted(MUTATIONS))
    common.add_argument("--profile", choices=sorted(PROFILES), default="default")

    p = sub.add_parser("sim", parents=[common], help="run one seeded schedule in the simulator")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--clean", action="store_true", help="no faults")
    p.add_argument("--timeline", action="store_true")

    p = sub.add_parser("chaos", parents=[common], help="run many schedules, shrink the first failure")
    p.add_argument("--runs", type=int, default=200)
    p.add_argument("--first-seed", type=int, default=0)
    p.add_argument("--shrink", action="store_true")
    p.add_argument("--out", default="repro.json")

    p = sub.add_parser("replay", help="re-run a saved repro and print the timeline")
    p.add_argument("file")

    p = sub.add_parser("net", parents=[common], help="run one schedule on real localhost TCP sockets")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--clean", action="store_true")

    a = ap.parse_args(argv)
    if a.cmd == "sim":
        cfg = make_cfg(a.seed, a.nodes, a.mode, a.mutation, faulty=not a.clean, profile=a.profile)
        print("faults:", json.dumps(cfg.faults))
        sim, viol = run_checked(cfg)
        _print_summary(cfg, sim, viol)
        for v in viol[:5]:
            print("VIOLATION", v)
        if a.timeline or viol:
            _timeline(sim, viol)
        return 1 if viol else 0
    if a.cmd == "chaos":
        cfgs = [make_cfg(s, a.nodes, a.mode, a.mutation, profile=a.profile)
                for s in range(a.first_seed, a.first_seed + a.runs)]
        res = run_many(cfgs)
        bad = [r for r in res if r["violations"]]
        print(f"{len(res)} schedules, {len(bad)} with violations:",
              dict(Counter(k for r in bad for k in r["violations"])))
        if bad and a.shrink:
            cfg = next(c for c in cfgs if c.seed == bad[0]["seed"])
            small, info = shrink(cfg)
            print("shrunk:", info)
            with open(a.out, "w") as f:
                json.dump({"config": small.to_json(), "shrink": info}, f, indent=1)
            print(f"wrote {a.out}; replay with: python3 -m fleetcc replay {a.out}")
        return 1 if bad else 0
    if a.cmd == "replay":
        with open(a.file) as f:
            data = json.load(f)
        cfg = RunConfig.from_json(data.get("config", data))
        print("config:", json.dumps(cfg.to_json()))
        sim, viol = run_checked(cfg)
        _print_summary(cfg, sim, viol)
        for v in viol[:5]:
            print("VIOLATION", v)
        _timeline(sim, viol)
        return 1 if viol else 0
    if a.cmd == "net":
        from .net import run_net
        cfg = make_cfg(a.seed, a.nodes, a.mode, a.mutation, faulty=not a.clean, profile=a.profile)
        print("faults:", json.dumps(cfg.faults))
        rt, wall = run_net(cfg)
        final = rt.final_state()
        viol = check(rt.history, final, cfg.mode, ALLOWED)
        s = summarize(rt.history, final, viol)
        print(json.dumps({"wall_s": round(wall, 2), "errors": final["errors"], **{
            k: s[k] for k in ("reads", "mixed_reads", "read_errors", "commits", "aborts", "violations",
                              "converge_ms")}, "net": rt.stats}, indent=1))
        return 1 if viol or final["errors"] else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
