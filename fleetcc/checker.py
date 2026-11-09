"""Invariant checker over a recorded run history.

I1 no_mixed_read      every client request saw exactly one (epoch, digest), and
                      in pinned mode that digest is the store's committed digest
                      for the pinned epoch.
I2 convergence        after the faults stop, every node + the router end on the
                      final committed config (checked by the run itself).
I3 no_resurrection    store commits, router flips and each node's activations
                      only move forward, and nodes only activate committed epochs.
I4 no_stranded_epoch  a live node never answers EPOCH_MISSING for the epoch that
                      is the store's latest committed epoch at that moment.
"""
from __future__ import annotations

import bisect
from collections import Counter

INVARIANTS = ("no_mixed_read", "convergence", "no_resurrection", "no_stranded_epoch")


def check(history: list[dict], final: dict, mode: str, allowed_digests=()) -> list[dict]:
    v: list[dict] = []
    pinned = mode == "pinned"
    commits = [(e["t"], e["epoch"], e["digest"]) for e in history if e["k"] == "store_commit"]
    committed_digest = {ep: d for _, ep, d in commits}
    commit_time = {ep: t for t, ep, _ in commits}
    commit_ts = [t for t, _, _ in commits]
    allowed = set(allowed_digests) | set(committed_digest.values())

    # I3: store commits strictly increase
    for (_, a, _), (t, b, _) in zip(commits, commits[1:]):
        if b <= a:
            v.append({"inv": "no_resurrection", "t": t, "detail": f"store committed {b} after {a}"})

    last_router, last_node = 1, {}
    for e in history:
        k = e["k"]
        if k == "router_flip":
            if e["epoch"] <= last_router:
                v.append({"inv": "no_resurrection", "t": e["t"],
                          "detail": f"router went {last_router} -> {e['epoch']}"})
            last_router = e["epoch"]
        elif k == "node_activate":
            prev = last_node.get(e["node"], 1)
            if e["epoch"] <= prev:
                v.append({"inv": "no_resurrection", "t": e["t"],
                          "detail": f"{e['node']} activated {e['epoch']} after {prev}"})
            last_node[e["node"]] = e["epoch"]
            if pinned and (e["epoch"] not in commit_time or commit_time[e["epoch"]] > e["t"]):
                v.append({"inv": "no_resurrection", "t": e["t"],
                          "detail": f"{e['node']} activated uncommitted epoch {e['epoch']}"})
        elif k == "read":
            seen = {(ep, d) for _, ep, d, _ in e["ok"]}
            if len(seen) > 1:
                v.append({"inv": "no_mixed_read", "t": e["t0"], "detail": f"req {e['req']} saw {sorted(seen)}"})
            elif seen:
                ep, d = next(iter(seen))
                good = committed_digest.get(e["epoch"]) == d if pinned else d in allowed
                if pinned and ep != e["epoch"]:
                    good = False
                if not good:
                    v.append({"inv": "no_mixed_read", "t": e["t0"],
                              "detail": f"req {e['req']} pinned {e['epoch']} got ({ep}, {d[:12]})"})
            if pinned:
                for node, reason, t in e["err"]:
                    if reason != "EPOCH_MISSING":
                        continue
                    i = bisect.bisect_right(commit_ts, t) - 1
                    if i >= 0 and commits[i][1] == e["epoch"]:
                        v.append({"inv": "no_stranded_epoch", "t": t,
                                  "detail": f"{node} missing committed epoch {e['epoch']}"})
    if final.get("converged_at") is None:
        v.append({"inv": "convergence", "t": final.get("end_ms"),
                  "detail": f"not converged; lagging={final.get('lagging')}"})
    return v


def summarize(history: list[dict], final: dict, violations: list[dict]) -> dict:
    reads = [e for e in history if e["k"] == "read"]
    mixed = sum(1 for e in reads if len({(ep, d) for _, ep, d, _ in e["ok"]}) > 1)
    errs = Counter(r for e in reads for _, r, _ in e["err"])
    lost = sum(len(e["lost"]) for e in reads)
    kinds = Counter(e["k"] for e in history)
    start = next((e["t"] for e in history if e["k"] == "prepare_start"), 0.0)
    bitrots = {e["a"]: e["t"] for e in history if e["k"] == "bitrot"}
    repairs = []
    for e in history:
        if e["k"] == "drift_repaired" and e["node"] in bitrots:
            repairs.append(e["t"] - bitrots.pop(e["node"]))
    conv = final.get("converged_at")
    return {
        "reads": len(reads),
        "mixed_reads": mixed,
        "read_errors": dict(errs),
        "read_timeouts": lost,
        "commits": (kinds["store_commit"] - 1) or kinds["coord_committed"],
        "aborts": kinds["store_abort"],
        "leader_terms": kinds["lease_granted"],
        "fenced_decides": kinds["decide_fenced"],
        "hash_rejects": kinds["hash_reject"],
        "violations": dict(Counter(x["inv"] for x in violations)),
        "converge_ms": None if conv is None else round(conv - start, 2),
        "converge_after_heal_ms": None if conv is None else round(conv - max(final.get("fault_end", 0), start), 2),
        "drift_repair_ms": [round(x, 2) for x in repairs],
    }
