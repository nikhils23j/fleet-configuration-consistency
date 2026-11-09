"""Make the README figures from results.json.

Reads ONLY results.json (written by run_eval.py) and writes PNGs to docs/img/.
Every plotted value comes from that file; nothing is typed in by hand.

    python3 scripts/make_figures.py

Needs matplotlib (the protocol itself is stdlib-only; this script is just for the README).
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results.json"
OUT = ROOT / "docs" / "img"

# palette (validated for colour-blind separation; every series is also direct-labelled)
SURFACE = "#ffffff"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
STRATEGY = {  # identity colours, same in every chart
    "pinned": ("pinned 2-phase (this)", "#2a78d6"),
    "naive": ("rolling push", "#eb6834"),
    "canary": ("canary push", "#1baf7a"),
}
PROFILE = {  # fault profiles in the mutation chart
    "default": ("default (all fault kinds)", "#6250d6"),
    "lease": ("lease (suspends, slow nodes)", "#e87ba4"),
    "reorder": ("reorder (delay, dup, loss)", "#008300"),
}
INVARIANT = {
    "no_mixed_read": "I1 mixed read",
    "convergence": "I2 convergence",
    "no_resurrection": "I3 resurrection",
    "no_stranded_epoch": "I4 stranded epoch",
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 11,
    "axes.edgecolor": AXIS,
    "axes.labelcolor": INK_2,
    "axes.titlecolor": INK,
    "xtick.color": MUTED,
    "ytick.color": INK_2,
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
})


def style(ax, grid_axis="x"):
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(axis="both", length=0)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def title(fig, main, sub):
    fig.text(0.012, 0.975, main, ha="left", va="top", fontsize=15, weight="bold", color=INK)
    fig.text(0.012, 0.905, sub, ha="left", va="top", fontsize=10.5, color=INK_2)


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"wrote {path.relative_to(ROOT)} ({path.stat().st_size // 1024} KB)")


def fmt_int(n):
    return f"{n:,}"


# --------------------------------------------------------------------------- 1. mixed reads
def range_rows(ax, rows, key, unit, xmax, label_fn):
    """One row per strategy: thin min-max whisker, thick p50-p95 bar, dot at the median."""
    for y, (strategy, block) in enumerate(rows):
        name, color = STRATEGY[strategy]
        s = block[key]
        ax.plot([s["min"], s["max"]], [y, y], color=color, linewidth=2, alpha=0.45,
                solid_capstyle="round")
        ax.plot([s["p50"], s["p95"]], [y, y], color=color, linewidth=9, solid_capstyle="round")
        ax.plot([s["p50"]], [y], "o", markersize=9, color=SURFACE, markeredgecolor=color,
                markeredgewidth=2.5, zorder=3)
        ax.text(max(s["min"], 0), y - 0.3, label_fn(s, block), va="bottom",
                ha="left", fontsize=9.5, color=INK_2)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([STRATEGY[s][0] for s, _ in rows])
    ax.invert_yaxis()
    ax.set_ylim(len(rows) - 0.5, -0.85)
    ax.set_xlim(-xmax * 0.02, xmax)
    ax.set_xlabel(unit)
    style(ax)


def fig_mixed_reads(r):
    panels = [
        ("clean", f"No faults: {r['clean']['nodes']} nodes, seeds {r['clean']['seeds']}"),
        ("chaos_main", f"Seeded faults: {r['chaos_main']['nodes']} nodes, "
                       f"seeds {r['chaos_main']['seeds']} ({r['chaos_main']['profile']} profile)"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), sharey=True)
    fig.subplots_adjust(left=0.15, right=0.98, top=0.74, bottom=0.14, wspace=0.08)

    def label(s, block):
        if block["mixed_reads"] == 0:
            return f"0 of {fmt_int(block['reads'])} reads mixed"
        return f"median {s['p50']}%, p95 {s['p95']}%"

    for ax, (section, sub) in zip(axes, panels):
        rows = [(k, r[section][k]) for k in ("pinned", "naive", "canary")]
        range_rows(ax, rows, "per_run_mixed_fraction", "share of client reads per run that saw "
                   "mixed configs (%)", 100, label)
        ax.set_title(sub, loc="left", fontsize=11, color=INK, pad=10)
    title(fig, "Mixed-config reads per run: pinned vs rolling vs canary",
          "Each 5-node fan-out read either saw one (epoch, sha256) or a mix. "
          "Thin line = min to max over runs, thick bar = median to p95, ring = median.")
    save(fig, "mixed_reads.png")


# --------------------------------------------------------------------------- 2. mutations
def fig_mutations(r):
    muts = r["mutations"]
    order = sorted(muts, key=lambda m: muts[m]["caught_total"])  # biggest bar at the top
    fig, ax = plt.subplots(figsize=(12, 5.0))
    fig.subplots_adjust(left=0.2, right=0.8, top=0.78, bottom=0.14)
    gap = 2  # px-ish surface gap between stacked segments, as a white edge
    xmax = max(m["caught_total"] for m in muts.values())
    for y, name in enumerate(order):
        m = muts[name]
        left = 0
        for prof, (_, color) in PROFILE.items():
            c = m[prof]["caught"]
            if c:
                ax.barh(y, c, left=left, height=0.56, color=color, edgecolor=SURFACE,
                        linewidth=gap)
            left += c
        inv = m.get("shrunk", {}).get("invariant")
        tail = f"{m['caught_total']} / {m['runs_total']}"
        tail += f"   {INVARIANT[inv]}" if inv else "   not caught by any invariant"
        ax.text(left + xmax * 0.015, y, tail, va="center", ha="left", fontsize=10,
                color=INK if m["caught_total"] else INK_2)
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels([f"{n}" for n in order], fontsize=10.5, family="monospace")
    ax.set_xlim(0, xmax * 1.02)
    ax.set_ylim(-0.6, len(order) - 0.4)
    ax.set_xlabel(f"schedules where the checker flagged a violation "
                  f"(of {muts[order[-1]]['runs_total']}, {muts[order[-1]]['nodes']} nodes)")
    style(ax)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for _, c in PROFILE.values()]
    ax.legend(handles, [n for n, _ in PROFILE.values()], loc="lower right", frameon=False,
              fontsize=9.5, title="fault profile (300 seeds each)", title_fontsize=9.5,
              bbox_to_anchor=(1.0, 0.0))
    seeds = muts[order[-1]]["seeds_per_profile"]
    title(fig, "Mutation testing: does the checker notice when a safety layer is removed?",
          f"Each row switches off ONE mechanism and runs seeds {seeds} under each of 3 fault "
          "profiles. Right: total caught and the invariant it broke.")
    save(fig, "mutation_detection.png")


# --------------------------------------------------------------------------- 3. convergence
def fig_convergence(r):
    sec = r["chaos_main"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), sharey=True)
    fig.subplots_adjust(left=0.15, right=0.97, top=0.74, bottom=0.14, wspace=0.1)
    panels = [("converge_ms", "after rollout start"),
              ("converge_after_last_fault_ms", "after the last fault ends")]
    xmax = max(sec[k][key]["max"] for k in STRATEGY for key, _ in panels) * 1.05

    for ax, (key, sub) in zip(axes, panels):
        rows = [(k, sec[k]) for k in ("pinned", "naive", "canary")]

        def label(s, block):
            return (f"p50 {s['p50']}, max {s['max']} ms   "
                    f"({block['converged_runs']}/{block['runs']} converged)")

        range_rows(ax, rows, key, "virtual ms", xmax, label)
        ax.set_title(f"Time to converge {sub}", loc="left", fontsize=11, color=INK, pad=10)
    title(fig, f"Convergence under seeded faults ({sec['nodes']} nodes, seeds {sec['seeds']})",
          "Only runs that converged are in the ranges. Thin line = min to max, thick bar = "
          "median to p95. Timings mostly reflect bake/wave/poll settings I picked: not a speed race.")
    save(fig, "convergence.png")


def main():
    r = json.loads(RESULTS.read_text())
    fig_mixed_reads(r)
    fig_mutations(r)
    fig_convergence(r)


if __name__ == "__main__":
    main()
