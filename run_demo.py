"""Reproducible 500-node, 100-fault evaluation for the fleet model."""
import argparse
import asyncio
import json
import platform
import statistics
from pathlib import Path

from fleet import Fleet, Manifest, detect_drift, naive_rollout_mixed_samples


async def evaluate(size: int = 500, fault_runs: int = 100) -> dict:
    initial = Manifest(0, {"risk_limit": 100, "route": "primary"})
    target = Manifest(1, {"risk_limit": 80, "route": "primary"})
    counts = {"reject": 0, "corrupt": 0, "timeout": 0}
    mixed_samples = 0
    for run in range(fault_runs):
        fleet = Fleet(size, initial)
        mode = ("reject", "corrupt", "timeout")[run % 3]
        fleet.nodes[(run * 37) % size].failure = mode
        result = await fleet.rollout(target)
        assert not result.committed, f"faulty run {run} committed"
        assert fleet.routed.version == 0, f"faulty run {run} changed route"
        assert fleet.served_versions() == {0}
        counts[mode] += 1
        mixed_samples += result.mixed_samples

    successful = Fleet(size, initial)
    success_result = await successful.rollout(target)
    assert success_result.committed
    assert successful.served_versions() == {1}
    mixed_samples += success_result.mixed_samples

    latencies_ms = []
    for run in range(20):
        fleet = Fleet(size, target)
        task = asyncio.create_task(detect_drift(fleet, interval=0.01, limit=2.0))
        await asyncio.sleep(0.02)
        loop = asyncio.get_running_loop()
        start = loop.time()
        node_id = (run * 37) % size
        fleet.nodes[node_id].active = Manifest(1, {"risk_limit": 999, "route": "primary"})
        found = await task
        latency_ms = (loop.time() - start) * 1000
        assert node_id in found, f"drift missed in run {run}"
        latencies_ms.append(latency_ms)

    return {
        "environment": {"python": platform.python_version(), "system": platform.platform()},
        "workload": {"logical_nodes": size, "fault_runs": fault_runs, "drift_trials": len(latencies_ms)},
        "fault_modes": counts,
        "aborted_fault_runs": sum(counts.values()),
        "successful_clean_rollout": success_result.committed,
        "mixed_served_version_samples": mixed_samples,
        "naive_sequential_rollout_mixed_samples": naive_rollout_mixed_samples(size),
        "drift_detected_trials": len(latencies_ms),
        "drift_detection_ms": {
            "max": round(max(latencies_ms), 2),
            "median": round(statistics.median(latencies_ms), 2),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=500)
    parser.add_argument("--fault-runs", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("results.json"))
    args = parser.parse_args()
    result = asyncio.run(evaluate(args.nodes, args.fault_runs))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
