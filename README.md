# Fleet Configuration Consistency

**Status:** New portfolio simulation built for the AlphaGrep general technology submission on 28 October 2026. It is separate from Nikhil's historical course projects and does not use employer systems or data.

## Why this exists

A failed rollout can leave servers on different configuration versions. The simulator tests a version-pinned gateway: every logical node keeps serving the current manifest while it prepares a new one; the gateway changes its routed epoch only after all nodes acknowledge the exact content hash. A rejected, corrupted, or timed-out prepare aborts and discards staged content. A scanner detects nodes whose active hash drifts from the routed manifest.

## Run

Python 3.10+ and standard library only. From this directory:

```sh
python3 -m unittest -v
python3 run_demo.py --nodes 500 --fault-runs 100 --output results.json
```

`run_demo.py` asserts every invariant before writing results. It runs 100 deterministic fault scenarios (rejection, content corruption, timeout), one clean rollout, and 20 real-time drift-detection trials. The JSON records the environment, counts, mixed-version samples, and measured detection latency.
It also counts mixed-version observation points during a naive sequential 500-node rollout as a comparison. This baseline assumes clients can reach each node directly; the version-pinned gateway removes that exposure in the model.

## Design decisions and limits

- Manifests use canonical JSON and SHA-256. Nodes stage an immutable manifest and return its hash; the coordinator compares every acknowledgment with the target hash.
- The gateway routes one epoch at a time. Staged nodes retain the old version, so prepare failures do not disrupt the current route. On a successful rollout, the single-process gateway flips its epoch after all nodes prepare.
- This is a **logical, single-process simulation** of 500 nodes, not 500 servers or a distributed consensus protocol. Its atomic gateway switch and reliable coordinator are model assumptions. A real deployment would need durable coordinator state, network-partition handling, authentication, and a production traffic gateway.
- The drift monitor polls local object hashes, not remote machines. Detection latency is measured on this laptop and depends on the 10 ms polling interval and scheduler.
- The sample configuration uses generated risk-limit and route fields only; it does not connect to a trading system. Measured simulation results must not be described as production impact.
