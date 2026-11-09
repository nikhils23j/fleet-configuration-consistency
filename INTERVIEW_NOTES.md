# Interview notes: Fleet Config Consistency

## 1. What it is

This is a small config-rollout protocol that switches a whole fleet to a new config together,
with prepare/commit, epochs, leases and fencing tokens. During a rollout, a client request that
touches several servers never sees a mix of old and new configs. I tested it with a
deterministic simulator that runs thousands of seeded fault schedules (crashes, partitions,
frozen coordinators, corrupted messages). An invariant checker reads every recorded history, and
the same code also runs as real asyncio TCP servers on localhost. It's a personal project, built
with AI pair-programming help, all stdlib Python.

## 2. How it works

**Actors.** There's a store (lease, epoch counter, write-once decision log), two coordinators
(a leader and a hot standby), a router, and N node agents. They're all I/O-free state machines
with `on_message` / `on_timer`, and they talk to the outside world through a tiny `ctx`
(`send`, `set_timer`, `record`, `persist`).
- *Why:* the same classes run inside the simulator and over real sockets, so what I test is the
  code that actually runs over sockets, not a separate model of it.

**Rollout.**
1. Take the lease (it comes with a fencing token).
2. Allocate an epoch from the store.
3. Send `PREPARE(epoch, manifest, sha256, token)` to all nodes.
4. If *all* nodes prepare, write `commit` to the store. On timeout, write `abort` and retry
   with a new epoch.
5. The router flips once the store says the epoch is committed. Then COMMIT goes out to nodes.

- *Why all nodes, not a majority:* the router sends a request to *any* node, so every node
  has to be able to serve the new epoch. A majority isn't enough. The `commit_on_majority`
  mutation shows this.

**Version-pinned reads.** The router stamps each request with one epoch, and each node serves
*that* epoch. It can serve it from a staged copy if it prepared but hasn't heard COMMIT yet,
and it keeps the last 3 committed epochs for in-flight requests. If it doesn't have the epoch,
it says `EPOCH_MISSING` instead of falling back to whatever it has.
- *Why:* staging alone doesn't stop mixed reads. A node has to answer for a specific version.

**SHA-256 everywhere.** Nodes hash the manifest before staging, and *again before serving*,
which catches bit rot on disk.
- *Why serve-time too:* corruption can happen after the prepare check passed.

**Fencing tokens.** The store only accepts alloc/decide from the current token. Nodes reject
any token older than the highest they've seen.
- *Why:* a coordinator that was frozen (VM suspend, where the monotonic clock also stops) wakes
  up believing its lease is still valid. Its own local lease check can't save it; only the
  receiver can reject it.

**Epochs only go forward.** The store refuses to commit an epoch at or below the current one,
and nodes never activate an older epoch. A rollback is a new epoch with old content.
- *Why:* that makes "rollback never resurrects an old epoch" a simple invariant you can check.

**Anti-entropy.** Nodes poll the store every 200 ms. That fixes missed COMMITs, crashed nodes
and drifted or corrupted configs.
- *Why:* messages get lost, so the store is the source of truth and messages are only a
  fast path.

**Simulator.** A heap of `(virtual_time, seq, event)`. Each message's fate (drop, duplicate,
delay, corrupt) comes from `blake2b(seed, src, dst, per-link seq)`, not one shared RNG.
- *Why:* the same seed gives a byte-identical history, and deleting one fault while shrinking
  doesn't reshuffle every later random draw.

**Checker invariants.**
- I1 no mixed reads
- I2 convergence after faults stop
- I3 no resurrection (commits, router flips and activations only move forward, and nodes only
  activate committed epochs)
- I4 no stranded epoch (a live node never says `EPOCH_MISSING` for the latest committed epoch)

**Shrinker.** It runs ddmin over the fault list, then simplifies each fault (halve durations,
zero the drop rate, shrink partition groups), then shrinks the fleet with node IDs renumbered.
It writes out a JSON repro and replays it with a timeline.

**Real-socket mode.** Each actor is an asyncio TCP server on 127.0.0.1, using length-prefixed
JSON frames. Node state goes to JSON files (atomic rename) and gets reloaded after a crash.
Faults are injected on the sending side.

## 3. Key results (all from `python3 run_eval.py`, which wrote `results.json`; 202 s on an 8-core M2)

**500 simulated nodes, 500 seeded fault schedules:**
- pinned had **0/500** runs with any invariant violation and **0 of 55,015** client reads mixed
- on the same schedules, rolling push had 17,856/111,419 mixed reads (per-run median 23.7%) and
  canary had 22,152/91,665 (per-run median 28.0%). 38 and 14 of their runs never converged,
  because nothing repairs bit rot.
- pinned converged after rollout start at p50 1114 / p99 1723 / max 1820 virtual ms
- after the last fault ended, it converged at p50 2.8 / p99 223 / max 355 ms
- the protocol handled real trouble along the way: 143 aborted prepares, a leader change in 167
  runs, 7 stale decides fenced, 3,012 corrupted manifests rejected, and 70 bit-rot repairs
  (median 88.9 ms, max 265.5 ms)

**Wider search:** 6,000 more schedules at 50 nodes across three fault profiles, with 0
violations.

**Mutation testing.** Each mutation removes one safety layer and runs on 900 schedules:
- commit-on-majority was caught 86 times
- no anti-entropy was caught 49 times
- no read verify was caught 39 times
- no epoch monotonicity was caught 5 times
- no fencing was caught **1** time
- skip-hash-check was caught **0** times

The shrunk repros were mostly 1 fault on 2–3 nodes.

**Hand-built stale-coordinator scenario (8 seeds):** with fencing, 0 violations and 4 stale
decides fenced. Without it, 3 of 8 runs violated, with 48 stranded reads.

**Real sockets, 50 node agents:**
- 40 fault runs had 0 violations and 0/4,602 mixed reads, at a median of 1.5 s wall time per run
- 15 rolling-push runs on the same kind of faults had 546/2,489 mixed reads

**What they do NOT show:**
- Not real machines. "500 nodes" are simulated actors, and the real-socket mode is one Python
  process on loopback with 3x timeouts.
- The store is assumed linearizable and never crashes. I didn't build consensus.
- The router never crashes.
- The faults are my model with hand-picked ranges. Different ranges would give different
  detection rates.
- The baseline mixed-read percentages depend on the wave pace I chose (10% every 20 ms), and
  the convergence times mostly reflect the bake and timeout parameters I picked. They're not a
  speed comparison.
- Random search barely found the missing fence (1/900). The hand-built scenario is the real
  evidence for it.
- Zero violations means none were found in these schedules. It's not a proof. I didn't
  model-check it with TLA+.

## 4. Two-minute answer (STAR)

**Situation.**
I got curious about a pretty boring-sounding question: what actually happens when you push a
config change and half the fleet gets it and half doesn't? Nothing crashes. But if one request
fans out to five servers, two of them might use the new timeout and three the old one. A normal
rolling deploy does that on every rollout, and a failed rollout can leave you stuck there.

**Task.**
I wanted to build a rollout scheme where a client request is never served by a mix of versions,
even with crashes, partitions and a coordinator that wakes up after a long pause. I also didn't
want to just claim it worked. I wanted a way to actually test it and to show my tests could fail.

**Action.**
I wrote the protocol as plain state machines: a lease with fencing tokens, epochs, a prepare
phase where every node stages the config and checks its SHA-256, a write-once commit in a
store, and a router that pins every request to one committed epoch. I built a small
deterministic simulator, kind of like what FoundationDB does at a tiny scale, so every fault
schedule is reproducible from a seed. There's a checker for four invariants over the whole
history, and a delta-debugging shrinker that cuts a failing run down to a minimal repro. I used
AI pair-programming a lot for the plumbing. The design decisions and the debugging were where I
spent my time. The same code also runs over real TCP sockets on localhost.

**Result.**
Across 500 fault schedules on 500 simulated nodes, my protocol had zero invariant violations
and zero mixed reads out of about 55,000. Rolling push and canary had mixed reads in every
single run, around a quarter of reads at the median. The more interesting part came out of
being suspicious of that zero. I deliberately removed one safety mechanism at a time. Most got
caught and shrunk to one fault on two or three nodes. But removing fencing was caught only once
in 900 runs, and removing the hash check never, because read-time verification covered for it.
And at first, fencing never triggered at all, until I modeled a VM suspend where the clock
stops too. So my takeaways were that fault models matter as much as the code, and that a green
chaos run means nothing until you've seen it go red. The limits: the store is assumed
reliable, and it's a simulation plus one process on localhost, not a real cluster.

## 5. Likely follow-ups

**Q: Why not just use etcd/Consul watches and let nodes update themselves?**
That's basically what my store plus anti-entropy is. But a watch alone gets you
*eventual* convergence, and every node switches at a slightly different time, so you still
get mixed reads during the switch. The prepare phase plus version-pinned routing is what closes
that window. And I do assume something like etcd under the store.

**Q: Isn't requiring *all* nodes to prepare terrible for availability?**
Yes. One dead node blocks the commit, and in my runs that shows up as aborts and retries. It's a
deliberate trade: safety over liveness. A real system would take dead nodes out of the
routing set first (membership), then require all of the *remaining* members. I didn't
build membership changes.

**Q: Where exactly do fencing tokens matter, if the coordinator already checks its lease?**
They matter when the local check lies. In my case that's a VM suspend, where the
monotonic clock stops, so the process wakes up thinking 0 ms passed. My first 400 random runs
fenced zero stale decisions, because my fault model couldn't make the clock lie. In the
shrunk repro without fencing, the store accepts a commit from token 1 while token 2's leader is
mid-prepare, and the router flips to an epoch a node already discarded.

**Q: Why did skip_hash_check never get caught? Isn't that a hole in your testing?**
It's defense in depth. A corrupted manifest gets staged, but the node re-hashes before
serving, refuses with CORRUPT, and anti-entropy repairs it. So no invariant breaks; you only
see more CORRUPT errors (72 vs 39). I could add an availability invariant, but I chose to
report the honest 0 instead of tuning the checker to the result.

**Q: How do you know the simulator is deterministic?**
The eval re-runs 20 seeds and compares SHA-256 hashes of the full histories (20/20
identical), and a unit test does the same across two `PYTHONHASHSEED` values. I avoid set
iteration order, and all randomness is derived from the seed.

**Q: What does the real-socket mode prove that the simulator doesn't?**
That the same state machines survive real serialization, real TCP connection handling,
reconnects after a peer restarts, and state reloaded from files. It also showed a real
problem: event-loop stalls from opening connections and writing files made the coordinator lose its
lease during the first rollout, so I added connection warm-up and longer timeouts. What it
doesn't prove: multi-machine behaviour or real process kills. It's one process.

**Q: How does the shrinker work and how well did it do?**
ddmin over the fault list, then per-fault simplification, then a smaller fleet with node IDs
renumbered. Typical result: 4–5 faults on 20 nodes down to 1 fault on 2–3 nodes, in 4–14 sim
runs. The no_fencing repro couldn't drop any of its 3 faults, which makes sense, since it needs a
frozen leader, a frozen standby and a slow node all at once.

**Q: What would you do next?**
Replace the assumed-reliable store with a real Raft group (or run against etcd), add
membership changes, run nodes as separate OS processes with real kills, and write a TLA+ spec
of the commit rule to compare against what the simulator explored.

## 6. Things I should be able to explain cold

- [ ] Why staging alone doesn't prevent mixed reads, and why pinned routing does
- [ ] The rollout sequence: lease → alloc epoch → PREPARE (hash + token check) → all acks →
      fenced write-once decide → router flip → COMMIT → anti-entropy
- [ ] Why rollback is a *new* epoch, and what I3 "no resurrection" checks
- [ ] Fencing tokens vs leases: why a local lease check isn't enough (VM suspend, clock stops)
- [ ] The no_fencing repro timeline: commit from token 1 at t≈976 ms while token 2 is preparing
- [ ] Why all-nodes rather than majority, and what that costs (availability; membership needed)
- [ ] Why nodes keep the last 3 committed epochs (in-flight requests pinned to an older epoch)
- [ ] How per-message randomness is seeded and why that helps shrinking
- [ ] ddmin in two sentences
- [ ] The four invariants and which mutation each one caught
- [ ] Why skip_hash_check scored 0 and why I left it that way
- [ ] Every number in section 3, what it's measured over, and that sim times are virtual ms
- [ ] What's assumed: reliable store, no router crash, one process for "real" mode, no fsync
- [ ] Where AI help was used (plumbing, boilerplate, drafting) and what I verified myself
