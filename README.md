# Quorum

A replicated key-value store with Raft written from scratch in Go, standard
library only - and the machinery to show it is correct: a deterministic
simulator that runs a whole cluster under crashes, partitions and a hostile
network inside one process, and a linearizability checker that verifies every
history the clients saw.

> **Status: stage 1 of 4.** The consensus core, the state machine, the
> simulator and the checker are done. Stage 2 runs it as real processes with a
> write-ahead log on disk, under Jepsen-style fault injection.

## The result so far

```
2000 seeds, 0 failed, 1587823 client operations checked (7509 with unknown outcome)
faults survived: 78099 crashes, 77959 partitions, 34507 elected leaders,
                 19386 snapshots installed, 3137305 of 38964741 messages dropped
```

Two thousand runs of a five-node cluster, each 30 virtual seconds of five
clients reading, writing, appending and compare-and-setting, while nodes
crash and restart, the network partitions and heals, and messages are dropped,
delayed, duplicated and reordered. Every history every client observed was
linearizable, no index was ever applied with different contents on two nodes,
and no term ever had two leaders. On a laptop, 75 seconds.

## How it is built

**Raft as a pure state machine.** The core (`internal/raft`) has no clock, no
network and no disk. Time arrives as `Tick`, messages as `Step`, and
everything the node wants done - entries to persist, messages to send,
committed entries to apply - comes out of `Ready`. The caller does those, in
that order, and calls `Advance`. It is how etcd's raft library is built, and
for the same reason: a node driven only by its inputs behaves identically
every time it gets the same inputs.

It implements leader election, log replication and commitment as in Figure 2
of the paper, a no-op entry on election so a new leader can commit, log
compaction with snapshots sent to followers that fall behind them, rejection
hints that skip a whole conflicting term per round trip, and **ReadIndex** for
linearizable reads that do not go through the log: the leader records its
commit index and serves the read only after a majority has answered a
heartbeat sent after the read arrived - proof it was still leader at that
moment. It does not implement membership changes.

**Exactly-once writes** (`internal/kv`). A client that times out cannot know
whether its write was applied, so it retries, and the retry may be committed
too. Every command carries a client id and sequence number, and the state
machine answers a duplicate with the original result instead of applying it
again. The table is part of every snapshot, so a duplicate arriving after a
compaction is still recognised.

**A deterministic simulator** (`internal/sim`). The whole cluster runs in one
process on virtual time, and everything that varies comes from one seed:
delays, losses, duplicates, stalls, which node crashes when, when partitions
start and heal, what the clients ask for. A crash discards everything a node
had not persisted; the simulator keeps its own copy of exactly what each
`Ready` asked to be written, and a restarted node is rebuilt from that alone.
So a failure on seed 81233 is a failure anyone can replay:

```bash
go run ./cmd/sim -seed 81233
```

**A linearizability checker** (`internal/check`). Wing and Gong's search with
Lowe's memoisation, as in Jepsen's Knossos and the Porcupine library. An
operation whose client timed out may have taken effect at any point after it
started, or never, and the checker allows both: counting a timeout as a
failure would accept a store that applied a write it told nobody about, and
counting it as a success would reject a store that correctly dropped it. When
a history is not linearizable it reports the longest valid prefix and the
operations that could not follow it.

## Do the tests catch anything?

A suite that passes two thousand seeds on code written days ago is as likely
to be blind as right. So five changes known to break Raft were made on
purpose, one at a time, and run against both the simulator and the tests:

| deliberately broken | simulation, 500 seeds | tests |
| --- | --- | --- |
| election restriction removed: votes go to shorter logs | caught | caught |
| ReadIndex served without confirming leadership | caught (21 of 500 seeds) | caught |
| exactly-once removed: retries applied twice | caught (500 of 500) | caught |
| Figure 8 rule removed: old terms committed by counting | **missed** | caught |
| vote not persisted: a restarted node votes twice | **missed** | caught |

Random simulation found three of the five. The two it missed both need a
precise sequence of five or six events - a node crashing and coming back
inside one election, an old term's entry reaching a majority just before the
new leader's own - and making the simulation harsher did not reach them:
per-seed fault intensity from mild to storm, network stalls of up to 1.5 s,
replication batches of one entry, more duplicates. Each widened what it
explores, and none found these two. So both are covered by scripted scenario
tests that drive the cluster into exactly that situation, the way etcd tests
its classic cases.

Writing those exposed two tests of my own that could not fail. The first
version of the Figure 8 test passed with the rule deleted: the leader only
ever heard back from one follower, so it knew of two copies and could not
commit with or without the rule. Its corrected version checks its own setup
before it asserts anything.

## Running it

```bash
go test ./...                       # 22 tests, a few seconds
go run ./cmd/sim -seeds 2000        # the simulation
go run ./cmd/sim -seed 7            # one seed, replayed exactly
```

## Plan

1. **Consensus, state machine, simulator, checker** - done.
2. **Real processes**: a write-ahead log with fsync, a network transport, a
   client library, and Jepsen-style tests that kill processes and cut the
   network between them, checked by the same linearizability checker; plus
   throughput and latency on real hardware.
3. **A dashboard**: the cluster's roles, terms and log positions live, and the
   history of a run with its partitions and crashes.
4. **Deployed**, with a write-up whose every number comes from a run that was
   executed.

## Licence

MIT.
