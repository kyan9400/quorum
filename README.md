# Quorum

A replicated key-value store with Raft written from scratch in Go, standard
library only - and the machinery to show it is correct: a deterministic
simulator that runs a whole cluster under crashes, partitions and a hostile
network inside one process, and a linearizability checker that verifies every
history the clients saw.

> **Status: stage 2 of 4.** Consensus, state machine, simulator and checker;
> real processes with a write-ahead log, tested Jepsen-style and benchmarked.
> A dashboard and a deployment come next.

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

## Real processes, real faults

Three runs of a five-node cluster on Linux, each node its own `quorumd`
process with its own write-ahead log and `fsync` on every write, three
minutes each, ten clients, a fault every five seconds on average:

| run | operations | partitions | leader isolated | `kill -9` | `kill -9` the leader | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| seed 1 | 49,685 | 14 | 12 | 8 | 5 | linearizable |
| seed 2 | 52,714 | 16 | 5 | 12 | 5 | linearizable |
| seed 3 | 52,162 | 12 | 7 | 6 | 9 | linearizable |

154,561 operations through 111 faults, every history linearizable. After each
run every node was killed and restarted from disk once more, and all five came
back at the same commit index. 61 operations ended with an unknown outcome -
the client's deadline passed during a fault - and the checker allowed for both
possibilities, as it must. Summaries, fault timelines and full histories are in
`results/`.

The harness (`cmd/jepsen`) runs every node-to-node link through its own proxy,
one per direction, so a partition can be asymmetric and a cut link
black-holes rather than refusing politely. Half the clients follow the leader;
half are pinned to one node each, which forwards to the leader over the same
cuttable links.

That split exists because the first version of the harness was blind. Run
against a build that answered reads from any node's local state - the classic
stale-read bug - it passed. Its clients all followed the leader, and a client
whose write stalls leaves an isolated node within a second and a half, so
nothing ever read from the minority side. With pinned clients the same bug
fails within a minute, with the history showing a read going back in time.

**What `kill -9` does not test.** Killing a process does not kill the
operating system: data written but not yet `fsync`ed still reaches the disk.
These runs prove recovery from process crashes, not from power loss, which is
also why none of them found a torn record to repair. The torn-tail and
mid-file-damage cases are covered by the storage tests instead.

### Throughput and latency

One virtual machine with four shared vCPUs, every node on it, talking over
loopback, `fsync` on a virtual disk, 100-byte values, closed loop:

| cluster | clients | writes / s | write p50 | write p99 | reads / s | read p50 | read p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 nodes | 1 | 255 | 3.6 ms | 8.8 ms | 960 | 0.9 ms | 2.7 ms |
| 3 nodes | 16 | 1,301 | 11.4 ms | 29.2 ms | 5,225 | 2.7 ms | 8.4 ms |
| 3 nodes | 64 | 2,309 | 25.3 ms | 65.5 ms | 7,509 | 7.7 ms | 22.0 ms |
| 5 nodes | 1 | 204 | 4.5 ms | 10.7 ms | 680 | 1.3 ms | 4.6 ms |
| 5 nodes | 16 | 838 | 17.8 ms | 41.0 ms | 3,011 | 4.7 ms | 14.5 ms |
| 5 nodes | 64 | 1,442 | 42.0 ms | 84.9 ms | 5,460 | 10.7 ms | 29.2 ms |

A write is acknowledged only after a majority has written it to disk, and a
read only after a majority has confirmed the leader is still the leader - so
reads are about four times cheaper than writes, not free. Writes scale with
clients because one `fsync` covers every entry that arrived while the last one
was running. Five nodes are slower than three on this machine because all five
share its four cores; on separate machines the gap would be smaller. Treat
every figure here as a lower bound from a small shared box, not as a
comparison with production systems.

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
go test ./...                       # 34 tests
go run ./cmd/sim -seeds 2000        # the simulation
go run ./cmd/sim -seed 7            # one seed, replayed exactly

go build -o bin/ ./cmd/...
bin/jepsen -bin bin/quorumd -duration 3m      # five real processes under faults
```

A cluster by hand:

```bash
P=1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003
bin/quorumd -id 1 -listen :7001 -peers $P -data d1 &
bin/quorumd -id 2 -listen :7002 -peers $P -data d2 &
bin/quorumd -id 3 -listen :7003 -peers $P -data d3 &
curl -s localhost:7002/kv -d '{"op":"put","key":"a","value":"1","client":7,"seq":1}'
curl -s localhost:7003/kv -d '{"op":"get","key":"a"}'
```

Any node accepts requests and forwards to the leader. Writes need a client id
and a sequence number - the `client` package supplies both and retries safely.

## Plan

1. **Consensus, state machine, simulator, checker** - done.
2. **Real processes** - done: a write-ahead log, an HTTP transport, a client
   library, Jepsen-style testing and benchmarks, above.
3. **A dashboard**: the cluster's roles, terms and log positions live, and the
   history of a run with its partitions and crashes.
4. **Deployed**, with a write-up whose every number comes from a run that was
   executed.

## Licence

MIT.
