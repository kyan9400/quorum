package sim

import (
	"testing"

	"github.com/kyan9400/quorum/internal/raft"
)

func TestTheSimulatedDiskKeepsExactlyWhatReadyAskedFor(t *testing.T) {
	// The simulator's crash model is only as honest as this copy. A disk that
	// silently dropped the vote would let a restarted node vote twice in a
	// term, and random simulation alone was shown not to reach that case.
	var d durable
	d.save(raft.Ready{HardState: raft.HardState{Term: 4, Vote: 2, Commit: 1}, HardStateChanged: true,
		Entries: []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 4}, {Index: 3, Term: 4}}})
	if d.hard != (raft.HardState{Term: 4, Vote: 2, Commit: 1}) {
		t.Fatalf("hard state %+v", d.hard)
	}
	// A conflicting entry at index 2 replaces it and everything after it.
	d.save(raft.Ready{Entries: []raft.Entry{{Index: 2, Term: 5}}})
	if len(d.entries) != 2 || d.entries[1].Term != 5 {
		t.Fatalf("entries after truncation: %+v", d.entries)
	}
	// A snapshot drops what it covers.
	d.save(raft.Ready{Snapshot: &raft.Snapshot{Index: 2, Term: 5}})
	if len(d.entries) != 0 || d.snapshot.Index != 2 {
		t.Fatalf("after snapshot: %+v %+v", d.entries, d.snapshot)
	}
}

func TestTheSameSeedReplaysTheSameRun(t *testing.T) {
	// Determinism is the whole value of the simulator: a failing seed must be
	// the same failure every time it is replayed.
	cfg := Defaults(42)
	cfg.Duration = 5_000
	a, b := Run(cfg), Run(cfg)
	if a.String() != b.String() || a.Operations != b.Operations || a.Messages != b.Messages {
		t.Fatalf("seed 42 ran differently twice:\n%s\n%s", a, b)
	}
}

func TestShortSeedRangeIsSafe(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		cfg := Defaults(seed)
		cfg.Duration = 6_000
		if r := Run(cfg); !r.OK {
			t.Fatal(r)
		}
	}
}
