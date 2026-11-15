package raft

import (
	"testing"
)

// net is a hand-driven network for scripted scenarios: messages wait in an
// outbox until a test delivers them, drops them, or lets everything flow.
type net struct {
	t     *testing.T
	nodes map[uint64]*Node
	box   []Message
	down  map[uint64]bool
	reads map[uint64][]ReadState // confirmed reads, by node
}

func newNet(t *testing.T, restores map[uint64]Restore, maxBatch int) *net {
	t.Helper()
	ids := make([]uint64, 0, len(restores))
	for id := uint64(1); id <= uint64(len(restores)); id++ {
		ids = append(ids, id)
	}
	n := &net{t: t, nodes: map[uint64]*Node{}, down: map[uint64]bool{}, reads: map[uint64][]ReadState{}}
	for _, id := range ids {
		n.nodes[id] = New(Config{ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 1, Seed: 1, MaxEntriesPerMsg: maxBatch}, restores[id])
	}
	return n
}

// collect moves every node's outgoing messages into the outbox, as if each
// Ready had been persisted.
func (n *net) collect() {
	for id := uint64(1); id <= uint64(len(n.nodes)); id++ {
		nd := n.nodes[id]
		rd := nd.Ready()
		if !n.down[id] {
			n.box = append(n.box, rd.Messages...)
		}
		n.reads[id] = append(n.reads[id], rd.ReadStates...)
		nd.Advance(rd)
	}
}

// deliver hands over queued messages that match keep, repeatedly, until none
// are left that match; the rest stay queued. Messages to a node that is down
// are lost, not queued: that is what makes a follower fall behind.
func (n *net) deliver(keep func(Message) bool) {
	for {
		n.collect()
		var rest, now []Message
		for _, m := range n.box {
			if n.down[m.To] {
				continue
			}
			if keep(m) {
				now = append(now, m)
			} else {
				rest = append(rest, m)
			}
		}
		n.box = rest
		if len(now) == 0 {
			return
		}
		for _, m := range now {
			n.nodes[m.To].Step(m)
		}
	}
}

func all(Message) bool { return true }

func between(a, b uint64) func(Message) bool {
	return func(m Message) bool {
		return (m.From == a && m.To == b) || (m.From == b && m.To == a)
	}
}

func campaign(n *net, id uint64) {
	nd := n.nodes[id]
	for nd.state != Candidate && nd.state != Leader {
		nd.Tick()
	}
}

func log(entries ...uint64) []Entry {
	out := make([]Entry, len(entries))
	for i, term := range entries {
		out[i] = Entry{Index: uint64(i + 1), Term: term, Data: []byte{byte(term)}}
	}
	return out
}

func TestFigure8ALeaderNeverCommitsAnEarlierTermsEntryByCounting(t *testing.T) {
	// Figure 8 of the Raft paper. S1 holds an entry from term 2 that no
	// other live node has; S5 holds a different entry at that index from term
	// 3, so S5 can still be elected later and overwrite index 2.
	n := newNet(t, map[uint64]Restore{
		1: {HardState: HardState{Term: 3, Commit: 1}, Entries: log(1, 2)},
		2: {HardState: HardState{Term: 3, Commit: 1}, Entries: log(1)},
		3: {HardState: HardState{Term: 3, Commit: 1}, Entries: log(1)},
		4: {HardState: HardState{Term: 3, Commit: 1}, Entries: log(1)},
		5: {HardState: HardState{Term: 3, Commit: 1}, Entries: log(1, 3)},
	}, 1) // one entry per message: the old entry reaches S3 before the leader's own
	n.down[5] = true
	n.down[4] = true
	campaign(n, 1)
	n.deliver(func(m Message) bool { return m.Type == MsgVote || m.Type == MsgVoteResp })
	if n.nodes[1].state != Leader || n.nodes[1].term != 4 {
		t.Fatalf("S1 should lead term 4, is %s in term %d", n.nodes[1].state, n.nodes[1].term)
	}
	// Replicate to S3 only until it holds the term 2 entry at index 2, and
	// stop before the leader's term 4 no-op reaches it.
	// The leader's first message to S3 carries the no-op after index 2, which
	// S3 rejects for lacking index 2; that rejection is what walks the leader
	// back. Only hold the term 4 entry once S3 could actually accept it.
	//
	// Both S2 and S3 must acknowledge index 2, or the leader knows of only two
	// copies and cannot commit it with or without the rule - which is how the
	// first version of this test passed with the rule deleted.
	n.deliver(func(m Message) bool {
		if !between(1, 3)(m) && !between(1, 2)(m) {
			return false
		}
		if m.Type == MsgApp && m.To != 1 && n.nodes[m.To].lastIndex() >= m.Index {
			for _, e := range m.Entries {
				if e.Term == 4 {
					return false
				}
			}
		}
		return true
	})
	if n.nodes[1].prs[2].match < 2 || n.nodes[1].prs[3].match < 2 {
		t.Fatalf("scenario setup: the leader does not know S2 and S3 hold index 2 (match %d, %d)",
			n.nodes[1].prs[2].match, n.nodes[1].prs[3].match)
	}
	if t3, _ := n.nodes[3].termAt(2); t3 != 2 {
		t.Fatal("scenario setup: S3 never received index 2")
	}
	// S1, S2 and S3 - a majority - now hold index 2 from term 2, and the
	// leader knows it. Only the rule stops it committing.
	if n.nodes[1].commit >= 2 {
		t.Fatalf("S1 committed index 2 (term 2) by counting replicas; S5 can still be elected and overwrite it")
	}
}

func TestARestartedNodeHonoursTheVoteItPersisted(t *testing.T) {
	// The vote is on disk; a second candidate in the same term must be refused,
	// or two leaders can be elected in one term.
	nd := New(Config{ID: 3, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1, Seed: 1},
		Restore{HardState: HardState{Term: 5, Vote: 1}})
	nd.Step(Message{Type: MsgVote, From: 2, To: 3, Term: 5, Index: 9, LogTerm: 5})
	rd := nd.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatalf("restarted node granted a second vote in term 5: %+v", rd.Messages)
	}
}

func TestAVoteGoesOnlyToACandidateWithAnUpToDateLog(t *testing.T) {
	nd := New(Config{ID: 2, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1, Seed: 1},
		Restore{HardState: HardState{Term: 2}, Entries: log(1, 2, 2)})
	// Same last term, shorter log: refused.
	nd.Step(Message{Type: MsgVote, From: 1, To: 2, Term: 3, Index: 2, LogTerm: 2})
	// Higher last term, even with a shorter log: granted.
	nd.Step(Message{Type: MsgVote, From: 3, To: 2, Term: 4, Index: 1, LogTerm: 3})
	rd := nd.Ready()
	if len(rd.Messages) != 2 || !rd.Messages[0].Reject || rd.Messages[1].Reject {
		t.Fatalf("votes: %+v", rd.Messages)
	}
}

func TestAStaleLeaderStepsDownWhenItHearsAHigherTerm(t *testing.T) {
	n := newNet(t, map[uint64]Restore{1: {}, 2: {}, 3: {}}, 64)
	campaign(n, 1)
	n.deliver(all)
	if n.nodes[1].state != Leader {
		t.Fatal("no leader")
	}
	n.nodes[1].Step(Message{Type: MsgApp, From: 2, To: 1, Term: n.nodes[1].term + 3})
	if n.nodes[1].state != Follower || n.nodes[1].lead != 2 {
		t.Fatalf("still %s after seeing a higher term", n.nodes[1].state)
	}
}

func TestACutOffLeaderCannotConfirmARead(t *testing.T) {
	// A leader that cannot reach a majority may already have been replaced;
	// serving a read from its own state would be a stale read.
	n := newNet(t, map[uint64]Restore{1: {}, 2: {}, 3: {}}, 64)
	campaign(n, 1)
	n.deliver(all)
	leader := n.nodes[1]
	n.down[2], n.down[3] = true, true
	leader.ReadIndex(7)
	n.deliver(all)
	for range 20 {
		leader.Tick()
		n.deliver(all)
	}
	if len(n.reads[1]) != 0 {
		t.Fatalf("read confirmed without a majority: %+v", n.reads[1])
	}
	// Reconnected, the same read is confirmed.
	n.down[2], n.down[3] = false, false
	leader.Tick()
	n.deliver(all)
	if len(n.reads[1]) != 1 || n.reads[1][0].ID != 7 {
		t.Fatalf("read never confirmed after reconnecting: %+v", n.reads[1])
	}
}

func TestALaggingFollowerIsCaughtUpBySnapshot(t *testing.T) {
	n := newNet(t, map[uint64]Restore{1: {}, 2: {}, 3: {}}, 64)
	campaign(n, 1)
	n.deliver(all)
	n.down[3] = true
	leader := n.nodes[1]
	for i := range 10 {
		leader.Propose([]byte{byte(i)})
		n.deliver(all)
	}
	rd := leader.Ready()
	leader.Advance(rd)
	leader.Compact(leader.applied, []byte("state"))
	n.down[3] = false
	leader.Tick()
	n.deliver(all)
	f := n.nodes[3]
	if f.offset() != leader.offset() || f.commit < leader.offset() {
		t.Fatalf("follower at offset %d commit %d; leader compacted to %d", f.offset(), f.commit, leader.offset())
	}
	if f.snapshot == nil || string(f.snapshot.Data) != "state" {
		t.Fatal("follower did not receive the snapshot")
	}
}

func TestAConflictingSuffixIsReplacedAndAWholeTermSkippedPerRound(t *testing.T) {
	// The follower holds five entries from a term nobody committed; the
	// leader's log diverges at index 2.
	n := newNet(t, map[uint64]Restore{
		1: {HardState: HardState{Term: 5, Commit: 1}, Entries: log(1, 4, 4)},
		2: {HardState: HardState{Term: 5, Commit: 1}, Entries: log(1, 3, 3, 3, 3, 3)},
		3: {HardState: HardState{Term: 5, Commit: 1}, Entries: log(1, 4, 4)},
	}, 64)
	campaign(n, 1)
	rounds := 0
	n.deliver(func(m Message) bool {
		if m.Type == MsgAppResp && m.From == 2 && m.Reject {
			rounds++
		}
		return true
	})
	f := n.nodes[2]
	for i := uint64(2); i <= f.lastIndex(); i++ {
		lt, _ := n.nodes[1].termAt(i)
		ft, _ := f.termAt(i)
		if lt != ft {
			t.Fatalf("index %d: follower term %d, leader term %d", i, ft, lt)
		}
	}
	if rounds > 2 {
		t.Fatalf("%d rejected rounds to skip one conflicting term: the hint is not doing its job", rounds)
	}
}

func TestOneLeaderPerTermUnderSplitVotes(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		ids := []uint64{1, 2, 3, 4, 5}
		nodes := map[uint64]*Node{}
		for _, id := range ids {
			nodes[id] = New(Config{ID: id, Peers: ids, ElectionTick: 3, HeartbeatTick: 1, Seed: seed}, Restore{})
		}
		leaders := map[uint64]uint64{}
		var box []Message
		for step := 0; step < 300; step++ {
			for _, id := range ids {
				nodes[id].Tick()
				rd := nodes[id].Ready()
				box = append(box, rd.Messages...)
				nodes[id].Advance(rd)
				if nodes[id].state == Leader {
					if l, ok := leaders[nodes[id].term]; ok && l != id {
						t.Fatalf("seed %d: two leaders in term %d", seed, nodes[id].term)
					}
					leaders[nodes[id].term] = id
				}
			}
			// Deliver in an order that changes every step.
			next := box
			box = nil
			for i := range next {
				m := next[(i*7+step)%len(next)]
				nodes[m.To].Step(m)
			}
		}
	}
}
