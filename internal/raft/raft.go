// Package raft is the Raft consensus algorithm as a pure state machine.
//
// It has no clock, no network and no disk. Time arrives as Tick, messages
// arrive as Step, and everything the node wants done - entries to persist,
// messages to send, committed entries to apply - comes out of Ready. The caller
// does those things, in that order, and calls Advance.
//
// That shape is what makes it testable. A node driven by ticks and messages
// alone behaves identically every time it is given the same ticks and messages,
// so a whole cluster can be run inside one process under a simulated network
// that drops, delays, duplicates and reorders, with crashes that lose whatever
// was not yet on disk - and a failure found that way can be replayed exactly
// from its seed. etcd's raft library is built the same way, for the same reason.
//
// It implements leader election, log replication and commitment as in Figure 2
// of the Raft paper, a no-op entry on election so a new leader can commit, log
// compaction with snapshots, and ReadIndex for linearizable reads that do not
// go through the log. It does not implement membership changes.
//
// The one ordering rule the caller must keep: persist Ready.HardState,
// Ready.Snapshot and Ready.Entries before sending Ready.Messages. A vote or an
// acknowledgement that leaves the node before the state behind it is on disk is
// a promise a crash can break, and that is how two leaders get elected in one
// term.
package raft

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// None is "no node": no vote cast, no leader known.
const None uint64 = 0

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

type MsgType int

const (
	MsgVote     MsgType = iota // candidate asks for a vote
	MsgVoteResp                // the answer
	MsgApp                     // leader appends entries; empty entries is a heartbeat
	MsgAppResp                 // follower's answer, carrying its match or a rejection hint
	MsgSnap                    // leader sends its snapshot to a follower that is too far behind
)

func (t MsgType) String() string {
	return [...]string{"vote", "vote_resp", "app", "app_resp", "snap"}[t]
}

type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte // nil for the leader's no-op on election
}

type Snapshot struct {
	Index uint64 // last index the snapshot covers
	Term  uint64 // term of that entry
	Data  []byte
}

// HardState is what must survive a crash for the safety argument to hold.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

type Message struct {
	Type    MsgType
	From    uint64
	To      uint64
	Term    uint64
	LogTerm uint64 // MsgApp: term at Index. MsgVote: term of the candidate's last entry.
	Index   uint64 // MsgApp: index before Entries. MsgVote: candidate's last index. MsgAppResp: match.
	Entries []Entry
	Commit  uint64
	Reject  bool
	// RejectHint is where the follower's log stops matching, so the leader can
	// skip back over a whole conflicting term in one round instead of one entry
	// per round trip.
	RejectHint uint64
	// Context echoes the leader's heartbeat round, which is how a ReadIndex
	// learns that a majority still accepted this node as leader after the read
	// arrived.
	Context  uint64
	Snapshot *Snapshot
}

// ReadState says a linearizable read with this id may be served once the
// state machine has applied Index.
type ReadState struct {
	ID    uint64
	Index uint64
}

// Ready is everything the node needs the caller to do.
type Ready struct {
	HardState        HardState
	HardStateChanged bool
	Snapshot         *Snapshot // persist, and restore the state machine from it
	Entries          []Entry   // persist; may overwrite entries already on disk at the same index
	Messages         []Message // send, after persisting the above
	CommittedEntries []Entry   // apply, in order
	ReadStates       []ReadState
}

func (r Ready) Empty() bool {
	return !r.HardStateChanged && r.Snapshot == nil && len(r.Entries) == 0 &&
		len(r.Messages) == 0 && len(r.CommittedEntries) == 0 && len(r.ReadStates) == 0
}

type Config struct {
	ID            uint64
	Peers         []uint64 // every member, this node included
	ElectionTick  int      // followers wait between this and twice this before standing
	HeartbeatTick int      // leaders send heartbeats this often; well under ElectionTick
	Seed          uint64   // for the randomised election timeout
	// MaxEntriesPerMsg bounds one MsgApp, so catching up a slow follower
	// does not mean one enormous message.
	MaxEntriesPerMsg int
}

// Restore is the state a node restarts from.
type Restore struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry // after the snapshot, in order
}

type progress struct {
	match uint64
	next  uint64
	// ack is the highest heartbeat round this follower has answered.
	ack uint64
}

type pendingRead struct {
	id    uint64
	index uint64
	round uint64
}

type Node struct {
	cfg   Config
	rng   *rand.Rand
	id    uint64
	peers []uint64

	state StateType
	term  uint64
	vote  uint64
	lead  uint64

	// log[0] is a sentinel standing for the snapshot: Index and Term of the
	// last compacted entry. Real entries follow.
	log    []Entry
	commit uint64

	snapshot *Snapshot // latest, kept to send to followers that fall behind it

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	votes map[uint64]bool
	prs   map[uint64]*progress

	// ReadIndex.
	round        uint64
	pendingReads []pendingRead
	readyReads   []ReadState
	waitingReads []uint64 // arrived before this leader had committed in its term

	// Bookkeeping for Ready.
	msgs         []Message
	prevHard     HardState
	unstableFrom uint64 // lowest index not yet handed out for persisting
	applied      uint64 // highest index handed out for applying
	pendingSnap  *Snapshot
}

func New(cfg Config, restore Restore) *Node {
	if cfg.ElectionTick <= cfg.HeartbeatTick {
		panic("raft: ElectionTick must be greater than HeartbeatTick")
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 64
	}
	n := &Node{
		cfg:   cfg,
		id:    cfg.ID,
		peers: slices.Clone(cfg.Peers),
		rng:   rand.New(rand.NewPCG(cfg.Seed, cfg.ID)),
		log:   []Entry{{}},
	}
	if s := restore.Snapshot; s != nil {
		n.log[0] = Entry{Index: s.Index, Term: s.Term}
		n.snapshot = s
		n.commit = s.Index
		n.applied = s.Index
	}
	for _, e := range restore.Entries {
		if e.Index != n.lastIndex()+1 {
			panic(fmt.Sprintf("raft: restored entries are not contiguous at %d", e.Index))
		}
		n.log = append(n.log, e)
	}
	n.term, n.vote = restore.HardState.Term, restore.HardState.Vote
	n.commit = max(n.commit, min(restore.HardState.Commit, n.lastIndex()))
	n.prevHard = HardState{Term: n.term, Vote: n.vote, Commit: n.commit}
	n.unstableFrom = n.lastIndex() + 1
	n.becomeFollower(n.term, None)
	return n
}

// ----------------------------------------------------------------- the log

func (n *Node) offset() uint64    { return n.log[0].Index }
func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }

// termAt returns the term of the entry at i, and false if i is compacted away
// or not yet in the log.
func (n *Node) termAt(i uint64) (uint64, bool) {
	if i < n.offset() || i > n.lastIndex() {
		return 0, false
	}
	return n.log[i-n.offset()].Term, true
}

func (n *Node) entriesFrom(lo uint64, limit int) []Entry {
	if lo > n.lastIndex() {
		return nil
	}
	start := lo - n.offset()
	end := min(uint64(len(n.log)), start+uint64(limit))
	return slices.Clone(n.log[start:end])
}

// upToDate is the election restriction: a vote goes only to a candidate whose
// log is at least as complete as the voter's, which is what guarantees every
// committed entry survives into the next leader's log.
func (n *Node) upToDate(lastTerm, lastIndex uint64) bool {
	return lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIndex >= n.lastIndex())
}

func (n *Node) append(entries ...Entry) {
	for _, e := range entries {
		n.log = append(n.log, e)
	}
}

// truncateFrom removes the entry at i and everything after it.
func (n *Node) truncateFrom(i uint64) {
	if i <= n.commit {
		panic(fmt.Sprintf("raft: node %d asked to truncate committed entry %d (commit %d)", n.id, i, n.commit))
	}
	n.log = n.log[:i-n.offset()]
	n.unstableFrom = min(n.unstableFrom, i)
}

// --------------------------------------------------------- role changes

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTick + n.rng.IntN(n.cfg.ElectionTick)
}

func (n *Node) becomeFollower(term, lead uint64) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.state = Follower
	n.lead = lead
	n.votes = nil
	n.prs = nil
	n.dropReads()
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.term++
	n.vote = n.id
	n.lead = None
	n.votes = map[uint64]bool{n.id: true}
	n.dropReads()
	n.resetElectionTimer()
	if n.quorum(len(n.votes)) {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{Type: MsgVote, To: p, Index: n.lastIndex(), LogTerm: n.lastTerm()})
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0
	n.prs = map[uint64]*progress{}
	for _, p := range n.peers {
		n.prs[p] = &progress{next: n.lastIndex() + 1}
	}
	// A leader may only count replicas to commit entries of its own term, so
	// until it has one, nothing from earlier terms can commit. The no-op gives
	// it one at once - and is what lets ReadIndex start serving.
	n.append(Entry{Term: n.term, Index: n.lastIndex() + 1})
	n.prs[n.id].match = n.lastIndex()
	n.prs[n.id].next = n.lastIndex() + 1
	n.maybeCommit()
	n.broadcastAppend()
}

func (n *Node) quorum(count int) bool { return count > len(n.peers)/2 }

// ----------------------------------------------------------------- time

func (n *Node) Tick() {
	if n.state == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.becomeCandidate()
	}
}

// ------------------------------------------------------------- messages

func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		lead := None
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// A stale sender. Tell it the current term so it steps down; a stale
		// leader's heartbeat must not reset anyone's election timer.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		case MsgApp, MsgSnap:
			n.send(Message{Type: MsgAppResp, To: m.From, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	case MsgSnap:
		n.handleSnapshot(m)
	}
}

func (n *Node) handleVote(m Message) {
	canVote := n.vote == None || n.vote == m.From
	if canVote && n.state == Follower && n.upToDate(m.LogTerm, m.Index) {
		n.vote = m.From
		n.resetElectionTimer()
		n.send(Message{Type: MsgVoteResp, To: m.From})
		return
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
}

func (n *Node) handleVoteResp(m Message) {
	if n.state != Candidate || m.Reject {
		return
	}
	n.votes[m.From] = true
	if n.quorum(len(n.votes)) {
		n.becomeLeader()
	}
}

func (n *Node) handleAppend(m Message) {
	if n.state == Candidate {
		n.becomeFollower(m.Term, m.From)
	}
	n.lead = m.From
	n.resetElectionTimer()

	prev, entries := m.Index, m.Entries
	// Entries the snapshot already covers are known committed; skip them.
	if prev < n.offset() {
		skip := n.offset() - prev
		if uint64(len(entries)) <= skip {
			n.send(Message{Type: MsgAppResp, To: m.From, Index: n.offset(), Context: m.Context})
			return
		}
		entries = entries[skip:]
		prev = n.offset()
	} else if term, ok := n.termAt(prev); !ok || term != m.LogTerm {
		// The hint is the last index the leader should assume matches: the
		// end of a log that is too short, or the entry before the start of a
		// conflicting term - so a whole stale term is skipped in one round.
		// Never below commit: committed entries match by construction.
		hint := n.lastIndex()
		if ok {
			first := prev
			floor := max(n.commit, n.offset())
			for first > floor+1 {
				if t, _ := n.termAt(first - 1); t != term {
					break
				}
				first--
			}
			hint = max(first-1, floor)
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, RejectHint: hint, Context: m.Context})
		return
	}

	for i, e := range entries {
		if term, ok := n.termAt(e.Index); ok {
			if term == e.Term {
				continue
			}
			n.truncateFrom(e.Index)
		}
		n.append(entries[i:]...)
		break
	}
	lastNew := prev + uint64(len(entries))
	if m.Commit > n.commit {
		n.commit = min(m.Commit, lastNew)
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Index: lastNew, Context: m.Context})
}

func (n *Node) handleAppendResp(m Message) {
	if n.state != Leader {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.ack = max(pr.ack, m.Context)
	if m.Reject {
		// Never move next past what the follower said, never below 1, and
		// ignore a stale rejection for an index already known to match.
		if m.RejectHint+1 <= pr.match {
			return
		}
		pr.next = max(pr.match+1, min(pr.next-1, m.RejectHint+1))
		n.sendAppend(m.From)
	} else if m.Index > pr.match {
		pr.match = m.Index
		pr.next = max(pr.next, m.Index+1)
		if n.maybeCommit() {
			n.broadcastAppend()
		} else if pr.next <= n.lastIndex() {
			n.sendAppend(m.From)
		}
	}
	n.advanceReads()
}

func (n *Node) handleSnapshot(m Message) {
	if n.state == Candidate {
		n.becomeFollower(m.Term, m.From)
	}
	n.lead = m.From
	n.resetElectionTimer()
	s := m.Snapshot
	if s.Index <= n.commit {
		n.send(Message{Type: MsgAppResp, To: m.From, Index: n.commit})
		return
	}
	if term, ok := n.termAt(s.Index); ok && term == s.Term {
		// Our log already runs past the snapshot: keep the tail after it.
		n.log = slices.Clone(n.log[s.Index-n.offset():])
		n.log[0] = Entry{Index: s.Index, Term: s.Term}
	} else {
		n.log = []Entry{{Index: s.Index, Term: s.Term}}
		n.unstableFrom = s.Index + 1
	}
	n.snapshot = s
	n.pendingSnap = s
	n.commit = s.Index
	n.applied = s.Index
	n.send(Message{Type: MsgAppResp, To: m.From, Index: s.Index})
}

// maybeCommit advances commit to the highest index a majority has, if that
// entry is from this leader's term.
func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.prs))
	for _, pr := range n.prs {
		matches = append(matches, pr.match)
	}
	slices.Sort(matches)
	// The highest index at least a majority has: sorted ascending, it sits
	// len/2 from the top.
	candidate := matches[len(matches)-1-len(matches)/2]
	if candidate <= n.commit {
		return false
	}
	if term, _ := n.termAt(candidate); term != n.term {
		// Figure 8 of the paper: committing an older term's entry by
		// counting replicas can be undone by a later leader.
		return false
	}
	n.commit = candidate
	return true
}

func (n *Node) broadcastAppend() {
	n.round++
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
	n.advanceReads()
}

func (n *Node) sendAppend(to uint64) {
	pr := n.prs[to]
	prev := pr.next - 1
	if prev < n.offset() {
		// The entries it needs are compacted: send the snapshot instead.
		if n.snapshot == nil {
			panic("raft: entries compacted with no snapshot to send")
		}
		n.send(Message{Type: MsgSnap, To: to, Snapshot: n.snapshot, Context: n.round})
		pr.next = n.snapshot.Index + 1
		return
	}
	term, _ := n.termAt(prev)
	n.send(Message{
		Type: MsgApp, To: to, Index: prev, LogTerm: term,
		Entries: n.entriesFrom(pr.next, n.cfg.MaxEntriesPerMsg),
		Commit:  n.commit, Context: n.round,
	})
}

// ------------------------------------------------------------- proposals

// Propose appends data to the log if this node is the leader, and says where.
// Being appended is not being committed: the caller learns that from
// CommittedEntries, and a proposal can still be lost if leadership changes
// first.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.state != Leader {
		return 0, 0, false
	}
	n.append(Entry{Term: n.term, Index: n.lastIndex() + 1, Data: data})
	n.prs[n.id].match = n.lastIndex()
	n.prs[n.id].next = n.lastIndex() + 1
	n.maybeCommit() // a single-node cluster commits on its own
	n.broadcastAppend()
	return n.lastIndex(), n.term, true
}

// ------------------------------------------------------------- ReadIndex

// ReadIndex asks for a linearizable read. When it is safe the node emits a
// ReadState with this id; the caller serves the read once it has applied up
// to its Index. Returns false if this node is not the leader.
//
// A leader cannot simply read its own state: it may have been deposed by a
// partition it has not noticed, and a newer leader may already have accepted
// writes. So it records its commit index, then waits until a majority has
// answered a heartbeat sent after the read arrived - proof that it was still
// leader at that moment. It must also have committed an entry in its own term,
// or its commit index may lag what an earlier leader already acknowledged.
func (n *Node) ReadIndex(id uint64) bool {
	if n.state != Leader {
		return false
	}
	if t, _ := n.termAt(n.commit); t != n.term {
		n.waitingReads = append(n.waitingReads, id)
		return true
	}
	n.queueRead(id)
	return true
}

func (n *Node) queueRead(id uint64) {
	if len(n.peers) == 1 {
		n.readyReads = append(n.readyReads, ReadState{ID: id, Index: n.commit})
		return
	}
	n.pendingReads = append(n.pendingReads, pendingRead{id: id, index: n.commit, round: n.round + 1})
	n.broadcastAppend()
}

func (n *Node) advanceReads() {
	if n.state != Leader {
		return
	}
	if len(n.waitingReads) > 0 {
		if t, _ := n.termAt(n.commit); t == n.term {
			waiting := n.waitingReads
			n.waitingReads = nil
			for _, id := range waiting {
				n.queueRead(id)
			}
		}
	}
	kept := n.pendingReads[:0]
	for _, r := range n.pendingReads {
		acks := 1
		for p, pr := range n.prs {
			if p != n.id && pr.ack >= r.round {
				acks++
			}
		}
		if n.quorum(acks) {
			n.readyReads = append(n.readyReads, ReadState{ID: r.id, Index: r.index})
		} else {
			kept = append(kept, r)
		}
	}
	n.pendingReads = kept
}

// dropReads abandons reads on losing leadership; their callers time out and
// retry against the new leader.
func (n *Node) dropReads() {
	n.pendingReads, n.waitingReads = nil, nil
}

// ------------------------------------------------------------- compaction

// Compact discards the log up to index, which the caller has applied and
// captured in data. The snapshot is kept to send to followers that fall
// behind it.
func (n *Node) Compact(index uint64, data []byte) {
	if index <= n.offset() {
		return
	}
	if index > n.applied {
		panic(fmt.Sprintf("raft: compact to %d beyond applied %d", index, n.applied))
	}
	term, _ := n.termAt(index)
	n.snapshot = &Snapshot{Index: index, Term: term, Data: data}
	n.log = slices.Clone(n.log[index-n.offset():])
	n.log[0] = Entry{Index: index, Term: term}
}

// ------------------------------------------------------------- Ready

func (n *Node) hardState() HardState { return HardState{Term: n.term, Vote: n.vote, Commit: n.commit} }

func (n *Node) Ready() Ready {
	hs := n.hardState()
	rd := Ready{HardState: hs, HardStateChanged: hs != n.prevHard, Snapshot: n.pendingSnap}
	if n.unstableFrom <= n.lastIndex() {
		rd.Entries = n.entriesFrom(n.unstableFrom, len(n.log))
	}
	rd.Messages = n.msgs
	if n.commit > n.applied {
		rd.CommittedEntries = n.entriesFrom(n.applied+1, int(n.commit-n.applied))
	}
	rd.ReadStates = n.readyReads
	return rd
}

// Advance records that a Ready returned by Ready has been handled.
func (n *Node) Advance(rd Ready) {
	n.prevHard = rd.HardState
	if len(rd.Entries) > 0 {
		n.unstableFrom = rd.Entries[len(rd.Entries)-1].Index + 1
	}
	if k := len(rd.CommittedEntries); k > 0 {
		n.applied = rd.CommittedEntries[k-1].Index
	}
	n.pendingSnap = nil
	n.msgs = n.msgs[len(rd.Messages):]
	n.readyReads = n.readyReads[len(rd.ReadStates):]
}

// ------------------------------------------------------------- status

type Status struct {
	ID       uint64 `json:"id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	Lead     uint64 `json:"lead"`
	Commit   uint64 `json:"commit"`
	Applied  uint64 `json:"applied"`
	Last     uint64 `json:"last_index"`
	Snapshot uint64 `json:"snapshot_index"`
	// Progress is the leader's view of each follower: the highest index known
	// to match its log, and the next one to send. Empty on a follower.
	Progress map[uint64]Progress `json:"progress,omitempty"`
}

type Progress struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
}

func (n *Node) Status() Status {
	st := Status{
		ID: n.id, State: n.state.String(), Term: n.term, Lead: n.lead,
		Commit: n.commit, Applied: n.applied, Last: n.lastIndex(), Snapshot: n.offset(),
	}
	if n.state == Leader {
		st.Progress = make(map[uint64]Progress, len(n.prs))
		for id, pr := range n.prs {
			st.Progress[id] = Progress{Match: pr.match, Next: pr.next}
		}
	}
	return st
}

// SnapshotIndex is the index of the last entry the log has been compacted to.
func (n *Node) SnapshotIndex() uint64 { return n.offset() }

// SnapshotTerm is the term of the last entry the log has been compacted to.
func (n *Node) SnapshotTerm() uint64 { return n.log[0].Term }

func (n *Node) State() StateType { return n.state }
func (n *Node) Lead() uint64     { return n.lead }
func (n *Node) Term() uint64     { return n.term }
