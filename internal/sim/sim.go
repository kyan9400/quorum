// Package sim runs a whole cluster inside one process, on virtual time, under a
// network and a fault schedule chosen by a seed - and then checks the result.
//
// Everything that varies comes from the seed: message delays, losses,
// duplicates and reorderings, when partitions start and heal, which node
// crashes and when it comes back, what the clients ask for. Nothing reads the
// wall clock and no goroutines race, so the same seed produces the same run,
// event for event. A failure found on seed 81233 is a failure anyone can
// replay, step through and fix, which is the difference between a bug report
// and an anecdote.
//
// A crash discards everything a node had not persisted: the simulator keeps a
// separate copy of exactly what each Ready asked to be written, and a
// restarted node is rebuilt from that copy alone.
//
// After a run three properties are checked:
//
//   - linearizability of every history the clients observed (package check);
//   - state machine safety: no index applied with different contents on two
//     nodes, ever;
//   - election safety: never two leaders in one term.
package sim

import (
	"cmp"
	"container/heap"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/kyan9400/quorum/internal/check"
	"github.com/kyan9400/quorum/internal/kv"
	"github.com/kyan9400/quorum/internal/raft"
)

// Config is one simulated run. Times are virtual milliseconds.
type Config struct {
	Seed     uint64
	Nodes    int
	Clients  int
	Keys     int
	Duration int64 // how long clients send
	Quiet    int64 // fault-free time at the end, for the cluster to converge

	TickMs        int64
	ElectionTick  int
	HeartbeatTick int
	SnapshotEvery uint64 // compact after this many applied entries; 0 never

	// Network.
	MinDelay, MaxDelay int64
	DropRate           float64
	DuplicateRate      float64
	// StallRate is the share of messages held up far longer than usual, as a
	// packet stuck in a queue is. It is what lets a stale vote request reach
	// a node after that node has crashed and come back.
	StallRate float64
	StallMax  int64
	// MaxEntriesPerMsg bounds a replication batch; small batches let a
	// lagging follower receive an old term's entries before the leader's own.
	MaxEntriesPerMsg int

	// Faults: a fault starts on average this often.
	FaultEvery int64
	// FaultKinds enables each fault; all on by default.
	NoPartitions, NoCrashes bool
	// Swarm lets each seed pick its own fault intensity, from a fault every
	// few seconds to a storm of them, instead of every seed sharing one.
	Swarm bool

	ClientTimeout int64 // per attempt
	OpDeadline    int64 // per operation, across retries; after it the outcome is unknown
}

func Defaults(seed uint64) Config {
	return Config{
		Seed: seed, Nodes: 5, Clients: 5, Keys: 3,
		Duration: 30_000, Quiet: 5_000,
		TickMs: 10, ElectionTick: 15, HeartbeatTick: 3, SnapshotEvery: 40,
		MinDelay: 1, MaxDelay: 25, DropRate: 0.02, DuplicateRate: 0.01,
		FaultEvery: 2_500, Swarm: true,
		ClientTimeout: 400, OpDeadline: 2_500,
	}
}

// ---------------------------------------------------------------- events

type eventKind int

const (
	evTick eventKind = iota
	evDeliver
	evClientStart
	evClientTimeout
	evClientReply
	evFault
	evHeal
	evRestart
)

type event struct {
	at   int64
	seq  uint64
	kind eventKind
	node uint64 // evTick, evDeliver, evRestart
	msg  raft.Message
	// client traffic
	client  int
	attempt uint64
	req     *request
	reply   *reply
	epoch   uint64 // evTick: ignore ticks scheduled before the node's last restart
	heal    int    // evHeal: which partition
}

type queue []*event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	return cmp.Or(cmp.Compare(q[i].at, q[j].at), cmp.Compare(q[i].seq, q[j].seq)) < 0
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *queue) Pop() any     { old := *q; e := old[len(old)-1]; *q = old[:len(old)-1]; return e }

// ---------------------------------------------------------------- nodes

type durable struct {
	hard     raft.HardState
	snapshot *raft.Snapshot
	entries  []raft.Entry
}

func (d *durable) save(rd raft.Ready) {
	if rd.Snapshot != nil {
		d.snapshot = rd.Snapshot
		var kept []raft.Entry
		for _, e := range d.entries {
			if e.Index > rd.Snapshot.Index {
				kept = append(kept, e)
			}
		}
		d.entries = kept
	}
	for _, e := range rd.Entries {
		// An entry at an index already on disk replaces it and everything
		// after: that is what truncating a conflicting suffix looks like.
		cut := sort.Search(len(d.entries), func(i int) bool { return d.entries[i].Index >= e.Index })
		d.entries = append(d.entries[:cut], e)
	}
	if rd.HardStateChanged {
		d.hard = rd.HardState
	}
}

func (d *durable) compact(s *raft.Snapshot) {
	d.snapshot = s
	cut := sort.Search(len(d.entries), func(i int) bool { return d.entries[i].Index > s.Index })
	d.entries = slices.Clone(d.entries[cut:])
}

type pendingWrite struct {
	index    uint64
	client   int
	attempt  uint64
	clientID uint64
	seq      uint64
}

type pendingRead struct {
	id      uint64
	client  int
	attempt uint64
	key     string
	index   uint64 // known once the ReadState arrives
	ready   bool
}

type node struct {
	id      uint64
	alive   bool
	epoch   uint64
	raft    *raft.Node
	store   *kv.Store
	disk    durable
	writes  []pendingWrite
	reads   []*pendingRead
	nextRid uint64
	skew    int64 // this node's tick interval differs slightly: clocks drift
}

// ---------------------------------------------------------------- clients

type request struct {
	client  int
	attempt uint64
	cmd     kv.Command
	isRead  bool
}

type reply struct {
	ok     bool // false: not leader / lost - retry
	hint   uint64
	result kv.Result
}

type client struct {
	id       uint64
	seq      uint64
	leader   uint64
	attempt  uint64
	op       *check.Operation
	cmd      kv.Command
	deadline int64
	busy     bool
}

// ---------------------------------------------------------------- the run

type Result struct {
	Seed       uint64
	OK         bool
	Problems   []string
	Check      check.Report
	Operations int
	Completed  int
	Unknown    int
	Leaders    int // distinct (term, leader) pairs seen
	Crashes    int
	Partitions int
	Messages   int
	Dropped    int
	MaxTerm    uint64
	Snapshots  int
}

func (r Result) String() string {
	var b strings.Builder
	verdict := "ok"
	if !r.OK {
		verdict = "FAILED"
	}
	fmt.Fprintf(&b, "seed %d: %s - %d ops (%d completed, %d unknown), %d leaders up to term %d, %d crashes, %d partitions, %d snapshots, %d/%d messages dropped",
		r.Seed, verdict, r.Operations, r.Completed, r.Unknown, r.Leaders, r.MaxTerm, r.Crashes, r.Partitions, r.Snapshots, r.Dropped, r.Messages)
	for _, p := range r.Problems {
		fmt.Fprintf(&b, "\n  %s", p)
	}
	return b.String()
}

type sim struct {
	cfg     Config
	rng     *rand.Rand
	now     int64
	seq     uint64
	q       queue
	nodes   []*node
	clients []*client
	history []check.Operation
	// partitions: node -> group; nodes in different groups cannot talk.
	group     map[uint64]int
	partition int

	// Safety bookkeeping.
	intensity float64           // 1 is the default fault rate; smaller is a storm
	appliedAt map[uint64]string // index -> term:data of the first application anywhere
	leaders   map[uint64]uint64 // term -> leader
	problems  []string

	res Result
}

func Run(cfg Config) Result {
	s := &sim{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 0x5eed)),
		group:     map[uint64]int{},
		appliedAt: map[uint64]string{},
		leaders:   map[uint64]uint64{},
		res:       Result{Seed: cfg.Seed},
	}
	if cfg.Swarm {
		// Swarm testing. With one fault rate for every seed, a thousand seeds
		// explore one kind of weather a thousand times. The two bugs a
		// mutation test showed this simulator missing - a vote not persisted
		// across a restart, and the Figure 8 commit rule - both need faults
		// in quick succession: a node crashing and coming back inside one
		// election, two leaders failing back to back. So each seed draws its
		// own intensity, and some seeds are storms.
		s.intensity = []float64{1, 0.4, 0.15, 0.06}[s.rng.IntN(4)]
		cfg.StallRate = []float64{0, 0.01, 0.05}[s.rng.IntN(3)]
		cfg.StallMax = 1_500
		cfg.MaxEntriesPerMsg = []int{1, 4, 64}[s.rng.IntN(3)]
		cfg.DuplicateRate = []float64{0.01, 0.05}[s.rng.IntN(2)]
		cfg.FaultEvery = max(100, int64(float64(cfg.FaultEvery)*s.intensity))
		s.cfg = cfg
	} else {
		s.intensity = 1
	}
	peers := make([]uint64, cfg.Nodes)
	for i := range peers {
		peers[i] = uint64(i + 1)
	}
	for _, id := range peers {
		n := &node{id: id, alive: true, skew: s.rng.Int64N(3) - 1}
		s.nodes = append(s.nodes, n)
		s.boot(n)
	}
	for i := range cfg.Clients {
		c := &client{id: uint64(1000 + i), leader: peers[s.rng.IntN(len(peers))]}
		s.clients = append(s.clients, c)
		s.schedule(&event{at: s.rng.Int64N(200), kind: evClientStart, client: i})
	}
	if cfg.FaultEvery > 0 {
		s.schedule(&event{at: s.faultGap(), kind: evFault})
	}

	end := cfg.Duration + cfg.Quiet
	for s.q.Len() > 0 {
		e := heap.Pop(&s.q).(*event)
		if e.at > end {
			break
		}
		s.now = e.at
		s.handle(e)
	}
	s.finish()
	return s.res
}

func (s *sim) schedule(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.q, e)
}

func (s *sim) faultGap() int64 { return s.cfg.FaultEvery/2 + s.rng.Int64N(s.cfg.FaultEvery) }

// scaled draws a fault duration, shorter in a storm: a node that is down for
// two seconds misses the elections a node down for forty milliseconds walks
// straight back into.
func (s *sim) scaled(base, spread int64) int64 {
	b := max(20, int64(float64(base)*s.intensity))
	sp := max(20, int64(float64(spread)*s.intensity))
	return b + s.rng.Int64N(sp)
}

func (s *sim) boot(n *node) {
	restore := raft.Restore{HardState: n.disk.hard, Snapshot: n.disk.snapshot, Entries: slices.Clone(n.disk.entries)}
	n.raft = raft.New(raft.Config{
		ID: n.id, Peers: s.peerIDs(), ElectionTick: s.cfg.ElectionTick, HeartbeatTick: s.cfg.HeartbeatTick,
		Seed: s.cfg.Seed ^ (n.epoch << 32), MaxEntriesPerMsg: s.cfg.MaxEntriesPerMsg,
	}, restore)
	n.store = kv.New()
	if n.disk.snapshot != nil {
		st, err := kv.Restore(n.disk.snapshot.Data)
		if err != nil {
			s.problem("node %d could not restore its snapshot: %v", n.id, err)
			st = kv.New()
		}
		n.store = st
	}
	n.writes, n.reads = nil, nil
	n.epoch++
	s.schedule(&event{at: s.now + s.tickInterval(n), kind: evTick, node: n.id, epoch: n.epoch})
	s.drain(n)
}

func (s *sim) peerIDs() []uint64 {
	ids := make([]uint64, s.cfg.Nodes)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

func (s *sim) tickInterval(n *node) int64 { return s.cfg.TickMs + n.skew }

func (s *sim) node(id uint64) *node { return s.nodes[id-1] }

func (s *sim) problem(format string, a ...any) {
	if len(s.problems) < 20 {
		s.problems = append(s.problems, fmt.Sprintf("t=%dms ", s.now)+fmt.Sprintf(format, a...))
	}
}

// ---------------------------------------------------------------- handling

func (s *sim) handle(e *event) {
	switch e.kind {
	case evTick:
		n := s.node(e.node)
		if !n.alive || e.epoch != n.epoch {
			return
		}
		n.raft.Tick()
		s.drain(n)
		s.schedule(&event{at: s.now + s.tickInterval(n), kind: evTick, node: n.id, epoch: n.epoch})
	case evDeliver:
		n := s.node(e.node)
		if !n.alive {
			return
		}
		if e.req != nil {
			s.serve(n, e.req)
		} else {
			n.raft.Step(e.msg)
		}
		s.drain(n)
	case evClientStart:
		s.clientStart(e.client)
	case evClientTimeout:
		s.clientTimeout(e.client, e.attempt)
	case evClientReply:
		s.clientReply(e.client, e.attempt, e.reply)
	case evFault:
		s.fault()
	case evHeal:
		if e.heal == s.partition {
			clear(s.group)
		}
	case evRestart:
		n := s.node(e.node)
		n.alive = true
		s.boot(n)
	}
}

// drain does everything a node's Ready asks, in the order the contract
// requires: persist, then send, then apply.
func (s *sim) drain(n *node) {
	for {
		rd := n.raft.Ready()
		if rd.Empty() {
			return
		}
		n.disk.save(rd)
		if rd.Snapshot != nil {
			st, err := kv.Restore(rd.Snapshot.Data)
			if err != nil {
				s.problem("node %d received an unreadable snapshot: %v", n.id, err)
			} else {
				n.store = st
				s.res.Snapshots++
			}
			s.failWrites(n, rd.Snapshot.Index)
		}
		for _, m := range rd.Messages {
			s.sendRaft(m)
		}
		for _, e := range rd.CommittedEntries {
			s.apply(n, e)
		}
		for _, r := range rd.ReadStates {
			for _, p := range n.reads {
				if p.id == r.ID {
					p.index, p.ready = r.Index, true
				}
			}
		}
		n.raft.Advance(rd)
		s.serveReads(n)
		s.observeLeader(n)
		if s.cfg.SnapshotEvery > 0 && n.store.Applied() >= s.lastCompact(n)+s.cfg.SnapshotEvery {
			data := n.store.Snapshot()
			n.raft.Compact(n.store.Applied(), data)
			n.disk.compact(&raft.Snapshot{Index: n.store.Applied(), Term: n.raft.SnapshotTerm(), Data: data})
		}
	}
}

func (s *sim) lastCompact(n *node) uint64 {
	if n.disk.snapshot == nil {
		return 0
	}
	return n.disk.snapshot.Index
}

func (s *sim) apply(n *node, e raft.Entry) {
	sig := fmt.Sprintf("%d:%x", e.Term, e.Data)
	if prev, ok := s.appliedAt[e.Index]; ok && prev != sig {
		s.problem("state machine safety: node %d applied %s at index %d, another node applied %s", n.id, sig, e.Index, prev)
	} else if !ok {
		s.appliedAt[e.Index] = sig
	}
	if n.store.Applied() >= e.Index {
		return // already in the snapshot it restored from
	}
	result, _ := n.store.Apply(e.Index, e.Data)
	kept := n.writes[:0]
	for _, w := range n.writes {
		if w.index != e.Index {
			kept = append(kept, w)
			continue
		}
		// The entry at the index this write was proposed at may be someone
		// else's, if leadership changed and the log was overwritten.
		var c kv.Command
		mine := len(e.Data) > 0 && decode(e.Data, &c) && c.Client == w.clientID && c.Seq == w.seq
		s.replyTo(w.client, w.attempt, &reply{ok: mine, result: result})
	}
	n.writes = kept
}

func (s *sim) failWrites(n *node, upTo uint64) {
	kept := n.writes[:0]
	for _, w := range n.writes {
		if w.index <= upTo {
			s.replyTo(w.client, w.attempt, &reply{ok: false})
			continue
		}
		kept = append(kept, w)
	}
	n.writes = kept
}

func (s *sim) serveReads(n *node) {
	kept := n.reads[:0]
	for _, p := range n.reads {
		if p.ready && n.store.Applied() >= p.index {
			v, found := n.store.Get(p.key)
			s.replyTo(p.client, p.attempt, &reply{ok: true, result: kv.Result{Value: v, Found: found, OK: true}})
			continue
		}
		kept = append(kept, p)
	}
	n.reads = kept
}

func (s *sim) observeLeader(n *node) {
	if n.raft.State() != raft.Leader {
		return
	}
	term := n.raft.Term()
	if prev, ok := s.leaders[term]; ok && prev != n.id {
		s.problem("election safety: nodes %d and %d both leader in term %d", prev, n.id, term)
	} else if !ok {
		s.leaders[term] = n.id
		s.res.MaxTerm = max(s.res.MaxTerm, term)
	}
}

// serve handles a client request arriving at a node.
func (s *sim) serve(n *node, r *request) {
	if n.raft.State() != raft.Leader {
		s.replyTo(r.client, r.attempt, &reply{ok: false, hint: n.raft.Lead()})
		return
	}
	if r.isRead {
		n.nextRid++
		id := n.nextRid
		if !n.raft.ReadIndex(id) {
			s.replyTo(r.client, r.attempt, &reply{ok: false})
			return
		}
		n.reads = append(n.reads, &pendingRead{id: id, client: r.client, attempt: r.attempt, key: r.cmd.Key})
		return
	}
	index, _, ok := n.raft.Propose(kv.Encode(r.cmd))
	if !ok {
		s.replyTo(r.client, r.attempt, &reply{ok: false})
		return
	}
	n.writes = append(n.writes, pendingWrite{index: index, client: r.client, attempt: r.attempt, clientID: r.cmd.Client, seq: r.cmd.Seq})
}

// ---------------------------------------------------------------- network

func (s *sim) reachable(a, b uint64) bool { return s.group[a] == s.group[b] }

func (s *sim) sendRaft(m raft.Message) {
	s.res.Messages++
	if !s.reachable(m.From, m.To) || s.rng.Float64() < s.cfg.DropRate {
		s.res.Dropped++
		return
	}
	copies := 1
	if s.rng.Float64() < s.cfg.DuplicateRate {
		copies = 2
	}
	for range copies {
		s.schedule(&event{at: s.now + s.delay(), kind: evDeliver, node: m.To, msg: cloneMsg(m)})
	}
}

func (s *sim) delay() int64 {
	if s.cfg.StallRate > 0 && s.rng.Float64() < s.cfg.StallRate {
		return s.cfg.MaxDelay + s.rng.Int64N(s.cfg.StallMax)
	}
	return s.cfg.MinDelay + s.rng.Int64N(s.cfg.MaxDelay-s.cfg.MinDelay+1)
}

// Client traffic crosses the same lossy network but is not partitioned:
// clients reach every node, as they would through a load balancer.
func (s *sim) replyTo(ci int, attempt uint64, r *reply) {
	if s.rng.Float64() < s.cfg.DropRate {
		return
	}
	s.schedule(&event{at: s.now + s.delay(), kind: evClientReply, client: ci, attempt: attempt, reply: r})
}

func cloneMsg(m raft.Message) raft.Message {
	m.Entries = slices.Clone(m.Entries)
	return m
}

// ---------------------------------------------------------------- clients

func (s *sim) clientStart(ci int) {
	c := s.clients[ci]
	if s.now >= s.cfg.Duration || c.busy {
		return
	}
	c.busy = true
	c.seq++
	key := fmt.Sprintf("k%d", s.rng.IntN(s.cfg.Keys))
	op := &check.Operation{Client: c.id, Key: key, Call: s.now}
	cmd := kv.Command{Key: key, Client: c.id, Seq: c.seq}
	switch x := s.rng.IntN(10); {
	case x < 4:
		op.Kind, cmd.Op = check.Get, kv.OpGet
	case x < 6:
		v := fmt.Sprintf("%d.%d", c.id, c.seq)
		op.Kind, cmd.Op, op.Value, cmd.Value = check.Put, kv.OpPut, v, v
	case x < 8:
		v := fmt.Sprintf("%d,", c.seq%10)
		op.Kind, cmd.Op, op.Value, cmd.Value = check.Append, kv.OpAppend, v, v
	default:
		v := fmt.Sprintf("%d.%d", c.id, c.seq)
		op.Kind, cmd.Op, op.Value, cmd.Value = check.CAS, kv.OpCAS, v, v
		// Compare against a value this client last saw - often stale on
		// purpose, so both outcomes of a CAS get exercised.
		op.ExpectAbsent = s.rng.IntN(3) == 0
		cmd.ExpectAbsent = op.ExpectAbsent
	}
	c.op, c.cmd = op, cmd
	c.deadline = s.now + s.cfg.OpDeadline
	s.clientSend(ci)
}

func (s *sim) clientSend(ci int) {
	c := s.clients[ci]
	c.attempt++
	req := &request{client: ci, attempt: c.attempt, cmd: c.cmd, isRead: c.cmd.Op == kv.OpGet}
	if s.rng.Float64() >= s.cfg.DropRate {
		s.schedule(&event{at: s.now + s.delay(), kind: evDeliver, node: c.leader, req: req})
	}
	s.schedule(&event{at: s.now + s.cfg.ClientTimeout, kind: evClientTimeout, client: ci, attempt: c.attempt})
}

func (s *sim) clientTimeout(ci int, attempt uint64) {
	c := s.clients[ci]
	if !c.busy || attempt != c.attempt {
		return
	}
	c.leader = uint64(s.rng.IntN(s.cfg.Nodes) + 1)
	s.retryOrGiveUp(ci)
}

func (s *sim) retryOrGiveUp(ci int) {
	c := s.clients[ci]
	if s.now >= c.deadline {
		// The outcome is unknown: the write may still be committed later by
		// a retry already in flight, or never.
		c.op.Unknown = true
		s.record(ci)
		return
	}
	s.clientSend(ci)
}

func (s *sim) clientReply(ci int, attempt uint64, r *reply) {
	c := s.clients[ci]
	if !c.busy || attempt != c.attempt {
		return
	}
	if !r.ok {
		if r.hint != raft.None {
			c.leader = r.hint
		} else {
			c.leader = uint64(s.rng.IntN(s.cfg.Nodes) + 1)
		}
		// A short pause before retrying, as a real client would.
		s.schedule(&event{at: s.now + 20, kind: evClientTimeout, client: ci, attempt: c.attempt})
		return
	}
	c.op.Return = s.now
	c.op.OutValue, c.op.OutFound, c.op.OutOK = r.result.Value, r.result.Found, r.result.OK
	s.record(ci)
}

func (s *sim) record(ci int) {
	c := s.clients[ci]
	s.history = append(s.history, *c.op)
	c.busy = false
	c.op = nil
	s.schedule(&event{at: s.now + s.rng.Int64N(60), kind: evClientStart, client: ci})
}

// ---------------------------------------------------------------- faults

func (s *sim) fault() {
	if s.now >= s.cfg.Duration {
		return
	}
	defer s.schedule(&event{at: s.now + s.faultGap(), kind: evFault})
	kinds := []string{}
	if !s.cfg.NoPartitions {
		kinds = append(kinds, "partition")
	}
	if !s.cfg.NoCrashes {
		kinds = append(kinds, "crash")
	}
	if len(kinds) == 0 {
		return
	}
	switch kinds[s.rng.IntN(len(kinds))] {
	case "partition":
		// Split the nodes into two groups at random; sometimes a single node
		// alone, sometimes the leader cut off with a minority.
		s.partition++
		clear(s.group)
		for _, n := range s.nodes {
			s.group[n.id] = s.rng.IntN(2)
		}
		s.res.Partitions++
		s.schedule(&event{at: s.now + s.scaled(300, 2_000), kind: evHeal, heal: s.partition})
	case "crash":
		var up []*node
		for _, n := range s.nodes {
			if n.alive {
				up = append(up, n)
			}
		}
		if len(up) == 0 {
			return
		}
		n := up[s.rng.IntN(len(up))]
		n.alive = false
		s.res.Crashes++
		// Everything in flight to it is lost; pending client requests on it
		// time out on the client side.
		n.writes, n.reads = nil, nil
		s.schedule(&event{at: s.now + s.scaled(200, 1_500), kind: evRestart, node: n.id})
	}
}

// ---------------------------------------------------------------- the verdict

func (s *sim) finish() {
	for _, op := range s.history {
		s.res.Operations++
		if op.Unknown {
			s.res.Unknown++
		} else {
			s.res.Completed++
		}
	}
	// Clients still mid-operation when time ran out: unknown outcomes.
	for _, c := range s.clients {
		if c.busy && c.op != nil {
			op := *c.op
			op.Unknown = true
			s.history = append(s.history, op)
			s.res.Operations++
			s.res.Unknown++
		}
	}
	s.res.Leaders = len(s.leaders)
	s.res.Check = check.Check(s.history, 20*time.Second)
	if !s.res.Check.Linearizable {
		s.problem("%s", strings.TrimSpace(s.res.Check.String()))
	}
	// Liveness, loosely: after the quiet period the cluster must have made
	// progress at all. A cluster that never commits is trivially "safe".
	if s.res.Completed == 0 {
		s.problem("no operation completed in the whole run")
	}
	// Every live node that applied the same prefix must hold the same data.
	digests := map[uint64]string{}
	for _, n := range s.nodes {
		if !n.alive {
			continue
		}
		a := n.store.Applied()
		d := n.store.Digest()
		if prev, ok := digests[a]; ok && prev != d {
			s.problem("replicas diverged at applied index %d", a)
		}
		digests[a] = d
	}
	s.res.Problems = s.problems
	s.res.OK = len(s.problems) == 0
}

func decode(b []byte, c *kv.Command) bool {
	return jsonUnmarshal(b, c) == nil
}
