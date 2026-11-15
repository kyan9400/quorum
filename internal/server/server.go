// Package server runs one Raft node for real: a goroutine that owns the node,
// a write-ahead log on disk, peers reached over HTTP, and a client API.
//
// The node stays single-threaded exactly as in the simulator. One goroutine -
// the loop - owns the raft.Node, the state machine and the storage; ticks,
// peer messages, client writes and client reads all arrive on channels, and
// every Ready is handled in the order the contract demands: fsync, then send,
// then apply. Nothing else touches the node, so there is no locking to get
// wrong around the part that has to be right.
package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kyan9400/quorum/internal/kv"
	"github.com/kyan9400/quorum/internal/raft"
	"github.com/kyan9400/quorum/internal/storage"
)

type Config struct {
	ID    uint64
	Peers map[uint64]string // id -> base URL other nodes reach it at (this node included)
	// Advertise is the URL clients should be redirected to for each node;
	// defaults to Peers.
	Advertise map[uint64]string
	DataDir   string
	Fsync     bool

	Tick          time.Duration
	ElectionTick  int
	HeartbeatTick int
	SnapshotEvery uint64
	RequestWait   time.Duration // how long a client request waits for its outcome
}

func (c *Config) defaults() {
	if c.Tick == 0 {
		c.Tick = 50 * time.Millisecond
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 2
	}
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 10_000
	}
	if c.RequestWait == 0 {
		c.RequestWait = 3 * time.Second
	}
	if c.Advertise == nil {
		c.Advertise = c.Peers
	}
}

type outcome struct {
	result  kv.Result
	ok      bool   // false: not leader, or the entry was lost to a leadership change
	leader  uint64 // hint when not ok
	unknown bool   // gave up waiting
}

type writeReq struct {
	cmd  kv.Command
	resp chan outcome
}

type readReq struct {
	key  string
	resp chan outcome
}

type waitingWrite struct {
	index  uint64
	client uint64
	seq    uint64
	resp   chan outcome
}

type waitingRead struct {
	id    uint64
	key   string
	index uint64
	ready bool
	resp  chan outcome
}

type Server struct {
	cfg     Config
	log     *slog.Logger
	node    *raft.Node
	store   *kv.Store
	storage *storage.Storage

	inbox  chan raft.Message
	writes chan writeReq
	reads  chan readReq

	pendingWrites []waitingWrite
	pendingReads  []*waitingRead
	nextRead      uint64
	wasLeader     bool

	peers     map[uint64]*sender
	forwarder *http.Client
	status    atomic.Pointer[Status]
	stop      chan struct{}
	stopped   chan struct{}

	// Counters for the status page; written by the loop only.
	applied, proposed, fsyncs, snapshots uint64
	fsyncTotal                           time.Duration
}

type Status struct {
	raft.Status
	Keys      int               `json:"keys"`
	Proposed  uint64            `json:"proposed"`
	Fsyncs    uint64            `json:"fsyncs"`
	FsyncMean float64           `json:"fsync_mean_ms"`
	Snapshots uint64            `json:"snapshots_taken"`
	Peers     map[uint64]string `json:"peers"`
	Recovered int64             `json:"recovered_torn_bytes"`
	At        time.Time         `json:"at"`
}

func New(cfg Config, log *slog.Logger) (*Server, error) {
	cfg.defaults()
	st, restore, err := storage.Open(cfg.DataDir, cfg.Fsync)
	if err != nil {
		return nil, fmt.Errorf("opening storage: %w", err)
	}
	if st.Recovered > 0 {
		log.Warn("cut a torn record off the end of the log", "bytes", st.Recovered)
	}
	ids := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s := &Server{
		cfg: cfg, log: log, storage: st,
		node: raft.New(raft.Config{
			ID: cfg.ID, Peers: ids, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
			Seed: uint64(time.Now().UnixNano()), MaxEntriesPerMsg: 256,
		}, restore),
		store:     kv.New(),
		inbox:     make(chan raft.Message, 4096),
		writes:    make(chan writeReq, 1024),
		reads:     make(chan readReq, 1024),
		peers:     map[uint64]*sender{},
		forwarder: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 32, IdleConnTimeout: 3 * time.Second}},
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	if restore.Snapshot != nil {
		store, err := kv.Restore(restore.Snapshot.Data)
		if err != nil {
			return nil, fmt.Errorf("restoring snapshot: %w", err)
		}
		s.store = store
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4, IdleConnTimeout: 3 * time.Second}}
	for id, url := range cfg.Peers {
		if id != cfg.ID {
			s.peers[id] = newSender(id, url, client, log)
		}
	}
	s.publish()
	return s, nil
}

func (s *Server) Run() {
	defer close(s.stopped)
	for _, p := range s.peers {
		go p.run(s.stop)
	}
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			s.storage.Close()
			return
		case <-ticker.C:
			s.node.Tick()
		case m := <-s.inbox:
			s.node.Step(m)
			// Take everything else already waiting before handling Ready, so
			// one fsync covers a burst.
			s.drainInbox()
		case w := <-s.writes:
			s.propose(w)
			s.drainWrites()
		case r := <-s.reads:
			s.read(r)
		}
		s.handleReady()
	}
}

func (s *Server) Stop() {
	close(s.stop)
	<-s.stopped
}

func (s *Server) drainInbox() {
	for range 256 {
		select {
		case m := <-s.inbox:
			s.node.Step(m)
		default:
			return
		}
	}
}

func (s *Server) drainWrites() {
	for range 256 {
		select {
		case w := <-s.writes:
			s.propose(w)
		default:
			return
		}
	}
}

func (s *Server) propose(w writeReq) {
	index, _, ok := s.node.Propose(kv.Encode(w.cmd))
	if !ok {
		w.resp <- outcome{leader: s.node.Lead()}
		return
	}
	s.proposed++
	s.pendingWrites = append(s.pendingWrites, waitingWrite{index: index, client: w.cmd.Client, seq: w.cmd.Seq, resp: w.resp})
}

func (s *Server) read(r readReq) {
	s.nextRead++
	if !s.node.ReadIndex(s.nextRead) {
		r.resp <- outcome{leader: s.node.Lead()}
		return
	}
	s.pendingReads = append(s.pendingReads, &waitingRead{id: s.nextRead, key: r.key, resp: r.resp})
}

func (s *Server) handleReady() {
	for {
		rd := s.node.Ready()
		if rd.Empty() {
			break
		}
		start := time.Now()
		if err := s.storage.Save(rd); err != nil {
			// A node that cannot persist must not send: anything it sent
			// would be a promise it cannot keep. Stop hard.
			s.log.Error("persisting failed; stopping", "error", err)
			panic(err)
		}
		if len(rd.Entries) > 0 || rd.HardStateChanged {
			s.fsyncs++
			s.fsyncTotal += time.Since(start)
		}
		if rd.Snapshot != nil {
			store, err := kv.Restore(rd.Snapshot.Data)
			if err != nil {
				panic(fmt.Errorf("a snapshot from the leader does not decode: %w", err))
			}
			s.store = store
			s.failWritesThrough(rd.Snapshot.Index)
		}
		for _, m := range rd.Messages {
			if p := s.peers[m.To]; p != nil {
				p.send(m)
			}
		}
		for _, e := range rd.CommittedEntries {
			s.apply(e)
		}
		for _, r := range rd.ReadStates {
			for _, w := range s.pendingReads {
				if w.id == r.ID {
					w.index, w.ready = r.Index, true
				}
			}
		}
		s.node.Advance(rd)
		s.serveReads()
		s.maybeCompact()
	}
	isLeader := s.node.State() == raft.Leader
	if s.wasLeader && !isLeader {
		// Raft drops unconfirmed reads when it loses leadership; tell their
		// clients now rather than let them wait out a timeout.
		for _, w := range s.pendingReads {
			w.resp <- outcome{leader: s.node.Lead()}
		}
		s.pendingReads = nil
	}
	s.wasLeader = isLeader
	s.publish()
}

func (s *Server) apply(e raft.Entry) {
	if s.store.Applied() >= e.Index {
		return
	}
	result, _ := s.store.Apply(e.Index, e.Data)
	s.applied++
	kept := s.pendingWrites[:0]
	for _, w := range s.pendingWrites {
		if w.index != e.Index {
			kept = append(kept, w)
			continue
		}
		var c kv.Command
		mine := len(e.Data) > 0 && json.Unmarshal(e.Data, &c) == nil && c.Client == w.client && c.Seq == w.seq
		w.resp <- outcome{result: result, ok: mine, leader: s.node.Lead()}
	}
	s.pendingWrites = kept
}

func (s *Server) failWritesThrough(index uint64) {
	kept := s.pendingWrites[:0]
	for _, w := range s.pendingWrites {
		if w.index <= index {
			w.resp <- outcome{leader: s.node.Lead()}
			continue
		}
		kept = append(kept, w)
	}
	s.pendingWrites = kept
}

func (s *Server) serveReads() {
	kept := s.pendingReads[:0]
	for _, w := range s.pendingReads {
		if w.ready && s.store.Applied() >= w.index {
			v, found := s.store.Get(w.key)
			w.resp <- outcome{ok: true, result: kv.Result{Value: v, Found: found, OK: true}}
			continue
		}
		kept = append(kept, w)
	}
	s.pendingReads = kept
}

func (s *Server) maybeCompact() {
	applied := s.store.Applied()
	if applied < s.node.Status().Snapshot+s.cfg.SnapshotEvery {
		return
	}
	data := s.store.Snapshot()
	s.node.Compact(applied, data)
	snap := &raft.Snapshot{Index: applied, Term: s.node.SnapshotTerm(), Data: data}
	if err := s.storage.Compact(snap); err != nil {
		panic(fmt.Errorf("compacting: %w", err))
	}
	s.snapshots++
}

func (s *Server) publish() {
	st := &Status{
		Status: s.node.Status(), Keys: s.store.Len(), Proposed: s.proposed, Fsyncs: s.fsyncs,
		Snapshots: s.snapshots, Peers: s.cfg.Advertise, Recovered: s.storage.Recovered, At: time.Now(),
	}
	if s.fsyncs > 0 {
		st.FsyncMean = float64(s.fsyncTotal.Microseconds()) / float64(s.fsyncs) / 1000
	}
	s.status.Store(st)
}

// ----------------------------------------------------------------- HTTP

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", s.handleRaft)
	mux.HandleFunc("POST /kv", s.handleKV)
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.status.Load())
	})
	return mux
}

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	var batch []raft.Message
	if err := gob.NewDecoder(io.LimitReader(r.Body, 256<<20)).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, m := range batch {
		if m.To != s.cfg.ID {
			continue
		}
		select {
		case s.inbox <- m:
		default:
			// The loop is behind. Dropping is safe - Raft retransmits - and
			// blocking here would let a slow node stall its peers' senders.
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// ClientReply is the client API's answer.
type ClientReply struct {
	Value  string `json:"value"`
	Found  bool   `json:"found"`
	OK     bool   `json:"ok"`
	Leader uint64 `json:"leader,omitempty"`
	URL    string `json:"leader_url,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	var c kv.Command
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&c); err != nil || c.Key == "" {
		reply(w, http.StatusBadRequest, ClientReply{Error: "body must be a command with a key"})
		return
	}
	resp := make(chan outcome, 1)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestWait)
	defer cancel()
	if c.Op == kv.OpGet {
		select {
		case s.reads <- readReq{key: c.Key, resp: resp}:
		case <-ctx.Done():
			reply(w, http.StatusServiceUnavailable, ClientReply{Error: "node busy"})
			return
		}
	} else {
		if c.Client == 0 || c.Seq == 0 {
			reply(w, http.StatusBadRequest, ClientReply{Error: "writes need a client id and a sequence number"})
			return
		}
		select {
		case s.writes <- writeReq{cmd: c, resp: resp}:
		case <-ctx.Done():
			reply(w, http.StatusServiceUnavailable, ClientReply{Error: "node busy"})
			return
		}
	}
	select {
	case o := <-resp:
		if !o.ok && o.leader != raft.None && o.leader != s.cfg.ID && r.Header.Get(forwardedHeader) == "" {
			// Forward to the leader, once. It goes over the peer links, so a
			// partition that cuts this node off from the leader cuts
			// forwarding too - a client pinned to a node on the minority side
			// gets no answer rather than a wrong one.
			s.forward(ctx, w, o.leader, c)
			return
		}
		if !o.ok {
			rep := ClientReply{Leader: o.leader, Error: "not the leader, or the write was lost to a leadership change; retry"}
			if o.leader != raft.None {
				rep.URL = s.cfg.Advertise[o.leader]
			}
			reply(w, http.StatusMisdirectedRequest, rep)
			return
		}
		reply(w, http.StatusOK, ClientReply{Value: o.result.Value, Found: o.result.Found, OK: o.result.OK})
	case <-ctx.Done():
		// The write may still commit. The client must retry with the same
		// sequence number; the state machine will recognise it.
		reply(w, http.StatusGatewayTimeout, ClientReply{Error: "outcome unknown; retry with the same sequence number"})
	}
}

const forwardedHeader = "X-Quorum-Forwarded"

func (s *Server) forward(ctx context.Context, w http.ResponseWriter, leader uint64, c kv.Command) {
	body, _ := json.Marshal(c)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Peers[leader]+"/kv", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedHeader, fmt.Sprint(s.cfg.ID))
	resp, err := s.forwarder.Do(req)
	if err != nil {
		reply(w, http.StatusGatewayTimeout, ClientReply{Error: "could not reach the leader; outcome unknown, retry with the same sequence number"})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func reply(w http.ResponseWriter, status int, r ClientReply) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(r)
}

// ----------------------------------------------------------------- peers

// sender delivers one peer's messages. Batches whatever is queued into one
// request; drops when the queue is full - Raft retransmits, and a peer that is
// down must not make its sender's queue grow without bound.
type sender struct {
	id     uint64
	url    string
	client *http.Client
	log    *slog.Logger
	queue  chan raft.Message
	mu     sync.Mutex
	failed int
}

func newSender(id uint64, url string, client *http.Client, log *slog.Logger) *sender {
	return &sender{id: id, url: url, client: client, log: log, queue: make(chan raft.Message, 1024)}
}

func (p *sender) send(m raft.Message) {
	select {
	case p.queue <- m:
	default:
	}
}

func (p *sender) run(stop chan struct{}) {
	for {
		var batch []raft.Message
		select {
		case <-stop:
			return
		case m := <-p.queue:
			batch = append(batch, m)
		}
	more:
		for len(batch) < 256 {
			select {
			case m := <-p.queue:
				batch = append(batch, m)
			default:
				break more
			}
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
			p.log.Error("encoding raft messages", "error", err)
			continue
		}
		resp, err := p.client.Post(p.url+"/raft", "application/octet-stream", &buf)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if err != nil || resp.StatusCode >= 300 {
			p.failed++
			if p.failed == 1 || p.failed%100 == 0 {
				p.log.Debug("peer unreachable", "peer", p.id, "failures", p.failed)
			}
			// Back off briefly so a dead peer does not spin a core.
			select {
			case <-time.After(20 * time.Millisecond):
			case <-stop:
				return
			}
			continue
		}
		p.failed = 0
	}
}

// ParsePeers reads "1=http://a:7001,2=http://b:7001".
func ParsePeers(spec string) (map[uint64]string, error) {
	out := map[uint64]string{}
	for _, part := range bytes.Split([]byte(spec), []byte(",")) {
		kv := bytes.SplitN(bytes.TrimSpace(part), []byte("="), 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("peer %q is not id=url", part)
		}
		id, err := strconv.ParseUint(string(kv[0]), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("peer id %q", kv[0])
		}
		out[id] = string(kv[1])
	}
	if len(out) == 0 {
		return nil, errors.New("no peers")
	}
	return out, nil
}
