// Package cluster runs a Quorum cluster as real processes on one machine, with
// a network that can be cut: the Jepsen harness and the live dashboard both
// drive one.
//
// Every node-to-node link goes through a proxy of its own, one per direction,
// so a partition can be asymmetric. A cut link black-holes: the request hangs
// as a dropped packet does, then the connection is closed. Clients reach the
// nodes directly, as an application would.
package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Bin           string // quorumd binary
	Dir           string // data directories and logs go here
	N             int
	BasePort      int // node i listens on BasePort+i; link a->b on BasePort+100+a*10+b
	Fsync         bool
	Tick          string // raft tick, e.g. "50ms"
	SnapshotEvery int
	// OnEvent, if set, is called for every fault and recovery, in order.
	OnEvent func(Event)
}

// Event is one thing done to the cluster.
type Event struct {
	T      float64 `json:"t"` // seconds since the cluster started
	Kind   string  `json:"kind"`
	Node   int     `json:"node,omitempty"`
	Groups [][]int `json:"groups,omitempty"`
	Text   string  `json:"text"`
}

type Cluster struct {
	cfg   Config
	start time.Time

	mu     sync.Mutex
	procs  []*exec.Cmd
	exited []chan struct{}
	logs   []*os.File
	cut    map[[2]int]bool
	groups [][]int // the partition in force, if any
	lns    []net.Listener
	events []Event
}

func New(cfg Config) (*Cluster, error) {
	if cfg.Tick == "" {
		cfg.Tick = "50ms"
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 500
	}
	bin, err := filepath.Abs(cfg.Bin)
	if err != nil {
		return nil, err
	}
	cfg.Bin = bin
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	c := &Cluster{
		cfg: cfg, start: time.Now(), cut: map[[2]int]bool{},
		procs: make([]*exec.Cmd, cfg.N+1), exited: make([]chan struct{}, cfg.N+1), logs: make([]*os.File, cfg.N+1),
	}
	for i := 1; i <= cfg.N; i++ {
		f, err := os.OpenFile(filepath.Join(cfg.Dir, fmt.Sprintf("n%d.log", i)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644) // one run's output; the data directory itself persists
		if err != nil {
			return nil, err
		}
		c.logs[i] = f
	}
	if err := c.startProxies(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cluster) N() int                { return c.cfg.N }
func (c *Cluster) Since() float64        { return time.Since(c.start).Seconds() }
func (c *Cluster) URL(i int) string      { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.BasePort+i) }
func (c *Cluster) linkPort(a, b int) int { return c.cfg.BasePort + 100 + a*10 + b }

func (c *Cluster) URLs() []string {
	var u []string
	for i := 1; i <= c.cfg.N; i++ {
		u = append(u, c.URL(i))
	}
	return u
}

func (c *Cluster) record(e Event) {
	e.T = c.Since()
	c.events = append(c.events, e)
	if c.cfg.OnEvent != nil {
		c.cfg.OnEvent(e)
	}
}

// Events returns every event so far.
func (c *Cluster) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.events...)
}

func (c *Cluster) isCut(a, b int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cut[[2]int{a, b}]
}

func (c *Cluster) startProxies() error {
	for from := 1; from <= c.cfg.N; from++ {
		for to := 1; to <= c.cfg.N; to++ {
			if from == to {
				continue
			}
			target, _ := url.Parse(c.URL(to))
			rp := httputil.NewSingleHostReverseProxy(target)
			rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusBadGateway) }
			a, b := from, to
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.isCut(a, b) {
					select {
					case <-time.After(800 * time.Millisecond):
					case <-r.Context().Done():
					}
					if hj, ok := w.(http.Hijacker); ok {
						if conn, _, err := hj.Hijack(); err == nil {
							conn.Close()
							return
						}
					}
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				rp.ServeHTTP(w, r)
			})
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", c.linkPort(from, to)))
			if err != nil {
				return err
			}
			c.lns = append(c.lns, ln)
			go http.Serve(ln, handler)
		}
	}
	return nil
}

// Start launches node i from its data directory. Starting a running node does
// nothing.
func (c *Cluster) Start(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startLocked(i, true)
}

func (c *Cluster) startLocked(i int, log bool) error {
	if c.procs[i] != nil {
		return nil
	}
	var peers, advertise []string
	for j := 1; j <= c.cfg.N; j++ {
		if j == i {
			peers = append(peers, fmt.Sprintf("%d=%s", j, c.URL(j)))
		} else {
			peers = append(peers, fmt.Sprintf("%d=http://127.0.0.1:%d", j, c.linkPort(i, j)))
		}
		advertise = append(advertise, fmt.Sprintf("%d=%s", j, c.URL(j)))
	}
	cmd := exec.Command(c.cfg.Bin,
		"-id", fmt.Sprint(i), "-listen", fmt.Sprintf("127.0.0.1:%d", c.cfg.BasePort+i),
		"-peers", strings.Join(peers, ","), "-advertise", strings.Join(advertise, ","),
		"-data", filepath.Join(c.cfg.Dir, fmt.Sprintf("n%d", i)), "-tick", c.cfg.Tick,
		"-fsync="+fmt.Sprint(c.cfg.Fsync), "-snapshot-every", fmt.Sprint(c.cfg.SnapshotEvery))
	cmd.Stdout = c.logs[i]
	cmd.Stderr = c.logs[i]
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	c.procs[i], c.exited[i] = cmd, done
	if log {
		c.record(Event{Kind: "start", Node: i, Text: fmt.Sprintf("started n%d", i)})
	}
	return nil
}

// StartAll launches every node that is not running.
func (c *Cluster) StartAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var started []int
	for i := 1; i <= c.cfg.N; i++ {
		if c.procs[i] == nil && c.startLocked(i, false) == nil {
			started = append(started, i)
		}
	}
	switch len(started) {
	case 0:
	case c.cfg.N:
		c.record(Event{Kind: "start-all", Text: fmt.Sprintf("started %d nodes", c.cfg.N)})
	default:
		for _, i := range started {
			c.record(Event{Kind: "start", Node: i, Text: fmt.Sprintf("started n%d", i)})
		}
	}
}

// Kill sends SIGKILL (on Linux): no shutdown, no flush.
func (c *Cluster) Kill(i int, why string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.killLocked(i)
	text := fmt.Sprintf("kill -9 n%d", i)
	if why != "" {
		text = fmt.Sprintf("kill -9 %s n%d", why, i)
	}
	c.record(Event{Kind: "kill", Node: i, Text: text})
}

func (c *Cluster) killLocked(i int) {
	if p := c.procs[i]; p != nil {
		p.Process.Kill()
		<-c.exited[i]
		c.procs[i] = nil
	}
}

// RestartAll kills and restarts every node: the check that everything
// acknowledged survives on disk.
func (c *Cluster) RestartAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 1; i <= c.cfg.N; i++ {
		c.killLocked(i)
		c.startLocked(i, false)
	}
	c.record(Event{Kind: "restart-all", Text: "kill -9 and restart every node"})
}

func (c *Cluster) Alive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.procs[i] != nil
}

// Partition cuts every link between nodes in different groups, in both
// directions. Nodes missing from every group are in a group of their own.
func (c *Cluster) Partition(groups [][]int, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	group := map[int]int{}
	for g, members := range groups {
		for _, m := range members {
			group[m] = g + 1
		}
	}
	for i := 1; i <= c.cfg.N; i++ {
		if group[i] == 0 {
			group[i] = -i
		}
	}
	clear(c.cut)
	for a := 1; a <= c.cfg.N; a++ {
		for b := 1; b <= c.cfg.N; b++ {
			if a != b && group[a] != group[b] {
				c.cut[[2]int{a, b}] = true
			}
		}
	}
	c.groups = groups
	if text == "" {
		var parts []string
		for _, g := range groups {
			parts = append(parts, fmt.Sprint(g))
		}
		text = "partition " + strings.Join(parts, " | ")
	}
	c.record(Event{Kind: "partition", Groups: groups, Text: text})
}

// Isolate cuts node i off from every other node.
func (c *Cluster) Isolate(i int, why string) {
	var rest []int
	for j := 1; j <= c.cfg.N; j++ {
		if j != i {
			rest = append(rest, j)
		}
	}
	text := fmt.Sprintf("isolated n%d", i)
	if why != "" {
		text = fmt.Sprintf("isolated %s n%d", why, i)
	}
	c.Partition([][]int{{i}, rest}, text)
}

// Heal restores every link. It records an event only if something was cut.
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cut) == 0 {
		return
	}
	clear(c.cut)
	c.groups = nil
	c.record(Event{Kind: "heal", Text: "healed the network"})
}

// Groups is the partition in force, nil when the network is whole.
func (c *Cluster) Groups() [][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups
}

// Cut lists the directed links that are down.
func (c *Cluster) Cut() [][2]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out [][2]int
	for k := range c.cut {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i][0] < out[j][0] || out[i][0] == out[j][0] && out[i][1] < out[j][1]
	})
	return out
}

func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 1; i <= c.cfg.N; i++ {
		c.killLocked(i)
	}
	for _, ln := range c.lns {
		ln.Close()
	}
	for _, f := range c.logs {
		if f != nil {
			f.Close()
		}
	}
}

// Status is what a node reports about itself, plus whether it answered.
type Status struct {
	ID       int                     `json:"id"`
	Up       bool                    `json:"up"`       // the process is running
	Answered bool                    `json:"answered"` // and its status endpoint replied
	State    string                  `json:"state,omitempty"`
	Term     uint64                  `json:"term,omitempty"`
	Lead     uint64                  `json:"lead,omitempty"`
	Commit   uint64                  `json:"commit,omitempty"`
	Applied  uint64                  `json:"applied,omitempty"`
	Last     uint64                  `json:"last_index,omitempty"`
	Snapshot uint64                  `json:"snapshot_index,omitempty"`
	Keys     int                     `json:"keys,omitempty"`
	Fsyncs   uint64                  `json:"fsyncs,omitempty"`
	Recov    int64                   `json:"recovered_torn_bytes,omitempty"`
	Progress map[string]ProgressView `json:"progress,omitempty"`
}

type ProgressView struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
}

var statusClient = &http.Client{Timeout: 300 * time.Millisecond}

// Status asks node i directly - not through a proxy, so a partition does not
// hide it.
func (c *Cluster) Status(i int) Status {
	st := Status{ID: i, Up: c.Alive(i)}
	if !st.Up {
		return st
	}
	resp, err := statusClient.Get(c.URL(i) + "/status")
	if err != nil {
		return st
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if json.Unmarshal(body, &st) == nil && st.State != "" {
		st.Answered = true
	}
	st.ID, st.Up = i, true
	return st
}

// Statuses asks every node at once.
func (c *Cluster) Statuses() []Status {
	out := make([]Status, c.cfg.N)
	var wg sync.WaitGroup
	for i := 1; i <= c.cfg.N; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); out[i-1] = c.Status(i) }()
	}
	wg.Wait()
	return out
}

// Leader is the node that says it leads in the highest term, or 0.
func (c *Cluster) Leader() int {
	best, term := 0, uint64(0)
	for _, s := range c.Statuses() {
		if s.State == "leader" && s.Term >= term {
			best, term = s.ID, s.Term
		}
	}
	return best
}

func (c *Cluster) WaitForLeader(limit time.Duration) bool {
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if c.Leader() != 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
