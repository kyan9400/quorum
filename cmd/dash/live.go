package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kyan9400/quorum/client"
	"github.com/kyan9400/quorum/internal/check"
	"github.com/kyan9400/quorum/internal/cluster"
)

// live runs a cluster that visitors can break, a small workload against it,
// and a linearizability check of that workload, window by window.
//
// The check needs to know each key's state at the start of what it checks.
// Rather than check an ever-growing history, the workload moves to fresh keys
// every window: window 41 writes only keys w41.k0..k2, which start absent, so
// each window's history is complete on its own and can be checked and
// discarded once its last operation has returned or timed out. (The keys also
// carry a tag for this run of the dashboard: the first version reused window
// numbers across restarts, found last run's values in "fresh" keys, and the
// checker duly reported both windows as not linearizable.)
type live struct {
	c      *cluster.Cluster
	window time.Duration
	keys   int
	// tag starts every key: the data directory outlives the dashboard, and
	// a window's keys must be new to the cluster, not just to this process.
	tag     string
	opLimit time.Duration
	clients int
	pause   time.Duration // between a client's operations: pause plus up to pause more

	mu       sync.Mutex
	started  time.Time
	windows  map[int]*window
	results  []windowResult // checked, oldest first, the last maxWindows
	totals   totals
	recent   []recentOp // newest last, at most maxRecent
	perSec   map[int64]*secondStats
	lastAct  time.Time
	faultAt  time.Time // when the cluster last left a healthy state; zero when healthy
	autoHeal time.Duration
}

type window struct {
	ops      []check.Operation
	inFlight int
}

type windowResult struct {
	Window       int     `json:"window"`
	Start        float64 `json:"start"`
	Ops          int     `json:"ops"`
	Unknown      int     `json:"unknown"`
	Linearizable bool    `json:"linearizable"`
	Undecided    bool    `json:"undecided"` // the checker ran out of budget
	CheckMs      float64 `json:"check_ms"`
}

type totals struct {
	Ops          int `json:"ops"`
	Unknown      int `json:"unknown"`
	Windows      int `json:"windows"`
	Linearizable int `json:"linearizable"`
	Violations   int `json:"violations"`
	Undecided    int `json:"undecided"`
}

type recentOp struct {
	T       float64 `json:"t"`
	Client  int     `json:"client"`
	Pinned  int     `json:"pinned,omitempty"` // node the client is pinned to
	Kind    string  `json:"kind"`
	Key     string  `json:"key"`
	Value   string  `json:"value,omitempty"`
	Out     string  `json:"out,omitempty"`
	Unknown bool    `json:"unknown,omitempty"`
	Ms      float64 `json:"ms"`
}

type secondStats struct {
	ok, unknown int
	lat         []float64
}

const (
	maxWindows = 60
	maxRecent  = 40
)

func newLive(c *cluster.Cluster, span, autoHeal time.Duration) *live {
	return &live{
		c: c, window: span, keys: 3, tag: strconv.FormatInt(time.Now().Unix(), 36), opLimit: 6 * time.Second, pause: 120 * time.Millisecond, started: time.Now(),
		windows: map[int]*window{}, perSec: map[int64]*secondStats{}, autoHeal: autoHeal,
	}
}

func (l *live) since() float64 { return time.Since(l.started).Seconds() }

func (l *live) windowAt(t time.Time) int { return int(t.Sub(l.started) / l.window) }

// run starts the workload and the checker; it returns when ctx is done.
func (l *live) run(ctx context.Context, clients int) {
	l.clients = clients
	urls := l.c.URLs()
	for ci := 0; ci < clients; ci++ {
		pinned := 0
		var cl *client.Client
		if ci%2 == 1 {
			pinned = ci/2%len(urls) + 1
			cl = client.NewPinned(urls[pinned-1])
		} else {
			cl = client.New(urls)
		}
		go l.client(ctx, ci+1, pinned, cl)
	}
	go l.checker(ctx)
	go l.watchdog(ctx)
	<-ctx.Done()
}

func (l *live) client(ctx context.Context, id, pinned int, cl *client.Client) {
	r := rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano())))
	n := 0
	for ctx.Err() == nil {
		n++
		// The window is chosen under the lock: the checker, which takes the
		// same lock, only ever closes windows that have already ended.
		l.mu.Lock()
		now := time.Now()
		w := l.windowAt(now)
		key := fmt.Sprintf("%s.w%d.k%d", l.tag, w, r.IntN(l.keys))
		op := check.Operation{Client: cl.ID(), Key: key}
		win := l.windows[w]
		if win == nil {
			win = &window{}
			l.windows[w] = win
		}
		win.inFlight++
		op.Call = time.Since(l.started).Nanoseconds()
		l.mu.Unlock()

		octx, cancel := context.WithTimeout(ctx, l.opLimit)
		var res client.Result
		var err error
		switch x := r.IntN(10); {
		case x < 4:
			op.Kind = check.Get
			res, err = cl.Get(octx, key)
		case x < 6:
			op.Kind, op.Value = check.Put, fmt.Sprintf("c%d.%d", id, n)
			res, err = cl.Put(octx, key, op.Value)
		case x < 8:
			op.Kind, op.Value = check.Append, fmt.Sprintf("%d", n%10)
			res, err = cl.Append(octx, key, op.Value)
		default:
			op.Kind, op.Value = check.CAS, fmt.Sprintf("c%d.%d", id, n)
			op.ExpectAbsent = true
			res, err = cl.CAS(octx, key, true, "", op.Value)
		}
		cancel()
		ms := float64(time.Since(now).Microseconds()) / 1000
		rec := recentOp{T: l.since(), Client: id, Pinned: pinned, Kind: string(op.Kind), Key: key, Value: op.Value, Ms: ms}
		switch {
		case err == nil:
			op.Return = time.Since(l.started).Nanoseconds()
			op.OutValue, op.OutFound, op.OutOK = res.Value, res.Found, res.OK
			rec.Out = describe(op)
		case errors.Is(err, client.ErrUnknown):
			op.Unknown, rec.Unknown = true, true
		default:
			if ctx.Err() == nil {
				log.Printf("client %d: %v", id, err)
			}
		}
		l.mu.Lock()
		win.inFlight--
		if err == nil || op.Unknown {
			win.ops = append(win.ops, op)
			l.recent = append(l.recent, rec)
			if len(l.recent) > maxRecent {
				l.recent = l.recent[len(l.recent)-maxRecent:]
			}
			sec := time.Now().Unix()
			s := l.perSec[sec]
			if s == nil {
				s = &secondStats{}
				l.perSec[sec] = s
			}
			if op.Unknown {
				s.unknown++
			} else {
				s.ok++
				s.lat = append(s.lat, ms)
			}
		}
		l.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-time.After(l.pause + time.Duration(r.Int64N(int64(l.pause)+1))):
		}
	}
}

func describe(op check.Operation) string {
	switch op.Kind {
	case check.Get:
		if !op.OutFound {
			return "absent"
		}
		return fmt.Sprintf("%q", op.OutValue)
	case check.CAS:
		if op.OutOK {
			return "set"
		}
		return "taken"
	case check.Append:
		return fmt.Sprintf("%q", op.OutValue)
	}
	return "ok"
}

// checker checks each window once nothing can still be added to it: the
// window has ended and every operation begun in it has returned or timed out.
func (l *live) checker(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		l.mu.Lock()
		current := l.windowAt(time.Now())
		var ready []int
		for w, win := range l.windows {
			if w < current && win.inFlight == 0 {
				ready = append(ready, w)
			}
		}
		sort.Ints(ready)
		var batches [][]check.Operation
		for _, w := range ready {
			batches = append(batches, l.windows[w].ops)
			delete(l.windows, w)
		}
		cutoff := time.Now().Add(-90 * time.Second).Unix()
		for s := range l.perSec {
			if s < cutoff {
				delete(l.perSec, s)
			}
		}
		l.mu.Unlock()

		for i, w := range ready {
			ops := batches[i]
			start := time.Now()
			rep := check.Check(ops, 10*time.Second)
			res := windowResult{
				Window: w, Start: (time.Duration(w) * l.window).Seconds(), Ops: len(ops),
				Linearizable: rep.Linearizable, Undecided: rep.Unknown,
				CheckMs: float64(time.Since(start).Microseconds()) / 1000,
			}
			for _, op := range ops {
				if op.Unknown {
					res.Unknown++
				}
			}
			if !rep.Linearizable && !rep.Unknown {
				log.Printf("window %d NOT linearizable:\n%s", w, rep)
			}
			l.mu.Lock()
			l.results = append(l.results, res)
			if len(l.results) > maxWindows {
				l.results = l.results[len(l.results)-maxWindows:]
			}
			l.totals.Windows++
			l.totals.Ops += res.Ops
			l.totals.Unknown += res.Unknown
			switch {
			case res.Undecided:
				l.totals.Undecided++
			case res.Linearizable:
				l.totals.Linearizable++
			default:
				l.totals.Violations++
			}
			l.mu.Unlock()
		}
	}
}

// watchdog puts the cluster back together if visitors leave it broken, so the
// next visitor finds it working.
func (l *live) watchdog(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		broken := l.c.Groups() != nil
		for i := 1; i <= l.c.N(); i++ {
			if !l.c.Alive(i) {
				broken = true
			}
		}
		l.mu.Lock()
		if !broken {
			l.faultAt = time.Time{}
			l.mu.Unlock()
			continue
		}
		if l.faultAt.IsZero() {
			l.faultAt = time.Now()
		}
		due := time.Since(l.faultAt) > l.autoHeal && time.Since(l.lastAct) > l.autoHeal
		l.mu.Unlock()
		if due {
			l.c.Heal()
			for i := 1; i <= l.c.N(); i++ {
				if !l.c.Alive(i) {
					l.c.Start(i)
				}
			}
		}
	}
}

var errTooSoon = errors.New("one fault at a time: try again in a moment")

// act applies a visitor's fault. Actions are spaced out so that one visitor
// cannot hammer the cluster faster than anyone can watch it.
func (l *live) act(action string, node int) error {
	l.mu.Lock()
	if time.Since(l.lastAct) < 1500*time.Millisecond {
		l.mu.Unlock()
		return errTooSoon
	}
	l.lastAct = time.Now()
	l.mu.Unlock()

	n := l.c.N()
	if node != 0 && (node < 1 || node > n) {
		return fmt.Errorf("no node %d", node)
	}
	switch action {
	case "kill":
		if node == 0 {
			return errors.New("which node?")
		}
		if !l.c.Alive(node) {
			return fmt.Errorf("n%d is already down", node)
		}
		l.c.Kill(node, "")
	case "start":
		if node == 0 {
			l.c.StartAll()
		} else {
			l.c.Start(node)
		}
	case "isolate":
		if node == 0 {
			return errors.New("which node?")
		}
		l.c.Isolate(node, "")
	case "kill-leader":
		ld := l.c.Leader()
		if ld == 0 {
			return errors.New("there is no leader right now")
		}
		l.c.Kill(ld, "the leader")
	case "isolate-leader":
		ld := l.c.Leader()
		if ld == 0 {
			return errors.New("there is no leader right now")
		}
		l.c.Isolate(ld, "the leader")
	case "partition":
		perm := rand.Perm(n)
		cut := 1 + rand.IntN(n-1)
		var a, b []int
		for k, p := range perm {
			if k < cut {
				a = append(a, p+1)
			} else {
				b = append(b, p+1)
			}
		}
		sort.Ints(a)
		sort.Ints(b)
		l.c.Partition([][]int{a, b}, "")
	case "heal":
		l.c.Heal()
		for i := 1; i <= n; i++ {
			if !l.c.Alive(i) {
				l.c.Start(i)
			}
		}
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	return nil
}

// snapshot is what the page draws, sent four times a second.
type snapshot struct {
	T         float64          `json:"t"`
	Nodes     []cluster.Status `json:"nodes"`
	Cut       [][2]int         `json:"cut"`
	Groups    [][]int          `json:"groups"`
	Events    []cluster.Event  `json:"events"`
	Windows   []windowResult   `json:"windows"`
	Pending   []pendingWindow  `json:"pending"`
	Totals    totals           `json:"totals"`
	Recent    []recentOp       `json:"recent"`
	Rate      []rate           `json:"rate"` // the last 60 seconds
	WindowS   float64          `json:"window_s"`
	AutoHealS float64          `json:"auto_heal_s"`
	Clients   int              `json:"clients"`
	HealIn    float64          `json:"heal_in_s,omitempty"`
}

type pendingWindow struct {
	Window   int `json:"window"`
	Ops      int `json:"ops"`
	InFlight int `json:"in_flight"`
}

type rate struct {
	T       int64   `json:"t"` // seconds ago, 0 = the current second
	OK      int     `json:"ok"`
	Unknown int     `json:"unknown"`
	P50     float64 `json:"p50"`
	P99     float64 `json:"p99"`
}

func (l *live) snapshot() snapshot {
	s := snapshot{
		T: l.since(), Nodes: l.c.Statuses(), Cut: l.c.Cut(), Groups: l.c.Groups(),
		WindowS: l.window.Seconds(), AutoHealS: l.autoHeal.Seconds(), Clients: l.clients,
	}
	events := l.c.Events()
	if len(events) > 30 {
		events = events[len(events)-30:]
	}
	s.Events = events
	l.mu.Lock()
	defer l.mu.Unlock()
	s.Windows = append([]windowResult(nil), l.results...)
	s.Totals = l.totals
	s.Recent = append([]recentOp(nil), l.recent...)
	for w, win := range l.windows {
		s.Pending = append(s.Pending, pendingWindow{Window: w, Ops: len(win.ops), InFlight: win.inFlight})
	}
	sort.Slice(s.Pending, func(i, j int) bool { return s.Pending[i].Window < s.Pending[j].Window })
	now := time.Now().Unix()
	for ago := int64(59); ago >= 0; ago-- {
		r := rate{T: ago}
		if st := l.perSec[now-ago]; st != nil {
			r.OK, r.Unknown = st.ok, st.unknown
			if len(st.lat) > 0 {
				lat := append([]float64(nil), st.lat...)
				sort.Float64s(lat)
				r.P50 = lat[len(lat)/2]
				r.P99 = lat[min(len(lat)-1, int(float64(len(lat))*0.99))]
			}
		}
		s.Rate = append(s.Rate, r)
	}
	if !l.faultAt.IsZero() {
		since := min(time.Since(l.faultAt), time.Since(l.lastAct))
		s.HealIn = max(0, (l.autoHeal - since).Seconds())
	}
	return s
}
