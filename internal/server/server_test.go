package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/kyan9400/quorum/client"
)

type node struct {
	srv  *Server
	http *http.Server
	ln   net.Listener
	dir  string
}

type cluster struct {
	t     *testing.T
	urls  map[uint64]string
	addrs map[uint64]string
	nodes map[uint64]*node
	dirs  map[uint64]string
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, urls: map[uint64]string{}, addrs: map[uint64]string{}, nodes: map[uint64]*node{}, dirs: map[uint64]string{}}
	for i := uint64(1); i <= uint64(n); i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.addrs[i] = ln.Addr().String()
		c.urls[i] = "http://" + ln.Addr().String()
		ln.Close()
		c.dirs[i] = t.TempDir()
	}
	for i := uint64(1); i <= uint64(n); i++ {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *cluster) start(i uint64) {
	c.t.Helper()
	s, err := New(Config{ID: i, Peers: c.urls, DataDir: c.dirs[i], Fsync: false,
		Tick: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 2, SnapshotEvery: 50,
		RequestWait: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		c.t.Fatal(err)
	}
	var ln net.Listener
	for range 50 { // the port may take a moment to be released after a stop
		if ln, err = net.Listen("tcp", c.addrs[i]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	h := &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	go s.Run()
	c.nodes[i] = &node{srv: s, http: h, ln: ln}
}

// stop kills a node abruptly: no graceful shutdown of in-flight requests.
func (c *cluster) stop(i uint64) {
	n := c.nodes[i]
	if n == nil {
		return
	}
	n.http.Close()
	n.srv.Stop()
	delete(c.nodes, i)
}

func (c *cluster) client() *client.Client {
	var urls []string
	for _, u := range c.urls {
		urls = append(urls, u)
	}
	cl := client.New(urls)
	cl.SetAttemptTimeout(500 * time.Millisecond)
	return cl
}

func (c *cluster) leader() uint64 {
	for id, n := range c.nodes {
		if st := n.srv.status.Load(); st != nil && st.State == "leader" {
			return id
		}
	}
	return 0
}

func ctx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

func TestWritesAndReadsThroughARealCluster(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	if _, err := cl.Put(ctx(t, 10*time.Second), "a", "1"); err != nil {
		t.Fatal(err)
	}
	r, err := cl.Append(ctx(t, 5*time.Second), "a", "2")
	if err != nil || r.Value != "12" {
		t.Fatalf("append: %+v %v", r, err)
	}
	got, err := cl.Get(ctx(t, 5*time.Second), "a")
	if err != nil || got.Value != "12" || !got.Found {
		t.Fatalf("get: %+v %v", got, err)
	}
	won, _ := cl.CAS(ctx(t, 5*time.Second), "lock", true, "", "me")
	lost, _ := c.client().CAS(ctx(t, 5*time.Second), "lock", true, "", "you")
	if !won.OK || lost.OK {
		t.Fatalf("both or neither got the lock: %+v %+v", won, lost)
	}
}

func TestTheClusterKeepsWorkingWhenTheLeaderDies(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	if _, err := cl.Put(ctx(t, 10*time.Second), "k", "before"); err != nil {
		t.Fatal(err)
	}
	c.stop(c.leader())
	if _, err := cl.Put(ctx(t, 10*time.Second), "k", "after"); err != nil {
		t.Fatalf("no write after the leader died: %v", err)
	}
	got, err := cl.Get(ctx(t, 5*time.Second), "k")
	if err != nil || got.Value != "after" {
		t.Fatalf("got %+v %v", got, err)
	}
}

func TestANodeRestartedFromDiskHasEverythingItAcknowledged(t *testing.T) {
	c := newCluster(t, 3)
	cl := c.client()
	for i := range 120 { // past SnapshotEvery: the restart goes through a snapshot
		if _, err := cl.Append(ctx(t, 10*time.Second), "log", "x"); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	for id := range c.urls {
		c.stop(id)
	}
	for id := range c.urls {
		c.start(id)
	}
	got, err := c.client().Get(ctx(t, 10*time.Second), "log")
	if err != nil || len(got.Value) != 120 {
		t.Fatalf("after restarting every node: %d appends survived, err %v", len(got.Value), err)
	}
}

func TestAFollowerForwardsToTheLeader(t *testing.T) {
	c := newCluster(t, 3)
	if _, err := c.client().Put(ctx(t, 10*time.Second), "f", "1"); err != nil {
		t.Fatal(err)
	}
	var follower uint64
	for id := range c.nodes {
		if id != c.leader() {
			follower = id
			break
		}
	}
	pinned := client.NewPinned(c.urls[follower])
	pinned.SetAttemptTimeout(500 * time.Millisecond)
	if r, err := pinned.Append(ctx(t, 5*time.Second), "f", "2"); err != nil || r.Value != "12" {
		t.Fatalf("write through a follower: %+v %v", r, err)
	}
	if r, err := pinned.Get(ctx(t, 5*time.Second), "f"); err != nil || r.Value != "12" {
		t.Fatalf("read through a follower: %+v %v", r, err)
	}
}

func TestAClientGivesUpWithUnknownRatherThanAGuess(t *testing.T) {
	// With a majority gone nothing can commit. The honest answer is "unknown".
	c := newCluster(t, 3)
	cl := c.client()
	if _, err := cl.Put(ctx(t, 10*time.Second), "u", "1"); err != nil {
		t.Fatal(err)
	}
	l := c.leader()
	for id := range c.urls {
		if id != l {
			c.stop(id)
		}
	}
	_, err := cl.Put(ctx(t, 2*time.Second), "u", "2")
	if err != client.ErrUnknown {
		t.Fatalf("got %v, want ErrUnknown with no majority", err)
	}
}
