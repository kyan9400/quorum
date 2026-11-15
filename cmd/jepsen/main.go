// Command jepsen tests a real cluster the way Jepsen does: real processes, a
// network it can cut, kill -9, and a linearizability check of everything the
// clients saw.
//
// It starts N quorumd processes, each with its own data directory, and routes
// every node-to-node link through a proxy of its own - one per direction, so a
// partition can be asymmetric. Clients reach the nodes directly, as an
// application would. A nemesis then partitions the cluster, isolates the
// leader, and kills nodes outright and restarts them from their data
// directories, while clients read, write, append and compare-and-set. At the
// end the network is healed, every node is restarted, and the history is
// checked.
//
//	jepsen -bin ./quorumd -duration 2m -out run1
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kyan9400/quorum/client"
	"github.com/kyan9400/quorum/internal/check"
)

type cluster struct {
	bin      string
	dir      string
	n        int
	nodePort func(int) int
	linkPort func(from, to int) int
	procs    []*exec.Cmd
	logs     []*os.File
	cut      sync.Map // "from-to" -> true
	fsync    bool
	tick     string
	events   *eventLog
}

type eventLog struct {
	mu    sync.Mutex
	start time.Time
	lines []string
}

func (e *eventLog) add(format string, a ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	line := fmt.Sprintf("%7.2fs  %s", time.Since(e.start).Seconds(), fmt.Sprintf(format, a...))
	e.lines = append(e.lines, line)
	log.Print(line)
}

func (c *cluster) url(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", c.nodePort(i)) }

// startProxies runs one reverse proxy per directed link. A cut link
// black-holes: the request hangs, as a dropped packet does, then fails.
func (c *cluster) startProxies() {
	for from := 1; from <= c.n; from++ {
		for to := 1; to <= c.n; to++ {
			if from == to {
				continue
			}
			target, _ := url.Parse(c.url(to))
			rp := httputil.NewSingleHostReverseProxy(target)
			rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusBadGateway) }
			key := fmt.Sprintf("%d-%d", from, to)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, cut := c.cut.Load(key); cut {
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
				log.Fatal(err)
			}
			go http.Serve(ln, handler)
		}
	}
}

func (c *cluster) start(i int) {
	var peers, advertise []string
	for j := 1; j <= c.n; j++ {
		if j == i {
			peers = append(peers, fmt.Sprintf("%d=%s", j, c.url(j)))
		} else {
			peers = append(peers, fmt.Sprintf("%d=http://127.0.0.1:%d", j, c.linkPort(i, j)))
		}
		advertise = append(advertise, fmt.Sprintf("%d=%s", j, c.url(j)))
	}
	cmd := exec.Command(c.bin,
		"-id", fmt.Sprint(i), "-listen", fmt.Sprintf("127.0.0.1:%d", c.nodePort(i)),
		"-peers", strings.Join(peers, ","), "-advertise", strings.Join(advertise, ","),
		"-data", filepath.Join(c.dir, fmt.Sprintf("n%d", i)), "-tick", c.tick,
		"-fsync="+fmt.Sprint(c.fsync), "-snapshot-every", "500")
	cmd.Stdout = c.logs[i]
	cmd.Stderr = c.logs[i]
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	c.procs[i] = cmd
}

func (c *cluster) kill(i int) {
	if p := c.procs[i]; p != nil && p.Process != nil {
		p.Process.Kill() // SIGKILL on Linux: no shutdown, no flush
		p.Wait()
		c.procs[i] = nil
	}
}

func (c *cluster) alive(i int) bool { return c.procs[i] != nil }

type status struct {
	ID     uint64 `json:"id"`
	State  string `json:"state"`
	Term   uint64 `json:"term"`
	Lead   uint64 `json:"lead"`
	Commit uint64 `json:"commit"`
	Recov  int64  `json:"recovered_torn_bytes"`
}

func (c *cluster) status(i int) (status, bool) {
	cl := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := cl.Get(c.url(i) + "/status")
	if err != nil {
		return status{}, false
	}
	defer resp.Body.Close()
	var s status
	if json.NewDecoder(resp.Body).Decode(&s) != nil {
		return status{}, false
	}
	return s, true
}

func (c *cluster) leader() int {
	for i := 1; i <= c.n; i++ {
		if s, ok := c.status(i); ok && s.State == "leader" {
			return i
		}
	}
	return 0
}

func (c *cluster) partition(groups [][]int) {
	group := map[int]int{}
	for g, members := range groups {
		for _, m := range members {
			group[m] = g
		}
	}
	for a := 1; a <= c.n; a++ {
		for b := 1; b <= c.n; b++ {
			if a != b && group[a] != group[b] {
				c.cut.Store(fmt.Sprintf("%d-%d", a, b), true)
			}
		}
	}
}

func (c *cluster) heal() { c.cut.Clear() }

func main() {
	bin := flag.String("bin", "./quorumd", "quorumd binary")
	out := flag.String("out", "jepsen-run", "directory for data, logs and the history")
	nodes := flag.Int("nodes", 5, "cluster size")
	clients := flag.Int("clients", 10, "concurrent clients")
	keys := flag.Int("keys", 5, "keys the clients share")
	duration := flag.Duration("duration", 2*time.Minute, "how long the nemesis and clients run")
	every := flag.Duration("nemesis-every", 5*time.Second, "average time between faults")
	base := flag.Int("port", 17100, "first port")
	fsync := flag.Bool("fsync", true, "fsync on the nodes")
	tick := flag.String("tick", "50ms", "raft tick on the nodes")
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "nemesis and workload seed")
	flag.Parse()

	abs, err := filepath.Abs(*bin)
	if err != nil {
		log.Fatal(err)
	}
	os.RemoveAll(*out)
	os.MkdirAll(*out, 0o755)
	events := &eventLog{start: time.Now()}
	c := &cluster{
		bin: abs, dir: *out, n: *nodes, fsync: *fsync, tick: *tick, events: events,
		nodePort: func(i int) int { return *base + i },
		linkPort: func(from, to int) int { return *base + 100 + from*10 + to },
		procs:    make([]*exec.Cmd, *nodes+1),
		logs:     make([]*os.File, *nodes+1),
	}
	for i := 1; i <= c.n; i++ {
		c.logs[i], _ = os.Create(filepath.Join(*out, fmt.Sprintf("n%d.log", i)))
	}
	c.startProxies()
	for i := 1; i <= c.n; i++ {
		c.start(i)
	}
	defer func() {
		for i := 1; i <= c.n; i++ {
			c.kill(i)
		}
	}()
	events.add("started %d nodes, seed %d, fsync %v", c.n, *seed, *fsync)
	waitForLeader(c, 20*time.Second)

	rng := rand.New(rand.NewPCG(*seed, 1))
	var urls []string
	for i := 1; i <= c.n; i++ {
		urls = append(urls, c.url(i))
	}

	var (
		mu      sync.Mutex
		history []check.Operation
		ok, unk atomic.Int64
	)
	start := time.Now()
	stamp := func() int64 { return time.Since(start).Nanoseconds() }
	stopClients := make(chan struct{})
	var wg sync.WaitGroup
	for ci := 0; ci < *clients; ci++ {
		wg.Add(1)
		cseed := rng.Uint64()
		ci := ci
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(cseed, 2))
			// Half the clients follow the leader; half are pinned to one node
			// each, which forwards. A test with only leader-followers never
			// asks a minority-side node anything - the first version of this
			// harness missed a follower answering reads from stale local
			// state for exactly that reason.
			var cl *client.Client
			if ci%2 == 1 {
				cl = client.NewPinned(urls[ci/2%len(urls)])
			} else {
				cl = client.New(urls)
			}
			n := 0
			for {
				select {
				case <-stopClients:
					return
				default:
				}
				n++
				key := fmt.Sprintf("k%d", r.IntN(*keys))
				op := check.Operation{Client: cl.ID(), Key: key}
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				op.Call = stamp()
				var res client.Result
				var err error
				switch x := r.IntN(10); {
				case x < 4:
					op.Kind = check.Get
					res, err = cl.Get(ctx, key)
				case x < 6:
					op.Kind, op.Value = check.Put, fmt.Sprintf("%x.%d", cl.ID()%0xffff, n)
					res, err = cl.Put(ctx, key, op.Value)
				case x < 8:
					op.Kind, op.Value = check.Append, fmt.Sprintf("%d,", n%10)
					res, err = cl.Append(ctx, key, op.Value)
				default:
					op.Kind, op.Value = check.CAS, fmt.Sprintf("%x.%d", cl.ID()%0xffff, n)
					op.ExpectAbsent = r.IntN(3) == 0
					res, err = cl.CAS(ctx, key, op.ExpectAbsent, "", op.Value)
				}
				cancel()
				switch {
				case err == nil:
					op.Return = stamp()
					op.OutValue, op.OutFound, op.OutOK = res.Value, res.Found, res.OK
					ok.Add(1)
				case errors.Is(err, client.ErrUnknown):
					op.Unknown = true
					unk.Add(1)
				default:
					log.Printf("client error: %v", err)
					continue
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
				time.Sleep(time.Duration(r.IntN(30)) * time.Millisecond)
			}
		}()
	}

	// The nemesis.
	deadline := time.Now().Add(*duration)
	faults := map[string]int{}
	for time.Now().Before(deadline) {
		time.Sleep(*every/2 + time.Duration(rng.Int64N(int64(*every))))
		if !time.Now().Before(deadline) {
			break
		}
		c.heal()
		for i := 1; i <= c.n; i++ {
			if !c.alive(i) {
				c.start(i)
				events.add("restarted n%d", i)
			}
		}
		switch rng.IntN(5) {
		case 0, 1:
			perm := rng.Perm(c.n)
			cut := 1 + rng.IntN(c.n-1)
			var a, b []int
			for k, p := range perm {
				if k < cut {
					a = append(a, p+1)
				} else {
					b = append(b, p+1)
				}
			}
			c.partition([][]int{a, b})
			faults["partition"]++
			events.add("partition %v | %v", a, b)
		case 2:
			if l := c.leader(); l != 0 {
				var rest []int
				for i := 1; i <= c.n; i++ {
					if i != l {
						rest = append(rest, i)
					}
				}
				c.partition([][]int{{l}, rest})
				faults["isolate-leader"]++
				events.add("isolated the leader n%d", l)
			}
		case 3:
			i := 1 + rng.IntN(c.n)
			c.kill(i)
			faults["kill"]++
			events.add("kill -9 n%d", i)
		case 4:
			if l := c.leader(); l != 0 {
				c.kill(l)
				faults["kill-leader"]++
				events.add("kill -9 the leader n%d", l)
			}
		}
	}
	c.heal()
	for i := 1; i <= c.n; i++ {
		if !c.alive(i) {
			c.start(i)
			events.add("restarted n%d", i)
		}
	}
	events.add("healed; letting clients finish")
	time.Sleep(5 * time.Second)
	close(stopClients)
	wg.Wait()

	// Every node restarted from disk at least once more, then all must agree.
	recovered := int64(0)
	for i := 1; i <= c.n; i++ {
		c.kill(i)
		c.start(i)
	}
	waitForLeader(c, 20*time.Second)
	time.Sleep(3 * time.Second)
	commits := map[uint64]int{}
	for i := 1; i <= c.n; i++ {
		if s, ok := c.status(i); ok {
			commits[s.Commit]++
			recovered += s.Recov
		}
	}
	events.add("final restart of every node; commit indexes %v", commits)

	report := check.Check(history, 2*time.Minute)
	f, _ := os.Create(filepath.Join(*out, "history.jsonl"))
	enc := json.NewEncoder(f)
	for _, op := range history {
		enc.Encode(op)
	}
	f.Close()
	os.WriteFile(filepath.Join(*out, "events.log"), []byte(strings.Join(events.lines, "\n")+"\n"), 0o644)

	summary := map[string]any{
		"seed": *seed, "nodes": c.n, "clients": *clients, "duration_s": duration.Seconds(), "fsync": *fsync,
		"operations": len(history), "completed": ok.Load(), "unknown": unk.Load(),
		"faults": faults, "linearizable": report.Linearizable, "verdict_unknown": report.Unknown,
		"keys": len(report.Keys), "torn_bytes_recovered": recovered,
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile(filepath.Join(*out, "summary.json"), b, 0o644)
	fmt.Println(string(b))
	fmt.Println(report)
	if !report.Linearizable {
		os.Exit(1)
	}
}

func waitForLeader(c *cluster, limit time.Duration) {
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if c.leader() != 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Print("no leader within ", limit)
}

var _ = io.Discard
