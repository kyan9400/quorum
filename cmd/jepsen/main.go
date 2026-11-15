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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kyan9400/quorum/client"
	"github.com/kyan9400/quorum/internal/check"
	"github.com/kyan9400/quorum/internal/cluster"
)

// sample is one poll of every node, for the dashboard's replay of the run.
type sample struct {
	T     float64     `json:"t"`
	Nodes []nodePoint `json:"nodes"`
}

// nodePoint: state is "" when the node did not answer and "down" when its
// process was not running.
type nodePoint struct {
	State  string `json:"s"`
	Term   uint64 `json:"term,omitempty"`
	Lead   uint64 `json:"lead,omitempty"`
	Commit uint64 `json:"commit,omitempty"`
	Last   uint64 `json:"last,omitempty"`
}

// recordTimeline polls every node until stop is closed.
func recordTimeline(c *cluster.Cluster, every time.Duration, stop <-chan struct{}) []sample {
	var out []sample
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return out
		case <-t.C:
		}
		s := sample{T: c.Since()}
		for _, st := range c.Statuses() {
			p := nodePoint{State: st.State, Term: st.Term, Lead: st.Lead, Commit: st.Commit, Last: st.Last}
			if !st.Up {
				p = nodePoint{State: "down"}
			}
			s.Nodes = append(s.Nodes, p)
		}
		out = append(out, s)
	}
}

func writeJSONL[T any](path string, items []T, gz bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var w interface {
		Write([]byte) (int, error)
	} = f
	var zw *gzip.Writer
	if gz {
		zw = gzip.NewWriter(f)
		w = zw
	}
	enc := json.NewEncoder(w)
	for _, it := range items {
		if err := enc.Encode(it); err != nil {
			return err
		}
	}
	if zw != nil {
		return zw.Close()
	}
	return nil
}

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

	os.RemoveAll(*out)
	var eventLines []string
	c, err := cluster.New(cluster.Config{
		Bin: *bin, Dir: *out, N: *nodes, BasePort: *base, Fsync: *fsync, Tick: *tick, SnapshotEvery: 500,
		OnEvent: func(e cluster.Event) {
			line := fmt.Sprintf("%7.2fs  %s", e.T, e.Text)
			eventLines = append(eventLines, line)
			log.Print(line)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	c.StartAll()
	stopTimeline := make(chan struct{})
	timelineDone := make(chan []sample)
	go func() { timelineDone <- recordTimeline(c, 250*time.Millisecond, stopTimeline) }()
	if !c.WaitForLeader(20 * time.Second) {
		log.Print("no leader within 20s")
	}

	rng := rand.New(rand.NewPCG(*seed, 1))
	urls := c.URLs()

	var (
		mu      sync.Mutex
		history []check.Operation
		ok, unk atomic.Int64
	)
	start := time.Now()
	stamp := func() int64 { return time.Since(start).Nanoseconds() }
	offset := c.Since() // history times are relative to start; the timeline's to the cluster's start
	stopClients := make(chan struct{})
	var wg sync.WaitGroup
	for ci := 0; ci < *clients; ci++ {
		wg.Add(1)
		cseed := rng.Uint64()
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
		c.Heal()
		for i := 1; i <= c.N(); i++ {
			if !c.Alive(i) {
				c.Start(i)
			}
		}
		switch rng.IntN(5) {
		case 0, 1:
			perm := rng.Perm(c.N())
			cut := 1 + rng.IntN(c.N()-1)
			var a, b []int
			for k, p := range perm {
				if k < cut {
					a = append(a, p+1)
				} else {
					b = append(b, p+1)
				}
			}
			c.Partition([][]int{a, b}, "")
			faults["partition"]++
		case 2:
			if l := c.Leader(); l != 0 {
				c.Isolate(l, "the leader")
				faults["isolate-leader"]++
			}
		case 3:
			c.Kill(1+rng.IntN(c.N()), "")
			faults["kill"]++
		case 4:
			if l := c.Leader(); l != 0 {
				c.Kill(l, "the leader")
				faults["kill-leader"]++
			}
		}
	}
	c.Heal()
	c.StartAll()
	time.Sleep(5 * time.Second) // let clients finish on a healed cluster
	close(stopClients)
	wg.Wait()

	// Every node restarted from disk at least once more, then all must agree.
	c.RestartAll()
	c.WaitForLeader(20 * time.Second)
	time.Sleep(3 * time.Second)
	commits := map[uint64]int{}
	recovered := int64(0)
	for _, s := range c.Statuses() {
		if s.Answered {
			commits[s.Commit]++
			recovered += s.Recov
		}
	}
	line := fmt.Sprintf("%7.2fs  commit indexes after the final restart: %v", c.Since(), commits)
	eventLines = append(eventLines, line)
	log.Print(line)
	close(stopTimeline)
	timeline := <-timelineDone

	report := check.Check(history, 2*time.Minute)
	if err := writeJSONL(filepath.Join(*out, "history.jsonl.gz"), history, true); err != nil {
		log.Fatal(err)
	}
	writeJSONL(filepath.Join(*out, "events.jsonl"), c.Events(), false)
	writeJSONL(filepath.Join(*out, "timeline.jsonl.gz"), timeline, true)
	os.WriteFile(filepath.Join(*out, "events.log"), []byte(strings.Join(eventLines, "\n")+"\n"), 0o644)

	summary := map[string]any{
		"seed": *seed, "nodes": c.N(), "clients": *clients, "duration_s": duration.Seconds(), "fsync": *fsync,
		"operations": len(history), "completed": ok.Load(), "unknown": unk.Load(),
		"faults": faults, "linearizable": report.Linearizable, "verdict_unknown": report.Unknown,
		"keys": len(report.Keys), "torn_bytes_recovered": recovered,
		"final_commit_indexes": commits, "history_offset_s": offset,
	}
	b, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile(filepath.Join(*out, "summary.json"), b, 0o644)
	fmt.Println(string(b))
	fmt.Println(report)
	if !report.Linearizable {
		os.Exit(1)
	}
}
