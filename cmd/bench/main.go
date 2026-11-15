// Command bench measures a running cluster: throughput and latency of writes
// and of linearizable reads, at a range of client counts.
//
// Closed loop on purpose: each client sends its next request when the last one
// returns, so the client count is the concurrency and throughput is what the
// cluster sustains at that concurrency. It answers "how fast", not "what
// happens past capacity" - that question needs an open loop.
//
//	bench -nodes http://a:7001,http://b:7001,http://c:7001 -clients 1,4,16,64
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kyan9400/quorum/client"
)

type result struct {
	Op        string  `json:"op"`
	Clients   int     `json:"clients"`
	Requests  int     `json:"requests"`
	Errors    int     `json:"errors"`
	PerSec    float64 `json:"per_sec"`
	P50ms     float64 `json:"p50_ms"`
	P99ms     float64 `json:"p99_ms"`
	DurationS float64 `json:"duration_s"`
}

func pct(v []time.Duration, q float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(q*float64(len(v)))) - 1
	return float64(v[max(0, min(i, len(v)-1))].Microseconds()) / 1000
}

func run(nodes []string, op string, clients int, d time.Duration, valueSize int) result {
	var mu sync.Mutex
	var lat []time.Duration
	errs := 0
	value := strings.Repeat("x", valueSize)
	ctxAll, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client.New(nodes)
			key := fmt.Sprintf("bench-%d", i%64)
			var local []time.Duration
			bad := 0
			for ctxAll.Err() == nil {
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				t := time.Now()
				var err error
				if op == "write" {
					_, err = cl.Put(ctx, key, value)
				} else {
					_, err = cl.Get(ctx, key)
				}
				c()
				if err != nil {
					bad++
					continue
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			lat = append(lat, local...)
			errs += bad
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return result{
		Op: op, Clients: clients, Requests: len(lat), Errors: errs,
		PerSec: math.Round(float64(len(lat))/elapsed.Seconds()*10) / 10,
		P50ms:  pct(lat, 0.5), P99ms: pct(lat, 0.99), DurationS: math.Round(elapsed.Seconds()*10) / 10,
	}
}

func main() {
	nodesFlag := flag.String("nodes", "http://127.0.0.1:7001", "comma-separated node URLs")
	clientsFlag := flag.String("clients", "1,4,16,64", "client counts to sweep")
	ops := flag.String("ops", "write,read", "write, read, or both")
	d := flag.Duration("duration", 15*time.Second, "per point")
	size := flag.Int("value", 100, "value size in bytes")
	label := flag.String("label", "", "printed with each line")
	flag.Parse()

	nodes := strings.Split(*nodesFlag, ",")
	// Warm up: elect a leader, open connections.
	cl := client.New(nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if _, err := cl.Put(ctx, "warmup", "x"); err != nil {
		fmt.Fprintln(os.Stderr, "cluster not answering:", err)
		os.Exit(1)
	}
	cancel()
	for _, op := range strings.Split(*ops, ",") {
		for _, cs := range strings.Split(*clientsFlag, ",") {
			n, _ := strconv.Atoi(cs)
			r := run(nodes, op, n, *d, *size)
			b, _ := json.Marshal(struct {
				Label string `json:"label"`
				result
			}{*label, r})
			fmt.Println(string(b))
		}
	}
}
