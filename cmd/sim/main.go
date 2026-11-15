// Command sim runs the deterministic simulation over a range of seeds and
// reports any that break linearizability, state machine safety or election
// safety. A failing seed replays exactly:
//
//	sim -seed 81233 -v
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kyan9400/quorum/internal/sim"
)

func main() {
	seed := flag.Uint64("seed", 0, "run this one seed")
	from := flag.Uint64("from", 1, "first seed of a range")
	count := flag.Int("seeds", 100, "how many seeds")
	duration := flag.Int64("duration", 30_000, "virtual milliseconds of client traffic")
	verbose := flag.Bool("v", false, "print every run, not only failures")
	flag.Parse()

	start := time.Now()
	first, n := *from, *count
	if *seed != 0 {
		first, n = *seed, 1
		*verbose = true
	}
	failed, ops := 0, 0
	var unknown, crashes, partitions, leaders, snapshots, messages, dropped int
	for i := 0; i < n; i++ {
		cfg := sim.Defaults(first + uint64(i))
		cfg.Duration = *duration
		r := sim.Run(cfg)
		ops += r.Operations
		unknown += r.Unknown
		crashes += r.Crashes
		partitions += r.Partitions
		leaders += r.Leaders
		snapshots += r.Snapshots
		messages += r.Messages
		dropped += r.Dropped
		if !r.OK {
			failed++
		}
		if !r.OK || *verbose {
			fmt.Println(r)
		}
	}
	fmt.Printf("%d seeds, %d failed, %d client operations checked (%d with unknown outcome), %s\n",
		n, failed, ops, unknown, time.Since(start).Round(time.Millisecond))
	fmt.Printf("faults survived: %d crashes, %d partitions, %d elected leaders, %d snapshots installed, %d of %d messages dropped\n",
		crashes, partitions, leaders, snapshots, dropped, messages)
	if failed > 0 {
		os.Exit(1)
	}
}
