// Command quorumd runs one node of a Quorum cluster.
//
//	quorumd -id 1 -listen :7001 -data /var/lib/quorum \
//	        -peers 1=http://a:7001,2=http://b:7001,3=http://c:7001
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kyan9400/quorum/internal/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's id, as in -peers")
	listen := flag.String("listen", ":7001", "address to serve peers and clients on")
	peers := flag.String("peers", "", "every node, this one included: 1=http://host:port,...")
	advertise := flag.String("advertise", "", "URLs clients are redirected to, if different from -peers")
	data := flag.String("data", "data", "directory for the log and snapshots")
	fsync := flag.Bool("fsync", true, "fsync every write; off only for benchmarks that say so")
	tick := flag.Duration("tick", 50*time.Millisecond, "one Raft tick")
	snapshotEvery := flag.Uint64("snapshot-every", 10_000, "compact after this many applied entries")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("node", *id)

	peerMap, err := server.ParsePeers(*peers)
	if err != nil || *id == 0 || peerMap[*id] == "" {
		log.Error("need -id and -peers including this node", "error", err)
		os.Exit(2)
	}
	cfg := server.Config{ID: *id, Peers: peerMap, DataDir: *data, Fsync: *fsync, Tick: *tick, SnapshotEvery: *snapshotEvery}
	if *advertise != "" {
		if cfg.Advertise, err = server.ParsePeers(*advertise); err != nil {
			log.Error("bad -advertise", "error", err)
			os.Exit(2)
		}
	}
	s, err := server.New(cfg, log)
	if err != nil {
		log.Error("cannot start", "error", err)
		os.Exit(1)
	}
	go s.Run()

	srv := &http.Server{Addr: *listen, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	log.Info("serving", "listen", *listen, "peers", len(peerMap), "fsync", *fsync)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
	s.Stop()
}
