// Command dash is Quorum's dashboard.
//
// It runs a five-node cluster of real quorumd processes behind cuttable links,
// a small workload against it, and a linearizability check of that workload,
// and lets whoever is watching kill nodes and partition the network. It also
// replays recorded Jepsen runs: what each node reported four times a second,
// what the nemesis did, and what the clients saw.
//
//	dash -bin ./quorumd -data /var/lib/quorum-demo -results ./results -listen :8080
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kyan9400/quorum/internal/cluster"
)

//go:embed web
var webFS embed.FS

func main() {
	bin := flag.String("bin", "./quorumd", "quorumd binary")
	data := flag.String("data", "demo-data", "data directories for the live cluster")
	results := flag.String("results", "results", "directory of recorded Jepsen runs")
	listen := flag.String("listen", "127.0.0.1:8080", "address to serve the dashboard on")
	base := flag.Int("port", 17300, "first port for the live cluster's nodes and links")
	nodes := flag.Int("nodes", 5, "live cluster size")
	clients := flag.Int("clients", 6, "clients running against the live cluster")
	windowD := flag.Duration("window", 10*time.Second, "how much history each linearizability check covers")
	autoHeal := flag.Duration("auto-heal", 30*time.Second, "heal and restart everything after this long broken and untouched")
	fsync := flag.Bool("fsync", true, "fsync on the live nodes")
	tick := flag.String("tick", "50ms", "raft tick on the live nodes")
	pause := flag.Duration("pause", 120*time.Millisecond, "each client waits between this and twice this between operations")
	noLive := flag.Bool("no-live", false, "serve recorded runs only")
	webDir := flag.String("web", "", "serve the page from this directory instead of the built-in copy (for development)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	rs := &runs{dir: *results}
	mux.HandleFunc("GET /api/runs", func(w http.ResponseWriter, _ *http.Request) {
		list, err := rs.list()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, list)
	})
	mux.HandleFunc("GET /api/runs/{name}", func(w http.ResponseWriter, r *http.Request) {
		d, err := rs.detail(r.PathValue("name"))
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=300")
		writeJSON(w, d)
	})

	var lv *live
	if !*noLive {
		c, err := cluster.New(cluster.Config{Bin: *bin, Dir: *data, N: *nodes, BasePort: *base, Fsync: *fsync, Tick: *tick, SnapshotEvery: 2000})
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		c.StartAll()
		c.WaitForLeader(20 * time.Second)
		lv = newLive(c, *windowD, *autoHeal)
		lv.pause = *pause
		go lv.run(ctx, *clients)

		mux.HandleFunc("GET /api/live", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, lv.snapshot())
		})
		mux.HandleFunc("GET /api/live/stream", func(w http.ResponseWriter, r *http.Request) {
			fl, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming unsupported", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for {
				b, _ := json.Marshal(lv.snapshot())
				if _, err := w.Write(append(append([]byte("data: "), b...), '\n', '\n')); err != nil {
					return
				}
				fl.Flush()
				select {
				case <-r.Context().Done():
					return
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		})
		mux.HandleFunc("POST /api/live/action", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				Action string `json:"action"`
				Node   int    `json:"node"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := lv.act(req.Action, req.Node); err != nil {
				status := http.StatusBadRequest
				if errors.Is(err, errTooSoon) {
					status = http.StatusTooManyRequests
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, map[string]bool{"ok": true})
		})
	}
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"live": lv != nil})
	})

	var page fs.FS
	page, _ = fs.Sub(webFS, "web")
	if *webDir != "" {
		page = os.DirFS(*webDir)
	}
	mux.Handle("GET /", http.FileServerFS(page))

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	log.Printf("dashboard on http://%s", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
