package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/kyan9400/quorum/internal/check"
	"github.com/kyan9400/quorum/internal/cluster"
)

// runs serves recorded Jepsen runs: what the nemesis did, what every node
// reported four times a second, and what the clients saw, bucketed.
type runs struct {
	dir   string
	mu    sync.Mutex
	cache map[string]*runDetail
}

type runSummary struct {
	Name    string         `json:"name"`
	Summary map[string]any `json:"summary"`
}

type runDetail struct {
	Name     string          `json:"name"`
	Summary  map[string]any  `json:"summary"`
	Events   []cluster.Event `json:"events"`
	Timeline json.RawMessage `json:"timeline"` // array of samples, as recorded
	Buckets  []bucket        `json:"buckets"`
	BucketS  float64         `json:"bucket_s"`
	Offset   float64         `json:"history_offset_s"`
}

// bucket is the clients' view of one slice of the run.
type bucket struct {
	T        float64        `json:"t"` // start, seconds on the cluster's clock
	OK       int            `json:"ok"`
	Unknown  int            `json:"unknown"` // operations begun in this slice that never got an answer
	ByKind   map[string]int `json:"by_kind"`
	P50      float64        `json:"p50"`
	P99      float64        `json:"p99"`
	Max      float64        `json:"max"`
	Failures int            `json:"failed_cas"`
}

func (r *runs) list() ([]runSummary, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	var out []runSummary
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var s map[string]any
		b, err := os.ReadFile(filepath.Join(r.dir, e.Name(), "summary.json"))
		if err != nil || json.Unmarshal(b, &s) != nil {
			continue
		}
		out = append(out, runSummary{Name: e.Name(), Summary: s})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (r *runs) detail(name string) (*runDetail, error) {
	if name != filepath.Base(name) || name == "." || name == ".." {
		return nil, os.ErrNotExist
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if d := r.cache[name]; d != nil {
		return d, nil
	}
	dir := filepath.Join(r.dir, name)
	d := &runDetail{Name: name, BucketS: 0.5}
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &d.Summary); err != nil {
		return nil, err
	}
	if off, ok := d.Summary["history_offset_s"].(float64); ok {
		d.Offset = off
	}
	if err := readJSONL(filepath.Join(dir, "events.jsonl"), func(raw []byte) error {
		var e cluster.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		d.Events = append(d.Events, e)
		return nil
	}); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	var samples []json.RawMessage
	if err := readJSONL(filepath.Join(dir, "timeline.jsonl.gz"), func(raw []byte) error {
		samples = append(samples, append(json.RawMessage(nil), raw...))
		return nil
	}); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	d.Timeline, _ = json.Marshal(samples)
	if samples == nil {
		d.Timeline = json.RawMessage("[]")
	}

	byBucket := map[int]*bucket{}
	lat := map[int][]float64{}
	err = readJSONL(filepath.Join(dir, "history.jsonl.gz"), func(raw []byte) error {
		var op check.Operation
		if err := json.Unmarshal(raw, &op); err != nil {
			return err
		}
		call := d.Offset + float64(op.Call)/1e9
		i := int(call / d.BucketS)
		bk := byBucket[i]
		if bk == nil {
			bk = &bucket{T: float64(i) * d.BucketS, ByKind: map[string]int{}}
			byBucket[i] = bk
		}
		bk.ByKind[string(op.Kind)]++
		if op.Unknown {
			bk.Unknown++
			return nil
		}
		bk.OK++
		if op.Kind == check.CAS && !op.OutOK {
			bk.Failures++
		}
		lat[i] = append(lat[i], float64(op.Return-op.Call)/1e6)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i, bk := range byBucket {
		if l := lat[i]; len(l) > 0 {
			sort.Float64s(l)
			bk.P50 = round(l[len(l)/2])
			bk.P99 = round(l[min(len(l)-1, int(float64(len(l))*0.99))])
			bk.Max = round(l[len(l)-1])
		}
		d.Buckets = append(d.Buckets, *bk)
	}
	sort.Slice(d.Buckets, func(i, j int) bool { return d.Buckets[i].T < d.Buckets[j].T })
	if r.cache == nil {
		r.cache = map[string]*runDetail{}
	}
	r.cache[name] = d
	return d, nil
}

func round(v float64) float64 { return math.Round(v*10) / 10 }

// readJSONL reads one JSON value per line, gunzipping if the name ends in .gz.
func readJSONL(path string, each func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var rd io.Reader = f
	if filepath.Ext(path) == ".gz" {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer zr.Close()
		rd = zr
	}
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		if err := each(sc.Bytes()); err != nil {
			return err
		}
	}
	return sc.Err()
}
