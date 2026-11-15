package main

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kyan9400/quorum/internal/check"
)

func TestARecordedRunIsBucketedOnTheClustersClock(t *testing.T) {
	dir := t.TempDir()
	run := filepath.Join(dir, "r1")
	os.MkdirAll(run, 0o755)
	os.WriteFile(filepath.Join(run, "summary.json"), []byte(`{"seed":1,"history_offset_s":2}`), 0o644)
	os.WriteFile(filepath.Join(run, "events.jsonl"), []byte(`{"t":2.5,"kind":"kill","node":3,"text":"kill -9 n3"}`+"\n"), 0o644)

	f, _ := os.Create(filepath.Join(run, "history.jsonl.gz"))
	zw := gzip.NewWriter(f)
	enc := json.NewEncoder(zw)
	ms := int64(1e6)
	for _, op := range []check.Operation{
		{Kind: check.Put, Key: "k", Call: 100 * ms, Return: 110 * ms},                   // 2.1 s on the cluster's clock
		{Kind: check.Get, Key: "k", Call: 200 * ms, Return: 230 * ms},                   // 2.2 s
		{Kind: check.CAS, Key: "k", Call: 300 * ms, Return: 301 * ms, OutOK: false},     // 2.3 s, failed
		{Kind: check.Append, Key: "k", Call: 700 * ms, Unknown: true},                   // 2.7 s, no answer
		{Kind: check.Get, Key: "k", Call: 1200 * ms, Return: 1400 * ms, OutFound: true}, // 3.2 s
	} {
		enc.Encode(op)
	}
	zw.Close()
	f.Close()

	rs := &runs{dir: dir}
	list, err := rs.list()
	if err != nil || len(list) != 1 || list[0].Name != "r1" {
		t.Fatalf("list: %+v %v", list, err)
	}
	d, err := rs.detail("r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Events) != 1 || d.Events[0].Node != 3 {
		t.Fatalf("events: %+v", d.Events)
	}
	if string(d.Timeline) != "[]" {
		t.Fatalf("a run without a timeline should give an empty one, got %s", d.Timeline)
	}
	want := []struct {
		t           float64
		ok, unknown int
		failed      int
	}{{2.0, 3, 0, 1}, {2.5, 0, 1, 0}, {3.0, 1, 0, 0}}
	if len(d.Buckets) != len(want) {
		t.Fatalf("buckets: %+v", d.Buckets)
	}
	for i, w := range want {
		b := d.Buckets[i]
		if b.T != w.t || b.OK != w.ok || b.Unknown != w.unknown || b.Failures != w.failed {
			t.Errorf("bucket %d: got %+v, want %+v", i, b, w)
		}
	}
	if d.Buckets[0].Max != 30 {
		t.Errorf("max latency in the first bucket: got %v ms, want 30", d.Buckets[0].Max)
	}
	if _, err := rs.detail("../r1"); err == nil {
		t.Error("a path outside the results directory was served")
	}
}
