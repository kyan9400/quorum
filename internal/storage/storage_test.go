package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kyan9400/quorum/internal/raft"
)

func entries(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func reopen(t *testing.T, dir string) (*Storage, raft.Restore) {
	t.Helper()
	s, r, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, r
}

func TestWhatWasSavedIsWhatARestartSees(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{HardState: raft.HardState{Term: 3, Vote: 2, Commit: 4}, HardStateChanged: true, Entries: entries(1, 5, 3)})
	s.Close()
	_, r := reopen(t, dir)
	if r.HardState != (raft.HardState{Term: 3, Vote: 2, Commit: 4}) || len(r.Entries) != 5 || r.Entries[4].Index != 5 {
		t.Fatalf("restored %+v with %d entries", r.HardState, len(r.Entries))
	}
}

func TestAConflictingSuffixIsReplacedOnDiskAsInMemory(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{Entries: entries(1, 6, 2)})
	s.Save(raft.Ready{Entries: entries(4, 4, 3)}) // index 4 from a later term: 5 and 6 are gone
	s.Close()
	_, r := reopen(t, dir)
	if len(r.Entries) != 4 || r.Entries[3].Term != 3 {
		t.Fatalf("after truncation the log is %+v", r.Entries)
	}
}

func TestATornFinalRecordIsCutOffAndEverythingBeforeItKept(t *testing.T) {
	// A crash part-way through writing the last record. Save had not
	// returned, so nothing about that record was acknowledged.
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{Entries: entries(1, 3, 1)})
	s.Close()
	wal := filepath.Join(dir, walName)
	info, _ := os.Stat(wal)
	os.Truncate(wal, info.Size()-3)
	s2, r := reopen(t, dir)
	if len(r.Entries) != 2 || s2.Recovered == 0 {
		t.Fatalf("kept %d entries, recovered %d bytes", len(r.Entries), s2.Recovered)
	}
	// And the log keeps working after the cut.
	s2.Save(raft.Ready{Entries: entries(3, 3, 1)})
	s2.Close()
	_, r = reopen(t, dir)
	if len(r.Entries) != 3 {
		t.Fatalf("%d entries after writing past a recovered tail", len(r.Entries))
	}
}

func TestDamageInTheMiddleRefusesToStartInsteadOfDroppingData(t *testing.T) {
	// A flipped bit with intact records after it is not a torn write.
	// Truncating there would silently discard entries clients were told
	// were committed.
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{Entries: entries(1, 5, 1)})
	s.Close()
	wal := filepath.Join(dir, walName)
	b, _ := os.ReadFile(wal)
	b[12] ^= 0xff // inside the first record
	os.WriteFile(wal, b, 0o644)
	_, _, err := Open(dir, false)
	if err == nil || !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("got %v, want a refusal", err)
	}
}

func TestACompactedLogRestartsFromTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{HardState: raft.HardState{Term: 2, Vote: 1, Commit: 8}, HardStateChanged: true, Entries: entries(1, 10, 2)})
	if err := s.Compact(&raft.Snapshot{Index: 8, Term: 2, Data: []byte("state at 8")}); err != nil {
		t.Fatal(err)
	}
	s.Save(raft.Ready{Entries: entries(11, 12, 2)})
	s.Close()
	_, r := reopen(t, dir)
	if r.Snapshot == nil || r.Snapshot.Index != 8 || string(r.Snapshot.Data) != "state at 8" {
		t.Fatalf("snapshot %+v", r.Snapshot)
	}
	if len(r.Entries) != 4 || r.Entries[0].Index != 9 || r.Entries[3].Index != 12 {
		t.Fatalf("entries after the snapshot: %+v", r.Entries)
	}
	if r.HardState.Vote != 1 {
		t.Fatal("the vote was lost when the log was rewritten")
	}
}

func TestADamagedSnapshotIsRefused(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Compact(&raft.Snapshot{Index: 3, Term: 1, Data: []byte("abc")})
	s.Close()
	path := filepath.Join(dir, snapName)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 1
	os.WriteFile(path, b, 0o644)
	if _, _, err := Open(dir, false); err == nil {
		t.Fatal("opened a snapshot that fails its checksum")
	}
}

func TestASnapshotFromTheLeaderReplacesTheWholeLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	s.Save(raft.Ready{Entries: entries(1, 3, 1)})
	s.Save(raft.Ready{Snapshot: &raft.Snapshot{Index: 50, Term: 4, Data: []byte("x")}})
	s.Close()
	_, r := reopen(t, dir)
	if r.Snapshot.Index != 50 || len(r.Entries) != 0 {
		t.Fatalf("snapshot %d, %d stale entries left", r.Snapshot.Index, len(r.Entries))
	}
}
