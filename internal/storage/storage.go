// Package storage keeps a node's Raft state on disk: a write-ahead log of
// entries and hard state, and the latest snapshot.
//
// The rules it follows, and why:
//
//   - Every record carries its length and a CRC. A crash can leave the last
//     record half-written; on recovery the log is read up to the first record
//     that is short or fails its checksum, and the file is truncated there.
//     Anything after that point was never acknowledged, because Save returns
//     only after fsync, and Ready's contract sends nothing until Save returns.
//   - One fsync per Save, however many records it wrote. Raft batches a
//     Ready's entries and hard state together, so this is group commit for
//     free, and it is what decides write throughput on real disks.
//   - Snapshots are written to a temporary file, fsynced, renamed into place,
//     and the directory fsynced, so a crash leaves either the old snapshot or
//     the new one and never a torn one. The log is then rewritten the same way
//     without the entries the snapshot covers.
//   - A later record for an index replaces the earlier one and everything after
//     it, exactly as Raft truncated a conflicting suffix in memory.
package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/kyan9400/quorum/internal/raft"
)

const (
	recEntry byte = 1
	recHard  byte = 2

	walName  = "wal"
	snapName = "snapshot"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type Storage struct {
	dir  string
	wal  *os.File
	w    *bufio.Writer
	sync bool

	// What is on disk, mirrored for Compact.
	hard     raft.HardState
	snapshot *raft.Snapshot
	entries  []raft.Entry

	// Recovered tells whether the last open found and removed a torn tail.
	Recovered int64
}

// Open reads whatever is in dir and returns the state to restart from.
// sync=false skips fsync, for tests that do not survive a power cut anyway.
func Open(dir string, sync bool) (*Storage, raft.Restore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.Restore{}, err
	}
	s := &Storage{dir: dir, sync: sync}
	if err := s.readSnapshot(); err != nil {
		return nil, raft.Restore{}, err
	}
	if err := s.readWAL(); err != nil {
		return nil, raft.Restore{}, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, raft.Restore{}, err
	}
	s.wal, s.w = f, bufio.NewWriterSize(f, 256*1024)
	restore := raft.Restore{HardState: s.hard, Snapshot: s.snapshot, Entries: append([]raft.Entry(nil), s.entries...)}
	return s, restore, nil
}

func (s *Storage) readSnapshot() error {
	b, err := os.ReadFile(filepath.Join(s.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) < 20 {
		return fmt.Errorf("snapshot file is %d bytes, too short to be one", len(b))
	}
	sum := binary.LittleEndian.Uint32(b[0:4])
	if crc32.Checksum(b[4:], table) != sum {
		// Written by rename, so this is not a torn write: it is damage, and
		// guessing past it would be worse than refusing to start.
		return errors.New("snapshot file fails its checksum")
	}
	s.snapshot = &raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:12]),
		Term:  binary.LittleEndian.Uint64(b[12:20]),
		Data:  append([]byte(nil), b[20:]...),
	}
	return nil
}

func (s *Storage) readWAL() error {
	path := filepath.Join(s.dir, walName)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256*1024)
	var good int64
	for {
		typ, payload, n, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			info, _ := f.Stat()
			// A crash can only tear the record being written last. A bad
			// record with intact data after it is damage to acknowledged
			// entries, and truncating there would throw them away silently.
			var bad *badRecord
			if errors.As(err, &bad) && good+bad.size < info.Size() {
				return fmt.Errorf("log is damaged at byte %d with %d bytes after it; refusing to start rather than drop acknowledged entries", good, info.Size()-good-bad.size)
			}
			// A torn tail. Everything before it is intact.
			s.Recovered = info.Size() - good
			f.Close()
			if err := os.Truncate(path, good); err != nil {
				return fmt.Errorf("truncating a torn log tail: %w", err)
			}
			break
		}
		good += n
		switch typ {
		case recHard:
			s.hard = raft.HardState{
				Term:   binary.LittleEndian.Uint64(payload[0:8]),
				Vote:   binary.LittleEndian.Uint64(payload[8:16]),
				Commit: binary.LittleEndian.Uint64(payload[16:24]),
			}
		case recEntry:
			e := raft.Entry{
				Index: binary.LittleEndian.Uint64(payload[0:8]),
				Term:  binary.LittleEndian.Uint64(payload[8:16]),
			}
			if len(payload) > 16 {
				e.Data = append([]byte(nil), payload[16:]...)
			}
			s.place(e)
		}
	}
	// Entries the snapshot already covers are not needed to restart.
	if s.snapshot != nil {
		s.dropThrough(s.snapshot.Index)
	}
	// A log that skips an index cannot be replayed; refuse rather than guess.
	for i := 1; i < len(s.entries); i++ {
		if s.entries[i].Index != s.entries[i-1].Index+1 {
			return fmt.Errorf("log has a gap between %d and %d", s.entries[i-1].Index, s.entries[i].Index)
		}
	}
	return nil
}

// place puts e at its index, discarding anything at or after it.
func (s *Storage) place(e raft.Entry) {
	cut := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].Index >= e.Index })
	s.entries = append(s.entries[:cut], e)
}

func (s *Storage) dropThrough(index uint64) {
	cut := sort.Search(len(s.entries), func(i int) bool { return s.entries[i].Index > index })
	s.entries = append([]raft.Entry(nil), s.entries[cut:]...)
}

// badRecord is a complete record whose checksum fails.
type badRecord struct{ size int64 }

func (b *badRecord) Error() string { return "record fails its checksum" }

// record layout: [length u32][crc u32][type u8][payload]; crc covers type+payload.
func readRecord(r *bufio.Reader) (byte, []byte, int64, error) {
	var head [8]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, 0, io.EOF
		}
		return 0, nil, 0, io.ErrUnexpectedEOF
	}
	length := binary.LittleEndian.Uint32(head[0:4])
	if length == 0 || length > 64<<20 {
		return 0, nil, 0, errors.New("implausible record length")
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, 0, io.ErrUnexpectedEOF
	}
	if crc32.Checksum(body, table) != binary.LittleEndian.Uint32(head[4:8]) {
		return 0, nil, 0, &badRecord{size: int64(8 + length)}
	}
	return body[0], body[1:], int64(8 + length), nil
}

func writeRecord(w io.Writer, typ byte, payload []byte) error {
	body := make([]byte, 1+len(payload))
	body[0] = typ
	copy(body[1:], payload)
	var head [8]byte
	binary.LittleEndian.PutUint32(head[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(head[4:8], crc32.Checksum(body, table))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func encodeHard(h raft.HardState) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], h.Term)
	binary.LittleEndian.PutUint64(b[8:16], h.Vote)
	binary.LittleEndian.PutUint64(b[16:24], h.Commit)
	return b
}

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 16+len(e.Data))
	binary.LittleEndian.PutUint64(b[0:8], e.Index)
	binary.LittleEndian.PutUint64(b[8:16], e.Term)
	copy(b[16:], e.Data)
	return b
}

// Save persists what a Ready asks for and returns only once it is durable.
func (s *Storage) Save(rd raft.Ready) error {
	if rd.Snapshot != nil {
		if err := s.installSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	wrote := false
	for _, e := range rd.Entries {
		if err := writeRecord(s.w, recEntry, encodeEntry(e)); err != nil {
			return err
		}
		s.place(e)
		wrote = true
	}
	if rd.HardStateChanged {
		if err := writeRecord(s.w, recHard, encodeHard(rd.HardState)); err != nil {
			return err
		}
		s.hard = rd.HardState
		wrote = true
	}
	if !wrote {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	if s.sync {
		return s.wal.Sync()
	}
	return nil
}

// Compact records a snapshot the state machine took at index, and drops the
// log entries it covers.
func (s *Storage) Compact(snap *raft.Snapshot) error {
	return s.installSnapshot(snap)
}

func (s *Storage) installSnapshot(snap *raft.Snapshot) error {
	var buf bytes.Buffer
	buf.Grow(20 + len(snap.Data))
	buf.Write(make([]byte, 4))
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], snap.Index)
	buf.Write(b[:])
	binary.LittleEndian.PutUint64(b[:], snap.Term)
	buf.Write(b[:])
	buf.Write(snap.Data)
	out := buf.Bytes()
	binary.LittleEndian.PutUint32(out[0:4], crc32.Checksum(out[4:], table))
	if err := s.replace(snapName, out); err != nil {
		return err
	}
	s.snapshot = snap
	s.dropThrough(snap.Index)
	return s.rewriteWAL()
}

// rewriteWAL replaces the log with the hard state and the entries still
// needed, atomically.
func (s *Storage) rewriteWAL() error {
	var buf bytes.Buffer
	if err := writeRecord(&buf, recHard, encodeHard(s.hard)); err != nil {
		return err
	}
	for _, e := range s.entries {
		if err := writeRecord(&buf, recEntry, encodeEntry(e)); err != nil {
			return err
		}
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	s.wal.Close()
	if err := s.replace(walName, buf.Bytes()); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, walName), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.wal, s.w = f, bufio.NewWriterSize(f, 256*1024)
	return nil
}

// replace writes name atomically: temporary file, fsync, rename, fsync the
// directory so the rename itself survives a crash.
func (s *Storage) replace(name string, data []byte) error {
	tmp := filepath.Join(s.dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if s.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, name)); err != nil {
		return err
	}
	if s.sync {
		syncDir(s.dir)
	}
	return nil
}

func (s *Storage) Close() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.wal.Close()
}
