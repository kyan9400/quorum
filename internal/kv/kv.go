// Package kv is the replicated state machine: a map from string keys to string
// values, changed only by applying committed log entries, in order.
//
// Every write carries a client id and a sequence number. A client that times
// out cannot know whether its write was applied - the leader may have
// committed it and crashed before answering - so it retries, and the retry may
// be committed too. Applying it twice would turn one "append" into two, or let
// a compare-and-set succeed against its own earlier effect. So the state
// machine remembers, per client, the last sequence number applied and its
// result, and answers a duplicate with the original result instead of applying
// it again. The table is part of the state machine, and so part of every
// snapshot: a duplicate arriving after a compaction must still be recognised.
//
// It assumes one request in flight per client, which is what the client
// library does; a client that pipelines must use a separate id per stream.
package kv

import (
	"encoding/json"
	"fmt"
	"sort"
)

type Op string

const (
	OpPut    Op = "put"
	OpAppend Op = "append" // appends to the value; makes lost or doubled writes visible
	OpDelete Op = "delete"
	OpCAS    Op = "cas" // set Value if the current value equals Expected
	OpGet    Op = "get" // only through the log in tests; normal reads use ReadIndex
)

type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
	// ExpectAbsent makes a CAS succeed only if the key does not exist, which
	// an empty Expected cannot express.
	ExpectAbsent bool   `json:"expect_absent,omitempty"`
	Client       uint64 `json:"client"`
	Seq          uint64 `json:"seq"`
}

type Result struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
	OK    bool   `json:"ok"` // CAS succeeded; always true for other writes
}

func Encode(c Command) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return b
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

type Store struct {
	data     map[string]string
	sessions map[uint64]session
	applied  uint64
}

func New() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply applies the committed entry at index. Entries must arrive in order.
// Returns the command's result, and whether it was a duplicate.
func (s *Store) Apply(index uint64, data []byte) (Result, bool) {
	if index != s.applied+1 {
		panic(fmt.Sprintf("kv: applying %d after %d", index, s.applied))
	}
	s.applied = index
	if len(data) == 0 {
		return Result{}, false // a leader's no-op
	}
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		// Committed garbage is still committed; skipping it on every replica
		// keeps them identical.
		return Result{}, false
	}
	if c.Client != 0 {
		if prev, ok := s.sessions[c.Client]; ok && c.Seq <= prev.Seq {
			return prev.Result, true
		}
	}
	r := s.exec(c)
	if c.Client != 0 {
		s.sessions[c.Client] = session{Seq: c.Seq, Result: r}
	}
	return r, false
}

func (s *Store) exec(c Command) Result {
	old, found := s.data[c.Key]
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
		return Result{Value: c.Value, Found: true, OK: true}
	case OpAppend:
		s.data[c.Key] = old + c.Value
		return Result{Value: old + c.Value, Found: true, OK: true}
	case OpDelete:
		delete(s.data, c.Key)
		return Result{Value: old, Found: found, OK: true}
	case OpCAS:
		matches := (c.ExpectAbsent && !found) || (!c.ExpectAbsent && found && old == c.Expected)
		if matches {
			s.data[c.Key] = c.Value
			return Result{Value: c.Value, Found: true, OK: true}
		}
		return Result{Value: old, Found: found, OK: false}
	case OpGet:
		return Result{Value: old, Found: found, OK: true}
	}
	return Result{}
}

// Get reads the current value. Linearizable only when called after a
// ReadIndex has been confirmed and applied up to its index.
func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Applied() uint64 { return s.applied }
func (s *Store) Len() int        { return len(s.data) }

type snapshot struct {
	Applied  uint64             `json:"applied"`
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
}

func (s *Store) Snapshot() []byte {
	b, err := json.Marshal(snapshot{Applied: s.applied, Data: s.data, Sessions: s.sessions})
	if err != nil {
		panic(err)
	}
	return b
}

func Restore(b []byte) (*Store, error) {
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, err
	}
	s := New()
	s.applied = snap.Applied
	for k, v := range snap.Data {
		s.data[k] = v
	}
	for k, v := range snap.Sessions {
		s.sessions[k] = v
	}
	return s, nil
}

// Digest is a deterministic summary of the whole state, for checking that
// replicas which applied the same prefix hold the same data.
func (s *Store) Digest() string {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b, _ := json.Marshal(struct {
		Applied uint64
		Keys    []string
		Data    map[string]string
	}{s.applied, keys, s.data})
	return string(b)
}
