// Package check decides whether a history of client operations on the store is
// linearizable: whether there is one order of the operations, consistent with
// real time, in which every result the clients saw is what a single correct
// copy of the data would have returned.
//
// It is the Wing and Gong search with Lowe's memoisation, as used by Jepsen's
// Knossos and the Porcupine library: walk the history in time order, try to
// place each operation that has started at the current point of the
// linearization, backtrack when an operation's return is reached without it
// having been placed, and never revisit a (placed set, state) pair already
// explored. Keys are independent - every operation touches one key - so each
// key's history is checked separately, which keeps the search small.
//
// An operation whose outcome the client never learned - it timed out - has no
// return time and no known result. It may have taken effect at any point after
// it started, or not at all; the search allows both. Treating a timeout as a
// failure instead would make the checker accept a store that applied a write
// it had told nobody about, and treating it as a success would reject a store
// that had correctly dropped it.
package check

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Kind string

const (
	Put    Kind = "put"
	Append Kind = "append"
	Delete Kind = "delete"
	CAS    Kind = "cas"
	Get    Kind = "get"
)

// Operation is one client request as the client saw it.
type Operation struct {
	Client uint64
	Kind   Kind
	Key    string
	// Input.
	Value        string
	Expected     string
	ExpectAbsent bool
	// When it started and returned. Return is ignored when Unknown.
	Call, Return int64
	// Output, ignored when Unknown.
	Unknown  bool
	OutValue string
	OutFound bool
	OutOK    bool
}

func (o Operation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "client %d %s(%s", o.Client, o.Kind, o.Key)
	switch o.Kind {
	case Put, Append:
		fmt.Fprintf(&b, ", %q", o.Value)
	case CAS:
		if o.ExpectAbsent {
			fmt.Fprintf(&b, ", absent -> %q", o.Value)
		} else {
			fmt.Fprintf(&b, ", %q -> %q", o.Expected, o.Value)
		}
	}
	b.WriteString(")")
	if o.Unknown {
		fmt.Fprintf(&b, " at [%d, ?] -> unknown", o.Call)
	} else {
		fmt.Fprintf(&b, " at [%d, %d] -> ", o.Call, o.Return)
		switch o.Kind {
		case Get, Delete:
			if o.OutFound {
				fmt.Fprintf(&b, "%q", o.OutValue)
			} else {
				b.WriteString("absent")
			}
		case CAS:
			fmt.Fprintf(&b, "ok=%v", o.OutOK)
		case Append:
			fmt.Fprintf(&b, "%q", o.OutValue)
		default:
			b.WriteString("ok")
		}
	}
	return b.String()
}

type state struct {
	value  string
	exists bool
}

// step applies op to s and reports whether the result it returned is one a
// correct register could have produced from s.
func step(s state, op Operation) (state, bool) {
	switch op.Kind {
	case Put:
		return state{op.Value, true}, true
	case Append:
		next := state{s.value + op.Value, true}
		return next, op.Unknown || op.OutValue == next.value
	case Delete:
		ok := op.Unknown || (op.OutFound == s.exists && (!s.exists || op.OutValue == s.value))
		return state{}, ok
	case CAS:
		matches := (op.ExpectAbsent && !s.exists) || (!op.ExpectAbsent && s.exists && s.value == op.Expected)
		if op.Unknown {
			if matches {
				return state{op.Value, true}, true
			}
			return s, true
		}
		if matches != op.OutOK {
			return s, false
		}
		if matches {
			return state{op.Value, true}, true
		}
		return s, true
	case Get:
		return s, op.Unknown || (op.OutFound == s.exists && (!s.exists || op.OutValue == s.value))
	}
	return s, false
}

type event struct {
	op     int
	isCall bool
	time   int64
	prev   *event
	next   *event
	match  *event // a call's return event
}

type bitset []uint64

func (b bitset) set(i int)   { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (i % 64) }
func (b bitset) key(s state) string {
	var sb strings.Builder
	for _, w := range b {
		fmt.Fprintf(&sb, "%x.", w)
	}
	if s.exists {
		sb.WriteString("+")
	} else {
		sb.WriteString("-")
	}
	sb.WriteString(s.value)
	return sb.String()
}

// Result of checking one key.
type KeyResult struct {
	Key          string
	Operations   int
	Linearizable bool
	TimedOut     bool
	// When not linearizable, the longest prefix the search managed to
	// linearize, and the operations it could not place after it - where to
	// start reading.
	Longest []Operation
	Stuck   []Operation
}

// Report is the verdict on a whole history.
type Report struct {
	Linearizable bool
	Unknown      bool // some key ran out of time: no verdict, not a pass
	Keys         []KeyResult
	Operations   int
}

func (r Report) String() string {
	var b strings.Builder
	switch {
	case !r.Linearizable && !r.Unknown:
		b.WriteString("NOT LINEARIZABLE")
	case r.Unknown:
		b.WriteString("UNKNOWN (search budget exhausted)")
	default:
		b.WriteString("linearizable")
	}
	fmt.Fprintf(&b, ": %d operations on %d keys\n", r.Operations, len(r.Keys))
	for _, k := range r.Keys {
		if k.Linearizable {
			continue
		}
		fmt.Fprintf(&b, "key %q (%d operations)", k.Key, k.Operations)
		if k.TimedOut {
			b.WriteString(": no verdict in time\n")
			continue
		}
		b.WriteString(": no valid order exists. Longest valid prefix ends:\n")
		start := max(0, len(k.Longest)-6)
		for _, op := range k.Longest[start:] {
			fmt.Fprintf(&b, "    %s\n", op)
		}
		b.WriteString("  and none of these could come next:\n")
		for _, op := range k.Stuck {
			fmt.Fprintf(&b, "    %s\n", op)
		}
	}
	return b.String()
}

// Check checks a whole history. budget bounds the search per key; a key that
// runs out is reported Unknown rather than passed.
func Check(history []Operation, budget time.Duration) Report {
	byKey := map[string][]Operation{}
	for _, op := range history {
		// A read that timed out had no effect and told nobody anything.
		if op.Kind == Get && op.Unknown {
			continue
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rep := Report{Linearizable: true}
	for _, k := range keys {
		kr := checkKey(k, byKey[k], budget)
		rep.Keys = append(rep.Keys, kr)
		rep.Operations += kr.Operations
		if kr.TimedOut {
			rep.Unknown = true
		} else if !kr.Linearizable {
			rep.Linearizable = false
		}
	}
	if rep.Unknown && rep.Linearizable {
		rep.Linearizable = false
	}
	return rep
}

func checkKey(key string, ops []Operation, budget time.Duration) KeyResult {
	n := len(ops)
	events := make([]*event, 0, 2*n)
	for i, op := range ops {
		ret := op.Return
		if op.Unknown {
			ret = math.MaxInt64
		}
		c := &event{op: i, isCall: true, time: op.Call}
		r := &event{op: i, time: ret}
		c.match = r
		events = append(events, c, r)
	}
	// At equal times calls go first: the two operations are then concurrent,
	// which is the permissive reading. A checker must never invent a
	// violation out of a tie.
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].time != events[j].time {
			return events[i].time < events[j].time
		}
		return events[i].isCall && !events[j].isCall
	})
	head := &event{}
	prev := head
	for _, e := range events {
		prev.next, e.prev = e, prev
		prev = e
	}

	lift := func(c *event) {
		c.prev.next = c.next
		if c.next != nil {
			c.next.prev = c.prev
		}
		r := c.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(c *event) {
		r := c.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		c.prev.next = c
		if c.next != nil {
			c.next.prev = c
		}
	}

	type frame struct {
		call  *event
		state state
	}
	words := (n + 63) / 64
	linearized := make(bitset, words)
	cache := map[string]struct{}{}
	var stack []frame
	var longest []int
	s := state{}
	deadline := time.Now().Add(budget)
	steps := 0

	result := KeyResult{Key: key, Operations: n}
	e := head.next
	for head.next != nil {
		steps++
		if steps%4096 == 0 && time.Now().After(deadline) {
			result.TimedOut = true
			return result
		}
		// Only unknown operations left: they can all go last, or never.
		if allUnknownReturns(head) {
			break
		}
		if e.isCall {
			op := ops[e.op]
			next, ok := step(s, op)
			if ok {
				linearized.set(e.op)
				k := linearized.key(next)
				if _, seen := cache[k]; !seen {
					cache[k] = struct{}{}
					stack = append(stack, frame{e, s})
					if len(stack) > len(longest) {
						longest = make([]int, len(stack))
						for i, f := range stack {
							longest[i] = f.call.op
						}
					}
					s = next
					lift(e)
					e = head.next
					continue
				}
				linearized.clear(e.op)
			}
			e = e.next
			continue
		}
		// Reached the return of an operation that has not been placed: the
		// current partial order cannot be extended. Undo the last placement.
		if len(stack) == 0 {
			result.Longest = pick(ops, longest)
			result.Stuck = stuck(ops, longest)
			return result
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		linearized.clear(top.call.op)
		s = top.state
		unlift(top.call)
		e = top.call.next
	}
	result.Linearizable = true
	return result
}

func allUnknownReturns(head *event) bool {
	for e := head.next; e != nil; e = e.next {
		if !e.isCall && e.time != math.MaxInt64 {
			return false
		}
	}
	return true
}

func pick(ops []Operation, idx []int) []Operation {
	out := make([]Operation, len(idx))
	for i, j := range idx {
		out[i] = ops[j]
	}
	return out
}

// stuck lists the operations the longest valid prefix could not take in: one
// of them is where the history stops making sense. Earliest first.
func stuck(ops []Operation, placed []int) []Operation {
	in := map[int]bool{}
	for _, i := range placed {
		in[i] = true
	}
	var out []Operation
	for i, op := range ops {
		if !in[i] {
			out = append(out, op)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Call < out[j].Call })
	if len(out) > 8 {
		out = out[:8]
	}
	return out
}
