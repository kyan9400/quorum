package check

import (
	"strings"
	"testing"
	"time"
)

func put(c uint64, k, v string, call, ret int64) Operation {
	return Operation{Client: c, Kind: Put, Key: k, Value: v, Call: call, Return: ret, OutOK: true}
}
func get(c uint64, k, out string, found bool, call, ret int64) Operation {
	return Operation{Client: c, Kind: Get, Key: k, Call: call, Return: ret, OutValue: out, OutFound: found, OutOK: true}
}
func appendOp(c uint64, k, v, out string, call, ret int64) Operation {
	return Operation{Client: c, Kind: Append, Key: k, Value: v, Call: call, Return: ret, OutValue: out, OutOK: true}
}
func cas(c uint64, k string, absent bool, exp, v string, ok bool, call, ret int64) Operation {
	return Operation{Client: c, Kind: CAS, Key: k, ExpectAbsent: absent, Expected: exp, Value: v, Call: call, Return: ret, OutOK: ok}
}
func unknown(op Operation) Operation { op.Unknown = true; op.Return = 0; return op }

func verdict(t *testing.T, h []Operation) Report {
	t.Helper()
	r := Check(h, 5*time.Second)
	if r.Unknown {
		t.Fatalf("no verdict: %s", r)
	}
	return r
}

func TestASequentialCorrectHistoryIsLinearizable(t *testing.T) {
	r := verdict(t, []Operation{
		put(1, "x", "1", 0, 1),
		get(2, "x", "1", true, 2, 3),
		put(1, "x", "2", 4, 5),
		get(2, "x", "2", true, 6, 7),
	})
	if !r.Linearizable {
		t.Fatal(r)
	}
}

func TestAStaleReadIsCaught(t *testing.T) {
	// The bug a deposed leader serving reads from its own state produces: the
	// new value was acknowledged, and a read that starts afterwards still sees
	// the old one.
	r := verdict(t, []Operation{
		put(1, "x", "1", 0, 1),
		put(1, "x", "2", 2, 3),
		get(2, "x", "1", true, 4, 5),
	})
	if r.Linearizable {
		t.Fatal("a stale read was accepted")
	}
	if !strings.Contains(r.String(), `get(x) at [4, 5] -> "1"`) {
		t.Fatalf("the report does not point at the stale read:\n%s", r)
	}
}

func TestConcurrentOperationsMayTakeEffectAnywhereInTheirInterval(t *testing.T) {
	// The put overlaps both reads, so absent-then-present is a valid order.
	r := verdict(t, []Operation{
		put(1, "x", "1", 0, 10),
		get(2, "x", "", false, 1, 2),
		get(2, "x", "1", true, 3, 4),
	})
	if !r.Linearizable {
		t.Fatal(r)
	}
}

func TestAValueCannotGoBackwards(t *testing.T) {
	// Once one read has seen the write, a later read cannot miss it.
	r := verdict(t, []Operation{
		put(1, "x", "1", 0, 10),
		get(2, "x", "1", true, 1, 2),
		get(3, "x", "", false, 3, 4),
	})
	if r.Linearizable {
		t.Fatal("a read went back in time and was accepted")
	}
}

func TestAWriteAppliedTwiceIsCaught(t *testing.T) {
	// What a retried append looks like without exactly-once handling.
	r := verdict(t, []Operation{
		appendOp(1, "x", "a", "a", 0, 1),
		get(2, "x", "aa", true, 2, 3),
	})
	if r.Linearizable {
		t.Fatal("a doubled append was accepted")
	}
}

func TestTwoCompareAndSetsCannotBothWinFromTheSameState(t *testing.T) {
	r := verdict(t, []Operation{
		cas(1, "lock", true, "", "a", true, 0, 5),
		cas(2, "lock", true, "", "b", true, 1, 6),
	})
	if r.Linearizable {
		t.Fatal("two clients both acquired the lock")
	}
}

func TestATimedOutWriteMayHaveHappenedOrNot(t *testing.T) {
	// The client never learned the outcome, so both later observations are
	// legitimate - which is exactly why it must not be scored either way.
	for _, seen := range []bool{true, false} {
		out := ""
		if seen {
			out = "1"
		}
		r := verdict(t, []Operation{
			unknown(put(1, "x", "1", 0, 0)),
			get(2, "x", out, seen, 5, 6),
		})
		if !r.Linearizable {
			t.Fatalf("seen=%v rejected:\n%s", seen, r)
		}
	}
}

func TestATimedOutWriteCannotTakeEffectBeforeItWasSent(t *testing.T) {
	r := verdict(t, []Operation{
		get(2, "x", "1", true, 0, 1),
		unknown(put(1, "x", "1", 5, 0)),
	})
	if r.Linearizable {
		t.Fatal("a read saw a write that had not been sent yet")
	}
}

func TestOnceSeenATimedOutWriteMustStaySeen(t *testing.T) {
	r := verdict(t, []Operation{
		unknown(put(1, "x", "1", 0, 0)),
		get(2, "x", "1", true, 5, 6),
		get(2, "x", "", false, 7, 8),
	})
	if r.Linearizable {
		t.Fatal("a timed-out write appeared and then vanished")
	}
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	r := verdict(t, []Operation{
		put(1, "a", "1", 0, 1),
		put(1, "b", "1", 2, 3),
		get(2, "a", "1", true, 4, 5),
		get(2, "b", "", false, 6, 7), // stale on b only
	})
	if r.Linearizable {
		t.Fatal("stale read on b accepted")
	}
	for _, k := range r.Keys {
		if k.Key == "a" && !k.Linearizable {
			t.Fatal("key a was blamed for key b's violation")
		}
	}
}

func TestManyConcurrentClientsFinishQuickly(t *testing.T) {
	// 5 clients x 40 operations on one key, heavily overlapping and correct
	// by construction (a serial order with widened intervals).
	var h []Operation
	val := ""
	for i := 0; i < 200; i++ {
		c := uint64(i%5 + 1)
		at := int64(i * 10)
		if i%3 == 0 {
			val += "x"
			h = append(h, appendOp(c, "k", "x", val, at, at+35))
		} else {
			h = append(h, get(c, "k", val, val != "", at, at+35))
		}
	}
	start := time.Now()
	r := Check(h, 10*time.Second)
	if !r.Linearizable {
		t.Fatal(r)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %s", time.Since(start))
	}
}
