// Package client talks to a Quorum cluster.
//
// It follows the leader - a node that is not the leader answers with where the
// leader is - and retries through timeouts, crashed nodes and elections until
// the caller's deadline. A write keeps the same sequence number across every
// retry, which is what lets the cluster apply it exactly once however many
// copies arrive. If the deadline passes without a definite answer the client
// returns ErrUnknown: the write may or may not have happened, and pretending
// otherwise either way is how applications lose data.
//
// One Client is one sequence of requests; it is safe for concurrent use, but
// requests from it are issued one at a time.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/kyan9400/quorum/internal/kv"
)

// ErrUnknown: the deadline passed without a definite answer. A write may or
// may not have been applied.
var ErrUnknown = errors.New("outcome unknown")

type Result struct {
	Value string
	Found bool
	OK    bool // compare-and-set succeeded
}

type Client struct {
	mu      sync.Mutex
	id      uint64
	seq     uint64
	nodes   []string
	leader  int
	http    *http.Client
	attempt time.Duration
	pinned  bool
}

// NewPinned talks to one node only and never follows the leader itself; that
// node forwards to the leader. It is how clients behind a load balancer
// behave, and the way to exercise a node's own read and write paths during a
// partition - a client that always finds the leader never asks a minority-side
// node anything.
func NewPinned(url string) *Client {
	c := New([]string{url})
	c.pinned = true
	return c
}

// New takes the base URL of every node.
func New(urls []string) *Client {
	sorted := append([]string(nil), urls...)
	sort.Strings(sorted)
	return &Client{
		id:      rand.Uint64()>>1 | 1,
		nodes:   sorted,
		leader:  rand.IntN(len(sorted)),
		http:    &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8, IdleConnTimeout: 3 * time.Second}},
		attempt: 1500 * time.Millisecond,
	}
}

// SetAttemptTimeout bounds one try at one node.
func (c *Client) SetAttemptTimeout(d time.Duration) { c.attempt = d }

func (c *Client) ID() uint64 { return c.id }

func (c *Client) Get(ctx context.Context, key string) (Result, error) {
	return c.do(ctx, kv.Command{Op: kv.OpGet, Key: key})
}

func (c *Client) Put(ctx context.Context, key, value string) (Result, error) {
	return c.do(ctx, kv.Command{Op: kv.OpPut, Key: key, Value: value})
}

func (c *Client) Append(ctx context.Context, key, value string) (Result, error) {
	return c.do(ctx, kv.Command{Op: kv.OpAppend, Key: key, Value: value})
}

func (c *Client) Delete(ctx context.Context, key string) (Result, error) {
	return c.do(ctx, kv.Command{Op: kv.OpDelete, Key: key})
}

// CAS sets value if the current value is expected, or - with absent - if the
// key does not exist.
func (c *Client) CAS(ctx context.Context, key string, absent bool, expected, value string) (Result, error) {
	return c.do(ctx, kv.Command{Op: kv.OpCAS, Key: key, Expected: expected, ExpectAbsent: absent, Value: value})
}

type answer struct {
	Value  string `json:"value"`
	Found  bool   `json:"found"`
	OK     bool   `json:"ok"`
	Leader uint64 `json:"leader"`
	URL    string `json:"leader_url"`
	Error  string `json:"error"`
}

func (c *Client) do(ctx context.Context, cmd kv.Command) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cmd.Op != kv.OpGet {
		c.seq++
		cmd.Client, cmd.Seq = c.id, c.seq
	}
	body, _ := json.Marshal(cmd)
	for {
		if ctx.Err() != nil {
			return Result{}, ErrUnknown
		}
		url := c.nodes[c.leader]
		actx, cancel := context.WithTimeout(ctx, c.attempt)
		req, _ := http.NewRequestWithContext(actx, http.MethodPost, url+"/kv", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			cancel()
			c.next()
			continue
		}
		var a answer
		json.NewDecoder(resp.Body).Decode(&a)
		resp.Body.Close()
		cancel()
		switch resp.StatusCode {
		case http.StatusOK:
			return Result{Value: a.Value, Found: a.Found, OK: a.OK}, nil
		case http.StatusMisdirectedRequest:
			if c.pinned || !c.follow(a.URL) {
				c.next()
			}
			c.pause(ctx, 20*time.Millisecond)
		case http.StatusBadRequest:
			return Result{}, fmt.Errorf("rejected: %s", a.Error)
		default:
			// Timeout or busy: the same request again, perhaps elsewhere.
			c.next()
			c.pause(ctx, 20*time.Millisecond)
		}
	}
}

func (c *Client) follow(url string) bool {
	for i, u := range c.nodes {
		if u == url {
			c.leader = i
			return true
		}
	}
	return false
}

func (c *Client) next() { c.leader = (c.leader + 1) % len(c.nodes) }

func (c *Client) pause(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
