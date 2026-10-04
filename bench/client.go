package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// libraryGlobalLimit is what a Discord library lets one process send per second by default,
	// as discord.js does with globalRequestsPerSecond.
	libraryGlobalLimit = 50
	// clockAllowance is what a library adds to a reset time before trusting it, as discord.js
	// does with its offset.
	clockAllowance = 50 * time.Millisecond
)

// botAuthorization builds an Authorization header with a bot token's shape for a made-up bot. It
// is assembled here so the source holds nothing a secret scanner could take for a token.
func botAuthorization(botID string) string {
	parts := []string{base64.RawStdEncoding.EncodeToString([]byte(botID)), "bench0", strings.Repeat("x", 38)}
	return "Bot " + strings.Join(parts, ".")
}

func seconds(value float64) time.Duration {
	return time.Duration(value * float64(time.Second))
}

func sleepFor(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// answer is one HTTP response, with its body kept only when it is a 429.
type answer struct {
	status  int
	header  http.Header
	payload []byte
}

func (a answer) ok() bool {
	return a.status >= 200 && a.status < 300
}

// label names an answer in the response counts: its status, and who produced it where that
// matters.
func (a answer) label() string {
	label := strconv.Itoa(a.status)
	switch {
	case a.header.Get("Generated-By-Proxy") == "true":
		return label + " sluice"
	case a.status == http.StatusTooManyRequests:
		return label + " discord"
	}
	return label
}

// retryAfter reads how long a 429 asks to wait, and whether it is the global limit's.
func (a answer) retryAfter() (time.Duration, bool) {
	var body struct {
		RetryAfter float64 `json:"retry_after"`
		Global     bool    `json:"global"`
	}
	if json.Unmarshal(a.payload, &body) != nil || body.RetryAfter <= 0 {
		body.RetryAfter, _ = strconv.ParseFloat(a.header.Get("Retry-After"), 64)
	}
	if body.RetryAfter <= 0 {
		body.RetryAfter = 1
	}
	return seconds(body.RetryAfter), body.Global || a.header.Get("X-RateLimit-Global") == "true"
}

// sender makes single requests.
type sender struct {
	http          *http.Client
	base          string
	authorization string
}

func (s *sender) send(ctx context.Context, method, path string, body []byte) (answer, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, s.base+path, reader)
	if err != nil {
		return answer{}, err
	}
	request.Header.Set("Authorization", s.authorization)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := s.http.Do(request)
	if err != nil {
		return answer{}, err
	}
	reply := answer{status: response.StatusCode, header: response.Header}
	if response.StatusCode == http.StatusTooManyRequests {
		reply.payload, _ = io.ReadAll(io.LimitReader(response.Body, 4096))
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return reply, nil
}

// tally counts every answer of a run, across goroutines.
type tally struct {
	mu        sync.Mutex
	responses map[string]int
	attempts  int
	succeeded int
	transport int
}

func newTally() *tally {
	return &tally{responses: map[string]int{}}
}

func (t *tally) count(label string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.responses[label]
}

// record counts one attempt and reports whether it succeeded.
func (t *tally) record(reply answer, err error) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts++
	if err != nil {
		t.transport++
		return false
	}
	t.responses[reply.label()]++
	if reply.ok() {
		t.succeeded++
	}
	return reply.ok()
}

type routeState struct {
	turn      sync.Mutex
	known     bool
	remaining int
	resets    time.Time
}

// libraryClient sends requests the way a Discord library does for one process: one request at a
// time on each route, counting down the limit each response reports, at most 50 requests a second
// overall, and waiting out a 429 before trying again. It knows nothing of any other process using
// the same token, which is what the benchmark is about.
type libraryClient struct {
	sender *sender
	tally  *tally

	mu           sync.Mutex
	windowEnds   time.Time
	windowLeft   int
	blockedUntil time.Time
	routes       map[string]*routeState
}

func newLibraryClient(sender *sender, tally *tally) *libraryClient {
	return &libraryClient{sender: sender, tally: tally, routes: map[string]*routeState{}}
}

func (c *libraryClient) route(key string) *routeState {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.routes[key]
	if state == nil {
		state = &routeState{}
		c.routes[key] = state
	}
	return state
}

// takeGlobal waits for a place in this process's own count of the global limit.
func (c *libraryClient) takeGlobal(ctx context.Context) error {
	for {
		c.mu.Lock()
		now := time.Now()
		wait := c.blockedUntil.Sub(now)
		if wait <= 0 {
			if !now.Before(c.windowEnds) {
				c.windowEnds, c.windowLeft = now.Add(time.Second), libraryGlobalLimit
			}
			if c.windowLeft > 0 {
				c.windowLeft--
				c.mu.Unlock()
				return nil
			}
			wait = c.windowEnds.Sub(now)
		}
		c.mu.Unlock()
		if err := sleepFor(ctx, wait); err != nil {
			return err
		}
	}
}

func (c *libraryClient) block(until time.Time) {
	c.mu.Lock()
	if until.After(c.blockedUntil) {
		c.blockedUntil = until
	}
	c.mu.Unlock()
}

// do sends one request and keeps at it through 429s until it succeeds or fails another way.
func (c *libraryClient) do(ctx context.Context, method, path, routeKey string, body []byte) error {
	route := c.route(routeKey)
	route.turn.Lock()
	defer route.turn.Unlock()
	for {
		if route.known && route.remaining <= 0 {
			if err := sleepFor(ctx, time.Until(route.resets)); err != nil {
				return err
			}
		}
		if err := c.takeGlobal(ctx); err != nil {
			return err
		}
		reply, err := c.sender.send(ctx, method, path, body)
		if !c.tally.record(reply, err) && err != nil {
			return err
		}
		now := time.Now()
		if remaining, err := strconv.Atoi(reply.header.Get("X-RateLimit-Remaining")); err == nil {
			if resetAfter, err := strconv.ParseFloat(reply.header.Get("X-RateLimit-Reset-After"), 64); err == nil {
				route.known, route.remaining = true, remaining
				route.resets = now.Add(seconds(resetAfter) + clockAllowance)
			}
		}
		switch {
		case reply.ok():
			return nil
		case reply.status != http.StatusTooManyRequests:
			return fmt.Errorf("status %d", reply.status)
		}
		wait, global := reply.retryAfter()
		if global {
			c.block(now.Add(wait))
		} else {
			route.known, route.remaining, route.resets = true, 0, now.Add(wait+clockAllowance)
		}
	}
}
