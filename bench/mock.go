package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const windowShards = 64

// sampleMessage is what the mock answers a successful call with: a message object of the size
// Discord returns for a short text message.
const sampleMessage = `{"type":0,"content":"The quick brown fox jumps over the lazy dog, twice, to fill a line of chat.",` +
	`"mentions":[],"mention_roles":[],"attachments":[],"embeds":[],"timestamp":"2026-01-01T00:00:00.000000+00:00",` +
	`"edited_timestamp":null,"flags":0,"components":[],"id":"1300000000000000001","channel_id":"1200000000000000001",` +
	`"author":{"id":"900000000000000001","username":"bench","avatar":null,"discriminator":"0000","public_flags":0,` +
	`"flags":0,"bot":true,"banner":null,"accent_color":null,"global_name":null,"avatar_decoration_data":null,` +
	`"collectibles":null,"display_name_styles":null,"banner_color":null,"clan":null,"primary_guild":null},` +
	`"pinned":false,"mention_everyone":false,"tts":false,"nonce":"1300000000000000002","referenced_message":null}`

// window is a rate limit as Discord describes its buckets: it opens with the first request and
// allows a fixed number of them until it closes.
type window struct {
	opened time.Time
	used   int
}

// take counts one request and reports what is left and how long until the window closes.
func (w *window) take(now time.Time, limit int, length time.Duration) (remaining int, resetAfter time.Duration, allowed bool) {
	if w.opened.IsZero() || !now.Before(w.opened.Add(length)) {
		w.opened, w.used = now, 0
	}
	resetAfter = w.opened.Add(length).Sub(now)
	if w.used >= limit {
		return 0, resetAfter, false
	}
	w.used++
	return limit - w.used, resetAfter, true
}

// mockStats is the mock's own count of what reached it, the ground truth every result is checked
// against.
type mockStats struct {
	Received      int64 `json:"received"`
	Succeeded     int64 `json:"succeeded"`
	LimitedGlobal int64 `json:"limitedGlobal"`
	LimitedRoute  int64 `json:"limitedRoute"`
	BodyBytes     int64 `json:"bodyBytes"`
	ResponseBytes int   `json:"responseBytes"`
}

// since is what the mock counted after an earlier reading of its stats.
func (s mockStats) since(earlier mockStats) mockStats {
	return mockStats{
		Received:      s.Received - earlier.Received,
		Succeeded:     s.Succeeded - earlier.Succeeded,
		LimitedGlobal: s.LimitedGlobal - earlier.LimitedGlobal,
		LimitedRoute:  s.LimitedRoute - earlier.LimitedRoute,
		BodyBytes:     s.BodyBytes - earlier.BodyBytes,
		ResponseBytes: s.ResponseBytes,
	}
}

// mockDiscord answers like Discord's REST API as far as rate limits go: a limit per route and
// channel, guild or webhook, a limit per bot across all routes, the headers that report both, and
// 429s shaped like Discord's.
type mockDiscord struct {
	routeLimit  int
	routeWindow time.Duration
	globalLimit int
	latency     time.Duration

	message, messageGzip []byte

	shards [windowShards]struct {
		sync.Mutex
		windows map[string]*window
	}

	received, succeeded, limitedGlobal, limitedRoute, bodyBytes atomic.Int64
}

func newMockDiscord(routeLimit int, routeWindow time.Duration, globalLimit int, latency time.Duration) (*mockDiscord, error) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(sampleMessage)); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	discord := &mockDiscord{
		routeLimit:  routeLimit,
		routeWindow: routeWindow,
		globalLimit: globalLimit,
		latency:     latency,
		message:     []byte(sampleMessage),
		messageGzip: compressed.Bytes(),
	}
	for i := range discord.shards {
		discord.shards[i].windows = map[string]*window{}
	}
	return discord, nil
}

func (m *mockDiscord) take(key string, now time.Time, limit int, length time.Duration) (int, time.Duration, bool) {
	shard := &m.shards[fnv32(key)%windowShards]
	shard.Lock()
	defer shard.Unlock()
	current := shard.windows[key]
	if current == nil {
		current = &window{}
		shard.windows[key] = current
	}
	return current.take(now, limit, length)
}

func (m *mockDiscord) stats() mockStats {
	return mockStats{
		Received:      m.received.Load(),
		Succeeded:     m.succeeded.Load(),
		LimitedGlobal: m.limitedGlobal.Load(),
		LimitedRoute:  m.limitedRoute.Load(),
		BodyBytes:     m.bodyBytes.Load(),
		ResponseBytes: len(m.message),
	}
}

func (m *mockDiscord) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	// Discord's own responses carry this; Sluice reads a 429 without it as a refusal by the edge.
	header.Set("Via", "1.1 google")
	header.Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/__bench/stats":
		_ = json.NewEncoder(w).Encode(m.stats())
		return
	case strings.HasSuffix(r.URL.Path, "/gateway"):
		_, _ = io.WriteString(w, `{"url":"wss://gateway.discord.gg"}`)
		return
	}

	read, _ := io.Copy(io.Discard, r.Body)
	m.received.Add(1)
	m.bodyBytes.Add(read)
	now := time.Now()
	credential := r.Header.Get("Authorization")
	if credential != "" {
		if _, resetAfter, allowed := m.take("global\x00"+credential, now, m.globalLimit, time.Second); !allowed {
			m.limitedGlobal.Add(1)
			header.Set("X-RateLimit-Global", "true")
			header.Set("X-RateLimit-Scope", "global")
			reject(w, resetAfter, true)
			return
		}
	}

	route, scope := routeOf(r.Method, r.URL.Path)
	remaining, resetAfter, allowed := m.take(credential+"\x00"+route+"\x00"+scope, now, m.routeLimit, m.routeWindow)
	header.Set("X-RateLimit-Bucket", strconv.FormatUint(uint64(fnv32(route)), 16))
	header.Set("X-RateLimit-Limit", strconv.Itoa(m.routeLimit))
	header.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	header.Set("X-RateLimit-Reset-After", formatSeconds(resetAfter))
	header.Set("X-RateLimit-Reset", strconv.FormatFloat(float64(now.Add(resetAfter).UnixMilli())/1000, 'f', 3, 64))
	if !allowed {
		m.limitedRoute.Add(1)
		header.Set("X-RateLimit-Scope", "user")
		reject(w, resetAfter, false)
		return
	}

	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.succeeded.Add(1)
	body := m.message
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		header.Set("Content-Encoding", "gzip")
		body = m.messageGzip
	}
	header.Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

// reject answers 429 the way Discord does: whole seconds in Retry-After and the exact wait in
// the body.
func reject(w http.ResponseWriter, wait time.Duration, global bool) {
	wait = max(wait, time.Millisecond)
	w.Header().Set("Retry-After", strconv.FormatInt(int64((wait+time.Second-1)/time.Second), 10))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = fmt.Fprintf(w, `{"message":"You are being rate limited.","retry_after":%s,"global":%t}`, formatSeconds(wait), global)
}

// routeOf reduces a path to what Discord limits: the route with its identifiers blanked, and the
// channel, guild or webhook the limit is scoped to.
func routeOf(method, path string) (route, scope string) {
	trimmed := strings.TrimPrefix(path, "/api/")
	if version, rest, found := strings.Cut(trimmed, "/"); found && strings.HasPrefix(version, "v") {
		trimmed = rest
	}
	var built strings.Builder
	built.WriteString(method)
	segments := strings.Split(trimmed, "/")
	for i, segment := range segments {
		built.WriteByte('/')
		if !numeric(segment) {
			built.WriteString(segment)
			continue
		}
		built.WriteByte('!')
		if i == 1 && (segments[0] == "channels" || segments[0] == "guilds" || segments[0] == "webhooks") {
			scope = segment
		}
	}
	return built.String(), scope
}

func numeric(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func fnv32(value string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= 16777619
	}
	return hash
}

func formatSeconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'f', 3, 64)
}

func runMock(args []string) error {
	flags := flag.NewFlagSet("mock", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:9100", "address to listen on")
	routeLimit := flags.Int("route-limit", 5, "requests each route allows per window")
	routeWindow := flags.Duration("route-window", 5*time.Second, "length of a route's window")
	globalLimit := flags.Int("global-limit", 50, "requests each bot may send per second")
	latency := flags.Duration("latency", 0, "time the mock takes to answer a request it accepts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	discord, err := newMockDiscord(*routeLimit, *routeWindow, *globalLimit, *latency)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *addr, Handler: discord, ReadHeaderTimeout: 10 * time.Second}
	return server.ListenAndServe()
}
