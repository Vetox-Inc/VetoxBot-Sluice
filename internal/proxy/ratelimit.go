package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	discordGlobalLimit = 50
	// InvalidRequestSafetyLimit leaves headroom below Discord's 10,000-event threshold.
	InvalidRequestSafetyLimit = 9500
	invalidRequestWindow      = 10 * time.Minute
	minimumRetryDelay         = 50 * time.Millisecond
	rateLimitHeaderSlack      = 5 * time.Millisecond
	globalPacingSlack         = 200
	maximumRateLimitDelay     = 24 * time.Hour
)

var errInvalidRequestBudget = fmt.Errorf("Discord invalid-request safety budget exhausted")

type invalidRequestGuard struct {
	mu         sync.Mutex
	limit      int
	window     time.Duration
	timestamps []time.Time
	head       int
	reserved   int
	// revision changes with every recorded invalid response, telling the state file to save.
	revision atomic.Uint64
}

func newInvalidRequestGuard(limit int, window time.Duration) *invalidRequestGuard {
	return &invalidRequestGuard{limit: limit, window: window}
}

func (g *invalidRequestGuard) available(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	return len(g.timestamps)-g.head+g.reserved < g.limit
}

func (g *invalidRequestGuard) count(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	return len(g.timestamps) - g.head
}

func (g *invalidRequestGuard) reserve(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	if len(g.timestamps)-g.head+g.reserved >= g.limit {
		return false
	}
	g.reserved++
	return true
}

func (g *invalidRequestGuard) complete(now time.Time, invalid bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved--
	if invalid {
		g.timestamps = append(g.timestamps, now)
		g.revision.Add(1)
	}
	g.prune(now)
}

// snapshot counts the invalid responses still in the window per unix second.
func (g *invalidRequestGuard) snapshot(now time.Time) [][2]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	var seconds [][2]int64
	for _, at := range g.timestamps[g.head:] {
		if last := len(seconds) - 1; last >= 0 && seconds[last][0] == at.Unix() {
			seconds[last][1]++
		} else {
			seconds = append(seconds, [2]int64{at.Unix(), 1})
		}
	}
	return seconds
}

// restore records saved invalid responses that are still inside the window, and returns how
// many it recorded.
func (g *invalidRequestGuard) restore(seconds [][2]int64, now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	oldest := now.Add(-g.window)
	restored := 0
	for _, entry := range seconds {
		at := time.Unix(entry[0], 0)
		if !at.After(oldest) || at.After(now) {
			continue
		}
		for count := entry[1]; count > 0 && restored < g.limit; count-- {
			g.timestamps = append(g.timestamps, at)
			restored++
		}
	}
	return restored
}

func (g *invalidRequestGuard) prune(now time.Time) {
	oldest := now.Add(-g.window)
	for g.head < len(g.timestamps) && !g.timestamps[g.head].After(oldest) {
		g.head++
	}
	if g.head > 4096 && g.head*2 > len(g.timestamps) {
		g.timestamps = append([]time.Time(nil), g.timestamps[g.head:]...)
		g.head = 0
	}
}

func invalidDiscordResponse(response *http.Response) bool {
	if response.StatusCode == http.StatusTooManyRequests {
		return !strings.EqualFold(response.Header.Get("X-RateLimit-Scope"), "shared")
	}
	return response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden
}

// pacer spaces global requests evenly. This is deliberately burst-free: a
// rolling one-second window cannot exceed Discord's documented allowance.
type pacer struct {
	gate fifoGate

	mu           sync.Mutex
	interval     time.Duration
	next         time.Time
	blockedUntil time.Time
	wake         chan struct{}
}

func newPacer(limit uint, maxWaiters int) *pacer {
	if limit == 0 {
		limit = discordGlobalLimit
	}
	interval := (time.Second + time.Duration(limit) - 1) / time.Duration(limit)
	interval += interval / globalPacingSlack
	return &pacer{
		gate:     newFIFOGate(maxWaiters),
		interval: interval,
		wake:     make(chan struct{}),
	}
}

func (p *pacer) wait(ctx context.Context) error {
	delay, err := p.waitFor(ctx)
	if err != nil || delay == 0 {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// waitFor takes the next send slot, or returns a global block's delay at once when it would
// outlast ctx.
func (p *pacer) waitFor(ctx context.Context) (time.Duration, error) {
	if err := p.gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer p.gate.release()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		p.mu.Lock()
		readyAt := p.next
		if p.blockedUntil.After(readyAt) {
			readyAt = p.blockedUntil
		}
		now := time.Now()
		if !readyAt.After(now) {
			p.next = now.Add(p.interval)
			p.mu.Unlock()
			return 0, nil
		}
		blockedDelay := p.blockedUntil.Sub(now)
		wake := p.wake
		p.mu.Unlock()
		if blockedDelay > 0 {
			if deadline, ok := ctx.Deadline(); ok && blockedDelay >= time.Until(deadline) {
				return blockedDelay, nil
			}
		}

		timer := time.NewTimer(time.Until(readyAt))
		select {
		case <-timer.C:
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, ctx.Err()
		}
	}
}

func (p *pacer) blockFor(delay time.Duration) {
	if delay < minimumRetryDelay {
		delay = minimumRetryDelay
	}
	p.mu.Lock()
	readyAt := time.Now().Add(delay)
	if readyAt.After(p.blockedUntil) {
		p.blockedUntil = readyAt
		close(p.wake)
		p.wake = make(chan struct{})
	}
	p.mu.Unlock()
}

func (p *pacer) blocked(now time.Time) bool {
	return p.retryAfter(now) > 0
}

func (p *pacer) retryAfter(now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.blockedUntil.After(now) {
		return 0
	}
	return p.blockedUntil.Sub(now)
}

type rateLimitInfo struct {
	bucket    string
	scope     string
	remaining int64
	// resetAfter is when the bucket reopens; retryAfter is a 429's own cooldown. They differ
	// for a shared-scope 429, which limits one resource while its bucket keeps capacity.
	resetAfter time.Duration
	retryAfter time.Duration
	global     bool
}

func (i rateLimitInfo) shared() bool { return i.scope == "shared" }

// parseRateLimitHeaders reads Discord's rate-limit headers. retryAfter is the 429's cooldown
// from discordRetryAfter, or "" for any other response.
func parseRateLimitHeaders(header http.Header, statusCode int, retryAfter string, now time.Time) (rateLimitInfo, error) {
	info := rateLimitInfo{remaining: -1}
	if header == nil {
		return info, fmt.Errorf("missing rate-limit headers")
	}
	if header.Get("X-RateLimit-Bucket") == "" && header.Get("X-RateLimit-Remaining") == "" &&
		header.Get("X-RateLimit-Reset-After") == "" && retryAfter == "" &&
		header.Get("X-RateLimit-Global") == "" && header.Get("X-RateLimit-Scope") == "" {
		return info, nil
	}

	info.bucket = header.Get("X-RateLimit-Bucket")
	info.scope = strings.ToLower(header.Get("X-RateLimit-Scope"))
	info.global = strings.EqualFold(header.Get("X-RateLimit-Global"), "true") || info.scope == "global"

	if value := header.Get("X-RateLimit-Remaining"); value != "" {
		remaining, err := strconv.ParseInt(value, 10, 32)
		if err != nil || remaining < 0 {
			return info, fmt.Errorf("invalid X-RateLimit-Remaining %q", value)
		}
		info.remaining = remaining
	}

	if statusCode == http.StatusTooManyRequests && retryAfter != "" {
		delay, err := parseRateLimitSeconds(retryAfter)
		if err != nil {
			return info, err
		}
		info.retryAfter = delay
	}
	resetValue := header.Get("X-RateLimit-Reset-After")
	if statusCode == http.StatusTooManyRequests && retryAfter != "" && !info.shared() {
		info.resetAfter = info.retryAfter
	} else if resetValue != "" {
		delay, err := parseRateLimitSeconds(resetValue)
		if err != nil {
			return info, err
		}
		info.resetAfter = delay
	} else if resetAt := header.Get("X-RateLimit-Reset"); resetAt != "" {
		seconds, err := strconv.ParseFloat(resetAt, 64)
		if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			return info, fmt.Errorf("invalid X-RateLimit-Reset %q", resetAt)
		}
		remainingSeconds := seconds - float64(now.UnixNano())/float64(time.Second)
		if remainingSeconds <= 0 {
			info.resetAfter = 0
		} else if remainingSeconds >= maximumRateLimitDelay.Seconds() {
			info.resetAfter = maximumRateLimitDelay
		} else {
			info.resetAfter = time.Duration(remainingSeconds * float64(time.Second))
		}
	}

	if (info.remaining == 0 || statusCode == http.StatusTooManyRequests) && info.resetAfter > 0 {
		info.resetAfter += rateLimitHeaderSlack
	}
	if info.retryAfter > 0 {
		info.retryAfter += rateLimitHeaderSlack
	}
	return info, nil
}

func parseRateLimitSeconds(value string) (time.Duration, error) {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, fmt.Errorf("invalid rate-limit reset %q", value)
	}
	if seconds >= maximumRateLimitDelay.Seconds() {
		return maximumRateLimitDelay, nil
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

// discordRetryAfter returns a 429's cooldown in seconds: the retry_after of Discord's body when
// it refines the whole-second Retry-After header, else the header. The body alone is not trusted
// because API v6 and v7 report it in milliseconds.
func discordRetryAfter(header http.Header, body []byte) string {
	value := header.Get("Retry-After")
	whole, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value
	}
	var payload struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if json.Unmarshal(body, &payload) == nil && payload.RetryAfter > whole-1 && payload.RetryAfter <= whole {
		return strconv.FormatFloat(payload.RetryAfter, 'f', -1, 64)
	}
	return value
}

func (s *clientState) observeResponse(routeHash uint64, majorKey, bucketPath string, interaction, credentialScoped bool, bucket *bucketState, response *http.Response, retryAfter string, disable401Lock bool) (*bucketState, rateLimitInfo) {
	if response.StatusCode == http.StatusUnauthorized && credentialScoped && s.identity.kind != authNone && !disable401Lock {
		s.validity.Store(clientInvalid)
	}

	info, err := parseRateLimitHeaders(response.Header, response.StatusCode, retryAfter, time.Now())
	if err != nil {
		if response.StatusCode == http.StatusTooManyRequests {
			if info.global {
				s.global.blockFor(minimumRetryDelay)
				if interaction {
					bucket.blockUntil(time.Now().Add(minimumRetryDelay))
				}
			} else if !info.shared() {
				bucket.blockUntil(time.Now().Add(minimumRetryDelay))
			}
		}
		target := s.learnBucket(routeHash, info.bucket, majorKey, bucket)
		logger.Warn("Ignoring invalid Discord rate-limit headers", "error", err, "path", bucketPath)
		return target, info
	}

	if info.global {
		if response.StatusCode == http.StatusTooManyRequests {
			s.global.blockFor(info.resetAfter)
			if interaction {
				delay := info.resetAfter
				if delay < minimumRetryDelay {
					delay = minimumRetryDelay
				}
				bucket.blockUntil(time.Now().Add(delay))
			}
		}
	} else if info.remaining == 0 || response.StatusCode == http.StatusTooManyRequests && !info.shared() {
		if info.resetAfter < minimumRetryDelay && response.StatusCode == http.StatusTooManyRequests {
			info.resetAfter = minimumRetryDelay
		}
		bucket.blockUntil(time.Now().Add(info.resetAfter))
	}
	return s.learnBucket(routeHash, info.bucket, majorKey, bucket), info
}

// parseBotWideRoutes reads comma-separated "METHOD /route" entries, with identifiers written as
// "!" the way the route label of sluice_requests shows them.
func parseBotWideRoutes(value string) (map[string]bool, error) {
	routes := make(map[string]bool)
	for raw := range strings.SplitSeq(value, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		method, route, found := strings.Cut(entry, " ")
		route = strings.TrimSpace(route)
		if !found || metricsMethodLabel(method) == "OTHER" || !strings.HasPrefix(route, "/") {
			return nil, fmt.Errorf("invalid BOT_WIDE_ROUTES entry %q; use a method and a route, such as GET /guilds/!/vanity-url", entry)
		}
		routes[method+" "+MetricsPathFromBucket(route)] = true
	}
	return routes, nil
}

func parseGlobalOverrides(value string) (map[string]uint, error) {
	overrides := make(map[string]uint)
	if strings.TrimSpace(value) == "" {
		return overrides, nil
	}
	for _, rawOverride := range strings.Split(value, ",") {
		key, rawLimit, ok := strings.Cut(strings.TrimSpace(rawOverride), ":")
		if !ok {
			return nil, fmt.Errorf("invalid BOT_RATELIMIT_OVERRIDES entry %q", rawOverride)
		}
		if key == "sha256" {
			fingerprint, remainder, found := strings.Cut(rawLimit, ":")
			if !found {
				return nil, fmt.Errorf("invalid token fingerprint override %q", rawOverride)
			}
			if fingerprint != strings.ToLower(fingerprint) {
				return nil, fmt.Errorf("token fingerprint must use lowercase hexadecimal")
			}
			if decoded, err := hex.DecodeString(fingerprint); err != nil || len(decoded) != sha256.Size {
				return nil, fmt.Errorf("invalid token fingerprint %q", fingerprint)
			}
			key, rawLimit = "sha256:"+fingerprint, remainder
		} else if key == "" || !isNumericInput(key) {
			return nil, fmt.Errorf("override key must be a bot ID or sha256 fingerprint")
		}

		limit, err := strconv.ParseUint(rawLimit, 10, 32)
		if err != nil || limit == 0 {
			return nil, fmt.Errorf("invalid global rate limit %q", rawLimit)
		}
		overrides[key] = uint(limit)
	}
	return overrides, nil
}
