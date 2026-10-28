package proxy

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	webhookFailureTTL     = time.Hour
	maxWebhookFailures    = 10_000
	discordErrorPeekBytes = 4 << 10

	unknownWebhookCode      = 10015
	invalidWebhookTokenCode = 50027

	defaultCloudflarePause = time.Minute
	maxCloudflarePause     = time.Hour
	edgeProbeInterval      = 30 * time.Second

	// sharedLimitThreshold shared-scope 429s for one resource within sharedLimitWindow close it
	// for at least minSharedLimitCooldown.
	sharedLimitThreshold   = 20
	sharedLimitWindow      = 10 * time.Second
	minSharedLimitCooldown = 10 * time.Second
	// maxSharedLimitCooldown bounds a closure however long Discord's cooldown is, because a key
	// can cover more than the resource Discord limits: every follow-up of an application shares
	// one, as its path names no channel.
	maxSharedLimitCooldown = time.Minute
	maxSharedLimitKeys     = 4096
)

// webhookGuard remembers webhooks Discord reported as deleted (10015) or whose token it
// rejected (50027). Discord restricts IPs that keep calling such webhooks, and both errors
// are permanent, so later calls are answered locally until the entry expires. The hour
// matches the Cache-Control Discord sends with 10015.
type webhookGuard struct {
	mu       sync.Mutex
	entries  map[string]*list.Element
	order    list.List
	revision atomic.Uint64
}

type webhookFailure struct {
	key     string
	status  int
	code    int
	expires time.Time
}

func newWebhookGuard() *webhookGuard {
	return &webhookGuard{entries: make(map[string]*list.Element)}
}

func (g *webhookGuard) lookup(key string, now time.Time) (webhookFailure, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	element, ok := g.entries[key]
	if !ok {
		return webhookFailure{}, false
	}
	failure := element.Value.(webhookFailure)
	if !now.Before(failure.expires) {
		g.order.Remove(element)
		delete(g.entries, key)
		return webhookFailure{}, false
	}
	return failure, true
}

func (g *webhookGuard) record(failure webhookFailure) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recordLocked(failure)
	g.revision.Add(1)
}

func (g *webhookGuard) recordLocked(failure webhookFailure) {
	if element, ok := g.entries[failure.key]; ok {
		g.order.Remove(element)
	}
	g.entries[failure.key] = g.order.PushFront(failure)
	for g.order.Len() > maxWebhookFailures {
		oldest := g.order.Back()
		g.order.Remove(oldest)
		delete(g.entries, oldest.Value.(webhookFailure).key)
	}
}

// snapshot lists the unexpired failures, oldest first.
func (g *webhookGuard) snapshot(now time.Time) []savedWebhook {
	g.mu.Lock()
	defer g.mu.Unlock()
	var saved []savedWebhook
	for element := g.order.Back(); element != nil; element = element.Prev() {
		if failure := element.Value.(webhookFailure); now.Before(failure.expires) {
			saved = append(saved, savedWebhook{Key: failure.key, Status: failure.status, Code: failure.code, Expires: failure.expires.UnixMilli()})
		}
	}
	return saved
}

// restore records the saved failures that are still valid and returns how many it recorded.
func (g *webhookGuard) restore(saved []savedWebhook, now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	restored := 0
	for _, entry := range saved {
		expires := time.UnixMilli(entry.Expires)
		permanent := entry.Status == http.StatusNotFound && entry.Code == unknownWebhookCode ||
			entry.Status == http.StatusUnauthorized && entry.Code == invalidWebhookTokenCode
		if !permanent || entry.Key == "" || !now.Before(expires) || expires.After(now.Add(webhookFailureTTL)) {
			continue
		}
		g.recordLocked(webhookFailure{key: entry.Key, status: entry.Status, code: entry.Code, expires: expires})
		restored++
	}
	return min(restored, g.order.Len())
}

// peekBody reads at most discordErrorPeekBytes of a response body and puts them back, so the
// client still receives Discord's response unchanged.
func peekBody(response *http.Response) ([]byte, error) {
	peeked, err := io.ReadAll(io.LimitReader(response.Body, discordErrorPeekBytes))
	response.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(peeked), response.Body), response.Body}
	return peeked, err
}

// observe records a permanent webhook failure.
func (g *webhookGuard) observe(key string, response *http.Response, now time.Time) {
	var want int
	switch response.StatusCode {
	case http.StatusNotFound:
		want = unknownWebhookCode
	case http.StatusUnauthorized:
		want = invalidWebhookTokenCode
	default:
		return
	}
	peeked, err := peekBody(response)
	if err != nil {
		return
	}
	var payload struct {
		Code int `json:"code"`
	}
	if json.Unmarshal(peeked, &payload) != nil || payload.Code != want {
		return
	}
	g.record(webhookFailure{key: key, status: response.StatusCode, code: want, expires: now.Add(webhookFailureTTL)})
}

func (f webhookFailure) response(request *http.Request) *http.Response {
	message := "Unknown Webhook"
	if f.code == invalidWebhookTokenCode {
		message = "Invalid Webhook Token"
	}
	return syntheticResponse(request, f.status, `{"message": "`+message+`", "code": `+strconv.Itoa(f.code)+`}`)
}

// sharedLimitGuard closes a resource after Discord refuses it with sharedLimitThreshold
// shared-scope 429s inside sharedLimitWindow, and answers its requests locally until the cooldown
// ends. Discord does not count shared 429s as invalid requests, but its edge still sees each one,
// and many requests for one hot resource, from one client or several, can collect them faster than
// any one client's backoff slows them. Keys come from sharedResourceKey; the oldest key is
// forgotten once maxSharedLimitKeys are tracked.
type sharedLimitGuard struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   list.List
}

type sharedLimitEntry struct {
	key string
	// refusals is a ring of the latest shared 429s; next is the slot the next one takes, which
	// once count reaches sharedLimitThreshold also holds the oldest.
	refusals  [sharedLimitThreshold]time.Time
	next      int
	count     int
	openUntil time.Time
}

func newSharedLimitGuard() *sharedLimitGuard {
	return &sharedLimitGuard{entries: make(map[string]*list.Element)}
}

// retryAfter returns how long key stays closed, or 0 while it is open.
func (g *sharedLimitGuard) retryAfter(key string, now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	element, ok := g.entries[key]
	if !ok {
		return 0
	}
	if openUntil := element.Value.(*sharedLimitEntry).openUntil; openUntil.After(now) {
		return openUntil.Sub(now)
	}
	return 0
}

// record notes a shared-scope 429 for key with Discord's cooldown, and returns how long key now
// stays closed when this 429 closed it.
func (g *sharedLimitGuard) record(key string, cooldown time.Duration, now time.Time) (time.Duration, bool) {
	if key == "" {
		return 0, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	var entry *sharedLimitEntry
	if element, ok := g.entries[key]; ok {
		entry = element.Value.(*sharedLimitEntry)
		g.order.MoveToFront(element)
	} else {
		entry = &sharedLimitEntry{key: key}
		g.entries[key] = g.order.PushFront(entry)
		for g.order.Len() > maxSharedLimitKeys {
			oldest := g.order.Back()
			g.order.Remove(oldest)
			delete(g.entries, oldest.Value.(*sharedLimitEntry).key)
		}
	}
	// A refusal of a request that was already in flight when key closed proves nothing new.
	if entry.openUntil.After(now) {
		return 0, false
	}
	entry.refusals[entry.next] = now
	entry.next = (entry.next + 1) % sharedLimitThreshold
	entry.count = min(entry.count+1, sharedLimitThreshold)
	if entry.count < sharedLimitThreshold || now.Sub(entry.refusals[entry.next]) > sharedLimitWindow {
		return 0, false
	}
	closure := min(max(cooldown, minSharedLimitCooldown), maxSharedLimitCooldown)
	entry.openUntil = now.Add(closure)
	entry.count = 0
	return closure, true
}

// sharedLimitResponse answers a request for a closed resource as Discord answers a shared-scope
// 429: Retry-After without an exhausted bucket, so clients hold back that request alone. It never
// reached Discord, so a client may retry it safely.
func sharedLimitResponse(request *http.Request, delay time.Duration) *http.Response {
	delay = max(delay, minimumRetryDelay)
	seconds := int64((delay + time.Second - 1) / time.Second)
	exact := strconv.FormatFloat(delay.Seconds(), 'f', 3, 64)
	response := syntheticResponse(request, http.StatusTooManyRequests,
		`{"message": "The resource is being rate limited.", "retry_after": `+exact+`, "global": false}`)
	response.Header.Set("Retry-After", strconv.FormatInt(seconds, 10))
	response.Header.Set("X-RateLimit-Scope", "shared")
	return response
}

// cloudflareGuard pauses outbound traffic while Discord's edge blocks this IP. Discord's own
// responses carry Via: 1.1 google, so a 429 or 403 without Via never reached Discord, and
// every further request only prolongs the block. The edge also refuses single requests, such
// as one with a User-Agent it rejects, so a probe of Sluice's own confirms the IP is blocked
// before every client is paused.
type cloudflareGuard struct {
	mu           sync.Mutex
	blockedUntil time.Time
	probing      bool
	clearedAt    time.Time
	revision     atomic.Uint64
}

// suspect reports whether the caller should start a probe. It does not while a probe or a block
// is under way, or shortly after a probe found this IP unblocked. Traffic keeps flowing while the
// probe runs, so one refused client never stalls the others.
func (g *cloudflareGuard) suspect(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.probing || g.blockedUntil.After(now) || now.Sub(g.clearedAt) < edgeProbeInterval {
		return false
	}
	g.probing = true
	return true
}

type edgeVerdict uint8

const (
	// edgeInconclusive: the probe failed or was never sent, which proves nothing.
	edgeInconclusive edgeVerdict = iota
	edgeClear
	edgeBlocked
)

// settle records a probe's verdict. A block pauses traffic for pause, a clear probe holds off the
// next one, and an inconclusive one lets the next refusal probe again.
func (g *cloudflareGuard) settle(now time.Time, verdict edgeVerdict, pause time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.probing = false
	switch verdict {
	case edgeClear:
		g.clearedAt = now
	case edgeBlocked:
		g.blockedUntil = now.Add(pause)
		g.revision.Add(1)
	}
}

// snapshot returns the end of the block in progress, or the zero time.
func (g *cloudflareGuard) snapshot(now time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.blockedUntil.After(now) {
		return time.Time{}
	}
	return g.blockedUntil
}

// restore resumes a saved block that has not ended, and reports whether it did.
func (g *cloudflareGuard) restore(until, now time.Time) bool {
	if !until.After(now) {
		return false
	}
	if latest := now.Add(maxCloudflarePause); until.After(latest) {
		until = latest
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.blockedUntil = until
	return true
}

func (g *cloudflareGuard) retryAfter(now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.blockedUntil.After(now) {
		return 0
	}
	return g.blockedUntil.Sub(now)
}

func isCloudflareBlock(response *http.Response) bool {
	return (response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusForbidden) &&
		response.Header.Get("Via") == ""
}

func cloudflarePause(header http.Header) time.Duration {
	seconds, err := strconv.ParseFloat(header.Get("Retry-After"), 64)
	if err != nil || seconds <= 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return defaultCloudflarePause
	}
	// Compared as seconds: a large enough number overflows the conversion to a Duration.
	if seconds >= maxCloudflarePause.Seconds() {
		return maxCloudflarePause
	}
	return max(time.Duration(seconds*float64(time.Second)), time.Second)
}

// cloudflarePauseResponse leaves out Via so clients recognise an edge block and back off.
func cloudflarePauseResponse(request *http.Request, pause time.Duration) *http.Response {
	response := rememberedRateLimitResponse(request, pause, true)
	response.Header.Del("Via")
	return response
}

// suspectEdgeBlock checks whether an edge refusal means this IP is blocked. route names the
// refused request in the log.
func (p *Proxy) suspectEdgeBlock(refusal *http.Response, route string) {
	if !p.cloudflare.suspect(time.Now()) {
		return
	}
	pause := cloudflarePause(refusal.Header)
	status := refusal.StatusCode
	started := p.goBackground(func() {
		ctx, cancel := context.WithTimeout(p.ctx, p.config.UpstreamTimeout)
		defer cancel()
		verdict, probePause, probeErr := p.probeEdge(ctx)
		pause = max(pause, probePause)
		p.cloudflare.settle(time.Now(), verdict, pause)
		switch verdict {
		case edgeBlocked:
			logger.Warn("Discord's edge blocked this IP; pausing outbound requests", "status", status, "pause", pause.String())
			if p.config.EnableMetrics {
				CloudflareBlocks.Inc()
			}
		case edgeClear:
			logger.Warn("Discord's edge refused a request, but this IP is not blocked; only that route backs off", "status", status, "route", route)
		default:
			logger.Warn("Could not check whether Discord's edge blocks this IP; only the refused route backs off", "status", status, "route", route, "error", probeErr)
		}
	})
	if !started {
		p.cloudflare.settle(time.Now(), edgeInconclusive, 0)
	}
}

// probeEdge requests the gateway URL with Sluice's own User-Agent and no credential. Only a refusal
// of that request too shows that the edge blocks this IP. The error says why a probe was
// inconclusive.
func (p *Proxy) probeEdge(ctx context.Context) (edgeVerdict, time.Duration, error) {
	target := *p.discordURL
	target.Path = "/api/v10/gateway"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return edgeInconclusive, 0, err
	}
	if !p.invalidRequests.reserve(time.Now()) {
		return edgeInconclusive, 0, errInvalidRequestBudget
	}
	request.Header.Set("User-Agent", userAgent())
	response, err := p.transport.RoundTrip(request)
	if err != nil || response == nil {
		p.invalidRequests.complete(time.Now(), false)
		if err == nil {
			err = errors.New("upstream transport returned no response")
		}
		return edgeInconclusive, 0, err
	}
	if response.Body != nil {
		defer func() { _ = response.Body.Close() }()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, discordErrorPeekBytes))
	}
	p.invalidRequests.complete(time.Now(), invalidDiscordResponse(response))
	if isCloudflareBlock(response) {
		return edgeBlocked, cloudflarePause(response.Header), nil
	}
	return edgeClear, 0, nil
}
