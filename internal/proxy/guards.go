package proxy

import (
	"bytes"
	"container/list"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
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
)

// webhookGuard remembers webhooks Discord reported as deleted (10015) or whose token it
// rejected (50027). Discord restricts IPs that keep calling such webhooks, and both errors
// are permanent, so later calls are answered locally until the entry expires. The hour
// matches the Cache-Control Discord sends with 10015.
type webhookGuard struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   list.List
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

// observe records a permanent webhook failure. It reads at most discordErrorPeekBytes of the
// body and puts them back, so the client still receives Discord's response unchanged.
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
	peeked, err := io.ReadAll(io.LimitReader(response.Body, discordErrorPeekBytes))
	response.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(peeked), response.Body), response.Body}
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

// cloudflareGuard pauses outbound traffic once Discord's edge blocks this IP. Discord's own
// responses carry Via: 1.1 google, so a 429 or 403 without Via never reached Discord, and
// every further request only prolongs the block.
type cloudflareGuard struct {
	mu           sync.Mutex
	blockedUntil time.Time
}

// block extends the pause and reports whether a new pause started.
func (g *cloudflareGuard) block(now time.Time, pause time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	started := !g.blockedUntil.After(now)
	if until := now.Add(pause); until.After(g.blockedUntil) {
		g.blockedUntil = until
	}
	return started
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
	return min(max(time.Duration(seconds*float64(time.Second)), time.Second), maxCloudflarePause)
}

// cloudflarePauseResponse leaves out Via so clients recognise an edge block and back off.
func cloudflarePauseResponse(request *http.Request, pause time.Duration) *http.Response {
	response := rememberedRateLimitResponse(request, pause, true)
	response.Header.Del("Via")
	return response
}
