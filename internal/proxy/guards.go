package proxy

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// An interaction token lives 15 minutes, so a 401 for one holds for the rest of its life.
	interactionFailureTTL  = 15 * time.Minute
	maxInteractionFailures = 10_000

	unknownWebhookCode      = 10015
	invalidWebhookTokenCode = 50027

	defaultCloudflarePause = time.Minute
	// maxCloudflarePause bounds a pause only so a malformed Retry-After cannot stop traffic for
	// good. Discord's edge can block an IP for a day, and a shorter bound would resume traffic
	// into a block still in force.
	maxCloudflarePause = 25 * time.Hour
	// maxEdgeRouteBackoff bounds how long one refused route backs off. A block of the whole IP
	// pauses everything for its own Retry-After instead, so a route need not wait as long.
	maxEdgeRouteBackoff = time.Hour
	// edgeProbeInterval holds off the next probe after one checks a suspected block, whatever it
	// found, so refusals never probe the edge more often.
	edgeProbeInterval = 30 * time.Second
	// edgeSuspicionRefusals edge refusals within one hold-off pause traffic provisionally even when
	// the probe found the edge clear or has not answered: the edge lets the odd request through.
	edgeSuspicionRefusals = 10
	// provisionalEdgePause is how long traffic pauses on suspicion alone.
	provisionalEdgePause = time.Minute
	// edgeClearConfirmations clear probes in a row end a pause in force: during a ban the edge
	// lets the odd request through, and one of them must not resume every client.
	edgeClearConfirmations = 2
	// edgeConfirmDelay is how soon a clear probe is confirmed, instead of the probe interval.
	edgeConfirmDelay = time.Minute

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
	// sharedTrialRetryDelay is what requests for a reopening resource are told to wait while its
	// trial request is out.
	sharedTrialRetryDelay = time.Second
)

// webhookGuard remembers webhooks Discord reported as deleted (10015) or whose token it
// rejected (50027). Discord restricts IPs that keep calling such webhooks, and both errors
// are permanent, so later calls are answered locally until the entry expires. The hour
// matches the Cache-Control Discord sends with 10015.
//
// The same guard, with its own bound and a 15-minute entry, remembers interaction tokens Discord
// answered 401: an expired token stays expired, and a bot that keeps following up on one would
// otherwise collect a 401 per call.
type webhookGuard struct {
	mu       sync.Mutex
	ttl      time.Duration
	limit    int
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
	return &webhookGuard{ttl: webhookFailureTTL, limit: maxWebhookFailures, entries: make(map[string]*list.Element)}
}

func newInteractionTokenGuard() *webhookGuard {
	return &webhookGuard{ttl: interactionFailureTTL, limit: maxInteractionFailures, entries: make(map[string]*list.Element)}
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
	for g.order.Len() > g.limit {
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
	g.record(webhookFailure{key: key, status: response.StatusCode, code: want, expires: now.Add(g.ttl)})
}

// observeInteraction records a 401 for an interaction token, with the code Discord gave it.
func (g *webhookGuard) observeInteraction(key string, response *http.Response, now time.Time) {
	if response.StatusCode != http.StatusUnauthorized {
		return
	}
	var payload struct {
		Code int `json:"code"`
	}
	if peeked, err := peekBody(response); err == nil {
		_ = json.Unmarshal(peeked, &payload)
	}
	g.record(webhookFailure{key: key, status: http.StatusUnauthorized, code: payload.Code, expires: now.Add(g.ttl)})
}

func (f webhookFailure) response(request *http.Request) *http.Response {
	message := "401: Unauthorized"
	switch f.code {
	case unknownWebhookCode:
		message = "Unknown Webhook"
	case invalidWebhookTokenCode:
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
	// halfOpen marks a closure that has ended without a request proving the limit gone: one trial
	// request at a time may go, and a shared 429 on it closes the key again.
	halfOpen bool
	trial    bool
}

func newSharedLimitGuard() *sharedLimitGuard {
	return &sharedLimitGuard{entries: make(map[string]*list.Element)}
}

// retryAfter returns how long key stays closed, or 0 while a request may try it.
func (g *sharedLimitGuard) retryAfter(key string, now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	element, ok := g.entries[key]
	if !ok {
		return 0
	}
	entry := element.Value.(*sharedLimitEntry)
	switch {
	case entry.openUntil.After(now):
		return entry.openUntil.Sub(now)
	case entry.halfOpen && entry.trial:
		return sharedTrialRetryDelay
	}
	return 0
}

// admit is retryAfter for a request about to be sent. When key's closure has ended, it lets one
// request through as the trial, which reports back with settleTrial.
func (g *sharedLimitGuard) admit(key string, now time.Time) (time.Duration, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	element, ok := g.entries[key]
	if !ok {
		return 0, false
	}
	entry := element.Value.(*sharedLimitEntry)
	switch {
	case entry.openUntil.After(now):
		return entry.openUntil.Sub(now), false
	case !entry.halfOpen:
		return 0, false
	case entry.trial:
		return sharedTrialRetryDelay, false
	}
	entry.trial = true
	return 0, true
}

// settleTrial ends key's trial: a shared 429 closes key again for Discord's cooldown, any other
// answer from Discord opens it, and no answer lets the next request try.
func (g *sharedLimitGuard) settleTrial(key string, now time.Time, response *http.Response, info rateLimitInfo) (time.Duration, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	element, ok := g.entries[key]
	if !ok {
		return 0, false
	}
	entry := element.Value.(*sharedLimitEntry)
	entry.trial = false
	switch {
	case response == nil:
	case response.StatusCode == http.StatusTooManyRequests && info.shared():
		closure := min(max(info.retryAfter, minSharedLimitCooldown), maxSharedLimitCooldown)
		entry.openUntil = now.Add(closure)
		return closure, true
	default:
		entry.halfOpen = false
		entry.count = 0
	}
	return 0, false
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
	// A refusal of a request that was already in flight when key closed proves nothing new, and
	// once key reopens only its trial decides.
	if entry.openUntil.After(now) || entry.halfOpen {
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
	entry.halfOpen = true
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
	// probedAt starts the hold-off after a suspicion probe, and suspicions counts the edge
	// refusals since, the one that started the probe included.
	probedAt   time.Time
	suspicions int
	// clears counts clear probes in a row during the pause in force.
	clears   int
	revision atomic.Uint64
}

type suspicion uint8

const (
	suspicionNone suspicion = iota
	// suspicionProbe: the caller probes the edge and settles the verdict.
	suspicionProbe
	// suspicionPause: refusals alone paused traffic provisionally.
	suspicionPause
)

// suspect records an edge refusal and says what to do about it. The first refusal after a
// hold-off starts a probe, and the hold-off follows every probe whatever it finds, so refusals
// never probe the edge more often than once per edgeProbeInterval. Traffic keeps flowing while the
// probe runs, so one refused client never stalls the others; but edgeSuspicionRefusals within one
// hold-off pause traffic provisionally, whether the probe found the edge clear or has not answered.
func (g *cloudflareGuard) suspect(now time.Time) suspicion {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blockedUntil.After(now) {
		return suspicionNone
	}
	if !g.probing && now.Sub(g.probedAt) >= edgeProbeInterval {
		g.probing = true
		g.probedAt = now
		g.suspicions = 1
		return suspicionProbe
	}
	if g.suspicions++; g.suspicions < edgeSuspicionRefusals {
		return suspicionNone
	}
	g.suspicions = 0
	g.pauseLocked(now, provisionalEdgePause)
	return suspicionPause
}

func (g *cloudflareGuard) pauseLocked(now time.Time, pause time.Duration) bool {
	if until := now.Add(pause); until.After(g.blockedUntil) {
		g.blockedUntil = until
		g.clears = 0
		g.revision.Add(1)
		return true
	}
	return false
}

type edgeVerdict uint8

const (
	// edgeInconclusive: the probe failed or was never sent, which proves nothing.
	edgeInconclusive edgeVerdict = iota
	edgeClear
	edgeBlocked
)

// beginPausedProbe reports whether a probe should check the pause in force, and marks it
// running. As with a suspected block, one probe runs at a time.
func (g *cloudflareGuard) beginPausedProbe(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.probing || !g.blockedUntil.After(now) {
		return false
	}
	g.probing = true
	return true
}

// settle records a probe's verdict and reports whether the pause changed. A block pauses traffic
// for pause, or longer if a pause in force already runs past it, so a refusal without Retry-After
// cannot cut a pause short. A pause in force ends after edgeClearConfirmations clear probes in a
// row, as the block lifted early, and without a hold-off, so the first refusal after it can re-arm
// the pause at once. An inconclusive probe without a pause in force pauses provisionally, as the
// refusal that started it stays unexplained; during a pause it changes nothing but the run of
// clear ones.
func (g *cloudflareGuard) settle(now time.Time, verdict edgeVerdict, pause time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.probing = false
	if verdict != edgeClear {
		g.clears = 0
	}
	switch verdict {
	case edgeClear:
		if !g.blockedUntil.After(now) {
			g.clears = 0
			return false
		}
		g.clears++
		if g.clears < edgeClearConfirmations {
			return false
		}
		g.clears = 0
		g.blockedUntil = time.Time{}
		g.probedAt = time.Time{}
		g.revision.Add(1)
		return true
	case edgeBlocked:
		return g.pauseLocked(now, pause)
	}
	if !g.blockedUntil.After(now) {
		return g.pauseLocked(now, provisionalEdgePause)
	}
	return false
}

// abandonProbe ends a probe that never started, leaving the hold-off it began.
func (g *cloudflareGuard) abandonProbe() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.probing = false
}

// confirming reports whether a clear probe during the pause in force awaits confirmation.
func (g *cloudflareGuard) confirming() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.clears > 0
}

// nextProbe is the wait before the next paused-edge probe: interval, or edgeConfirmDelay when
// a clear probe awaits confirmation and interval is longer.
func (g *cloudflareGuard) nextProbe(interval time.Duration) time.Duration {
	if g.confirming() {
		return min(interval, edgeConfirmDelay)
	}
	return interval
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
	status := refusal.StatusCode
	switch p.cloudflare.suspect(time.Now()) {
	case suspicionNone:
		return
	case suspicionPause:
		logger.Warn("Discord's edge keeps refusing requests from this IP; pausing outbound requests provisionally",
			"refusals", edgeSuspicionRefusals, "within", edgeProbeInterval.String(), "pause", provisionalEdgePause.String(), "route", route)
		if p.config.EnableMetrics {
			EdgeProvisionalPauses.Inc()
		}
		p.saveStateSoon()
		return
	}
	pause := cloudflarePause(refusal.Header)
	started := p.goBackground(func() {
		ctx, cancel := context.WithTimeout(p.ctx, p.config.UpstreamTimeout)
		defer cancel()
		verdict, probePause, probeErr := p.probeEdge(ctx)
		pause = max(pause, probePause)
		if p.cloudflare.settle(time.Now(), verdict, pause) {
			p.saveStateSoon()
		}
		switch verdict {
		case edgeBlocked:
			logger.Warn("Discord's edge blocked this IP; pausing outbound requests", "status", status, "pause", pause.String())
			if p.config.EnableMetrics {
				CloudflareBlocks.Inc()
			}
		case edgeClear:
			logger.Warn("Discord's edge refused a request, but this IP is not blocked; only that route backs off", "status", status, "route", route)
		default:
			logger.Warn("Could not check whether Discord's edge blocks this IP; pausing outbound requests provisionally",
				"status", status, "route", route, "pause", provisionalEdgePause.String(), "error", probeErr)
			if p.config.EnableMetrics {
				EdgeProvisionalPauses.Inc()
			}
		}
	})
	if !started {
		p.cloudflare.abandonProbe()
	}
}

// watchEdge probes Discord's edge every interval while a block pauses outbound traffic, so a block
// that lifts early, or outlasts the pause taken from its Retry-After, is noticed without any
// client request reaching the edge.
func (p *Proxy) watchEdge(interval time.Duration) {
	defer p.wg.Done()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			p.probePausedEdge()
			timer.Reset(p.cloudflare.nextProbe(interval))
		case <-p.ctx.Done():
			return
		}
	}
}

// probePausedEdge checks the pause in force, if any: a clear edge ends it, a refusal re-arms it
// from that refusal's Retry-After, and a probe that gets no answer leaves it as it is.
func (p *Proxy) probePausedEdge() {
	if !p.cloudflare.beginPausedProbe(time.Now()) {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.config.UpstreamTimeout)
	defer cancel()
	verdict, pause, _ := p.probeEdge(ctx)
	now := time.Now()
	if p.cloudflare.settle(now, verdict, pause) {
		p.saveStateSoon()
	}
	switch {
	case verdict == edgeClear && p.cloudflare.confirming():
		logger.Warn("Discord answered a probe through the edge; confirming before resuming outbound requests", "in", edgeConfirmDelay.String())
	case verdict == edgeClear:
		logger.Warn("Discord's edge no longer blocks this IP; resuming outbound requests")
	case verdict == edgeBlocked:
		logger.Warn("Discord's edge still blocks this IP; outbound requests stay paused", "pause", p.cloudflare.retryAfter(now).Round(time.Second).String())
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
	// Only Discord's own success proves the edge passes requests again: an error page, a 5xx or
	// an answer without Discord's Via may come from the edge itself.
	if response.StatusCode/100 == 2 && response.Header.Get("Via") != "" {
		return edgeClear, 0, nil
	}
	return edgeInconclusive, 0, fmt.Errorf("the edge answered %d without Discord's success", response.StatusCode)
}

const (
	// DefaultMaxRefusalsPerSecond is the refusal guard's ceiling unless MAX_REFUSALS_PER_SECOND sets
	// another. Discord's edge blocks an IP after 10,000 invalid responses in 10 minutes, about 16.7 a
	// second, and also sees how fast they come.
	DefaultMaxRefusalsPerSecond = 20
	refusalWindow               = time.Second
	// refusalCooldown is how long the guard answers every request itself once refusals reach the
	// ceiling, so clients back off before anything more is sent.
	refusalCooldown = 2 * time.Second
	// refusalProbation is how long after a trip, or after refusals reach half the ceiling, the guard
	// keeps attempts in flight below the ceiling, so the end of a cooldown cannot release a burst.
	refusalProbation = 30 * time.Second
)

// refusalGuard caps how fast this IP collects refusals: every 401, 403 and 429 from upstream,
// shared-scope 429s and edge refusals included, 404s not. The invalid-request budget bounds the
// 10-minute total but can be spent in a second, and leaves out shared 429s, which the edge still
// sees.
//
// Under pressure, once the last second's refusals reach half the ceiling and for refusalProbation
// after, an attempt is sent only while those refusals plus the attempts in flight stay below the
// ceiling, so a second holds at most the ceiling however the attempts end; others wait for a slot
// within their queue deadline. Refusals reaching the ceiling trip the guard, which then answers
// every request itself for refusalCooldown. Without pressure nothing waits, so only the attempts
// already in flight when pressure starts can pass the ceiling, once.
type refusalGuard struct {
	mu       sync.Mutex
	limit    int
	refusals []time.Time
	head     int
	inFlight int
	waiters  int
	wake     chan struct{}
	// blockedUntil ends a trip's cooldown; guardedUntil ends the pressure.
	blockedUntil time.Time
	guardedUntil time.Time
	// onTrip runs outside the lock each time the guard trips, with the refusals that tripped it.
	onTrip func(refusals int)
}

func newRefusalGuard(limit int) *refusalGuard {
	return &refusalGuard{limit: max(limit, 1), wake: make(chan struct{})}
}

func isRefusal(response *http.Response) bool {
	switch response.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		return true
	}
	return false
}

// admit takes a slot for an attempt, waiting for one under pressure. It returns how long the guard
// answers requests itself when it does, or ctx's error. Every admitted attempt ends with complete.
func (g *refusalGuard) admit(ctx context.Context) (time.Duration, error) {
	g.mu.Lock()
	for {
		now := time.Now()
		g.prune(now)
		if g.blockedUntil.After(now) {
			delay := g.blockedUntil.Sub(now)
			g.mu.Unlock()
			return delay, nil
		}
		count := len(g.refusals) - g.head
		if !g.pressedLocked(now, count) || count+g.inFlight < g.limit {
			g.inFlight++
			g.mu.Unlock()
			return 0, nil
		}
		if count >= g.limit {
			delay := g.tripLocked(now)
			g.mu.Unlock()
			g.tripped(count)
			return delay, nil
		}
		// Every slot is taken: one frees when an attempt ends or a refusal leaves the window.
		wait := time.Duration(-1)
		if count > 0 {
			wait = g.refusals[g.head].Add(refusalWindow).Sub(now)
		}
		wake := g.wake
		g.waiters++
		g.mu.Unlock()
		err := sleepUntilWoken(ctx, wake, wait)
		g.mu.Lock()
		g.waiters--
		if err != nil {
			g.mu.Unlock()
			return 0, err
		}
	}
}

// sleepUntilWoken waits for wake, for wait unless it is negative, or for ctx to end.
func sleepUntilWoken(ctx context.Context, wake <-chan struct{}, wait time.Duration) error {
	var expired <-chan time.Time
	if wait >= 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-wake:
	case <-expired:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// complete ends an admitted attempt, recording whether upstream refused it.
func (g *refusalGuard) complete(now time.Time, refused bool) {
	g.mu.Lock()
	g.inFlight--
	count, trip := 0, false
	if refused {
		g.refusals = append(g.refusals, now)
		g.prune(now)
		count = len(g.refusals) - g.head
		if until := now.Add(refusalProbation); count*2 >= g.limit && until.After(g.guardedUntil) {
			g.guardedUntil = until
		}
		if count >= g.limit && !g.blockedUntil.After(now) {
			g.tripLocked(now)
			trip = true
		}
	}
	if g.waiters > 0 {
		close(g.wake)
		g.wake = make(chan struct{})
	}
	g.mu.Unlock()
	if trip {
		g.tripped(count)
	}
}

func (g *refusalGuard) pressedLocked(now time.Time, count int) bool {
	return g.guardedUntil.After(now) || count*2 >= g.limit
}

func (g *refusalGuard) tripLocked(now time.Time) time.Duration {
	g.blockedUntil = now.Add(refusalCooldown)
	g.guardedUntil = g.blockedUntil.Add(refusalProbation)
	return refusalCooldown
}

func (g *refusalGuard) tripped(refusals int) {
	if g.onTrip != nil {
		g.onTrip(refusals)
	}
}

// trip starts a cooldown at once.
func (g *refusalGuard) trip(now time.Time) {
	g.mu.Lock()
	g.tripLocked(now)
	g.mu.Unlock()
}

// guarded reports whether attempts are held below the ceiling, or not sent at all, now.
func (g *refusalGuard) guarded(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	return g.blockedUntil.After(now) || g.pressedLocked(now, len(g.refusals)-g.head)
}

// blocked reports whether the guard answers every request itself now.
func (g *refusalGuard) blocked(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blockedUntil.After(now)
}

// count returns the refusals of the last second.
func (g *refusalGuard) count(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prune(now)
	return len(g.refusals) - g.head
}

// snapshot returns the end of the pressure that follows a trip, or the zero time.
func (g *refusalGuard) snapshot(now time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.guardedUntil.After(now) {
		return time.Time{}
	}
	return g.guardedUntil
}

// restore resumes saved pressure that has not ended, and reports whether it did.
func (g *refusalGuard) restore(until, now time.Time) bool {
	if !until.After(now) {
		return false
	}
	if latest := now.Add(refusalCooldown + refusalProbation); until.After(latest) {
		until = latest
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.guardedUntil = until
	return true
}

func (g *refusalGuard) prune(now time.Time) {
	oldest := now.Add(-refusalWindow)
	for g.head < len(g.refusals) && !g.refusals[g.head].After(oldest) {
		g.head++
	}
	if g.head > 1024 && g.head*2 > len(g.refusals) {
		g.refusals = append([]time.Time(nil), g.refusals[g.head:]...)
		g.head = 0
	}
}

// refusalGuardResponse answers a request while the refusal guard holds traffic back. The request
// never reached Discord, and the limit is the IP's, so it is a global 429 clients retry safely.
func refusalGuardResponse(request *http.Request, delay time.Duration) *http.Response {
	return rememberedRateLimitResponse(request, delay, true)
}
