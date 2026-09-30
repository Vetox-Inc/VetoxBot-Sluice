package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanceledMiddleWaiterDoesNotBlockFollower(t *testing.T) {
	bucket := newBucketState(4)
	if err := bucket.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	holderReleased := false
	defer func() {
		if !holderReleased {
			bucket.release()
		}
	}()

	middleContext, cancelMiddle := context.WithCancel(context.Background())
	defer cancelMiddle()
	middleResult := make(chan error, 1)
	go func() { middleResult <- bucket.acquire(middleContext) }()
	waitForGateWaiters(t, &bucket.gate, 1)

	followerResult := make(chan error, 1)
	go func() {
		err := bucket.acquire(context.Background())
		if err == nil {
			bucket.release()
		}
		followerResult <- err
	}()
	waitForGateWaiters(t, &bucket.gate, 2)

	cancelMiddle()
	if err := <-middleResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("middle acquire error = %v, want context.Canceled", err)
	}
	waitForGateWaiters(t, &bucket.gate, 1)

	bucket.release()
	holderReleased = true
	select {
	case err := <-followerResult:
		if err != nil {
			t.Fatalf("follower acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower remained blocked after canceled waiter was removed")
	}

	bucket.gate.mu.Lock()
	held, waiters := bucket.gate.held, bucket.gate.waiters.Len()
	bucket.gate.mu.Unlock()
	if active := bucket.active.Load(); active != 0 || held || waiters != 0 {
		t.Fatalf("bucket leaked state: active=%d held=%v waiters=%d", active, held, waiters)
	}
}

func TestLearnedDiscordBucketAliasesRoutesWithinMajorParameter(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, http.Header{
			"X-Ratelimit-Bucket":    {"discord-bucket"},
			"X-Ratelimit-Remaining": {"1"},
		}, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}

	paths := []string{
		"/api/v10/channels/123/messages",
		"/api/v10/channels/123/pins",
		"/api/v10/channels/456/messages",
	}
	for _, path := range paths {
		response, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, path, nil, true))
		if err != nil {
			t.Fatalf("request %s: %v", path, err)
		}
		_ = response.Body.Close()
	}

	first := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(paths[0], http.MethodGet), majorParameter(paths[0])))
	second := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(paths[1], http.MethodGet), majorParameter(paths[1])))
	differentMajor := mustBucket(t, proxy.noAuth, routeHash(http.MethodGet, GetOptimisticBucketPath(paths[2], http.MethodGet), majorParameter(paths[2])))
	if first != second {
		t.Fatal("routes with the same Discord bucket and major parameter were not aliased")
	}
	if first == differentMajor {
		t.Fatal("Discord bucket alias crossed major-parameter boundaries")
	}
}

func TestQueuedRequestMigratesToLearnedBucket(t *testing.T) {
	started := make(chan struct{}, 1)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		started <- struct{}{}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	firstPath := "/api/v10/channels/123/messages"
	queuedPath := "/api/v10/channels/123/pins"
	majorKey := majorParameter(firstPath)
	firstHash := routeHash(http.MethodGet, GetOptimisticBucketPath(firstPath, http.MethodGet), majorKey)
	queuedHash := routeHash(http.MethodGet, GetOptimisticBucketPath(queuedPath, http.MethodGet), majorKey)

	canonical := mustBucket(t, proxy.noAuth, firstHash)
	proxy.noAuth.learnBucket(firstHash, "shared-discord-bucket", majorKey, canonical)
	oldBucket := mustBucket(t, proxy.noAuth, queuedHash)
	if oldBucket == canonical {
		t.Fatal("test setup did not create distinct optimistic buckets")
	}
	if err := canonical.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	canonicalHeld := true
	defer func() {
		if canonicalHeld {
			canonical.release()
		}
	}()
	if err := oldBucket.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldHeld := true
	defer func() {
		if oldHeld {
			oldBucket.release()
		}
	}()

	result := make(chan error, 1)
	go func() {
		response, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), proxy.noAuth, http.MethodGet, queuedPath, nil, true))
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()
	waitForGateWaiters(t, &oldBucket.gate, 1)
	if target := proxy.noAuth.learnBucket(queuedHash, "shared-discord-bucket", majorKey, oldBucket); target != canonical {
		t.Fatal("queued route did not learn the existing canonical bucket")
	}
	oldBucket.release()
	oldHeld = false
	waitForGateWaiters(t, &canonical.gate, 1)
	select {
	case <-started:
		t.Fatal("queued request reached Discord before acquiring the learned bucket")
	default:
	}

	canonical.release()
	canonicalHeld = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request did not proceed after the learned bucket was released")
	}
	if active := oldBucket.active.Load() + canonical.active.Load(); active != 0 {
		t.Fatalf("bucket handoff leaked %d active requests", active)
	}
}

func TestWebhookTokenIsPartOfOptimisticRouteIdentity(t *testing.T) {
	const webhookID = "203039963636301824"
	firstPath := "/api/v10/webhooks/" + webhookID + "/" + strings.Repeat("a", 64)
	secondPath := "/api/v10/webhooks/" + webhookID + "/" + strings.Repeat("b", 64)
	bucketPath := GetOptimisticBucketPath(firstPath, http.MethodPost)
	if bucketPath != GetOptimisticBucketPath(secondPath, http.MethodPost) {
		t.Fatal("test setup expected the same redacted optimistic path")
	}
	first := routeHash(http.MethodPost, bucketPath, majorParameter(firstPath))
	second := routeHash(http.MethodPost, bucketPath, majorParameter(secondPath))
	if first == second {
		t.Fatal("different webhook tokens shared optimistic scheduler identity")
	}
}

func TestIdentifyCanonicalizesAuthorization(t *testing.T) {
	botID := "123456789012345678"
	token := base64.RawStdEncoding.EncodeToString([]byte(botID)) + ".signature"
	bare := identify(token)
	canonical := identify("Bot " + token)
	folded := identify("  bot   " + token + "  ")
	if bare.key != canonical.key || canonical.key != folded.key || bare.kind != authBot {
		t.Fatal("bare and case-insensitive Bot credentials did not canonicalize to one identity")
	}
	if got := identify("Bot opaque-token").fingerprint(); got != "sha256:84d3f23da9b5f51b3269566eff05d3fb23607eeef89567f9cd280b90ca0dbc5c" {
		t.Fatalf("credential fingerprint = %q", got)
	}
	if canonical.botID != botID || canonical.label != "Bot" {
		t.Fatalf("bot identity = id %q label %q", canonical.botID, canonical.label)
	}

	bearer := identify("Bearer bearer-token")
	foldedBearer := identify(" bearer   bearer-token ")
	if bearer.key != foldedBearer.key || bearer.kind != authBearer || bearer.label != "Bearer" {
		t.Fatal("Bearer credentials did not canonicalize")
	}
	if basic, none := identify("Basic credentials"), identify("   "); basic.kind != authNone || basic != none {
		t.Fatalf("Basic identity = %#v, want no-auth identity %#v", basic, none)
	}
	if unknown, none := identify("Digest credentials"), identify(""); unknown.kind != authNone || unknown != none {
		t.Fatalf("unknown authorization identity = %#v, want no-auth identity %#v", unknown, none)
	}
}

func TestInteractionUnauthorizedDoesNotLockCredential(t *testing.T) {
	var calls atomic.Int64
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return testResponse(request, http.StatusUnauthorized, nil, nil), nil
		}
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	state := newClientState(identify("Bot interaction-token"), discordGlobalLimit, 8, newResourceBudget(256))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	path := "/api/v10/interactions/123/token/callback"

	first, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), state, http.MethodPost, path, nil, true))
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	second, err := scheduled.RoundTrip(scheduledRequest(t, context.Background(), state, http.MethodPost, path, nil, true))
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusOK || calls.Load() != 2 || state.validity.Load() == clientInvalid {
		t.Fatalf("interaction credential was locked: status=%d calls=%d validity=%d", second.StatusCode, calls.Load(), state.validity.Load())
	}
}

func TestInteractionLeavesValidationForOrdinaryRequest(t *testing.T) {
	for _, interactionStatus := range []int{http.StatusOK, http.StatusUnauthorized} {
		t.Run(http.StatusText(interactionStatus), func(t *testing.T) {
			var calls atomic.Int64
			ordinaryStarted := make(chan struct{})
			releaseOrdinary := make(chan struct{})
			ordinaryReleased := false
			defer func() {
				if !ordinaryReleased {
					close(releaseOrdinary)
				}
			}()
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch calls.Add(1) {
				case 1:
					return testResponse(request, interactionStatus, nil, nil), nil
				case 2:
					close(ordinaryStarted)
					<-releaseOrdinary
					return testResponse(request, http.StatusUnauthorized, nil, nil), nil
				default:
					return nil, errors.New("unexpected request reached Discord")
				}
			})
			proxy := newTestProxy(t, testConfig(transport))
			state := newClientState(identify("Bot interaction-validation-token"), 1_000_000, 8, newResourceBudget(256))
			scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}

			interaction := scheduledRequest(
				t,
				context.Background(),
				state,
				http.MethodPost,
				"/api/v10/interactions/123/token/callback",
				nil,
				true,
			)
			response, err := scheduled.RoundTrip(interaction)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if got := state.validity.Load(); got != clientUnknown {
				t.Fatalf("validity after interaction = %d, want unknown", got)
			}

			type result struct {
				response *http.Response
				err      error
			}
			firstRequest := scheduledRequest(t, context.Background(), state, http.MethodGet, "/api/v10/channels/1/messages", nil, false)
			firstResult := make(chan result, 1)
			go func() {
				response, err := scheduled.RoundTrip(firstRequest)
				firstResult <- result{response: response, err: err}
			}()
			select {
			case <-ordinaryStarted:
			case <-time.After(time.Second):
				t.Fatal("ordinary validation request was not dispatched")
			}

			secondRequest := scheduledRequest(t, context.Background(), state, http.MethodGet, "/api/v10/guilds/2/roles", nil, false)
			secondResult := make(chan result, 1)
			go func() {
				response, err := scheduled.RoundTrip(secondRequest)
				secondResult <- result{response: response, err: err}
			}()
			waitForGateWaiters(t, &state.validation, 1)
			if got := calls.Load(); got != 2 {
				t.Fatalf("Discord calls while validation was in flight = %d, want 2", got)
			}

			close(releaseOrdinary)
			ordinaryReleased = true
			for index, results := range []<-chan result{firstResult, secondResult} {
				select {
				case got := <-results:
					if got.err != nil {
						t.Fatalf("ordinary request %d: %v", index+1, got.err)
					}
					if got.response.StatusCode != http.StatusUnauthorized {
						t.Fatalf("ordinary request %d status = %d, want 401", index+1, got.response.StatusCode)
					}
					_ = got.response.Body.Close()
				case <-time.After(time.Second):
					t.Fatalf("ordinary request %d remained blocked", index+1)
				}
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("Discord calls after credential lock = %d, want 2", got)
			}
			if got := state.validity.Load(); got != clientInvalid {
				t.Fatalf("ordinary 401 validity = %d, want invalid", got)
			}
		})
	}
}

func TestUnknownCredentialDoesNotSerializeInteractions(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-release
		return testResponse(request, http.StatusOK, nil, nil), nil
	})
	proxy := newTestProxy(t, testConfig(transport))
	state := newClientState(identify("Bot interaction-concurrency-token"), discordGlobalLimit, 8, newResourceBudget(256))
	scheduled := &scheduledTransport{base: proxy.transport, proxy: proxy}
	requests := []*http.Request{
		scheduledRequest(t, context.Background(), state, http.MethodPost, "/api/v10/interactions/123456789012345678/token/callback", nil, true),
		scheduledRequest(t, context.Background(), state, http.MethodPost, "/api/v10/interactions/223456789012345678/token/callback", nil, true),
	}

	type result struct {
		response *http.Response
		err      error
	}
	results := make(chan result, len(requests))
	for _, request := range requests {
		go func() {
			response, err := scheduled.RoundTrip(request)
			results <- result{response: response, err: err}
		}()
	}
	for range requests {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("interaction was serialized behind credential validation")
		}
	}
	close(release)
	released = true
	for range requests {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatal(got.err)
			}
			_ = got.response.Body.Close()
		case <-time.After(time.Second):
			t.Fatal("interaction remained blocked after upstream release")
		}
	}
	if got := state.validity.Load(); got != clientUnknown {
		t.Fatalf("validity after concurrent interactions = %d, want unknown", got)
	}
}

func TestBlockedBucketAndPacerCannotBeEvicted(t *testing.T) {
	now := time.Now()
	old := now.Add(-clientIdleTimeout - time.Hour).UnixNano()

	t.Run("bucket", func(t *testing.T) {
		proxy := newTestProxy(t, testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return testResponse(request, http.StatusOK, nil, nil), nil
		})))
		identified := identify("Bearer blocked-bucket")
		state := newClientState(identified, discordGlobalLimit, 8, proxy.bucketSlots)
		state.lastUsed.Store(old)
		bucket := mustBucket(t, state, 11)
		bucket.lastUsed.Store(old)
		bucket.blockUntil(now.Add(time.Hour))
		proxy.clientsMu.Lock()
		proxy.bearers[identified.key] = state
		proxy.clientsMu.Unlock()

		if bucket.idle(now) {
			t.Fatal("blocked bucket reported idle")
		}
		proxy.sweepClients(now)
		proxy.clientsMu.Lock()
		got := proxy.bearers[identified.key]
		proxy.clientsMu.Unlock()
		if got != state {
			t.Fatal("client owning a blocked bucket was evicted")
		}
		state.mu.RLock()
		gotBucket := state.buckets[11]
		state.mu.RUnlock()
		if gotBucket != bucket {
			t.Fatal("blocked bucket was swept")
		}
	})

	t.Run("pacer", func(t *testing.T) {
		config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return testResponse(request, http.StatusOK, nil, nil), nil
		}))
		config.MaxBearerClients = 1
		proxy := newTestProxy(t, config)
		identified := identify("Bearer blocked-pacer")
		state := newClientState(identified, discordGlobalLimit, 8, proxy.bucketSlots)
		state.lastUsed.Store(old)
		state.global.blockFor(time.Hour)
		proxy.clientsMu.Lock()
		proxy.bearers[identified.key] = state
		proxy.clientsMu.Unlock()

		if state.idle(now) {
			t.Fatal("client with blocked global pacer reported idle")
		}
		proxy.sweepClients(now)
		proxy.clientsMu.Lock()
		got := proxy.bearers[identified.key]
		proxy.clientsMu.Unlock()
		if got != state {
			t.Fatal("client with blocked global pacer was evicted")
		}
		if admitted, err := proxy.client(identify("Bearer replacement")); !errors.Is(err, errTooManyClients) {
			if err == nil {
				admitted.end()
			}
			t.Fatalf("replacement admission error = %v, want %v", err, errTooManyClients)
		}
	})
}

func TestAcquireBucketRechecksCrossedAliasesWithoutDeadlock(t *testing.T) {
	state := newClientState(identity{kind: authNone, label: "NoAuth"}, discordGlobalLimit, 8, newResourceBudget(256))
	routeA, routeB := HashCRC64("route-a"), HashCRC64("route-b")
	major := "channels:123"
	canonicalA := HashCRC64("discord-a\x00" + major)
	canonicalB := HashCRC64("discord-b\x00" + major)
	if routeA == routeB || canonicalA == canonicalB || routeA == canonicalA || routeA == canonicalB || routeB == canonicalA || routeB == canonicalB {
		t.Fatal("test hashes unexpectedly collided")
	}

	oldA, oldB := newBucketState(8), newBucketState(8)
	state.mu.Lock()
	state.buckets[routeA] = oldA
	state.buckets[routeB] = oldB
	state.buckets[canonicalA] = oldA
	state.buckets[canonicalB] = oldB
	state.mu.Unlock()
	if err := oldA.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	heldA := true
	defer func() {
		if heldA {
			oldA.release()
		}
	}()
	if err := oldB.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	heldB := true
	defer func() {
		if heldB {
			oldB.release()
		}
	}()

	type result struct {
		bucket *bucketState
		err    error
	}
	resultsA, resultsB := make(chan result, 1), make(chan result, 1)
	go func() {
		bucket, err := state.acquireBucket(context.Background(), routeA)
		if err == nil {
			bucket.release()
		}
		resultsA <- result{bucket: bucket, err: err}
	}()
	go func() {
		bucket, err := state.acquireBucket(context.Background(), routeB)
		if err == nil {
			bucket.release()
		}
		resultsB <- result{bucket: bucket, err: err}
	}()
	waitForGateWaiters(t, &oldA.gate, 1)
	waitForGateWaiters(t, &oldB.gate, 1)

	state.learnBucket(routeA, "discord-b", major, oldA)
	state.learnBucket(routeB, "discord-a", major, oldB)
	oldA.release()
	heldA = false
	oldB.release()
	heldB = false

	for name, check := range map[string]struct {
		results <-chan result
		want    *bucketState
	}{
		"route A": {results: resultsA, want: oldB},
		"route B": {results: resultsB, want: oldA},
	} {
		select {
		case got := <-check.results:
			if got.err != nil {
				t.Fatalf("%s acquire: %v", name, got.err)
			}
			if got.bucket != check.want {
				t.Fatalf("%s acquired stale bucket %p, want latest alias %p", name, got.bucket, check.want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s deadlocked after crossed remap", name)
		}
	}
	if oldA.active.Load() != 0 || oldB.active.Load() != 0 {
		t.Fatalf("crossed remap leaked activity: A=%d B=%d", oldA.active.Load(), oldB.active.Load())
	}
}

func TestLearnedAliasIsBlockedBeforePublication(t *testing.T) {
	state := newClientState(identity{kind: authNone, label: "NoAuth"}, discordGlobalLimit, 8, newResourceBudget(16))
	const (
		route         = uint64(1)
		discordBucket = "canonical-bucket"
		major         = "channels:123"
	)
	current := mustBucket(t, state, route)
	canonical := HashCRC64(discordBucket + "\x00" + major)
	target := mustBucket(t, state, canonical)

	// Stall the current-to-target merge. A safe implementation holds state.mu
	// here, so no resolver can observe the alias until target carries the block.
	target.mu.Lock()
	targetLocked := true
	defer func() {
		if targetLocked {
			target.mu.Unlock()
		}
	}()
	response := testResponse(httptest.NewRequest(http.MethodGet, "/", nil), http.StatusOK, testHeaders(map[string]string{
		"X-RateLimit-Bucket":      discordBucket,
		"X-RateLimit-Remaining":   "0",
		"X-RateLimit-Reset-After": "1",
	}), nil)
	done := make(chan *bucketState, 1)
	go func() {
		done <- state.observeResponse(route, major, "/channels/:channel_id/messages", false, current, response, false)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		if !state.mu.TryRLock() {
			break
		}
		state.mu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("alias merge did not retain the state lock")
		}
		time.Sleep(100 * time.Microsecond)
	}
	current.mu.Lock()
	currentBlocked := current.readyAt.After(time.Now())
	current.mu.Unlock()
	if !currentBlocked {
		t.Fatal("response block was not applied before alias publication")
	}

	target.mu.Unlock()
	targetLocked = false
	select {
	case got := <-done:
		if got != target {
			t.Fatalf("learned target = %p, want %p", got, target)
		}
	case <-time.After(time.Second):
		t.Fatal("alias merge remained blocked")
	}
	resolved := mustBucket(t, state, route)
	if resolved != target || !resolved.blocked(time.Now()) {
		t.Fatal("published alias did not resolve to the blocked canonical bucket")
	}
}
