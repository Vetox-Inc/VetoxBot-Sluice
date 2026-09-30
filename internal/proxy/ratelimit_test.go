package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGlobalPacerDefaultSpacingAndCancellation(t *testing.T) {
	defaultPacer := newPacer(0, 2)
	wantInterval := (time.Second + discordGlobalLimit - 1) / discordGlobalLimit
	wantInterval += wantInterval / globalPacingSlack
	if defaultPacer.interval != wantInterval {
		t.Fatalf("default interval = %s, want %s", defaultPacer.interval, wantInterval)
	}
	if err := defaultPacer.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := defaultPacer.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < wantInterval-2*time.Millisecond {
		t.Fatalf("global requests were not paced: elapsed=%s interval=%s", elapsed, wantInterval)
	}

	slowPacer := newPacer(1, 2)
	if err := slowPacer.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := slowPacer.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled pacer wait = %v, want context.DeadlineExceeded", err)
	}
}

func testCooldownExtensionWakesWaiter(t *testing.T, wait func(context.Context, time.Duration) (time.Duration, error), extend func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		delay time.Duration
		err   error
	}
	done := make(chan result, 1)
	go func() {
		delay, err := wait(ctx, 100*time.Millisecond)
		done <- result{delay: delay, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("wait returned before cooldown extension: delay=%s err=%v", got.delay, got.err)
	case <-time.After(25 * time.Millisecond):
	}
	extend()
	select {
	case got := <-done:
		if got.err != nil || got.delay <= 5*time.Second {
			t.Fatalf("extended wait = (%s, %v), want delay beyond context remainder", got.delay, got.err)
		}
	case <-time.After(250 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("cooldown extension did not wake waiter")
	}
}

func TestCooldownExtensionWakesDeadlineAwareWaiters(t *testing.T) {
	t.Run("bucket", func(t *testing.T) {
		bucket := newBucketState(1)
		bucket.blockUntil(time.Now().Add(time.Second))
		testCooldownExtensionWakesWaiter(t, bucket.wait, func() {
			bucket.blockUntil(time.Now().Add(10 * time.Second))
		})
	})
	t.Run("global", func(t *testing.T) {
		pacer := newPacer(discordGlobalLimit, 1)
		pacer.blockFor(time.Second)
		testCooldownExtensionWakesWaiter(t, pacer.waitFor, func() {
			pacer.blockFor(10 * time.Second)
		})
	})
}

func TestInvalidRequestGuardReservesAndExpiresCapacity(t *testing.T) {
	now := time.Now()
	guard := newInvalidRequestGuard(2, time.Minute)
	if !guard.reserve(now) || !guard.reserve(now) || guard.reserve(now) {
		t.Fatal("guard did not bound concurrent potential invalid responses")
	}
	guard.complete(now, false)
	if !guard.reserve(now) {
		t.Fatal("valid response did not return its reservation")
	}
	guard.complete(now, true)
	guard.complete(now, true)
	if guard.reserve(now) {
		t.Fatal("recorded invalid responses did not exhaust the budget")
	}
	if !guard.reserve(now.Add(time.Minute + time.Nanosecond)) {
		t.Fatal("expired invalid responses did not release capacity")
	}

	shared := testResponse(httptest.NewRequest(http.MethodGet, "/", nil), http.StatusTooManyRequests, http.Header{"X-Ratelimit-Scope": {"shared"}}, nil)
	if invalidDiscordResponse(shared) {
		t.Fatal("shared 429 counted against Discord's invalid-request budget")
	}
}

func TestGlobalLimitDefaultsAndOverrides(t *testing.T) {
	botID := "123456789012345678"
	identifiedBot := identify("Bot " + base64.RawStdEncoding.EncodeToString([]byte(botID)) + ".signature")
	fingerprintedBot := identify("Bot opaque-token")
	overrides := botID + ":73," + fingerprintedBot.fingerprint() + ":61"
	config := testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	}))
	config.GlobalOverrides = overrides
	proxy := newTestProxy(t, config)

	if got := proxy.globalLimit(identifiedBot); got != 73 {
		t.Fatalf("bot-ID override = %d, want 73", got)
	}
	if got := proxy.globalLimit(fingerprintedBot); got != 61 {
		t.Fatalf("fingerprint override = %d, want 61", got)
	}
	if got := proxy.globalLimit(identify("Bot another-token")); got != discordGlobalLimit {
		t.Fatalf("default bot global limit = %d, want %d", got, discordGlobalLimit)
	}
	if got := proxy.globalLimit(identify("Bearer bearer-token")); got != discordGlobalLimit {
		t.Fatalf("default bearer global limit = %d, want %d", got, discordGlobalLimit)
	}
}

func TestHugeRateLimitDelaysAreClampedBeforeDurationConversion(t *testing.T) {
	tests := []struct {
		name   string
		status int
		header http.Header
	}{
		{
			name:   "retry after",
			status: http.StatusTooManyRequests,
			header: testHeaders(map[string]string{"Retry-After": "1e300"}),
		},
		{
			name:   "reset after",
			status: http.StatusOK,
			header: testHeaders(map[string]string{
				"X-RateLimit-Remaining":   "0",
				"X-RateLimit-Reset-After": "1e300",
			}),
		},
		{
			name:   "absolute reset",
			status: http.StatusOK,
			header: testHeaders(map[string]string{
				"X-RateLimit-Remaining": "0",
				"X-RateLimit-Reset":     "1e300",
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := parseRateLimitHeaders(test.header, test.status, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			want := maximumRateLimitDelay + rateLimitHeaderSlack
			if info.resetAfter != want {
				t.Fatalf("reset delay = %v, want safe clamp %v", info.resetAfter, want)
			}
		})
	}
}

func TestGlobalOverrideRejectsUppercaseFingerprint(t *testing.T) {
	uppercaseFingerprint := strings.Repeat("a", 63) + "A"
	_, err := parseGlobalOverrides("sha256:" + uppercaseFingerprint + ":51")
	if err == nil || !strings.Contains(err.Error(), "lowercase") {
		t.Fatalf("uppercase fingerprint error = %v, want lowercase validation error", err)
	}
}
