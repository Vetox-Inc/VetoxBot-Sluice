package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStateFileCarriesProtectionAcrossRestarts(t *testing.T) {
	config := testConfig(nil)
	config.CloudflareBanDetection = true
	config.StateFile = filepath.Join(t.TempDir(), "nested", "state.json")
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for range 3 {
		first.invalidRequests.reserve(now)
		first.invalidRequests.complete(now, true)
	}
	first.webhooks.record(webhookFailure{key: "deleted", status: http.StatusNotFound, code: unknownWebhookCode, expires: now.Add(webhookFailureTTL)})
	first.cloudflare.settle(now, edgeBlocked, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}

	second := newTestProxy(t, config)
	restored := time.Now()
	if got := second.invalidRequests.count(restored); got != 3 {
		t.Fatalf("restored %d invalid responses, want 3", got)
	}
	if _, blocked := second.webhooks.lookup("deleted", restored); !blocked {
		t.Fatal("the deleted webhook was forgotten across the restart")
	}
	if pause := second.cloudflare.retryAfter(restored); pause <= 0 || pause > 30*time.Second {
		t.Fatalf("restored Cloudflare pause = %v, want the rest of 30s", pause)
	}
}

func TestSavedStateOutsideItsWindowIsDropped(t *testing.T) {
	now := time.Now()
	saved, err := json.Marshal(savedState{
		Version:          stateVersion,
		InvalidRequests:  [][2]int64{{now.Add(-11 * time.Minute).Unix(), 5}, {now.Add(-time.Minute).Unix(), 2}},
		EdgeBlockedUntil: now.Add(-time.Second).UnixMilli(),
		Webhooks: []savedWebhook{
			{Key: "expired", Status: http.StatusNotFound, Code: unknownWebhookCode, Expires: now.Add(-time.Second).UnixMilli()},
			{Key: "not-permanent", Status: http.StatusNotFound, Code: 10008, Expires: now.Add(time.Minute).UnixMilli()},
			{Key: "live", Status: http.StatusUnauthorized, Code: invalidWebhookTokenCode, Expires: now.Add(time.Minute).UnixMilli()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(nil)
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(config.StateFile, saved, 0o600); err != nil {
		t.Fatal(err)
	}
	proxy := newTestProxy(t, config)
	if got := proxy.invalidRequests.count(time.Now()); got != 2 {
		t.Fatalf("restored %d invalid responses, want only the 2 inside the window", got)
	}
	if proxy.cloudflare.retryAfter(time.Now()) > 0 {
		t.Fatal("a Cloudflare block that had ended was restored")
	}
	for key, want := range map[string]bool{"expired": false, "not-permanent": false, "live": true} {
		if _, blocked := proxy.webhooks.lookup(key, time.Now()); blocked != want {
			t.Errorf("webhook %q restored = %v, want %v", key, blocked, want)
		}
	}
}

func TestUnreadableStateFileIsMovedAsideNotOverwritten(t *testing.T) {
	config := testConfig(nil)
	config.StateFile = filepath.Join(t.TempDir(), "state.json")
	const unrelated = "important unrelated content"
	if err := os.WriteFile(config.StateFile, []byte(unrelated), 0o600); err != nil {
		t.Fatal(err)
	}
	proxy, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if got := proxy.invalidRequests.count(time.Now()); got != 0 {
		t.Fatalf("an unreadable state file restored %d invalid responses", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxy.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if kept, err := os.ReadFile(config.StateFile + ".invalid"); err != nil || string(kept) != unrelated {
		t.Fatalf("the unreadable file was not kept aside: %q, %v", kept, err)
	}
}

func TestRestoredCloudflareBlockNeedsDetection(t *testing.T) {
	saved, err := json.Marshal(savedState{Version: stateVersion, EdgeBlockedUntil: time.Now().Add(time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	for _, detection := range []bool{true, false} {
		config := testConfig(nil)
		config.CloudflareBanDetection = detection
		config.StateFile = filepath.Join(t.TempDir(), "state.json")
		if err := os.WriteFile(config.StateFile, saved, 0o600); err != nil {
			t.Fatal(err)
		}
		proxy := newTestProxy(t, config)
		if paused := proxy.cloudflare.retryAfter(time.Now()) > 0; paused != detection {
			t.Fatalf("CLOUDFLARE_BAN_DETECTION=%v: restored block pauses traffic = %v", detection, paused)
		}
	}
}
