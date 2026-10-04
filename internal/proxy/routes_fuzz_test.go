package proxy

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func FuzzBucketPathsNeverCarryWebhookTokens(f *testing.F) {
	for _, seed := range []string{
		"token",
		"aW50ZXJhY3Rpb246MTIzNDU2Nzg5MDEyMzQ1Njc4OnJhbmRvbQ",
		"aW50ZXJhY3Rpb246",
		strings.Repeat("t", 68),
		"@original",
		"%F0%9F%8E%89",
	} {
		f.Add(seed, http.MethodPatch)
	}
	f.Fuzz(func(t *testing.T, token, method string) {
		if token == "" || token == "!" || token == "Unknown" || strings.ContainsAny(token, "/?#") || isNumericInput(token) {
			t.Skip()
		}
		for _, major := range []string{majorWebhooks, majorInteractions} {
			bucket := GetOptimisticBucketPath("/api/v10/"+major+"/123456789012345678/"+token+"/messages/@original", method)
			segments := strings.Split(bucket, "/")
			if len(segments) < 4 || segments[3] == token {
				t.Fatalf("bucket %q keeps the %s token %q", bucket, major, token)
			}
			label := strings.Split(MetricsPathFromBucket(bucket), "/")
			if len(label) < 4 || label[3] != "!" && label[3] != "Unknown" {
				t.Fatalf("metric label %q keeps an identifier where the token was", strings.Join(label, "/"))
			}
			// A client that swaps the two sends the token where the ID belongs.
			swapped := strings.Split(GetOptimisticBucketPath("/api/v10/"+major+"/"+token+"/123456789012345678", method), "/")
			if len(swapped) < 3 || swapped[2] != "!" {
				t.Fatalf("bucket %q keeps the %s token %q sent as the ID", strings.Join(swapped, "/"), major, token)
			}
		}
	})
}

func FuzzPathClassificationIsStable(f *testing.F) {
	for _, seed := range []string{
		"/api/v10/channels/123456789012345678/messages",
		"/api//v10/webhooks/123456789012345678/token",
		"/v10/guilds/123456789012345678/members",
		"/channels/1/messages/2/reactions/%F0%9F%8E%89/@me",
		"//api///v9//users/@me",
		"/api/v10/channels/%2e%2e/messages",
		"/api/v6/interactions/1/aW50ZXJhY3Rpb246MTIz/callback",
	} {
		f.Add(seed, http.MethodGet)
	}
	f.Fuzz(func(t *testing.T, raw, method string) {
		if !strings.HasPrefix(raw, "/") {
			t.Skip()
		}
		target, err := url.Parse("http://sluice.invalid" + raw)
		if err != nil || target.RawQuery != "" || target.Fragment != "" || !isCleanDiscordPath(target) {
			t.Skip()
		}
		collapseSlashes(target)
		ensureAPIPrefix(target)
		normalised := target.String()
		collapseSlashes(target)
		ensureAPIPrefix(target)
		if target.String() != normalised {
			t.Fatalf("normalising %q twice gave %q, then %q", raw, normalised, target.String())
		}
		bucket := GetOptimisticBucketPath(target.Path, method)
		for _, segment := range strings.Split(MetricsPathFromBucket(bucket), "/") {
			if isNumericInput(segment) {
				t.Fatalf("metric label for %q keeps the identifier %q", raw, segment)
			}
		}
		_ = majorParameter(target.Path)
		_ = isInteractionEndpoint(target.Path)
		_ = outdatedAPIVersion(target.Path)
		_ = webhookCredentialKey(target.Path, false)
	})
}
