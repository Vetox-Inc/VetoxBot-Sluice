package proxy

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

func snowflakeFromTime(value time.Time) string {
	milliseconds := value.UnixMilli() - discordEpochMilliseconds
	return strconv.FormatUint(uint64(milliseconds)<<22, 10)
}

func TestGetOptimisticBucketPath(t *testing.T) {
	oldMessageID := snowflakeFromTime(time.Now().Add(-15 * 24 * time.Hour))
	recentMessageID := snowflakeFromTime(time.Now().Add(time.Minute))

	tests := []struct {
		path   string
		method string
		want   string
	}{
		{path: "/api/v9/guilds/103039963636301824", method: "GET", want: "/guilds/103039963636301824"},
		{path: "/api/v8/channels/203039963636301824", method: "GET", want: "/channels/203039963636301824"},
		{path: "/api/v8/channels/872712139712913438", method: "PATCH", want: "/channels/872712139712913438"},
		{path: "/api/v7/channels/203039963636301824/pins", method: "GET", want: "/channels/203039963636301824/pins"},
		{path: "/api/v6/channels/872712139712913438/messages/872712150509047809/reactions/%F0%9F%98%8B", method: "GET", want: "/channels/872712139712913438/messages/!/reactions/!/!"},
		{path: "/api/v10/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195", method: "GET", want: "/channels/872712139712913438/messages/!/reactions/!/!"},
		{path: "/api/v9/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195", method: "PUT", want: "/channels/872712139712913438/messages/!/reactions/!modify"},
		{path: "/api/v9/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195", method: "DELETE", want: "/channels/872712139712913438/messages/!/reactions/!modify"},
		{path: "/api/v9/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195/@me", method: "DELETE", want: "/channels/872712139712913438/messages/!/reactions/!modify"},
		{path: "/api/v9/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195/203039963636301824", method: "DELETE", want: "/channels/872712139712913438/messages/!/reactions/!modify"},
		{path: "/api/v9/channels/872712139712913438/messages/" + oldMessageID, method: "DELETE", want: "/channels/872712139712913438/messages/!14dmsg"},
		{path: "/api/v9/channels/872712139712913438/messages/" + recentMessageID, method: "DELETE", want: "/channels/872712139712913438/messages/!10smsg"},
		{path: "/api/v9/webhooks/203039963636301824", method: "GET", want: "/webhooks/203039963636301824"},
		{path: "/api/v9/webhooks/203039963636301824/VSOzAqY1OZFF5WJVtbIzFtmjGupk-84Hn0A_ZzToF_CHsPIeCk0Q9Uok_mjxR0dNtApI", method: "POST", want: "/webhooks/203039963636301824/!"},
		{path: "/api/v10/webhooks/203039963636301824/short-secret", method: "POST", want: "/webhooks/203039963636301824/!"},
		{path: "/api/v10/interactions/203039963636301824/short-secret/followup", method: "POST", want: "/interactions/203039963636301824/!/followup"},
		{path: "/api/v10/users/787061750905962567", method: "GET", want: "/users/!"},
		{path: "/api/v10/users/1145768749882937486", method: "GET", want: "/users/!"},
		{path: "/api/v10/users/@me", method: "GET", want: "/users/@me"},
		{path: "/api/v9/invites/dyno", method: "GET", want: "/invites/!"},
		{path: "/api/v9/interactions/203039963636301824/aW50ZXJhY3Rpb246ODg3NTU5MDA01AY4NTUxNDU0OnZwS3QycDhvREk2aVF3U1BqN2prcXBkRmNqNlp4VEhGRjZvSVlXSGh4WG4yb3l6Z3B6NTBPNVc3OHphV05OULLMOHBMa2RTZmVKd3lzVDA2b2h3OTUxaFJ4QlN0dkxXallPcmhnSHNJb0tSV0M5ZzY1NkN4VGRvemFOSHY4b05c/callback", method: "GET", want: "/interactions/203039963636301824/!/callback"},
		{path: "/api/v10/webhooks/203039963636301824/aW50ZXJhY3Rpb246MTEwMzA0OTQyMDkzMDU2ODMyMjpOZUllWHdNU2J4RXBFMHVYRjBpU0pHMDdEb3BhM3ZlYklBODlMUmtlUXlRbzlpZzYyTnpLU0dqdWlyVlBvZnBSUlJHbUJHYlJ0N29MbE9KQUJVTFk4bTR4UzFtZEpEeXJyY0hBUERmTEhKVE9wRkNzU1FFWUkwTnlpWFY2WHdrRg/messages/@original", method: "POST", want: "/webhooks/203039963636301824/1103049420930568322/messages/@original"},
		{path: "/api/v9/invalid/203039963636301824", method: "GET", want: "/invalid/203039963636301824"},
		{path: "/api/v9/invalid/203039963636301824/route/203039963636301824", method: "GET", want: "/invalid/203039963636301824/route/!"},
		{path: "/api/v9/guilds/203039963636301824/channels", method: "GET", want: "/guilds/203039963636301824/channels"},
		{path: "/api/v9/guilds/templates/203039963636301824", method: "GET", want: "/guilds/templates/!"},
		{path: "/api/webhooks/203039963636301824/VSOzAqY1OZFF5WJVtbIzFtmjGupk-84Hn0A_ZzToF_CHsPIeCk0Q9Uok_mjxR0dNtApI", method: "POST", want: "/webhooks/203039963636301824/!"},
		{path: "/api/interactions/203039963636301824/aW50ZXJhY3Rpb246ODg3NTU5MDA01AY4NTUxNDU0OnZwS3QycDhvREk2aVF3U1BqN2prcXBkRmNqNlp4VEhGRjZvSVlXSGh4WG4yb3l6Z3B6NTBPNVc3OHphV05OULLMOHBMa2RTZmVKd3lzVDA2b2h3OTUxaFJ4QlN0dkxXallPcmhnSHNJb0tSV0M5ZzY1NkN4VGRvemFOSHY4b05c/callback", method: "GET", want: "/interactions/203039963636301824/!/callback"},
		{path: "/api/channels/872712139712913438/messages/872712150509047809/reactions/PandaOhShit:863985751205085195", method: "GET", want: "/channels/872712139712913438/messages/!/reactions/!/!"},
		{path: "/api/invites/dyno", method: "GET", want: "/invites/!"},
		{path: "/api/v9/applications/203039963636301824/commands", method: "GET", want: "/applications/203039963636301824/commands"},
		{path: "/api/v9/applications/203039963636301824/commands/203039963636301824", method: "GET", want: "/applications/203039963636301824/commands/!"},
		{path: "/api/v10/webhooks/203039963636301824/aW50ZXJhY3Rpb246!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", method: "POST", want: "/webhooks/203039963636301824/Unknown"},
		{path: "/api/version/channels", method: "GET", want: "/version/channels"},
		{path: "/api/v/channels", method: "GET", want: "/v/channels"},
	}

	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			if got := GetOptimisticBucketPath(test.path, test.method); got != test.want {
				t.Fatalf("GetOptimisticBucketPath() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestUserIDsShareOptimisticBucket(t *testing.T) {
	paths := []string{"/api/v10/users/787061750905962567", "/api/v10/users/1145768749882937486"}
	first := routeHash(http.MethodGet, GetOptimisticBucketPath(paths[0], http.MethodGet), majorParameter(paths[0]))
	second := routeHash(http.MethodGet, GetOptimisticBucketPath(paths[1], http.MethodGet), majorParameter(paths[1]))
	if first != second {
		t.Fatal("distinct user IDs bypassed the shared optimistic scheduler bucket")
	}
}

func TestMetricsPathFromBucket(t *testing.T) {
	tests := []struct {
		name  string
		route string
		want  string
	}{
		{name: "numeric labels", route: "/channels/203039963636301824/messages/123", want: "/channels/!/messages/!"},
		{name: "empty path", route: "/", want: ""},
		{name: "invalid UTF-8", route: string([]byte{'/', 'x', '/', 0xff}), want: "/x/@"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := MetricsPathFromBucket(test.route); got != test.want {
				t.Fatalf("MetricsPathFromBucket() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestHashCRC64Compatibility(t *testing.T) {
	const want uint64 = 10232006911339297906
	if got := HashCRC64("test data"); got != want {
		t.Fatalf("HashCRC64() = %d, want %d", got, want)
	}
}
