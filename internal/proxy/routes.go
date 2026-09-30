package proxy

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"hash/crc64"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	discordEpochMilliseconds = 1420070400000
	interactionTokenPrefix   = "aW50ZXJhY3Rpb246"

	majorChannels     = "channels"
	majorGuilds       = "guilds"
	majorWebhooks     = "webhooks"
	majorInvites      = "invites"
	majorInteractions = "interactions"
)

var crc64Table = crc64.MakeTable(crc64.ISO)

// identifierFollows names literal segments followed by a non-numeric identifier, so neither
// buckets nor metric labels multiply per activity instance, provider identity or template code.
var identifierFollows = map[string]bool{"activity-instances": true, "identities": true, "templates": true}

// HashCRC64 returns the stable hash used for bucket and cluster affinity.
func HashCRC64(data string) uint64 {
	return crc64.Checksum([]byte(data), crc64Table)
}

func routeHash(method, bucketPath, majorKey string) uint64 {
	return HashCRC64(method + "\x00" + bucketPath + "\x00" + majorKey)
}

func isSnowflake(value string) bool {
	if len(value) < 17 || len(value) > 20 {
		return false
	}
	return isNumericInput(value)
}

func isNumericInput(value string) bool {
	if value == "" {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func snowflakeCreatedAt(snowflake string) (time.Time, error) {
	parsedID, err := strconv.ParseUint(snowflake, 10, 64)
	if err != nil {
		return time.Now(), err
	}
	epoch := (parsedID >> 22) + discordEpochMilliseconds
	return time.Unix(int64(epoch)/1000, 0), nil
}

// MetricsPathFromBucket removes numeric path labels to keep metric cardinality bounded.
func MetricsPathFromBucket(route string) string {
	var path strings.Builder
	for _, part := range strings.Split(route, "/") {
		if part == "" {
			continue
		}
		if isNumericInput(part) {
			path.WriteString("/!")
		} else {
			path.WriteByte('/')
			path.WriteString(part)
		}
	}

	result := path.String()
	if !utf8.ValidString(result) {
		logger.Warn("Non-UTF-8 path detected; invalid runes become @ in metric labels", "path", result)
		result = strings.ToValidUTF8(result, "@")
	}
	return result
}

// GetOptimisticBucketPath groups Discord routes before a bucket identifier is known.
func GetOptimisticBucketPath(path, method string) string {
	var bucket strings.Builder
	bucket.WriteByte('/')

	cleanPath := strings.SplitN(path, "?", 2)[0]
	if versioned := strings.TrimPrefix(cleanPath, "/api/v"); versioned != cleanPath {
		if slash := strings.IndexByte(versioned, '/'); slash >= 0 && isNumericInput(versioned[:slash]) {
			cleanPath = versioned[slash+1:]
		} else {
			cleanPath = strings.TrimPrefix(cleanPath, "/api/")
		}
	} else if unversioned := strings.TrimPrefix(cleanPath, "/api/"); unversioned != cleanPath {
		cleanPath = unversioned
	} else {
		cleanPath = strings.TrimPrefix(cleanPath, "/")
	}

	parts := strings.Split(cleanPath, "/")
	if len(parts) <= 1 {
		return cleanPath
	}

	currentMajor := parts[0]
	switch parts[0] {
	case majorChannels:
		bucket.WriteString(majorChannels)
		bucket.WriteByte('/')
		bucket.WriteString(parts[1])
	case "users":
		bucket.WriteString("users/")
		if isSnowflake(parts[1]) {
			bucket.WriteByte('!')
		} else {
			bucket.WriteString(parts[1])
		}
	case majorInvites:
		bucket.WriteString(majorInvites)
		bucket.WriteString("/!")
	case majorGuilds:
		fallthrough
	case majorInteractions:
		if len(parts) == 4 && parts[3] == "callback" {
			return "/" + majorInteractions + "/" + parts[1] + "/!/callback"
		}
		fallthrough
	case majorWebhooks:
		fallthrough
	default:
		bucket.WriteString(parts[0])
		bucket.WriteByte('/')
		bucket.WriteString(parts[1])
	}

	if len(parts) == 2 {
		return bucket.String()
	}

	remainingParts := parts[2:]
	collapseRest := false
	for index, part := range remainingParts {
		if identifierFollows[parts[index+1]] || collapseRest && part != "delete" {
			bucket.WriteString("/!")
			continue
		}
		collapseRest = collapseRest || part == "application-identities"
		if isSnowflake(part) {
			if currentMajor == majorChannels && parts[index+1] == "messages" && method == "DELETE" && index == len(remainingParts)-1 {
				createdAt, _ := snowflakeCreatedAt(part)
				if createdAt.Before(time.Now().Add(-14 * 24 * time.Hour)) {
					bucket.WriteString("/!14dmsg")
				} else if createdAt.After(time.Now().Add(-10 * time.Second)) {
					bucket.WriteString("/!10smsg")
				}
				continue
			}
			bucket.WriteString("/!")
			continue
		}

		if currentMajor == majorChannels && part == "reactions" {
			if method == "PUT" || method == "DELETE" {
				bucket.WriteString("/reactions/!modify")
			} else {
				bucket.WriteString("/reactions/!/!")
			}
			break
		}

		sensitiveToken := index == 0 && (currentMajor == majorWebhooks || currentMajor == majorInteractions)
		if sensitiveToken || len(part) >= 64 {
			if !strings.HasPrefix(part, interactionTokenPrefix) {
				bucket.WriteString("/!")
				continue
			}

			if padding := len(part) % 4; padding != 0 {
				part += strings.Repeat("=", 4-padding)
			}
			interactionID := "Unknown"
			if decodedPart, err := base64.StdEncoding.DecodeString(part); err == nil {
				_, interactionID, _ = strings.Cut(string(decodedPart), ":")
				interactionID, _, _ = strings.Cut(interactionID, ":")
				if interactionID == "" {
					interactionID = "Unknown"
				}
			}

			bucket.WriteByte('/')
			bucket.WriteString(interactionID)
			continue
		}

		bucket.WriteByte('/')
		bucket.WriteString(part)
	}

	return bucket.String()
}

func cleanAPIPath(path string) []string {
	path = strings.SplitN(path, "?", 2)[0]
	path = strings.TrimPrefix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) > 0 && parts[0] == "api" {
		parts = parts[1:]
		if len(parts) > 0 && len(parts[0]) > 1 && parts[0][0] == 'v' && isNumericInput(parts[0][1:]) {
			parts = parts[1:]
		}
	}
	return parts
}

func majorParameter(path string) string {
	parts := cleanAPIPath(path)
	if len(parts) < 2 {
		return ""
	}
	switch parts[0] {
	case majorChannels, majorGuilds:
		return parts[0] + ":" + parts[1]
	case majorWebhooks:
		if len(parts) >= 3 {
			return parts[0] + ":" + parts[1] + ":" + strconv.FormatUint(HashCRC64(parts[2]), 10)
		}
		return parts[0] + ":" + parts[1]
	default:
		return ""
	}
}

func isInteractionEndpoint(path string) bool {
	parts := cleanAPIPath(path)
	if len(parts) >= 4 && parts[0] == majorInteractions && parts[3] == "callback" {
		return true
	}
	return len(parts) >= 3 && parts[0] == majorWebhooks && strings.HasPrefix(parts[2], interactionTokenPrefix)
}

// isCleanDiscordPath rejects dot segments, encoded separators and encoded question marks, which
// could make Discord resolve a different route from the one this proxy rate-limited.
func isCleanDiscordPath(u *url.URL) bool {
	for _, segment := range strings.Split(u.EscapedPath(), "/") {
		decoded, err := url.PathUnescape(segment)
		if err != nil || decoded == "." || decoded == ".." || strings.ContainsAny(decoded, "/\\?") {
			return false
		}
	}
	return true
}

// ensureAPIPrefix lets clients whose base URL omits /api work unchanged.
func ensureAPIPrefix(u *url.URL) {
	if u.Path == "/api" || strings.HasPrefix(u.Path, "/api/") {
		return
	}
	u.Path = "/api" + u.Path
	if u.RawPath != "" {
		u.RawPath = "/api" + u.RawPath
	}
}

// collapseSlashes merges repeated slashes as Discord does, so a client base URL ending in "/"
// is classified as the route Discord will serve.
func collapseSlashes(u *url.URL) {
	for strings.Contains(u.Path, "//") {
		u.Path = strings.ReplaceAll(u.Path, "//", "/")
	}
	for strings.Contains(u.RawPath, "//") {
		u.RawPath = strings.ReplaceAll(u.RawPath, "//", "/")
	}
}

// webhookCredentialKey identifies a webhook authenticated by the token in its path, or ""
// for every other route. Interaction follow-ups are excluded: their tokens expire on their own.
func webhookCredentialKey(path string, interaction bool) string {
	parts := cleanAPIPath(path)
	if interaction || len(parts) < 3 || parts[0] != majorWebhooks {
		return ""
	}
	digest := sha256.Sum256([]byte(parts[1] + "/" + parts[2]))
	return hex.EncodeToString(digest[:16])
}
