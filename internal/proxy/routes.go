package proxy

import (
	"encoding/base64"
	"hash/crc64"
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
		logger.Warn("Non utf-8 path detected, Prometheus only supports utf-8, invalid runes will be replaced with @ in metrics. Path: " + result)
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
	for index, part := range remainingParts {
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
