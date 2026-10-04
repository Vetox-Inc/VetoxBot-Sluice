package proxy

import (
	"net/http"
	"sync"
	"time"
	"unicode/utf8"
)

// failureWarningInterval spaces the warnings for one reason: what fails one request usually
// fails the next thousand.
const failureWarningInterval = 30 * time.Second

// failureReasons lists every reason sluice_failures_total can carry. Each is exported at 0 from
// the start, because Prometheus sees no increase in a series whose first sample is already 1.
var failureReasons = []string{
	"bad_request", "bucket_limit", "client_auth", "client_closed", "client_limit", "cloudflare_pause",
	"cluster_over_capacity", "cluster_routing", "deadline", "in_flight_limit", "invalid_request_budget",
	"invalid_token", "peer_error", "queue_full", "queue_timeout", "rate_limit_deadline", "response_aborted",
	"retry_capture_limit", "retry_preparation", "shutting_down", "unknown_endpoint", "unsupported_authorization",
	"upstream_error", "upstream_timeout",
}

// failureWarnings says what to tell an operator about the failures they have to act on. Every
// other reason is an answer clients retry or expect, and is only counted.
var failureWarnings = map[string]string{
	"upstream_error":         "Could not reach Discord",
	"upstream_timeout":       "Discord did not answer within REQUEST_TIMEOUT",
	"deadline":               "A network timeout ended an attempt before REQUEST_TIMEOUT did",
	"response_aborted":       "A response from Discord ended before its body did",
	"peer_error":             "Could not reach a cluster peer",
	"retry_preparation":      "Could not keep a request body for a retry",
	"invalid_token":          "Answering 401 for a token Discord rejected, until its client has been quiet for 10 minutes",
	"invalid_request_budget": "Refusing requests: the invalid-request budget is used up",
	"in_flight_limit":        "Refusing requests: MAX_IN_FLIGHT_REQUESTS is reached",
	"client_limit":           "Refusing a new client: MAX_CLIENT_STATES or MAX_BEARER_COUNT is reached",
	"bucket_limit":           "Refusing requests that need a new bucket: MAX_BUCKET_STATES is reached and every bucket is in use",
	"retry_capture_limit":    "Refusing an upload: MAX_RETRY_CAPTURE_BYTES is reached",
}

type failureWarning struct {
	reason, method, route string
	// bot is the user ID in the request's bot token, which Discord may not have accepted.
	bot   string
	cause error
	// suppressed counts the failures of this reason since its last warning.
	suppressed int
}

// failureLog queues warnings for failureLogLoop. A request must never wait for the log, so a
// warning that finds the queue full is left out and counted with the next.
type failureLog struct {
	mu         sync.Mutex
	next       map[string]time.Time
	suppressed map[string]int
	warnings   chan failureWarning
	// stopped is closed when failureLogLoop returns.
	stopped chan struct{}
}

func newFailureLog() *failureLog {
	return &failureLog{
		next:       make(map[string]time.Time, len(failureWarnings)),
		suppressed: make(map[string]int, len(failureWarnings)),
		warnings:   make(chan failureWarning, len(failureWarnings)),
		stopped:    make(chan struct{}),
	}
}

func (l *failureLog) report(now time.Time, reason string, request *http.Request, cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Before(l.next[reason]) {
		l.suppressed[reason]++
		return
	}
	warning := failureWarning{reason: reason, cause: cause, suppressed: l.suppressed[reason]}
	warning.method, warning.route, warning.bot = describeFailedRequest(request)
	select {
	case l.warnings <- warning:
		l.next[reason] = now.Add(failureWarningInterval)
		l.suppressed[reason] = 0
	default:
		l.suppressed[reason]++
	}
}

// describeFailedRequest names a request the way the metrics do, without the identifiers in its
// path, and the bot it claims to come from.
func describeFailedRequest(request *http.Request) (method, route, bot string) {
	if metadata, ok := request.Context().Value(requestMetadataContextKey).(*requestMetadata); ok {
		return metadata.metricsMethod, metadata.metricsPath, metadata.state.identity.botID
	}
	route = overflowMetricsRouteLabel
	if utf8.ValidString(request.URL.Path) {
		if named := MetricsPathFromBucket(GetOptimisticBucketPath(request.URL.Path, request.Method)); len(named) <= maxMetricsRouteLabelBytes {
			route = named
		}
	}
	return metricsMethodLabel(request.Method), route, ""
}

// fail counts a request Sluice answers with an error of its own, or abandons, and warns about
// the ones an operator has to act on. cause may be nil.
func (p *Proxy) fail(reason string, request *http.Request, cause error) {
	ProxyFailures.WithLabelValues(reason).Inc()
	if _, warned := failureWarnings[reason]; warned {
		p.failures.report(time.Now(), reason, request, cause)
	}
}

// failureLogLoop writes the warnings. Close waits for it only after the state is saved, so a log
// that has stopped taking output cannot cost the protection state.
func (p *Proxy) failureLogLoop() {
	defer close(p.failures.stopped)
	for {
		select {
		case warning := <-p.failures.warnings:
			attributes := []any{"reason", warning.reason, "method", warning.method, "route", warning.route}
			if warning.bot != "" {
				attributes = append(attributes, "bot", warning.bot)
			}
			if warning.cause != nil {
				attributes = append(attributes, "error", warning.cause)
			}
			if warning.suppressed > 0 {
				attributes = append(attributes, "suppressed", warning.suppressed)
			}
			logger.Warn(failureWarnings[warning.reason], attributes...)
		case <-p.ctx.Done():
			return
		}
	}
}

// responseAborted counts a response from Discord that ended before its body did. A client that
// leaves and a shutdown end the body too, and each is counted as what it is.
func (p *Proxy) responseAborted(request *http.Request, cause error) {
	switch {
	case p.ctx.Err() != nil:
		p.fail("shutting_down", request, nil)
	case request.Context().Err() != nil:
		p.fail("client_closed", request, nil)
	default:
		p.fail("response_aborted", request, cause)
	}
}
