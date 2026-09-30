package proxy

import (
	"net/http"
	"net/http/pprof"
	"regexp"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

const (
	maxMetricsRouteLabels     = 1024
	maxMetricsRouteLabelBytes = 512
	overflowMetricsRouteLabel = "/unknown"
	auxiliaryReadTimeout      = 15 * time.Second
	auxiliaryWriteTimeout     = 2 * time.Minute
	auxiliaryIdleTimeout      = 30 * time.Second
)

var (
	ErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_error",
		Help: "The total number of errors when processing requests",
	})
	ProxyFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nirn_proxy_failures_total",
		Help: "Proxy failures by bounded reason",
	}, []string{"reason"})

	RequestHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nirn_proxy_requests",
		Help:    "Request histogram",
		Buckets: []float64{.1, .25, 1, 2.5, 5, 20},
	}, []string{"method", "status", "route", "clientId"})

	ConnectionsOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nirn_proxy_open_connections",
		Help: "Gauge for requests currently active in the proxy handler",
	}, []string{"method", "route"})

	RequestsRoutedSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_requests_routed_sent",
		Help: "Counter for requests routed from this node into other nodes",
	})

	RequestsRoutedRecv = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_requests_routed_received",
		Help: "Counter for requests received from other nodes",
	})

	RequestsRoutedError = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nirn_proxy_requests_routed_error",
		Help: "Counter for failed requests routed from this node",
	})

	registerMetrics sync.Once
	metricsRoutes   = newRouteLabelLimiter(maxMetricsRouteLabels)

	logger          = newLogger()
	loggerHookRegex = regexp.MustCompile(`(/(?:webhooks|interactions)/[^/?\s]+/)[^/?\s]+`)
)

type routeLabelLimiter struct {
	mu    sync.Mutex
	limit int
	seen  map[string]struct{}
}

func newRouteLabelLimiter(limit int) *routeLabelLimiter {
	return &routeLabelLimiter{
		limit: limit,
		seen:  map[string]struct{}{overflowMetricsRouteLabel: {}},
	}
}

func (l *routeLabelLimiter) label(route string) string {
	if len(route) > maxMetricsRouteLabelBytes {
		return overflowMetricsRouteLabel
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[route]; ok {
		return route
	}
	if len(l.seen) >= l.limit {
		return overflowMetricsRouteLabel
	}
	l.seen[route] = struct{}{}
	return route
}

// metricsRouteLabel bounds the number of request-derived Prometheus route labels.
func metricsRouteLabel(route string) string {
	return metricsRoutes.label(route)
}

func metricsMethodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

type GlobalHook struct{}

func newLogger() *logrus.Logger {
	configured := logrus.New()
	configured.AddHook(&GlobalHook{})
	return configured
}

func (*GlobalHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (*GlobalHook) Fire(entry *logrus.Entry) error {
	entry.Message = redactLogSecrets(entry.Message)
	for _, field := range []string{"path", "route", logrus.ErrorKey} {
		switch value := entry.Data[field].(type) {
		case string:
			entry.Data[field] = redactLogSecrets(value)
		case error:
			entry.Data[field] = redactLogSecrets(value.Error())
		}
	}
	if logrus.ErrorLevel >= entry.Level {
		ErrorCounter.Inc()
	}
	return nil
}

func redactLogSecrets(value string) string {
	return loggerHookRegex.ReplaceAllString(value, "$1:token")
}

func SetLogger(replacement *logrus.Logger) {
	logger = replacement
	logger.AddHook(&GlobalHook{})
}

func NewMetricsServer(addr string) *http.Server {
	registerMetrics.Do(func() { prometheus.MustRegister(RequestHistogram) })
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return newAuxiliaryServer(addr, mux)
}

func NewProfileServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return newAuxiliaryServer(addr, mux)
}

func newAuxiliaryServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       auxiliaryReadTimeout,
		WriteTimeout:      auxiliaryWriteTimeout,
		IdleTimeout:       auxiliaryIdleTimeout,
	}
}
