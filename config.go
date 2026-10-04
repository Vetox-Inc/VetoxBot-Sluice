package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy"
)

const (
	maxConfiguredTimeout = 24 * time.Hour
	// shortestUsualTimeout is the least a timeout is likely to be meant as: below it, the value
	// was probably written in seconds.
	shortestUsualTimeout = 100 * time.Millisecond
)

var metricsNamespacePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// settingGroups lists every environment variable Sluice reads. It drives --help, and a
// test keeps it, the variables read below, and CONFIG.md in agreement.
var settingGroups = []struct {
	title string
	names []string
}{
	{"Listening", []string{"BIND_IP", "PORT", "CLIENT_AUTH_SECRET"}},
	{"Reaching Discord", []string{"DISCORD_API_URL", "OUTBOUND_IP", "DISABLE_HTTP_2", "REQUEST_TIMEOUT"}},
	{"Queues and limits", []string{"QUEUE_TIMEOUT", "MAX_QUEUE_DEPTH", "MAX_IN_FLIGHT_REQUESTS", "BOT_RATELIMIT_OVERRIDES", "BOT_WIDE_ROUTES"}},
	{"Retries", []string{"MAX_RETRY_BODY_BYTES", "MAX_RETRY_CAPTURE_BYTES"}},
	{"Protecting your IP", []string{"DISABLE_401_LOCK", "CLOUDFLARE_BAN_DETECTION", "STATE_FILE"}},
	{"Memory bounds", []string{"MAX_CLIENT_STATES", "MAX_BEARER_COUNT", "MAX_BUCKET_STATES"}},
	{"Logs, metrics and profiling", []string{"LOG_LEVEL", "LOG_FORMAT", "ENABLE_METRICS", "METRICS_PORT", "METRICS_NAMESPACE", "ENABLE_PPROF", "PPROF_PORT"}},
	{"Cluster", []string{"CLUSTER_MEMBERS", "CLUSTER_DNS", "CLUSTER_SECRET", "CLUSTER_CA_FILE", "CLUSTER_CERT_FILE", "CLUSTER_KEY_FILE", "CLUSTER_PORT", "CLUSTER_PEER_PORT", "CLUSTER_ADVERTISE_ADDR", "CLUSTER_MAX_NODES", "NODE_NAME"}},
}

// obsoleteSettings are accepted from older configurations but no longer have an effect.
var obsoleteSettings = []struct{ name, advice string }{
	{"BUFFER_SIZE", "it sized a queue that no longer exists; MAX_QUEUE_DEPTH bounds each queue"},
	{"DISABLE_GLOBAL_RATELIMIT_DETECTION", "Sluice never guesses the global limit; set BOT_RATELIMIT_OVERRIDES for raised limits"},
}

type appConfig struct {
	bindIP      string
	port        int
	metricsPort int
	pprofPort   int

	enableMetrics    bool
	enablePprof      bool
	metricsNamespace string

	clusterPort          int
	clusterPeerPort      int
	clusterMaxNodes      int
	clusterMembers       []string
	clusterDNS           string
	clusterAdvertiseAddr string
	nodeName             string
	clusterSecret        string
	clusterServerTLS     *tls.Config
	clusterClientTLS     *tls.Config

	proxy proxy.Config

	// warnings are settings that are valid and probably not what was meant.
	warnings []string
}

func loadConfig() (appConfig, error) {
	requestTimeout, err := envDurationMilliseconds("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return appConfig{}, err
	}
	// With REQUEST_TIMEOUT's default, a request is answered within discord.js's default timeout.
	queueTimeout, err := envDurationMilliseconds("QUEUE_TIMEOUT", 10*time.Second)
	if err != nil {
		return appConfig{}, err
	}
	port, err := envInt("PORT", 8080, 1, 65535)
	if err != nil {
		return appConfig{}, err
	}
	metricsPort, err := envInt("METRICS_PORT", 9000, 1, 65535)
	if err != nil {
		return appConfig{}, err
	}
	pprofPort, err := envInt("PPROF_PORT", 7654, 1, 65535)
	if err != nil {
		return appConfig{}, err
	}
	clusterPort, err := envInt("CLUSTER_PORT", 7946, 1, 65535)
	if err != nil {
		return appConfig{}, err
	}
	clusterPeerPort, err := envInt("CLUSTER_PEER_PORT", 8443, 1, 65535)
	if err != nil {
		return appConfig{}, err
	}
	clusterMaxNodes, err := envInt("CLUSTER_MAX_NODES", 32, 1, proxy.InvalidRequestSafetyLimit)
	if err != nil {
		return appConfig{}, err
	}
	maxBearerClients, err := envInt("MAX_BEARER_COUNT", 1024, 1, 1_000_000)
	if err != nil {
		return appConfig{}, err
	}
	maxClientStates, err := envInt("MAX_CLIENT_STATES", 4096, 1, 1_000_000)
	if err != nil {
		return appConfig{}, err
	}
	maxInFlightRequests, err := envInt("MAX_IN_FLIGHT_REQUESTS", 4096, 1, 1_000_000)
	if err != nil {
		return appConfig{}, err
	}
	maxBucketStates, err := envInt("MAX_BUCKET_STATES", 65536, 1, 10_000_000)
	if err != nil {
		return appConfig{}, err
	}
	maxQueueDepth, err := envInt("MAX_QUEUE_DEPTH", 1000, 1, 1_000_000)
	if err != nil {
		return appConfig{}, err
	}
	maxRetryCaptureBytes, err := envInt64("MAX_RETRY_CAPTURE_BYTES", 256<<20, 0, 1<<40)
	if err != nil {
		return appConfig{}, err
	}
	maxRetryBodyBytes, err := envInt64("MAX_RETRY_BODY_BYTES", 25<<20, 0, 1<<30)
	if err != nil {
		return appConfig{}, err
	}
	enableMetrics, err := envBool("ENABLE_METRICS", true)
	if err != nil {
		return appConfig{}, err
	}
	enablePprof, err := envBool("ENABLE_PPROF", false)
	if err != nil {
		return appConfig{}, err
	}
	disableHTTP2, err := envBool("DISABLE_HTTP_2", true)
	if err != nil {
		return appConfig{}, err
	}
	disable401Lock, err := envBool("DISABLE_401_LOCK", false)
	if err != nil {
		return appConfig{}, err
	}
	cloudflareBanDetection, err := envBool("CLOUDFLARE_BAN_DETECTION", true)
	if err != nil {
		return appConfig{}, err
	}
	discordURL := envString("DISCORD_API_URL", proxy.DefaultDiscordURL)
	if _, err := proxy.ParseDiscordURL(discordURL); err != nil {
		return appConfig{}, fmt.Errorf("DISCORD_API_URL: %w", err)
	}
	clusterAdvertiseAddr := strings.TrimSpace(os.Getenv("CLUSTER_ADVERTISE_ADDR"))
	if clusterAdvertiseAddr != "" && net.ParseIP(clusterAdvertiseAddr) == nil {
		return appConfig{}, fmt.Errorf("CLUSTER_ADVERTISE_ADDR must be an IP address")
	}
	metricsNamespace := envString("METRICS_NAMESPACE", proxy.DefaultMetricsNamespace)
	if !metricsNamespacePattern.MatchString(metricsNamespace) {
		return appConfig{}, fmt.Errorf("METRICS_NAMESPACE must match [a-zA-Z_][a-zA-Z0-9_]*")
	}
	clusterMembers := splitNonempty(os.Getenv("CLUSTER_MEMBERS"))
	clusterDNS := strings.TrimSpace(os.Getenv("CLUSTER_DNS"))
	clusterEnabled := len(clusterMembers) > 0 || clusterDNS != ""
	clusterSecret := os.Getenv("CLUSTER_SECRET")
	var clusterServerTLS, clusterClientTLS *tls.Config
	if clusterEnabled {
		if utf8.RuneCountInString(clusterSecret) < 32 {
			return appConfig{}, fmt.Errorf("CLUSTER_SECRET must contain at least 32 characters when clustering is enabled")
		}
		clusterServerTLS, clusterClientTLS, err = loadClusterTLS(
			strings.TrimSpace(os.Getenv("CLUSTER_CA_FILE")),
			strings.TrimSpace(os.Getenv("CLUSTER_CERT_FILE")),
			strings.TrimSpace(os.Getenv("CLUSTER_KEY_FILE")),
		)
		if err != nil {
			return appConfig{}, err
		}
	}
	invalidRequestLimit := proxy.InvalidRequestSafetyLimit
	if clusterEnabled {
		invalidRequestLimit = max(1, proxy.InvalidRequestSafetyLimit/clusterMaxNodes)
	}
	clientAuthSecret := os.Getenv("CLIENT_AUTH_SECRET")
	if clientAuthSecret != "" && utf8.RuneCountInString(clientAuthSecret) < 32 {
		return appConfig{}, fmt.Errorf("CLIENT_AUTH_SECRET must contain at least 32 characters")
	}

	var warnings []string
	for _, timeout := range []struct {
		name  string
		value time.Duration
	}{{"REQUEST_TIMEOUT", requestTimeout}, {"QUEUE_TIMEOUT", queueTimeout}} {
		if timeout.value < shortestUsualTimeout {
			warnings = append(warnings, fmt.Sprintf("%s is in milliseconds: %s is less than Discord takes to answer", timeout.name, timeout.value))
		}
	}
	if maxRetryCaptureBytes > 0 && maxRetryCaptureBytes < maxRetryBodyBytes {
		warnings = append(warnings, "MAX_RETRY_CAPTURE_BYTES is below MAX_RETRY_BODY_BYTES: a body larger than the first and within the second always gets a 503")
	}
	stateFile, stateWarning := stateFilePath(port)
	if stateWarning != "" {
		warnings = append(warnings, stateWarning)
	}

	return appConfig{
		warnings:             warnings,
		bindIP:               envString("BIND_IP", "0.0.0.0"),
		port:                 port,
		metricsPort:          metricsPort,
		pprofPort:            pprofPort,
		enableMetrics:        enableMetrics,
		enablePprof:          enablePprof,
		metricsNamespace:     metricsNamespace,
		clusterPort:          clusterPort,
		clusterPeerPort:      clusterPeerPort,
		clusterMaxNodes:      clusterMaxNodes,
		clusterMembers:       clusterMembers,
		clusterDNS:           clusterDNS,
		clusterAdvertiseAddr: clusterAdvertiseAddr,
		nodeName:             strings.TrimSpace(os.Getenv("NODE_NAME")),
		clusterSecret:        clusterSecret,
		clusterServerTLS:     clusterServerTLS,
		clusterClientTLS:     clusterClientTLS,
		proxy: proxy.Config{
			OutboundIP:             strings.TrimSpace(os.Getenv("OUTBOUND_IP")),
			UpstreamTimeout:        requestTimeout,
			QueueTimeout:           queueTimeout,
			DisableHTTP2:           disableHTTP2,
			Disable401Lock:         disable401Lock,
			EnableMetrics:          enableMetrics,
			GlobalOverrides:        strings.TrimSpace(os.Getenv("BOT_RATELIMIT_OVERRIDES")),
			BotWideRoutes:          strings.TrimSpace(os.Getenv("BOT_WIDE_ROUTES")),
			MaxBearerClients:       maxBearerClients,
			MaxClientStates:        maxClientStates,
			MaxInFlightRequests:    maxInFlightRequests,
			MaxBucketStates:        maxBucketStates,
			MaxQueueDepth:          maxQueueDepth,
			MaxRetryBodyBytes:      maxRetryBodyBytes,
			MaxRetryCaptureBytes:   maxRetryCaptureBytes,
			InvalidRequestLimit:    invalidRequestLimit,
			DiscordURL:             discordURL,
			CloudflareBanDetection: cloudflareBanDetection,
			ClientAuthSecret:       clientAuthSecret,
			StateFile:              stateFile,
		},
	}, nil
}

// stateFilePath is STATE_FILE, where an empty value turns the file off, or by default a file
// named after the proxy port in the user cache directory. Without such a directory there is no
// file, and a warning that says so.
func stateFilePath(port int) (path, warning string) {
	if value, set := os.LookupEnv("STATE_FILE"); set {
		return strings.TrimSpace(value), ""
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", "The protection state is not kept across restarts: there is no user cache directory to default to, so set STATE_FILE"
	}
	return filepath.Join(cache, "sluice", fmt.Sprintf("state-%d.json", port)), ""
}

func (c appConfig) clusteringEnabled() bool {
	return len(c.clusterMembers) > 0 || c.clusterDNS != ""
}

func loadClusterTLS(caFile, certFile, keyFile string) (*tls.Config, *tls.Config, error) {
	for _, required := range []struct{ name, value string }{
		{"CLUSTER_CA_FILE", caFile},
		{"CLUSTER_CERT_FILE", certFile},
		{"CLUSTER_KEY_FILE", keyFile},
	} {
		if required.value == "" {
			return nil, nil, fmt.Errorf("%s is required when clustering is enabled", required.name)
		}
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read CLUSTER_CA_FILE: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("CLUSTER_CA_FILE contains no valid PEM certificates")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load cluster certificate and key: %w", err)
	}
	if err := verifyClusterCertificate(&certificate, caPool); err != nil {
		return nil, nil, err
	}
	server := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	client := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      caPool,
	}
	return server, client, nil
}

func verifyClusterCertificate(certificate *tls.Certificate, roots *x509.CertPool) error {
	if len(certificate.Certificate) == 0 {
		return fmt.Errorf("CLUSTER_CERT_FILE contains no certificates")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse CLUSTER_CERT_FILE leaf: %w", err)
	}
	intermediates := x509.NewCertPool()
	for _, raw := range certificate.Certificate[1:] {
		parsed, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("parse CLUSTER_CERT_FILE chain: %w", err)
		}
		intermediates.AddCert(parsed)
	}
	for _, required := range []struct {
		name  string
		usage x509.ExtKeyUsage
	}{
		{name: "server", usage: x509.ExtKeyUsageServerAuth},
		{name: "client", usage: x509.ExtKeyUsageClientAuth},
	} {
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{required.usage},
		}); err != nil {
			return fmt.Errorf("verify cluster certificate for %s authentication: %w", required.name, err)
		}
	}
	certificate.Leaf = leaf
	return nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) (bool, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false: %w", name, err)
	}
	return parsed, nil
}

func envInt(name string, fallback, minimum, maximum int) (int, error) {
	value, err := envInt64(name, int64(fallback), int64(minimum), int64(maximum))
	return int(value), err
}

func envInt64(name string, fallback, minimum, maximum int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return parsed, nil
}

func envDurationMilliseconds(name string, fallback time.Duration) (time.Duration, error) {
	milliseconds, err := envInt64(name, fallback.Milliseconds(), 1, maxConfiguredTimeout.Milliseconds())
	if err != nil {
		return 0, err
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

func splitNonempty(value string) []string {
	var values []string
	for raw := range strings.SplitSeq(value, ",") {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}
