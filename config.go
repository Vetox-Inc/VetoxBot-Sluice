package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Melonly-Moderation/nirn-proxy/internal/proxy"
)

const maxConfiguredTimeout = 24 * time.Hour

type appConfig struct {
	bindIP      string
	port        int
	metricsPort int
	pprofPort   int

	enableMetrics bool
	enablePprof   bool

	clusterPort      int
	clusterPeerPort  int
	clusterMaxNodes  int
	clusterMembers   []string
	clusterDNS       string
	nodeName         string
	clusterSecret    string
	clusterServerTLS *tls.Config
	clusterClientTLS *tls.Config

	proxy proxy.Config
}

func loadConfig() (appConfig, error) {
	requestTimeout, err := envDurationMilliseconds("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return appConfig{}, err
	}
	queueTimeout, err := envDurationMilliseconds("QUEUE_TIMEOUT", time.Minute)
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

	return appConfig{
		bindIP:           envString("BIND_IP", "0.0.0.0"),
		port:             port,
		metricsPort:      metricsPort,
		pprofPort:        pprofPort,
		enableMetrics:    enableMetrics,
		enablePprof:      enablePprof,
		clusterPort:      clusterPort,
		clusterPeerPort:  clusterPeerPort,
		clusterMaxNodes:  clusterMaxNodes,
		clusterMembers:   clusterMembers,
		clusterDNS:       clusterDNS,
		nodeName:         strings.TrimSpace(os.Getenv("NODE_NAME")),
		clusterSecret:    clusterSecret,
		clusterServerTLS: clusterServerTLS,
		clusterClientTLS: clusterClientTLS,
		proxy: proxy.Config{
			OutboundIP:           strings.TrimSpace(os.Getenv("OUTBOUND_IP")),
			UpstreamTimeout:      requestTimeout,
			QueueTimeout:         queueTimeout,
			DisableHTTP2:         disableHTTP2,
			Disable401Lock:       disable401Lock,
			EnableMetrics:        enableMetrics,
			GlobalOverrides:      strings.TrimSpace(os.Getenv("BOT_RATELIMIT_OVERRIDES")),
			MaxBearerClients:     maxBearerClients,
			MaxClientStates:      maxClientStates,
			MaxInFlightRequests:  maxInFlightRequests,
			MaxBucketStates:      maxBucketStates,
			MaxQueueDepth:        maxQueueDepth,
			MaxRetryBodyBytes:    maxRetryBodyBytes,
			MaxRetryCaptureBytes: maxRetryCaptureBytes,
			InvalidRequestLimit:  invalidRequestLimit,
		},
	}, nil
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
