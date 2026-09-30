package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Melonly-Moderation/nirn-proxy/internal/proxy"
)

func TestStandaloneConfigNeedsNoClusterCredentials(t *testing.T) {
	clearClusterEnvironment(t)
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.clusteringEnabled() || config.clusterServerTLS != nil || config.clusterClientTLS != nil {
		t.Fatal("standalone configuration unexpectedly enabled cluster TLS")
	}
	if config.proxy.InvalidRequestLimit != proxy.InvalidRequestSafetyLimit {
		t.Fatalf("standalone invalid-request limit = %d, want %d", config.proxy.InvalidRequestLimit, proxy.InvalidRequestSafetyLimit)
	}
}

func TestClusterConfigRequiresStrongSecret(t *testing.T) {
	clearClusterEnvironment(t)
	t.Setenv("CLUSTER_MEMBERS", "127.0.0.1")
	t.Setenv("CLUSTER_SECRET", "too-short")
	_, err := loadConfig()
	if err == nil || !strings.Contains(err.Error(), "at least 32 characters") {
		t.Fatalf("loadConfig error = %v, want strong-secret error", err)
	}
}

func TestClusterMaximumCannotExceedInvalidRequestBudget(t *testing.T) {
	clearClusterEnvironment(t)
	t.Setenv("CLUSTER_MAX_NODES", "9501")
	_, err := loadConfig()
	if err == nil || !strings.Contains(err.Error(), "between 1 and 9500") {
		t.Fatalf("loadConfig error = %v, want CLUSTER_MAX_NODES range error", err)
	}
}

func TestInFlightAndMetricsConfiguration(t *testing.T) {
	clearClusterEnvironment(t)
	t.Setenv("MAX_IN_FLIGHT_REQUESTS", "7")
	t.Setenv("ENABLE_METRICS", "false")
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.proxy.MaxInFlightRequests != 7 || config.proxy.EnableMetrics || config.enableMetrics {
		t.Fatalf("in-flight=%d proxyMetrics=%v listenerMetrics=%v, want 7/false/false", config.proxy.MaxInFlightRequests, config.proxy.EnableMetrics, config.enableMetrics)
	}
}

func TestHTTP2IsDisabledByDefaultButCanBeEnabled(t *testing.T) {
	clearClusterEnvironment(t)
	t.Setenv("DISABLE_HTTP_2", "")
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !config.proxy.DisableHTTP2 {
		t.Fatal("outbound HTTP/2 was enabled by default")
	}

	t.Setenv("DISABLE_HTTP_2", "false")
	config, err = loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.proxy.DisableHTTP2 {
		t.Fatal("DISABLE_HTTP_2=false did not enable outbound HTTP/2")
	}
}

func TestDotEnvMissingIsOptionalButMalformedFails(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		t.Chdir(t.TempDir())
		if err := loadDotEnv(); err != nil {
			t.Fatalf("missing .env: %v", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, ".env"), []byte("BROKEN='unterminated\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Chdir(directory)
		if err := loadDotEnv(); err == nil || !strings.Contains(err.Error(), "load .env") {
			t.Fatalf("malformed .env error = %v, want explicit load error", err)
		}
	})
}

func TestClusterTLSRequiresAndValidatesPeerCertificates(t *testing.T) {
	clearClusterEnvironment(t)
	caFile, certFile, keyFile := writeClusterTestCertificate(t)
	t.Setenv("CLUSTER_MEMBERS", "127.0.0.1")
	t.Setenv("CLUSTER_SECRET", strings.Repeat("s", 32))
	t.Setenv("CLUSTER_CA_FILE", caFile)
	t.Setenv("CLUSTER_CERT_FILE", certFile)
	t.Setenv("CLUSTER_KEY_FILE", keyFile)
	t.Setenv("CLUSTER_MAX_NODES", "32")

	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.proxy.InvalidRequestLimit != proxy.InvalidRequestSafetyLimit/32 {
		t.Fatalf("cluster invalid-request limit = %d, want %d", config.proxy.InvalidRequestLimit, proxy.InvalidRequestSafetyLimit/32)
	}
	withoutCertificate := config.clusterClientTLS.Clone()
	withoutCertificate.Certificates = nil
	withoutCertificate.ServerName = "127.0.0.1"
	serverErr, _ := handshakeClusterPeers(config.clusterServerTLS, withoutCertificate)
	if serverErr == nil {
		t.Fatal("peer server accepted a client without a certificate")
	}

	untrustedServer := config.clusterClientTLS.Clone()
	untrustedServer.RootCAs = x509.NewCertPool()
	untrustedServer.ServerName = "127.0.0.1"
	_, clientErr := handshakeClusterPeers(config.clusterServerTLS, untrustedServer)
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(clientErr, &unknownAuthority) {
		t.Fatalf("untrusted peer client error = %v, want x509.UnknownAuthorityError", clientErr)
	}

	authenticated := config.clusterClientTLS.Clone()
	authenticated.ServerName = "127.0.0.1"
	serverErr, clientErr = handshakeClusterPeers(config.clusterServerTLS, authenticated)
	if serverErr != nil || clientErr != nil {
		t.Fatalf("mutual TLS handshake errors: server=%v client=%v", serverErr, clientErr)
	}
}

func TestClusterTLSRejectsCertificateOutsideConfiguredCA(t *testing.T) {
	trustedCA, _, _ := writeClusterTestCertificate(t)
	_, untrustedCertificate, untrustedKey := writeClusterTestCertificate(t)
	if _, _, err := loadClusterTLS(trustedCA, untrustedCertificate, untrustedKey); err == nil || !strings.Contains(err.Error(), "verify cluster certificate") {
		t.Fatalf("untrusted cluster certificate error = %v, want chain verification error", err)
	}
}

func handshakeClusterPeers(serverConfig, clientConfig *tls.Config) (error, error) {
	serverConnection, clientConnection := net.Pipe()
	deadline := time.Now().Add(250 * time.Millisecond)
	_ = serverConnection.SetDeadline(deadline)
	_ = clientConnection.SetDeadline(deadline)
	server := tls.Server(serverConnection, serverConfig.Clone())
	client := tls.Client(clientConnection, clientConfig.Clone())
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Handshake() }()
	clientErr := client.Handshake()
	var clientReadDone chan struct{}
	if clientErr != nil {
		_ = clientConnection.Close()
	} else {
		// net.Pipe has no buffer. Keep consuming post-handshake TLS records so
		// the server can finish instead of waiting for the test deadline.
		clientReadDone = make(chan struct{})
		go func() {
			var byteBuffer [1]byte
			_, _ = client.Read(byteBuffer[:])
			close(clientReadDone)
		}()
	}
	serverErr := <-serverResult
	_ = clientConnection.Close()
	_ = serverConnection.Close()
	if clientReadDone != nil {
		<-clientReadDone
	}
	return serverErr, clientErr
}

func clearClusterEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"CLUSTER_MEMBERS", "CLUSTER_DNS", "CLUSTER_SECRET", "CLUSTER_CA_FILE", "CLUSTER_CERT_FILE", "CLUSTER_KEY_FILE", "CLUSTER_MAX_NODES",
	} {
		t.Setenv(name, "")
	}
}

func writeClusterTestCertificate(t *testing.T) (string, string, string) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Nirn test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Nirn test peer"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	caFile := filepath.Join(directory, "ca.pem")
	certFile := filepath.Join(directory, "peer.pem")
	keyFile := filepath.Join(directory, "peer-key.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caFile, certFile, keyFile
}
