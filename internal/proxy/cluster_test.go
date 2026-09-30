package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestClusterSecretKeyUsesSHA256AndRejectsShortSecrets(t *testing.T) {
	if _, err := clusterSecretKey(strings.Repeat("x", 31)); err == nil {
		t.Fatal("31-character cluster secret was accepted")
	}
	secret := strings.Repeat("secret-", 6)
	key, err := clusterSecretKey(secret)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(secret))
	if string(key) != string(want[:]) {
		t.Fatal("memberlist secret key is not SHA-256(CLUSTER_SECRET)")
	}
}

func TestClusterRejectsNonIPBindAddress(t *testing.T) {
	if err := (&Proxy{}).JoinCluster(ClusterConfig{Port: 7946, PeerPort: 8443, BindAddress: "localhost"}); err == nil || !strings.Contains(err.Error(), "must be an IP address") {
		t.Fatalf("non-IP cluster bind error = %v", err)
	}
}

func TestPeerCertificateMustMatchAdvertisedAddress(t *testing.T) {
	configuration := &tls.Config{Certificates: []tls.Certificate{{
		Leaf: &x509.Certificate{IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}},
	}}}
	if err := validatePeerCertificateAddress(configuration, net.ParseIP("127.0.0.1")); err != nil {
		t.Fatalf("matching peer address: %v", err)
	}
	if err := validatePeerCertificateAddress(configuration, net.ParseIP("127.0.0.2")); err == nil {
		t.Fatal("certificate without the advertised IP SAN was accepted")
	}
}

func TestConfiguredClusterJoinFailsClosed(t *testing.T) {
	want := errors.New("secret mismatch")
	err := joinKnownMembers([]string{"seed:7946"}, func([]string) (int, error) { return 0, want })
	if !errors.Is(err, want) {
		t.Fatalf("joinKnownMembers error = %v, want wrapped seed error", err)
	}
}

func TestPeerTransportRequiresVerifiedMutualTLSAndIgnoresEnvironmentProxy(t *testing.T) {
	if _, err := newPeerHTTPTransport(nil); err == nil {
		t.Fatal("nil peer TLS configuration was accepted")
	}
	if _, err := newPeerHTTPTransport(&tls.Config{Certificates: []tls.Certificate{{}}}); err == nil {
		t.Fatal("peer TLS configuration without explicit roots was accepted")
	}
	if _, err := newPeerHTTPTransport(&tls.Config{RootCAs: x509.NewCertPool()}); err == nil {
		t.Fatal("peer TLS configuration without a client certificate was accepted")
	}
	if _, err := newPeerHTTPTransport(&tls.Config{
		RootCAs:            x509.NewCertPool(),
		Certificates:       []tls.Certificate{{}},
		InsecureSkipVerify: true,
	}); err == nil {
		t.Fatal("peer TLS configuration with verification disabled was accepted")
	}

	configuration := &tls.Config{RootCAs: x509.NewCertPool(), Certificates: []tls.Certificate{{}}}
	transport, err := newPeerHTTPTransport(configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	if transport.Proxy != nil {
		t.Fatal("peer transport can use HTTP_PROXY")
	}
	if transport.TLSClientConfig == configuration {
		t.Fatal("peer transport retained a mutable caller TLS configuration")
	}
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("peer transport disabled server-certificate validation")
	}
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("peer minimum TLS version = %x, want TLS 1.3", transport.TLSClientConfig.MinVersion)
	}
	if transport.MaxConnsPerHost != 256 {
		t.Fatalf("peer connection cap = %d, want 256", transport.MaxConnsPerHost)
	}
}

func TestClusterCapacityFailsClosedAndRecovers(t *testing.T) {
	const maximum = 2
	cluster := &clusterSecurityFake{nodes: []*memberlist.Node{
		{Name: "one", Addr: net.ParseIP("127.0.0.1"), Meta: []byte("8443")},
		{Name: "two", Addr: net.ParseIP("127.0.0.2"), Meta: []byte("8443")},
		{Name: "three", Addr: net.ParseIP("127.0.0.3"), Meta: []byte("8443")},
	}}
	proxy := &Proxy{cluster: cluster, localAddr: "127.0.0.1:8443", maxClusterNodes: maximum}
	proxy.reindexMembers()
	if !proxy.clusterOverCapacity.Load() || proxy.routes.Load() != nil {
		t.Fatal("over-capacity cluster did not fail closed")
	}

	cluster.nodes = cluster.nodes[:maximum]
	proxy.reindexMembers()
	if proxy.clusterOverCapacity.Load() {
		t.Fatal("cluster did not recover after returning below its node cap")
	}
	if table := proxy.routes.Load(); table == nil || len(table.members) != maximum {
		t.Fatalf("recovered route table = %#v", table)
	}
}

type clusterSecurityFake struct {
	nodes     []*memberlist.Node
	shutdowns atomic.Int64
}

func (*clusterSecurityFake) Leave(time.Duration) error     { return nil }
func (f *clusterSecurityFake) Shutdown() error             { f.shutdowns.Add(1); return nil }
func (f *clusterSecurityFake) Members() []*memberlist.Node { return f.nodes }
func (f *clusterSecurityFake) LocalNode() *memberlist.Node { return f.nodes[0] }

func TestJoinClusterAfterCloseDoesNotInstallResources(t *testing.T) {
	configuration := testConfig(roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.Canceled }))
	proxy, err := New(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if err := proxy.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	tlsConfiguration := &tls.Config{RootCAs: x509.NewCertPool(), Certificates: []tls.Certificate{{}}}
	err = proxy.JoinCluster(ClusterConfig{
		Port: 7946, PeerPort: 8443, MaxNodes: 1,
		Secret: strings.Repeat("s", 32), PeerTLS: tlsConfiguration,
	})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("JoinCluster after Close error = %v, want closed error", err)
	}
	if proxy.cluster != nil || proxy.peerTransport != nil {
		t.Fatal("JoinCluster installed resources after Close")
	}
}

func TestCloseClusterCannotMissConcurrentInstallation(t *testing.T) {
	joinDone := make(chan struct{})
	cluster := &clusterSecurityFake{nodes: []*memberlist.Node{{Name: "local"}}}
	proxy := &Proxy{clusterJoining: true, clusterJoinDone: joinDone}
	closed := make(chan error, 1)
	go func() { closed <- proxy.closeCluster(context.Background()) }()

	proxy.clusterMu.Lock()
	proxy.cluster = cluster
	proxy.clusterJoining = false
	proxy.clusterJoinDone = nil
	close(joinDone)
	proxy.clusterMu.Unlock()

	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if cluster.shutdowns.Load() != 1 || proxy.cluster != nil {
		t.Fatalf("concurrent cluster cleanup: shutdowns=%d installed=%v", cluster.shutdowns.Load(), proxy.cluster != nil)
	}
}

func TestClusterSelectionIsDeterministicAndOnlyRemapsToAddedMember(t *testing.T) {
	initial := []clusterMember{
		newClusterMember("alpha", "10.0.0.1:8080"),
		newClusterMember("charlie", "10.0.0.3:8080"),
		newClusterMember("echo", "10.0.0.5:8080"),
	}
	const addedAddress = "10.0.0.2:8080"
	expanded := []clusterMember{
		initial[0],
		newClusterMember("bravo", addedAddress),
		initial[1],
		initial[2],
	}
	proxy := &Proxy{}
	proxy.routes.Store(&routeTable{members: initial, localAddr: "127.0.0.1:8080"})

	const keys = 10_000
	before := make([]string, keys)
	for key := range keys {
		before[key] = proxy.calculateRoute(uint64(key))
		if repeated := proxy.calculateRoute(uint64(key)); repeated != before[key] {
			t.Fatalf("selection for key %d changed without a membership change: %q then %q", key, before[key], repeated)
		}
	}

	proxy.routes.Store(&routeTable{members: expanded, localAddr: "127.0.0.1:8080"})
	moved := 0
	for key := range keys {
		after := proxy.calculateRoute(uint64(key))
		if after == before[key] {
			continue
		}
		moved++
		if after != addedAddress {
			t.Fatalf("adding bravo remapped key %d from %q to existing member %q", key, before[key], after)
		}
	}
	if moved == 0 {
		t.Fatal("added cluster member received no keys")
	}
}

func TestCloseTimeoutDoesNotAbandonCleanup(t *testing.T) {
	proxy, err := New(testConfig(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return testResponse(request, http.StatusOK, nil, nil), nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	joinDone := make(chan struct{})
	proxy.clusterMu.Lock()
	proxy.clusterJoining = true
	proxy.clusterJoinDone = joinDone
	proxy.clusterMu.Unlock()

	firstContext, cancelFirst := context.WithCancel(context.Background())
	cancelFirst()
	if err := proxy.Close(firstContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("first close error = %v, want context.Canceled", err)
	}
	select {
	case <-proxy.closeDone:
		t.Fatal("cleanup completed while cluster initialization was still pending")
	default:
	}

	proxy.clusterMu.Lock()
	proxy.clusterJoining = false
	proxy.clusterJoinDone = nil
	close(joinDone)
	proxy.clusterMu.Unlock()
	secondContext, cancelSecond := context.WithTimeout(context.Background(), time.Second)
	defer cancelSecond()
	if err := proxy.Close(secondContext); err != nil {
		t.Fatalf("eventual close: %v", err)
	}
	proxy.clientsMu.Lock()
	cleared := proxy.bots == nil && proxy.bearers == nil && proxy.noAuth == nil
	proxy.clientsMu.Unlock()
	if !cleared {
		t.Fatal("eventual cleanup did not clear client state")
	}
}
