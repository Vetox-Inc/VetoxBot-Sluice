package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/memberlist"
)

const maxClusterLeaveTime = 5 * time.Second

// ClusterConfig enables authenticated gossip and mutually authenticated peer routing.
type ClusterConfig struct {
	KnownMembers []string
	BindAddress  string
	Port         int
	PeerPort     int
	MaxNodes     int
	NodeName     string
	Secret       string
	PeerTLS      *tls.Config
}

type clusterHandle interface {
	Leave(time.Duration) error
	Shutdown() error
	Members() []*memberlist.Node
	LocalNode() *memberlist.Node
}

type clusterMember struct {
	name    string
	address string
	hash    uint64
}

func newClusterMember(name, address string) clusterMember {
	digest := sha256.Sum256([]byte(name))
	return clusterMember{name: name, address: address, hash: binary.BigEndian.Uint64(digest[:8])}
}

type routeTable struct {
	members   []clusterMember
	localAddr string
}

type clusterDelegate struct {
	peerPort string
}

func (d clusterDelegate) NodeMeta(int) []byte           { return []byte(d.peerPort) }
func (clusterDelegate) NotifyMsg([]byte)                {}
func (clusterDelegate) GetBroadcasts(int, int) [][]byte { return nil }
func (clusterDelegate) LocalState(bool) []byte          { return nil }
func (clusterDelegate) MergeRemoteState([]byte, bool)   {}

type clusterEvents struct {
	proxy *Proxy
}

func (e clusterEvents) NotifyJoin(node *memberlist.Node) {
	logger.WithField("node", node.Name).Info("Cluster node joined")
	go e.proxy.reindexMembers()
}

func (e clusterEvents) NotifyLeave(node *memberlist.Node) {
	logger.WithField("node", node.Name).Info("Cluster node left")
	go e.proxy.reindexMembers()
}

func (e clusterEvents) NotifyUpdate(node *memberlist.Node) {
	logger.WithField("node", node.Name).Info("Cluster node updated")
	go e.proxy.reindexMembers()
}

func (p *Proxy) JoinCluster(config ClusterConfig) error {
	if config.Port < 1 || config.Port > 65535 || config.PeerPort < 1 || config.PeerPort > 65535 {
		return fmt.Errorf("cluster and peer ports must be between 1 and 65535")
	}
	if config.BindAddress != "" && net.ParseIP(config.BindAddress) == nil {
		return fmt.Errorf("cluster BIND_IP must be an IP address")
	}
	if config.MaxNodes < 1 || config.MaxNodes > InvalidRequestSafetyLimit {
		return fmt.Errorf("cluster max nodes must be between 1 and %d", InvalidRequestSafetyLimit)
	}
	secretKey, err := clusterSecretKey(config.Secret)
	if err != nil {
		return err
	}
	peerTransport, err := newPeerHTTPTransport(config.PeerTLS)
	if err != nil {
		return err
	}

	p.clusterMu.Lock()
	select {
	case <-p.ctx.Done():
		p.clusterMu.Unlock()
		peerTransport.CloseIdleConnections()
		return fmt.Errorf("proxy is closed")
	default:
	}
	if p.cluster != nil || p.clusterJoining {
		p.clusterMu.Unlock()
		peerTransport.CloseIdleConnections()
		return fmt.Errorf("cluster is already initialized or initializing")
	}
	p.clusterJoining = true
	joinDone := make(chan struct{})
	p.clusterJoinDone = joinDone
	p.clusterMu.Unlock()

	installed := false
	var cluster *memberlist.Memberlist
	defer func() {
		if !installed {
			if cluster != nil {
				_ = cluster.Shutdown()
			}
			peerTransport.CloseIdleConnections()
		}
		p.clusterMu.Lock()
		if p.clusterJoinDone == joinDone {
			p.clusterJoining = false
			p.clusterJoinDone = nil
			close(joinDone)
		}
		p.clusterMu.Unlock()
	}()

	memberConfig := memberlist.DefaultLANConfig()
	if config.BindAddress != "" {
		memberConfig.BindAddr = config.BindAddress
	}
	memberConfig.BindPort = config.Port
	memberConfig.AdvertisePort = config.Port
	memberConfig.SecretKey = secretKey
	memberConfig.Delegate = clusterDelegate{peerPort: strconv.Itoa(config.PeerPort)}
	memberConfig.Events = clusterEvents{proxy: p}
	if config.NodeName != "" {
		memberConfig.Name = config.NodeName
	}

	cluster, err = memberlist.Create(memberConfig)
	if err != nil {
		return fmt.Errorf("create memberlist: %w", err)
	}
	if err := validatePeerCertificateAddress(config.PeerTLS, cluster.LocalNode().Addr); err != nil {
		return err
	}

	if err := joinKnownMembers(config.KnownMembers, cluster.Join); err != nil {
		return err
	}
	if members := len(cluster.Members()); members > config.MaxNodes {
		_ = cluster.Leave(maxClusterLeaveTime)
		return fmt.Errorf("cluster has %d members, exceeding CLUSTER_MAX_NODES=%d", members, config.MaxNodes)
	}

	// Installation is atomic with respect to Close. Close cancels p.ctx before
	// waiting on joinDone, so a candidate created during shutdown is cleaned up
	// instead of becoming an unowned live memberlist.
	p.clusterMu.Lock()
	select {
	case <-p.ctx.Done():
		p.clusterMu.Unlock()
		return fmt.Errorf("proxy is closed")
	default:
	}
	p.cluster = cluster
	p.peerTransport = peerTransport
	p.maxClusterNodes = config.MaxNodes
	p.localAddr = net.JoinHostPort(cluster.LocalNode().Addr.String(), strconv.Itoa(config.PeerPort))
	p.clusterJoining = false
	p.clusterJoinDone = nil
	close(joinDone)
	p.clusterMu.Unlock()
	installed = true
	p.reindexMembers()
	return nil
}

func validatePeerCertificateAddress(config *tls.Config, address net.IP) error {
	if config == nil || len(config.Certificates) == 0 {
		return fmt.Errorf("cluster peer TLS requires a static certificate")
	}
	certificate := &config.Certificates[0]
	leaf := certificate.Leaf
	if leaf == nil {
		if len(certificate.Certificate) == 0 {
			return fmt.Errorf("cluster peer TLS certificate is empty")
		}
		parsed, err := x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return fmt.Errorf("parse cluster peer TLS certificate: %w", err)
		}
		leaf = parsed
	}
	if err := leaf.VerifyHostname(address.String()); err != nil {
		return fmt.Errorf("cluster certificate is not valid for advertised IP %s: %w", address, err)
	}
	return nil
}

func joinKnownMembers(members []string, join func([]string) (int, error)) error {
	if len(members) == 0 {
		return nil
	}
	if _, err := join(members); err != nil {
		return fmt.Errorf("join configured cluster members: %w", err)
	}
	return nil
}

func clusterSecretKey(secret string) ([]byte, error) {
	if utf8.RuneCountInString(secret) < 32 {
		return nil, fmt.Errorf("CLUSTER_SECRET must contain at least 32 characters")
	}
	digest := sha256.Sum256([]byte(secret))
	return digest[:], nil
}

func (p *Proxy) reindexMembers() {
	p.clusterIndexMu.Lock()
	defer p.clusterIndexMu.Unlock()
	p.clusterMu.RLock()
	cluster, localAddr, maxNodes := p.cluster, p.localAddr, p.maxClusterNodes
	p.clusterMu.RUnlock()
	if cluster == nil {
		return
	}

	table := &routeTable{localAddr: localAddr}
	members := cluster.Members()
	if len(members) > maxNodes {
		p.clusterOverCapacity.Store(true)
		p.routes.Store(nil)
		logger.WithField("members", len(members)).WithField("maximum", maxNodes).Error("Cluster exceeds configured node capacity; refusing Discord traffic")
		return
	}
	p.clusterOverCapacity.Store(false)
	for _, node := range members {
		port := string(node.Meta)
		parsedPort, err := strconv.Atoi(port)
		if err != nil || parsedPort < 1 || parsedPort > 65535 {
			logger.WithField("node", node.Name).Warn("Ignoring cluster node with invalid peer port metadata")
			continue
		}
		table.members = append(table.members, newClusterMember(node.Name, net.JoinHostPort(node.Addr.String(), port)))
	}
	sort.Slice(table.members, func(i, j int) bool { return table.members[i].name < table.members[j].name })
	p.routes.Store(table)
}

func (p *Proxy) calculateRoute(key uint64) string {
	table := p.routes.Load()
	if table == nil || len(table.members) == 0 {
		return ""
	}
	member := rendezvousMember(key, table.members)
	if member.address == table.localAddr {
		return ""
	}
	return member.address
}

func rendezvousMember(key uint64, members []clusterMember) clusterMember {
	selected, bestScore := members[0], uint64(0)
	for index, member := range members {
		score := mix64(key ^ member.hash)
		if index == 0 || score > bestScore {
			selected, bestScore = member, score
		}
	}
	return selected
}

func mix64(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}

func (p *Proxy) closeCluster(ctx context.Context) error {
	var cluster clusterHandle
	var peerTransport *http.Transport
	for {
		p.clusterMu.Lock()
		if joinDone := p.clusterJoinDone; joinDone != nil {
			p.clusterMu.Unlock()
			select {
			case <-joinDone:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		cluster = p.cluster
		peerTransport = p.peerTransport
		p.cluster = nil
		p.peerTransport = nil
		p.maxClusterNodes = 0
		p.localAddr = ""
		p.clusterMu.Unlock()
		break
	}
	p.routes.Store(nil)
	p.clusterOverCapacity.Store(false)
	if peerTransport != nil {
		peerTransport.CloseIdleConnections()
	}
	if cluster == nil {
		return nil
	}

	leaveTime := maxClusterLeaveTime
	if ctx.Err() != nil {
		leaveTime = 0
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < leaveTime {
			leaveTime = remaining
		}
	}
	var leaveErr error
	if leaveTime > 0 {
		leaveErr = cluster.Leave(leaveTime)
	}
	return errors.Join(leaveErr, cluster.Shutdown())
}
