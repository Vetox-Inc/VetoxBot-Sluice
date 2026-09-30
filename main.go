package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Melonly-Moderation/nirn-proxy/internal/proxy"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

const (
	shutdownTimeout = 20 * time.Second
	cleanupTimeout  = 5 * time.Second
)

var logger = logrus.New()

type runningServer struct {
	name     string
	server   *http.Server
	listener net.Listener
	tls      bool
}

func main() {
	if err := run(); err != nil {
		logger.WithError(err).Fatal("Proxy stopped")
	}
}

func run() error {
	if err := loadDotEnv(); err != nil {
		return err
	}
	if err := configureLogger(); err != nil {
		return err
	}
	config, err := loadConfig()
	if err != nil {
		return err
	}

	serverProxy, err := proxy.New(config.proxy)
	if err != nil {
		return fmt.Errorf("configure proxy: %w", err)
	}
	proxy.SetLogger(logger)
	var servers []runningServer
	defer func() {
		cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancelCleanup()
		_ = serverProxy.Close(cleanupContext)
		for _, running := range servers {
			_ = running.server.Shutdown(cleanupContext)
			_ = running.listener.Close()
		}
	}()
	requestLifetime := config.proxy.QueueTimeout + config.proxy.UpstreamTimeout + 5*time.Second
	publicAddress := net.JoinHostPort(config.bindIP, fmt.Sprint(config.port))
	publicServer := newProxyServer(publicAddress, serverProxy, requestLifetime)
	publicListener, err := net.Listen("tcp", publicAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", publicAddress, err)
	}
	servers = append(servers, runningServer{name: "proxy", server: publicServer, listener: publicListener})

	if config.enableMetrics {
		address := net.JoinHostPort(config.bindIP, fmt.Sprint(config.metricsPort))
		server := proxy.NewMetricsServer(address)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return fmt.Errorf("listen for metrics on %s: %w", address, err)
		}
		servers = append(servers, runningServer{name: "metrics", server: server, listener: listener})
	}
	if config.enablePprof {
		address := net.JoinHostPort(config.bindIP, fmt.Sprint(config.pprofPort))
		server := proxy.NewProfileServer(address)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return fmt.Errorf("listen for pprof on %s: %w", address, err)
		}
		servers = append(servers, runningServer{name: "pprof", server: server, listener: listener})
	}

	clustered := config.clusteringEnabled()
	if clustered {
		address := net.JoinHostPort(config.bindIP, fmt.Sprint(config.clusterPeerPort))
		server := newProxyServer(address, serverProxy, requestLifetime)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return fmt.Errorf("listen for cluster peers on %s: %w", address, err)
		}
		server.TLSConfig = config.clusterServerTLS.Clone()
		servers = append(servers, runningServer{name: "cluster peer", server: server, listener: listener, tls: true})
	}

	serveErrors := make(chan error, len(servers))
	start := func(running runningServer) {
		go func() {
			var err error
			if running.tls {
				err = running.server.ServeTLS(running.listener, "", "")
			} else {
				err = running.server.Serve(running.listener)
			}
			if err == nil {
				err = fmt.Errorf("server stopped unexpectedly")
			}
			if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				serveErrors <- fmt.Errorf("%s server: %w", running.name, err)
			}
		}()
	}
	if clustered {
		// The final entry is the mTLS listener. Make it reachable before gossip
		// advertises this node's peer port.
		start(servers[len(servers)-1])
		if err := joinCluster(serverProxy, config); err != nil {
			return err
		}
		select {
		case err := <-serveErrors:
			return err
		default:
		}
	} else {
		logger.Info("Running in stand-alone mode")
	}
	last := len(servers)
	if clustered {
		last--
	}
	for _, running := range servers[:last] {
		start(running)
	}
	logger.WithFields(logrus.Fields{
		"address":        publicAddress,
		"disableHTTP2":   config.proxy.DisableHTTP2,
		"queueTimeout":   config.proxy.QueueTimeout,
		"requestTimeout": config.proxy.UpstreamTimeout,
	}).Info("Proxy started")

	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-signalContext.Done():
		logger.Info("Shutdown signal received")
	case err := <-serveErrors:
		return err
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	var shutdownErrors []error
	if err := serverProxy.Close(shutdownContext); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("close proxy: %w", err))
	}
	for _, running := range servers {
		if err := running.server.Shutdown(shutdownContext); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("shutdown %s server: %w", running.name, err))
		}
	}
	logger.Info("Proxy stopped")
	return errors.Join(shutdownErrors...)
}

func loadDotEnv() error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load .env: %w", err)
	}
	return nil
}

func newProxyServer(address string, handler http.Handler, requestLifetime time.Duration) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       requestLifetime,
		WriteTimeout:      requestLifetime,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
}

func configureLogger() error {
	level, err := logrus.ParseLevel(envString("LOG_LEVEL", "info"))
	if err != nil {
		return fmt.Errorf("parse LOG_LEVEL: %w", err)
	}
	logger.SetLevel(level)
	return nil
}

func joinCluster(serverProxy *proxy.Proxy, config appConfig) error {
	knownMembers := config.clusterMembers
	if len(knownMembers) == 0 {
		addresses, err := net.LookupIP(config.clusterDNS)
		if err != nil {
			return fmt.Errorf("resolve CLUSTER_DNS: %w", err)
		}
		if len(addresses) == 0 {
			return fmt.Errorf("CLUSTER_DNS returned no addresses")
		}
		for _, address := range addresses {
			knownMembers = append(knownMembers, net.JoinHostPort(address.String(), fmt.Sprint(config.clusterPort)))
		}
	}
	if err := serverProxy.JoinCluster(proxy.ClusterConfig{
		KnownMembers: knownMembers,
		BindAddress:  config.bindIP,
		Port:         config.clusterPort,
		PeerPort:     config.clusterPeerPort,
		MaxNodes:     config.clusterMaxNodes,
		NodeName:     config.nodeName,
		Secret:       config.clusterSecret,
		PeerTLS:      config.clusterClientTLS,
	}); err != nil {
		return fmt.Errorf("initialize cluster: %w", err)
	}
	return nil
}
