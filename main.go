package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy"
	"github.com/joho/godotenv"
)

const (
	shutdownTimeout    = 20 * time.Second
	cleanupTimeout     = 5 * time.Second
	configReferenceURL = "https://github.com/Vetox-Inc/VetoxBot-Sluice/blob/master/CONFIG.md"
)

var logger = proxy.NewLogger(os.Stderr, slog.LevelInfo, "text")

type runningServer struct {
	name     string
	server   *http.Server
	listener net.Listener
	tls      bool
}

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Usage = printUsage
	flag.Parse()
	if *showVersion {
		fmt.Println("sluice", proxy.Version)
		return
	}
	if err := run(); err != nil {
		logger.Error("Proxy stopped", "error", err)
		os.Exit(1)
	}
}

func printUsage() {
	output := flag.CommandLine.Output()
	var usage strings.Builder
	fmt.Fprintf(&usage, "Sluice %s, a Discord REST rate-limit proxy.\n\nUsage: sluice [--version]\n\n", proxy.Version)
	usage.WriteString("Configuration comes from environment variables and an optional .env file:\n")
	for _, group := range settingGroups {
		fmt.Fprintf(&usage, "\n  %s\n", group.title)
		for _, name := range group.names {
			fmt.Fprintf(&usage, "    %s\n", name)
		}
	}
	fmt.Fprintf(&usage, "\nDefaults and details: %s\n", configReferenceURL)
	_, _ = io.WriteString(output, usage.String())
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
	proxy.ConfigureMetrics(config.metricsNamespace)
	warnObsoleteSettings()

	serverProxy, err := proxy.New(config.proxy)
	if err != nil {
		return fmt.Errorf("configure proxy: %w", err)
	}
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
	logger.Info("Proxy started",
		"version", proxy.Version,
		"address", publicAddress,
		"disableHTTP2", config.proxy.DisableHTTP2,
		"queueTimeout", config.proxy.QueueTimeout.String(),
		"requestTimeout", config.proxy.UpstreamTimeout.String(),
	)

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
	err := godotenv.Load()
	var pathError *fs.PathError
	switch {
	case err == nil || errors.Is(err, os.ErrNotExist):
		return nil
	case errors.As(err, &pathError):
		return fmt.Errorf("load .env: %w", err)
	default:
		// The parser's message quotes the rest of the file, secrets included.
		return errors.New("load .env: the file is malformed; check its quoting and variable names")
	}
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
	level, err := parseLogLevel(envString("LOG_LEVEL", "info"))
	if err != nil {
		return err
	}
	format := strings.ToLower(envString("LOG_FORMAT", "text"))
	if format != "text" && format != "json" {
		return fmt.Errorf("LOG_FORMAT must be text or json")
	}
	logger = proxy.NewLogger(os.Stderr, level, format)
	proxy.SetLogger(logger)
	return nil
}

// parseLogLevel accepts nirn-proxy's logrus level names; slog has no trace, fatal or panic.
func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "trace", "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "fatal", "panic":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("LOG_LEVEL must be trace, debug, info, warn, error, fatal or panic")
}

func warnObsoleteSettings() {
	for _, setting := range obsoleteSettings {
		if os.Getenv(setting.name) != "" {
			logger.Warn("Ignoring obsolete setting", "setting", setting.name, "advice", setting.advice)
		}
	}
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
		KnownMembers:     knownMembers,
		BindAddress:      config.bindIP,
		AdvertiseAddress: config.clusterAdvertiseAddr,
		Port:             config.clusterPort,
		PeerPort:         config.clusterPeerPort,
		MaxNodes:         config.clusterMaxNodes,
		NodeName:         config.nodeName,
		Secret:           config.clusterSecret,
		PeerTLS:          config.clusterClientTLS,
	}); err != nil {
		return fmt.Errorf("initialize cluster: %w", err)
	}
	return nil
}
