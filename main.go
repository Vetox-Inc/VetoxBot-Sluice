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
	"sync"
	"syscall"
	"time"

	"github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy"
	"github.com/joho/godotenv"
)

const (
	// drainTimeout covers a request under the default QUEUE_TIMEOUT and REQUEST_TIMEOUT.
	drainTimeout         = 15 * time.Second
	shutdownTimeout      = 5 * time.Second
	cleanupTimeout       = 5 * time.Second
	repeatedSignalWindow = time.Second
	configReferenceURL   = "https://github.com/Vetox-Inc/VetoxBot-Sluice/blob/master/CONFIG.md"
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
	checkOnly := flag.Bool("check", false, "check the configuration and exit")
	flag.Usage = printUsage
	flag.Parse()
	if *showVersion {
		fmt.Println("sluice", proxy.Version)
		return
	}
	if *checkOnly {
		if err := checkConfiguration(); err != nil {
			// Through the logger, which redacts: an error can quote a setting.
			logger.Error("The configuration is not valid", "error", err)
			os.Exit(1)
		}
		fmt.Println("sluice: the configuration is valid")
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
	fmt.Fprintf(&usage, "Sluice %s, a Discord REST rate-limit proxy.\n\nUsage: sluice [--version | --check]\n\n", proxy.Version)
	usage.WriteString("  --version  print the version and exit\n")
	usage.WriteString("  --check    check the configuration, as a start would, and exit without listening\n\n")
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

// checkConfiguration reads the configuration as a start does and stops short of the network, so
// a mistake shows before a restart takes the running proxy down. It opens no port, resolves no
// CLUSTER_DNS and joins no cluster: what only those can refuse still shows at the start.
func checkConfiguration() error {
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
	warnAboutSettings(config)
	if err := config.proxy.Validate(); err != nil {
		return fmt.Errorf("configure proxy: %w", err)
	}
	if config.clusteringEnabled() {
		if err := clusterConfig(config, nil).Validate(); err != nil {
			return fmt.Errorf("initialize cluster: %w", err)
		}
	}
	return nil
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
	warnAboutSettings(config)

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
	requestLifetime := config.proxy.RequestLifetime()
	publicAddress := net.JoinHostPort(config.bindIP, fmt.Sprint(config.port))
	publicServer := newProxyServer(publicAddress, serverProxy.PublicHandler(), requestLifetime)
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
		logger.Info("Running as a single node")
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
		"upstream", config.proxy.DiscordURL,
		"disableHTTP2", config.proxy.DisableHTTP2,
		"queueTimeout", config.proxy.QueueTimeout.String(),
		"requestTimeout", config.proxy.UpstreamTimeout.String(),
		"globalOverrides", len(splitNonempty(config.proxy.GlobalOverrides)),
		"botWideRoutes", len(splitNonempty(config.proxy.BotWideRoutes)),
		"clientAuth", config.proxy.ClientAuthSecret != "",
		"stateFile", config.proxy.StateFile,
	)

	signalContext, stopSignals := signal.NotifyContext(context.Background(), stopSignalsFor(signal.Ignored)...)
	defer stopSignals()
	select {
	case <-signalContext.Done():
		// A second signal stops the process at once instead of waiting for the drain. One that
		// follows the first within repeatedSignalWindow is the same stop arriving twice, as it
		// does when a launcher forwards a signal its whole process group was sent.
		time.AfterFunc(repeatedSignalWindow, stopSignals)
		logger.Info("Shutdown signal received; draining requests in flight", "timeout", drainTimeout.String())
	case err := <-serveErrors:
		return err
	}
	err = shutdownGracefully(serverProxy, servers, drainTimeout)
	logger.Info("Proxy stopped")
	return err
}

// stopSignalsFor lists the signals that start a graceful stop. A hangup is one of them, unless
// the process was started to ignore it, as nohup does: asking for it would undo that.
func stopSignalsFor(ignored func(os.Signal) bool) []os.Signal {
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	if !ignored(syscall.SIGHUP) {
		signals = append(signals, syscall.SIGHUP)
	}
	return signals
}

// shutdownGracefully lets admitted requests finish: the proxy fails its health checks and leaves
// its cluster, the listeners close, requests in flight get until drain to complete, and Close
// cancels whatever remains.
func shutdownGracefully(serverProxy *proxy.Proxy, servers []runningServer, drain time.Duration) error {
	shutdownServers := func(ctx context.Context) error {
		errs := make([]error, len(servers))
		var shutdowns sync.WaitGroup
		for index, running := range servers {
			shutdowns.Go(func() {
				if err := running.server.Shutdown(ctx); err != nil {
					errs[index] = fmt.Errorf("shutdown %s server: %w", running.name, err)
				}
			})
		}
		shutdowns.Wait()
		return errors.Join(errs...)
	}

	drainContext, cancelDrain := context.WithTimeout(context.Background(), drain)
	defer cancelDrain()
	var leaveErr error
	if err := serverProxy.Drain(drainContext); err != nil {
		leaveErr = fmt.Errorf("leave cluster: %w", err)
	}
	if err := shutdownServers(drainContext); err != nil {
		logger.Warn("Cancelling requests still running after the drain", "error", err)
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelClose()
	var closeErr error
	if err := serverProxy.Close(closeContext); err != nil {
		closeErr = fmt.Errorf("close proxy: %w", err)
	}
	return errors.Join(leaveErr, closeErr, shutdownServers(closeContext))
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

// parseLogLevel also takes trace, fatal and panic, which older configurations use and slog lacks.
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

// warnAboutSettings names the settings that are accepted and will not do what they seem to.
func warnAboutSettings(config appConfig) {
	for _, setting := range obsoleteSettings {
		if os.Getenv(setting.name) != "" {
			logger.Warn("Ignoring obsolete setting", "setting", setting.name, "advice", setting.advice)
		}
	}
	for _, warning := range config.warnings {
		logger.Warn(warning)
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
	if err := serverProxy.JoinCluster(clusterConfig(config, knownMembers)); err != nil {
		return fmt.Errorf("initialize cluster: %w", err)
	}
	return nil
}

func clusterConfig(config appConfig, knownMembers []string) proxy.ClusterConfig {
	return proxy.ClusterConfig{
		KnownMembers:     knownMembers,
		BindAddress:      config.bindIP,
		AdvertiseAddress: config.clusterAdvertiseAddr,
		Port:             config.clusterPort,
		PeerPort:         config.clusterPeerPort,
		MaxNodes:         config.clusterMaxNodes,
		NodeName:         config.nodeName,
		Secret:           config.clusterSecret,
		PeerTLS:          config.clusterClientTLS,
	}
}
