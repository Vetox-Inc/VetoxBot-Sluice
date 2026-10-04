package main

import (
	"log/slog"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestMalformedDotEnvDoesNotEchoItsContents(t *testing.T) {
	t.Chdir(t.TempDir())
	const secret = "not-a-real-cluster-secret"
	if err := os.WriteFile(".env", []byte("BROKEN LINE\nCLUSTER_SECRET="+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := loadDotEnv()
	if err == nil {
		t.Fatal("a malformed .env was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("the error echoes the file: %v", err)
	}
}

func TestCheckRefusesWhatAStartWouldRefuse(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("STATE_FILE", "")
	if err := checkConfiguration(); err != nil {
		t.Fatalf("the defaults were refused: %v", err)
	}
	for setting, value := range map[string]string{
		"PORT":                    "70000",
		"LOG_FORMAT":              "xml",
		"BOT_RATELIMIT_OVERRIDES": "not-an-override",
		"BOT_WIDE_ROUTES":         "vanity-url",
		// No resolver is asked about a name with a space in it, so this fails offline too.
		"OUTBOUND_IP": "not an address",
	} {
		t.Run(setting, func(t *testing.T) {
			t.Setenv(setting, value)
			err := checkConfiguration()
			if err == nil {
				t.Fatalf("%s=%s was accepted", setting, value)
			}
			if !strings.Contains(err.Error(), setting) {
				t.Errorf("the error does not name %s: %v", setting, err)
			}
		})
	}
}

func TestCheckRefusesAClusterAStartWouldRefuse(t *testing.T) {
	t.Chdir(t.TempDir())
	clearClusterEnvironment(t)
	caFile, certFile, keyFile := writeClusterTestCertificate(t)
	for setting, value := range map[string]string{
		"STATE_FILE": "", "BIND_IP": "127.0.0.1", "CLUSTER_MEMBERS": "127.0.0.1", "CLUSTER_SECRET": strings.Repeat("s", 32),
		"CLUSTER_CA_FILE": caFile, "CLUSTER_CERT_FILE": certFile, "CLUSTER_KEY_FILE": keyFile,
	} {
		t.Setenv(setting, value)
	}
	if err := checkConfiguration(); err != nil {
		t.Fatalf("a valid cluster was refused: %v", err)
	}
	// A start refuses this when it joins the cluster, which a check never does.
	t.Setenv("BIND_IP", "localhost")
	if err := checkConfiguration(); err == nil || !strings.Contains(err.Error(), "BIND_IP") {
		t.Fatalf("a cluster bound to a hostname: error = %v, want one that names BIND_IP", err)
	}
}

func TestSettingsThatLookMistakenAreWarnedAbout(t *testing.T) {
	t.Setenv("STATE_FILE", "")
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(config.warnings) != 0 {
		t.Fatalf("the defaults raise warnings: %q", config.warnings)
	}

	t.Setenv("REQUEST_TIMEOUT", "30")
	t.Setenv("MAX_RETRY_CAPTURE_BYTES", "1024")
	config, err = loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	warnings := strings.Join(config.warnings, "\n")
	for _, setting := range []string{"REQUEST_TIMEOUT", "MAX_RETRY_CAPTURE_BYTES"} {
		if !strings.Contains(warnings, setting) {
			t.Errorf("no warning names %s:\n%s", setting, warnings)
		}
	}
	if strings.Contains(warnings, "QUEUE_TIMEOUT") {
		t.Errorf("the default QUEUE_TIMEOUT is warned about:\n%s", warnings)
	}
}

func TestStateFileWithoutACacheDirectoryIsOffAndSaysSo(t *testing.T) {
	// Setenv restores the variables afterwards; the test needs them gone.
	for _, name := range []string{"STATE_FILE", "HOME", "XDG_CACHE_HOME"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.UserCacheDir(); err == nil {
		t.Skip("this platform finds a cache directory without HOME")
	}
	path, warning := stateFilePath(8080)
	if path != "" || !strings.Contains(warning, "STATE_FILE") {
		t.Fatalf("path %q, warning %q; want no file and a warning that names STATE_FILE", path, warning)
	}
	t.Setenv("STATE_FILE", "")
	if path, warning := stateFilePath(8080); path != "" || warning != "" {
		t.Fatalf("STATE_FILE set empty gave path %q and warning %q, want neither", path, warning)
	}
}

func TestHangupStopsSluiceUnlessItWasStartedToIgnoreIt(t *testing.T) {
	asked := func(ignored bool) bool {
		for _, signal := range stopSignalsFor(func(os.Signal) bool { return ignored }) {
			if signal == syscall.SIGHUP {
				return true
			}
		}
		return false
	}
	if !asked(false) {
		t.Error("a hangup is not handled")
	}
	if asked(true) {
		t.Error("a hangup is handled although the process was started to ignore it")
	}
}

func TestParseLogLevelAcceptsLegacyNames(t *testing.T) {
	for value, want := range map[string]slog.Level{
		"trace": slog.LevelDebug, "debug": slog.LevelDebug, "INFO": slog.LevelInfo,
		"warn": slog.LevelWarn, "warning": slog.LevelWarn,
		"error": slog.LevelError, "fatal": slog.LevelError, "panic": slog.LevelError,
	} {
		got, err := parseLogLevel(value)
		if err != nil || got != want {
			t.Errorf("parseLogLevel(%q) = %v, %v; want %v", value, got, err, want)
		}
	}
	if _, err := parseLogLevel("verbose"); err == nil {
		t.Fatal("unknown LOG_LEVEL was accepted")
	}
}

func TestConfigureLoggerValidatesFormat(t *testing.T) {
	t.Setenv("LOG_LEVEL", "info")
	for _, format := range []string{"", "text", "JSON"} {
		t.Setenv("LOG_FORMAT", format)
		if err := configureLogger(); err != nil {
			t.Errorf("LOG_FORMAT=%q: %v", format, err)
		}
	}
	t.Setenv("LOG_FORMAT", "xml")
	if err := configureLogger(); err == nil {
		t.Fatal("LOG_FORMAT=xml was accepted")
	}
}
