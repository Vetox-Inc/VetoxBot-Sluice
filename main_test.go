package main

import (
	"log/slog"
	"testing"
)

func TestParseLogLevelAcceptsNirnLevelNames(t *testing.T) {
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
