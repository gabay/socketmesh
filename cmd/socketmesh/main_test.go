package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"Debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"WARN", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"", slog.LevelInfo},
		{"unknown", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseLogLevel(tt.input)
			if got != tt.expected {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestHealthcheckURL(t *testing.T) {
	tests := []struct {
		listen   string
		expected string
	}{
		{":2375", "http://127.0.0.1:2375/_ping"},
		{"0.0.0.0:2375", "http://127.0.0.1:2375/_ping"},
		{"[::]:2375", "http://127.0.0.1:2375/_ping"},
		{"127.0.0.1:8080", "http://127.0.0.1:8080/_ping"},
		{"192.168.1.10:2375", "http://192.168.1.10:2375/_ping"},
		{"8080", "http://127.0.0.1:8080/_ping"},
		{"", "http://127.0.0.1:2375/_ping"},
		{":8000", "http://127.0.0.1:8000/_ping"},
		{"localhost:2375", "http://localhost:2375/_ping"},
	}

	for _, tt := range tests {
		t.Run(tt.listen, func(t *testing.T) {
			got := healthcheckURL(tt.listen)
			if got != tt.expected {
				t.Errorf("healthcheckURL(%q) = %q, want %q", tt.listen, got, tt.expected)
			}
		})
	}
}

func TestHealthcheckMode_LiveServer(t *testing.T) {
	t.Run("200 OK", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/_ping" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer ts.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := run(ctx, []string{"--healthcheck", "--listen", ts.Listener.Addr().String()}, nil)
		if err != nil {
			t.Fatalf("expected healthcheck to succeed on 200 OK, got: %v", err)
		}
	})

	t.Run("403 Forbidden", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/_ping" {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("Forbidden"))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer ts.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := run(ctx, []string{"--healthcheck", "--listen", ts.Listener.Addr().String()}, nil)
		if err != nil {
			t.Fatalf("expected healthcheck to succeed on 403 Forbidden (< 500), got: %v", err)
		}
	})

	t.Run("500 Internal Server Error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer ts.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := run(ctx, []string{"--healthcheck", "--listen", ts.Listener.Addr().String()}, nil)
		if err == nil {
			t.Fatal("expected healthcheck to fail on 500 Internal Server Error, got nil")
		}
	})

	t.Run("Connection Refused", func(t *testing.T) {
		// Pick an unused port
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = run(ctx, []string{"--healthcheck", "--listen", addr}, nil)
		if err == nil {
			t.Fatal("expected healthcheck to fail on closed port, got nil")
		}
	})
}

func TestRun_Lifecycle(t *testing.T) {
	// 1. Mock downstream Docker daemon
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_ping" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer downstream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyCh := make(chan net.Addr, 1)
	errCh := make(chan error, 1)

	var logBuf bytes.Buffer
	go func() {
		args := []string{
			"--listen", "127.0.0.1:0",
			"--hosts", downstream.Listener.Addr().String(),
			"--health-interval", "50ms",
		}
		errCh <- run(ctx, args, func(string) string { return "" },
			withReadyChan(readyCh),
			withLogWriter(&logBuf),
		)
	}()

	var serverAddr net.Addr
	select {
	case serverAddr = <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server ready signal")
	}

	// Make request to the running proxy
	resp, err := http.Get("http://" + serverAddr.String() + "/_ping")
	if err != nil {
		t.Fatalf("failed to request /_ping: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from proxy /_ping, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Trigger graceful shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected run() to return nil on graceful shutdown, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for run() to exit after cancellation")
	}
}

func TestRun_StartupWarnings(t *testing.T) {
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer downstream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	readyCh := make(chan net.Addr, 1)
	errCh := make(chan error, 1)
	var logBuf bytes.Buffer

	env := map[string]string{
		"POST":   "1",
		"EVENTS": "1",
		"HOSTS":  downstream.Listener.Addr().String(),
	}
	getenv := func(key string) string {
		return env[key]
	}

	go func() {
		args := []string{"--listen", "127.0.0.1:0"}
		errCh <- run(ctx, args, getenv,
			withReadyChan(readyCh),
			withLogWriter(&logBuf),
		)
	}()

	select {
	case <-readyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server ready")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for run exit")
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "Write/mutating environment variables detected") {
		t.Errorf("expected write env warning in log output, got:\n%s", logOutput)
	}
	if strings.Contains(logOutput, "EVENTS=1 is set, but /events is not implemented") {
		t.Errorf("unexpected EVENTS warning in log output, got:\n%s", logOutput)
	}
}

func TestRun_ConfigErrors(t *testing.T) {
	ctx := context.Background()

	// Missing hosts/swarm service without healthcheck mode
	err := run(ctx, []string{}, func(string) string { return "" })
	if err == nil {
		t.Fatal("expected error for missing downstream hosts, got nil")
	}

	// Invalid CLI flag
	err = run(ctx, []string{"--nonexistent-flag"}, func(string) string { return "" })
	if err == nil {
		t.Fatal("expected error for nonexistent CLI flag, got nil")
	}

	// Invalid listen address
	err = run(ctx, []string{"--hosts", "127.0.0.1:2375", "--listen", "999.999.999.999:99999"}, func(string) string { return "" })
	if err == nil {
		t.Fatal("expected error for invalid listen address, got nil")
	}
}
