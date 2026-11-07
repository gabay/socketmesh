package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/socketmesh/socketmesh/internal/config"
	"github.com/socketmesh/socketmesh/internal/discovery"
	"github.com/socketmesh/socketmesh/internal/proxy"
)

type runOption func(*runConfig)

type runConfig struct {
	readyChan  chan<- net.Addr
	httpClient *http.Client
	logWriter  io.Writer
	listener   net.Listener
}

func withReadyChan(ch chan<- net.Addr) runOption {
	return func(rc *runConfig) {
		rc.readyChan = ch
	}
}

func withHTTPClient(client *http.Client) runOption {
	return func(rc *runConfig) {
		rc.httpClient = client
	}
}

func withLogWriter(w io.Writer) runOption {
	return func(rc *runConfig) {
		rc.logWriter = w
	}
}

func withListener(ln net.Listener) runOption {
	return func(rc *runConfig) {
		rc.listener = ln
	}
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func healthcheckURL(listenAddr string) string {
	addr := strings.TrimSpace(listenAddr)
	if addr == "" {
		addr = ":2375"
	}

	var host, port string
	if _, err := strconv.Atoi(addr); err == nil {
		host = "127.0.0.1"
		port = addr
	} else if strings.HasPrefix(addr, ":") {
		host = "127.0.0.1"
		port = strings.TrimPrefix(addr, ":")
	} else {
		h, p, err := net.SplitHostPort(addr)
		if err == nil {
			host = h
			port = p
		} else {
			host = addr
			port = discovery.DefaultDockerPort
		}
	}

	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}

	return fmt.Sprintf("http://%s/_ping", net.JoinHostPort(host, port))
}

func runHealthcheck(ctx context.Context, listenAddr string, client *http.Client) error {
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	url := healthcheckURL(listenAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create healthcheck request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck request failed to %s: %w", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 500 {
		return fmt.Errorf("healthcheck returned server error status %d", resp.StatusCode)
	}
	return nil
}

func run(ctx context.Context, args []string, getenv func(string) string, opts ...runOption) error {
	var rc runConfig
	for _, opt := range opts {
		opt(&rc)
	}

	cfg, err := config.Parse(args, getenv)
	if err != nil {
		return err
	}

	if cfg.HealthcheckMode {
		hcCtx, hcCancel := context.WithTimeout(ctx, 3*time.Second)
		defer hcCancel()
		return runHealthcheck(hcCtx, cfg.ListenAddr, rc.httpClient)
	}

	logOut := io.Writer(os.Stderr)
	if rc.logWriter != nil {
		logOut = rc.logWriter
	}
	logger := slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	if cfg.WarnWriteEnvSet {
		logger.Warn("Write/mutating environment variables detected; SocketMesh is strictly read-only by design and will block all write requests")
	}

	mgr, err := discovery.NewManager(cfg, discovery.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("failed to initialize discovery manager: %w", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	mgr.Start(runCtx)

	p := proxy.NewProxy(cfg, mgr, proxy.WithLogger(logger))

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           p,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln := rc.listener
	if ln == nil {
		var listenErr error
		ln, listenErr = net.Listen("tcp", srv.Addr)
		if listenErr != nil {
			runCancel()
			mgr.Wait()
			return fmt.Errorf("failed to listen on %s: %w", srv.Addr, listenErr)
		}
	}
	defer ln.Close()

	logger.Info("SocketMesh server listening", "addr", ln.Addr().String())

	if rc.readyChan != nil {
		rc.readyChan <- ln.Addr()
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down SocketMesh server...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown failed", "error", err)
		}
		runCancel()
		mgr.Wait()
		logger.Info("SocketMesh server stopped")
		return nil
	case err := <-serverErr:
		runCancel()
		mgr.Wait()
		return err
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "socketmesh: %v\n", err)
		os.Exit(1)
	}
}
