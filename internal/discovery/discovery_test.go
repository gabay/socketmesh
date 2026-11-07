package discovery

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/socketmesh/socketmesh/internal/config"
)

func TestNormalizeEndpoint_Valid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "bare ipv4 with port",
			input:    "10.0.0.1:2375",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "bare ipv4 without port",
			input:    "10.0.0.1",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "http scheme with port",
			input:    "http://10.0.0.1:2375",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "http scheme without port",
			input:    "http://10.0.0.1",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "tcp scheme with port",
			input:    "tcp://10.0.0.1:2375",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "tcp scheme without port",
			input:    "tcp://10.0.0.1",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "https scheme with port preserved",
			input:    "https://10.0.0.1:2376",
			expected: "https://10.0.0.1:2376",
		},
		{
			name:     "https scheme without port",
			input:    "https://10.0.0.1",
			expected: "https://10.0.0.1:2375",
		},
		{
			name:     "ipv6 with brackets and port",
			input:    "[::1]:2375",
			expected: "http://[::1]:2375",
		},
		{
			name:     "bare ipv6 without brackets or port",
			input:    "::1",
			expected: "http://[::1]:2375",
		},
		{
			name:     "ipv6 with brackets without port",
			input:    "[::1]",
			expected: "http://[::1]:2375",
		},
		{
			name:     "tcp ipv6 with brackets and port",
			input:    "tcp://[::1]:2375",
			expected: "http://[::1]:2375",
		},
		{
			name:     "https ipv6 with brackets and port",
			input:    "https://[::1]:2376",
			expected: "https://[::1]:2376",
		},
		{
			name:     "trailing single slash stripped",
			input:    "http://10.0.0.1:2375/",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "trailing multiple slashes stripped",
			input:    "http://10.0.0.1:2375///",
			expected: "http://10.0.0.1:2375",
		},
		{
			name:     "hostname without port",
			input:    "my-docker-host",
			expected: "http://my-docker-host:2375",
		},
		{
			name:     "hostname with custom port",
			input:    "my-docker-host:8080",
			expected: "http://my-docker-host:8080",
		},
		{
			name:     "tcp hostname with custom port",
			input:    "tcp://my-docker-host:8080",
			expected: "http://my-docker-host:8080",
		},
		{
			name:     "https hostname with custom port",
			input:    "https://my-docker-host:8443",
			expected: "https://my-docker-host:8443",
		},
		{
			name:     "whitespace trimmed",
			input:    "  10.0.0.1:2375  ",
			expected: "http://10.0.0.1:2375",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeEndpoint(tt.input)
			if err != nil {
				t.Fatalf("NormalizeEndpoint(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("NormalizeEndpoint(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNormalizeEndpoint_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"whitespace only", "   "},
		{"unsupported scheme ftp", "ftp://10.0.0.1:2375"},
		{"unsupported scheme unix", "unix:///var/run/docker.sock"},
		{"missing host after scheme", "http://"},
		{"missing host with port", ":2375"},
		{"invalid port non-numeric", "10.0.0.1:abc"},
		{"invalid port zero", "10.0.0.1:0"},
		{"invalid port negative", "10.0.0.1:-1"},
		{"invalid port out of range", "10.0.0.1:65536"},
		{"bracketed ipv6 missing closing bracket", "[::1:2375"},
		{"bracketed ipv6 with empty port", "[::1]:"},
		{"subpath not supported", "http://10.0.0.1:2375/v1.45"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeEndpoint(tt.input)
			if err == nil {
				t.Fatalf("NormalizeEndpoint(%q) expected error, got %q", tt.input, got)
			}
		})
	}
}

func TestSwarmDNSResolution(t *testing.T) {
	var mu sync.Mutex
	queries := make(map[string]int)

	mockLookup := func(ctx context.Context, host string) ([]string, error) {
		mu.Lock()
		queries[host]++
		mu.Unlock()

		switch host {
		case "tasks.socket-proxy":
			return []string{"10.0.0.2", "10.0.0.3"}, nil
		case "tasks.custom-proxy":
			return []string{"10.0.0.4"}, nil
		case "tasks.ipv6-proxy":
			return []string{"::1", "fd00::2"}, nil
		default:
			return nil, &net.DNSError{Err: "no such host", Name: host}
		}
	}

	cfg := &config.Config{
		SwarmServices: []string{
			"socket-proxy",                 // tests missing tasks. prefix
			"tasks.socket-proxy",           // tests existing tasks. prefix (must not become tasks.tasks.socket-proxy)
			"custom-proxy:9000",            // tests custom port
			"https://tasks.ipv6-proxy:2376", // tests https and ipv6 resolution
		},
	}

	mgr, err := NewManager(cfg, WithLookupHost(mockLookup))
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	ctx := context.Background()
	if err := mgr.RefreshDNS(ctx); err != nil {
		t.Fatalf("RefreshDNS error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify tasks. prefix handling
	if queries["tasks.socket-proxy"] == 0 {
		t.Errorf("expected query for tasks.socket-proxy, got none")
	}
	if queries["tasks.tasks.socket-proxy"] > 0 {
		t.Errorf("incorrect double-prefix query tasks.tasks.socket-proxy occurred")
	}
	if queries["tasks.custom-proxy"] == 0 {
		t.Errorf("expected query for tasks.custom-proxy, got none")
	}
	if queries["tasks.ipv6-proxy"] == 0 {
		t.Errorf("expected query for tasks.ipv6-proxy, got none")
	}

	endpoints := mgr.AllEndpoints()
	expected := []string{
		"http://10.0.0.2:2375",
		"http://10.0.0.3:2375",
		"http://10.0.0.4:9000",
		"https://[::1]:2376",
		"https://[fd00::2]:2376",
	}

	if len(endpoints) != len(expected) {
		t.Fatalf("expected %d endpoints, got %d: %v", len(expected), len(endpoints), endpoints)
	}

	for i, ep := range endpoints {
		if ep != expected[i] {
			t.Errorf("endpoint[%d] = %q, want %q", i, ep, expected[i])
		}
	}
}

func TestCombinedStaticAndSwarmEndpoints_Deduplication(t *testing.T) {
	mockLookup := func(ctx context.Context, host string) ([]string, error) {
		if host == "tasks.swarm-proxy" {
			// Returns 10.0.0.2 (which is also in static hosts) and 10.0.0.3
			return []string{"10.0.0.2", "10.0.0.3"}, nil
		}
		return nil, errors.New("host not found")
	}

	cfg := &config.Config{
		Hosts:         []string{"10.0.0.1:2375", "http://10.0.0.2:2375", "10.0.0.1"},
		SwarmServices: []string{"swarm-proxy"},
	}

	mgr, err := NewManager(cfg, WithLookupHost(mockLookup))
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	if err := mgr.RefreshDNS(context.Background()); err != nil {
		t.Fatalf("RefreshDNS error: %v", err)
	}

	endpoints := mgr.AllEndpoints()
	expected := []string{
		"http://10.0.0.1:2375",
		"http://10.0.0.2:2375",
		"http://10.0.0.3:2375",
	}

	if len(endpoints) != len(expected) {
		t.Fatalf("expected %d endpoints, got %d: %v", len(expected), len(endpoints), endpoints)
	}

	for i, ep := range endpoints {
		if ep != expected[i] {
			t.Errorf("endpoint[%d] = %q, want %q", i, ep, expected[i])
		}
	}
}

func TestSwarmDNSFailure_PreservesPreviousEndpoints(t *testing.T) {
	var failDNS atomic.Bool

	mockLookup := func(ctx context.Context, host string) ([]string, error) {
		if failDNS.Load() {
			return nil, errors.New("temporary DNS timeout")
		}
		return []string{"10.0.0.2", "10.0.0.3"}, nil
	}

	cfg := &config.Config{
		Hosts:         []string{"10.0.0.1:2375"},
		SwarmServices: []string{"socket-proxy"},
	}

	mgr, err := NewManager(cfg, WithLookupHost(mockLookup))
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	// 1. Initial successful DNS resolution
	if err := mgr.RefreshDNS(context.Background()); err != nil {
		t.Fatalf("RefreshDNS initial error: %v", err)
	}

	if len(mgr.AllEndpoints()) != 3 {
		t.Fatalf("expected 3 endpoints, got %d", len(mgr.AllEndpoints()))
	}

	// 2. DNS lookup now fails
	failDNS.Store(true)
	err = mgr.RefreshDNS(context.Background())
	if err == nil {
		t.Errorf("expected error from RefreshDNS when DNS fails, got nil")
	}

	// Previous endpoints must still be preserved, no crash, static hosts intact
	endpoints := mgr.AllEndpoints()
	expected := []string{
		"http://10.0.0.1:2375",
		"http://10.0.0.2:2375",
		"http://10.0.0.3:2375",
	}

	if len(endpoints) != len(expected) {
		t.Fatalf("expected 3 endpoints preserved after DNS failure, got %d: %v", len(endpoints), endpoints)
	}
	for i, ep := range endpoints {
		if ep != expected[i] {
			t.Errorf("endpoint[%d] = %q, want %q", i, ep, expected[i])
		}
	}
}

func TestHealthTracking_MarkUnhealthyAndFallback(t *testing.T) {
	cfg := &config.Config{
		Hosts: []string{"10.0.0.1:2375", "10.0.0.2:2375"},
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	ep1 := "http://10.0.0.1:2375"
	ep2 := "http://10.0.0.2:2375"

	// Initially all endpoints are healthy
	if !mgr.IsHealthy(ep1) || !mgr.IsHealthy(ep2) {
		t.Fatalf("expected endpoints to be initially healthy")
	}
	healthy := mgr.HealthyEndpoints()
	if len(healthy) != 2 {
		t.Fatalf("expected 2 healthy endpoints, got %d", len(healthy))
	}

	// Mark ep1 unhealthy
	mgr.MarkUnhealthy(ep1)
	if mgr.IsHealthy(ep1) {
		t.Errorf("expected ep1 to be unhealthy")
	}
	if !mgr.IsHealthy(ep2) {
		t.Errorf("expected ep2 to remain healthy")
	}

	healthy = mgr.HealthyEndpoints()
	if len(healthy) != 1 || healthy[0] != ep2 {
		t.Fatalf("expected only ep2 in healthy list, got %v", healthy)
	}

	// Mark ep2 unhealthy as well (ALL endpoints are now unhealthy)
	mgr.MarkUnhealthy(ep2)
	if mgr.IsHealthy(ep2) {
		t.Errorf("expected ep2 to be unhealthy")
	}

	// Fallback verification: when all endpoints are unhealthy, HealthyEndpoints falls back to returning all endpoints
	fallback := mgr.HealthyEndpoints()
	if len(fallback) != 2 {
		t.Fatalf("expected fallback to return all 2 endpoints, got %d: %v", len(fallback), fallback)
	}

	// Mark ep1 healthy again
	mgr.MarkHealthy(ep1)
	if !mgr.IsHealthy(ep1) {
		t.Errorf("expected ep1 to be healthy after MarkHealthy")
	}
	healthy = mgr.HealthyEndpoints()
	if len(healthy) != 1 || healthy[0] != ep1 {
		t.Fatalf("expected only ep1 in healthy list, got %v", healthy)
	}
}

func TestCheckHealth_WithServerRecovery(t *testing.T) {
	var statusCode atomic.Int32
	statusCode.Store(http.StatusServiceUnavailable) // Initially failing (503)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_ping" {
			w.WriteHeader(int(statusCode.Load()))
			_, _ = w.Write([]byte("OK\n"))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	cfg := &config.Config{
		Hosts: []string{server.URL},
	}

	mgr, err := NewManager(cfg, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	normalizedServerURL, err := NormalizeEndpoint(server.URL)
	if err != nil {
		t.Fatalf("NormalizeEndpoint(%q) error: %v", server.URL, err)
	}

	// 1. Initial check when server returns 503: marks unhealthy
	mgr.CheckHealth(context.Background())
	if mgr.IsHealthy(normalizedServerURL) {
		t.Fatalf("expected endpoint to be unhealthy when /_ping returns 503")
	}

	// 2. Server recovers and returns 200 OK
	statusCode.Store(http.StatusOK)
	mgr.CheckHealth(context.Background())
	if !mgr.IsHealthy(normalizedServerURL) {
		t.Fatalf("expected endpoint to recover and be healthy when /_ping returns 200")
	}

	// 3. Status < 500 (e.g. 403 Forbidden because downstream ACL denied /_ping) is still considered reachable
	statusCode.Store(http.StatusForbidden)
	mgr.CheckHealth(context.Background())
	if !mgr.IsHealthy(normalizedServerURL) {
		t.Fatalf("expected endpoint with status 403 (< 500) to remain healthy")
	}

	// 4. Server error (502) causes unhealthy
	statusCode.Store(http.StatusBadGateway)
	mgr.CheckHealth(context.Background())
	if mgr.IsHealthy(normalizedServerURL) {
		t.Fatalf("expected endpoint with status 502 to become unhealthy")
	}
}

func TestCheckHealth_ConnectionRefused(t *testing.T) {
	// Test with a closed server port
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := server.URL
	server.Close() // Close immediately so connection fails

	cfg := &config.Config{
		Hosts: []string{closedURL},
	}

	mgr, err := NewManager(cfg, WithTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	mgr.CheckHealth(context.Background())
	if mgr.IsHealthy(closedURL) {
		t.Fatalf("expected connection-refused endpoint to be marked unhealthy")
	}
}

func TestBackgroundStartAndStop(t *testing.T) {
	var dnsQueries atomic.Int32
	var pingRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_ping") {
			pingRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK\n"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Extract port from test server
	serverAddr := server.Listener.Addr().String()
	_, port, _ := net.SplitHostPort(serverAddr)

	mockLookup := func(ctx context.Context, host string) ([]string, error) {
		dnsQueries.Add(1)
		return []string{"127.0.0.1"}, nil
	}

	cfg := &config.Config{
		Hosts:          []string{server.URL},
		SwarmServices:  []string{"my-service:" + port},
		DNSRefresh:     20 * time.Millisecond,
		HealthInterval: 20 * time.Millisecond,
	}

	mgr, err := NewManager(cfg,
		WithLookupHost(mockLookup),
		WithHTTPClient(server.Client()),
	)
	if err != nil {
		t.Fatalf("NewManager error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mgr.Start(ctx)

	// Wait for background loops to execute at least twice
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if dnsQueries.Load() >= 2 && pingRequests.Load() >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if dnsQueries.Load() < 2 {
		t.Errorf("expected at least 2 DNS queries in background, got %d", dnsQueries.Load())
	}
	if pingRequests.Load() < 2 {
		t.Errorf("expected at least 2 health check pings in background, got %d", pingRequests.Load())
	}

	// Cancel context and ensure Wait cleanly shuts down without blocking
	cancel()

	done := make(chan struct{})
	go func() {
		mgr.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown verified
	case <-time.After(1 * time.Second):
		t.Fatalf("Manager did not shut down cleanly after context cancellation")
	}
}
