package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/socketmesh/socketmesh/internal/config"
)

// DefaultDockerPort is the standard Docker daemon TCP port.
const DefaultDockerPort = "2375"

// DefaultPingTimeout is the default timeout used when pinging downstream endpoints.
const DefaultPingTimeout = 3 * time.Second

// LookupHostFunc resolves a hostname to a list of IP addresses.
type LookupHostFunc func(ctx context.Context, host string) ([]string, error)

// NormalizeEndpoint normalizes a downstream Docker daemon endpoint into a clean base URL string.
// Accepts inputs such as "10.0.0.1:2375", "10.0.0.1", "http://10.0.0.1:2375", "tcp://10.0.0.1:2375",
// "https://10.0.0.1:2376", "[::1]:2375", "::1", "[::1]".
// Converts "tcp://" or scheme-less inputs to "http://", preserves "https://", adds default port 2375
// if no port is specified, strips trailing slashes, and returns a clean URL like "http://10.0.0.1:2375".
func NormalizeEndpoint(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("empty endpoint")
	}

	scheme := "http"
	if idx := strings.Index(trimmed, "://"); idx != -1 {
		rawScheme := strings.ToLower(trimmed[:idx])
		trimmed = trimmed[idx+3:]
		switch rawScheme {
		case "tcp", "http":
			scheme = "http"
		case "https":
			scheme = "https"
		default:
			return "", fmt.Errorf("unsupported scheme %q in endpoint %q", rawScheme, raw)
		}
	}

	// Strip query parameters and fragments
	if idx := strings.IndexAny(trimmed, "?#"); idx != -1 {
		trimmed = trimmed[:idx]
	}

	// Strip trailing slashes
	trimmed = strings.TrimRight(trimmed, "/")
	if trimmed == "" {
		return "", fmt.Errorf("missing host in endpoint %q", raw)
	}

	// Downstream endpoints do not support subpaths
	if strings.Contains(trimmed, "/") {
		return "", fmt.Errorf("endpoint path is not supported in %q", raw)
	}

	var host, port string
	if strings.HasPrefix(trimmed, "[") {
		closeBracket := strings.Index(trimmed, "]")
		if closeBracket == -1 {
			return "", fmt.Errorf("unmatched bracket in endpoint %q", raw)
		}
		host = trimmed[1:closeBracket]
		afterBracket := trimmed[closeBracket+1:]
		if afterBracket == "" {
			port = DefaultDockerPort
		} else if strings.HasPrefix(afterBracket, ":") {
			port = afterBracket[1:]
		} else {
			return "", fmt.Errorf("invalid endpoint format %q", raw)
		}
	} else if ip := net.ParseIP(trimmed); ip != nil {
		// Bare IP (IPv4 e.g. 10.0.0.1 or IPv6 e.g. ::1 without brackets or port)
		host = trimmed
		port = DefaultDockerPort
	} else {
		h, p, err := net.SplitHostPort(trimmed)
		if err == nil {
			host = h
			port = p
		} else {
			if strings.Contains(trimmed, ":") {
				return "", fmt.Errorf("invalid host/port in endpoint %q: %w", raw, err)
			}
			host = trimmed
			port = DefaultDockerPort
		}
	}

	if host == "" {
		return "", fmt.Errorf("missing host in endpoint %q", raw)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return "", fmt.Errorf("invalid port %q in endpoint %q", port, raw)
	}

	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, strconv.Itoa(portNum))), nil
}

// parseSwarmService parses a swarm service entry and extracts the scheme, lookup host (with "tasks." prefix), and port.
func parseSwarmService(raw string) (scheme, lookupHost, port string, err error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", "", errors.New("empty swarm service")
	}

	scheme = "http"
	if idx := strings.Index(trimmed, "://"); idx != -1 {
		rawScheme := strings.ToLower(trimmed[:idx])
		trimmed = trimmed[idx+3:]
		switch rawScheme {
		case "tcp", "http":
			scheme = "http"
		case "https":
			scheme = "https"
		default:
			return "", "", "", fmt.Errorf("unsupported scheme %q in swarm service %q", rawScheme, raw)
		}
	}

	if idx := strings.IndexAny(trimmed, "?#"); idx != -1 {
		trimmed = trimmed[:idx]
	}
	trimmed = strings.TrimRight(trimmed, "/")
	if trimmed == "" {
		return "", "", "", fmt.Errorf("missing service name in %q", raw)
	}
	if strings.Contains(trimmed, "/") {
		return "", "", "", fmt.Errorf("path not allowed in swarm service %q", raw)
	}

	var host string
	if strings.HasPrefix(trimmed, "[") {
		closeBracket := strings.Index(trimmed, "]")
		if closeBracket == -1 {
			return "", "", "", fmt.Errorf("unmatched bracket in swarm service %q", raw)
		}
		host = trimmed[1:closeBracket]
		after := trimmed[closeBracket+1:]
		if after == "" {
			port = DefaultDockerPort
		} else if strings.HasPrefix(after, ":") {
			port = after[1:]
		} else {
			return "", "", "", fmt.Errorf("invalid format in swarm service %q", raw)
		}
	} else {
		h, p, splitErr := net.SplitHostPort(trimmed)
		if splitErr == nil {
			host = h
			port = p
		} else {
			if strings.Contains(trimmed, ":") {
				return "", "", "", fmt.Errorf("invalid host/port in swarm service %q: %w", raw, splitErr)
			}
			host = trimmed
			port = DefaultDockerPort
		}
	}

	if host == "" {
		return "", "", "", fmt.Errorf("missing service name in %q", raw)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil || portNum <= 0 || portNum > 65535 {
		return "", "", "", fmt.Errorf("invalid port %q in swarm service %q", port, raw)
	}

	lookupHost = host
	if !strings.HasPrefix(lookupHost, "tasks.") {
		lookupHost = "tasks." + lookupHost
	}

	return scheme, lookupHost, strconv.Itoa(portNum), nil
}

// Option allows configuring optional parameters for Manager.
type Option func(*Manager)

// WithLookupHost sets a custom DNS host lookup function.
func WithLookupHost(fn LookupHostFunc) Option {
	return func(m *Manager) {
		if fn != nil {
			m.lookupHost = fn
		}
	}
}

// WithHTTPClient sets a custom HTTP client for health checks.
func WithHTTPClient(client *http.Client) Option {
	return func(m *Manager) {
		if client != nil {
			m.client = client
		}
	}
}

// WithLogger sets a custom structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(m *Manager) {
		if logger != nil {
			m.logger = logger
		}
	}
}

// WithTimeout sets a custom request timeout for health checks.
func WithTimeout(timeout time.Duration) Option {
	return func(m *Manager) {
		m.timeout = timeout
	}
}

// Manager handles endpoint discovery, Swarm DNS resolution, and endpoint health tracking.
type Manager struct {
	mu                 sync.RWMutex
	staticEndpoints    []string
	swarmServices      []string
	swarmEndpoints     map[string][]string // service -> resolved endpoints
	health             map[string]bool     // endpoint URL -> healthy status
	dnsRefresh         time.Duration
	healthInterval     time.Duration
	timeout            time.Duration
	lookupHost         LookupHostFunc
	client             *http.Client
	logger             *slog.Logger
	triggerHealthCheck chan struct{}
	dnsMu              sync.Mutex
	checkMu            sync.Mutex
	startOnce          sync.Once
	wg                 sync.WaitGroup
}

// NewManager creates a new Manager instance from the provided configuration.
func NewManager(cfg *config.Config, opts ...Option) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}

	m := &Manager{
		swarmEndpoints:     make(map[string][]string),
		health:             make(map[string]bool),
		dnsRefresh:         cfg.DNSRefresh,
		healthInterval:     cfg.HealthInterval,
		timeout:            cfg.Timeout,
		lookupHost:         net.DefaultResolver.LookupHost,
		client:             &http.Client{},
		logger:             slog.Default(),
		triggerHealthCheck: make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(m)
	}

	// Normalize and deduplicate static hosts
	staticSet := make(map[string]struct{})
	for _, host := range cfg.Hosts {
		norm, err := NormalizeEndpoint(host)
		if err != nil {
			return nil, fmt.Errorf("invalid static host %q: %w", host, err)
		}
		if _, exists := staticSet[norm]; !exists {
			staticSet[norm] = struct{}{}
			m.staticEndpoints = append(m.staticEndpoints, norm)
			m.health[norm] = true
		}
	}
	sort.Strings(m.staticEndpoints)

	// Validate swarm services
	for _, svc := range cfg.SwarmServices {
		if _, _, _, err := parseSwarmService(svc); err != nil {
			return nil, fmt.Errorf("invalid swarm service %q: %w", svc, err)
		}
		m.swarmServices = append(m.swarmServices, svc)
	}

	return m, nil
}

// allEndpointsLocked returns all known endpoints (static + swarm), deduplicated and sorted.
// Assumes m.mu is held for reading or writing.
func (m *Manager) allEndpointsLocked() []string {
	set := make(map[string]struct{})
	for _, ep := range m.staticEndpoints {
		set[ep] = struct{}{}
	}
	for _, eps := range m.swarmEndpoints {
		for _, ep := range eps {
			set[ep] = struct{}{}
		}
	}

	result := make([]string, 0, len(set))
	for ep := range set {
		result = append(result, ep)
	}
	sort.Strings(result)
	return result
}

// AllEndpoints returns all currently known endpoints in deterministic sorted order.
func (m *Manager) AllEndpoints() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.allEndpointsLocked()
}

// HealthyEndpoints returns all currently healthy endpoints in deterministic sorted order.
// If all known endpoints are marked unhealthy, it falls back to returning all known endpoints
// so that requests can still attempt to reach downstreams rather than failing immediately.
func (m *Manager) HealthyEndpoints() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	all := m.allEndpointsLocked()
	var healthy []string
	for _, ep := range all {
		if m.health[ep] {
			healthy = append(healthy, ep)
		}
	}

	if len(healthy) == 0 && len(all) > 0 {
		return all
	}
	return healthy
}

// Endpoints returns all currently healthy endpoints in deterministic order (or all endpoints as fallback).
func (m *Manager) Endpoints() []string {
	return m.HealthyEndpoints()
}

// IsHealthy reports whether the given endpoint is marked as healthy.
func (m *Manager) IsHealthy(endpoint string) bool {
	target, err := NormalizeEndpoint(endpoint)
	if err != nil {
		target = endpoint
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if h, exists := m.health[target]; exists {
		return h
	}
	if h, exists := m.health[endpoint]; exists {
		return h
	}
	return false
}

// MarkUnhealthy marks an endpoint as unhealthy and triggers/schedules a background recovery check.
func (m *Manager) MarkUnhealthy(endpoint string) {
	target, err := NormalizeEndpoint(endpoint)
	if err != nil {
		target = endpoint
	}

	m.mu.Lock()
	if _, exists := m.health[target]; !exists {
		if _, exists := m.health[endpoint]; exists {
			target = endpoint
		}
	}

	wasHealthy := m.health[target]
	m.health[target] = false
	m.mu.Unlock()

	if wasHealthy {
		m.logger.Warn("downstream endpoint marked unhealthy", "endpoint", target)
	}

	// Trigger background recovery check
	select {
	case m.triggerHealthCheck <- struct{}{}:
	default:
	}
}

// MarkHealthy marks an endpoint as healthy.
func (m *Manager) MarkHealthy(endpoint string) {
	target, err := NormalizeEndpoint(endpoint)
	if err != nil {
		target = endpoint
	}

	m.mu.Lock()
	if _, exists := m.health[target]; !exists {
		if _, exists := m.health[endpoint]; exists {
			target = endpoint
		}
	}

	wasUnhealthy := !m.health[target]
	m.health[target] = true
	m.mu.Unlock()

	if wasUnhealthy {
		m.logger.Info("downstream endpoint marked healthy", "endpoint", target)
	}
}

// RefreshDNS resolves all configured SwarmServices and updates the endpoint set.
// It preserves existing health status for already-known endpoints and initializes newly discovered endpoints as healthy.
// If a DNS lookup fails temporarily, it logs a warning, keeps previously resolved endpoints for that service,
// and continues with other services / static hosts.
func (m *Manager) RefreshDNS(ctx context.Context) error {
	if !m.dnsMu.TryLock() {
		return nil
	}
	defer m.dnsMu.Unlock()

	if len(m.swarmServices) == 0 {
		return nil
	}

	var errs []error
	newSwarmEndpoints := make(map[string][]string)

	m.mu.RLock()
	for k, v := range m.swarmEndpoints {
		newSwarmEndpoints[k] = v
	}
	m.mu.RUnlock()

	for _, svc := range m.swarmServices {
		scheme, lookupHost, port, err := parseSwarmService(svc)
		if err != nil {
			m.logger.Warn("invalid swarm service specification", "service", svc, "error", err)
			errs = append(errs, err)
			continue
		}

		ips, err := m.lookupHost(ctx, lookupHost)
		if err != nil {
			m.logger.Warn("swarm service DNS lookup failed", "service", svc, "lookup_host", lookupHost, "error", err)
			errs = append(errs, fmt.Errorf("swarm DNS lookup failed for %s (%s): %w", svc, lookupHost, err))
			continue // keep previously resolved endpoints for this service
		}

		var eps []string
		for _, ip := range ips {
			norm, err := NormalizeEndpoint(fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(ip, port)))
			if err != nil {
				m.logger.Warn("failed to normalize resolved endpoint", "service", svc, "ip", ip, "error", err)
				continue
			}
			eps = append(eps, norm)
		}
		newSwarmEndpoints[svc] = eps
	}

	m.mu.Lock()
	m.swarmEndpoints = newSwarmEndpoints

	// Rebuild active endpoints set
	activeSet := make(map[string]struct{})
	for _, ep := range m.staticEndpoints {
		activeSet[ep] = struct{}{}
	}
	for _, eps := range m.swarmEndpoints {
		for _, ep := range eps {
			activeSet[ep] = struct{}{}
		}
	}

	// Add newly discovered endpoints as healthy
	for ep := range activeSet {
		if _, exists := m.health[ep]; !exists {
			m.health[ep] = true
		}
	}

	// Remove endpoints that no longer exist
	for ep := range m.health {
		if _, exists := activeSet[ep]; !exists {
			delete(m.health, ep)
		}
	}
	m.mu.Unlock()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CheckHealth pings all known endpoints concurrently via GET <endpoint>/_ping.
// If status < 500 (e.g. 200 OK or 403 Forbidden), the endpoint is considered reachable and marked healthy.
// If a connection error or status >= 500 occurs, it is marked unhealthy.
// When an unhealthy endpoint recovers, it logs at Info level and marks it healthy.
func (m *Manager) CheckHealth(ctx context.Context) {
	if !m.checkMu.TryLock() {
		return
	}
	defer m.checkMu.Unlock()

	m.mu.RLock()
	endpoints := m.allEndpointsLocked()
	m.mu.RUnlock()

	if len(endpoints) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, ep := range endpoints {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			m.checkSingleEndpoint(ctx, endpoint)
		}(ep)
	}
	wg.Wait()
}

func (m *Manager) checkSingleEndpoint(ctx context.Context, endpoint string) {
	pingURL := endpoint + "/_ping"

	timeout := m.timeout
	if timeout <= 0 {
		timeout = DefaultPingTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, pingURL, nil)
	if err != nil {
		m.logger.Warn("failed to create ping request", "endpoint", endpoint, "error", err)
		m.setEndpointHealth(endpoint, false)
		return
	}

	resp, err := m.client.Do(req)
	if err != nil {
		m.logger.Warn("downstream endpoint ping failed", "endpoint", endpoint, "error", err)
		m.setEndpointHealth(endpoint, false)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 500 {
		m.setEndpointHealth(endpoint, true)
	} else {
		m.logger.Warn("downstream endpoint ping returned server error", "endpoint", endpoint, "status", resp.StatusCode)
		m.setEndpointHealth(endpoint, false)
	}
}

func (m *Manager) setEndpointHealth(endpoint string, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	prev, exists := m.health[endpoint]
	if !exists {
		return
	}

	if prev != healthy {
		m.health[endpoint] = healthy
		if healthy {
			m.logger.Info("downstream endpoint recovered", "endpoint", endpoint)
		} else {
			m.logger.Warn("downstream endpoint unhealthy", "endpoint", endpoint)
		}
	}
}

// Start runs background goroutines for periodic RefreshDNS (if SwarmServices are configured
// and DNSRefresh > 0) and periodic CheckHealth (if HealthInterval > 0), stopping cleanly when ctx is canceled.
func (m *Manager) Start(ctx context.Context) {
	m.startOnce.Do(func() {
		// Initial resolution if swarm services are configured
		if len(m.swarmServices) > 0 {
			_ = m.RefreshDNS(ctx)
		}
		// Initial health check
		m.CheckHealth(ctx)

		// Periodic DNS refresh
		if len(m.swarmServices) > 0 && m.dnsRefresh > 0 {
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				ticker := time.NewTicker(m.dnsRefresh)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						_ = m.RefreshDNS(ctx)
					}
				}
			}()
		}

		// Periodic Health Check + Triggered Recovery Check
		if m.healthInterval > 0 {
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				ticker := time.NewTicker(m.healthInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						m.CheckHealth(ctx)
					case <-m.triggerHealthCheck:
						m.CheckHealth(ctx)
					}
				}
			}()
		} else {
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case <-m.triggerHealthCheck:
						m.CheckHealth(ctx)
					}
				}
			}()
		}
	})
}

// Wait blocks until all background discovery goroutines have terminated.
func (m *Manager) Wait() {
	m.wg.Wait()
}
