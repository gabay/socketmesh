package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// ACLConfig defines the access control list permissions for Docker API endpoints.
type ACLConfig struct {
	Containers   bool
	Images       bool
	Networks     bool
	Volumes      bool
	Info         bool
	Version      bool
	Ping         bool
	Services     bool
	Tasks        bool
	Nodes        bool
	Swarm        bool
	Secrets      bool
	Configs      bool
	System       bool
	Distribution bool
	Plugins      bool
	Events       bool
}

// Config represents the application configuration for SocketMesh.
type Config struct {
	ListenAddr      string
	Hosts           []string
	SwarmServices   []string
	DNSRefresh      time.Duration
	HealthInterval  time.Duration
	Timeout         time.Duration
	CacheTTL        time.Duration
	LogLevel        string
	HealthcheckMode bool
	ACL             ACLConfig
	WarnWriteEnvSet bool
}

// ParseBool parses a string into a boolean according to SocketMesh conventions.
// Accepts "1", "true", "yes" as true.
// Accepts "0", "false", "no", "" as false.
// Comparisons are case-insensitive.
func ParseBool(s string) (bool, error) {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	switch trimmed {
	case "1", "true", "yes":
		return true, nil
	case "0", "false", "no", "":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value %q", s)
	}
}

// ParseDuration parses a string as either an integer number of seconds or a standard Go duration string.
func ParseDuration(s string) (time.Duration, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, nil
	}
	if sec, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return time.Duration(sec) * time.Second, nil
	}
	if sec, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return time.Duration(sec * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return d, nil
}

// splitCommaList splits a comma-separated string, trims whitespace from each element, and ignores empty elements.
func splitCommaList(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// Custom flag.Value wrappers

type boolFlagValue struct {
	target *bool
}

func (b *boolFlagValue) IsBoolFlag() bool {
	return true
}

func (b *boolFlagValue) String() string {
	if b.target == nil {
		return "false"
	}
	return strconv.FormatBool(*b.target)
}

func (b *boolFlagValue) Set(s string) error {
	val, err := ParseBool(s)
	if err != nil {
		return err
	}
	*b.target = val
	return nil
}

type durationFlagValue struct {
	target *time.Duration
}

func (d *durationFlagValue) String() string {
	if d.target == nil {
		return "0s"
	}
	return d.target.String()
}

func (d *durationFlagValue) Set(s string) error {
	dur, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d.target = dur
	return nil
}

type stringFlagValue struct {
	target *string
}

func (s *stringFlagValue) String() string {
	if s.target == nil {
		return ""
	}
	return *s.target
}

func (s *stringFlagValue) Set(val string) error {
	*s.target = val
	return nil
}

type listFlagValue struct {
	target *[]string
}

func (l *listFlagValue) String() string {
	if l.target == nil {
		return ""
	}
	return strings.Join(*l.target, ",")
}

func (l *listFlagValue) Set(s string) error {
	*l.target = splitCommaList(s)
	return nil
}

func setupFlagSet(cfg *Config) *flag.FlagSet {
	fs := flag.NewFlagSet("socketmesh", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.Var(&stringFlagValue{target: &cfg.ListenAddr}, "listen", "Address to listen on (default :2375)")
	fs.Var(&listFlagValue{target: &cfg.Hosts}, "hosts", "Comma-separated downstream host addresses")
	fs.Var(&listFlagValue{target: &cfg.SwarmServices}, "swarm-service", "Comma-separated swarm service names or service:port")
	fs.Var(&listFlagValue{target: &cfg.SwarmServices}, "swarm-services", "Alias for --swarm-service")
	fs.Var(&durationFlagValue{target: &cfg.DNSRefresh}, "dns-refresh", "Swarm service DNS refresh interval (default 15s)")
	fs.Var(&durationFlagValue{target: &cfg.HealthInterval}, "health-interval", "Downstream health check ping interval (default 10s)")
	fs.Var(&durationFlagValue{target: &cfg.Timeout}, "timeout", "Request timeout (default 0)")
	fs.Var(&durationFlagValue{target: &cfg.CacheTTL}, "cache-ttl", "Response cache TTL (default 0)")
	fs.Var(&stringFlagValue{target: &cfg.LogLevel}, "log-level", "Log level (default info)")
	fs.Var(&boolFlagValue{target: &cfg.HealthcheckMode}, "healthcheck", "Run in Docker healthcheck mode")

	// ACL flags
	fs.Var(&boolFlagValue{target: &cfg.ACL.Containers}, "containers", "Allow container endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Images}, "images", "Allow image endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Networks}, "networks", "Allow network endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Volumes}, "volumes", "Allow volume endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Info}, "info", "Allow /info endpoint")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Version}, "version", "Allow /version endpoint")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Ping}, "ping", "Allow /_ping endpoint")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Services}, "services", "Allow service endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Tasks}, "tasks", "Allow task endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Nodes}, "nodes", "Allow node endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Swarm}, "swarm", "Allow swarm endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Secrets}, "secrets", "Allow secret endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Configs}, "configs", "Allow config endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.System}, "system", "Allow system endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Distribution}, "distribution", "Allow distribution endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Plugins}, "plugins", "Allow plugin endpoints")
	fs.Var(&boolFlagValue{target: &cfg.ACL.Events}, "events", "Allow events endpoint")

	return fs
}

// Parse initializes and validates configuration from environment variables and CLI flags.
// CLI flags take precedence over environment variables, which in turn override default values.
func Parse(args []string, getenv func(string) string) (*Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	// 1. Initialize default configuration
	cfg := &Config{
		ListenAddr:     ":2375",
		Hosts:          nil,
		SwarmServices:  nil,
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		Timeout:        0,
		CacheTTL:       0,
		LogLevel:       "info",
		ACL: ACLConfig{
			Ping: true,
		},
	}

	// 2. Load environment variables
	listenEnv := strings.TrimSpace(getenv("LISTEN_ADDR"))
	if listenEnv != "" {
		cfg.ListenAddr = listenEnv
	} else if portEnv := strings.TrimSpace(getenv("PORT")); portEnv != "" {
		if !strings.Contains(portEnv, ":") {
			cfg.ListenAddr = ":" + portEnv
		} else {
			cfg.ListenAddr = portEnv
		}
	}

	if hostsEnv := getenv("HOSTS"); hostsEnv != "" {
		cfg.Hosts = splitCommaList(hostsEnv)
	}

	swarmEnv := getenv("SWARM_SERVICE")
	if swarmEnv == "" {
		swarmEnv = getenv("SWARM_SERVICES")
	}
	if swarmEnv != "" {
		cfg.SwarmServices = splitCommaList(swarmEnv)
	}

	if dnsEnv := getenv("DNS_REFRESH"); dnsEnv != "" {
		d, err := ParseDuration(dnsEnv)
		if err != nil {
			return nil, fmt.Errorf("invalid DNS_REFRESH: %w", err)
		}
		cfg.DNSRefresh = d
	}

	if hiEnv := getenv("HEALTH_INTERVAL"); hiEnv != "" {
		d, err := ParseDuration(hiEnv)
		if err != nil {
			return nil, fmt.Errorf("invalid HEALTH_INTERVAL: %w", err)
		}
		cfg.HealthInterval = d
	}

	timeoutEnv := getenv("TIMEOUT")
	if timeoutEnv == "" {
		timeoutEnv = getenv("REQUEST_TIMEOUT")
	}
	if timeoutEnv != "" {
		d, err := ParseDuration(timeoutEnv)
		if err != nil {
			return nil, fmt.Errorf("invalid TIMEOUT: %w", err)
		}
		cfg.Timeout = d
	}

	if cacheEnv := getenv("CACHE_TTL"); cacheEnv != "" {
		d, err := ParseDuration(cacheEnv)
		if err != nil {
			return nil, fmt.Errorf("invalid CACHE_TTL: %w", err)
		}
		cfg.CacheTTL = d
	}

	if logEnv := strings.TrimSpace(getenv("LOG_LEVEL")); logEnv != "" {
		cfg.LogLevel = logEnv
	}

	if hcEnv := getenv("HEALTHCHECK"); hcEnv != "" {
		b, err := ParseBool(hcEnv)
		if err != nil {
			return nil, fmt.Errorf("invalid HEALTHCHECK: %w", err)
		}
		cfg.HealthcheckMode = b
	}

	// ACL environment variables
	aclEnvs := []struct {
		key    string
		target *bool
	}{
		{"CONTAINERS", &cfg.ACL.Containers},
		{"IMAGES", &cfg.ACL.Images},
		{"NETWORKS", &cfg.ACL.Networks},
		{"VOLUMES", &cfg.ACL.Volumes},
		{"INFO", &cfg.ACL.Info},
		{"VERSION", &cfg.ACL.Version},
		{"PING", &cfg.ACL.Ping},
		{"SERVICES", &cfg.ACL.Services},
		{"TASKS", &cfg.ACL.Tasks},
		{"NODES", &cfg.ACL.Nodes},
		{"SWARM", &cfg.ACL.Swarm},
		{"SECRETS", &cfg.ACL.Secrets},
		{"CONFIGS", &cfg.ACL.Configs},
		{"SYSTEM", &cfg.ACL.System},
		{"DISTRIBUTION", &cfg.ACL.Distribution},
		{"PLUGINS", &cfg.ACL.Plugins},
		{"EVENTS", &cfg.ACL.Events},
	}

	for _, item := range aclEnvs {
		val := getenv(item.key)
		if val != "" {
			b, err := ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", item.key, err)
			}
			*item.target = b
		}
	}

	// WarnWriteEnvSet check: true if POST, BUILD, COMMIT, EXEC, ALLOW_RESTARTS, or WRITE is truthy
	writeEnvs := []string{"POST", "BUILD", "COMMIT", "EXEC", "ALLOW_RESTARTS", "WRITE"}
	for _, key := range writeEnvs {
		val := getenv(key)
		if val != "" {
			b, err := ParseBool(val)
			if err == nil && b {
				cfg.WarnWriteEnvSet = true
				break
			}
		}
	}

	// 3. Parse CLI flags
	// Flags take precedence over environment variables.
	fs := setupFlagSet(cfg)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}

	// 4. Validation
	if cfg.DNSRefresh < 0 {
		return nil, fmt.Errorf("dns-refresh cannot be negative: %v", cfg.DNSRefresh)
	}
	if cfg.HealthInterval < 0 {
		return nil, fmt.Errorf("health-interval cannot be negative: %v", cfg.HealthInterval)
	}
	if cfg.Timeout < 0 {
		return nil, fmt.Errorf("timeout cannot be negative: %v", cfg.Timeout)
	}
	if cfg.CacheTTL < 0 {
		return nil, fmt.Errorf("cache-ttl cannot be negative: %v", cfg.CacheTTL)
	}

	if !cfg.HealthcheckMode {
		if len(cfg.Hosts) == 0 && len(cfg.SwarmServices) == 0 {
			return nil, errors.New("at least one downstream host or swarm service must be configured (via --hosts/HOSTS or --swarm-service/SWARM_SERVICE)")
		}
	}

	return cfg, nil
}
