package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/socketmesh/socketmesh/internal/config"
)

// helper to simulate environment variables via a map
func mockEnv(m map[string]string) func(string) string {
	return func(k string) string {
		return m[k]
	}
}

func emptyEnv(string) string {
	return ""
}

func TestDefaults(t *testing.T) {
	cfg, err := config.Parse([]string{"--hosts", "10.0.0.1:2375"}, emptyEnv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != ":2375" {
		t.Errorf("expected ListenAddr :2375, got %q", cfg.ListenAddr)
	}
	if cfg.DNSRefresh != 15*time.Second {
		t.Errorf("expected DNSRefresh 15s, got %v", cfg.DNSRefresh)
	}
	if cfg.HealthInterval != 10*time.Second {
		t.Errorf("expected HealthInterval 10s, got %v", cfg.HealthInterval)
	}
	if cfg.Timeout != 0 {
		t.Errorf("expected Timeout 0, got %v", cfg.Timeout)
	}
	if cfg.CacheTTL != 0 {
		t.Errorf("expected CacheTTL 0, got %v", cfg.CacheTTL)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel info, got %q", cfg.LogLevel)
	}
	if cfg.HealthcheckMode != false {
		t.Errorf("expected HealthcheckMode false, got %v", cfg.HealthcheckMode)
	}
	if cfg.ACL.Ping != true {
		t.Errorf("expected ACL.Ping true by default, got %v", cfg.ACL.Ping)
	}
	if cfg.ACL.Containers != false || cfg.ACL.Images != false || cfg.ACL.Networks != false ||
		cfg.ACL.Volumes != false || cfg.ACL.Info != false || cfg.ACL.Version != false ||
		cfg.ACL.Services != false || cfg.ACL.Tasks != false || cfg.ACL.Nodes != false ||
		cfg.ACL.Swarm != false || cfg.ACL.Secrets != false || cfg.ACL.Configs != false ||
		cfg.ACL.System != false || cfg.ACL.Distribution != false || cfg.ACL.Plugins != false ||
		cfg.ACL.Events != false {
		t.Errorf("expected all ACL flags except Ping to be false by default: %+v", cfg.ACL)
	}
	if cfg.WarnWriteEnvSet != false {
		t.Errorf("expected WarnWriteEnvSet false, got %v", cfg.WarnWriteEnvSet)
	}
}

func TestEnvParsing(t *testing.T) {
	t.Run("LISTEN_ADDR and PORT", func(t *testing.T) {
		// Bare PORT without colon
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"PORT":  "8080",
			"HOSTS": "127.0.0.1:2375",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ListenAddr != ":8080" {
			t.Errorf("expected :8080, got %q", cfg.ListenAddr)
		}

		// PORT with colon
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"PORT":  ":9000",
			"HOSTS": "127.0.0.1:2375",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ListenAddr != ":9000" {
			t.Errorf("expected :9000, got %q", cfg.ListenAddr)
		}

		// LISTEN_ADDR takes precedence over PORT
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"LISTEN_ADDR": "0.0.0.0:2376",
			"PORT":        "8080",
			"HOSTS":       "127.0.0.1:2375",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ListenAddr != "0.0.0.0:2376" {
			t.Errorf("expected 0.0.0.0:2376, got %q", cfg.ListenAddr)
		}
	})

	t.Run("HOSTS and SWARM_SERVICE", func(t *testing.T) {
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":         " 10.0.0.1:2375 , 10.0.0.2:2375, , 10.0.0.3:2375 ",
			"SWARM_SERVICE": " tasks.dockersock , tasks.backup:2375 ",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Hosts) != 3 || cfg.Hosts[0] != "10.0.0.1:2375" || cfg.Hosts[1] != "10.0.0.2:2375" || cfg.Hosts[2] != "10.0.0.3:2375" {
			t.Errorf("unexpected Hosts: %v", cfg.Hosts)
		}
		if len(cfg.SwarmServices) != 2 || cfg.SwarmServices[0] != "tasks.dockersock" || cfg.SwarmServices[1] != "tasks.backup:2375" {
			t.Errorf("unexpected SwarmServices: %v", cfg.SwarmServices)
		}

		// SWARM_SERVICES plural fallback
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"SWARM_SERVICES": "tasks.swarm:2375",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.SwarmServices) != 1 || cfg.SwarmServices[0] != "tasks.swarm:2375" {
			t.Errorf("expected SWARM_SERVICES fallback to work: %v", cfg.SwarmServices)
		}
	})

	t.Run("Durations integer seconds vs duration string", func(t *testing.T) {
		// Integer seconds for CACHE_TTL, TIMEOUT, DNS_REFRESH, HEALTH_INTERVAL
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":           "127.0.0.1:2375",
			"CACHE_TTL":       "5",
			"TIMEOUT":         "10",
			"DNS_REFRESH":     "20",
			"HEALTH_INTERVAL": "8",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.CacheTTL != 5*time.Second {
			t.Errorf("expected CacheTTL 5s, got %v", cfg.CacheTTL)
		}
		if cfg.Timeout != 10*time.Second {
			t.Errorf("expected Timeout 10s, got %v", cfg.Timeout)
		}
		if cfg.DNSRefresh != 20*time.Second {
			t.Errorf("expected DNSRefresh 20s, got %v", cfg.DNSRefresh)
		}
		if cfg.HealthInterval != 8*time.Second {
			t.Errorf("expected HealthInterval 8s, got %v", cfg.HealthInterval)
		}

		// Duration strings
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":           "127.0.0.1:2375",
			"CACHE_TTL":       "2m",
			"TIMEOUT":         "30s",
			"DNS_REFRESH":     "45s",
			"HEALTH_INTERVAL": "15s",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.CacheTTL != 2*time.Minute {
			t.Errorf("expected CacheTTL 2m, got %v", cfg.CacheTTL)
		}
		if cfg.Timeout != 30*time.Second {
			t.Errorf("expected Timeout 30s, got %v", cfg.Timeout)
		}
		if cfg.DNSRefresh != 45*time.Second {
			t.Errorf("expected DNSRefresh 45s, got %v", cfg.DNSRefresh)
		}
		if cfg.HealthInterval != 15*time.Second {
			t.Errorf("expected HealthInterval 15s, got %v", cfg.HealthInterval)
		}

		// REQUEST_TIMEOUT fallback when TIMEOUT is unset
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":           "127.0.0.1:2375",
			"REQUEST_TIMEOUT": "25s",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Timeout != 25*time.Second {
			t.Errorf("expected Timeout 25s from REQUEST_TIMEOUT, got %v", cfg.Timeout)
		}

		// TIMEOUT takes precedence over REQUEST_TIMEOUT
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":           "127.0.0.1:2375",
			"TIMEOUT":         "10s",
			"REQUEST_TIMEOUT": "25s",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Timeout != 10*time.Second {
			t.Errorf("expected Timeout 10s from TIMEOUT, got %v", cfg.Timeout)
		}
	})

	t.Run("LOG_LEVEL", func(t *testing.T) {
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":     "127.0.0.1:2375",
			"LOG_LEVEL": "debug",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.LogLevel != "debug" {
			t.Errorf("expected debug, got %q", cfg.LogLevel)
		}
	})

	t.Run("ACL flags via env", func(t *testing.T) {
		// All truthy formats: "1", "true", "yes"
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":        "127.0.0.1:2375",
			"CONTAINERS":   "1",
			"IMAGES":       "true",
			"NETWORKS":     "yes",
			"VOLUMES":      "TRUE",
			"INFO":         "Yes",
			"VERSION":      "1",
			"PING":         "0", // Disable Ping explicitly
			"SERVICES":     "1",
			"TASKS":        "true",
			"NODES":        "yes",
			"SWARM":        "1",
			"SECRETS":      "true",
			"CONFIGS":      "yes",
			"SYSTEM":       "1",
			"DISTRIBUTION": "true",
			"PLUGINS":      "yes",
			"EVENTS":       "1",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !cfg.ACL.Containers || !cfg.ACL.Images || !cfg.ACL.Networks || !cfg.ACL.Volumes ||
			!cfg.ACL.Info || !cfg.ACL.Version || !cfg.ACL.Services || !cfg.ACL.Tasks ||
			!cfg.ACL.Nodes || !cfg.ACL.Swarm || !cfg.ACL.Secrets || !cfg.ACL.Configs ||
			!cfg.ACL.System || !cfg.ACL.Distribution || !cfg.ACL.Plugins || !cfg.ACL.Events {
			t.Errorf("expected all truthy ACLs to be true: %+v", cfg.ACL)
		}
		if cfg.ACL.Ping != false {
			t.Errorf("expected Ping to be false when PING=0, got %v", cfg.ACL.Ping)
		}

		// Falsy formats: "0", "false", "no", ""
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":      "127.0.0.1:2375",
			"CONTAINERS": "0",
			"IMAGES":     "false",
			"NETWORKS":   "no",
			"VOLUMES":    "",
			"PING":       "false",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ACL.Containers || cfg.ACL.Images || cfg.ACL.Networks || cfg.ACL.Volumes || cfg.ACL.Ping {
			t.Errorf("expected falsy ACLs to be false: %+v", cfg.ACL)
		}
	})

	t.Run("WarnWriteEnvSet", func(t *testing.T) {
		writeKeys := []string{"POST", "BUILD", "COMMIT", "EXEC", "ALLOW_RESTARTS", "WRITE"}
		for _, key := range writeKeys {
			cfg, err := config.Parse(nil, mockEnv(map[string]string{
				"HOSTS": "127.0.0.1:2375",
				key:     "1",
			}))
			if err != nil {
				t.Fatalf("unexpected error for %s=1: %v", key, err)
			}
			if !cfg.WarnWriteEnvSet {
				t.Errorf("expected WarnWriteEnvSet true when %s=1", key)
			}
		}

		// Falsy value shouldn't set WarnWriteEnvSet
		cfg, err := config.Parse(nil, mockEnv(map[string]string{
			"HOSTS": "127.0.0.1:2375",
			"POST":  "0",
			"WRITE": "false",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.WarnWriteEnvSet {
			t.Errorf("expected WarnWriteEnvSet false when write envs are 0/false")
		}
	})
}

func TestCLIFlagOverrides(t *testing.T) {
	env := mockEnv(map[string]string{
		"LISTEN_ADDR":     ":2375",
		"HOSTS":           "10.0.0.1:2375",
		"SWARM_SERVICE":   "tasks.app",
		"DNS_REFRESH":     "15s",
		"HEALTH_INTERVAL": "10s",
		"TIMEOUT":         "5s",
		"CACHE_TTL":       "10s",
		"LOG_LEVEL":       "info",
		"CONTAINERS":      "0",
		"PING":            "1",
	})

	args := []string{
		"--listen", "127.0.0.1:8080",
		"--hosts", "192.168.1.1:2375,192.168.1.2:2375",
		"--swarm-service", "tasks.swarm_new:2375",
		"--dns-refresh", "30s",
		"--health-interval", "20s",
		"--timeout", "15s",
		"--cache-ttl", "1m",
		"--log-level", "debug",
		"--containers",
		"--ping=0",
	}

	cfg, err := config.Parse(args, env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != "127.0.0.1:8080" {
		t.Errorf("expected ListenAddr 127.0.0.1:8080, got %q", cfg.ListenAddr)
	}
	if len(cfg.Hosts) != 2 || cfg.Hosts[0] != "192.168.1.1:2375" || cfg.Hosts[1] != "192.168.1.2:2375" {
		t.Errorf("expected overridden Hosts, got %v", cfg.Hosts)
	}
	if len(cfg.SwarmServices) != 1 || cfg.SwarmServices[0] != "tasks.swarm_new:2375" {
		t.Errorf("expected overridden SwarmServices, got %v", cfg.SwarmServices)
	}
	if cfg.DNSRefresh != 30*time.Second {
		t.Errorf("expected DNSRefresh 30s, got %v", cfg.DNSRefresh)
	}
	if cfg.HealthInterval != 20*time.Second {
		t.Errorf("expected HealthInterval 20s, got %v", cfg.HealthInterval)
	}
	if cfg.Timeout != 15*time.Second {
		t.Errorf("expected Timeout 15s, got %v", cfg.Timeout)
	}
	if cfg.CacheTTL != 1*time.Minute {
		t.Errorf("expected CacheTTL 1m, got %v", cfg.CacheTTL)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %q", cfg.LogLevel)
	}
	if !cfg.ACL.Containers {
		t.Errorf("expected ACL.Containers true from CLI flag")
	}
	if cfg.ACL.Ping {
		t.Errorf("expected ACL.Ping false from CLI flag --ping=0")
	}
}

func TestValidationErrors(t *testing.T) {
	t.Run("nil getenv uses os.Getenv", func(t *testing.T) {
		cfg, err := config.Parse([]string{"--hosts", "127.0.0.1:2375"}, nil)
		if err != nil {
			t.Fatalf("unexpected error with nil getenv: %v", err)
		}
		if len(cfg.Hosts) != 1 || cfg.Hosts[0] != "127.0.0.1:2375" {
			t.Errorf("unexpected Hosts: %v", cfg.Hosts)
		}
	})

	t.Run("unexpected positional arguments return error", func(t *testing.T) {
		_, err := config.Parse([]string{"--hosts", "127.0.0.1:2375", "unexpected_arg"}, emptyEnv)
		if err == nil {
			t.Fatal("expected error for unexpected positional argument")
		}
		if !strings.Contains(err.Error(), "unexpected argument") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("healthcheck mode bypasses host requirement", func(t *testing.T) {
		// via CLI flag
		cfg, err := config.Parse([]string{"--healthcheck"}, emptyEnv)
		if err != nil {
			t.Fatalf("expected healthcheck flag to bypass host requirement, got: %v", err)
		}
		if !cfg.HealthcheckMode {
			t.Errorf("expected HealthcheckMode true")
		}

		// via env var
		cfg, err = config.Parse(nil, mockEnv(map[string]string{
			"HEALTHCHECK": "true",
		}))
		if err != nil {
			t.Fatalf("expected HEALTHCHECK env to bypass host requirement, got: %v", err)
		}
		if !cfg.HealthcheckMode {
			t.Errorf("expected HealthcheckMode true")
		}
	})

	t.Run("negative durations rejected", func(t *testing.T) {
		cases := []struct {
			name string
			args []string
			env  map[string]string
		}{
			{"negative timeout flag", []string{"--hosts", "127.0.0.1:2375", "--timeout", "-5s"}, nil},
			{"negative cache-ttl flag", []string{"--hosts", "127.0.0.1:2375", "--cache-ttl", "-1s"}, nil},
			{"negative dns-refresh flag", []string{"--hosts", "127.0.0.1:2375", "--dns-refresh", "-10s"}, nil},
			{"negative health-interval flag", []string{"--hosts", "127.0.0.1:2375", "--health-interval", "-2s"}, nil},
			{"negative timeout env", nil, map[string]string{"HOSTS": "127.0.0.1:2375", "TIMEOUT": "-5"}},
			{"negative cache-ttl env", nil, map[string]string{"HOSTS": "127.0.0.1:2375", "CACHE_TTL": "-1s"}},
			{"negative dns-refresh env", nil, map[string]string{"HOSTS": "127.0.0.1:2375", "DNS_REFRESH": "-10"}},
			{"negative health-interval env", nil, map[string]string{"HOSTS": "127.0.0.1:2375", "HEALTH_INTERVAL": "-2"}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var getenv func(string) string
				if tc.env != nil {
					getenv = mockEnv(tc.env)
				} else {
					getenv = emptyEnv
				}
				_, err := config.Parse(tc.args, getenv)
				if err == nil {
					t.Errorf("expected error for %s, got nil", tc.name)
				}
			})
		}
	})

	t.Run("invalid boolean values", func(t *testing.T) {
		_, err := config.Parse([]string{"--hosts", "127.0.0.1:2375", "--containers=maybe"}, emptyEnv)
		if err == nil {
			t.Error("expected error for invalid boolean in flag")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":      "127.0.0.1:2375",
			"CONTAINERS": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid boolean in env")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":       "127.0.0.1:2375",
			"HEALTHCHECK": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid HEALTHCHECK in env")
		}
	})

	t.Run("invalid duration values", func(t *testing.T) {
		_, err := config.Parse([]string{"--hosts", "127.0.0.1:2375", "--timeout", "invalid"}, emptyEnv)
		if err == nil {
			t.Error("expected error for invalid duration in flag")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":   "127.0.0.1:2375",
			"TIMEOUT": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid duration in env")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":       "127.0.0.1:2375",
			"DNS_REFRESH": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid DNS_REFRESH in env")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":           "127.0.0.1:2375",
			"HEALTH_INTERVAL": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid HEALTH_INTERVAL in env")
		}

		_, err = config.Parse(nil, mockEnv(map[string]string{
			"HOSTS":     "127.0.0.1:2375",
			"CACHE_TTL": "invalid",
		}))
		if err == nil {
			t.Error("expected error for invalid CACHE_TTL in env")
		}
	})
}

func TestHelpers(t *testing.T) {
	t.Run("ParseBool", func(t *testing.T) {
		truthy := []string{"1", "true", "yes", "TRUE", "Yes", "  true  "}
		for _, s := range truthy {
			val, err := config.ParseBool(s)
			if err != nil || !val {
				t.Errorf("expected %q to be true, err: %v", s, err)
			}
		}

		falsy := []string{"0", "false", "no", "", "FALSE", "No", "  0 "}
		for _, s := range falsy {
			val, err := config.ParseBool(s)
			if err != nil || val {
				t.Errorf("expected %q to be false, err: %v", s, err)
			}
		}

		invalid := []string{"2", "maybe", "on", "off", "enabled"}
		for _, s := range invalid {
			_, err := config.ParseBool(s)
			if err == nil {
				t.Errorf("expected error for %q", s)
			}
		}
	})

	t.Run("ParseDuration", func(t *testing.T) {
		valid := map[string]time.Duration{
			"":      0,
			"0":     0,
			"15":    15 * time.Second,
			"15s":   15 * time.Second,
			"2m":    2 * time.Minute,
			"500ms": 500 * time.Millisecond,
			"1.5":   1500 * time.Millisecond,
		}
		for s, expected := range valid {
			d, err := config.ParseDuration(s)
			if err != nil {
				t.Errorf("unexpected error for %q: %v", s, err)
			}
			if d != expected {
				t.Errorf("for %q expected %v, got %v", s, expected, d)
			}
		}

		invalid := []string{"invalid", "abc", "10xyz"}
		for _, s := range invalid {
			_, err := config.ParseDuration(s)
			if err == nil {
				t.Errorf("expected error for %q", s)
			}
		}
	})
}
