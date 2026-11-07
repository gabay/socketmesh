package acl

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/socketmesh/socketmesh/internal/config"
)

func TestStripVersionPrefix(t *testing.T) {
	tests := []struct {
		name               string
		path               string
		expectedVersion    string
		expectedCleanPath  string
	}{
		// Versioned paths
		{
			name:              "versioned container path",
			path:              "/v1.45/containers/json",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "versioned ping path",
			path:              "/v1.24/_ping",
			expectedVersion:   "/v1.24",
			expectedCleanPath: "/_ping",
		},
		{
			name:              "versioned image path",
			path:              "/v1.41/images/json",
			expectedVersion:   "/v1.41",
			expectedCleanPath: "/images/json",
		},
		{
			name:              "versioned info path",
			path:              "/v1.40/info",
			expectedVersion:   "/v1.40",
			expectedCleanPath: "/info",
		},
		{
			name:              "version prefix exact",
			path:              "/v1.45",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/",
		},
		{
			name:              "version prefix with trailing slash",
			path:              "/v1.45/",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/",
		},
		{
			name:              "redundant slashes in versioned path",
			path:              "//v1.45///containers//json",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "no leading slash in versioned path",
			path:              "v1.45/containers/json",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "v2 prefix",
			path:              "/v2.0/volumes",
			expectedVersion:   "/v2.0",
			expectedCleanPath: "/volumes",
		},
		{
			name:              "versioned path with dot-dot traversal",
			path:              "/v1.45/containers/../images",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/images",
		},
		// Unversioned paths
		{
			name:              "unversioned container path",
			path:              "/containers/json",
			expectedVersion:   "",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "unversioned ping path",
			path:              "/_ping",
			expectedVersion:   "",
			expectedCleanPath: "/_ping",
		},
		{
			name:              "unversioned ping path with trailing slash",
			path:              "/_ping/",
			expectedVersion:   "",
			expectedCleanPath: "/_ping",
		},
		{
			name:              "unversioned version path",
			path:              "/version",
			expectedVersion:   "",
			expectedCleanPath: "/version",
		},
		{
			name:              "unversioned info path",
			path:              "/info",
			expectedVersion:   "",
			expectedCleanPath: "/info",
		},
		{
			name:              "unversioned without leading slash",
			path:              "containers/json",
			expectedVersion:   "",
			expectedCleanPath: "/containers/json",
		},
		// Malformed version prefixes
		{
			name:              "missing minor version /v1/foo",
			path:              "/v1/foo",
			expectedVersion:   "",
			expectedCleanPath: "/v1/foo",
		},
		{
			name:              "non-digit minor version /v1.a/foo",
			path:              "/v1.a/foo",
			expectedVersion:   "",
			expectedCleanPath: "/v1.a/foo",
		},
		{
			name:              "three version components /v1.45.1/foo",
			path:              "/v1.45.1/foo",
			expectedVersion:   "",
			expectedCleanPath: "/v1.45.1/foo",
		},
		{
			name:              "alphabetic component /v1.foo/bar",
			path:              "/v1.foo/bar",
			expectedVersion:   "",
			expectedCleanPath: "/v1.foo/bar",
		},
		{
			name:              "starts with v: /volumes",
			path:              "/volumes",
			expectedVersion:   "",
			expectedCleanPath: "/volumes",
		},
		{
			name:              "starts with v: /version",
			path:              "/version",
			expectedVersion:   "",
			expectedCleanPath: "/version",
		},
		{
			name:              "version followed by characters without slash /v1.45foo",
			path:              "/v1.45foo",
			expectedVersion:   "",
			expectedCleanPath: "/v1.45foo",
		},
		// Root path and empty path
		{
			name:              "root slash",
			path:              "/",
			expectedVersion:   "",
			expectedCleanPath: "/",
		},
		{
			name:              "empty path",
			path:              "",
			expectedVersion:   "",
			expectedCleanPath: "/",
		},
		{
			name:              "multiple slashes root",
			path:              "///",
			expectedVersion:   "",
			expectedCleanPath: "/",
		},
		// Query string and fragment handling
		{
			name:              "versioned path with query string",
			path:              "/v1.45/containers/json?all=1",
			expectedVersion:   "/v1.45",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "unversioned path with query string",
			path:              "/containers/json?all=1",
			expectedVersion:   "",
			expectedCleanPath: "/containers/json",
		},
		{
			name:              "path with fragment",
			path:              "/containers/json#section",
			expectedVersion:   "",
			expectedCleanPath: "/containers/json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vPrefix, cPath := StripVersionPrefix(tc.path)
			if vPrefix != tc.expectedVersion {
				t.Errorf("StripVersionPrefix(%q) versionPrefix = %q, want %q", tc.path, vPrefix, tc.expectedVersion)
			}
			if cPath != tc.expectedCleanPath {
				t.Errorf("StripVersionPrefix(%q) cleanPath = %q, want %q", tc.path, cPath, tc.expectedCleanPath)
			}
		})
	}
}

func allACLsEnabled() config.ACLConfig {
	return config.ACLConfig{
		Containers:   true,
		Images:       true,
		Networks:     true,
		Volumes:      true,
		Info:         true,
		Version:      true,
		Ping:         true,
		Services:     true,
		Tasks:        true,
		Nodes:        true,
		Swarm:        true,
		Secrets:      true,
		Configs:      true,
		System:       true,
		Distribution: true,
		Plugins:      true,
		Events:       true,
	}
}

func TestReadOnlyEnforcement(t *testing.T) {
	cfg := allACLsEnabled()

	forbiddenMethods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
		http.MethodOptions,
		http.MethodConnect,
		http.MethodTrace,
		"POST",
		"post",
		"PUT",
		"put",
		"DELETE",
		"delete",
		"PATCH",
		"patch",
		"CUSTOM",
	}

	testEndpoints := []string{
		"/containers/create",
		"/v1.45/containers/create",
		"/containers/json",
		"/images/create",
		"/v1.45/images/create",
		"/networks/create",
		"/volumes/create",
		"/_ping",
		"/v1.45/_ping",
		"/version",
		"/info",
		"/events",
		"/v1.44/events",
		"/exec/123/start",
		"/build",
		"/commit",
	}

	for _, method := range forbiddenMethods {
		for _, endpoint := range testEndpoints {
			t.Run(method+" "+endpoint, func(t *testing.T) {
				status, reason := Check(method, endpoint, cfg)
				if status != http.StatusForbidden {
					t.Errorf("Check(%q, %q) status = %d, want %d", method, endpoint, status, http.StatusForbidden)
				}
				if reason != ReasonReadOnly {
					t.Errorf("Check(%q, %q) reason = %q, want %q", method, endpoint, reason, ReasonReadOnly)
				}
			})
		}
	}

	t.Run("GET and HEAD are permitted", func(t *testing.T) {
		permittedMethods := []string{
			http.MethodGet,
			http.MethodHead,
			"get",
			"head",
		}
		for _, m := range permittedMethods {
			status, reason := Check(m, "/_ping", cfg)
			if status != http.StatusOK {
				t.Errorf("Check(%q, %q) status = %d, want %d", m, "/_ping", status, http.StatusOK)
			}
			if reason != "" {
				t.Errorf("Check(%q, %q) reason = %q, want empty", m, "/_ping", reason)
			}
		}
	})
}

func TestEventsACL(t *testing.T) {
	endpoints := []string{
		"/events",
		"/events/",
		"/events/stream",
		"/v1.44/events",
		"/v1.44/events/",
		"/v1.44/events/subpath",
		"/v1.45/events",
	}

	for _, endpoint := range endpoints {
		for _, eventsFlag := range []bool{false, true} {
			cfg := config.ACLConfig{
				Events: eventsFlag,
			}

			for _, method := range []string{http.MethodGet, http.MethodHead} {
				t.Run(method+" "+endpoint+FormFlagString(eventsFlag), func(t *testing.T) {
					status, reason := Check(method, endpoint, cfg)
					if eventsFlag {
						if status != http.StatusOK {
							t.Errorf("Check(%q, %q, Events=true) status = %d, want %d",
								method, endpoint, status, http.StatusOK)
						}
						if reason != "" {
							t.Errorf("Check(%q, %q, Events=true) reason = %q, want empty",
								method, endpoint, reason)
						}
					} else {
						if status != http.StatusForbidden {
							t.Errorf("Check(%q, %q, Events=false) status = %d, want %d",
								method, endpoint, status, http.StatusForbidden)
						}
						if reason != ReasonACLDenied {
							t.Errorf("Check(%q, %q, Events=false) reason = %q, want %q",
								method, endpoint, reason, ReasonACLDenied)
						}
					}
				})
			}
		}
	}
}

func FormFlagString(b bool) string {
	if b {
		return "_EventsTrue"
	}
	return "_EventsFalse"
}

func TestACLCategories(t *testing.T) {
	type categoryTest struct {
		name       string
		setFlag    func(cfg *config.ACLConfig, val bool)
		validPaths []string
	}

	categories := []categoryTest{
		{
			name: "Ping",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Ping = val
			},
			validPaths: []string{"/_ping", "/v1.45/_ping", "/_ping/"},
		},
		{
			name: "Version",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Version = val
			},
			validPaths: []string{"/version", "/v1.45/version", "/version/"},
		},
		{
			name: "Info",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Info = val
			},
			validPaths: []string{"/info", "/v1.45/info", "/info/"},
		},
		{
			name: "Containers",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Containers = val
			},
			validPaths: []string{
				"/containers",
				"/containers/json",
				"/containers/abc123/logs",
				"/v1.45/containers",
				"/v1.45/containers/json",
				"/v1.45/containers/abc123/json",
			},
		},
		{
			name: "Images",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Images = val
			},
			validPaths: []string{
				"/images",
				"/images/json",
				"/images/ubuntu/json",
				"/v1.45/images",
				"/v1.45/images/json",
			},
		},
		{
			name: "Networks",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Networks = val
			},
			validPaths: []string{
				"/networks",
				"/networks/bridge",
				"/v1.45/networks",
				"/v1.45/networks/bridge",
			},
		},
		{
			name: "Volumes",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Volumes = val
			},
			validPaths: []string{
				"/volumes",
				"/volumes/my-vol",
				"/v1.45/volumes",
				"/v1.45/volumes/my-vol",
			},
		},
		{
			name: "Services",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Services = val
			},
			validPaths: []string{
				"/services",
				"/services/my-svc",
				"/v1.45/services",
				"/v1.45/services/my-svc",
			},
		},
		{
			name: "Tasks",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Tasks = val
			},
			validPaths: []string{
				"/tasks",
				"/tasks/my-task",
				"/v1.45/tasks",
				"/v1.45/tasks/my-task",
			},
		},
		{
			name: "Nodes",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Nodes = val
			},
			validPaths: []string{
				"/nodes",
				"/nodes/my-node",
				"/v1.45/nodes",
				"/v1.45/nodes/my-node",
			},
		},
		{
			name: "Swarm",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Swarm = val
			},
			validPaths: []string{
				"/swarm",
				"/swarm/unlockkey",
				"/v1.45/swarm",
				"/v1.45/swarm/unlockkey",
			},
		},
		{
			name: "Secrets",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Secrets = val
			},
			validPaths: []string{
				"/secrets",
				"/secrets/my-sec",
				"/v1.45/secrets",
				"/v1.45/secrets/my-sec",
			},
		},
		{
			name: "Configs",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Configs = val
			},
			validPaths: []string{
				"/configs",
				"/configs/my-cfg",
				"/v1.45/configs",
				"/v1.45/configs/my-cfg",
			},
		},
		{
			name: "System",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.System = val
			},
			validPaths: []string{
				"/system",
				"/system/df",
				"/v1.45/system",
				"/v1.45/system/df",
			},
		},
		{
			name: "Distribution",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Distribution = val
			},
			validPaths: []string{
				"/distribution",
				"/distribution/my-img/json",
				"/v1.45/distribution",
				"/v1.45/distribution/my-img/json",
			},
		},
		{
			name: "Plugins",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Plugins = val
			},
			validPaths: []string{
				"/plugins",
				"/plugins/my-plugin",
				"/v1.45/plugins",
				"/v1.45/plugins/my-plugin",
			},
		},
		{
			name: "Events",
			setFlag: func(cfg *config.ACLConfig, val bool) {
				cfg.Events = val
			},
			validPaths: []string{
				"/events",
				"/events/stream",
				"/v1.44/events",
				"/v1.44/events/stream",
				"/v1.45/events",
			},
		},
	}

	for _, cat := range categories {
		t.Run(cat.name+"_Enabled", func(t *testing.T) {
			var cfg config.ACLConfig
			cat.setFlag(&cfg, true)

			for _, p := range cat.validPaths {
				status, reason := Check(http.MethodGet, p, cfg)
				if status != http.StatusOK {
					t.Errorf("Check(GET, %q) with %s=true: status = %d, want %d", p, cat.name, status, http.StatusOK)
				}
				if reason != "" {
					t.Errorf("Check(GET, %q) with %s=true: reason = %q, want empty", p, cat.name, reason)
				}
			}
		})

		t.Run(cat.name+"_Disabled", func(t *testing.T) {
			var cfg config.ACLConfig
			cat.setFlag(&cfg, false)

			for _, p := range cat.validPaths {
				status, reason := Check(http.MethodGet, p, cfg)
				if status != http.StatusForbidden {
					t.Errorf("Check(GET, %q) with %s=false: status = %d, want %d", p, cat.name, status, http.StatusForbidden)
				}
				if reason != ReasonACLDenied {
					t.Errorf("Check(GET, %q) with %s=false: reason = %q, want %q", p, cat.name, reason, ReasonACLDenied)
				}
			}
		})
	}
}

func TestUnknownAndDeniedPaths(t *testing.T) {
	cfg := allACLsEnabled()

	deniedPaths := []string{
		"/",
		"",
		"/exec/123/json",
		"/v1.45/exec/123/json",
		"/build",
		"/v1.45/build",
		"/commit",
		"/v1.45/commit",
		"/auth",
		"/v1.45/auth",
		"/session",
		"/v1.45/session",
		"/unknown",
		"/containers_invalid",
		"/images_invalid",
		"/_ping/subpath",
		"/version/subpath",
		"/info/subpath",
	}

	for _, p := range deniedPaths {
		t.Run("GET "+p, func(t *testing.T) {
			status, reason := Check(http.MethodGet, p, cfg)
			if status != http.StatusForbidden {
				t.Errorf("Check(GET, %q) status = %d, want %d", p, status, http.StatusForbidden)
			}
			if reason != ReasonACLDenied {
				t.Errorf("Check(GET, %q) reason = %q, want %q", p, reason, ReasonACLDenied)
			}
		})
	}
}

func TestMiddleware(t *testing.T) {
	t.Run("allowed request forwards to next handler", func(t *testing.T) {
		cfg := config.ACLConfig{Containers: true}

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.Header().Set("X-Custom-Header", "allowed")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"Id":"c1"}]`))
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodGet, "/v1.45/containers/json", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if !nextCalled {
			t.Fatal("expected next handler to be called")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusOK)
		}
		if rec.Header().Get("X-Custom-Header") != "allowed" {
			t.Errorf("header X-Custom-Header = %q, want %q", rec.Header().Get("X-Custom-Header"), "allowed")
		}
		if rec.Body.String() != `[{"Id":"c1"}]` {
			t.Errorf("body = %q, want %q", rec.Body.String(), `[{"Id":"c1"}]`)
		}
	})

	t.Run("allowed HEAD request forwards to next handler", func(t *testing.T) {
		cfg := config.ACLConfig{Ping: true}

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodHead, "/_ping", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if !nextCalled {
			t.Fatal("expected next handler to be called")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("read-only blocked POST returns 403 JSON", func(t *testing.T) {
		cfg := allACLsEnabled()

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodPost, "/v1.45/containers/create", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if nextCalled {
			t.Fatal("expected next handler NOT to be called")
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json")
		}

		var resp errorResponse
		body, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("failed to unmarshal JSON body %q: %v", string(body), err)
		}
		if resp.Message != ReasonReadOnly {
			t.Errorf("resp.Message = %q, want %q", resp.Message, ReasonReadOnly)
		}
	})

	t.Run("ACL denied returns 403 JSON", func(t *testing.T) {
		cfg := config.ACLConfig{Containers: false}

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodGet, "/v1.45/containers/json", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if nextCalled {
			t.Fatal("expected next handler NOT to be called")
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json")
		}

		var resp errorResponse
		body, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("failed to unmarshal JSON body %q: %v", string(body), err)
		}
		if resp.Message != ReasonACLDenied {
			t.Errorf("resp.Message = %q, want %q", resp.Message, ReasonACLDenied)
		}
	})

	t.Run("events endpoint returns 403 when disabled", func(t *testing.T) {
		cfg := allACLsEnabled()
		cfg.Events = false

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodGet, "/v1.44/events", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if nextCalled {
			t.Fatal("expected next handler NOT to be called")
		}
		if rec.Code != http.StatusForbidden {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json")
		}

		var resp errorResponse
		body, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("failed to unmarshal JSON body %q: %v", string(body), err)
		}
		if resp.Message != ReasonACLDenied {
			t.Errorf("resp.Message = %q, want %q", resp.Message, ReasonACLDenied)
		}
	})

	t.Run("events endpoint forwards when enabled", func(t *testing.T) {
		cfg := allACLsEnabled()
		cfg.Events = true

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		handler := Middleware(cfg, next)

		req := httptest.NewRequest(http.MethodGet, "/v1.44/events", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if !nextCalled {
			t.Fatal("expected next handler to be called")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}
