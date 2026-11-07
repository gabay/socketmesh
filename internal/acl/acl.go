package acl

import (
	"encoding/json"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/socketmesh/socketmesh/internal/config"
)

// Reason messages returned by Check and Middleware.
const (
	ReasonReadOnly             = "method not allowed: socketmesh is strictly read-only"
	ReasonEventsNotImplemented = "events endpoint is not supported"
	ReasonACLDenied            = "access denied by socketmesh ACL"
)

var versionRegex = regexp.MustCompile(`^(/v\d+\.\d+)(/.*)?$`)

type errorResponse struct {
	Message string `json:"message"`
}

// StripVersionPrefix cleans the path and strips an optional Docker API version prefix matching /v\d+\.\d+.
func StripVersionPrefix(rawPath string) (versionPrefix string, cleanPath string) {
	if idx := strings.IndexAny(rawPath, "?#"); idx != -1 {
		rawPath = rawPath[:idx]
	}
	cleaned := path.Clean("/" + rawPath)

	matches := versionRegex.FindStringSubmatch(cleaned)
	if matches != nil {
		versionPrefix = matches[1]
		cleanPath = matches[2]
		if cleanPath == "" {
			cleanPath = "/"
		}
		return versionPrefix, cleanPath
	}

	return "", cleaned
}

// Check evaluates whether an incoming HTTP request is permitted under the given ACL configuration.
func Check(method string, rawPath string, cfg config.ACLConfig) (status int, reason string) {
	m := strings.ToUpper(strings.TrimSpace(method))
	if m != http.MethodGet && m != http.MethodHead {
		return http.StatusForbidden, ReasonReadOnly
	}

	_, cleanPath := StripVersionPrefix(rawPath)

	if cleanPath == "/events" || strings.HasPrefix(cleanPath, "/events/") {
		return http.StatusNotImplemented, ReasonEventsNotImplemented
	}

	switch {
	case cleanPath == "/_ping":
		if cfg.Ping {
			return http.StatusOK, ""
		}
	case cleanPath == "/version":
		if cfg.Version {
			return http.StatusOK, ""
		}
	case cleanPath == "/info":
		if cfg.Info {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/containers"):
		if cfg.Containers {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/images"):
		if cfg.Images {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/networks"):
		if cfg.Networks {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/volumes"):
		if cfg.Volumes {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/services"):
		if cfg.Services {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/tasks"):
		if cfg.Tasks {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/nodes"):
		if cfg.Nodes {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/swarm"):
		if cfg.Swarm {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/secrets"):
		if cfg.Secrets {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/configs"):
		if cfg.Configs {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/system"):
		if cfg.System {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/distribution"):
		if cfg.Distribution {
			return http.StatusOK, ""
		}
	case matchCategory(cleanPath, "/plugins"):
		if cfg.Plugins {
			return http.StatusOK, ""
		}
	default:
		return http.StatusForbidden, ReasonACLDenied
	}

	return http.StatusForbidden, ReasonACLDenied
}

func matchCategory(p, prefix string) bool {
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

// Middleware returns an HTTP handler that enforces SocketMesh ACL rules before forwarding to next.
func Middleware(cfg config.ACLConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status, reason := Check(r.Method, r.URL.Path, cfg)
		if status != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(errorResponse{Message: reason})
			return
		}
		if next != nil {
			next.ServeHTTP(w, r)
		}
	})
}
