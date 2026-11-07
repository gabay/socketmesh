package proxy

import (
	"strings"
	"sync"
)

// AffinityCache provides thread-safe mapping of object identifiers (containers, images,
// volumes, networks) to downstream endpoint URLs.
type AffinityCache struct {
	mu         sync.RWMutex
	containers map[string]string            // exact matches: full ID, 12-char ID, names (with & without /)
	knownIDs   map[string]string            // full container ID (lowercased) -> endpoint for prefix search
	generic    map[string]map[string]string // category -> id -> endpoint
}

// NewAffinityCache creates an empty, thread-safe AffinityCache.
func NewAffinityCache() *AffinityCache {
	return &AffinityCache{
		containers: make(map[string]string),
		knownIDs:   make(map[string]string),
		generic:    make(map[string]map[string]string),
	}
}

// isHexString reports whether s consists entirely of valid hexadecimal characters.
func isHexString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
			return false
		}
	}
	return true
}

// SetContainer records mapping for a container's full ID, 12-char short ID, and names
// (both with and without leading slash) to the specified downstream endpoint.
func (c *AffinityCache) SetContainer(id string, names []string, endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id = strings.TrimSpace(id)
	if id != "" {
		c.containers[id] = endpoint
		c.containers[strings.ToLower(id)] = endpoint
		c.knownIDs[strings.ToLower(id)] = endpoint

		if len(id) >= 12 {
			c.containers[id[:12]] = endpoint
			c.containers[strings.ToLower(id[:12])] = endpoint
		}
	}

	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		c.containers[trimmed] = endpoint
		withSlash := "/" + strings.TrimPrefix(trimmed, "/")
		withoutSlash := strings.TrimPrefix(trimmed, "/")
		c.containers[withSlash] = endpoint
		c.containers[withoutSlash] = endpoint
	}
}

// GetContainer looks up the downstream endpoint for a container by full ID, short ID,
// container name (with or without leading slash), or hex prefix lookup (>= 6 hex chars).
func (c *AffinityCache) GetContainer(query string) (string, bool) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 1. Exact match in containers map
	if ep, ok := c.containers[query]; ok {
		return ep, true
	}
	if ep, ok := c.containers[strings.ToLower(query)]; ok {
		return ep, true
	}

	// 2. Normalized with and without leading slash
	withSlash := "/" + strings.TrimPrefix(query, "/")
	if ep, ok := c.containers[withSlash]; ok {
		return ep, true
	}
	withoutSlash := strings.TrimPrefix(query, "/")
	if ep, ok := c.containers[withoutSlash]; ok {
		return ep, true
	}

	// 3. Hex prefix lookup if length >= 6
	if len(query) >= 6 && isHexString(query) {
		lower := strings.ToLower(query)
		for id, ep := range c.knownIDs {
			if strings.HasPrefix(id, lower) {
				return ep, true
			}
		}
	}

	return "", false
}

// Set records a generic category:id -> endpoint mapping (e.g. for volumes, networks, images).
func (c *AffinityCache) Set(category, id, endpoint string) {
	if category == "containers" {
		c.SetContainer(id, nil, endpoint)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	category = strings.ToLower(strings.TrimSpace(category))
	id = strings.TrimSpace(id)
	if category == "" || id == "" {
		return
	}

	m, ok := c.generic[category]
	if !ok {
		m = make(map[string]string)
		c.generic[category] = m
	}
	m[id] = endpoint
	m[strings.ToLower(id)] = endpoint
	if strings.HasPrefix(id, "/") {
		m[strings.TrimPrefix(id, "/")] = endpoint
	} else {
		m["/"+id] = endpoint
	}
}

// Get retrieves a generic category:id -> endpoint mapping.
func (c *AffinityCache) Get(category, id string) (string, bool) {
	if category == "containers" {
		return c.GetContainer(id)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	category = strings.ToLower(strings.TrimSpace(category))
	id = strings.TrimSpace(id)
	if category == "" || id == "" {
		return "", false
	}

	m, ok := c.generic[category]
	if !ok {
		return "", false
	}

	if ep, ok := m[id]; ok {
		return ep, true
	}
	if ep, ok := m[strings.ToLower(id)]; ok {
		return ep, true
	}
	if ep, ok := m[strings.TrimPrefix(id, "/")]; ok {
		return ep, true
	}
	if ep, ok := m["/"+strings.TrimPrefix(id, "/")]; ok {
		return ep, true
	}

	return "", false
}
