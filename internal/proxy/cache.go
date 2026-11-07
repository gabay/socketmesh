package proxy

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// CacheEntry represents a cached HTTP response.
type CacheEntry struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	ExpiresAt  time.Time
}

// ResponseCache is a thread-safe in-memory cache for Docker API responses.
type ResponseCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]*CacheEntry
}

// NewResponseCache initializes a ResponseCache with the given TTL duration.
func NewResponseCache(ttl time.Duration) *ResponseCache {
	return &ResponseCache{
		ttl:     ttl,
		entries: make(map[string]*CacheEntry),
	}
}

// CacheKey constructs the cache key from HTTP method, cleanPath, and rawQuery.
// Format: Method + CleanPath + "?" + RawQuery
func CacheKey(method, cleanPath, rawQuery string) string {
	return method + cleanPath + "?" + rawQuery
}

// IsCacheable reports whether a request is eligible for caching.
// Streaming requests, events, and non-GET/HEAD methods are never cached.
func IsCacheable(method, cleanPath string) bool {
	m := strings.ToUpper(strings.TrimSpace(method))
	if m != http.MethodGet && m != http.MethodHead {
		return false
	}

	// Never cache streaming requests
	if strings.HasSuffix(cleanPath, "/logs") ||
		strings.HasSuffix(cleanPath, "/stats") ||
		strings.HasSuffix(cleanPath, "/archive") ||
		strings.HasSuffix(cleanPath, "/get") ||
		cleanPath == "/images/get" {
		return false
	}

	// Never cache events
	if cleanPath == "/events" || strings.HasPrefix(cleanPath, "/events/") {
		return false
	}

	return true
}

// Get looks up a cached entry by key, returning it if present and unexpired.
// Expired entries are removed upon retrieval.
func (c *ResponseCache) Get(key string) (*CacheEntry, bool) {
	if c == nil || c.ttl <= 0 {
		return nil, false
	}

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}

	return entry, true
}

// Set stores a response in the cache with the configured TTL.
func (c *ResponseCache) Set(key string, statusCode int, header http.Header, body []byte) {
	if c == nil || c.ttl <= 0 {
		return
	}

	// Only cache successful 2xx responses
	if statusCode < 200 || statusCode >= 300 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = &CacheEntry{
		StatusCode: statusCode,
		Header:     header.Clone(),
		Body:       body,
		ExpiresAt:  time.Now().Add(c.ttl),
	}
}

// Delete removes a key from the cache.
func (c *ResponseCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// Len returns the current number of cached entries.
func (c *ResponseCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
