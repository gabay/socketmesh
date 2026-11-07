package proxy

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestResponseCache_Basic(t *testing.T) {
	ttl := 50 * time.Millisecond
	cache := NewResponseCache(ttl)

	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	header.Set("Api-Version", "1.45")

	key := CacheKey("GET", "/containers/json", "")
	body := []byte(`[{"Id":"123"}]`)

	cache.Set(key, http.StatusOK, header, body)

	entry, ok := cache.Get(key)
	if !ok || entry == nil {
		t.Fatalf("expected cache hit")
	}
	if entry.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", entry.StatusCode)
	}
	if string(entry.Body) != string(body) {
		t.Fatalf("expected body %s, got %s", body, entry.Body)
	}
	if entry.Header.Get("Api-Version") != "1.45" {
		t.Fatalf("expected Api-Version 1.45, got %s", entry.Header.Get("Api-Version"))
	}

	// Wait for TTL expiration
	time.Sleep(70 * time.Millisecond)

	_, ok = cache.Get(key)
	if ok {
		t.Fatalf("expected cache entry to be expired")
	}
}

func TestResponseCache_IsCacheable(t *testing.T) {
	tests := []struct {
		method    string
		cleanPath string
		expected  bool
	}{
		{"GET", "/containers/json", true},
		{"GET", "/info", true},
		{"GET", "/version", true},
		{"GET", "/images/json", true},
		{"GET", "/volumes", true},
		{"HEAD", "/_ping", true},
		// Streaming endpoints
		{"GET", "/containers/123/logs", false},
		{"GET", "/containers/123/stats", false},
		{"GET", "/containers/123/archive", false},
		{"GET", "/images/get", false},
		{"GET", "/images/alpine/get", false},
		// Events
		{"GET", "/events", false},
		// Non-GET/HEAD
		{"POST", "/containers/create", false},
		{"DELETE", "/containers/123", false},
	}

	for _, tc := range tests {
		got := IsCacheable(tc.method, tc.cleanPath)
		if got != tc.expected {
			t.Errorf("IsCacheable(%q, %q) = %v; want %v", tc.method, tc.cleanPath, got, tc.expected)
		}
	}
}

func TestResponseCache_Concurrency(t *testing.T) {
	cache := NewResponseCache(500 * time.Millisecond)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := CacheKey("GET", "/containers/json", "")
			cache.Set(key, http.StatusOK, http.Header{}, []byte("data"))
			_, _ = cache.Get(key)
		}(i)
	}

	wg.Wait()
}
