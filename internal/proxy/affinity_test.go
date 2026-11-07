package proxy

import (
	"fmt"
	"sync"
	"testing"
)

func TestAffinityCache_Containers(t *testing.T) {
	cache := NewAffinityCache()

	fullID := "a1b2c3d4e5f67890123456789012345678901234567890123456789012345678"
	shortID := fullID[:12]
	endpoint := "http://node1:2375"

	cache.SetContainer(fullID, []string{"/web", "frontend"}, endpoint)

	// Full ID match (case-insensitive)
	ep, ok := cache.GetContainer(fullID)
	if !ok || ep != endpoint {
		t.Fatalf("expected %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Short ID match
	ep, ok = cache.GetContainer(shortID)
	if !ok || ep != endpoint {
		t.Fatalf("expected short ID match %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Name with leading slash
	ep, ok = cache.GetContainer("/web")
	if !ok || ep != endpoint {
		t.Fatalf("expected /web match %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Name without leading slash queried with leading slash
	ep, ok = cache.GetContainer("web")
	if !ok || ep != endpoint {
		t.Fatalf("expected web match %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Name without leading slash registered
	ep, ok = cache.GetContainer("frontend")
	if !ok || ep != endpoint {
		t.Fatalf("expected frontend match %s, got %s (ok=%v)", endpoint, ep, ok)
	}
	ep, ok = cache.GetContainer("/frontend")
	if !ok || ep != endpoint {
		t.Fatalf("expected /frontend match %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Hex prefix match >= 6 chars
	ep, ok = cache.GetContainer("a1b2c3d4")
	if !ok || ep != endpoint {
		t.Fatalf("expected prefix a1b2c3d4 match %s, got %s (ok=%v)", endpoint, ep, ok)
	}

	// Non-matching query
	_, ok = cache.GetContainer("nonexistent")
	if ok {
		t.Fatalf("expected nonexistent container not to match")
	}

	// Hex prefix < 6 chars should not match via prefix
	_, ok = cache.GetContainer("a1b2c")
	if ok {
		t.Fatalf("expected 5-char hex prefix not to match")
	}
}

func TestAffinityCache_Generic(t *testing.T) {
	cache := NewAffinityCache()

	cache.Set("networks", "net-123", "http://node2:2375")
	cache.Set("volumes", "vol-data", "http://node3:2375")
	cache.Set("images", "alpine:latest", "http://node1:2375")

	if ep, ok := cache.Get("networks", "net-123"); !ok || ep != "http://node2:2375" {
		t.Fatalf("unexpected network affinity: %s (ok=%v)", ep, ok)
	}
	if ep, ok := cache.Get("volumes", "vol-data"); !ok || ep != "http://node3:2375" {
		t.Fatalf("unexpected volume affinity: %s (ok=%v)", ep, ok)
	}
	if ep, ok := cache.Get("images", "alpine:latest"); !ok || ep != "http://node1:2375" {
		t.Fatalf("unexpected image affinity: %s (ok=%v)", ep, ok)
	}
	if _, ok := cache.Get("networks", "unknown"); ok {
		t.Fatalf("expected unknown network not found")
	}
}

func TestAffinityCache_Concurrency(t *testing.T) {
	cache := NewAffinityCache()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fullID := fmt.Sprintf("%064x", id)
			name := fmt.Sprintf("container-%d", id)
			ep := fmt.Sprintf("http://node-%d:2375", id%3)

			cache.SetContainer(fullID, []string{name}, ep)
			_, _ = cache.GetContainer(fullID)
			_, _ = cache.GetContainer(name)
			_, _ = cache.GetContainer(fullID[:8])
		}(i)
	}

	wg.Wait()
}
