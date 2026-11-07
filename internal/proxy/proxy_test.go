package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/socketmesh/socketmesh/internal/config"
	"github.com/socketmesh/socketmesh/internal/discovery"
)

func testACL() config.ACLConfig {
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

// Test 1: Fan-out GET /v1.45/containers/json merging containers from Node 1 and Node 2,
// leaving JSON pure, and populating the affinity cache.
func TestProxy_ContainersFanOutAndAffinity(t *testing.T) {
	c1ID := "c111111111111111111111111111111111111111111111111111111111111111"
	c2ID := "c222222222222222222222222222222222222222222222222222222222222222"

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/containers/json") {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Api-Version", "1.45")
			_, _ = w.Write([]byte(fmt.Sprintf(`[{"Id":"%s","Names":["/web"],"Image":"nginx:latest"}]`, c1ID)))
			return
		}
		http.NotFound(w, r)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/containers/json") {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Api-Version", "1.45")
			_, _ = w.Write([]byte(fmt.Sprintf(`[{"Id":"%s","Names":["/db"],"Image":"postgres:latest"}]`, c2ID)))
			return
		}
		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	req := httptest.NewRequest(http.MethodGet, "/v1.45/containers/json", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var containers []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &containers); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(containers))
	}

	// Verify purity: check that custom labels were NOT injected
	for _, c := range containers {
		if _, hasInjected := c["SocketMesh"]; hasInjected {
			t.Fatalf("container JSON was mutated: %v", c)
		}
	}

	// Verify affinity cache was automatically populated
	ep1, ok := p.Affinity().GetContainer(c1ID)
	if !ok || !strings.Contains(ep1, node1.Listener.Addr().String()) {
		t.Errorf("affinity for C1 expected %s, got %s (ok=%v)", node1.URL, ep1, ok)
	}
	ep1Short, ok := p.Affinity().GetContainer(c1ID[:12])
	if !ok || !strings.Contains(ep1Short, node1.Listener.Addr().String()) {
		t.Errorf("affinity for C1 short ID failed: %s (ok=%v)", ep1Short, ok)
	}
	ep1Name, ok := p.Affinity().GetContainer("web")
	if !ok || !strings.Contains(ep1Name, node1.Listener.Addr().String()) {
		t.Errorf("affinity for C1 name 'web' failed: %s (ok=%v)", ep1Name, ok)
	}

	ep2, ok := p.Affinity().GetContainer(c2ID)
	if !ok || !strings.Contains(ep2, node2.Listener.Addr().String()) {
		t.Errorf("affinity for C2 expected %s, got %s (ok=%v)", node2.URL, ep2, ok)
	}
	ep2Name, ok := p.Affinity().GetContainer("/db")
	if !ok || !strings.Contains(ep2Name, node2.Listener.Addr().String()) {
		t.Errorf("affinity for C2 name '/db' failed: %s (ok=%v)", ep2Name, ok)
	}
}

// Test 2: Partial downstream failure: Node 1 is down, Node 2 is up -> GET /containers/json still
// returns 200 OK with Node 2's containers, and Node 1 is marked unhealthy in discovery.Manager!
func TestProxy_PartialDownstreamFailure(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Will be closed immediately
	}))
	node1URL := node1.URL
	node1.Close() // Simulate dead/down node

	c2ID := "c222222222222222222222222222222222222222222222222222222222222222"
	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/containers/json") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fmt.Sprintf(`[{"Id":"%s","Names":["/app2"]}]`, c2ID)))
			return
		}
		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	req := httptest.NewRequest(http.MethodGet, "/containers/json", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var containers []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &containers); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(containers) != 1 {
		t.Fatalf("expected 1 container from healthy Node 2, got %d", len(containers))
	}
	if containers[0]["Id"] != c2ID {
		t.Fatalf("expected container %s, got %v", c2ID, containers[0]["Id"])
	}

	// Verify Node 1 is marked unhealthy in discovery.Manager
	if disc.IsHealthy(node1URL) {
		t.Fatalf("expected failed Node 1 to be marked unhealthy in discovery")
	}
	// Verify Node 2 remains healthy
	if !disc.IsHealthy(node2.URL) {
		t.Fatalf("expected healthy Node 2 to remain healthy")
	}
}

// Test 3: Targeted GET /containers/{id}/json and GET /containers/{id}/logs?follow=1
// - Verify cache hit routes ONLY to owning node (Node 2) without hitting Node 1
// - Verify cache miss probes nodes, finds Node 2, caches it, and streams logs from Node 2
// - Verify 404 when container does not exist on any node
func TestProxy_TargetedContainerRouting(t *testing.T) {
	c2ID := "c222222222222222222222222222222222222222222222222222222222222222"
	c3ID := "c33333333333333333333333333333333333333333333333333333333333333"

	var node1Hits atomic.Int64
	var node2Hits atomic.Int64

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node1Hits.Add(1)
		http.NotFound(w, r)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node2Hits.Add(1)
		path := r.URL.Path

		if strings.Contains(path, c2ID[:12]) || strings.Contains(path, "app2") {
			if strings.HasSuffix(path, "/json") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(fmt.Sprintf(`{"Id":"%s","Name":"/app2"}`, c2ID)))
				return
			}
			if strings.HasSuffix(path, "/logs") {
				w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
				flusher, _ := w.(http.Flusher)
				_, _ = w.Write([]byte("log-chunk-1\n"))
				if flusher != nil {
					flusher.Flush()
				}
				_, _ = w.Write([]byte("log-chunk-2\n"))
				return
			}
		}

		if strings.Contains(path, c3ID[:12]) || strings.Contains(path, "app3") {
			if strings.HasSuffix(path, "/json") {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(fmt.Sprintf(`{"Id":"%s","Name":"/app3"}`, c3ID)))
				return
			}
			if strings.HasSuffix(path, "/logs") {
				w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
				_, _ = w.Write([]byte("logs-from-node2-for-c3\n"))
				return
			}
		}

		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	// Subtest A: Pre-populate affinity for C2 -> Node 2 (simulating prior list)
	p.Affinity().SetContainer(c2ID, []string{"/app2"}, node2.URL)

	node1Before := node1Hits.Load()
	node2Before := node2Hits.Load()

	// Hit /containers/{id}/logs?follow=1 with cache hit
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1.45/containers/%s/logs?follow=1", c2ID[:12]), nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for logs, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "log-chunk-1") {
		t.Fatalf("expected log stream content, got %s", rec.Body.String())
	}

	// Verify hit routed ONLY to Node 2 without touching Node 1!
	if node1Hits.Load() != node1Before {
		t.Fatalf("expected 0 hits to Node 1 on affinity hit, got %d additional hits", node1Hits.Load()-node1Before)
	}
	if node2Hits.Load() <= node2Before {
		t.Fatalf("expected Node 2 hits to increase")
	}

	// Subtest B: Cache miss for C3 (unknown to affinity)
	// Query logs by short ID: should probe nodes, find Node 2, cache affinity, and stream logs
	reqMiss := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1.45/containers/%s/logs?follow=1", c3ID[:12]), nil)
	recMiss := httptest.NewRecorder()
	p.ServeHTTP(recMiss, reqMiss)

	if recMiss.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on probe & stream, got %d: %s", recMiss.Code, recMiss.Body.String())
	}
	if !strings.Contains(recMiss.Body.String(), "logs-from-node2-for-c3") {
		t.Fatalf("expected streamed logs from Node 2, got %s", recMiss.Body.String())
	}

	// Verify C3 is now in affinity cache
	cachedEP, ok := p.Affinity().GetContainer(c3ID)
	if !ok || !strings.Contains(cachedEP, node2.Listener.Addr().String()) {
		t.Fatalf("expected C3 to be cached to Node 2 after probe, got %s (ok=%v)", cachedEP, ok)
	}

	// Subtest C: Container that does NOT exist on any node
	req404 := httptest.NewRequest(http.MethodGet, "/v1.45/containers/nonexistent/json", nil)
	rec404 := httptest.NewRecorder()
	p.ServeHTTP(rec404, req404)

	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent container, got %d", rec404.Code)
	}

	req404Logs := httptest.NewRequest(http.MethodGet, "/v1.45/containers/nonexistent/logs?follow=1", nil)
	rec404Logs := httptest.NewRecorder()
	p.ServeHTTP(rec404Logs, req404Logs)

	if rec404Logs.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent container logs, got %d", rec404Logs.Code)
	}
}

// Test 4: Swarm Manager Failover
// Node 1 returns 503 ("This node is not a swarm manager")
// Node 2 returns 200 OK with services
// SocketMesh automatically fails over and returns Node 2's 200 OK
func TestProxy_SwarmManagerFailover(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"This node is not a swarm manager"}`))
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/services") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"ID":"svc-1","Spec":{"Name":"web"}}]`))
			return
		}
		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	req := httptest.NewRequest(http.MethodGet, "/v1.45/services", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK after swarm manager failover, got %d: %s", rec.Code, rec.Body.String())
	}

	var svcs []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &svcs); err != nil {
		t.Fatalf("failed to unmarshal services: %v", err)
	}
	if len(svcs) != 1 || svcs[0]["ID"] != "svc-1" {
		t.Fatalf("expected svc-1 from Node 2, got %v", svcs)
	}
}

// Test 5: Aggregated GET /info summing Containers, ContainersRunning, NCPU, MemTotal across nodes
func TestProxy_ClusterInfoAggregation(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"ID": "node-1",
			"Containers": 3,
			"ContainersRunning": 2,
			"ContainersPaused": 1,
			"ContainersStopped": 0,
			"NCPU": 4,
			"MemTotal": 8589934592,
			"Images": 5
		}`))
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"ID": "node-2",
			"Containers": 5,
			"ContainersRunning": 4,
			"ContainersPaused": 0,
			"ContainersStopped": 1,
			"NCPU": 8,
			"MemTotal": 17179869184,
			"Images": 7
		}`))
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()

	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("failed to unmarshal info: %v", err)
	}

	if int64(toFloat(info["Containers"])) != 8 {
		t.Errorf("expected Containers 8, got %v", info["Containers"])
	}
	if int64(toFloat(info["ContainersRunning"])) != 6 {
		t.Errorf("expected ContainersRunning 6, got %v", info["ContainersRunning"])
	}
	if int64(toFloat(info["ContainersPaused"])) != 1 {
		t.Errorf("expected ContainersPaused 1, got %v", info["ContainersPaused"])
	}
	if int64(toFloat(info["ContainersStopped"])) != 1 {
		t.Errorf("expected ContainersStopped 1, got %v", info["ContainersStopped"])
	}
	if int64(toFloat(info["NCPU"])) != 12 {
		t.Errorf("expected NCPU 12, got %v", info["NCPU"])
	}
	if int64(toFloat(info["MemTotal"])) != 25769803776 {
		t.Errorf("expected MemTotal 25769803776, got %v", info["MemTotal"])
	}
}

// Test 6: Aggregated GET /volumes, GET /images/json, GET /networks
func TestProxy_AggregatedResources(t *testing.T) {
	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/volumes"):
			_, _ = w.Write([]byte(`{"Volumes":[{"Name":"vol1","Driver":"local","Mountpoint":"/data1"}],"Warnings":[]}`))
		case strings.HasSuffix(r.URL.Path, "/images/json"):
			_, _ = w.Write([]byte(`[{"Id":"sha256:img1","RepoTags":["app:v1"]}]`))
		case strings.HasSuffix(r.URL.Path, "/networks"):
			_, _ = w.Write([]byte(`[{"Id":"net1","Name":"bridge"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/volumes"):
			// includes duplicate vol1 and unique vol2
			_, _ = w.Write([]byte(`{"Volumes":[{"Name":"vol1","Driver":"local","Mountpoint":"/data1"},{"Name":"vol2","Driver":"local","Mountpoint":"/data2"}],"Warnings":["warn-node2"]}`))
		case strings.HasSuffix(r.URL.Path, "/images/json"):
			// includes duplicate img1 and unique img2
			_, _ = w.Write([]byte(`[{"Id":"sha256:img1","RepoTags":["app:v1"]},{"Id":"sha256:img2","RepoTags":["app:v2"]}]`))
		case strings.HasSuffix(r.URL.Path, "/networks"):
			_, _ = w.Write([]byte(`[{"Id":"net2","Name":"overlay"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	// Volumes
	recV := httptest.NewRecorder()
	p.ServeHTTP(recV, httptest.NewRequest(http.MethodGet, "/volumes", nil))
	if recV.Code != http.StatusOK {
		t.Fatalf("volumes expected 200, got %d", recV.Code)
	}
	var volData struct {
		Volumes  []map[string]any `json:"Volumes"`
		Warnings []string         `json:"Warnings"`
	}
	if err := json.Unmarshal(recV.Body.Bytes(), &volData); err != nil {
		t.Fatalf("unmarshal volumes failed: %v", err)
	}
	if len(volData.Volumes) != 2 {
		t.Fatalf("expected 2 unique volumes, got %d", len(volData.Volumes))
	}
	if len(volData.Warnings) != 1 || volData.Warnings[0] != "warn-node2" {
		t.Fatalf("expected warn-node2 warning, got %v", volData.Warnings)
	}

	// Images
	recI := httptest.NewRecorder()
	p.ServeHTTP(recI, httptest.NewRequest(http.MethodGet, "/images/json", nil))
	if recI.Code != http.StatusOK {
		t.Fatalf("images expected 200, got %d", recI.Code)
	}
	var imgList []map[string]any
	if err := json.Unmarshal(recI.Body.Bytes(), &imgList); err != nil {
		t.Fatalf("unmarshal images failed: %v", err)
	}
	if len(imgList) != 2 {
		t.Fatalf("expected 2 unique images, got %d", len(imgList))
	}

	// Networks
	recN := httptest.NewRecorder()
	p.ServeHTTP(recN, httptest.NewRequest(http.MethodGet, "/networks", nil))
	if recN.Code != http.StatusOK {
		t.Fatalf("networks expected 200, got %d", recN.Code)
	}
	var netList []map[string]any
	if err := json.Unmarshal(recN.Body.Bytes(), &netList); err != nil {
		t.Fatalf("unmarshal networks failed: %v", err)
	}
	if len(netList) != 2 {
		t.Fatalf("expected 2 networks, got %d", len(netList))
	}
}

// Test 7: Response Cache (CacheTTL > 0)
// - Verify second identical GET /containers/json within TTL is served from cache (0 new hits on downstreams)
// - Verify expires after TTL
// - Verify /logs and /stats bypass cache
func TestProxy_ResponseCache(t *testing.T) {
	var listHits atomic.Int64
	var logsHits atomic.Int64
	var statsHits atomic.Int64

	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/containers/json"):
			listHits.Add(1)
			_, _ = w.Write([]byte(`[{"Id":"c100","Names":["/app"]}]`))
		case strings.HasSuffix(r.URL.Path, "/logs"):
			logsHits.Add(1)
			_, _ = w.Write([]byte("log-content\n"))
		case strings.HasSuffix(r.URL.Path, "/stats"):
			statsHits.Add(1)
			_, _ = w.Write([]byte(`{"memory_stats":{}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer node.Close()

	cfg := &config.Config{
		Hosts:          []string{node.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		CacheTTL:       60 * time.Millisecond,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	// 1. Initial request -> cache miss, hits downstream
	rec1 := httptest.NewRecorder()
	p.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/containers/json", nil))
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec1.Code)
	}
	if listHits.Load() != 1 {
		t.Fatalf("expected 1 downstream hit, got %d", listHits.Load())
	}

	// 2. Second request within TTL -> cache hit, 0 new downstream hits!
	rec2 := httptest.NewRecorder()
	p.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/containers/json", nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec2.Code)
	}
	if listHits.Load() != 1 {
		t.Fatalf("expected 1 downstream hit (cached), got %d", listHits.Load())
	}

	// 3. Wait for TTL to expire
	time.Sleep(80 * time.Millisecond)

	// 4. Third request after TTL -> cache expired, hits downstream again
	rec3 := httptest.NewRecorder()
	p.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/containers/json", nil))
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec3.Code)
	}
	if listHits.Load() != 2 {
		t.Fatalf("expected 2 downstream hits after TTL expiration, got %d", listHits.Load())
	}

	// 5. Verify /logs bypasses cache
	p.Affinity().SetContainer("c100", nil, node.URL)

	recLogs1 := httptest.NewRecorder()
	p.ServeHTTP(recLogs1, httptest.NewRequest(http.MethodGet, "/containers/c100/logs", nil))
	if logsHits.Load() != 1 {
		t.Fatalf("expected 1 logs hit, got %d", logsHits.Load())
	}

	recLogs2 := httptest.NewRecorder()
	p.ServeHTTP(recLogs2, httptest.NewRequest(http.MethodGet, "/containers/c100/logs", nil))
	if logsHits.Load() != 2 {
		t.Fatalf("expected 2 logs hits (logs must bypass cache), got %d", logsHits.Load())
	}

	// 6. Verify /stats bypasses cache
	recStats1 := httptest.NewRecorder()
	p.ServeHTTP(recStats1, httptest.NewRequest(http.MethodGet, "/containers/c100/stats", nil))
	if statsHits.Load() != 1 {
		t.Fatalf("expected 1 stats hit, got %d", statsHits.Load())
	}

	recStats2 := httptest.NewRecorder()
	p.ServeHTTP(recStats2, httptest.NewRequest(http.MethodGet, "/containers/c100/stats", nil))
	if statsHits.Load() != 2 {
		t.Fatalf("expected 2 stats hits (stats must bypass cache), got %d", statsHits.Load())
	}
}

// Test 8: Request Timeout (Timeout > 0)
// - Slow downstream exceeding timeout is aborted and marked unhealthy
// - Fast downstreams succeed
func TestProxy_RequestTimeout(t *testing.T) {
	slowNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(300 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"Id":"slow-container"}]`))
	}))
	defer slowNode.Close()

	fastNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"Id":"fast-container"}]`))
	}))
	defer fastNode.Close()

	cfg := &config.Config{
		Hosts:          []string{slowNode.URL, fastNode.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		Timeout:        60 * time.Millisecond,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	start := time.Now()
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/containers/json", nil))
	duration := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from fast node, got %d: %s", rec.Code, rec.Body.String())
	}

	// Should complete shortly after timeout (~60ms), well before 300ms
	if duration >= 250*time.Millisecond {
		t.Fatalf("request took too long (%v), timeout did not abort slow node promptly", duration)
	}

	var containers []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &containers); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(containers) != 1 || containers[0]["Id"] != "fast-container" {
		t.Fatalf("expected fast-container, got %v", containers)
	}

	// Slow node should be marked unhealthy in discovery
	if disc.IsHealthy(slowNode.URL) {
		t.Fatalf("expected slow node exceeding timeout to be marked unhealthy in discovery")
	}
	// Fast node should remain healthy
	if !disc.IsHealthy(fastNode.URL) {
		t.Fatalf("expected fast node to remain healthy")
	}
}

// Test 9: ACL Integration
// - POST /containers/create -> 403 Forbidden
// - GET /events -> 403 Forbidden when Events=false, 200 OK when Events=true
// - Disallowed category -> 403 Forbidden
func TestProxy_ACLIntegration(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	defer node.Close()

	cfg := &config.Config{
		Hosts: []string{node.URL},
		ACL: config.ACLConfig{
			Ping:       true,
			Containers: true,
			Images:     false, // explicitly disallowed
			Events:     false, // default disabled
		},
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	// POST write attempt -> 403
	recPost := httptest.NewRecorder()
	p.ServeHTTP(recPost, httptest.NewRequest(http.MethodPost, "/containers/create", nil))
	if recPost.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for POST, got %d", recPost.Code)
	}

	// GET /events when Events is false -> 403 Forbidden
	recEvents := httptest.NewRecorder()
	p.ServeHTTP(recEvents, httptest.NewRequest(http.MethodGet, "/events", nil))
	if recEvents.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed Events ACL, got %d", recEvents.Code)
	}

	// GET /events when Events is true -> 200 OK
	cfgEvents := &config.Config{
		Hosts: []string{node.URL},
		ACL: config.ACLConfig{
			Events: true,
		},
	}
	discEvents, _ := discovery.NewManager(cfgEvents)
	pEvents := NewProxy(cfgEvents, discEvents)
	recEventsEnabled := httptest.NewRecorder()
	pEvents.ServeHTTP(recEventsEnabled, httptest.NewRequest(http.MethodGet, "/events", nil))
	if recEventsEnabled.Code != http.StatusOK {
		t.Fatalf("expected 200 for allowed Events ACL, got %d", recEventsEnabled.Code)
	}

	// GET /images/json when Images is false -> 403
	recImages := httptest.NewRecorder()
	p.ServeHTTP(recImages, httptest.NewRequest(http.MethodGet, "/images/json", nil))
	if recImages.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for disallowed Images ACL, got %d", recImages.Code)
	}

	// GET /containers/json when Containers is true -> 200
	recContainers := httptest.NewRecorder()
	p.ServeHTTP(recContainers, httptest.NewRequest(http.MethodGet, "/containers/json", nil))
	if recContainers.Code != http.StatusOK {
		t.Fatalf("expected 200 for allowed Containers ACL, got %d", recContainers.Code)
	}
}

// Test 14: Events Fan-Out merging streams from 2+ downstreams and populating container affinity
func TestProxy_EventsFanOutAndAffinity(t *testing.T) {
	ev1 := `{"Type":"container","Action":"start","Actor":{"ID":"c111111111111111111111111111111111111111111111111111111111111111","Attributes":{"name":"web-app"}}}`
	ev2 := `{"Type":"container","Action":"die","Actor":{"ID":"c111111111111111111111111111111111111111111111111111111111111111","Attributes":{"name":"web-app"}}}`
	ev3 := `{"Type":"container","Action":"create","Actor":{"ID":"c222222222222222222222222222222222222222222222222222222222222222","Attributes":{"name":"db-app"}}}`

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "application/json")
			flusher, ok := w.(http.Flusher)
			if r.Method == http.MethodHead {
				return
			}
			_, _ = fmt.Fprintf(w, "%s\n", ev1)
			if ok {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
			_, _ = fmt.Fprintf(w, "%s\n", ev2)
			if ok {
				flusher.Flush()
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "application/json")
			flusher, ok := w.(http.Flusher)
			if r.Method == http.MethodHead {
				return
			}
			_, _ = fmt.Fprintf(w, "%s\n", ev3)
			if ok {
				flusher.Flush()
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc, WithEventsPollInterval(20*time.Millisecond))

	// Test HEAD /events
	recHead := httptest.NewRecorder()
	p.ServeHTTP(recHead, httptest.NewRequest(http.MethodHead, "/events", nil))
	if recHead.Code != http.StatusOK {
		t.Fatalf("HEAD /events code = %d, want 200", recHead.Code)
	}
	if recHead.Body.Len() != 0 {
		t.Fatalf("HEAD /events body length = %d, want 0", recHead.Body.Len())
	}

	// Test GET /events
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /events code = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	body := rec.Body.String()
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 streamed event lines, got %d: %v", len(lines), lines)
	}

	// Verify all 3 events are present
	if !strings.Contains(body, "web-app") || !strings.Contains(body, "db-app") {
		t.Fatalf("missing expected event content in response:\n%s", body)
	}

	// Verify opportunistic affinity caching
	if ep, ok := p.Affinity().GetContainer("web-app"); !ok || ep != node1.URL {
		t.Errorf("affinity for web-app = %q (ok=%v), want %q", ep, ok, node1.URL)
	}
	if ep, ok := p.Affinity().GetContainer("c111111111111"); !ok || ep != node1.URL {
		t.Errorf("affinity for c111111111111 = %q (ok=%v), want %q", ep, ok, node1.URL)
	}
	if ep, ok := p.Affinity().GetContainer("db-app"); !ok || ep != node2.URL {
		t.Errorf("affinity for db-app = %q (ok=%v), want %q", ep, ok, node2.URL)
	}
}

// Test 15: Dynamic Node Addition during /events stream
func TestProxy_EventsDynamicNodeAddition(t *testing.T) {
	node1Done := make(chan struct{})
	node2Connected := make(chan struct{})

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "application/json")
			flusher, ok := w.(http.Flusher)
			_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"c1","Attributes":{"name":"node1-app"}}}`+"\n")
			if ok {
				flusher.Flush()
			}
			// Keep stream open until node2 connects and streams
			select {
			case <-node1Done:
			case <-time.After(3 * time.Second):
			}
			return
		}
		http.NotFound(w, r)
	}))
	defer node1.Close()

	var node2Since atomic.Value
	var node2Once sync.Once
	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			node2Since.Store(r.URL.Query().Get("since"))
			w.Header().Set("Content-Type", "application/json")
			flusher, ok := w.(http.Flusher)
			_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"c2","Attributes":{"name":"node2-app"}}}`+"\n")
			if ok {
				flusher.Flush()
			}
			node2Once.Do(func() {
				close(node2Connected)
			})
			// Small sleep to ensure event was delivered
			time.Sleep(20 * time.Millisecond)
			return
		}
		http.NotFound(w, r)
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts:          []string{node1.URL, node2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	// Initially mark node2 unhealthy so it is NOT active
	disc.MarkUnhealthy(node2.URL)

	p := NewProxy(cfg, disc, WithEventsPollInterval(20*time.Millisecond))
	proxyServer := httptest.NewServer(p)
	defer proxyServer.Close()

	// Client starts streaming /events
	resp, err := http.Get(proxyServer.URL + "/events")
	if err != nil {
		t.Fatalf("failed to GET /events: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	// Read first line from node1
	line1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read line1: %v", err)
	}
	if !strings.Contains(line1, "node1-app") {
		t.Fatalf("expected node1-app in line1, got: %s", line1)
	}

	// Now dynamically mark node2 healthy!
	disc.MarkHealthy(node2.URL)

	// Wait for node2 to receive connection
	select {
	case <-node2Connected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dynamic node2 connection")
	}

	// Let node1 close cleanly now
	close(node1Done)

	// Read line from node2
	line2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read line2: %v", err)
	}
	if !strings.Contains(line2, "node2-app") {
		t.Fatalf("expected node2-app in line2, got: %s", line2)
	}

	// Verify dynamically added node received since query parameter
	sinceVal, _ := node2Since.Load().(string)
	if sinceVal == "" {
		t.Errorf("expected since query parameter on dynamically connected node2, got empty")
	}

	// Verify affinity cache for node2-app
	if ep, ok := p.Affinity().GetContainer("node2-app"); !ok || ep != node2.URL {
		t.Errorf("affinity for node2-app = %q (ok=%v), want %q", ep, ok, node2.URL)
	}
}

// Test 16: Partial downstream failure during /events connection
func TestProxy_EventsPartialFailure(t *testing.T) {
	// Node 1 fails immediately
	brokenNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer brokenNode.Close()

	healthyNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"c_healthy","Attributes":{"name":"healthy-app"}}}`+"\n")
			return
		}
		http.NotFound(w, r)
	}))
	defer healthyNode.Close()

	cfg := &config.Config{
		Hosts:          []string{brokenNode.URL, healthyNode.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from healthy downstream, got %d: %s", rec.Code, rec.Body.String())
	}

	if !strings.Contains(rec.Body.String(), "healthy-app") {
		t.Fatalf("expected healthy-app in response body, got: %s", rec.Body.String())
	}

	// Broken node should have been marked unhealthy in discovery
	if disc.IsHealthy(brokenNode.URL) {
		t.Errorf("expected broken node %q to be marked unhealthy", brokenNode.URL)
	}
	if !disc.IsHealthy(healthyNode.URL) {
		t.Errorf("expected healthy node %q to remain healthy", healthyNode.URL)
	}
}

// Test 17: All downstreams fail initially -> 502 Bad Gateway
func TestProxy_EventsAllDownstreamsFail(t *testing.T) {
	brokenNode1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error 1", http.StatusInternalServerError)
	}))
	defer brokenNode1.Close()

	brokenNode2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error 2", http.StatusInternalServerError)
	}))
	defer brokenNode2.Close()

	cfg := &config.Config{
		Hosts:          []string{brokenNode1.URL, brokenNode2.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Test 18: Client disconnect cancels /events downstream requests promptly
func TestProxy_EventsClientCancel(t *testing.T) {
	serverConnected := make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		flusher, ok := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"c1","Attributes":{"name":"stream-app"}}}`+"\n")
		if ok {
			flusher.Flush()
		}
		close(serverConnected)
		// Wait until client cancels
		<-r.Context().Done()
	}))
	defer node.Close()

	cfg := &config.Config{
		Hosts:          []string{node.URL},
		DNSRefresh:     15 * time.Second,
		HealthInterval: 10 * time.Second,
		ACL:            testACL(),
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}

	p := NewProxy(cfg, disc)
	proxyServer := httptest.NewServer(p)
	defer proxyServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyServer.URL+"/events", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer resp.Body.Close()

	select {
	case <-serverConnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server connection")
	}

	// Cancel client context
	cancel()

	// Reading should terminate promptly with error or EOF
	buf := make([]byte, 1024)
	done := make(chan struct{})
	go func() {
		for {
			_, err := resp.Body.Read(buf)
			if err != nil {
				break
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream body read after cancellation")
	}
}

func TestProxy_StreamEvents_FanOutAndCloseAll(t *testing.T) {
	node1Close := make(chan struct{})
	node2Close := make(chan struct{})

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1.45/events" && r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"1111111111112222222222223333333333334444444444445555555555556666","Attributes":{"name":"web-node1"}}}`+"\n")
		if flusher != nil {
			flusher.Flush()
		}
		<-node1Close
	}))
	defer node1.Close()

	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1.45/events" && r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"aaaaaaaabbbbccccddddeeeeffff000011112222333344445555666677778888","Attributes":{"name":"api-node2"}}}`+"\n")
		if flusher != nil {
			flusher.Flush()
		}
		<-node2Close
	}))
	defer node2.Close()

	cfg := &config.Config{
		Hosts: []string{node1.URL, node2.URL},
		ACL: config.ACLConfig{
			Events:     true,
			Containers: true,
		},
	}
	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}
	p := NewProxy(cfg, disc)
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	// Node 1 closes first, Node 2 closes shortly after -> proxy stream should close only after both close
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(node1Close)
		time.Sleep(30 * time.Millisecond)
		close(node2Close)
	}()

	resp, err := http.Get(proxySrv.URL + "/v1.45/events")
	if err != nil {
		t.Fatalf("GET /events failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed reading /events body: %v", err)
	}
	body := string(bodyBytes)
	if !strings.Contains(body, "web-node1") || !strings.Contains(body, "api-node2") {
		t.Fatalf("expected events from both node1 and node2, got:\n%s", body)
	}

	// Verify opportunistic affinity population from container events
	if ep, ok := p.Affinity().GetContainer("web-node1"); !ok || ep != node1.URL {
		t.Errorf("expected web-node1 affinity -> %s, got %q (ok=%v)", node1.URL, ep, ok)
	}
	if ep, ok := p.Affinity().GetContainer("api-node2"); !ok || ep != node2.URL {
		t.Errorf("expected api-node2 affinity -> %s, got %q (ok=%v)", node2.URL, ep, ok)
	}
}

func TestProxy_StreamEvents_DynamicNodeAddition(t *testing.T) {
	node1Close := make(chan struct{})
	node2Connected := make(chan struct{})

	node1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"node1container","Attributes":{"name":"from-node1"}}}`+"\n")
		if flusher != nil {
			flusher.Flush()
		}
		<-node1Close
	}))
	defer node1.Close()

	var node2Once sync.Once
	node2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `{"Type":"container","Action":"start","Actor":{"ID":"node2container","Attributes":{"name":"from-node2-dynamic"}}}`+"\n")
		if flusher != nil {
			flusher.Flush()
		}
		node2Once.Do(func() {
			close(node2Connected)
		})
	}))
	defer node2.Close()

	// Initially only node1 is healthy; node2 starts unhealthy and recovers dynamically
	cfg := &config.Config{
		Hosts: []string{node1.URL, node2.URL},
		ACL: config.ACLConfig{
			Events: true,
		},
	}

	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}
	// Mark node2 unhealthy before starting /events so only node1 is connected initially
	disc.MarkUnhealthy(node2.URL)

	p := NewProxy(cfg, disc, WithEventsPollInterval(20*time.Millisecond))
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	go func() {
		time.Sleep(40 * time.Millisecond)
		// Node 2 recovers (or is newly discovered) while /events stream is active
		disc.MarkHealthy(node2.URL)
		<-node2Connected
		time.Sleep(20 * time.Millisecond)
		close(node1Close)
	}()

	resp, err := http.Get(proxySrv.URL + "/events")
	if err != nil {
		t.Fatalf("GET /events failed: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed reading /events body: %v", err)
	}
	body := string(bodyBytes)
	if !strings.Contains(body, "from-node1") {
		t.Fatalf("missing event from initial node1, got:\n%s", body)
	}
	if !strings.Contains(body, "from-node2-dynamic") {
		t.Fatalf("missing event from dynamically added node2, got:\n%s", body)
	}
}

func TestProxy_StreamEvents_PartialFailure(t *testing.T) {
	// Node 1 is closed (dead)
	deadNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := deadNode.URL
	deadNode.Close()

	// Node 2 is healthy and streams an event then closes
	liveNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"Type":"network","Action":"create","Actor":{"ID":"net1"}}`+"\n")
	}))
	defer liveNode.Close()

	cfg := &config.Config{
		Hosts: []string{deadURL, liveNode.URL},
		ACL: config.ACLConfig{
			Events: true,
		},
	}
	disc, err := discovery.NewManager(cfg)
	if err != nil {
		t.Fatalf("failed to create discovery: %v", err)
	}
	p := NewProxy(cfg, disc)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on partial failure, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"ID":"net1"`) {
		t.Fatalf("expected event from live node, got %s", rec.Body.String())
	}
	if disc.IsHealthy(deadURL) {
		t.Errorf("expected deadNode %s to be marked unhealthy", deadURL)
	}
}
