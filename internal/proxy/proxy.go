package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/socketmesh/socketmesh/internal/acl"
	"github.com/socketmesh/socketmesh/internal/config"
	"github.com/socketmesh/socketmesh/internal/discovery"
)

var errNotFound = errors.New("not found on any downstream node")

// Option configures a Proxy instance.
type Option func(*Proxy)

// WithHTTPClient overrides the HTTP client used for downstream requests.
func WithHTTPClient(client *http.Client) Option {
	return func(p *Proxy) {
		p.client = client
	}
}

// WithAffinityCache sets a custom AffinityCache.
func WithAffinityCache(cache *AffinityCache) Option {
	return func(p *Proxy) {
		p.affinity = cache
	}
}

// WithResponseCache sets a custom ResponseCache.
func WithResponseCache(cache *ResponseCache) Option {
	return func(p *Proxy) {
		p.cache = cache
	}
}

// WithLogger sets the structured logger for the proxy.
func WithLogger(logger *slog.Logger) Option {
	return func(p *Proxy) {
		if logger != nil {
			p.logger = logger
		}
	}
}

// Proxy coordinates routing, fan-out aggregation, affinity caching, and failover
// across Docker daemon downstreams.
type Proxy struct {
	cfg           *config.Config
	discovery     *discovery.Manager
	affinity      *AffinityCache
	cache         *ResponseCache
	client        *http.Client
	logger        *slog.Logger
	handler       http.Handler
	mgrMu         sync.RWMutex
	swarmManagers map[string]bool
}

// NewProxy creates and initializes a Proxy with the provided configuration and discovery manager.
func NewProxy(cfg *config.Config, disc *discovery.Manager, opts ...Option) *Proxy {
	dialTimeout := 5 * time.Second
	if cfg.Timeout > 0 && cfg.Timeout < dialTimeout {
		dialTimeout = cfg.Timeout
	}
	var headerTimeout time.Duration
	if cfg.Timeout > 0 {
		headerTimeout = cfg.Timeout
	}

	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: headerTimeout,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
	}

	p := &Proxy{
		cfg:           cfg,
		discovery:     disc,
		affinity:      NewAffinityCache(),
		logger:        slog.Default(),
		swarmManagers: make(map[string]bool),
		client: &http.Client{
			Transport: tr,
		},
	}

	if cfg.CacheTTL > 0 {
		p.cache = NewResponseCache(cfg.CacheTTL)
	}

	for _, opt := range opts {
		opt(p)
	}

	// Wrap the core routing logic with ACL enforcement
	p.handler = acl.Middleware(cfg.ACL, http.HandlerFunc(p.route))

	return p
}

// ServeHTTP delegates request handling to the ACL-wrapped proxy pipeline.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.handler.ServeHTTP(w, r)
}

// Affinity returns the underlying AffinityCache.
func (p *Proxy) Affinity() *AffinityCache {
	return p.affinity
}

// Cache returns the underlying ResponseCache, or nil if disabled.
func (p *Proxy) Cache() *ResponseCache {
	return p.cache
}

// route inspects the request path and dispatches to the appropriate routing strategy.
func (p *Proxy) route(w http.ResponseWriter, r *http.Request) {
	versionPrefix, cleanPath := acl.StripVersionPrefix(r.URL.Path)

	// Check response cache if enabled and eligible
	cacheKey := CacheKey(r.Method, cleanPath, r.URL.RawQuery)
	if p.cache != nil && IsCacheable(r.Method, cleanPath) {
		if entry, ok := p.cache.Get(cacheKey); ok {
			p.writeResponse(w, entry.StatusCode, entry.Header, entry.Body)
			return
		}
	}

	// Strategy A: Fan-Out & Merge JSON Arrays
	switch {
	case cleanPath == "/containers/json":
		p.handleContainersList(w, r, cacheKey)
		return
	case cleanPath == "/images/json":
		p.handleImagesList(w, r, cacheKey)
		return
	case cleanPath == "/networks" || cleanPath == "/networks/":
		p.handleNetworksList(w, r, cacheKey)
		return
	case cleanPath == "/plugins" || cleanPath == "/plugins/":
		p.handlePluginsList(w, r, cacheKey)
		return
	case cleanPath == "/volumes" || cleanPath == "/volumes/":
		p.handleVolumesList(w, r, cacheKey)
		return
	case cleanPath == "/system/df":
		p.handleSystemDf(w, r, cacheKey)
		return
	}

	// Strategy B: Aggregated Cluster /info
	if cleanPath == "/info" {
		p.handleClusterInfo(w, r, cacheKey)
		return
	}

	// Strategy C: Targeted Single-Node Routing
	switch {
	case strings.HasPrefix(cleanPath, "/containers/"):
		p.handleTargetedContainer(w, r, versionPrefix, cleanPath, cacheKey)
		return
	case strings.HasPrefix(cleanPath, "/networks/"):
		p.handleTargetedNetwork(w, r, versionPrefix, cleanPath, cacheKey)
		return
	case strings.HasPrefix(cleanPath, "/volumes/"):
		p.handleTargetedVolume(w, r, versionPrefix, cleanPath, cacheKey)
		return
	case strings.HasPrefix(cleanPath, "/images/") &&
		cleanPath != "/images/json" &&
		cleanPath != "/images/search" &&
		cleanPath != "/images/get":
		p.handleTargetedImage(w, r, versionPrefix, cleanPath, cacheKey)
		return
	}

	// Strategy D: Any-Healthy-Node / Swarm Manager Failover
	if isSwarmEndpoint(cleanPath) {
		p.handleSwarm(w, r, cleanPath, cacheKey)
		return
	}

	p.handleAnyHealthy(w, r, cleanPath, cacheKey)
}

// --- Strategy A: Fan-Out & Merge Handlers ---

type downstreamResult struct {
	endpoint string
	resp     *http.Response
	body     []byte
	err      error
}

func (p *Proxy) fanOut(ctx context.Context, r *http.Request) ([]downstreamResult, []string) {
	endpoints := p.discovery.HealthyEndpoints()
	if len(endpoints) == 0 {
		return nil, nil
	}

	results := make([]downstreamResult, len(endpoints))
	var wg sync.WaitGroup

	for i, ep := range endpoints {
		wg.Add(1)
		go func(idx int, endpoint string) {
			defer wg.Done()
			resp, body, err := p.doDownstream(ctx, endpoint, r, false)
			results[idx] = downstreamResult{
				endpoint: endpoint,
				resp:     resp,
				body:     body,
				err:      err,
			}
		}(i, ep)
	}

	wg.Wait()
	return results, endpoints
}

func (p *Proxy) handleContainersList(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	var allContainers []json.RawMessage
	seenIDs := make(map[string]bool)
	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var rawList []json.RawMessage
		if err := json.Unmarshal(res.body, &rawList); err != nil {
			p.logger.Warn("failed to parse containers json", "endpoint", res.endpoint, "err", err)
			continue
		}

		var summaries []struct {
			ID    string   `json:"Id"`
			Names []string `json:"Names"`
		}
		_ = json.Unmarshal(res.body, &summaries)

		for i, raw := range rawList {
			var id string
			var names []string
			if i < len(summaries) {
				id = summaries[i].ID
				names = summaries[i].Names
			}

			if id != "" {
				p.affinity.SetContainer(id, names, res.endpoint)
				if seenIDs[id] {
					continue
				}
				seenIDs[id] = true
			}
			allContainers = append(allContainers, raw)
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allContainers == nil {
		allContainers = make([]json.RawMessage, 0)
	}

	merged, err := json.Marshal(allContainers)
	if err != nil {
		http.Error(w, `{"message":"failed to marshal containers"}`, http.StatusInternalServerError)
		return
	}

	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

func (p *Proxy) handleImagesList(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	var allImages []json.RawMessage
	seenIDs := make(map[string]bool)
	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var rawList []json.RawMessage
		if err := json.Unmarshal(res.body, &rawList); err != nil {
			continue
		}
		var summaries []struct {
			ID string `json:"Id"`
		}
		_ = json.Unmarshal(res.body, &summaries)

		for i, raw := range rawList {
			var id string
			if i < len(summaries) {
				id = summaries[i].ID
			}
			if id != "" {
				p.affinity.Set("images", id, res.endpoint)
				if seenIDs[id] {
					continue
				}
				seenIDs[id] = true
			}
			allImages = append(allImages, raw)
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allImages == nil {
		allImages = make([]json.RawMessage, 0)
	}

	merged, _ := json.Marshal(allImages)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

func (p *Proxy) handleNetworksList(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	var allNetworks []json.RawMessage
	seen := make(map[string]bool)
	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var rawList []json.RawMessage
		if err := json.Unmarshal(res.body, &rawList); err != nil {
			continue
		}
		var summaries []struct {
			ID   string `json:"Id"`
			Name string `json:"Name"`
		}
		_ = json.Unmarshal(res.body, &summaries)

		for i, raw := range rawList {
			id := ""
			name := ""
			if i < len(summaries) {
				id = summaries[i].ID
				name = summaries[i].Name
			}
			key := id
			if key == "" {
				key = name
			}
			if key != "" {
				if id != "" {
					p.affinity.Set("networks", id, res.endpoint)
				}
				if name != "" {
					p.affinity.Set("networks", name, res.endpoint)
				}
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			allNetworks = append(allNetworks, raw)
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allNetworks == nil {
		allNetworks = make([]json.RawMessage, 0)
	}

	merged, _ := json.Marshal(allNetworks)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

func (p *Proxy) handlePluginsList(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	var allPlugins []json.RawMessage
	seen := make(map[string]bool)
	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var rawList []json.RawMessage
		if err := json.Unmarshal(res.body, &rawList); err != nil {
			continue
		}
		var summaries []struct {
			ID   string `json:"Id"`
			Name string `json:"Name"`
		}
		_ = json.Unmarshal(res.body, &summaries)

		for i, raw := range rawList {
			key := ""
			if i < len(summaries) {
				key = summaries[i].ID
				if key == "" {
					key = summaries[i].Name
				}
			}
			if key != "" {
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			allPlugins = append(allPlugins, raw)
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allPlugins == nil {
		allPlugins = make([]json.RawMessage, 0)
	}

	merged, _ := json.Marshal(allPlugins)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

func (p *Proxy) handleVolumesList(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	type volumeItem struct {
		Name       string `json:"Name"`
		Driver     string `json:"Driver"`
		Mountpoint string `json:"Mountpoint"`
	}
	type volumesPayload struct {
		Volumes  []json.RawMessage `json:"Volumes"`
		Warnings []string          `json:"Warnings"`
	}

	var allVolumes []json.RawMessage
	var allWarnings []string
	seen := make(map[string]bool)
	seenWarnings := make(map[string]bool)
	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var payload volumesPayload
		if err := json.Unmarshal(res.body, &payload); err != nil {
			continue
		}

		var items []volumeItem
		_ = json.Unmarshal(res.body, &struct {
			Volumes *[]volumeItem `json:"Volumes"`
		}{Volumes: &items})

		for i, raw := range payload.Volumes {
			key := ""
			if i < len(items) {
				item := items[i]
				if item.Name != "" {
					p.affinity.Set("volumes", item.Name, res.endpoint)
				}
				key = item.Driver + ":" + item.Name + ":" + item.Mountpoint
			}
			if key == "::" || key == "" {
				allVolumes = append(allVolumes, raw)
			} else if !seen[key] {
				seen[key] = true
				allVolumes = append(allVolumes, raw)
			}
		}

		for _, w := range payload.Warnings {
			if w != "" && !seenWarnings[w] {
				seenWarnings[w] = true
				allWarnings = append(allWarnings, w)
			}
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allVolumes == nil {
		allVolumes = make([]json.RawMessage, 0)
	}
	if allWarnings == nil {
		allWarnings = make([]string, 0)
	}

	respObj := volumesPayload{
		Volumes:  allVolumes,
		Warnings: allWarnings,
	}

	merged, _ := json.Marshal(respObj)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

func (p *Proxy) handleSystemDf(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	type dfPayload struct {
		LayersSize int64             `json:"LayersSize"`
		Images     []json.RawMessage `json:"Images"`
		Containers []json.RawMessage `json:"Containers"`
		Volumes    []json.RawMessage `json:"Volumes"`
		BuildCache []json.RawMessage `json:"BuildCache"`
	}

	var totalLayersSize int64
	var allImages []json.RawMessage
	var allContainers []json.RawMessage
	var allVolumes []json.RawMessage
	var allBuildCache []json.RawMessage

	seenImages := make(map[string]bool)
	seenContainers := make(map[string]bool)
	seenVolumes := make(map[string]bool)
	seenBuildCache := make(map[string]bool)

	var firstRespHeader http.Header
	var successCount int

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var payload dfPayload
		if err := json.Unmarshal(res.body, &payload); err != nil {
			continue
		}

		totalLayersSize += payload.LayersSize

		// Images
		var imgSummaries []struct {
			ID string `json:"Id"`
		}
		_ = json.Unmarshal(res.body, &struct {
			Images *[]struct {
				ID string `json:"Id"`
			} `json:"Images"`
		}{Images: &imgSummaries})

		for i, raw := range payload.Images {
			id := ""
			if i < len(imgSummaries) {
				id = imgSummaries[i].ID
			}
			if id != "" {
				if seenImages[id] {
					continue
				}
				seenImages[id] = true
			}
			allImages = append(allImages, raw)
		}

		// Containers
		var cntSummaries []struct {
			ID string `json:"Id"`
		}
		_ = json.Unmarshal(res.body, &struct {
			Containers *[]struct {
				ID string `json:"Id"`
			} `json:"Containers"`
		}{Containers: &cntSummaries})

		for i, raw := range payload.Containers {
			id := ""
			if i < len(cntSummaries) {
				id = cntSummaries[i].ID
			}
			if id != "" {
				if seenContainers[id] {
					continue
				}
				seenContainers[id] = true
			}
			allContainers = append(allContainers, raw)
		}

		// Volumes
		var volSummaries []struct {
			Name   string `json:"Name"`
			Driver string `json:"Driver"`
		}
		_ = json.Unmarshal(res.body, &struct {
			Volumes *[]struct {
				Name   string `json:"Name"`
				Driver string `json:"Driver"`
			} `json:"Volumes"`
		}{Volumes: &volSummaries})

		for i, raw := range payload.Volumes {
			key := ""
			if i < len(volSummaries) {
				key = volSummaries[i].Driver + ":" + volSummaries[i].Name
			}
			if key != ":" && key != "" {
				if seenVolumes[key] {
					continue
				}
				seenVolumes[key] = true
			}
			allVolumes = append(allVolumes, raw)
		}

		// BuildCache
		var bcSummaries []struct {
			ID string `json:"ID"`
		}
		_ = json.Unmarshal(res.body, &struct {
			BuildCache *[]struct {
				ID string `json:"ID"`
			} `json:"BuildCache"`
		}{BuildCache: &bcSummaries})

		for i, raw := range payload.BuildCache {
			id := ""
			if i < len(bcSummaries) {
				id = bcSummaries[i].ID
			}
			if id != "" {
				if seenBuildCache[id] {
					continue
				}
				seenBuildCache[id] = true
			}
			allBuildCache = append(allBuildCache, raw)
		}
	}

	if successCount == 0 {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	if allImages == nil {
		allImages = make([]json.RawMessage, 0)
	}
	if allContainers == nil {
		allContainers = make([]json.RawMessage, 0)
	}
	if allVolumes == nil {
		allVolumes = make([]json.RawMessage, 0)
	}
	if allBuildCache == nil {
		allBuildCache = make([]json.RawMessage, 0)
	}

	mergedObj := dfPayload{
		LayersSize: totalLayersSize,
		Images:     allImages,
		Containers: allContainers,
		Volumes:    allVolumes,
		BuildCache: allBuildCache,
	}

	merged, _ := json.Marshal(mergedObj)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

// --- Strategy B: Cluster /info ---

func (p *Proxy) handleClusterInfo(w http.ResponseWriter, r *http.Request, cacheKey string) {
	results, endpoints := p.fanOut(r.Context(), r)
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	var base map[string]any
	var firstRespHeader http.Header
	var successCount int

	var totalContainers float64
	var totalRunning float64
	var totalPaused float64
	var totalStopped float64
	var totalNCPU float64
	var totalMemTotal float64
	var totalImages float64

	for _, res := range results {
		if res.err != nil || res.resp.StatusCode != http.StatusOK {
			continue
		}
		if firstRespHeader == nil {
			firstRespHeader = res.resp.Header
		}
		successCount++

		var info map[string]any
		if err := json.Unmarshal(res.body, &info); err != nil {
			continue
		}

		if base == nil {
			base = info
		}

		totalContainers += toFloat(info["Containers"])
		totalRunning += toFloat(info["ContainersRunning"])
		totalPaused += toFloat(info["ContainersPaused"])
		totalStopped += toFloat(info["ContainersStopped"])
		totalNCPU += toFloat(info["NCPU"])
		totalMemTotal += toFloat(info["MemTotal"])
		totalImages += toFloat(info["Images"])
	}

	if successCount == 0 || base == nil {
		http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
		return
	}

	base["Containers"] = int64(totalContainers)
	base["ContainersRunning"] = int64(totalRunning)
	base["ContainersPaused"] = int64(totalPaused)
	base["ContainersStopped"] = int64(totalStopped)
	base["NCPU"] = int64(totalNCPU)
	base["MemTotal"] = int64(totalMemTotal)
	base["Images"] = int64(totalImages)

	merged, _ := json.Marshal(base)
	header := make(http.Header)
	copyForwardHeaders(firstRespHeader, header)
	header.Set("Content-Type", "application/json")

	p.writeResponse(w, http.StatusOK, header, merged)
	if p.cache != nil {
		p.cache.Set(cacheKey, http.StatusOK, header, merged)
	}
}

// --- Strategy C: Targeted Single-Node Routing ---

func (p *Proxy) handleTargetedContainer(w http.ResponseWriter, r *http.Request, versionPrefix, cleanPath, cacheKey string) {
	sub := strings.TrimPrefix(cleanPath, "/containers/")
	parts := strings.SplitN(sub, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	isStreaming := isContainerStreaming(action, r.URL.Query())

	// 1. Check affinity cache
	cachedEP, found := p.affinity.GetContainer(id)
	if found && p.discovery.IsHealthy(cachedEP) {
		if isStreaming {
			resp, err := p.doRequest(r.Context(), cachedEP, r, true)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					_ = p.streamResponse(w, resp)
					return
				}
				resp.Body.Close()
			}
		} else {
			resp, body, err := p.doDownstream(r.Context(), cachedEP, r, false)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					if action == "json" {
						p.updateAffinityFromContainerInspect(body, cachedEP, id)
					}
					p.writeResponse(w, resp.StatusCode, resp.Header, body)
					if p.cache != nil && IsCacheable(r.Method, cleanPath) {
						p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
					}
					return
				}
			}
		}
	}

	// 2. Cache miss or cached endpoint failed/404 -> probe healthy endpoints
	healthy := p.discovery.HealthyEndpoints()
	if len(healthy) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	if isStreaming {
		// Probe with GET <versionPrefix>/containers/{id}/json to find owner node
		ownerEP, inspectBody, err := p.probeContainerOwner(r.Context(), versionPrefix, id, healthy)
		if err == nil && ownerEP != "" {
			p.updateAffinityFromContainerInspect(inspectBody, ownerEP, id)
			resp, sErr := p.doRequest(r.Context(), ownerEP, r, true)
			if sErr == nil {
				_ = p.streamResponse(w, resp)
				return
			}
			http.Error(w, sErr.Error(), http.StatusBadGateway)
			return
		}

		if errors.Is(err, errNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "No such container: " + id})
			return
		}

		http.Error(w, `{"message":"container not found on any healthy node"}`, http.StatusBadGateway)
		return
	}

	// Non-streaming container request: probe directly
	resp, body, ownerEP, err := p.probeRequest(r.Context(), r, healthy)
	if err == nil && resp != nil {
		if resp.StatusCode == http.StatusOK {
			if action == "json" {
				p.updateAffinityFromContainerInspect(body, ownerEP, id)
			} else {
				p.affinity.SetContainer(id, nil, ownerEP)
			}
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
		}
		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		return
	}

	http.Error(w, `{"message":"container request failed across all nodes"}`, http.StatusBadGateway)
}

func (p *Proxy) handleTargetedNetwork(w http.ResponseWriter, r *http.Request, versionPrefix, cleanPath, cacheKey string) {
	id := strings.TrimPrefix(cleanPath, "/networks/")
	cachedEP, found := p.affinity.Get("networks", id)
	if found && p.discovery.IsHealthy(cachedEP) {
		resp, body, err := p.doDownstream(r.Context(), cachedEP, r, false)
		if err == nil && resp.StatusCode == http.StatusOK {
			p.updateAffinityFromNetworkInspect(body, cachedEP, id)
			p.writeResponse(w, resp.StatusCode, resp.Header, body)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
			return
		}
	}

	healthy := p.discovery.HealthyEndpoints()
	if len(healthy) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	resp, body, ownerEP, err := p.probeRequest(r.Context(), r, healthy)
	if err == nil && resp != nil {
		if resp.StatusCode == http.StatusOK {
			p.updateAffinityFromNetworkInspect(body, ownerEP, id)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
		}
		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		return
	}

	http.Error(w, `{"message":"network request failed across all nodes"}`, http.StatusBadGateway)
}

func (p *Proxy) handleTargetedVolume(w http.ResponseWriter, r *http.Request, versionPrefix, cleanPath, cacheKey string) {
	name := strings.TrimPrefix(cleanPath, "/volumes/")
	cachedEP, found := p.affinity.Get("volumes", name)
	if found && p.discovery.IsHealthy(cachedEP) {
		resp, body, err := p.doDownstream(r.Context(), cachedEP, r, false)
		if err == nil && resp.StatusCode == http.StatusOK {
			p.affinity.Set("volumes", name, cachedEP)
			p.writeResponse(w, resp.StatusCode, resp.Header, body)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
			return
		}
	}

	healthy := p.discovery.HealthyEndpoints()
	if len(healthy) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	resp, body, ownerEP, err := p.probeRequest(r.Context(), r, healthy)
	if err == nil && resp != nil {
		if resp.StatusCode == http.StatusOK {
			p.affinity.Set("volumes", name, ownerEP)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
		}
		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		return
	}

	http.Error(w, `{"message":"volume request failed across all nodes"}`, http.StatusBadGateway)
}

func (p *Proxy) handleTargetedImage(w http.ResponseWriter, r *http.Request, versionPrefix, cleanPath, cacheKey string) {
	nameWithSuffix := strings.TrimPrefix(cleanPath, "/images/")
	var name string
	isGet := strings.HasSuffix(nameWithSuffix, "/get")
	isHistory := strings.HasSuffix(nameWithSuffix, "/history")
	isJSON := strings.HasSuffix(nameWithSuffix, "/json")

	switch {
	case isGet:
		name = strings.TrimSuffix(nameWithSuffix, "/get")
	case isHistory:
		name = strings.TrimSuffix(nameWithSuffix, "/history")
	case isJSON:
		name = strings.TrimSuffix(nameWithSuffix, "/json")
	default:
		name = nameWithSuffix
	}

	cachedEP, found := p.affinity.Get("images", name)

	if isGet {
		// Streaming image archive
		if found && p.discovery.IsHealthy(cachedEP) {
			resp, err := p.doRequest(r.Context(), cachedEP, r, true)
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = p.streamResponse(w, resp)
				return
			}
			if resp != nil {
				resp.Body.Close()
			}
		}

		// Probe owner with /images/{name}/json
		healthy := p.discovery.HealthyEndpoints()
		ownerEP, _, err := p.probeImageOwner(r.Context(), versionPrefix, name, healthy)
		if err == nil && ownerEP != "" {
			p.affinity.Set("images", name, ownerEP)
			resp, sErr := p.doRequest(r.Context(), ownerEP, r, true)
			if sErr == nil {
				_ = p.streamResponse(w, resp)
				return
			}
			http.Error(w, sErr.Error(), http.StatusBadGateway)
			return
		}

		if errors.Is(err, errNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "No such image: " + name})
			return
		}

		http.Error(w, `{"message":"image not found on any healthy node"}`, http.StatusBadGateway)
		return
	}

	// Non-streaming image inspect or history
	if found && p.discovery.IsHealthy(cachedEP) {
		resp, body, err := p.doDownstream(r.Context(), cachedEP, r, false)
		if err == nil && resp.StatusCode == http.StatusOK {
			p.writeResponse(w, resp.StatusCode, resp.Header, body)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
			return
		}
	}

	healthy := p.discovery.HealthyEndpoints()
	if len(healthy) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	resp, body, ownerEP, err := p.probeRequest(r.Context(), r, healthy)
	if err == nil && resp != nil {
		if resp.StatusCode == http.StatusOK {
			p.affinity.Set("images", name, ownerEP)
			if p.cache != nil && IsCacheable(r.Method, cleanPath) {
				p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
			}
		}
		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		return
	}

	http.Error(w, `{"message":"image request failed across all nodes"}`, http.StatusBadGateway)
}

// --- Strategy D: Swarm Manager Failover & Any Healthy ---

func (p *Proxy) handleSwarm(w http.ResponseWriter, r *http.Request, cleanPath, cacheKey string) {
	endpoints := p.discovery.HealthyEndpoints()
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	// Prioritize known managers
	p.mgrMu.RLock()
	var managers []string
	var others []string
	for _, ep := range endpoints {
		if p.swarmManagers[ep] {
			managers = append(managers, ep)
		} else {
			others = append(others, ep)
		}
	}
	p.mgrMu.RUnlock()

	ordered := append(managers, others...)

	var last503Resp *http.Response
	var last503Body []byte

	for _, ep := range ordered {
		resp, body, err := p.doDownstream(r.Context(), ep, r, false)
		if err != nil {
			continue
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			// Downstream is a Swarm worker node, not a manager
			p.setSwarmManager(ep, false)
			last503Resp = resp
			last503Body = body
			continue
		}

		// Success or other status (e.g. 200 OK or 404 Not Found for specific item)
		if resp.StatusCode == http.StatusOK {
			p.setSwarmManager(ep, true)
		}

		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		if p.cache != nil && IsCacheable(r.Method, cleanPath) {
			p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
		}
		return
	}

	if last503Resp != nil {
		p.writeResponse(w, http.StatusServiceUnavailable, last503Resp.Header, last503Body)
		return
	}

	http.Error(w, `{"message":"no swarm managers reachable"}`, http.StatusBadGateway)
}

func (p *Proxy) handleAnyHealthy(w http.ResponseWriter, r *http.Request, cleanPath, cacheKey string) {
	endpoints := p.discovery.HealthyEndpoints()
	if len(endpoints) == 0 {
		http.Error(w, `{"message":"no healthy downstream endpoints"}`, http.StatusBadGateway)
		return
	}

	isStreaming := cleanPath == "/images/get"

	for _, ep := range endpoints {
		if isStreaming {
			resp, err := p.doRequest(r.Context(), ep, r, true)
			if err != nil {
				continue
			}
			_ = p.streamResponse(w, resp)
			return
		}

		resp, body, err := p.doDownstream(r.Context(), ep, r, false)
		if err != nil {
			continue
		}

		p.writeResponse(w, resp.StatusCode, resp.Header, body)
		if p.cache != nil && IsCacheable(r.Method, cleanPath) {
			p.cache.Set(cacheKey, resp.StatusCode, resp.Header, body)
		}
		return
	}

	http.Error(w, `{"message":"all downstream endpoints failed"}`, http.StatusBadGateway)
}

// --- Probing & Downstream Request Helpers ---

type probeResult struct {
	endpoint string
	resp     *http.Response
	body     []byte
	err      error
}

func (p *Proxy) probeContainerOwner(ctx context.Context, versionPrefix, id string, endpoints []string) (string, []byte, error) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan probeResult, len(endpoints))

	for _, ep := range endpoints {
		go func(endpoint string) {
			probePath := versionPrefix + "/containers/" + id + "/json"
			targetURL := strings.TrimRight(endpoint, "/") + probePath
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, targetURL, nil)
			if err != nil {
				resCh <- probeResult{endpoint: endpoint, err: err}
				return
			}
			resp, body, err := p.doDownstream(probeCtx, endpoint, req, false)
			resCh <- probeResult{endpoint: endpoint, resp: resp, body: body, err: err}
		}(ep)
	}

	all404 := true
	var ownerEP string
	var ownerBody []byte
	found := false

	for i := 0; i < len(endpoints); i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil {
			if res.resp.StatusCode == http.StatusOK {
				if !found {
					found = true
					ownerEP = res.endpoint
					ownerBody = res.body
					cancel()
				}
			} else if res.resp.StatusCode != http.StatusNotFound {
				all404 = false
			}
		} else {
			all404 = false
		}
	}

	if found {
		return ownerEP, ownerBody, nil
	}
	if all404 {
		return "", nil, errNotFound
	}
	return "", nil, errors.New("container not found or downstreams unreachable")
}

func (p *Proxy) probeImageOwner(ctx context.Context, versionPrefix, name string, endpoints []string) (string, []byte, error) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan probeResult, len(endpoints))

	for _, ep := range endpoints {
		go func(endpoint string) {
			probePath := versionPrefix + "/images/" + name + "/json"
			targetURL := strings.TrimRight(endpoint, "/") + probePath
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, targetURL, nil)
			if err != nil {
				resCh <- probeResult{endpoint: endpoint, err: err}
				return
			}
			resp, body, err := p.doDownstream(probeCtx, endpoint, req, false)
			resCh <- probeResult{endpoint: endpoint, resp: resp, body: body, err: err}
		}(ep)
	}

	all404 := true
	var ownerEP string
	var ownerBody []byte
	found := false

	for i := 0; i < len(endpoints); i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil {
			if res.resp.StatusCode == http.StatusOK {
				if !found {
					found = true
					ownerEP = res.endpoint
					ownerBody = res.body
					cancel()
				}
			} else if res.resp.StatusCode != http.StatusNotFound {
				all404 = false
			}
		} else {
			all404 = false
		}
	}

	if found {
		return ownerEP, ownerBody, nil
	}
	if all404 {
		return "", nil, errNotFound
	}
	return "", nil, errors.New("image not found or downstreams unreachable")
}

func (p *Proxy) probeRequest(ctx context.Context, origReq *http.Request, endpoints []string) (*http.Response, []byte, string, error) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan probeResult, len(endpoints))

	for _, ep := range endpoints {
		go func(endpoint string) {
			targetURL := strings.TrimRight(endpoint, "/") + origReq.URL.Path
			if origReq.URL.RawQuery != "" {
				targetURL += "?" + origReq.URL.RawQuery
			}
			req, err := http.NewRequestWithContext(probeCtx, origReq.Method, targetURL, nil)
			if err != nil {
				resCh <- probeResult{endpoint: endpoint, err: err}
				return
			}
			copyRequestHeaders(origReq.Header, req.Header)
			resp, body, err := p.doDownstream(probeCtx, endpoint, req, false)
			resCh <- probeResult{endpoint: endpoint, resp: resp, body: body, err: err}
		}(ep)
	}

	var first404Resp *http.Response
	var first404Body []byte
	var first404EP string
	var okResult *probeResult

	for i := 0; i < len(endpoints); i++ {
		res := <-resCh
		if res.err == nil && res.resp != nil {
			if res.resp.StatusCode == http.StatusOK {
				if okResult == nil {
					resCopy := res
					okResult = &resCopy
					cancel()
				}
			} else if res.resp.StatusCode == http.StatusNotFound && first404Resp == nil {
				first404Resp = res.resp
				first404Body = res.body
				first404EP = res.endpoint
			}
		}
	}

	if okResult != nil {
		return okResult.resp, okResult.body, okResult.endpoint, nil
	}
	if first404Resp != nil {
		return first404Resp, first404Body, first404EP, nil
	}
	return nil, nil, "", errors.New("downstream requests failed")
}

func (p *Proxy) doDownstream(ctx context.Context, endpoint string, r *http.Request, isStreaming bool) (*http.Response, []byte, error) {
	targetURL := strings.TrimRight(endpoint, "/") + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	reqCtx := ctx
	var cancel context.CancelFunc
	if !isStreaming && p.cfg.Timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
		defer cancel()
	}

	outReq, err := http.NewRequestWithContext(reqCtx, r.Method, targetURL, nil)
	if err != nil {
		return nil, nil, err
	}
	copyRequestHeaders(r.Header, outReq.Header)

	resp, err := p.client.Do(outReq)
	if err != nil {
		if !isClientCanceled(r.Context(), err) {
			p.discovery.MarkUnhealthy(endpoint)
			p.logger.Warn("downstream request failed", "endpoint", endpoint, "err", err)
		}
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if !isClientCanceled(r.Context(), err) {
			p.discovery.MarkUnhealthy(endpoint)
			p.logger.Warn("failed to read downstream body", "endpoint", endpoint, "err", err)
		}
		return nil, nil, err
	}

	return resp, body, nil
}

func (p *Proxy) doRequest(ctx context.Context, endpoint string, r *http.Request, isStreaming bool) (*http.Response, error) {
	targetURL := strings.TrimRight(endpoint, "/") + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	outReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	copyRequestHeaders(r.Header, outReq.Header)

	resp, err := p.client.Do(outReq)
	if err != nil {
		if !isClientCanceled(r.Context(), err) {
			p.discovery.MarkUnhealthy(endpoint)
			p.logger.Warn("downstream request failed", "endpoint", endpoint, "err", err)
		}
		return nil, err
	}

	return resp, nil
}

func (p *Proxy) updateAffinityFromContainerInspect(body []byte, endpoint, fallbackID string) {
	var inspect struct {
		ID   string `json:"Id"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &inspect); err == nil && inspect.ID != "" {
		names := []string{inspect.Name}
		if fallbackID != "" && fallbackID != inspect.ID {
			names = append(names, fallbackID)
		}
		p.affinity.SetContainer(inspect.ID, names, endpoint)
	} else if fallbackID != "" {
		p.affinity.SetContainer(fallbackID, nil, endpoint)
	}
}

func (p *Proxy) updateAffinityFromNetworkInspect(body []byte, endpoint, fallbackID string) {
	var inspect struct {
		ID   string `json:"Id"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &inspect); err == nil {
		if inspect.ID != "" {
			p.affinity.Set("networks", inspect.ID, endpoint)
		}
		if inspect.Name != "" {
			p.affinity.Set("networks", inspect.Name, endpoint)
		}
	} else if fallbackID != "" {
		p.affinity.Set("networks", fallbackID, endpoint)
	}
}

func (p *Proxy) setSwarmManager(endpoint string, isManager bool) {
	p.mgrMu.Lock()
	defer p.mgrMu.Unlock()
	p.swarmManagers[endpoint] = isManager
}

func (p *Proxy) writeResponse(w http.ResponseWriter, statusCode int, header http.Header, body []byte) {
	for k, vv := range header {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
}

func (p *Proxy) streamResponse(w http.ResponseWriter, resp *http.Response) error {
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	flusher, hasFlusher := w.(http.Flusher)
	if hasFlusher {
		flusher.Flush()
	}

	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, wErr := w.Write(buf[:n]); wErr != nil {
				return wErr
			}
			if hasFlusher {
				flusher.Flush()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// --- Utility Helpers ---

func isHopByHop(header string) bool {
	switch strings.ToLower(header) {
	case "connection",
		"keep-alive",
		"proxy-authenticate",
		"proxy-authorization",
		"te",
		"trailer",
		"trailers",
		"transfer-encoding",
		"upgrade":
		return true
	default:
		return false
	}
}

func copyRequestHeaders(src, dst http.Header) {
	for k, vv := range src {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func copyForwardHeaders(src, dst http.Header) {
	if src == nil || dst == nil {
		return
	}
	for k, vv := range src {
		if isHopByHop(k) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func isContainerStreaming(action string, q url.Values) bool {
	if action == "logs" || action == "archive" {
		return true
	}
	if action == "stats" {
		s := strings.ToLower(strings.TrimSpace(q.Get("stream")))
		return s != "0" && s != "false"
	}
	return false
}

func isSwarmEndpoint(cleanPath string) bool {
	prefixes := []string{
		"/services",
		"/tasks",
		"/nodes",
		"/swarm",
		"/secrets",
		"/configs",
	}
	for _, pfx := range prefixes {
		if cleanPath == pfx || strings.HasPrefix(cleanPath, pfx+"/") {
			return true
		}
	}
	return false
}

func isClientCanceled(clientCtx context.Context, err error) bool {
	if clientCtx != nil && clientCtx.Err() != nil {
		return true
	}
	return false
}

func toFloat(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f
		}
	case string:
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
			return f
		}
	}
	return 0
}
