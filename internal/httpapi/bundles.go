package httpapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

// AssetBundlesHandler exposes the bundle-oriented asset browser:
//
//	GET /api/assets/bundles?server=jp[&prefix=][&limit=][&cursor=]
//	GET /api/assets/bundle-files?server=jp&path=event/foo[&limit=][&cursor=]
//
// /bundles lists the immediate children of prefix where leaves are bundles
// (backed by the materialized current_*_bundles tables, so a page never scans
// per-file rows). /bundle-files lists the current files of one bundle. Both
// share the browse API's parameter rules, response cache, and Cache-Control.
type AssetBundlesHandler struct {
	DB             *sql.DB
	AllowedServers map[string]struct{}
	DefaultLimit   int
	MaxLimit       int
	CacheTTL       time.Duration
	MaxCacheItems  int

	mu    sync.Mutex
	cache map[string]cachedBrowseResponse
}

type bundleBrowseResponse struct {
	Server           string             `json:"server"`
	Prefix           string             `json:"prefix"`
	Limit            int                `json:"limit"`
	NextCursor       string             `json:"nextCursor,omitempty"`
	SnapshotRevision uint64             `json:"snapshotRevision"`
	Items            []bundleBrowseItem `json:"items"`
}

type bundleBrowseItem struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	Fingerprint string `json:"fingerprint,omitempty"`
	FileCount   *int64 `json:"fileCount,omitempty"`
	TotalSize   *int64 `json:"totalSize,omitempty"`
	Source      string `json:"source,omitempty"`
	FilesURL    string `json:"filesUrl,omitempty"`
}

type bundleFilesResponse struct {
	Server           string            `json:"server"`
	Bundle           bundleMeta        `json:"bundle"`
	Limit            int               `json:"limit"`
	NextCursor       string            `json:"nextCursor,omitempty"`
	SnapshotRevision uint64            `json:"snapshotRevision"`
	Items            []assetBrowseItem `json:"items"`
}

type bundleMeta struct {
	Path        string `json:"path"`
	Fingerprint string `json:"fingerprint,omitempty"`
	FileCount   int64  `json:"fileCount"`
	TotalSize   int64  `json:"totalSize"`
	Source      string `json:"source"`
}

func (h *AssetBundlesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.DB == nil {
		http.Error(w, "bundle browser unavailable", http.StatusServiceUnavailable)
		return
	}

	server := strings.ToLower(r.URL.Query().Get("server"))
	if server == "" {
		http.Error(w, "server is required", http.StatusBadRequest)
		return
	}
	if _, ok := h.AllowedServers[server]; !ok {
		http.NotFound(w, r)
		return
	}

	if strings.HasSuffix(r.URL.Path, "/bundle-files") {
		h.serveBundleFiles(w, r, server)
		return
	}
	h.serveBundles(w, r, server)
}

func (h *AssetBundlesHandler) serveBundles(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	prefix, err := normalizeBrowsePrefix(q.Get("prefix"))
	if err != nil {
		http.Error(w, "bad prefix", http.StatusBadRequest)
		return
	}
	cursor, err := normalizeBrowseCursor(q.Get("cursor"), prefix)
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("bundles\x00%d\x00%s\x00%s\x00%s\x00%d", revision, server, prefix, cursor, limit)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	result, err := store.BrowseBundles(r.Context(), h.DB, server, prefix, cursor, limit)
	if err != nil {
		http.Error(w, "browse bundles", http.StatusInternalServerError)
		return
	}
	resp := bundleBrowseResponse{
		Server:           server,
		Prefix:           prefix,
		Limit:            limit,
		NextCursor:       result.NextCursor,
		SnapshotRevision: result.Revision,
		Items:            make([]bundleBrowseItem, 0, len(result.Items)),
	}
	for _, entry := range result.Items {
		resp.Items = append(resp.Items, toBundleBrowseItem(server, entry))
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func (h *AssetBundlesHandler) serveBundleFiles(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	bundlePath, err := normalizeBundlePath(q.Get("path"))
	if err != nil {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	cursor, err := normalizeBrowseCursor(q.Get("cursor"), bundlePath+"/")
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("bundle-files\x00%d\x00%s\x00%s\x00%s\x00%d", revision, server, bundlePath, cursor, limit)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	result, info, found, err := store.BundleFiles(r.Context(), h.DB, server, bundlePath, cursor, limit)
	if err != nil {
		http.Error(w, "list bundle files", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	source := "override"
	if info.FromShared {
		source = "shared"
	}
	resp := bundleFilesResponse{
		Server: server,
		Bundle: bundleMeta{
			Path:        info.Path,
			Fingerprint: info.Fingerprint,
			FileCount:   info.FileCount,
			TotalSize:   info.TotalSize,
			Source:      source,
		},
		Limit:            limit,
		NextCursor:       result.NextCursor,
		SnapshotRevision: result.Revision,
		Items:            make([]assetBrowseItem, 0, len(result.Items)),
	}
	for _, item := range result.Items {
		resp.Items = append(resp.Items, toAssetBrowseItem(server, item))
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func toBundleBrowseItem(server string, entry store.BundleBrowseEntry) bundleBrowseItem {
	item := bundleBrowseItem{
		Type: entry.Kind,
		Name: entry.Name,
		Path: entry.Path,
	}
	if entry.Kind != store.BrowseKindBundle {
		return item
	}
	source := "override"
	if entry.FromShared {
		source = "shared"
	}
	fileCount := entry.FileCount
	totalSize := entry.TotalSize
	item.Fingerprint = entry.Fingerprint
	item.FileCount = &fileCount
	item.TotalSize = &totalSize
	item.Source = source
	item.FilesURL = "/api/assets/bundle-files?server=" + url.QueryEscape(server) + "&path=" + url.QueryEscape(entry.Path)
	return item
}

// normalizeBundlePath validates the ?path= of a bundle: a canonical relative
// path with no trailing slash (one is tolerated and stripped).
func normalizeBundlePath(raw string) (string, error) {
	if raw == "" {
		return "", storage.ErrBadPath
	}
	if strings.Contains(raw, "\\") || strings.Contains(raw, "\x00") || strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, ".") {
		return "", storage.ErrBadPath
	}
	trimmed := strings.TrimSuffix(raw, "/")
	if trimmed == "" {
		return "", storage.ErrBadPath
	}
	safe, err := storage.SafeRelPath(trimmed)
	if err != nil {
		return "", err
	}
	if raw != safe && raw != safe+"/" {
		return "", storage.ErrBadPath
	}
	return safe, nil
}

func (h *AssetBundlesHandler) defaultLimit() int {
	if h.DefaultLimit > 0 {
		return h.DefaultLimit
	}
	return defaultBrowseLimit
}

func (h *AssetBundlesHandler) maxLimit() int {
	if h.MaxLimit > 0 {
		return h.MaxLimit
	}
	return maxBrowseLimit
}

func (h *AssetBundlesHandler) cacheTTL() time.Duration {
	if h.CacheTTL > 0 {
		return h.CacheTTL
	}
	return defaultBrowseCacheTTL
}

func (h *AssetBundlesHandler) maxCacheItems() int {
	if h.MaxCacheItems > 0 {
		return h.MaxCacheItems
	}
	return defaultBrowseCacheItems
}

func (h *AssetBundlesHandler) getCached(key string) ([]byte, bool) {
	ttl := h.cacheTTL()
	if ttl <= 0 {
		return nil, false
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cache == nil {
		return nil, false
	}
	cached, ok := h.cache[key]
	if !ok || now.After(cached.expires) {
		if ok {
			delete(h.cache, key)
		}
		return nil, false
	}
	return cached.body, true
}

func (h *AssetBundlesHandler) setCached(key string, body []byte) {
	ttl := h.cacheTTL()
	if ttl <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cache == nil {
		h.cache = make(map[string]cachedBrowseResponse, 128)
	}
	if len(h.cache) >= h.maxCacheItems() {
		h.cache = make(map[string]cachedBrowseResponse, 128)
	}
	h.cache[key] = cachedBrowseResponse{
		expires: time.Now().Add(ttl),
		body:    append([]byte(nil), body...),
	}
}
