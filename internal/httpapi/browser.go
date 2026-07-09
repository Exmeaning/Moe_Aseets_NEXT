package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

const (
	defaultBrowseLimit      = 100
	maxBrowseLimit          = 200
	defaultBrowseCacheTTL   = 15 * time.Second
	defaultBrowseCacheItems = 2048
)

// AssetBrowserHandler exposes a bounded JSON directory listing for the public
// asset browser. It builds pages from SQLite on demand to avoid holding a full
// process-wide directory tree for every asset path.
type AssetBrowserHandler struct {
	DB             *sql.DB
	AllowedServers map[string]struct{}
	DefaultLimit   int
	MaxLimit       int
	CacheTTL       time.Duration
	MaxCacheItems  int

	mu    sync.Mutex
	cache map[string]cachedBrowseResponse
}

type cachedBrowseResponse struct {
	expires time.Time
	body    []byte
}

type assetBrowseResponse struct {
	Server           string            `json:"server"`
	Prefix           string            `json:"prefix"`
	Limit            int               `json:"limit"`
	NextCursor       string            `json:"nextCursor,omitempty"`
	SnapshotRevision uint64            `json:"snapshotRevision"`
	Items            []assetBrowseItem `json:"items"`
}

type assetBrowseItem struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	URL         string `json:"url,omitempty"`
	Source      string `json:"source,omitempty"`
	Size        *int64 `json:"size,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Sha256      string `json:"sha256,omitempty"`
	Version     string `json:"version,omitempty"`
}

func (h *AssetBrowserHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.DB == nil {
		http.Error(w, "asset browser unavailable", http.StatusServiceUnavailable)
		return
	}

	q := r.URL.Query()
	server := strings.ToLower(q.Get("server"))
	if server == "" {
		http.Error(w, "server is required", http.StatusBadRequest)
		return
	}
	if _, ok := h.AllowedServers[server]; !ok {
		http.NotFound(w, r)
		return
	}
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
	cacheKey := fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%d", revision, server, prefix, cursor, limit)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	result, err := store.BrowseCurrent(r.Context(), h.DB, server, prefix, cursor, limit)
	if err != nil {
		http.Error(w, "browse assets", http.StatusInternalServerError)
		return
	}
	resp := assetBrowseResponse{
		Server:           server,
		Prefix:           prefix,
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

func (h *AssetBrowserHandler) defaultLimit() int {
	if h.DefaultLimit > 0 {
		return h.DefaultLimit
	}
	return defaultBrowseLimit
}

func (h *AssetBrowserHandler) maxLimit() int {
	if h.MaxLimit > 0 {
		return h.MaxLimit
	}
	return maxBrowseLimit
}

func (h *AssetBrowserHandler) cacheTTL() time.Duration {
	if h.CacheTTL > 0 {
		return h.CacheTTL
	}
	return defaultBrowseCacheTTL
}

func (h *AssetBrowserHandler) maxCacheItems() int {
	if h.MaxCacheItems > 0 {
		return h.MaxCacheItems
	}
	return defaultBrowseCacheItems
}

func (h *AssetBrowserHandler) getCached(key string) ([]byte, bool) {
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

func (h *AssetBrowserHandler) setCached(key string, body []byte) {
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

func writeBrowseJSON(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=30, stale-while-revalidate=60")
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(body)
}

func toAssetBrowseItem(server string, entry store.BrowseEntry) assetBrowseItem {
	item := assetBrowseItem{
		Type: entry.Kind,
		Name: entry.Name,
		Path: entry.Path,
	}
	if entry.Kind != store.BrowseKindAsset {
		return item
	}
	source := "override"
	if entry.FromShared {
		source = "shared"
	}
	size := entry.Size
	item.URL = "/sekai-" + server + "-assets/" + escapeAssetPath(entry.Path)
	item.Source = source
	item.Size = &size
	item.Fingerprint = entry.Fingerprint
	item.Sha256 = entry.Sha256
	item.Version = entry.Version
	return item
}

func normalizeBrowsePrefix(raw string) (string, error) {
	if raw == "" {
		return "", nil
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
	return safe + "/", nil
}

func normalizeBrowseCursor(raw, prefix string) (string, error) {
	if raw == "" {
		return "", nil
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
	normalized := safe
	if strings.HasSuffix(raw, "/") {
		normalized += "/"
	}
	if normalized != raw {
		return "", storage.ErrBadPath
	}
	if prefix != "" && !strings.HasPrefix(normalized, prefix) {
		return "", storage.ErrBadPath
	}
	return normalized, nil
}

func parseBrowseLimit(raw string, def, max int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, errors.New("limit must be positive")
	}
	if n > max {
		return max, nil
	}
	return n, nil
}

func escapeAssetPath(p string) string {
	parts := strings.Split(p, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
