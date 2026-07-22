package httpapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

const maxAssetVersionLen = 128

// maxDiffTypes bounds the ?types= list so a request cannot inflate the SQL
// OR-chain arbitrarily.
const maxDiffTypes = 16

// defaultDiffTypes keeps the diff payload to webp and mp3 files by default.
var defaultDiffTypes = []string{"webp", "mp3"}

// defaultMinMp3Size restricts mp3 files to >= 1MB (1,048,576 bytes) when using default types filter.
const defaultMinMp3Size int64 = 1 * 1024 * 1024

// AssetVersionsHandler exposes the committed version history and the
// per-version update diff for the public frontend:
//
//	GET /api/assets/versions?server=jp[&limit=][&cursor=]
//	GET /api/assets/diff?server=jp&version=6.0.0.11[&limit=][&cursor=][&types=][&action=]
//
// Both read SQLite on demand (same pattern as the asset browser) and keep a
// short-lived in-process response cache keyed by asset revision.
type AssetVersionsHandler struct {
	DB             *sql.DB
	AllowedServers map[string]struct{}
	DefaultLimit   int
	MaxLimit       int
	CacheTTL       time.Duration
	MaxCacheItems  int
	// DefaultTypes overrides the built-in webp/mp3 diff filter.
	DefaultTypes []string
	// DefaultMinMp3Size overrides the built-in 1MB mp3 size filter for default types.
	DefaultMinMp3Size *int64

	mu    sync.Mutex
	cache map[string]cachedBrowseResponse
}

type assetVersionsResponse struct {
	Server     string             `json:"server"`
	Limit      int                `json:"limit"`
	NextCursor string             `json:"nextCursor,omitempty"`
	Items      []assetVersionItem `json:"items"`
}

type assetVersionItem struct {
	AssetVersion  string          `json:"assetVersion"`
	AppVersion    string          `json:"appVersion"`
	AssetHash     string          `json:"assetHash,omitempty"`
	BundleCount   int64           `json:"bundleCount,omitempty"`
	CommittedAt   int64           `json:"committedAt"`
	ChangedAssets int64           `json:"changedAssets"`
	Stats         json.RawMessage `json:"stats,omitempty"`
	DiffURL       string          `json:"diffUrl"`
	BundleDiffURL string          `json:"bundleDiffUrl,omitempty"`
}

type assetDiffResponse struct {
	Server       string          `json:"server"`
	AssetVersion string          `json:"assetVersion"`
	AppVersion   string          `json:"appVersion"`
	AssetHash    string          `json:"assetHash,omitempty"`
	CommittedAt  int64           `json:"committedAt"`
	Action       string          `json:"action,omitempty"`
	Types        []string        `json:"types,omitempty"`
	TotalChanged int64           `json:"totalChanged"`
	Limit        int             `json:"limit"`
	NextCursor   string          `json:"nextCursor,omitempty"`
	Items        []assetDiffItem `json:"items"`
}

type bundleDiffsResponse struct {
	Server       string           `json:"server"`
	AssetVersion string           `json:"assetVersion"`
	AppVersion   string           `json:"appVersion"`
	AssetHash    string           `json:"assetHash,omitempty"`
	CommittedAt  int64            `json:"committedAt"`
	Prefix       string           `json:"prefix,omitempty"`
	TotalBundles int64            `json:"totalBundles"`
	Limit        int              `json:"limit"`
	NextCursor   string           `json:"nextCursor,omitempty"`
	Items        []bundleDiffItem `json:"items"`
}

type bundleDiffItem struct {
	BundlePath  string `json:"bundlePath"`
	Fingerprint string `json:"fingerprint,omitempty"`
	FileCount   int64  `json:"fileCount"`
	TotalSize   int64  `json:"totalSize"`
	Source      string `json:"source,omitempty"`
	FilesURL    string `json:"filesUrl,omitempty"`
}

type bundleDiffFilesResponse struct {
	Server       string          `json:"server"`
	AssetVersion string          `json:"assetVersion"`
	AppVersion   string          `json:"appVersion"`
	BundlePath   string          `json:"bundlePath"`
	TotalChanged int64           `json:"totalChanged"`
	Limit        int             `json:"limit"`
	NextCursor   string          `json:"nextCursor,omitempty"`
	Items        []assetDiffItem `json:"items"`
}

type assetDiffItem struct {
	ChangeType  string `json:"changeType"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	Source      string `json:"source"`
	Size        int64  `json:"size"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Sha256      string `json:"sha256,omitempty"`
	BundlePath  string `json:"bundlePath,omitempty"`
}

func (h *AssetVersionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.DB == nil {
		http.Error(w, "version api unavailable", http.StatusServiceUnavailable)
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

	if strings.HasSuffix(r.URL.Path, "/bundle-diffs") {
		h.serveBundleDiffs(w, r, server)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/bundle-diff-files") {
		h.serveBundleDiffFiles(w, r, server)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/diff") {
		h.serveDiff(w, r, server)
		return
	}
	h.serveVersions(w, r, server)
}

func (h *AssetVersionsHandler) serveVersions(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}
	beforeID, err := parseVersionsCursor(q.Get("cursor"))
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("versions\x00%d\x00%s\x00%d\x00%d", revision, server, beforeID, limit)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	versions, nextBeforeID, err := store.ListVersions(r.Context(), h.DB, server, beforeID, limit)
	if err != nil {
		http.Error(w, "list versions", http.StatusInternalServerError)
		return
	}
	resp := assetVersionsResponse{
		Server: server,
		Limit:  limit,
		Items:  make([]assetVersionItem, 0, len(versions)),
	}
	if nextBeforeID > 0 {
		resp.NextCursor = strconv.FormatInt(nextBeforeID, 10)
	}
	for _, v := range versions {
		item := assetVersionItem{
			AssetVersion:  v.AssetVersion,
			AppVersion:    v.AppVersion,
			AssetHash:     v.AssetHash,
			BundleCount:   v.BundleCount,
			CommittedAt:   v.CommittedAt,
			ChangedAssets: v.ChangedAssets,
			DiffURL:       "/api/assets/diff?server=" + url.QueryEscape(server) + "&version=" + url.QueryEscape(v.AssetVersion),
			BundleDiffURL: "/api/assets/bundle-diffs?server=" + url.QueryEscape(server) + "&version=" + url.QueryEscape(v.AssetVersion),
		}
		if stats := strings.TrimSpace(v.StatsJSON); stats != "" && json.Valid([]byte(stats)) {
			item.Stats = json.RawMessage(stats)
		}
		resp.Items = append(resp.Items, item)
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func (h *AssetVersionsHandler) serveBundleDiffs(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	version, err := normalizeAssetVersion(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
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

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("bundle-diffs\x00%d\x00%s\x00%s\x00%s\x00%s\x00%d", revision, server, version, prefix, cursor, limit)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	diff, found, err := store.DiffVersionBundles(r.Context(), h.DB, server, version, store.DiffBundleOptions{
		Prefix: prefix,
		Cursor: cursor,
		Limit:  limit,
	})
	if err != nil {
		http.Error(w, "diff version bundles", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}

	resp := bundleDiffsResponse{
		Server:       server,
		AssetVersion: diff.Version.AssetVersion,
		AppVersion:   diff.Version.AppVersion,
		AssetHash:    diff.Version.AssetHash,
		CommittedAt:  diff.Version.CommittedAt,
		Prefix:       prefix,
		TotalBundles: diff.TotalChanged,
		Limit:        limit,
		NextCursor:   diff.NextCursor,
		Items:        make([]bundleDiffItem, 0, len(diff.Items)),
	}
	for _, e := range diff.Items {
		filesURL := "/api/assets/bundle-diff-files?server=" + url.QueryEscape(server) +
			"&version=" + url.QueryEscape(version) +
			"&path=" + url.QueryEscape(e.BundlePath)
		resp.Items = append(resp.Items, bundleDiffItem{
			BundlePath:  e.BundlePath,
			Fingerprint: e.Fingerprint,
			FileCount:   e.FileCount,
			TotalSize:   e.TotalSize,
			Source:      e.Source,
			FilesURL:    filesURL,
		})
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func (h *AssetVersionsHandler) serveBundleDiffFiles(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	version, err := normalizeAssetVersion(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	bundlePath := strings.TrimPrefix(q.Get("path"), "/")
	if bundlePath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}
	action, err := parseDiffAction(q.Get("action"))
	if err != nil {
		http.Error(w, "bad action", http.StatusBadRequest)
		return
	}
	rawTypes := q.Get("types")
	var exts []string
	if rawTypes != "" {
		exts, err = parseDiffTypes(rawTypes, nil)
		if err != nil {
			http.Error(w, "bad types", http.StatusBadRequest)
			return
		}
	}
	cursor, err := parseDiffCursor(q.Get("cursor"))
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("bundle-diff-files\x00%d\x00%s\x00%s\x00%s\x00%s\x00%d\x00%s\x00%d\x00%t",
		revision, server, version, bundlePath, action, limit, cursor.Path, cursor.Size, cursor.Set)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	diff, found, err := store.DiffBundleFiles(r.Context(), h.DB, server, version, bundlePath, store.DiffOptions{
		Action: action,
		Exts:   exts,
		Cursor: cursor,
		Limit:  limit,
	})
	if err != nil {
		http.Error(w, "diff bundle files", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}

	resp := bundleDiffFilesResponse{
		Server:       server,
		AssetVersion: diff.Version.AssetVersion,
		AppVersion:   diff.Version.AppVersion,
		BundlePath:   bundlePath,
		TotalChanged: diff.TotalChanged,
		Limit:        limit,
		NextCursor:   encodeDiffCursor(diff.NextCursor),
		Items:        make([]assetDiffItem, 0, len(diff.Items)),
	}
	for _, e := range diff.Items {
		resp.Items = append(resp.Items, toAssetDiffItem(server, e))
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func (h *AssetVersionsHandler) serveDiff(w http.ResponseWriter, r *http.Request, server string) {
	q := r.URL.Query()
	version, err := normalizeAssetVersion(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	limit, err := parseBrowseLimit(q.Get("limit"), h.defaultLimit(), h.maxLimit())
	if err != nil {
		http.Error(w, "bad limit", http.StatusBadRequest)
		return
	}
	action, err := parseDiffAction(q.Get("action"))
	if err != nil {
		http.Error(w, "bad action", http.StatusBadRequest)
		return
	}
	rawTypes := q.Get("types")
	var exts []string
	var minMp3Size int64
	if rawTypes == "" {
		exts = h.defaultTypes()
		minMp3Size = h.defaultMinMp3Size()
	} else {
		exts, err = parseDiffTypes(rawTypes, h.defaultTypes())
		if err != nil {
			http.Error(w, "bad types", http.StatusBadRequest)
			return
		}
		minMp3Size = 0
	}
	cursor, err := parseDiffCursor(q.Get("cursor"))
	if err != nil {
		http.Error(w, "bad cursor", http.StatusBadRequest)
		return
	}

	revision, err := store.LatestAssetID(r.Context(), h.DB)
	if err != nil {
		http.Error(w, "lookup revision", http.StatusInternalServerError)
		return
	}
	cacheKey := fmt.Sprintf("diff\x00%d\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00%d\x00%t",
		revision, server, version, action, strings.Join(exts, ","), minMp3Size, limit, cursor.Path, cursor.Size, cursor.Set)
	if body, ok := h.getCached(cacheKey); ok {
		writeBrowseJSON(w, r, body)
		return
	}

	diff, found, err := store.DiffVersion(r.Context(), h.DB, server, version, store.DiffOptions{
		Action:     action,
		Exts:       exts,
		MinMp3Size: minMp3Size,
		Cursor:     cursor,
		Limit:      limit,
	})
	if err != nil {
		http.Error(w, "diff version", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	resp := assetDiffResponse{
		Server:       server,
		AssetVersion: diff.Version.AssetVersion,
		AppVersion:   diff.Version.AppVersion,
		AssetHash:    diff.Version.AssetHash,
		CommittedAt:  diff.Version.CommittedAt,
		Action:       action,
		Types:        exts,
		TotalChanged: diff.TotalChanged,
		Limit:        limit,
		NextCursor:   encodeDiffCursor(diff.NextCursor),
		Items:        make([]assetDiffItem, 0, len(diff.Items)),
	}
	for _, e := range diff.Items {
		resp.Items = append(resp.Items, toAssetDiffItem(server, e))
	}

	body, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)
		return
	}
	h.setCached(cacheKey, body)
	writeBrowseJSON(w, r, body)
}

func toAssetDiffItem(server string, e store.DiffEntry) assetDiffItem {
	changeType := "added"
	if e.Existed {
		changeType = "updated"
	}
	source := "shared"
	if e.IsOverride {
		source = "override"
	}
	return assetDiffItem{
		ChangeType:  changeType,
		Path:        e.Path,
		URL:         "/sekai-" + server + "-assets/" + escapeAssetPath(e.Path),
		Source:      source,
		Size:        e.Size,
		Fingerprint: e.Fingerprint,
		Sha256:      e.Sha256,
		BundlePath:  e.BundlePath,
	}
}

// normalizeAssetVersion accepts the version strings HIP clients commit
// (dotted numerics in practice) without being able to smuggle control bytes
// or unbounded input into SQL/logs.
func normalizeAssetVersion(raw string) (string, error) {
	if raw == "" || len(raw) > maxAssetVersionLen {
		return "", errBadAssetVersion
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x20 || raw[i] == 0x7f {
			return "", errBadAssetVersion
		}
	}
	return raw, nil
}

var errBadAssetVersion = fmt.Errorf("bad asset version")

// parseDiffTypes resolves the ?types= filter: absent → the handler default,
// "all" → no filter, otherwise a comma-separated list of extension tokens
// restricted to [a-z0-9] so they can be embedded in LIKE patterns verbatim.
func parseDiffTypes(raw string, def []string) ([]string, error) {
	if raw == "" {
		return def, nil
	}
	if raw == "all" {
		return nil, nil
	}
	parts := strings.Split(strings.ToLower(raw), ",")
	if len(parts) > maxDiffTypes {
		return nil, fmt.Errorf("too many types")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.TrimPrefix(p, "."))
		if p == "" || len(p) > 16 {
			return nil, fmt.Errorf("bad type token")
		}
		for i := 0; i < len(p); i++ {
			c := p[i]
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
				return nil, fmt.Errorf("bad type token")
			}
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out, nil
}

// parseDiffCursor decodes "<size>:<path>" as emitted by encodeDiffCursor.
// The path part reuses the browse cursor validation so only safe relative
// paths round-trip.
func parseDiffCursor(raw string) (store.DiffCursor, error) {
	if raw == "" {
		return store.DiffCursor{}, nil
	}
	sep := strings.IndexByte(raw, ':')
	if sep <= 0 || sep == len(raw)-1 {
		return store.DiffCursor{}, fmt.Errorf("bad cursor")
	}
	size, err := strconv.ParseInt(raw[:sep], 10, 64)
	if err != nil || size < 0 {
		return store.DiffCursor{}, fmt.Errorf("bad cursor")
	}
	path, err := normalizeBrowseCursor(raw[sep+1:], "")
	if err != nil {
		return store.DiffCursor{}, err
	}
	return store.DiffCursor{Size: size, Path: path, Set: true}, nil
}

func encodeDiffCursor(c store.DiffCursor) string {
	if !c.Set {
		return ""
	}
	return strconv.FormatInt(c.Size, 10) + ":" + c.Path
}

func parseVersionsCursor(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("bad cursor")
	}
	return id, nil
}

// parseDiffAction validates the ?action= query parameter.
// Allowed values: "added" (default), "updated" (or "replaced"), "all".
func parseDiffAction(raw string) (string, error) {
	if raw == "" {
		return "added", nil
	}
	switch strings.ToLower(raw) {
	case "added":
		return "added", nil
	case "updated", "replaced":
		return "updated", nil
	case "all":
		return "all", nil
	default:
		return "", fmt.Errorf("bad action")
	}
}

func (h *AssetVersionsHandler) defaultTypes() []string {
	if h.DefaultTypes != nil {
		return h.DefaultTypes
	}
	return defaultDiffTypes
}

func (h *AssetVersionsHandler) defaultMinMp3Size() int64 {
	if h.DefaultMinMp3Size != nil {
		return *h.DefaultMinMp3Size
	}
	return defaultMinMp3Size
}


func (h *AssetVersionsHandler) defaultLimit() int {
	if h.DefaultLimit > 0 {
		return h.DefaultLimit
	}
	return defaultBrowseLimit
}

func (h *AssetVersionsHandler) maxLimit() int {
	if h.MaxLimit > 0 {
		return h.MaxLimit
	}
	return maxBrowseLimit
}

func (h *AssetVersionsHandler) cacheTTL() time.Duration {
	if h.CacheTTL > 0 {
		return h.CacheTTL
	}
	return defaultBrowseCacheTTL
}

func (h *AssetVersionsHandler) maxCacheItems() int {
	if h.MaxCacheItems > 0 {
		return h.MaxCacheItems
	}
	return defaultBrowseCacheItems
}

func (h *AssetVersionsHandler) getCached(key string) ([]byte, bool) {
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

func (h *AssetVersionsHandler) setCached(key string, body []byte) {
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
