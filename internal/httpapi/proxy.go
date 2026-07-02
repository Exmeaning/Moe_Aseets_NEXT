package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/Team-Haruki/moe-assets-gateway/internal/index"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
)

// ProxyHandler is the readonly reverse proxy for
// GET /sekai-{server}-assets/{path...}.
type ProxyHandler struct {
	Idx     *index.Index
	Storage *storage.Client
	Log     *slog.Logger
	// AllowedServers is the whitelist derived from config.
	AllowedServers map[string]struct{}
	// Metrics hooks (nil-safe).
	Metrics *ProxyMetrics
}

// ProxyMetrics is a minimal set of counters recorded per request.
type ProxyMetrics struct {
	RequestsTotal func(server, result string)
	BytesOut      func(n int64)
}

func (m *ProxyMetrics) requests(server, result string) {
	if m != nil && m.RequestsTotal != nil {
		m.RequestsTotal(server, result)
	}
}
func (m *ProxyMetrics) bytes(n int64) {
	if m != nil && m.BytesOut != nil {
		m.BytesOut(n)
	}
}

// ServeHTTP implements http.Handler. Expects r.URL.Path already stripped of
// the /sekai-{server}-assets/ prefix by the router; the resolved server is
// stashed in the request context via serverKey / relPathKey.
func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	server, _ := r.Context().Value(serverKey{}).(string)
	relPath, _ := r.Context().Value(relPathKey{}).(string)

	if _, ok := h.AllowedServers[server]; !ok {
		h.Metrics.requests(server, "bad_server")
		http.NotFound(w, r)
		return
	}
	safe, err := storage.SafeRelPath(relPath)
	if err != nil {
		h.Metrics.requests(server, "bad_path")
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}

	res := h.Idx.Load().Lookup(server, safe)
	if !res.Found {
		h.Metrics.requests(server, "miss_index")
		w.Header().Set("X-Miss", "not-indexed")
		http.NotFound(w, r)
		return
	}

	var storeKey, serveFrom string
	if res.FromShared {
		storeKey = storage.SharedKey(safe)
		serveFrom = "shared"
	} else {
		storeKey = storage.OverrideKey(server, safe)
		serveFrom = "override"
	}

	// Pre-emit standard cache headers (they apply even to error responses that
	// go through 200 body; not for the 404 path above).
	setCacheHeaders(w, res.Fingerprint, res.Version, serveFrom)

	// Content-Type hint by extension; upstream may override on the actual
	// response.
	if ct := mimeByExt(safe); ct != "" {
		w.Header().Set("Content-Type", ct)
	}

	// Build upstream request; pass through Range for byte-range support.
	upstreamHeaders := http.Header{}
	if rng := r.Header.Get("Range"); rng != "" {
		upstreamHeaders.Set("Range", rng)
	}
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		// Handle 304 directly rather than round-tripping to filer: we already
		// know the current ETag from the index.
		if etagMatch(ifNoneMatch, res.Fingerprint) {
			w.WriteHeader(http.StatusNotModified)
			h.Metrics.requests(server, "304")
			return
		}
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// HEAD short-circuit: use HEAD upstream and copy over Content-Length only.
	if r.Method == http.MethodHead {
		hdrs, ok, err := h.Storage.Head(ctx, storeKey)
		if err != nil {
			h.Log.Error("upstream HEAD failed", "err", err, "key", storeKey)
			h.Metrics.requests(server, "upstream_err")
			http.Error(w, "upstream error", http.StatusBadGateway)
			return
		}
		if !ok {
			w.Header().Set("X-Miss", "upstream")
			h.Metrics.requests(server, "miss_upstream")
			http.NotFound(w, r)
			return
		}
		if cl := hdrs.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
		if lm := hdrs.Get("Last-Modified"); lm != "" {
			w.Header().Set("Last-Modified", lm)
		}
		w.WriteHeader(http.StatusOK)
		h.Metrics.requests(server, "head_ok")
		return
	}

	resp, err := h.Storage.Get(ctx, storeKey, upstreamHeaders)
	if err != nil {
		h.Log.Error("upstream GET failed", "err", err, "key", storeKey)
		h.Metrics.requests(server, "upstream_err")
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		w.Header().Set("X-Miss", "upstream")
		h.Metrics.requests(server, "miss_upstream")
		http.NotFound(w, r)
		return
	}
	if resp.StatusCode >= 400 {
		h.Log.Warn("upstream status", "status", resp.StatusCode, "key", storeKey)
		h.Metrics.requests(server, "upstream_err")
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}

	// Pass through content-length / content-range / accept-ranges for range
	// support and correct streaming client behaviour.
	for _, k := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	// Upstream may know a better content-type than our extension guess.
	if ct := resp.Header.Get("Content-Type"); ct != "" && !isGenericOctet(ct) {
		w.Header().Set("Content-Type", ct)
	}

	w.WriteHeader(resp.StatusCode)
	n, err := io.Copy(w, resp.Body)
	if err != nil && !errors.Is(err, context.Canceled) {
		h.Log.Warn("stream copy error", "err", err, "bytes", n, "key", storeKey)
	}
	h.Metrics.bytes(n)
	h.Metrics.requests(server, "ok")
}

func setCacheHeaders(w http.ResponseWriter, fingerprint, version, from string) {
	if fingerprint != "" {
		w.Header().Set("ETag", `"`+fingerprint+`"`)
	}
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("X-Serve-From", from)
	if version != "" {
		w.Header().Set("X-Version", version)
	}
}

// etagMatch is a lenient comparison — clients often send quoted or "W/" prefixed.
func etagMatch(headerVal, fingerprint string) bool {
	quoted := `"` + fingerprint + `"`
	for _, part := range strings.Split(headerVal, ",") {
		p := strings.TrimSpace(part)
		p = strings.TrimPrefix(p, "W/")
		if p == "*" || p == quoted || p == fingerprint {
			return true
		}
	}
	return false
}

func isGenericOctet(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	return ct == "application/octet-stream" || ct == ""
}

func mimeByExt(p string) string {
	ext := strings.ToLower(filepath.Ext(p))
	if ext == "" {
		return ""
	}
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	// Fallback map for a couple of pjsk-common extensions the stdlib may miss.
	switch ext {
	case ".webp":
		return "image/webp"
	case ".acb", ".awb":
		return "application/octet-stream"
	case ".usm":
		return "application/octet-stream"
	}
	return ""
}
