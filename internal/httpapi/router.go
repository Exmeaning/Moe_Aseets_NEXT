package httpapi

import (
	"context"
	"net/http"
	"strings"
)

// context keys used to hand parsed (server, relPath) from router → handler.
type serverKey struct{}
type relPathKey struct{}

// Router wires the routes: /healthz, /metrics, /api/assets/browse,
// /sekai-{server}-assets/*.
type Router struct {
	Proxy   *ProxyHandler
	Browser *AssetBrowserHandler
	Metrics http.Handler // optional
	Limiter *IPRateLimiter
}

// Handler returns the top-level http.Handler for the read port.
func (r *Router) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	if r.Metrics != nil {
		mux.Handle("/metrics", r.Metrics)
	}
	if r.Browser != nil {
		mux.Handle("/api/assets/browse", r.Browser)
	}

	proxy := r.Proxy
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		server, rel, ok := parseSekaiPath(req.URL.Path)
		if !ok {
			http.NotFound(w, req)
			return
		}
		ctx := context.WithValue(req.Context(), serverKey{}, server)
		ctx = context.WithValue(ctx, relPathKey{}, rel)
		proxy.ServeHTTP(w, req.WithContext(ctx))
	})

	var h http.Handler = mux
	if r.Limiter != nil {
		h = r.Limiter.Middleware(h)
	}
	// CORS is applied outermost so that OPTIONS preflights never spend a
	// rate-limit token and never hit the proxy handler.
	h = withCORS(h)
	h = withAccessLog(h)
	return h
}

// parseSekaiPath parses `/sekai-{server}-assets/{path...}` and returns
// (server, path, ok). Trailing slashes are treated as a miss to avoid
// serving directory listings.
func parseSekaiPath(p string) (string, string, bool) {
	const prefix = "/sekai-"
	const suffix = "-assets/"
	if !strings.HasPrefix(p, prefix) {
		return "", "", false
	}
	rest := p[len(prefix):]
	slash := strings.Index(rest, suffix)
	if slash <= 0 {
		return "", "", false
	}
	server := rest[:slash]
	rel := rest[slash+len(suffix):]
	if rel == "" {
		return "", "", false
	}
	// A "server" token must be all lowercase letters (mvp: jp/en/tw/kr/cn).
	for i := 0; i < len(server); i++ {
		c := server[i]
		if c < 'a' || c > 'z' {
			return "", "", false
		}
	}
	return server, rel, true
}

// withAccessLog is a minimal access log middleware using r.Header for X-Forwarded-For.
// (For MVP we log via the proxy handler's own logger for cache hits/misses;
// this middleware is a placeholder for future full access logs.)
func withAccessLog(h http.Handler) http.Handler {
	return h
}
