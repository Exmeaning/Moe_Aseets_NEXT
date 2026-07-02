package httpapi

import "net/http"

// withCORS is a permissive CORS middleware suitable for a public read-only
// static-asset CDN. It:
//
//   - always sets `Access-Control-Allow-Origin: *`
//   - answers OPTIONS preflights directly (204) without hitting the proxy
//   - exposes the headers pjsk browser clients typically want to read
//     (ETag / Content-Length / X-Serve-From / X-Version)
//
// The read path is credential-free, so wildcard origin is safe: no cookies /
// Authorization headers are ever set on the response, and browsers refuse
// to send them with `credentials: 'omit'` (the default for cross-origin
// fetch of subresources like <img> / <audio>).
func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := w.Header()
		hdr.Set("Access-Control-Allow-Origin", "*")
		hdr.Set("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Range, Accept-Ranges, X-Serve-From, X-Version, X-Miss")
		// Vary lets caches key on Origin correctly if this middleware is ever
		// tightened to reflect the request origin.
		hdr.Add("Vary", "Origin")

		if r.Method == http.MethodOptions {
			// Preflight. Echo whatever the client asked for; wildcard is fine
			// because we never send credentials.
			if reqMethod := r.Header.Get("Access-Control-Request-Method"); reqMethod != "" {
				hdr.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
			}
			if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
				hdr.Set("Access-Control-Allow-Headers", reqHeaders)
			}
			hdr.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h.ServeHTTP(w, r)
	})
}
