package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCORSSimpleRequest(t *testing.T) {
	h := withCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/sekai-jp-assets/foo", nil)
	req.Header.Set("Origin", "https://example.pjsk.moe")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("ACAO = %q, want *", got)
	}
	expose := rr.Header().Get("Access-Control-Expose-Headers")
	for _, want := range []string{"ETag", "Content-Length", "X-Serve-From"} {
		if !strings.Contains(expose, want) {
			t.Fatalf("expose-headers missing %q, got %q", want, expose)
		}
	}
}

func TestCORSPreflight(t *testing.T) {
	// Inner handler must NOT run for a preflight.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("inner handler ran for preflight")
	})
	h := withCORS(inner)
	req := httptest.NewRequest(http.MethodOptions, "/sekai-jp-assets/foo", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "range,if-none-match")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("ACAO = %q", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "GET") {
		t.Fatalf("allow-methods = %q", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Headers"); got != "range,if-none-match" {
		t.Fatalf("allow-headers = %q", got)
	}
	if rr.Header().Get("Access-Control-Max-Age") == "" {
		t.Fatalf("no max-age")
	}
}
