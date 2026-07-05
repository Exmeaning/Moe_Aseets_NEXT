package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Team-Haruki/moe-assets-gateway/internal/index"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func TestProxyUsesIndexedStorageKey(t *testing.T) {
	var requestedPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		if r.URL.Path != "/shared-assets/source.dat" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAsset(context.Background(), tx, store.Asset{
		Server:      "kr",
		BundlePath:  "public",
		Path:        "public/file.dat",
		Version:     "v1",
		Fingerprint: "fp",
		Sha256:      "sha",
		Size:        2,
		IsOverride:  true,
		StorageKey:  "/shared-assets/source.dat",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	idx := index.New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	handler := &ProxyHandler{
		Idx:            idx,
		Storage:        storage.New(upstream.URL),
		Log:            slog.Default(),
		AllowedServers: map[string]struct{}{"kr": {}},
	}

	req := httptest.NewRequest(http.MethodGet, "/ignored", nil)
	ctx := context.WithValue(req.Context(), serverKey{}, "kr")
	ctx = context.WithValue(ctx, relPathKey{}, "public/file.dat")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req.WithContext(ctx))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q requested=%q", rr.Code, rr.Body.String(), requestedPath)
	}
	if requestedPath != "/shared-assets/source.dat" {
		t.Fatalf("proxy requested %q, want indexed storage key", requestedPath)
	}
}
