package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func insertBrowserAsset(t *testing.T, db *sql.DB, a store.Asset) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAsset(context.Background(), tx, a); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAssetBrowserHandlerListsPagedDirectory(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	insertBrowserAsset(t, db, store.Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "shared-a", Sha256: "sha-a",
		IsOverride: false, StorageKey: "/shared-assets/img/a.png", Size: 100})
	insertBrowserAsset(t, db, store.Asset{Server: "jp", Path: "img/sub/b.png", Version: "v1", Fingerprint: "shared-b", Sha256: "sha-b",
		IsOverride: false, StorageKey: "/shared-assets/img/sub/b.png", Size: 200})
	insertBrowserAsset(t, db, store.Asset{Server: "en", Path: "img/a.png", Version: "v2", Fingerprint: "override-a", Sha256: "sha-override",
		IsOverride: true, StorageKey: "/overrides/en/img/a.png", Size: 300})

	handler := &AssetBrowserHandler{
		DB:             db,
		AllowedServers: map[string]struct{}{"en": {}},
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/assets/browse?server=en&prefix=img&limit=1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	var resp assetBrowseResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Server != "en" || resp.Prefix != "img/" || resp.Limit != 1 || resp.NextCursor != "img/a.png" {
		t.Fatalf("bad response envelope: %+v", resp)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items=%d: %+v", len(resp.Items), resp.Items)
	}
	item := resp.Items[0]
	if item.Type != store.BrowseKindAsset || item.Path != "img/a.png" || item.Source != "override" || item.URL != "/sekai-en-assets/img/a.png" {
		t.Fatalf("bad item: %+v", item)
	}
	if item.Size == nil || *item.Size != 300 || item.Fingerprint != "override-a" {
		t.Fatalf("bad metadata: %+v", item)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/assets/browse?server=en&prefix=img&cursor=img%2Fa.png&limit=1", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	resp = assetBrowseResponse{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Type != store.BrowseKindDirectory || resp.Items[0].Path != "img/sub/" || resp.NextCursor != "" {
		t.Fatalf("bad second page: %+v", resp)
	}
}

func TestAssetBrowserHandlerRejectsBadInput(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := &AssetBrowserHandler{
		DB:             db,
		AllowedServers: map[string]struct{}{"jp": {}},
	}

	cases := []string{
		"/api/assets/browse?server=jp&prefix=..%2Fx",
		"/api/assets/browse?server=jp&prefix=/x",
		"/api/assets/browse?server=jp&limit=0",
		"/api/assets/browse?server=jp&prefix=img&cursor=other%2Fa.png",
	}
	for _, target := range cases {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d body=%q", target, rr.Code, rr.Body.String())
		}
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/assets/browse?server=kr", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown server status=%d", rr.Code)
	}
}
