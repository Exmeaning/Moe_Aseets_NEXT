package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func newBundlesFixture(t *testing.T) *AssetBundlesHandler {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/logo.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s2", Size: 50, StorageKey: "/shared-assets/event/foo/logo.webp"})
	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "sound/bgm", Path: "sound/bgm/main.mp3", Version: "v1",
		Fingerprint: "b-bgm", Sha256: "s3", Size: 300, StorageKey: "/shared-assets/sound/bgm/main.mp3"})
	insertBrowserAsset(t, db, store.Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v2",
		Fingerprint: "b-foo-en", Sha256: "s1e", Size: 120, IsOverride: true, StorageKey: "/overrides/en/event/foo/bg.webp"})
	return &AssetBundlesHandler{
		DB:             db,
		AllowedServers: map[string]struct{}{"jp": {}, "en": {}},
	}
}

func TestAssetBundlesHandlerListsBundles(t *testing.T) {
	handler := newBundlesFixture(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/assets/bundles?server=jp", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	var resp bundleBrowseResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 2 ||
		resp.Items[0].Type != store.BrowseKindDirectory || resp.Items[0].Path != "event/" ||
		resp.Items[1].Type != store.BrowseKindDirectory || resp.Items[1].Path != "sound/" {
		t.Fatalf("bad root listing: %+v", resp.Items)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/assets/bundles?server=en&prefix=event", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	resp = bundleBrowseResponse{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items=%d: %+v", len(resp.Items), resp.Items)
	}
	item := resp.Items[0]
	if item.Type != store.BrowseKindBundle || item.Path != "event/foo" || item.Name != "foo" ||
		item.Source != "override" || item.Fingerprint != "b-foo-en" {
		t.Fatalf("bad bundle item: %+v", item)
	}
	if item.FileCount == nil || *item.FileCount != 1 || item.TotalSize == nil || *item.TotalSize != 120 {
		t.Fatalf("bad bundle aggregates: %+v", item)
	}
	if item.FilesURL != "/api/assets/bundle-files?server=en&path=event%2Ffoo" {
		t.Fatalf("bad filesUrl: %q", item.FilesURL)
	}
}

func TestAssetBundlesHandlerListsBundleFiles(t *testing.T) {
	handler := newBundlesFixture(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/assets/bundle-files?server=en&path=event%2Ffoo", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	var resp bundleFilesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Bundle.Path != "event/foo" || resp.Bundle.Source != "override" || resp.Bundle.Fingerprint != "b-foo-en" {
		t.Fatalf("bad bundle meta: %+v", resp.Bundle)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items=%d: %+v", len(resp.Items), resp.Items)
	}
	bg := resp.Items[0]
	if bg.Path != "event/foo/bg.webp" || bg.Name != "bg.webp" || bg.Source != "override" ||
		bg.URL != "/sekai-en-assets/event/foo/bg.webp" || bg.Size == nil || *bg.Size != 120 {
		t.Fatalf("bad bg item: %+v", bg)
	}
	logo := resp.Items[1]
	if logo.Path != "event/foo/logo.webp" || logo.Source != "shared" {
		t.Fatalf("bad logo item: %+v", logo)
	}

	// Pagination via cursor.
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet,
		"/api/assets/bundle-files?server=en&path=event%2Ffoo&limit=1&cursor=event%2Ffoo%2Fbg.webp", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	resp = bundleFilesResponse{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Path != "event/foo/logo.webp" || resp.NextCursor != "" {
		t.Fatalf("bad second page: %+v", resp)
	}
}

func TestAssetBundlesHandlerRejectsBadInput(t *testing.T) {
	handler := newBundlesFixture(t)

	badRequests := []string{
		"/api/assets/bundles?server=jp&prefix=..%2Fx",
		"/api/assets/bundles?server=jp&limit=0",
		"/api/assets/bundle-files?server=jp",
		"/api/assets/bundle-files?server=jp&path=%2Fabs",
		"/api/assets/bundle-files?server=jp&path=event%2Ffoo&cursor=other%2Fa.png",
	}
	for _, target := range badRequests {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d body=%q", target, rr.Code, rr.Body.String())
		}
	}

	notFound := []string{
		"/api/assets/bundles?server=kr",
		"/api/assets/bundle-files?server=jp&path=event%2Fnope",
	}
	for _, target := range notFound {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status=%d body=%q", target, rr.Code, rr.Body.String())
		}
	}
}
