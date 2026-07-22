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

func insertVersionRow(t *testing.T, db *sql.DB, v store.Version) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertVersion(context.Background(), tx, v); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// seedVersionsDB commits jp v1 (a.webp/100, c.acb/999) then jp v2
// (a.webp/300 updated, b.mp3/1500000 added).
func seedVersionsDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "img", Path: "img/a.webp", Version: "v1", Fingerprint: "fa", Sha256: "sa", Size: 100, StorageKey: "/shared-assets/img/a.webp"})
	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "snd", Path: "snd/c.acb", Version: "v1", Fingerprint: "fc", Sha256: "sc", Size: 999, StorageKey: "/shared-assets/snd/c.acb"})
	insertVersionRow(t, db, store.Version{Server: "jp", AppVersion: "1.0", AssetVersion: "v1", AssetHash: "h1", BundleCount: 2, StatsJSON: `{"uploaded_shared":2}`})

	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "img", Path: "img/a.webp", Version: "v2", Fingerprint: "fa2", Sha256: "sa2", Size: 300, StorageKey: "/shared-assets/img/a.webp"})
	insertBrowserAsset(t, db, store.Asset{Server: "jp", BundlePath: "snd", Path: "snd/b.mp3", Version: "v2", Fingerprint: "fb", Sha256: "sb", Size: 1500000, StorageKey: "/shared-assets/snd/b.mp3"})
	insertVersionRow(t, db, store.Version{Server: "jp", AppVersion: "1.1", AssetVersion: "v2", AssetHash: "h2", BundleCount: 2, StatsJSON: `{"uploaded_shared":2}`})
	return db
}

func newVersionsHandler(db *sql.DB) *AssetVersionsHandler {
	return &AssetVersionsHandler{
		DB:             db,
		AllowedServers: map[string]struct{}{"jp": {}},
	}
}

func getJSON(t *testing.T, h http.Handler, target string, out any) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	h.ServeHTTP(rr, req)
	if rr.Code == http.StatusOK && out != nil {
		if err := json.Unmarshal(rr.Body.Bytes(), out); err != nil {
			t.Fatalf("unmarshal %s: %v body=%q", target, err, rr.Body.String())
		}
	}
	return rr
}

func TestAssetVersionsHandlerListsVersions(t *testing.T) {
	db := seedVersionsDB(t)
	h := newVersionsHandler(db)

	var resp assetVersionsResponse
	rr := getJSON(t, h, "/api/assets/versions?server=jp", &resp)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	if resp.Server != "jp" || len(resp.Items) != 2 || resp.NextCursor != "" {
		t.Fatalf("bad envelope: %+v", resp)
	}
	newest := resp.Items[0]
	if newest.AssetVersion != "v2" || newest.AppVersion != "1.1" || newest.ChangedAssets != 2 {
		t.Fatalf("bad newest item: %+v", newest)
	}
	if newest.DiffURL != "/api/assets/diff?server=jp&version=v2" {
		t.Fatalf("bad diffUrl: %q", newest.DiffURL)
	}
	var stats map[string]int64
	if err := json.Unmarshal(newest.Stats, &stats); err != nil || stats["uploaded_shared"] != 2 {
		t.Fatalf("bad stats passthrough: %s err=%v", newest.Stats, err)
	}

	// Page walk with limit=1.
	resp = assetVersionsResponse{}
	rr = getJSON(t, h, "/api/assets/versions?server=jp&limit=1", &resp)
	if rr.Code != http.StatusOK || len(resp.Items) != 1 || resp.Items[0].AssetVersion != "v2" || resp.NextCursor == "" {
		t.Fatalf("bad first page: %d %+v", rr.Code, resp)
	}
	resp2 := assetVersionsResponse{}
	rr = getJSON(t, h, "/api/assets/versions?server=jp&limit=1&cursor="+resp.NextCursor, &resp2)
	if rr.Code != http.StatusOK || len(resp2.Items) != 1 || resp2.Items[0].AssetVersion != "v1" || resp2.NextCursor != "" {
		t.Fatalf("bad second page: %d %+v", rr.Code, resp2)
	}
}

func TestAssetDiffHandlerSizeOrderTypesAndCursor(t *testing.T) {
	db := seedVersionsDB(t)
	h := newVersionsHandler(db)

	// Default types (webp, mp3>=1M) and default action (added): v1's c.acb is hidden.
	var v1 assetDiffResponse
	rr := getJSON(t, h, "/api/assets/diff?server=jp&version=v1", &v1)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	if v1.TotalChanged != 1 || len(v1.Items) != 1 || v1.Items[0].Path != "img/a.webp" {
		t.Fatalf("default type filter wrong: %+v", v1)
	}
	if v1.Items[0].ChangeType != "added" || v1.Items[0].URL != "/sekai-jp-assets/img/a.webp" || v1.Items[0].Source != "shared" {
		t.Fatalf("bad v1 item: %+v", v1.Items[0])
	}

	// types=all exposes c.acb, sorted by size desc.
	var all assetDiffResponse
	rr = getJSON(t, h, "/api/assets/diff?server=jp&version=v1&types=all", &all)
	if rr.Code != http.StatusOK || all.TotalChanged != 2 || len(all.Items) != 2 ||
		all.Items[0].Path != "snd/c.acb" || all.Items[1].Path != "img/a.webp" {
		t.Fatalf("types=all wrong: %d %+v", rr.Code, all)
	}
	if len(all.Types) != 0 {
		t.Fatalf("types=all should omit types echo: %+v", all.Types)
	}

	// v2 default: action=added (default), webp + mp3>=1M.
	// b.mp3 is 1.5MB added, a.webp is updated (so excluded by default action).
	var v2Default assetDiffResponse
	rr = getJSON(t, h, "/api/assets/diff?server=jp&version=v2", &v2Default)
	if rr.Code != http.StatusOK || v2Default.TotalChanged != 1 || len(v2Default.Items) != 1 || v2Default.Items[0].Path != "snd/b.mp3" {
		t.Fatalf("v2 default diff wrong: %+v", v2Default)
	}
	if v2Default.Action != "added" {
		t.Fatalf("v2 default action echo wrong: %q", v2Default.Action)
	}

	// v2 with action=all & types=all: size desc → b.mp3(1500000, added) then a.webp(300, updated); walk limit=1.
	var page assetDiffResponse
	rr = getJSON(t, h, "/api/assets/diff?server=jp&version=v2&action=all&types=all&limit=1", &page)
	if rr.Code != http.StatusOK || page.TotalChanged != 2 || len(page.Items) != 1 {
		t.Fatalf("bad v2 page1: %d %+v", rr.Code, page)
	}
	if page.Items[0].Path != "snd/b.mp3" || page.Items[0].ChangeType != "added" || page.NextCursor != "1500000:snd/b.mp3" {
		t.Fatalf("bad v2 page1 item: %+v", page)
	}
	var page2 assetDiffResponse
	rr = getJSON(t, h, "/api/assets/diff?server=jp&version=v2&action=all&types=all&limit=1&cursor=1500000%3Asnd%2Fb.mp3", &page2)
	if rr.Code != http.StatusOK || len(page2.Items) != 1 ||
		page2.Items[0].Path != "img/a.webp" || page2.Items[0].ChangeType != "updated" {
		t.Fatalf("bad v2 page2: %d %+v", rr.Code, page2)
	}
	if page2.NextCursor != "" {
		t.Fatalf("no third page expected: %+v", page2)
	}
}

func TestAssetVersionsHandlerRejectsBadInput(t *testing.T) {
	db := seedVersionsDB(t)
	h := newVersionsHandler(db)

	badRequests := []string{
		"/api/assets/versions",                                // missing server
		"/api/assets/versions?server=jp&limit=0",              // bad limit
		"/api/assets/versions?server=jp&cursor=abc",           // non-numeric cursor
		"/api/assets/versions?server=jp&cursor=-1",            // negative cursor
		"/api/assets/diff?server=jp",                          // missing version
		"/api/assets/diff?server=jp&version=v1&limit=x",       // bad limit
		"/api/assets/diff?server=jp&version=v1&cursor=nope",   // cursor without size prefix
		"/api/assets/diff?server=jp&version=v1&cursor=5:",     // empty cursor path
		"/api/assets/diff?server=jp&version=v1&cursor=5:/abs", // absolute cursor path
		"/api/assets/diff?server=jp&version=v1&types=w!p",     // bad type token
		"/api/assets/diff?server=jp&version=v1&types=a,,b",    // empty type token
		"/api/assets/diff?server=jp&version=v1&action=bogus",  // bad action
	}
	for _, target := range badRequests {
		if rr := getJSON(t, h, target, nil); rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d body=%q", target, rr.Code, rr.Body.String())
		}
	}

	notFound := []string{
		"/api/assets/versions?server=en",          // unlisted server
		"/api/assets/diff?server=en&version=v1",   // unlisted server
		"/api/assets/diff?server=jp&version=nope", // unknown version
	}
	for _, target := range notFound {
		if rr := getJSON(t, h, target, nil); rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status=%d body=%q", target, rr.Code, rr.Body.String())
		}
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/assets/versions?server=jp", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d", rr.Code)
	}
}

func TestBundleDiffEndpoints(t *testing.T) {
	db := seedVersionsDB(t)
	h := newVersionsHandler(db)

	var bResp bundleDiffsResponse
	rr := getJSON(t, h, "/api/assets/bundle-diffs?server=jp&version=v1", &bResp)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	if bResp.Server != "jp" || bResp.AssetVersion != "v1" || bResp.TotalBundles != 2 || len(bResp.Items) != 2 {
		t.Fatalf("bad bundle diff response: %+v", bResp)
	}
	if bResp.Items[0].BundlePath != "img" || bResp.Items[0].FilesURL == "" {
		t.Fatalf("bad item 0: %+v", bResp.Items[0])
	}

	var fResp bundleDiffFilesResponse
	rr = getJSON(t, h, "/api/assets/bundle-diff-files?server=jp&version=v1&path=img", &fResp)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}
	if fResp.Server != "jp" || fResp.AssetVersion != "v1" || fResp.BundlePath != "img" || fResp.TotalChanged != 1 || len(fResp.Items) != 1 {
		t.Fatalf("bad bundle diff files response: %+v", fResp)
	}
	if fResp.Items[0].Path != "img/a.webp" {
		t.Fatalf("unexpected file in bundle: %+v", fResp.Items[0])
	}
}

