package index

import (
	"context"
	"database/sql"
	"testing"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insert(t *testing.T, db *sql.DB, a store.Asset) {
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

func TestRebuildAndLookup(t *testing.T) {
	db := openMem(t)
	insert(t, db, store.Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "111", Sha256: "sha1",
		IsOverride: false, StorageKey: "/shared-assets/img/a.png", Size: 100})
	insert(t, db, store.Asset{Server: "en", Path: "img/a.png", Version: "v1", Fingerprint: "222", Sha256: "sha2",
		IsOverride: true, StorageKey: "/overrides/en/img/a.png", Size: 200})
	// en also references shared baseline for a different path
	insert(t, db, store.Asset{Server: "en", Path: "img/b.png", Version: "v1", Fingerprint: "333", Sha256: "sha3",
		IsOverride: false, StorageKey: "/shared-assets/img/b.png", Size: 300})

	idx := New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	snap := idx.Load()

	// jp/img/a.png → shared
	r := snap.Lookup("jp", "img/a.png")
	if !r.Found || !r.FromShared || r.StorageKey != "/shared-assets/img/a.png" {
		t.Fatalf("jp shared: %+v", r)
	}
	// en/img/a.png → override
	r = snap.Lookup("en", "img/a.png")
	if !r.Found || r.FromShared || r.StorageKey != "/overrides/en/img/a.png" {
		t.Fatalf("en override: %+v", r)
	}
	// en/img/b.png → shared (baseline present, no override for en on b)
	r = snap.Lookup("en", "img/b.png")
	if !r.Found || !r.FromShared || r.StorageKey != "/shared-assets/img/b.png" {
		t.Fatalf("en b shared: %+v", r)
	}
	// tw/img/a.png → shared (no override for tw)
	r = snap.Lookup("tw", "img/a.png")
	if !r.Found || !r.FromShared {
		t.Fatalf("tw fallback shared: %+v", r)
	}
	// unknown path → miss
	if snap.Lookup("jp", "no/such").Found {
		t.Fatal("should miss")
	}
}

func TestRebuildOverridePromotedBack(t *testing.T) {
	db := openMem(t)
	// v1: en had an override.
	insert(t, db, store.Asset{Server: "en", Path: "img/x", Version: "v1", Fingerprint: "1", Sha256: "s1",
		IsOverride: true, StorageKey: "/overrides/en/img/x"})
	// v2: en now matches shared baseline again, so is_override=0.
	insert(t, db, store.Asset{Server: "en", Path: "img/x", Version: "v2", Fingerprint: "2", Sha256: "s2",
		IsOverride: false, StorageKey: "/shared-assets/img/x"})

	idx := New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	snap := idx.Load()
	// en/img/x should NOT be in OverrideIndex (newest row is is_override=0)
	if _, ok := snap.OverrideIndex[Key("en", "img/x")]; ok {
		t.Fatal("stale override entry survived rebuild")
	}
	r := snap.Lookup("en", "img/x")
	if !r.Found || !r.FromShared {
		t.Fatalf("expected shared: %+v", r)
	}
}

func TestBrowseMergesSharedAndOverrideDirectory(t *testing.T) {
	db := openMem(t)
	insert(t, db, store.Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "shared-a", Sha256: "sha-a",
		IsOverride: false, StorageKey: "/shared-assets/img/a.png", Size: 100})
	insert(t, db, store.Asset{Server: "jp", Path: "img/sub/b.png", Version: "v1", Fingerprint: "shared-b", Sha256: "sha-b",
		IsOverride: false, StorageKey: "/shared-assets/img/sub/b.png", Size: 200})
	insert(t, db, store.Asset{Server: "en", Path: "img/a.png", Version: "v2", Fingerprint: "override-a", Sha256: "sha-override",
		IsOverride: true, StorageKey: "/overrides/en/img/a.png", Size: 300})

	idx := New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	en := idx.Load().Browse("en", "img/", "", 10)
	if len(en.Items) != 2 {
		t.Fatalf("en items=%d: %+v", len(en.Items), en.Items)
	}
	if en.Items[0].Kind != BrowseKindAsset || en.Items[0].Path != "img/a.png" || en.Items[0].FromShared || en.Items[0].Fingerprint != "override-a" {
		t.Fatalf("expected en override file first, got %+v", en.Items[0])
	}
	if en.Items[1].Kind != BrowseKindDirectory || en.Items[1].Path != "img/sub/" {
		t.Fatalf("expected sub directory second, got %+v", en.Items[1])
	}

	jp := idx.Load().Browse("jp", "img/", "", 10)
	if len(jp.Items) != 2 || !jp.Items[0].FromShared || jp.Items[0].Fingerprint != "shared-a" {
		t.Fatalf("expected jp shared file, got %+v", jp.Items)
	}
}

func TestBrowsePagination(t *testing.T) {
	db := openMem(t)
	insert(t, db, store.Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "a", Sha256: "sha-a",
		IsOverride: false, StorageKey: "/shared-assets/img/a.png"})
	insert(t, db, store.Asset{Server: "jp", Path: "img/b.png", Version: "v1", Fingerprint: "b", Sha256: "sha-b",
		IsOverride: false, StorageKey: "/shared-assets/img/b.png"})
	insert(t, db, store.Asset{Server: "jp", Path: "img/c.png", Version: "v1", Fingerprint: "c", Sha256: "sha-c",
		IsOverride: false, StorageKey: "/shared-assets/img/c.png"})

	idx := New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	page1 := idx.Load().Browse("jp", "img/", "", 2)
	if len(page1.Items) != 2 || page1.NextCursor != "img/b.png" {
		t.Fatalf("bad page1: %+v", page1)
	}
	page2 := idx.Load().Browse("jp", "img/", page1.NextCursor, 2)
	if len(page2.Items) != 1 || page2.Items[0].Path != "img/c.png" || page2.NextCursor != "" {
		t.Fatalf("bad page2: %+v", page2)
	}
}
