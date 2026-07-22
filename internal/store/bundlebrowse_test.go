package store

import (
	"context"
	"testing"
)

func TestBrowseBundlesListsDirectoriesAndBundles(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/logo.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s2", Size: 50, StorageKey: "/shared-assets/event/foo/logo.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "event/bar", Path: "event/bar/bgm.mp3", Version: "v1",
		Fingerprint: "b-bar", Sha256: "s3", Size: 200, StorageKey: "/shared-assets/event/bar/bgm.mp3"})
	insert(t, db, Asset{Server: "jp", BundlePath: "sound/bgm", Path: "sound/bgm/main.mp3", Version: "v1",
		Fingerprint: "b-bgm", Sha256: "s4", Size: 300, StorageKey: "/shared-assets/sound/bgm/main.mp3"})

	// Root: two directories, no bundles.
	res, err := BrowseBundles(context.Background(), db, "jp", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("root items=%d: %+v", len(res.Items), res.Items)
	}
	if res.Items[0].Kind != BrowseKindDirectory || res.Items[0].Path != "event/" ||
		res.Items[1].Kind != BrowseKindDirectory || res.Items[1].Path != "sound/" {
		t.Fatalf("bad root listing: %+v", res.Items)
	}

	// event/: two bundles with aggregate metadata.
	res, err = BrowseBundles(context.Background(), db, "jp", "event/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 {
		t.Fatalf("event items=%d: %+v", len(res.Items), res.Items)
	}
	bar, foo := res.Items[0], res.Items[1]
	if bar.Kind != BrowseKindBundle || bar.Path != "event/bar" || bar.Name != "bar" ||
		bar.FileCount != 1 || bar.TotalSize != 200 || bar.Fingerprint != "b-bar" || !bar.FromShared {
		t.Fatalf("bad bar bundle: %+v", bar)
	}
	if foo.Kind != BrowseKindBundle || foo.Path != "event/foo" ||
		foo.FileCount != 2 || foo.TotalSize != 150 || foo.Fingerprint != "b-foo" {
		t.Fatalf("bad foo bundle: %+v", foo)
	}

	// Pagination: limit=1 pages through with the cursor.
	res, err = BrowseBundles(context.Background(), db, "jp", "event/", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Path != "event/bar" || res.NextCursor != "event/bar" {
		t.Fatalf("bad first page: %+v", res)
	}
	res, err = BrowseBundles(context.Background(), db, "jp", "event/", res.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Path != "event/foo" || res.NextCursor != "" {
		t.Fatalf("bad second page: %+v", res)
	}
}

func TestBrowseBundlesOverrideWins(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insert(t, db, Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v2",
		Fingerprint: "b-foo-en", Sha256: "s1e", Size: 120, IsOverride: true, StorageKey: "/overrides/en/event/foo/bg.webp"})

	// en sees the override metadata; jp keeps the shared metadata.
	res, err := BrowseBundles(context.Background(), db, "en", "event/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Fingerprint != "b-foo-en" || res.Items[0].FromShared || res.Items[0].TotalSize != 120 {
		t.Fatalf("bad en bundle: %+v", res.Items)
	}
	res, err = BrowseBundles(context.Background(), db, "jp", "event/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Fingerprint != "b-foo" || !res.Items[0].FromShared {
		t.Fatalf("bad jp bundle: %+v", res.Items)
	}
}

func TestBundleFilesMergesOverride(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/logo.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s2", Size: 50, StorageKey: "/shared-assets/event/foo/logo.webp"})
	insert(t, db, Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v2",
		Fingerprint: "b-foo-en", Sha256: "s1e", Size: 120, IsOverride: true, StorageKey: "/overrides/en/event/foo/bg.webp"})

	result, info, found, err := BundleFiles(context.Background(), db, "en", "event/foo", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found || info.Fingerprint != "b-foo-en" || info.FromShared {
		t.Fatalf("bad info: found=%v %+v", found, info)
	}
	if len(result.Items) != 2 {
		t.Fatalf("items=%d: %+v", len(result.Items), result.Items)
	}
	bg, logo := result.Items[0], result.Items[1]
	if bg.Path != "event/foo/bg.webp" || bg.Name != "bg.webp" || bg.FromShared || bg.Size != 120 || bg.Fingerprint != "b-foo-en" {
		t.Fatalf("override should win for bg: %+v", bg)
	}
	if logo.Path != "event/foo/logo.webp" || !logo.FromShared || logo.Size != 50 {
		t.Fatalf("bad logo: %+v", logo)
	}

	// jp sees pure shared content.
	_, info, found, err = BundleFiles(context.Background(), db, "jp", "event/foo", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found || info.Fingerprint != "b-foo" || !info.FromShared || info.FileCount != 2 || info.TotalSize != 150 {
		t.Fatalf("bad jp info: %+v", info)
	}

	// Unknown bundle → not found.
	_, _, found, err = BundleFiles(context.Background(), db, "jp", "event/nope", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestBundleFilesPagination(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "b", Path: "b/1.webp", Version: "v1",
		Fingerprint: "f", Sha256: "s", Size: 1, StorageKey: "/shared-assets/b/1.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "b", Path: "b/2.webp", Version: "v1",
		Fingerprint: "f", Sha256: "s", Size: 1, StorageKey: "/shared-assets/b/2.webp"})

	result, _, found, err := BundleFiles(context.Background(), db, "jp", "b", "", 1)
	if err != nil || !found {
		t.Fatalf("err=%v found=%v", err, found)
	}
	if len(result.Items) != 1 || result.Items[0].Path != "b/1.webp" || result.NextCursor != "b/1.webp" {
		t.Fatalf("bad first page: %+v", result)
	}
	result, _, _, err = BundleFiles(context.Background(), db, "jp", "b", result.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].Path != "b/2.webp" || result.NextCursor != "" {
		t.Fatalf("bad second page: %+v", result)
	}
}

func TestBundleAggregatesFollowOverrideFlip(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b1", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insert(t, db, Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v2",
		Fingerprint: "b2", Sha256: "s2", Size: 120, IsOverride: true, StorageKey: "/overrides/en/event/foo/bg.webp"})

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM current_override_bundles WHERE server='en'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 override bundle, got %d", n)
	}

	// en adopts the shared fingerprint again → its row flips back to shared
	// placement and the override bundle aggregate must disappear.
	insert(t, db, Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v3",
		Fingerprint: "b1", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	if err := db.QueryRow(`SELECT COUNT(*) FROM current_override_bundles WHERE server='en'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("override bundle should be gone, got %d", n)
	}
}

func TestEnsureReadIndexesRebuildsBundleTables(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v1",
		Fingerprint: "b-foo", Sha256: "s1", Size: 100, StorageKey: "/shared-assets/event/foo/bg.webp"})
	insert(t, db, Asset{Server: "en", BundlePath: "event/foo", Path: "event/foo/bg.webp", Version: "v2",
		Fingerprint: "b-foo-en", Sha256: "s1e", Size: 120, IsOverride: true, StorageKey: "/overrides/en/event/foo/bg.webp"})

	// Simulate a database from before the bundle browser: wipe the
	// materialized tables and the meta marker, then rebuild from assets.
	for _, stmt := range []string{
		`DELETE FROM current_assets`,
		`DELETE FROM current_shared_assets`,
		`DELETE FROM current_shared_bundles`,
		`DELETE FROM current_override_bundles`,
		`DELETE FROM read_index_meta`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := EnsureReadIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	res, err := BrowseBundles(context.Background(), db, "en", "event/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Path != "event/foo" || res.Items[0].Fingerprint != "b-foo-en" || res.Items[0].FromShared {
		t.Fatalf("bad rebuilt en listing: %+v", res.Items)
	}
	res, err = BrowseBundles(context.Background(), db, "jp", "event/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.Items[0].Fingerprint != "b-foo" || !res.Items[0].FromShared {
		t.Fatalf("bad rebuilt jp listing: %+v", res.Items)
	}
}
