package store

import (
	"context"
	"database/sql"
	"testing"
)

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insert(t *testing.T, db *sql.DB, a Asset) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := InsertAsset(context.Background(), tx, a); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func insertBundleCompletion(t *testing.T, db *sql.DB, c BundleCompletion) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := InsertBundleCompletion(context.Background(), tx, c); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckPathFreshShared(t *testing.T) {
	db := openMem(t)
	d, err := CheckPath(context.Background(), db, "jp", "img/a.png", "111")
	if err != nil {
		t.Fatal(err)
	}
	if d.Skip || d.Placement != "SHARED" {
		t.Fatalf("bad decision: %+v", d)
	}
}

func TestCheckPathSameServerSameFp(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "111", Sha256: "x", StorageKey: "/shared-assets/img/a.png"})
	d, err := CheckPath(context.Background(), db, "jp", "img/a.png", "111")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip || d.SharedReuse {
		t.Fatalf("bad: %+v", d)
	}
}

func TestCheckPathCrossRegionSharedReuse(t *testing.T) {
	db := openMem(t)
	// jp posted the shared baseline
	insert(t, db, Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "111",
		Sha256: "x", IsOverride: false, StorageKey: "/shared-assets/img/a.png"})
	// en probes with same fp → should get SKIP + SharedReuse=true
	d, err := CheckPath(context.Background(), db, "en", "img/a.png", "111")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip || !d.SharedReuse || d.SharedStorageKey != "/shared-assets/img/a.png" {
		t.Fatalf("bad: %+v", d)
	}
}

func TestCheckPathCrossRegionSharedReuseCanUseOlderExactMatch(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "111",
		Sha256: "x", IsOverride: false, StorageKey: "/shared-assets/img/a-v1.png"})
	insert(t, db, Asset{Server: "jp", Path: "img/a.png", Version: "v2", Fingerprint: "222",
		Sha256: "y", IsOverride: false, StorageKey: "/shared-assets/img/a-v2.png"})

	d, err := CheckPath(context.Background(), db, "en", "img/a.png", "111")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip || !d.SharedReuse || d.SharedStorageKey != "/shared-assets/img/a-v1.png" {
		t.Fatalf("bad: %+v", d)
	}
}

func TestCheckPathOverride(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", Path: "img/a.png", Version: "v1", Fingerprint: "111",
		Sha256: "x", IsOverride: false, StorageKey: "/shared-assets/img/a.png"})
	d, err := CheckPath(context.Background(), db, "en", "img/a.png", "222")
	if err != nil {
		t.Fatal(err)
	}
	if d.Skip || d.Placement != "OVERRIDE" {
		t.Fatalf("bad: %+v", d)
	}
}

func TestCheckBundleUsesBundlePath(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{
		Server: "jp", BundlePath: "character/member_small/res026_no048",
		Path: "character/member_small/res026_no048/card_normal.webp", Version: "v1",
		Fingerprint: "111", Sha256: "x", IsOverride: false,
		StorageKey: "/shared-assets/character/member_small/res026_no048/card_normal.webp",
	})

	d, err := CheckBundle(context.Background(), db, "en", "character/member_small/res026_no048", "111")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip {
		t.Fatalf("same shared bundle fingerprint should skip: %+v", d)
	}

	d, err = CheckBundle(context.Background(), db, "en", "character/member_small/res026_no048", "222")
	if err != nil {
		t.Fatal(err)
	}
	if d.Skip || d.Placement != "OVERRIDE" {
		t.Fatalf("changed bundle should upload as override: %+v", d)
	}
}

func TestCheckBundleCrossRegionExactFingerprintReusesBundle(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{
		Server: "jp", BundlePath: "sound/foo",
		Path: "sound/foo/a.mp3", Version: "v1",
		Fingerprint: "bundle-fp", Sha256: "sha-a", IsOverride: false,
		StorageKey: "/shared-assets/sound/foo/a.mp3",
	})

	d, err := CheckBundle(context.Background(), db, "kr", "sound/foo", "bundle-fp")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip || !d.BundleReuse {
		t.Fatalf("same bundle fingerprint should be bundle reuse: %+v", d)
	}
}

func TestCheckBundleUsesBundleCompletion(t *testing.T) {
	db := openMem(t)
	insertBundleCompletion(t, db, BundleCompletion{
		VersionID:    1,
		Server:       "jp",
		AssetVersion: "6.0.0.1",
		AssetHash:    "hash-jp",
		BundlePath:   "zero/file/bundle",
		Fingerprint:  "123",
		Source:       BundleCompletionSourceZeroFile,
	})

	completed, err := BundleCompleted(context.Background(), db, "jp", "zero/file/bundle", "123")
	if err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("expected bundle completion lookup to succeed")
	}

	d, err := CheckBundle(context.Background(), db, "jp", "zero/file/bundle", "123")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip {
		t.Fatalf("bundle completion should skip without asset rows: %+v", d)
	}
}

func TestCheckBundleCrossRegionBundleCompletionReusesBundle(t *testing.T) {
	db := openMem(t)
	insertBundleCompletion(t, db, BundleCompletion{
		VersionID:    1,
		Server:       "jp",
		AssetVersion: "6.0.0",
		AssetHash:    "hash-jp",
		BundlePath:   "zero/file/bundle",
		Fingerprint:  "123",
		Source:       BundleCompletionSourceZeroFile,
	})

	d, err := CheckBundle(context.Background(), db, "kr", "zero/file/bundle", "123")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Skip || !d.BundleReuse {
		t.Fatalf("cross-region bundle completion should be bundle reuse: %+v", d)
	}
}

func TestInsertAssetUpsert(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "jp", Path: "p", Version: "v1", Fingerprint: "1", Sha256: "a", Size: 10, StorageKey: "/k1"})
	insert(t, db, Asset{Server: "jp", Path: "p", Version: "v1", Fingerprint: "1", Sha256: "b", Size: 20, StorageKey: "/k2"})
	a, ok, err := CurrentByServerPath(context.Background(), db, "jp", "p")
	if err != nil || !ok {
		t.Fatalf("lookup: err=%v ok=%v", err, ok)
	}
	if a.Sha256 != "b" || a.Size != 20 || a.StorageKey != "/k2" {
		t.Fatalf("upsert didn't apply: %+v", a)
	}
	res, err := LookupPlacement(context.Background(), db, "jp", "p")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || res.StorageKey != "/k2" {
		t.Fatalf("current read index not updated: %+v", res)
	}
}

func TestEnsureReadIndexesBackfillsLegacyAssets(t *testing.T) {
	db := openMem(t)
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO assets(server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, created_at)
		VALUES
			('jp', 'b', 'img/a.png', 'v1', 'old', 'sha-old', 10, 0, '/shared-assets/img/a-old.png', 1),
			('jp', 'b', 'img/a.png', 'v2', 'new', 'sha-new', 20, 0, '/shared-assets/img/a-new.png', 2),
			('en', 'b', 'img/a.png', 'v2', 'override', 'sha-override', 30, 1, '/overrides/en/img/a.png', 3)
	`)
	if err != nil {
		t.Fatal(err)
	}

	if err := EnsureReadIndexes(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	jp, err := LookupPlacement(context.Background(), db, "jp", "img/a.png")
	if err != nil {
		t.Fatal(err)
	}
	if !jp.Found || !jp.FromShared || jp.StorageKey != "/shared-assets/img/a-new.png" {
		t.Fatalf("bad jp placement: %+v", jp)
	}
	en, err := LookupPlacement(context.Background(), db, "en", "img/a.png")
	if err != nil {
		t.Fatal(err)
	}
	if !en.Found || en.FromShared || en.StorageKey != "/overrides/en/img/a.png" {
		t.Fatalf("bad en placement: %+v", en)
	}
}

func TestReusableAssetBySHAPrefersShared(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "cn", Path: "p", Version: "v1", Fingerprint: "1", Sha256: "same", Size: 10, IsOverride: true, StorageKey: "/overrides/cn/p"})
	insert(t, db, Asset{Server: "jp", Path: "p", Version: "v1", Fingerprint: "1", Sha256: "same", Size: 10, IsOverride: false, StorageKey: "/shared-assets/p"})

	a, ok, err := ReusableAssetBySHA(context.Background(), db, "same")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || a.StorageKey != "/shared-assets/p" {
		t.Fatalf("wanted shared reusable key, got ok=%v asset=%+v", ok, a)
	}
}

func TestReusableBundleAssetsPrefersSharedAndOneRowPerPath(t *testing.T) {
	db := openMem(t)
	insert(t, db, Asset{Server: "cn", BundlePath: "b", Path: "b/a", Version: "v1", Fingerprint: "fp", Sha256: "sha", Size: 10, IsOverride: true, StorageKey: "/overrides/cn/b/a"})
	insert(t, db, Asset{Server: "jp", BundlePath: "b", Path: "b/a", Version: "v1", Fingerprint: "fp", Sha256: "sha", Size: 10, IsOverride: false, StorageKey: "/shared-assets/b/a"})
	insert(t, db, Asset{Server: "jp", BundlePath: "b", Path: "b/b", Version: "v1", Fingerprint: "fp", Sha256: "sha-b", Size: 20, IsOverride: false, StorageKey: "/shared-assets/b/b"})

	assets, err := ReusableBundleAssets(context.Background(), db, "b", "fp")
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 2 {
		t.Fatalf("wanted 2 unique paths, got %d: %+v", len(assets), assets)
	}
	if assets[0].Path != "b/a" || assets[0].StorageKey != "/shared-assets/b/a" {
		t.Fatalf("wanted shared row for b/a first, got %+v", assets[0])
	}
}
