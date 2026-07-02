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
}
