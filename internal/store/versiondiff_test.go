package store

import (
	"context"
	"database/sql"
	"testing"
)

func insertVersionRow(t *testing.T, db *sql.DB, v Version) int64 {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := InsertVersion(context.Background(), tx, v)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return id
}

// seedDiffFixture commits two jp versions:
//
//	v1: a.webp(100) d.webp(200) b.mp3(300) c.acb(999)   — all "added"
//	v2: b.mp3(500, updated) e.json(500, added) g.webp(10, added)
func seedDiffFixture(t *testing.T, db *sql.DB) (v1ID, v2ID int64) {
	t.Helper()
	insert(t, db, Asset{Server: "jp", BundlePath: "img", Path: "img/a.webp", Version: "v1", Fingerprint: "fa", Sha256: "sa", Size: 100, StorageKey: "/shared-assets/img/a.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "img", Path: "img/d.webp", Version: "v1", Fingerprint: "fd", Sha256: "sd", Size: 200, StorageKey: "/shared-assets/img/d.webp"})
	insert(t, db, Asset{Server: "jp", BundlePath: "snd", Path: "snd/b.mp3", Version: "v1", Fingerprint: "fb", Sha256: "sb", Size: 300, StorageKey: "/shared-assets/snd/b.mp3"})
	insert(t, db, Asset{Server: "jp", BundlePath: "snd", Path: "snd/c.acb", Version: "v1", Fingerprint: "fc", Sha256: "sc", Size: 999, StorageKey: "/shared-assets/snd/c.acb"})
	v1ID = insertVersionRow(t, db, Version{Server: "jp", AppVersion: "1.0", AssetVersion: "v1", AssetHash: "h1", BundleCount: 2, StatsJSON: `{"uploaded_shared":4}`})

	insert(t, db, Asset{Server: "jp", BundlePath: "snd", Path: "snd/b.mp3", Version: "v2", Fingerprint: "fb2", Sha256: "sb2", Size: 500, StorageKey: "/shared-assets/snd/b.mp3"})
	insert(t, db, Asset{Server: "jp", BundlePath: "ui", Path: "ui/e.json", Version: "v2", Fingerprint: "fe", Sha256: "se", Size: 500, StorageKey: "/shared-assets/ui/e.json"})
	insert(t, db, Asset{Server: "jp", BundlePath: "img", Path: "img/g.webp", Version: "v2", Fingerprint: "fg", Sha256: "sg", Size: 10, StorageKey: "/shared-assets/img/g.webp"})
	v2ID = insertVersionRow(t, db, Version{Server: "jp", AppVersion: "1.1", AssetVersion: "v2", AssetHash: "h2", BundleCount: 3, StatsJSON: `{"uploaded_shared":3}`})
	return v1ID, v2ID
}

func TestListVersionsNewestFirstWithCounts(t *testing.T) {
	db := openMem(t)
	_, _ = seedDiffFixture(t, db)

	got, next, err := ListVersions(context.Background(), db, "jp", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if next != 0 {
		t.Fatalf("nextBeforeID=%d, want 0", next)
	}
	if len(got) != 2 || got[0].AssetVersion != "v2" || got[1].AssetVersion != "v1" {
		t.Fatalf("bad order: %+v", got)
	}
	if got[0].ChangedAssets != 3 || got[1].ChangedAssets != 4 {
		t.Fatalf("bad counts: v2=%d v1=%d", got[0].ChangedAssets, got[1].ChangedAssets)
	}
	if got[0].AppVersion != "1.1" || got[0].BundleCount != 3 || got[0].StatsJSON != `{"uploaded_shared":3}` {
		t.Fatalf("bad metadata: %+v", got[0])
	}

	// Other servers see nothing.
	other, _, err := ListVersions(context.Background(), db, "en", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("en should have no versions: %+v", other)
	}
}

func TestListVersionsPagination(t *testing.T) {
	db := openMem(t)
	seedDiffFixture(t, db)

	page1, next, err := ListVersions(context.Background(), db, "jp", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 1 || page1[0].AssetVersion != "v2" || next == 0 {
		t.Fatalf("bad first page: %+v next=%d", page1, next)
	}
	page2, next2, err := ListVersions(context.Background(), db, "jp", next, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || page2[0].AssetVersion != "v1" || next2 != 0 {
		t.Fatalf("bad second page: %+v next=%d", page2, next2)
	}
}

func TestDiffVersionSizeOrderAndChangeType(t *testing.T) {
	db := openMem(t)
	seedDiffFixture(t, db)

	diff, found, err := DiffVersion(context.Background(), db, "jp", "v2", nil, DiffCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("v2 should be found")
	}
	if diff.Version.AppVersion != "1.1" || diff.Version.AssetHash != "h2" {
		t.Fatalf("bad version meta: %+v", diff.Version)
	}
	if diff.TotalChanged != 3 {
		t.Fatalf("TotalChanged=%d, want 3", diff.TotalChanged)
	}
	// size DESC, path ASC on ties: b.mp3(500) < e.json(500) by path, then g.webp(10).
	if len(diff.Items) != 3 ||
		diff.Items[0].Path != "snd/b.mp3" || diff.Items[1].Path != "ui/e.json" || diff.Items[2].Path != "img/g.webp" {
		t.Fatalf("bad order: %+v", diff.Items)
	}
	if !diff.Items[0].Existed {
		t.Fatalf("b.mp3 should be an update: %+v", diff.Items[0])
	}
	if diff.Items[1].Existed || diff.Items[2].Existed {
		t.Fatalf("e.json / g.webp should be additions: %+v", diff.Items[1:])
	}
	if diff.NextCursor.Set {
		t.Fatalf("no more pages expected: %+v", diff.NextCursor)
	}
}

func TestDiffVersionKeysetPaginationAcrossEqualSizes(t *testing.T) {
	db := openMem(t)
	seedDiffFixture(t, db)

	var got []string
	cursor := DiffCursor{}
	for i := 0; i < 4; i++ {
		diff, found, err := DiffVersion(context.Background(), db, "jp", "v2", nil, cursor, 1)
		if err != nil || !found {
			t.Fatalf("page %d: found=%v err=%v", i, found, err)
		}
		for _, e := range diff.Items {
			got = append(got, e.Path)
		}
		if !diff.NextCursor.Set {
			break
		}
		cursor = diff.NextCursor
	}
	want := []string{"snd/b.mp3", "ui/e.json", "img/g.webp"}
	if len(got) != len(want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v want=%v", got, want)
		}
	}
}

func TestDiffVersionExtensionFilter(t *testing.T) {
	db := openMem(t)
	seedDiffFixture(t, db)

	diff, found, err := DiffVersion(context.Background(), db, "jp", "v1", []string{"webp", "mp3", "json"}, DiffCursor{}, 10)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	// c.acb is excluded both from the page and from TotalChanged.
	if diff.TotalChanged != 3 || len(diff.Items) != 3 {
		t.Fatalf("TotalChanged=%d items=%d, want 3/3", diff.TotalChanged, len(diff.Items))
	}
	for _, e := range diff.Items {
		if e.Path == "snd/c.acb" {
			t.Fatalf("c.acb should be filtered out: %+v", diff.Items)
		}
	}

	only, _, err := DiffVersion(context.Background(), db, "jp", "v1", []string{"webp"}, DiffCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if only.TotalChanged != 2 || len(only.Items) != 2 ||
		only.Items[0].Path != "img/d.webp" || only.Items[1].Path != "img/a.webp" {
		t.Fatalf("webp-only diff wrong: total=%d %+v", only.TotalChanged, only.Items)
	}
}

func TestDiffVersionUnknownVersion(t *testing.T) {
	db := openMem(t)
	seedDiffFixture(t, db)

	_, found, err := DiffVersion(context.Background(), db, "jp", "nope", nil, DiffCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unknown version must report found=false")
	}
	_, found, err = DiffVersion(context.Background(), db, "en", "v1", nil, DiffCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("version committed by jp must not be visible for en")
	}
}
