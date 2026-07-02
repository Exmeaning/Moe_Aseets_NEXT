package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Asset mirrors one row of the assets table.
type Asset struct {
	ID          int64
	Server      string
	BundlePath  string
	Path        string
	Version     string
	Fingerprint string
	Sha256      string
	Size        int64
	IsOverride  bool
	StorageKey  string
	CreatedAt   int64
}

// CheckDecision is the server-authoritative outcome for a single CHECK item.
type CheckDecision struct {
	// SKIP action classification. If Skip==true then Placement is empty.
	Skip bool
	// Placement is "SHARED" or "OVERRIDE" when Skip==false.
	Placement string
	// SharedStorageKey is set when Skip==true because a shared baseline with
	// the same fingerprint already exists (allows COMMIT to record a reuse
	// row for this server without re-uploading). Empty for other SKIP cases.
	SharedStorageKey string
	// SharedFingerprintMatch indicates cross-region shared reuse (client did
	// not upload, but this server still gains a versioned row on COMMIT).
	SharedReuse bool
}

// CheckPath applies §7.3 logic for one canonical public asset path
// (server, bundle_path/asset_path, fingerprint) triple.
func CheckPath(ctx context.Context, db *sql.DB, server, path, fingerprint string) (CheckDecision, error) {
	// 1) same server already has same (path, fingerprint) → SKIP.
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM assets WHERE server=? AND path=? AND fingerprint=? LIMIT 1`,
		server, path, fingerprint).Scan(&one)
	if err == nil {
		return CheckDecision{Skip: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return CheckDecision{}, err
	}

	// 2) look up newest shared baseline for this canonical asset path.
	var sharedFp, sharedKey string
	err = db.QueryRowContext(ctx, `SELECT fingerprint, storage_key FROM assets
		WHERE path=? AND is_override=0 ORDER BY id DESC LIMIT 1`, path).Scan(&sharedFp, &sharedKey)
	if errors.Is(err, sql.ErrNoRows) {
		return CheckDecision{Placement: "SHARED"}, nil
	}
	if err != nil {
		return CheckDecision{}, err
	}
	if sharedFp == fingerprint {
		// Cross-region shared reuse: SKIP, but COMMIT should mint a new row
		// for (server, path, version) pointing at the existing shared key.
		return CheckDecision{Skip: true, SharedReuse: true, SharedStorageKey: sharedKey}, nil
	}
	return CheckDecision{Placement: "OVERRIDE"}, nil
}

// CheckBundle applies the pre-download HIP CHECK semantics. The updater sends
// bundle_path here (before it has exported per-file asset paths), so this must
// inspect bundle_path + fingerprint, not the canonical per-file public path.
func CheckBundle(ctx context.Context, db *sql.DB, server, bundlePath, fingerprint string) (CheckDecision, error) {
	// Same server already committed this bundle fingerprint: no need to download
	// and export it again.
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM assets WHERE server=? AND bundle_path=? AND fingerprint=? LIMIT 1`,
		server, bundlePath, fingerprint).Scan(&one)
	if err == nil {
		return CheckDecision{Skip: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return CheckDecision{}, err
	}

	// If a shared baseline for this exact bundle fingerprint exists, any region
	// can read it through the shared index without region-specific rows.
	var sharedFp string
	err = db.QueryRowContext(ctx, `SELECT fingerprint FROM assets
		WHERE bundle_path=? AND is_override=0 ORDER BY id DESC LIMIT 1`, bundlePath).Scan(&sharedFp)
	if errors.Is(err, sql.ErrNoRows) {
		return CheckDecision{Placement: "SHARED"}, nil
	}
	if err != nil {
		return CheckDecision{}, err
	}
	if sharedFp == fingerprint {
		return CheckDecision{Skip: true}, nil
	}
	return CheckDecision{Placement: "OVERRIDE"}, nil
}

// InsertAsset upserts one row (uniqueness is (server, path, version)). Used
// inside COMMIT transactions.
func InsertAsset(ctx context.Context, tx *sql.Tx, a Asset) error {
	if a.CreatedAt == 0 {
		a.CreatedAt = time.Now().Unix()
	}
	iso := 0
	if a.IsOverride {
		iso = 1
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO assets(server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server, path, version) DO UPDATE SET
			bundle_path=excluded.bundle_path,
			fingerprint=excluded.fingerprint,
			sha256=excluded.sha256,
			size=excluded.size,
			is_override=excluded.is_override,
			storage_key=excluded.storage_key
	`, a.Server, a.BundlePath, a.Path, a.Version, a.Fingerprint, a.Sha256, a.Size, iso, a.StorageKey, a.CreatedAt)
	return err
}

// CurrentByServerPath returns the newest asset row for (server, path), or
// (nil,false) if none. Used by the reverse-proxy read path.
func CurrentByServerPath(ctx context.Context, db *sql.DB, server, path string) (*Asset, bool, error) {
	row := db.QueryRowContext(ctx, `SELECT id, server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, created_at
		FROM assets WHERE server=? AND path=? ORDER BY id DESC LIMIT 1`, server, path)
	var a Asset
	var iso int
	err := row.Scan(&a.ID, &a.Server, &a.BundlePath, &a.Path, &a.Version, &a.Fingerprint, &a.Sha256, &a.Size, &iso, &a.StorageKey, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	a.IsOverride = iso == 1
	return &a, true, nil
}
