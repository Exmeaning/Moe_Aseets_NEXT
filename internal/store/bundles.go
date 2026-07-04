package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	BundleCompletionSourceUploaded  = "uploaded"
	BundleCompletionSourceZeroFile  = "zero_file"
	BundleCompletionSourceCheckSkip = "check_skip"
)

// BundleCompletion mirrors one row of the bundle_completions table.
type BundleCompletion struct {
	ID           int64
	VersionID    int64
	Server       string
	AssetVersion string
	AssetHash    string
	BundlePath   string
	Fingerprint  string
	Source       string
	CompletedAt  int64
}

// InsertBundleCompletion upserts one completed bundle for a committed version.
func InsertBundleCompletion(ctx context.Context, tx *sql.Tx, c BundleCompletion) error {
	if c.CompletedAt == 0 {
		c.CompletedAt = time.Now().Unix()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO bundle_completions(version_id, server, asset_version, asset_hash, bundle_path, fingerprint, source, completed_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server, asset_version, asset_hash, bundle_path) DO UPDATE SET
			version_id=excluded.version_id,
			fingerprint=excluded.fingerprint,
			source=excluded.source,
			completed_at=excluded.completed_at
	`, c.VersionID, c.Server, c.AssetVersion, c.AssetHash, c.BundlePath, c.Fingerprint, c.Source, c.CompletedAt)
	return err
}

// BundleCompleted returns true if this server already committed the bundle fingerprint.
func BundleCompleted(ctx context.Context, db *sql.DB, server, bundlePath, fingerprint string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM bundle_completions WHERE server=? AND bundle_path=? AND fingerprint=? LIMIT 1`,
		server, bundlePath, fingerprint).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
