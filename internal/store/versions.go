package store

import (
	"context"
	"database/sql"
	"time"
)

// Version mirrors one row of the versions table.
type Version struct {
	ID           int64
	Server       string
	AppVersion   string
	AssetVersion string
	AssetHash    string
	BundleCount  int64
	CommittedAt  int64
	StatsJSON    string
}

// InsertVersion writes a versions row inside a tx and returns the assigned id.
func InsertVersion(ctx context.Context, tx *sql.Tx, v Version) (int64, error) {
	if v.CommittedAt == 0 {
		v.CommittedAt = time.Now().Unix()
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO versions(server, app_version, asset_version, asset_hash, bundle_count, committed_at, stats_json)
		VALUES(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server, asset_version, asset_hash) DO UPDATE SET
			app_version=excluded.app_version,
			bundle_count=excluded.bundle_count,
			committed_at=excluded.committed_at,
			stats_json=excluded.stats_json
	`, v.Server, v.AppVersion, v.AssetVersion, v.AssetHash, v.BundleCount, v.CommittedAt, v.StatsJSON)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if id == 0 {
		// UPSERT case: re-select
		err = tx.QueryRowContext(ctx, `SELECT id FROM versions WHERE server=? AND asset_version=? AND asset_hash=?`,
			v.Server, v.AssetVersion, v.AssetHash).Scan(&id)
		if err != nil {
			return 0, err
		}
	}
	return id, nil
}

// KnownVersion returns true if (server, asset_version, asset_hash) already has
// a committed row.
func KnownVersion(ctx context.Context, db *sql.DB, server, assetVersion, assetHash string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM versions WHERE server=? AND asset_version=? AND asset_hash=? LIMIT 1`,
		server, assetVersion, assetHash).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}
