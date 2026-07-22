// Package store owns the SQLite data plane. All writes go through a single
// *sql.DB configured for single-writer usage (WAL mode + serialised
// transactions). Reads may happen concurrently.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Open opens (and migrates) a SQLite DB at path. The DSN sets busy_timeout
// and WAL mode. Set path to ":memory:" for tests.
func Open(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(on)&_time_format=sqlite", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite writes are inherently serialised. Give readers a small pool.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Index creation on a populated multi-GB database (e.g. the partial
	// override-path index) can take well over the old 10s on first boot.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, Schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	// Pre-existing databases created before the bundle browser lack the
	// bundle_path column on the materialized current tables. Backfill happens
	// via EnsureReadIndexes (its meta key changed, forcing one full rebuild).
	for _, m := range []struct{ table, column string }{
		{"current_assets", "bundle_path"},
		{"current_shared_assets", "bundle_path"},
	} {
		if err := ensureColumn(ctx, db, m.table, m.column,
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s TEXT NOT NULL DEFAULT ''", m.table, m.column)); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("migrate %s.%s: %w", m.table, m.column, err)
		}
	}
	if _, err := db.ExecContext(ctx, SchemaPostMigrate); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate indexes: %w", err)
	}
	return db, nil
}

// ensureColumn adds a column via ddl when table does not have it yet.
func ensureColumn(ctx context.Context, db *sql.DB, table, column, ddl string) error {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, column).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err = db.ExecContext(ctx, ddl)
	return err
}
