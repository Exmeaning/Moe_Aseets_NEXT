// Package store owns the SQLite data plane. All writes go through a single
// *sql.DB configured for single-writer usage (WAL mode + serialised
// transactions). Reads may happen concurrently.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// memDBSeq gives each Open(":memory:") its own named shared-cache memory DB.
var memDBSeq atomic.Uint64

// Open opens (and migrates) a SQLite DB at path. The DSN sets busy_timeout
// and WAL mode. Set path to ":memory:" for tests. mattn/go-sqlite3 requires
// CGO_ENABLED=1 (the pure-Go modernc driver it replaced was several times
// slower on scan-heavy queries).
func Open(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=on", path)
	if path == ":memory:" {
		// database/sql pools several connections, so the memory DB must use a
		// shared cache or every connection would see its own empty database —
		// but a *named* one, otherwise all Open(":memory:") calls in the
		// process (e.g. every test) would share a single database. WAL does
		// not apply in memory, so it is omitted.
		dsn = fmt.Sprintf("file:memdb%d?mode=memory&cache=shared&_busy_timeout=5000&_foreign_keys=on", memDBSeq.Add(1))
	}
	db, err := sql.Open("sqlite3", dsn)
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
