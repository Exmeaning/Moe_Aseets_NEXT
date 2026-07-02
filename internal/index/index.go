// Package index maintains an in-memory snapshot of "current" placement info
// derived from the assets table. It exists to keep the read path lock-free
// and DB-free on the hot path.
//
// The snapshot is fully rebuilt on startup and after every successful COMMIT
// transaction. Readers grab a pointer via Load(); writers publish a new
// snapshot atomically via Rebuild().
package index

import (
	"context"
	"database/sql"
	"sync/atomic"
)

// SharedRow is the subset of an assets row that the read path needs when
// serving from the shared baseline.
type SharedRow struct {
	Fingerprint string
	StorageKey  string
	Sha256      string
	Version     string
	Size        int64
}

// OverrideRow is what the read path needs when serving from a per-server override.
type OverrideRow struct {
	Fingerprint string
	StorageKey  string
	Sha256      string
	Version     string
	Size        int64
}

// Snapshot is an immutable placement view.
type Snapshot struct {
	// OverrideIndex: (server, path) → OverrideRow, only entries whose CURRENT
	// row (newest by id) has is_override=1.
	OverrideIndex map[string]OverrideRow
	// CurrentShared: path → newest shared-baseline row.
	CurrentShared map[string]SharedRow
}

// Key builds the (server, path) composite key used in OverrideIndex.
func Key(server, path string) string {
	return server + "\x00" + path
}

// Index is the atomic holder.
type Index struct {
	ptr atomic.Pointer[Snapshot]
}

// New creates an empty index (Snapshot with empty maps).
func New() *Index {
	i := &Index{}
	empty := &Snapshot{
		OverrideIndex: map[string]OverrideRow{},
		CurrentShared: map[string]SharedRow{},
	}
	i.ptr.Store(empty)
	return i
}

// Load returns the current snapshot. Zero-cost, lock-free.
func (i *Index) Load() *Snapshot { return i.ptr.Load() }

// Rebuild scans the assets table and publishes a fresh snapshot atomically.
// Must be called after every successful COMMIT transaction. Reads from the
// DB — should not run concurrently with a write tx, but safe against other
// reads.
func (i *Index) Rebuild(ctx context.Context, db *sql.DB) error {
	snap := &Snapshot{
		OverrideIndex: map[string]OverrideRow{},
		CurrentShared: map[string]SharedRow{},
	}

	// Newest override row per (server, path): those whose id is max within
	// (server, path) AND that row has is_override=1.
	overrideRows, err := db.QueryContext(ctx, `
		SELECT a.server, a.path, a.fingerprint, a.storage_key, a.sha256, a.version, a.size
		FROM assets a
		JOIN (
			SELECT server, path, MAX(id) AS mid FROM assets GROUP BY server, path
		) m ON a.id = m.mid
		WHERE a.is_override = 1
	`)
	if err != nil {
		return err
	}
	func() {
		defer overrideRows.Close()
		for overrideRows.Next() {
			var srv, p string
			var row OverrideRow
			if err = overrideRows.Scan(&srv, &p, &row.Fingerprint, &row.StorageKey, &row.Sha256, &row.Version, &row.Size); err != nil {
				return
			}
			snap.OverrideIndex[Key(srv, p)] = row
		}
	}()
	if err != nil {
		return err
	}

	// Newest shared baseline per path.
	sharedRows, err := db.QueryContext(ctx, `
		SELECT a.path, a.fingerprint, a.storage_key, a.sha256, a.version, a.size
		FROM assets a
		JOIN (
			SELECT path, MAX(id) AS mid FROM assets WHERE is_override=0 GROUP BY path
		) m ON a.id = m.mid
	`)
	if err != nil {
		return err
	}
	func() {
		defer sharedRows.Close()
		for sharedRows.Next() {
			var p string
			var row SharedRow
			if err = sharedRows.Scan(&p, &row.Fingerprint, &row.StorageKey, &row.Sha256, &row.Version, &row.Size); err != nil {
				return
			}
			snap.CurrentShared[p] = row
		}
	}()
	if err != nil {
		return err
	}

	i.ptr.Store(snap)
	return nil
}

// LookupResult is what the read path gets back from Lookup.
type LookupResult struct {
	Found       bool
	FromShared  bool // true → served from /shared-assets/{path}
	Fingerprint string
	StorageKey  string
	Sha256      string
	Version     string
	Size        int64
}

// Lookup applies the placement policy: override first, shared second.
func (s *Snapshot) Lookup(server, path string) LookupResult {
	if s == nil {
		return LookupResult{}
	}
	if row, ok := s.OverrideIndex[Key(server, path)]; ok {
		return LookupResult{
			Found:       true,
			FromShared:  false,
			Fingerprint: row.Fingerprint,
			StorageKey:  row.StorageKey,
			Sha256:      row.Sha256,
			Version:     row.Version,
			Size:        row.Size,
		}
	}
	if row, ok := s.CurrentShared[path]; ok {
		return LookupResult{
			Found:       true,
			FromShared:  true,
			Fingerprint: row.Fingerprint,
			StorageKey:  row.StorageKey,
			Sha256:      row.Sha256,
			Version:     row.Version,
			Size:        row.Size,
		}
	}
	return LookupResult{}
}
