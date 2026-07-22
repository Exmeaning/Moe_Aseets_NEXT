package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

const (
	BrowseKindDirectory = "directory"
	BrowseKindAsset     = "asset"
	BrowseKindBundle    = "bundle"
	// The "_v2" suffix forces one full EnsureReadIndexes rebuild on databases
	// created before the bundle browser (backfills bundle_path + bundle tables).
	readIndexMaxAssetID = "current_assets_max_id_v2"
)

// Placement is the current read-path resolution for one public asset path.
type Placement struct {
	Found       bool
	FromShared  bool
	Fingerprint string
	StorageKey  string
	Sha256      string
	Version     string
	Size        int64
}

// BrowseEntry is one immediate child in an asset browser listing.
type BrowseEntry struct {
	Kind        string
	Name        string
	Path        string
	Fingerprint string
	Sha256      string
	Version     string
	Size        int64
	FromShared  bool
}

// BrowseResult is a single-page directory listing.
type BrowseResult struct {
	Items      []BrowseEntry
	NextCursor string
	Revision   uint64
}

// LookupPlacement applies the read-path placement policy without loading a
// full in-memory index: server override first, newest shared baseline second.
func LookupPlacement(ctx context.Context, db *sql.DB, server, path string) (Placement, error) {
	current, ok, err := CurrentIndexedByServerPath(ctx, db, server, path)
	if err != nil {
		return Placement{}, err
	}
	if ok && current.IsOverride {
		return placementFromAsset(current, false), nil
	}

	shared, ok, err := CurrentIndexedSharedByPath(ctx, db, path)
	if err != nil {
		return Placement{}, err
	}
	if ok {
		return placementFromAsset(shared, true), nil
	}

	// Migration fallback: if a DB has not been backfilled yet, preserve the
	// old assets-table semantics instead of returning a false 404.
	current, ok, err = CurrentByServerPath(ctx, db, server, path)
	if err != nil {
		return Placement{}, err
	}
	if ok && current.IsOverride {
		return placementFromAsset(current, false), nil
	}

	shared, ok, err = CurrentSharedByPath(ctx, db, path)
	if err != nil {
		return Placement{}, err
	}
	if ok {
		return placementFromAsset(shared, true), nil
	}
	return Placement{}, nil
}

func placementFromAsset(a *Asset, fromShared bool) Placement {
	return Placement{
		Found:       true,
		FromShared:  fromShared,
		Fingerprint: a.Fingerprint,
		StorageKey:  a.StorageKey,
		Sha256:      a.Sha256,
		Version:     a.Version,
		Size:        a.Size,
	}
}

// CurrentSharedByPath returns the newest shared-baseline row for path.
func CurrentSharedByPath(ctx context.Context, db *sql.DB, path string) (*Asset, bool, error) {
	row := db.QueryRowContext(ctx, `SELECT id, server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, created_at
		FROM assets WHERE path=? AND is_override=0 ORDER BY id DESC LIMIT 1`, path)
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

// CurrentIndexedByServerPath returns the materialized current row for
// (server,path), maintained on disk by InsertAsset/EnsureReadIndexes.
func CurrentIndexedByServerPath(ctx context.Context, db *sql.DB, server, path string) (*Asset, bool, error) {
	row := db.QueryRowContext(ctx, `SELECT server, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at
		FROM current_assets WHERE server=? AND path=? LIMIT 1`, server, path)
	return scanCurrentAssetRow(row)
}

// CurrentIndexedSharedByPath returns the materialized newest shared baseline
// row for path.
func CurrentIndexedSharedByPath(ctx context.Context, db *sql.DB, path string) (*Asset, bool, error) {
	row := db.QueryRowContext(ctx, `SELECT '' AS server, path, version, fingerprint, sha256, size, 0 AS is_override, storage_key, updated_at
		FROM current_shared_assets WHERE path=? LIMIT 1`, path)
	return scanCurrentAssetRow(row)
}

func scanCurrentAssetRow(row *sql.Row) (*Asset, bool, error) {
	var a Asset
	var iso int
	err := row.Scan(&a.Server, &a.Path, &a.Version, &a.Fingerprint, &a.Sha256, &a.Size, &iso, &a.StorageKey, &a.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	a.IsOverride = iso == 1
	return &a, true, nil
}

// LatestAssetID is a cheap revision value for short-lived browser caches.
func LatestAssetID(ctx context.Context, db *sql.DB) (uint64, error) {
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM assets`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid || id.Int64 < 0 {
		return 0, nil
	}
	return uint64(id.Int64), nil
}

// EnsureReadIndexes prebuilds the on-disk current read index from historical
// assets rows. It is safe to call at startup; later HIP commits maintain the
// same tables incrementally through InsertAsset.
func EnsureReadIndexes(ctx context.Context, db *sql.DB) error {
	latest, err := LatestAssetID(ctx, db)
	if err != nil {
		return err
	}
	indexed, err := readIndexMetaMaxID(ctx, db)
	if err != nil {
		return err
	}
	if indexed == latest {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rollback := func() { _ = tx.Rollback() }

	if _, err := tx.ExecContext(ctx, `DELETE FROM current_assets`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM current_shared_assets`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM current_shared_bundles`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM current_override_bundles`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO current_assets(server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at)
		SELECT a.server, a.bundle_path, a.path, a.version, a.fingerprint, a.sha256, a.size, a.is_override, a.storage_key, a.created_at
		FROM assets a
		JOIN (
			SELECT server, path, MAX(id) AS mid
			FROM assets
			GROUP BY server, path
		) m ON a.id = m.mid
	`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO current_shared_assets(path, bundle_path, version, fingerprint, sha256, size, storage_key, updated_at)
		SELECT a.path, a.bundle_path, a.version, a.fingerprint, a.sha256, a.size, a.storage_key, a.created_at
		FROM assets a
		JOIN (
			SELECT path, MAX(id) AS mid
			FROM assets
			WHERE is_override=0
			GROUP BY path
		) m ON a.id = m.mid
	`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO current_shared_bundles(bundle_path, fingerprint, file_count, total_size, updated_at)
		SELECT c.bundle_path,
			(SELECT c2.fingerprint FROM current_shared_assets c2
				WHERE c2.bundle_path=c.bundle_path ORDER BY c2.updated_at DESC, c2.path ASC LIMIT 1),
			COUNT(*), SUM(c.size), MAX(c.updated_at)
		FROM current_shared_assets c
		WHERE c.bundle_path<>''
		GROUP BY c.bundle_path
	`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO current_override_bundles(server, bundle_path, fingerprint, file_count, total_size, updated_at)
		SELECT c.server, c.bundle_path,
			(SELECT c2.fingerprint FROM current_assets c2
				WHERE c2.server=c.server AND c2.bundle_path=c.bundle_path AND c2.is_override=1
				ORDER BY c2.updated_at DESC, c2.path ASC LIMIT 1),
			COUNT(*), SUM(c.size), MAX(c.updated_at)
		FROM current_assets c
		WHERE c.is_override=1 AND c.bundle_path<>''
		GROUP BY c.server, c.bundle_path
	`); err != nil {
		rollback()
		return err
	}
	if err := setReadIndexMetaMaxID(ctx, tx, latest); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func readIndexMetaMaxID(ctx context.Context, db *sql.DB) (uint64, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT value FROM read_index_meta WHERE key=?`, readIndexMaxAssetID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, nil
	}
	return value, nil
}

func setReadIndexMetaMaxID(ctx context.Context, tx *sql.Tx, maxID uint64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO read_index_meta(key, value)
		VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, readIndexMaxAssetID, strconv.FormatUint(maxID, 10))
	return err
}

// MarkReadIndexCurrent records that the materialized read index is up to date
// with the current assets table within the same commit transaction.
func MarkReadIndexCurrent(ctx context.Context, tx *sql.Tx) error {
	var id sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(id) FROM assets`).Scan(&id); err != nil {
		return err
	}
	if !id.Valid || id.Int64 < 0 {
		return setReadIndexMetaMaxID(ctx, tx, 0)
	}
	return setReadIndexMetaMaxID(ctx, tx, uint64(id.Int64))
}

func upsertCurrentAsset(ctx context.Context, tx *sql.Tx, a Asset, iso int) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO current_assets(server, bundle_path, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(server, path) DO UPDATE SET
			bundle_path=excluded.bundle_path,
			version=excluded.version,
			fingerprint=excluded.fingerprint,
			sha256=excluded.sha256,
			size=excluded.size,
			is_override=excluded.is_override,
			storage_key=excluded.storage_key,
			updated_at=excluded.updated_at
	`, a.Server, a.BundlePath, a.Path, a.Version, a.Fingerprint, a.Sha256, a.Size, iso, a.StorageKey, a.CreatedAt)
	return err
}

func upsertCurrentSharedAsset(ctx context.Context, tx *sql.Tx, a Asset) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO current_shared_assets(path, bundle_path, version, fingerprint, sha256, size, storage_key, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			bundle_path=excluded.bundle_path,
			version=excluded.version,
			fingerprint=excluded.fingerprint,
			sha256=excluded.sha256,
			size=excluded.size,
			storage_key=excluded.storage_key,
			updated_at=excluded.updated_at
	`, a.Path, a.BundlePath, a.Version, a.Fingerprint, a.Sha256, a.Size, a.StorageKey, a.CreatedAt)
	return err
}

// BrowseCurrent builds one browser page from SQLite on demand. It walks the
// path-ordered current tables with keyset seeks: each immediate child costs a
// couple of index probes and a directory's whole subtree is skipped in one
// jump, so a page touches O(limit·log n) index entries. The previous
// implementation evaluated `path LIKE 'prefix%'` over every row (LIKE cannot
// use the BINARY-collated path indexes), which made every request scan the
// full table — a public crawler paginating the tree turned that into a
// sustained multi-core burn.
func BrowseCurrent(ctx context.Context, db *sql.DB, server, prefix, cursor string, limit int) (BrowseResult, error) {
	if limit <= 0 {
		return BrowseResult{}, nil
	}
	revision, err := LatestAssetID(ctx, db)
	if err != nil {
		return BrowseResult{}, err
	}

	// bound is the inclusive lower key for the next seek. A directory cursor
	// resumes after its whole subtree; a file cursor resumes just after the
	// exact key (paths cannot contain NUL, so path+"\x00" is safe).
	bound := prefix
	if cursor != "" {
		if strings.HasSuffix(cursor, "/") {
			bound = nextAfterSubtree(cursor)
		} else {
			bound = cursor + "\x00"
		}
	}
	upper := nextAfterPrefix(prefix)

	items := make([]BrowseEntry, 0, limit+1)
	var nextShared, nextOverride *Asset
	sharedDone, overrideDone := false, false
	for len(items) < limit+1 {
		if nextShared == nil && !sharedDone {
			if nextShared, err = seekCurrentShared(ctx, db, bound, upper); err != nil {
				return BrowseResult{}, err
			}
			sharedDone = nextShared == nil
		}
		if nextOverride == nil && !overrideDone {
			if nextOverride, err = seekCurrentOverride(ctx, db, server, bound, upper); err != nil {
				return BrowseResult{}, err
			}
			overrideDone = nextOverride == nil
		}

		pick, fromShared := nextShared, true
		if pick == nil || (nextOverride != nil && nextOverride.Path <= pick.Path) {
			// Same path in both tables → the override placement wins,
			// matching the read-path policy.
			pick, fromShared = nextOverride, false
		}
		if pick == nil {
			break
		}

		rest := strings.TrimPrefix(pick.Path, prefix)
		if name, _, nested := strings.Cut(rest, "/"); nested {
			dirPath := prefix + name + "/"
			items = append(items, BrowseEntry{
				Kind: BrowseKindDirectory,
				Name: name,
				Path: dirPath,
			})
			bound = nextAfterSubtree(dirPath)
		} else {
			items = append(items, BrowseEntry{
				Kind:        BrowseKindAsset,
				Name:        rest,
				Path:        pick.Path,
				Fingerprint: pick.Fingerprint,
				Sha256:      pick.Sha256,
				Version:     pick.Version,
				Size:        pick.Size,
				FromShared:  fromShared,
			})
			bound = pick.Path + "\x00"
		}
		// Drop cached seek rows the new bound has moved past; anything still
		// ahead of it stays valid and saves a probe on the next iteration.
		if nextShared != nil && nextShared.Path < bound {
			nextShared = nil
		}
		if nextOverride != nil && nextOverride.Path < bound {
			nextOverride = nil
		}
	}

	result := BrowseResult{Items: items, Revision: revision}
	if len(result.Items) > limit {
		result.NextCursor = result.Items[limit-1].Path
		result.Items = result.Items[:limit]
	}
	return result, nil
}

// seekCurrentShared returns the first shared row with path >= bound (and
// < upper when upper is non-empty), or nil when the range is exhausted.
func seekCurrentShared(ctx context.Context, db *sql.DB, bound, upper string) (*Asset, error) {
	query := `
		SELECT '' AS server, path, version, fingerprint, sha256, size, 0 AS is_override, storage_key, updated_at
		FROM current_shared_assets
		WHERE path >= ?`
	args := []any{bound}
	if upper != "" {
		query += ` AND path < ?`
		args = append(args, upper)
	}
	query += ` ORDER BY path ASC LIMIT 1`
	a, ok, err := scanCurrentAssetRow(db.QueryRowContext(ctx, query, args...))
	if err != nil || !ok {
		return nil, err
	}
	return a, nil
}

// seekCurrentOverride is seekCurrentShared for one server's override rows.
// Served by the partial index idx_current_assets_override_path.
func seekCurrentOverride(ctx context.Context, db *sql.DB, server, bound, upper string) (*Asset, error) {
	query := `
		SELECT server, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at
		FROM current_assets
		WHERE server=? AND is_override=1 AND path >= ?`
	args := []any{server, bound}
	if upper != "" {
		query += ` AND path < ?`
		args = append(args, upper)
	}
	query += ` ORDER BY path ASC LIMIT 1`
	a, ok, err := scanCurrentAssetRow(db.QueryRowContext(ctx, query, args...))
	if err != nil || !ok {
		return nil, err
	}
	return a, nil
}

// nextAfterPrefix returns the smallest string greater than every key that
// starts with prefix, or "" when unbounded (empty prefix).
func nextAfterPrefix(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// nextAfterSubtree returns the inclusive seek key just past a directory's
// subtree: every key under "dir/" is < nextAfterPrefix("dir/").
func nextAfterSubtree(dirPath string) string {
	if next := nextAfterPrefix(dirPath); next != "" {
		return next
	}
	// Unreachable for canonical dir paths (they end in '/'), but fall back to
	// an impossible high key rather than restarting from the top.
	return dirPath + "\xff"
}

func scanBrowseRows(rows *sql.Rows, fn func(Asset)) error {
	for rows.Next() {
		var a Asset
		var iso int
		if err := rows.Scan(&a.Server, &a.Path, &a.Version, &a.Fingerprint, &a.Sha256, &a.Size, &iso, &a.StorageKey, &a.CreatedAt); err != nil {
			return err
		}
		a.IsOverride = iso == 1
		fn(a)
	}
	return rows.Err()
}
