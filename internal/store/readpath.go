package store

import (
	"context"
	"database/sql"
	"errors"
	"sort"
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

// BrowseCurrent builds one browser page from SQLite on demand. It avoids the
// old process-wide browse tree, so memory is proportional to a single request
// rather than to the full assets table.
func BrowseCurrent(ctx context.Context, db *sql.DB, server, prefix, cursor string, limit int) (BrowseResult, error) {
	if limit <= 0 {
		return BrowseResult{}, nil
	}
	revision, err := LatestAssetID(ctx, db)
	if err != nil {
		return BrowseResult{}, err
	}

	entries := map[string]BrowseEntry{}
	pattern := likePrefix(prefix)
	if err := scanCurrentSharedForBrowse(ctx, db, pattern, func(a Asset) {
		addBrowseAsset(entries, prefix, &a, true)
	}); err != nil {
		return BrowseResult{}, err
	}
	if err := scanCurrentOverridesForBrowse(ctx, db, server, pattern, func(a Asset) {
		addBrowseAsset(entries, prefix, &a, false)
	}); err != nil {
		return BrowseResult{}, err
	}

	items := make([]BrowseEntry, 0, len(entries))
	for _, entry := range entries {
		if cursor != "" && entry.Path <= cursor {
			continue
		}
		items = append(items, entry)
	}
	sort.Slice(items, func(i, j int) bool {
		return browseLess(items[i], items[j])
	})

	result := BrowseResult{Items: items, Revision: revision}
	if len(result.Items) > limit {
		result.NextCursor = result.Items[limit-1].Path
		result.Items = result.Items[:limit]
	}
	return result, nil
}

func scanCurrentSharedForBrowse(ctx context.Context, db *sql.DB, pattern string, fn func(Asset)) error {
	rows, err := db.QueryContext(ctx, `
		SELECT '' AS server, path, version, fingerprint, sha256, size, 0 AS is_override, storage_key, updated_at
		FROM current_shared_assets
		WHERE path LIKE ? ESCAPE '\'
		ORDER BY path ASC
	`, pattern)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanBrowseRows(rows, fn)
}

func scanCurrentOverridesForBrowse(ctx context.Context, db *sql.DB, server, pattern string, fn func(Asset)) error {
	rows, err := db.QueryContext(ctx, `
		SELECT server, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at
		FROM current_assets
		WHERE server=? AND is_override=1 AND path LIKE ? ESCAPE '\'
		ORDER BY path ASC
	`, server, pattern)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanBrowseRows(rows, fn)
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

func addBrowseAsset(entries map[string]BrowseEntry, prefix string, a *Asset, fromShared bool) {
	if a.Path == "" || !strings.HasPrefix(a.Path, prefix) {
		return
	}
	rest := strings.TrimPrefix(a.Path, prefix)
	if rest == "" {
		return
	}
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		name := rest[:slash]
		if name == "" {
			return
		}
		path := prefix + name + "/"
		entries[BrowseKindDirectory+"\x00"+path] = BrowseEntry{
			Kind: BrowseKindDirectory,
			Name: name,
			Path: path,
		}
		return
	}

	entry := BrowseEntry{
		Kind:        BrowseKindAsset,
		Name:        rest,
		Path:        a.Path,
		Fingerprint: a.Fingerprint,
		Sha256:      a.Sha256,
		Version:     a.Version,
		Size:        a.Size,
		FromShared:  fromShared,
	}
	entries[BrowseKindAsset+"\x00"+entry.Path] = entry
}

func browseLess(a, b BrowseEntry) bool {
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	return a.Kind < b.Kind
}

func likePrefix(prefix string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(prefix) + "%"
}
