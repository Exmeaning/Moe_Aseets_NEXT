package store

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
)

// BundleBrowseEntry is one immediate child in a bundle browser listing:
// either a directory that (transitively) contains bundles, or a bundle.
type BundleBrowseEntry struct {
	Kind        string
	Name        string
	Path        string
	Fingerprint string
	FileCount   int64
	TotalSize   int64
	UpdatedAt   int64
	FromShared  bool
}

// BundleBrowseResult is a single-page bundle listing.
type BundleBrowseResult struct {
	Items      []BundleBrowseEntry
	NextCursor string
	Revision   uint64
}

// BundleInfo describes one current bundle as seen by a server.
type BundleInfo struct {
	Path        string
	Fingerprint string
	FileCount   int64
	TotalSize   int64
	UpdatedAt   int64
	FromShared  bool
}

// refreshBundleAggregates recomputes the current_*_bundles rows touched by an
// asset upsert. Runs inside the same COMMIT transaction as InsertAsset, so the
// bundle tables can never drift from the current asset tables.
func refreshBundleAggregates(ctx context.Context, tx *sql.Tx, a Asset) error {
	if a.BundlePath == "" {
		return nil
	}
	if !a.IsOverride {
		// The shared group for this bundle just gained/updated a row, so it is
		// guaranteed non-empty; shared rows are never deleted.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO current_shared_bundles(bundle_path, fingerprint, file_count, total_size, updated_at)
			SELECT ?,
				(SELECT fingerprint FROM current_shared_assets
					WHERE bundle_path=? ORDER BY updated_at DESC, path ASC LIMIT 1),
				COUNT(*), COALESCE(SUM(size), 0), COALESCE(MAX(updated_at), 0)
			FROM current_shared_assets WHERE bundle_path=?
			HAVING COUNT(*) > 0
			ON CONFLICT(bundle_path) DO UPDATE SET
				fingerprint=excluded.fingerprint,
				file_count=excluded.file_count,
				total_size=excluded.total_size,
				updated_at=excluded.updated_at
		`, a.BundlePath, a.BundlePath, a.BundlePath); err != nil {
			return err
		}
	}
	// The override group can shrink to zero when a server's row flips from
	// override back to shared placement, so delete-when-empty first.
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM current_override_bundles
		WHERE server=? AND bundle_path=? AND NOT EXISTS (
			SELECT 1 FROM current_assets WHERE server=? AND bundle_path=? AND is_override=1
		)
	`, a.Server, a.BundlePath, a.Server, a.BundlePath); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO current_override_bundles(server, bundle_path, fingerprint, file_count, total_size, updated_at)
		SELECT ?, ?,
			(SELECT fingerprint FROM current_assets
				WHERE server=? AND bundle_path=? AND is_override=1
				ORDER BY updated_at DESC, path ASC LIMIT 1),
			COUNT(*), COALESCE(SUM(size), 0), COALESCE(MAX(updated_at), 0)
		FROM current_assets WHERE server=? AND bundle_path=? AND is_override=1
		HAVING COUNT(*) > 0
		ON CONFLICT(server, bundle_path) DO UPDATE SET
			fingerprint=excluded.fingerprint,
			file_count=excluded.file_count,
			total_size=excluded.total_size,
			updated_at=excluded.updated_at
	`, a.Server, a.BundlePath, a.Server, a.BundlePath, a.Server, a.BundlePath)
	return err
}

// BrowseBundles builds one bundle browser page from the materialized bundle
// tables. The listing shows the immediate children of prefix where leaves are
// bundles (not files); the merge policy matches the file browser: a bundle the
// server overrides is presented with the override metadata.
func BrowseBundles(ctx context.Context, db *sql.DB, server, prefix, cursor string, limit int) (BundleBrowseResult, error) {
	if limit <= 0 {
		return BundleBrowseResult{}, nil
	}
	revision, err := LatestAssetID(ctx, db)
	if err != nil {
		return BundleBrowseResult{}, err
	}

	entries := map[string]BundleBrowseEntry{}
	upper := nextAfterPrefix(prefix)
	sharedQuery := `
		SELECT bundle_path, fingerprint, file_count, total_size, updated_at
		FROM current_shared_bundles
		WHERE bundle_path >= ?`
	sharedArgs := []any{prefix}
	overrideQuery := `
		SELECT bundle_path, fingerprint, file_count, total_size, updated_at
		FROM current_override_bundles
		WHERE server=? AND bundle_path >= ?`
	overrideArgs := []any{server, prefix}
	if upper != "" {
		sharedQuery += ` AND bundle_path < ?`
		sharedArgs = append(sharedArgs, upper)
		overrideQuery += ` AND bundle_path < ?`
		overrideArgs = append(overrideArgs, upper)
	}
	if err := scanBundleRows(ctx, db, sharedQuery+` ORDER BY bundle_path ASC`, sharedArgs, func(e BundleBrowseEntry) {
		e.FromShared = true
		addBundleBrowseEntry(entries, prefix, e)
	}); err != nil {
		return BundleBrowseResult{}, err
	}
	if err := scanBundleRows(ctx, db, overrideQuery+` ORDER BY bundle_path ASC`, overrideArgs, func(e BundleBrowseEntry) {
		e.FromShared = false
		addBundleBrowseEntry(entries, prefix, e)
	}); err != nil {
		return BundleBrowseResult{}, err
	}

	items := make([]BundleBrowseEntry, 0, len(entries))
	for _, entry := range entries {
		if cursor != "" && entry.Path <= cursor {
			continue
		}
		items = append(items, entry)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Path != items[j].Path {
			return items[i].Path < items[j].Path
		}
		return items[i].Kind < items[j].Kind
	})

	result := BundleBrowseResult{Items: items, Revision: revision}
	if len(result.Items) > limit {
		result.NextCursor = result.Items[limit-1].Path
		result.Items = result.Items[:limit]
	}
	return result, nil
}

func scanBundleRows(ctx context.Context, db *sql.DB, query string, args []any, fn func(BundleBrowseEntry)) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var e BundleBrowseEntry
		if err := rows.Scan(&e.Path, &e.Fingerprint, &e.FileCount, &e.TotalSize, &e.UpdatedAt); err != nil {
			return err
		}
		fn(e)
	}
	return rows.Err()
}

func addBundleBrowseEntry(entries map[string]BundleBrowseEntry, prefix string, e BundleBrowseEntry) {
	if e.Path == "" || !strings.HasPrefix(e.Path, prefix) {
		return
	}
	rest := strings.TrimPrefix(e.Path, prefix)
	if rest == "" {
		return
	}
	if name, _, nested := strings.Cut(rest, "/"); nested {
		if name == "" {
			return
		}
		path := prefix + name + "/"
		entries[BrowseKindDirectory+"\x00"+path] = BundleBrowseEntry{
			Kind: BrowseKindDirectory,
			Name: name,
			Path: path,
		}
		return
	}
	e.Kind = BrowseKindBundle
	e.Name = rest
	entries[BrowseKindBundle+"\x00"+e.Path] = e
}

// BundleFiles lists the current files of one bundle as seen by a server:
// shared baseline rows merged with the server's override rows, override
// metadata winning per path. The second return value is the bundle's own
// current metadata, or (zero, false) when the bundle is unknown.
func BundleFiles(ctx context.Context, db *sql.DB, server, bundlePath, cursor string, limit int) (BrowseResult, BundleInfo, bool, error) {
	if limit <= 0 {
		return BrowseResult{}, BundleInfo{}, false, nil
	}
	revision, err := LatestAssetID(ctx, db)
	if err != nil {
		return BrowseResult{}, BundleInfo{}, false, err
	}

	info, found, err := lookupBundleInfo(ctx, db, server, bundlePath)
	if err != nil {
		return BrowseResult{}, BundleInfo{}, false, err
	}
	if !found {
		return BrowseResult{Revision: revision}, BundleInfo{}, false, nil
	}

	entries := map[string]BrowseEntry{}
	if err := scanBundleFileRows(ctx, db, `
		SELECT '' AS server, path, version, fingerprint, sha256, size, 0 AS is_override, storage_key, updated_at
		FROM current_shared_assets
		WHERE bundle_path=?
		ORDER BY path ASC
	`, []any{bundlePath}, func(a Asset) {
		addBundleFileEntry(entries, bundlePath, &a, true)
	}); err != nil {
		return BrowseResult{}, BundleInfo{}, false, err
	}
	if err := scanBundleFileRows(ctx, db, `
		SELECT server, path, version, fingerprint, sha256, size, is_override, storage_key, updated_at
		FROM current_assets
		WHERE server=? AND is_override=1 AND bundle_path=?
		ORDER BY path ASC
	`, []any{server, bundlePath}, func(a Asset) {
		addBundleFileEntry(entries, bundlePath, &a, false)
	}); err != nil {
		return BrowseResult{}, BundleInfo{}, false, err
	}

	items := make([]BrowseEntry, 0, len(entries))
	for _, entry := range entries {
		if cursor != "" && entry.Path <= cursor {
			continue
		}
		items = append(items, entry)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Path < items[j].Path
	})

	result := BrowseResult{Items: items, Revision: revision}
	if len(result.Items) > limit {
		result.NextCursor = result.Items[limit-1].Path
		result.Items = result.Items[:limit]
	}
	return result, info, true, nil
}

func lookupBundleInfo(ctx context.Context, db *sql.DB, server, bundlePath string) (BundleInfo, bool, error) {
	info := BundleInfo{Path: bundlePath}
	err := db.QueryRowContext(ctx, `
		SELECT fingerprint, file_count, total_size, updated_at
		FROM current_override_bundles WHERE server=? AND bundle_path=? LIMIT 1
	`, server, bundlePath).Scan(&info.Fingerprint, &info.FileCount, &info.TotalSize, &info.UpdatedAt)
	if err == nil {
		return info, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BundleInfo{}, false, err
	}
	err = db.QueryRowContext(ctx, `
		SELECT fingerprint, file_count, total_size, updated_at
		FROM current_shared_bundles WHERE bundle_path=? LIMIT 1
	`, bundlePath).Scan(&info.Fingerprint, &info.FileCount, &info.TotalSize, &info.UpdatedAt)
	if err == nil {
		info.FromShared = true
		return info, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return BundleInfo{}, false, nil
	}
	return BundleInfo{}, false, err
}

func scanBundleFileRows(ctx context.Context, db *sql.DB, query string, args []any, fn func(Asset)) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scanBrowseRows(rows, fn)
}

func addBundleFileEntry(entries map[string]BrowseEntry, bundlePath string, a *Asset, fromShared bool) {
	if a.Path == "" {
		return
	}
	// Name is relative to the bundle; a single-file bundle (path == bundle
	// path) falls back to its last path segment.
	name := strings.TrimPrefix(a.Path, bundlePath+"/")
	if name == a.Path {
		if idx := strings.LastIndexByte(a.Path, '/'); idx >= 0 {
			name = a.Path[idx+1:]
		}
	}
	entries[a.Path] = BrowseEntry{
		Kind:        BrowseKindAsset,
		Name:        name,
		Path:        a.Path,
		Fingerprint: a.Fingerprint,
		Sha256:      a.Sha256,
		Version:     a.Version,
		Size:        a.Size,
		FromShared:  fromShared,
	}
}
