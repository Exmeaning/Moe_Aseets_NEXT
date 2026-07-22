package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// VersionSummary is one committed versions row plus the number of asset rows
// minted by that commit (i.e. the size of that version's update diff, before
// any file-type filtering).
type VersionSummary struct {
	Version
	ChangedAssets int64
}

// ListVersions returns committed versions for one server, newest first.
// beforeID pages backwards through history: pass 0 for the first page, then
// the returned nextBeforeID for the next one (0 means no more pages).
func ListVersions(ctx context.Context, db *sql.DB, server string, beforeID int64, limit int) ([]VersionSummary, int64, error) {
	if limit <= 0 {
		return nil, 0, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT id, server, app_version, asset_version, asset_hash, bundle_count, committed_at, stats_json
		FROM versions
		WHERE server=? AND (?=0 OR id<?)
		ORDER BY id DESC
		LIMIT ?
	`, server, beforeID, beforeID, limit+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []VersionSummary{}
	for rows.Next() {
		var v VersionSummary
		if err := rows.Scan(&v.ID, &v.Server, &v.AppVersion, &v.AssetVersion, &v.AssetHash, &v.BundleCount, &v.CommittedAt, &v.StatsJSON); err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var nextBeforeID int64
	if len(out) > limit {
		out = out[:limit]
		nextBeforeID = out[limit-1].ID
	}
	if err := fillChangedAssetCounts(ctx, db, server, out); err != nil {
		return nil, 0, err
	}
	return out, nextBeforeID, nil
}

// fillChangedAssetCounts resolves ChangedAssets for one page of versions with
// a single grouped query over idx_assets_server_version.
func fillChangedAssetCounts(ctx context.Context, db *sql.DB, server string, page []VersionSummary) error {
	if len(page) == 0 {
		return nil
	}
	args := make([]any, 0, len(page)+1)
	args = append(args, server)
	placeholders := make([]string, 0, len(page))
	for _, v := range page {
		placeholders = append(placeholders, "?")
		args = append(args, v.AssetVersion)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT version, COUNT(*)
		FROM assets
		WHERE server=? AND path<>'' AND version IN (`+strings.Join(placeholders, ",")+`)
		GROUP BY version
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var version string
		var n int64
		if err := rows.Scan(&version, &n); err != nil {
			return err
		}
		counts[version] = n
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range page {
		page[i].ChangedAssets = counts[page[i].AssetVersion]
	}
	return nil
}

// DiffEntry is one asset row minted by a version commit: a file that this
// version added to the server, or replaced with new content.
type DiffEntry struct {
	Path        string
	BundlePath  string
	Fingerprint string
	Sha256      string
	Size        int64
	IsOverride  bool
	// Existed reports whether the server already had any row for this path
	// before this commit ("updated"); false means first appearance ("added").
	Existed bool
}

// DiffCursor is a keyset position inside the (size DESC, path ASC) diff
// ordering. The zero value means "start from the top".
type DiffCursor struct {
	Size int64
	Path string
	Set  bool
}

// VersionDiff is one page of a version's update diff plus version metadata.
type VersionDiff struct {
	Version      Version
	TotalChanged int64
	Items        []DiffEntry
	NextCursor   DiffCursor
}

// DiffOptions defines filters and pagination parameters for DiffVersion.
type DiffOptions struct {
	// Action filters by change type: "added" (default, Existed==false),
	// "updated" (Existed==true), or "all" (both).
	Action string
	// Exts, when non-empty, keeps only paths matching the specified extension suffixes.
	Exts []string
	// MinMp3Size, when > 0, restricts ".mp3" files to size >= MinMp3Size.
	MinMp3Size int64
	Cursor     DiffCursor
	Limit      int
}

// DiffVersion returns the files added/updated by (server, assetVersion),
// largest first (path breaks ties so pages are stable), resuming after
// cursor. Filter options in opts control change type, extension whitelist,
// and minimum size constraints. TotalChanged counts with the same filters
// applied so pagination maths stay consistent.
// found=false means no such version was ever committed. Deletions never
// happen in this store, so a diff is only ever additions and content updates.
func DiffVersion(ctx context.Context, db *sql.DB, server, assetVersion string, opts DiffOptions) (VersionDiff, bool, error) {
	var out VersionDiff
	if opts.Limit <= 0 {
		return out, false, nil
	}
	// A re-commit of the same asset_version with a different asset_hash upserts
	// a second versions row; asset rows key on the version string alone, so the
	// newest row is the authoritative metadata for the merged diff.
	row := db.QueryRowContext(ctx, `
		SELECT id, server, app_version, asset_version, asset_hash, bundle_count, committed_at, stats_json
		FROM versions
		WHERE server=? AND asset_version=?
		ORDER BY id DESC
		LIMIT 1
	`, server, assetVersion)
	v := &out.Version
	err := row.Scan(&v.ID, &v.Server, &v.AppVersion, &v.AssetVersion, &v.AssetHash, &v.BundleCount, &v.CommittedAt, &v.StatsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}

	// Both queries narrow through idx_assets_server_version first; the LIKE
	// suffix filter and the size sort then only touch this version's delta
	// rows, which stay small compared to the whole assets table.
	actCond := actionCondition(opts.Action)
	extCond, extArgs := extSuffixCondition(opts.Exts, opts.MinMp3Size)

	countArgs := append([]any{server, assetVersion}, extArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM assets a
		WHERE a.server=? AND a.version=? AND a.path<>''`+actCond+extCond,
		countArgs...).Scan(&out.TotalChanged); err != nil {
		return out, false, err
	}

	pageCond := ""
	pageArgs := append([]any{server, assetVersion}, extArgs...)
	if opts.Cursor.Set {
		pageCond = ` AND (a.size<? OR (a.size=? AND a.path>?))`
		pageArgs = append(pageArgs, opts.Cursor.Size, opts.Cursor.Size, opts.Cursor.Path)
	}
	pageArgs = append(pageArgs, opts.Limit+1)
	rows, err := db.QueryContext(ctx, `
		SELECT a.path, a.bundle_path, a.fingerprint, a.sha256, a.size, a.is_override,
		       EXISTS(SELECT 1 FROM assets p WHERE p.server=a.server AND p.path=a.path AND p.id<a.id)
		FROM assets a
		WHERE a.server=? AND a.version=? AND a.path<>''`+actCond+extCond+pageCond+`
		ORDER BY a.size DESC, a.path ASC
		LIMIT ?
	`, pageArgs...)
	if err != nil {
		return out, false, err
	}
	defer rows.Close()

	out.Items = []DiffEntry{}
	for rows.Next() {
		var e DiffEntry
		var iso, existed int
		if err := rows.Scan(&e.Path, &e.BundlePath, &e.Fingerprint, &e.Sha256, &e.Size, &iso, &existed); err != nil {
			return out, false, err
		}
		e.IsOverride = iso == 1
		e.Existed = existed == 1
		out.Items = append(out.Items, e)
	}
	if err := rows.Err(); err != nil {
		return out, false, err
	}
	if len(out.Items) > opts.Limit {
		out.Items = out.Items[:opts.Limit]
		last := out.Items[opts.Limit-1]
		out.NextCursor = DiffCursor{Size: last.Size, Path: last.Path, Set: true}
	}
	return out, true, nil
}

func actionCondition(action string) string {
	switch strings.ToLower(action) {
	case "updated", "replaced":
		return " AND EXISTS(SELECT 1 FROM assets p WHERE p.server=a.server AND p.path=a.path AND p.id<a.id)"
	case "all":
		return ""
	case "added", "":
		fallthrough
	default:
		return " AND NOT EXISTS(SELECT 1 FROM assets p WHERE p.server=a.server AND p.path=a.path AND p.id<a.id)"
	}
}

// extSuffixCondition builds "AND (a.path LIKE '%.webp' OR ...)" for validated
// extension tokens. Tokens are [a-z0-9]+ so no LIKE metacharacters can leak.
// If minMp3Size > 0, .mp3 extension checks additionally require a.size >= minMp3Size.
func extSuffixCondition(exts []string, minMp3Size int64) (string, []any) {
	if len(exts) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(exts))
	args := make([]any, 0, len(exts)*2)
	for _, ext := range exts {
		if ext == "mp3" && minMp3Size > 0 {
			parts = append(parts, "(a.path LIKE ? AND a.size >= ?)")
			args = append(args, "%."+ext, minMp3Size)
		} else {
			parts = append(parts, "a.path LIKE ?")
			args = append(args, "%."+ext)
		}
	}
	return " AND (" + strings.Join(parts, " OR ") + ")", args
}

// BundleDiffEntry is one bundle added or updated in a version commit.
type BundleDiffEntry struct {
	BundlePath  string
	Fingerprint string
	Source      string
	FileCount   int64
	TotalSize   int64
}

// VersionBundleDiff represents one page of a version's bundle update diff.
type VersionBundleDiff struct {
	Version      Version
	TotalChanged int64
	Items        []BundleDiffEntry
	NextCursor   string
}

// DiffBundleOptions defines pagination and prefix parameters for DiffVersionBundles.
type DiffBundleOptions struct {
	Prefix string
	Cursor string
	Limit  int
}

// VersionBundleFilesDiff is a list of files within a specific bundle changed in a version.
type VersionBundleFilesDiff struct {
	Version      Version
	BundlePath   string
	TotalChanged int64
	Items        []DiffEntry
	NextCursor   DiffCursor
}

// DiffVersionBundles returns the bundles added/updated by (server, assetVersion),
// resuming after cursor.
func DiffVersionBundles(ctx context.Context, db *sql.DB, server, assetVersion string, opts DiffBundleOptions) (VersionBundleDiff, bool, error) {
	var out VersionBundleDiff
	if opts.Limit <= 0 {
		return out, false, nil
	}

	row := db.QueryRowContext(ctx, `
		SELECT id, server, app_version, asset_version, asset_hash, bundle_count, committed_at, stats_json
		FROM versions
		WHERE server=? AND asset_version=?
		ORDER BY id DESC
		LIMIT 1
	`, server, assetVersion)
	v := &out.Version
	err := row.Scan(&v.ID, &v.Server, &v.AppVersion, &v.AssetVersion, &v.AssetHash, &v.BundleCount, &v.CommittedAt, &v.StatsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}

	var count int64
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bundle_completions WHERE server=? AND asset_version=?`, server, assetVersion).Scan(&count)
	hasCompletions := count > 0

	prefixFilter := ""
	prefixLike := ""
	if opts.Prefix != "" {
		prefixFilter = " AND bc.bundle_path LIKE ?"
		prefixLike = strings.ReplaceAll(strings.ReplaceAll(opts.Prefix, "%", "\\%"), "_", "\\_") + "%"
	}

	if hasCompletions {
		countQuery := `SELECT COUNT(*) FROM bundle_completions bc WHERE bc.server=? AND bc.asset_version=?`
		countArgs := []any{server, assetVersion}
		if opts.Prefix != "" {
			countQuery += " AND bc.bundle_path LIKE ?"
			countArgs = append(countArgs, prefixLike)
		}
		if err := db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&out.TotalChanged); err != nil {
			return out, false, err
		}

		pageCond := ""
		pageArgs := []any{server, assetVersion, server, assetVersion}
		if opts.Prefix != "" {
			pageArgs = append(pageArgs, prefixLike)
		}
		if opts.Cursor != "" {
			pageCond = ` AND bc.bundle_path > ?`
			pageArgs = append(pageArgs, opts.Cursor)
		}
		pageArgs = append(pageArgs, opts.Limit+1)

		q := `
			SELECT bc.bundle_path, bc.fingerprint, bc.source,
			       COALESCE(a.file_count, 0) AS file_count,
			       COALESCE(a.total_size, 0) AS total_size
			FROM bundle_completions bc
			LEFT JOIN (
				SELECT bundle_path, COUNT(*) AS file_count, SUM(size) AS total_size
				FROM assets
				WHERE server=? AND version=? AND bundle_path<>''
				GROUP BY bundle_path
			) a ON bc.bundle_path = a.bundle_path
			WHERE bc.server=? AND bc.asset_version=?` + prefixFilter + pageCond + `
			ORDER BY bc.bundle_path ASC
			LIMIT ?
		`
		rows, err := db.QueryContext(ctx, q, pageArgs...)
		if err != nil {
			return out, false, err
		}
		defer rows.Close()

		out.Items = []BundleDiffEntry{}
		for rows.Next() {
			var e BundleDiffEntry
			if err := rows.Scan(&e.BundlePath, &e.Fingerprint, &e.Source, &e.FileCount, &e.TotalSize); err != nil {
				return out, false, err
			}
			out.Items = append(out.Items, e)
		}
		if err := rows.Err(); err != nil {
			return out, false, err
		}
	} else {
		assetPrefixFilter := ""
		if opts.Prefix != "" {
			assetPrefixFilter = " AND bundle_path LIKE ?"
		}

		countQuery := `SELECT COUNT(DISTINCT bundle_path) FROM assets WHERE server=? AND version=? AND bundle_path<>''`
		countArgs := []any{server, assetVersion}
		if opts.Prefix != "" {
			countQuery += assetPrefixFilter
			countArgs = append(countArgs, prefixLike)
		}
		if err := db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&out.TotalChanged); err != nil {
			return out, false, err
		}

		pageCond := ""
		pageArgs := []any{server, assetVersion}
		if opts.Prefix != "" {
			pageArgs = append(pageArgs, prefixLike)
		}
		if opts.Cursor != "" {
			pageCond = ` AND bundle_path > ?`
			pageArgs = append(pageArgs, opts.Cursor)
		}
		pageArgs = append(pageArgs, opts.Limit+1)

		q := `
			SELECT bundle_path, fingerprint, 'uploaded' AS source, COUNT(*) AS file_count, SUM(size) AS total_size
			FROM assets
			WHERE server=? AND version=? AND bundle_path<>''` + assetPrefixFilter + pageCond + `
			GROUP BY bundle_path
			ORDER BY bundle_path ASC
			LIMIT ?
		`
		rows, err := db.QueryContext(ctx, q, pageArgs...)
		if err != nil {
			return out, false, err
		}
		defer rows.Close()

		out.Items = []BundleDiffEntry{}
		for rows.Next() {
			var e BundleDiffEntry
			if err := rows.Scan(&e.BundlePath, &e.Fingerprint, &e.Source, &e.FileCount, &e.TotalSize); err != nil {
				return out, false, err
			}
			out.Items = append(out.Items, e)
		}
		if err := rows.Err(); err != nil {
			return out, false, err
		}
	}

	if len(out.Items) > opts.Limit {
		out.Items = out.Items[:opts.Limit]
		out.NextCursor = out.Items[opts.Limit-1].BundlePath
	}
	return out, true, nil
}

// DiffBundleFiles returns the files added/updated in a single bundle for (server, assetVersion).
func DiffBundleFiles(ctx context.Context, db *sql.DB, server, assetVersion, bundlePath string, opts DiffOptions) (VersionBundleFilesDiff, bool, error) {
	var out VersionBundleFilesDiff
	out.BundlePath = bundlePath
	if opts.Limit <= 0 {
		return out, false, nil
	}

	row := db.QueryRowContext(ctx, `
		SELECT id, server, app_version, asset_version, asset_hash, bundle_count, committed_at, stats_json
		FROM versions
		WHERE server=? AND asset_version=?
		ORDER BY id DESC
		LIMIT 1
	`, server, assetVersion)
	v := &out.Version
	err := row.Scan(&v.ID, &v.Server, &v.AppVersion, &v.AssetVersion, &v.AssetHash, &v.BundleCount, &v.CommittedAt, &v.StatsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}

	actCond := actionCondition(opts.Action)
	extCond, extArgs := extSuffixCondition(opts.Exts, opts.MinMp3Size)

	countArgs := append([]any{server, assetVersion, bundlePath}, extArgs...)
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM assets a
		WHERE a.server=? AND a.version=? AND a.bundle_path=? AND a.path<>''`+actCond+extCond,
		countArgs...).Scan(&out.TotalChanged); err != nil {
		return out, false, err
	}

	pageCond := ""
	pageArgs := append([]any{server, assetVersion, bundlePath}, extArgs...)
	if opts.Cursor.Set {
		pageCond = ` AND (a.size<? OR (a.size=? AND a.path>?))`
		pageArgs = append(pageArgs, opts.Cursor.Size, opts.Cursor.Size, opts.Cursor.Path)
	}
	pageArgs = append(pageArgs, opts.Limit+1)
	rows, err := db.QueryContext(ctx, `
		SELECT a.path, a.bundle_path, a.fingerprint, a.sha256, a.size, a.is_override,
		       EXISTS(SELECT 1 FROM assets p WHERE p.server=a.server AND p.path=a.path AND p.id<a.id)
		FROM assets a
		WHERE a.server=? AND a.version=? AND a.bundle_path=? AND a.path<>''`+actCond+extCond+pageCond+`
		ORDER BY a.size DESC, a.path ASC
		LIMIT ?
	`, pageArgs...)
	if err != nil {
		return out, false, err
	}
	defer rows.Close()

	out.Items = []DiffEntry{}
	for rows.Next() {
		var e DiffEntry
		var iso, existed int
		if err := rows.Scan(&e.Path, &e.BundlePath, &e.Fingerprint, &e.Sha256, &e.Size, &iso, &existed); err != nil {
			return out, false, err
		}
		e.IsOverride = iso == 1
		e.Existed = existed == 1
		out.Items = append(out.Items, e)
	}
	if err := rows.Err(); err != nil {
		return out, false, err
	}
	if len(out.Items) > opts.Limit {
		out.Items = out.Items[:opts.Limit]
		last := out.Items[opts.Limit-1]
		out.NextCursor = DiffCursor{Size: last.Size, Path: last.Path, Set: true}
	}
	return out, true, nil
}


