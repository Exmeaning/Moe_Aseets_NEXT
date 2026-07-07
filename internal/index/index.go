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
	"sort"
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

const (
	BrowseKindDirectory = "directory"
	BrowseKindAsset     = "asset"
)

// BrowseEntry is one immediate child in the asset browser tree. Directory
// entries only carry Kind/Name/Path; asset entries also carry metadata from
// the current read-path placement.
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
}

// Snapshot is an immutable placement view.
type Snapshot struct {
	// Revision increments every time Rebuild publishes a new snapshot. HTTP
	// handlers can use it for short-lived response-cache keys.
	Revision uint64
	// OverrideIndex: (server, path) → OverrideRow, only entries whose CURRENT
	// row (newest by id) has is_override=1.
	OverrideIndex map[string]OverrideRow
	// CurrentShared: path → newest shared-baseline row.
	CurrentShared map[string]SharedRow
	// SharedBrowse maps a directory prefix ("" or "foo/bar/") to its sorted
	// immediate shared-baseline children.
	SharedBrowse map[string][]BrowseEntry
	// OverrideBrowse maps server → directory prefix → sorted immediate
	// override children for that server.
	OverrideBrowse map[string]map[string][]BrowseEntry
}

// Key builds the (server, path) composite key used in OverrideIndex.
func Key(server, path string) string {
	return server + "\x00" + path
}

// Index is the atomic holder.
type Index struct {
	ptr atomic.Pointer[Snapshot]
	rev atomic.Uint64
}

// New creates an empty index (Snapshot with empty maps).
func New() *Index {
	i := &Index{}
	empty := &Snapshot{
		OverrideIndex:  map[string]OverrideRow{},
		CurrentShared:  map[string]SharedRow{},
		SharedBrowse:   map[string][]BrowseEntry{},
		OverrideBrowse: map[string]map[string][]BrowseEntry{},
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
		Revision:       i.rev.Add(1),
		OverrideIndex:  map[string]OverrideRow{},
		CurrentShared:  map[string]SharedRow{},
		SharedBrowse:   map[string][]BrowseEntry{},
		OverrideBrowse: map[string]map[string][]BrowseEntry{},
	}
	sharedBrowse := newBrowseBuilder()
	overrideBrowse := map[string]browseBuilder{}

	// Newest override row per (server, path): those whose id is max within
	// (server, path) AND that row has is_override=1.
	overrideRows, err := db.QueryContext(ctx, `
		SELECT a.server, a.path, a.fingerprint, a.storage_key, a.sha256, a.version, a.size
		FROM assets a
		JOIN (
			SELECT server, path, MAX(id) AS mid FROM assets GROUP BY server, path
		) m ON a.id = m.mid
		WHERE a.is_override = 1
		ORDER BY a.server ASC, a.path ASC
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
			builder := overrideBrowse[srv]
			if builder == nil {
				builder = newBrowseBuilder()
				overrideBrowse[srv] = builder
			}
			builder.addAsset(BrowseEntry{
				Kind:        BrowseKindAsset,
				Path:        p,
				Fingerprint: row.Fingerprint,
				Sha256:      row.Sha256,
				Version:     row.Version,
				Size:        row.Size,
				FromShared:  false,
			})
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
		ORDER BY a.path ASC
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
			sharedBrowse.addAsset(BrowseEntry{
				Kind:        BrowseKindAsset,
				Path:        p,
				Fingerprint: row.Fingerprint,
				Sha256:      row.Sha256,
				Version:     row.Version,
				Size:        row.Size,
				FromShared:  true,
			})
		}
	}()
	if err != nil {
		return err
	}

	snap.SharedBrowse = sharedBrowse.freeze()
	for srv, builder := range overrideBrowse {
		snap.OverrideBrowse[srv] = builder.freeze()
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

// Browse returns one page of immediate children for prefix. prefix must be
// either "" or a slash-suffixed canonical directory path; cursor is the Path
// of the last item returned by the previous page.
func (s *Snapshot) Browse(server, prefix, cursor string, limit int) BrowseResult {
	if s == nil || limit <= 0 {
		return BrowseResult{}
	}
	shared := s.SharedBrowse[prefix]
	var overrides []BrowseEntry
	if byPrefix := s.OverrideBrowse[server]; byPrefix != nil {
		overrides = byPrefix[prefix]
	}

	items := make([]BrowseEntry, 0, limit)
	i, j := 0, 0
	for len(items) < limit+1 && (i < len(shared) || j < len(overrides)) {
		var entry BrowseEntry
		if j < len(overrides) && (i >= len(shared) || browseLess(overrides[j], shared[i])) {
			entry = overrides[j]
			j++
		} else if i < len(shared) && j < len(overrides) && sameBrowseEntry(shared[i], overrides[j]) {
			// Same file or directory exists in both lists. For files the
			// override metadata wins; for directories either entry is enough.
			entry = overrides[j]
			i++
			j++
		} else {
			entry = shared[i]
			i++
			if entry.Kind == BrowseKindAsset {
				if _, ok := s.OverrideIndex[Key(server, entry.Path)]; ok {
					continue
				}
			}
		}
		if cursor != "" && entry.Path <= cursor {
			continue
		}
		items = append(items, entry)
	}

	result := BrowseResult{Items: items}
	if len(result.Items) > limit {
		result.NextCursor = result.Items[limit-1].Path
		result.Items = result.Items[:limit]
	}
	return result
}

type browseBuilder map[string]map[string]BrowseEntry

func newBrowseBuilder() browseBuilder {
	return browseBuilder{}
}

func (b browseBuilder) addAsset(asset BrowseEntry) {
	if asset.Path == "" {
		return
	}
	asset.Kind = BrowseKindAsset
	asset.Name = baseName(asset.Path)
	if asset.Name == "" {
		return
	}

	prefix := ""
	start := 0
	for i := 0; i < len(asset.Path); i++ {
		if asset.Path[i] != '/' {
			continue
		}
		name := asset.Path[start:i]
		if name == "" {
			return
		}
		dirPath := asset.Path[:i+1]
		b.add(prefix, BrowseEntry{
			Kind: BrowseKindDirectory,
			Name: name,
			Path: dirPath,
		})
		prefix = dirPath
		start = i + 1
	}
	b.add(prefix, asset)
}

func (b browseBuilder) add(prefix string, entry BrowseEntry) {
	entries := b[prefix]
	if entries == nil {
		entries = map[string]BrowseEntry{}
		b[prefix] = entries
	}
	entries[entry.Kind+"\x00"+entry.Path] = entry
}

func (b browseBuilder) freeze() map[string][]BrowseEntry {
	out := make(map[string][]BrowseEntry, len(b))
	for prefix, entries := range b {
		list := make([]BrowseEntry, 0, len(entries))
		for _, entry := range entries {
			list = append(list, entry)
		}
		sort.Slice(list, func(i, j int) bool {
			return browseLess(list[i], list[j])
		})
		out[prefix] = list
	}
	return out
}

func browseLess(a, b BrowseEntry) bool {
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	return a.Kind < b.Kind
}

func sameBrowseEntry(a, b BrowseEntry) bool {
	return a.Kind == b.Kind && a.Path == b.Path
}

func baseName(p string) string {
	end := len(p)
	for end > 0 && p[end-1] == '/' {
		end--
	}
	start := end
	for start > 0 && p[start-1] != '/' {
		start--
	}
	return p[start:end]
}
