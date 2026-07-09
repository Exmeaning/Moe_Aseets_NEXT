package store

// Schema is the full DDL. Applied idempotently on Open().
const Schema = `
CREATE TABLE IF NOT EXISTS assets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    server        TEXT NOT NULL,
    bundle_path   TEXT NOT NULL,
    path          TEXT NOT NULL,
    version       TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    size          INTEGER NOT NULL,
    is_override   INTEGER NOT NULL,
    storage_key   TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    UNIQUE(server, path, version)
);
CREATE INDEX IF NOT EXISTS idx_assets_path_fp   ON assets(path, fingerprint);
CREATE INDEX IF NOT EXISTS idx_assets_bundle_fp ON assets(bundle_path, fingerprint);
CREATE INDEX IF NOT EXISTS idx_assets_sha       ON assets(sha256);
CREATE INDEX IF NOT EXISTS idx_assets_current   ON assets(server, path);

CREATE TABLE IF NOT EXISTS current_assets (
    server      TEXT NOT NULL,
    path        TEXT NOT NULL,
    version     TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    is_override INTEGER NOT NULL,
    storage_key TEXT NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(server, path)
);
CREATE INDEX IF NOT EXISTS idx_current_assets_path
    ON current_assets(path);

CREATE TABLE IF NOT EXISTS current_shared_assets (
    path        TEXT PRIMARY KEY,
    version     TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    storage_key TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS read_index_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS versions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    server        TEXT NOT NULL,
    app_version   TEXT NOT NULL,
    asset_version TEXT NOT NULL,
    asset_hash    TEXT NOT NULL,
    bundle_count  INTEGER NOT NULL,
    committed_at  INTEGER NOT NULL,
    stats_json    TEXT NOT NULL,
    UNIQUE(server, asset_version, asset_hash)
);

CREATE TABLE IF NOT EXISTS bundle_completions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id    INTEGER NOT NULL,
    server        TEXT NOT NULL,
    asset_version TEXT NOT NULL,
    asset_hash    TEXT NOT NULL,
    bundle_path   TEXT NOT NULL,
    fingerprint   TEXT NOT NULL,
    source        TEXT NOT NULL,
    completed_at  INTEGER NOT NULL,
    UNIQUE(server, asset_version, asset_hash, bundle_path)
);
CREATE INDEX IF NOT EXISTS idx_bundle_completions_lookup
    ON bundle_completions(server, bundle_path, fingerprint);
CREATE INDEX IF NOT EXISTS idx_bundle_completions_bundle_fp
    ON bundle_completions(bundle_path, fingerprint);
CREATE INDEX IF NOT EXISTS idx_bundle_completions_version
    ON bundle_completions(version_id);
`
