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
CREATE INDEX IF NOT EXISTS idx_assets_path_fp  ON assets(path, fingerprint);
CREATE INDEX IF NOT EXISTS idx_assets_sha      ON assets(sha256);
CREATE INDEX IF NOT EXISTS idx_assets_current  ON assets(server, path);

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
`
