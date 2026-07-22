# moe-assets-gateway

Single-binary Go service that fronts SeaweedFS with two purposes:

1. **Public read-only reverse proxy** — exposes Project Sekai's five-region static
   assets under a uniform URL shape:

   ```
   https://storage.exmeaning.com/sekai-{server}-assets/{path...}
   ```

   Example: `https://storage.exmeaning.com/sekai-jp-assets/event/event_frontlines_2026/screen/bg.webp`

   Server tokens: `jp | en | tw | kr | cn`. Each request is served either from
   the deduplicated `/shared-assets/{path}` baseline or, when that region has
   a diverging artefact, from `/overrides/{server}/{path}`. The gateway keeps
   an in-memory placement index so the read path never touches SQL.

2. **Private HIP/1 TCP ingest** — a length-prefixed msgpack binary protocol
   spoken by the [Haruki Sekai Asset Updater](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater).
   One TCP session == one region's one atomic version commit. The server is
   authoritative for the SKIP / SHARED / OVERRIDE decision, streams sha256 of
   every uploaded byte, commits metadata atomically, and publishes a rebuilt
   read-path index after COMMIT.

The spec lives in [`Haruki-Sekai-Asset-Updater/docs/hip.md`](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater/blob/main/docs/hip.md).
This project's wire format is verified to match by the integration tests in
[`tests/integration_test.go`](./tests/integration_test.go).

---

## Layout

```
cmd/gateway/          # main.go, boot / signal handling
internal/config/      # 12-factor env parsing
internal/hipproto/    # HIP/1 frame + msgpack messages (no I/O)
internal/hipserver/   # TCP accept loop + per-session state machine
internal/store/       # SQLite schema, assets/versions layer, CHECK logic
internal/index/       # atomic.Pointer[Snapshot] read + browser index
internal/storage/     # SeaweedFS filer client (PUT/GET/HEAD/DELETE/Copy)
internal/httpapi/     # Read-path HTTP router + reverse proxy + rate limit
internal/metrics/     # Tiny stdlib-only Prometheus text exporter
tests/                # End-to-end integration test
deploy/               # Dockerfile + docker-compose.yml
```

---

## Quick start (docker compose)

```bash
cd deploy
docker compose up --build
```

Then, from a HIP client:

```
tcp://localhost:7420    (bearer: change-me-please)
```

And from a browser / curl:

```bash
curl -I http://localhost:8080/healthz
curl http://localhost:8080/sekai-jp-assets/event/foo/bar.webp
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `HARUKI_GW_HTTP_ADDR` | `:8080` | Read-path bind address |
| `HARUKI_GW_HIP_ADDR` | `127.0.0.1:7420` | HIP ingest bind address |
| `HARUKI_GW_HIP_TLS_CERT` | `""` | PEM cert file; empty → plaintext |
| `HARUKI_GW_HIP_TLS_KEY` | `""` | PEM key file |
| `HARUKI_GW_HIP_BEARER_TOKEN` | *(required)* | Compared against `HELLO.bearer_token` in constant time |
| `HARUKI_GW_MAX_FRAME_BYTES` | `16777216` | HIP `MAX_FRAME` (default 16 MiB) |
| `HARUKI_GW_MAX_INFLIGHT_UPLOADS` | `8` | Initial `max_in_flight_uploads` window |
| `HARUKI_GW_SEAWEED_FILER` | `http://seaweedfs-filer:8888` | SeaweedFS filer base URL |
| `HARUKI_GW_SQLITE_PATH` | `/data/gateway.db` | SQLite database path (WAL) |
| `HARUKI_GW_HTTP_RATE_LIMIT_RPS` | `200` | Per-IP token-bucket rate. `0` disables |
| `HARUKI_GW_HTTP_RATE_LIMIT_BURST` | `400` | Per-IP token-bucket burst |
| `HARUKI_GW_ALLOWED_SERVERS` | `jp,en,tw,kr,cn` | Comma-separated region whitelist |
| `HARUKI_GW_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

Startup validation:
- Bearer token must be non-empty (a session-wide reject would be worse than a crash).
- Allowed servers list must be non-empty.
- MAX_FRAME must be ≥ 4 KiB.
- SeaweedFS is pinged on boot; failure is logged but non-fatal.

---

## Read path vs HIP ingest port

|  | Read path (`:8080`) | HIP ingest (`:7420`) |
|---|---|---|
| Audience | Public (CDN / browser) | Same-pod updater sidecar |
| Protocol | HTTP/1.1 | Custom TCP + msgpack + length-prefixed frames |
| Direction | Read-only | Write-only (never fetches from SeaweedFS) |
| Auth | None (rate limited by IP) | Bearer in HELLO (constant-time compare) |
| Bind | `0.0.0.0` | `127.0.0.1` by default; require TLS if binding to `0.0.0.0` |
| Concurrency | High: fully lock-free per request | One TCP connection = one atomic version commit |

---

## HTTP API: asset browser

`GET /api/assets/browse` returns a bounded, non-recursive directory listing of
the current public assets for one region.

Example:

```bash
curl 'http://localhost:8080/api/assets/browse?server=jp&prefix=event&limit=100'
```

Query parameters:

| Name | Required | Default | Notes |
|---|---:|---|---|
| `server` | yes | - | One of the configured server tokens, e.g. `jp`, `en`, `tw`, `kr`, `cn`. |
| `prefix` | no | root | Directory prefix to list. `event` and `event/` both list `event/`. Must be relative and canonical. |
| `limit` | no | `100` | Page size. Values above `200` are capped to `200`. |
| `cursor` | no | - | Opaque cursor from `nextCursor`. URL-encode it when sending the next page. |

Response:

```json
{
  "server": "jp",
  "prefix": "event/",
  "limit": 100,
  "nextCursor": "event/foo/",
  "snapshotRevision": 12,
  "items": [
    {
      "type": "directory",
      "name": "event_frontlines_2026",
      "path": "event/event_frontlines_2026/"
    },
    {
      "type": "asset",
      "name": "logo.webp",
      "path": "event/logo.webp",
      "url": "/sekai-jp-assets/event/logo.webp",
      "source": "shared",
      "size": 12345,
      "fingerprint": "123456789",
      "sha256": "abcdef...",
      "version": "6.0.0.1"
    }
  ]
}
```

Operational constraints:

- The API reads the immutable in-memory snapshot rebuilt after HIP commits. It
  does not list SeaweedFS directories and does not query SQLite per request.
- Listings are one directory level only. There is deliberately no recursive
  full-tree dump and no public global fuzzy search endpoint.
- Results are paginated and capped at 200 items per request.
- Responses include `Cache-Control: public, max-age=30, stale-while-revalidate=60`.
  The handler also keeps a small 15-second in-process cache keyed by snapshot
  revision and query parameters.
- `snapshotRevision` changes when the gateway publishes a rebuilt index. A HIP
  COMMIT is durable before the rebuild completes, so browser API visibility can
  lag a just-committed version briefly.

---

## HTTP API: bundle browser

The bundle-oriented alternative to `/api/assets/browse`: the tree is truncated
at the bundle level, and a bundle's contents are fetched with a second call.
The frontend flow is: list a directory of bundles, then follow a bundle's
`filesUrl`.

```bash
curl 'http://localhost:8080/api/assets/bundles?server=jp&prefix=event&limit=100'
curl 'http://localhost:8080/api/assets/bundle-files?server=jp&path=event%2Ffoo'
```

### `GET /api/assets/bundles`

Query parameters are identical to `/api/assets/browse` (`server`, `prefix`,
`limit`, `cursor`). Leaves are bundles instead of files:

```json
{
  "server": "jp",
  "prefix": "event/",
  "limit": 100,
  "snapshotRevision": 12,
  "items": [
    {
      "type": "directory",
      "name": "event_frontlines_2026",
      "path": "event/event_frontlines_2026/"
    },
    {
      "type": "bundle",
      "name": "screen",
      "path": "event/screen",
      "fingerprint": "123456789",
      "fileCount": 12,
      "totalSize": 3456789,
      "source": "shared",
      "filesUrl": "/api/assets/bundle-files?server=jp&path=event%2Fscreen"
    }
  ]
}
```

`source` is `override` when the requested server currently overrides that
bundle; the bundle metadata then describes the override placement.

### `GET /api/assets/bundle-files`

| Name | Required | Default | Notes |
|---|---:|---|---|
| `server` | yes | - | One of the configured server tokens. |
| `path` | yes | - | A bundle path from the bundles endpoint. Unknown → `404`. |
| `limit` | no | `100` | Page size, capped at `200`. |
| `cursor` | no | - | Opaque file-path cursor from `nextCursor`. URL-encode it. |

```json
{
  "server": "jp",
  "bundle": {
    "path": "event/screen",
    "fingerprint": "123456789",
    "fileCount": 12,
    "totalSize": 3456789,
    "source": "shared"
  },
  "limit": 100,
  "snapshotRevision": 12,
  "items": [
    {
      "type": "asset",
      "name": "bg.webp",
      "path": "event/screen/bg.webp",
      "url": "/sekai-jp-assets/event/screen/bg.webp",
      "source": "shared",
      "size": 152341,
      "fingerprint": "123456789",
      "sha256": "abcdef...",
      "version": "6.0.0.1"
    }
  ]
}
```

Operational constraints:

- Both endpoints read the materialized `current_shared_bundles` /
  `current_override_bundles` SQLite tables, which are maintained inside the
  same COMMIT transaction as the per-file current tables — a bundle listing
  page never scans per-file rows, so it is strictly cheaper than the file
  browser for the same prefix.
- Bundle file listings are a single indexed range over one bundle's rows.
  Shared baseline and the server's overrides are merged per path with the
  override winning, exactly like the file browser.
- Pagination, the 200-item cap, the 15-second in-process cache, and
  `Cache-Control: public, max-age=30, stale-while-revalidate=60` all match
  `/api/assets/browse`.
- On first boot after upgrading, the gateway performs one full
  `EnsureReadIndexes` rebuild to backfill `bundle_path` on the current tables
  and populate the bundle tables (the read-index meta key changed).

---

## HTTP API: version history & update diff

`GET /api/assets/versions` lists the committed asset versions of one region,
newest first. `GET /api/assets/diff` returns what one version changed — the
frontend flow is: pick a version from the list, then follow its `diffUrl`.

```bash
curl 'http://localhost:8080/api/assets/versions?server=jp&limit=20'
curl 'http://localhost:8080/api/assets/diff?server=jp&version=6.0.0.11&limit=100'
```

### `GET /api/assets/versions`

| Name | Required | Default | Notes |
|---|---:|---|---|
| `server` | yes | - | One of the configured server tokens. |
| `limit` | no | `100` | Page size, capped at `200`. |
| `cursor` | no | - | Opaque numeric cursor from `nextCursor`. |

```json
{
  "server": "jp",
  "limit": 20,
  "nextCursor": "41",
  "items": [
    {
      "assetVersion": "6.0.0.11",
      "appVersion": "6.0.0",
      "assetHash": "abcd...",
      "committedAt": 1751234567,
      "changedAssets": 1234,
      "stats": {"skipped_by_layer1": 100, "uploaded_shared": 34},
      "diffUrl": "/api/assets/diff?server=jp&version=6.0.0.11"
    }
  ]
}
```

`changedAssets` counts every asset row the commit minted (all file types);
`stats` is the client-reported COMMIT stats JSON passed through verbatim.

### `GET /api/assets/diff`

| Name | Required | Default | Notes |
|---|---:|---|---|
| `server` | yes | - | One of the configured server tokens. |
| `version` | yes | - | An `assetVersion` from the versions endpoint. Unknown → `404`. |
| `action` | no | `added` | Change type filter: `added` (new files only), `updated` (replaced files only), or `all` (both). |
| `types` | no | `webp,mp3` | Comma-separated extension whitelist. By default, includes `.webp` and `.mp3` files $\ge$ 1MB (`1048576` bytes). `types=all` disables extension/size filtering. Tokens are `[a-z0-9]`, max 16. |
| `limit` | no | `100` | Page size, capped at `200`. |
| `cursor` | no | - | Opaque `size:path` cursor from `nextCursor`. URL-encode it. |

```json
{
  "server": "jp",
  "assetVersion": "6.0.0.11",
  "appVersion": "6.0.0",
  "assetHash": "abcd...",
  "committedAt": 1751234567,
  "action": "added",
  "types": ["webp", "mp3"],
  "totalChanged": 890,
  "limit": 100,
  "nextCursor": "52341:event/foo/bg.webp",
  "items": [
    {
      "changeType": "added",
      "path": "event/foo/bg.webp",
      "url": "/sekai-jp-assets/event/foo/bg.webp",
      "source": "shared",
      "size": 152341,
      "fingerprint": "123456789",
      "sha256": "abcdef...",
      "bundlePath": "event/foo"
    }
  ]
}
```

Semantics & operational constraints:

- By default, the diff of a version returns only newly added files (`changeType: "added"`, `action=added`) and restricts types to `.webp` and `.mp3` files $\ge$ 1MB. Use `action=all` or `action=updated` and `types=all` to override filters.
- Items are ordered largest-first (`size DESC`, `path ASC` for ties), so one
  page is enough to show a version's heaviest additions.
- `totalChanged` is computed with the active `action` and `types` filters applied, so it
  always matches what pagination will eventually return.
- Cross-region reuse rows (a region adopting already-uploaded shared bytes)
  report `size: 0` and no `sha256` — no bytes travelled for them.
- Both endpoints page through SQLite via the `(server, version, path)` index:
  the page query touches only the requested version's delta rows and the
  counts are covering-index-only. Responses carry the same
  `Cache-Control: public, max-age=30, stale-while-revalidate=60` and 15-second
  in-process cache as the browser API.

---

## HIP/1 wire format (summary)

Frame layout:

```
+--------------------+--------+----------------------+
|  length (u32 BE)   |  type  |       payload        |
+--------------------+--------+----------------------+
      4 bytes          1 byte   (length - 1) bytes
```

Frame types:

| Value | Name           | Direction |
|-------|----------------|-----------|
| 0x01  | `HELLO`        | C → S     |
| 0x02  | `HELLO_ACK`    | S → C     |
| 0x03  | `CHECK_BATCH`  | C → S     |
| 0x04  | `CHECK_ACK`    | S → C     |
| 0x05  | `UPLOAD_BEGIN` | C → S     |
| 0x06  | `UPLOAD_CHUNK` | C → S     |
| 0x07  | `UPLOAD_END`   | C → S     |
| 0x08  | `UPLOAD_ACK`   | S → C     |
| 0x09  | `COMMIT`       | C → S     |
| 0x0A  | `COMMIT_ACK`   | S → C     |
| 0x0B  | `BYE`          | C → S     |
| 0x0C  | `WINDOW`       | S → C     |
| 0x0E  | `PING`         | ↔         |
| 0x0F  | `PONG`         | ↔         |
| 0x1F  | `ERROR`        | ↔         |

All payloads are msgpack except `UPLOAD_CHUNK`, whose payload is
`[u32 BE stream_id][raw bytes...]`. Field names are `snake_case`, compatible
with rmp-serde `write_named` on the Rust client.

**Full spec (authoritative):** [`hip.md`](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater/blob/main/docs/hip.md).

### Server state machine

```
CLOSED ──HELLO──▶ HANDSHAKED ──CHECK/UPLOAD──▶ RUNNING ──COMMIT──▶ COMMITTING
                                                                    │
                                                       COMMIT_ACK   ▼
                                                                 FINALIZED
                                                                    │
                                                               BYE / FIN
                                                                    ▼
                                                                 CLOSED
```

Any `ERROR{fatal:true}` in either direction jumps directly to `CLOSED`.

### Server-authoritative CHECK decision

For each `(server, path, fingerprint)`:

1. Same-server same-fingerprint row exists → `SKIP`.
2. No shared baseline (no `is_override=0` row for this `path`) → `UPLOAD` + `SHARED`.
3. Shared baseline exists with same fingerprint → `SKIP` (cross-region reuse;
   COMMIT will still mint a versioned row for this server pointing at the
   existing shared key).
4. Shared baseline exists with different fingerprint → `UPLOAD` + `OVERRIDE`.

### Failure semantics

- Any disconnect before `COMMIT_ACK` discards the whole `run_id`'s state.
- A `SHA_MISMATCH` or `SIZE_MISMATCH` in a stream is per-stream: the session
  stays open, the tmp key is deleted, and the corrupt bytes never touch the
  final placement.
- The read-path index snapshot is swapped atomically **after** the SQLite
  transaction commits, so readers see either the old or the new full state,
  never a half-applied version.

---

## Security & operational notes

- HIP ingest defaults to `127.0.0.1:7420`. If you must expose it to another
  host, set `HARUKI_GW_HIP_TLS_CERT` and `HARUKI_GW_HIP_TLS_KEY` so the socket
  is wrapped in TLS 1.2+; bearer is *always* checked with `crypto/subtle`.
- Read path is public; an IP token-bucket middleware caps request rate. `X-Forwarded-For`
  is honoured (Zeabur ingress terminates TLS upstream). Set
  `HARUKI_GW_HTTP_RATE_LIMIT_RPS=0` to disable.
- All SQLite writes go through a single `BEGIN IMMEDIATE` transaction on the
  COMMIT path — no concurrent writers.
- Path safety: any client-supplied path containing `..`, backslashes, NULs,
  a leading slash, or a non-canonical form is rejected.
- Frame size hard cap: 64 MiB regardless of `HARUKI_GW_MAX_FRAME_BYTES`.

## Metrics

`GET /metrics` (Prometheus text format, no client library dependency):

- `http_requests_total{server, result}`
- `http_bytes_out_total`
- `hip_sessions_active`
- `hip_sessions_total{result}`
- `hip_uploads_total{status}`
- `hip_bytes_ingested_total`

---

## Testing

```bash
go test ./...
```

The integration test in `tests/` spins up an in-process filer mock plus the
gateway's HIP + HTTP ports, then walks a full `HELLO → CHECK_BATCH → UPLOAD →
COMMIT` session, verifies cross-region shared reuse, and covers the OVERRIDE
placement path.

---

## Architectural Decision Records

### ADR-001: No gRPC

Adding `protoc` + `.proto` codegen to the build chain would double the tool
surface for a two-service internal protocol. We also want no HTTP/2 semantics
leaking into the public read path, which is served by a plain HTTP/1
`http.Server`. HIP/1 is a straightforward length-prefixed binary protocol
that fits in ~300 lines of Go.

### ADR-002: `fingerprint` is Unity CRC32, not sha256

`AbCacheEntry` (both ColorfulPalette and Nuverse variants) already carries a
CRC32. Requiring clients to sha256 every bundle just to know whether to send
it would force a full-file read even for artefacts we'll ultimately SKIP.

### ADR-003: Server always computes sha256 anyway

The bytes stream through the gateway once. Piping them through
`sha256.Hash.Write` during upload costs less than the network I/O itself and
buys us two things: transport integrity checking, and true cross-provider
dedup at the storage layer.

### ADR-004: HIP is TCP+msgpack, not HTTP

The workload wants multi-plexed uploads inside one long-lived session, per-item
server-authoritative decisions issued mid-stream, and one connection ↔ one
atomic version commit. HTTP would need SSE / WebSocket / trailers / chunk
extensions to approximate the same, at more code than a bespoke binary frame
format.

### ADR-005: SQLite instead of Postgres

Single writer (one COMMIT tx at a time), single container, single 512 GB
NVMe. WAL mode gives concurrent readers zero contention with the writer. The
whole schema is two tables. Adding a Postgres process would only add ops
surface.

### ADR-006: In-memory index rebuilt on every COMMIT

The read path is by far the hotter side (many thousand req/s from the CDN
edge). Serving each request through SQL would cache-warm the DB but still
lose to a plain `map[string]…` behind an `atomic.Pointer`. Rebuild happens
once per COMMIT (i.e. per region per version bump), which is rare.
#   M o e _ A s e e t s _ N E X T  
 
