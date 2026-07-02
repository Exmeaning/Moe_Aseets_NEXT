// Package tests contains the end-to-end integration test that spins up:
//   - an in-process SeaweedFS filer mock (httptest.Server implementing
//     PUT/GET/HEAD/DELETE against a map)
//   - the gateway's HIP TCP server + HTTP proxy
//   - a mini HIP client that walks HELLO → CHECK → UPLOAD → COMMIT
//
// It verifies:
//   - CHECK treats path as bundle_path (the updater has not exported files yet)
//   - UPLOAD stores bundle_path + asset_path as the public path, and a
//     subsequent read via /sekai-{server}-assets/{bundle_path}/{asset_path}
//     serves the exact bytes with X-Serve-From=shared
//   - A same-path different-fp upload from another region gets OVERRIDE and
//     is served with X-Serve-From=override
//   - A deliberate sha256 mismatch is caught server-side.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/hipproto"
	"github.com/Team-Haruki/moe-assets-gateway/internal/hipserver"
	"github.com/Team-Haruki/moe-assets-gateway/internal/httpapi"
	"github.com/Team-Haruki/moe-assets-gateway/internal/index"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

// ---- Seaweed mock ----

type filerMock struct {
	mu    sync.Mutex
	files map[string][]byte
	ctype map[string]string
}

func newFilerMock() *filerMock {
	return &filerMock{files: map[string][]byte{}, ctype: map[string]string{}}
}

func (f *filerMock) handle(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		f.mu.Lock()
		f.files[key] = body
		if ct := r.Header.Get("Content-Type"); ct != "" {
			f.ctype[key] = ct
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		f.mu.Lock()
		body, ok := f.files[key]
		ct := f.ctype[key]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.Write(body)
	case http.MethodHead:
		f.mu.Lock()
		body, ok := f.files[key]
		ct := f.ctype[key]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		if ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	case http.MethodDelete:
		f.mu.Lock()
		// Delete supports both exact and prefix (for /tmp/{run_id}) removal.
		for k := range f.files {
			if k == key || strings.HasPrefix(k, key+"/") {
				delete(f.files, k)
				delete(f.ctype, k)
			}
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method", 405)
	}
}

// ---- HIP mini client (uses internal/hipproto directly) ----

type hipClient struct {
	conn     net.Conn
	maxFrame uint64
}

func dial(t *testing.T, addr string) *hipClient {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &hipClient{conn: c, maxFrame: hipproto.DefaultMaxFrameBytes}
}

func (c *hipClient) send(t *testing.T, typ hipproto.FrameType, v any) {
	t.Helper()
	var payload []byte
	if v != nil {
		b, err := hipproto.Encode(v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		payload = b
	}
	if err := hipproto.WriteFrame(c.conn, hipproto.Frame{Type: typ, Payload: payload}, c.maxFrame); err != nil {
		t.Fatalf("write %s: %v", typ, err)
	}
}

func (c *hipClient) sendChunk(t *testing.T, streamID uint32, data []byte) {
	t.Helper()
	payload := hipproto.EncodeChunk(streamID, data)
	if err := hipproto.WriteFrame(c.conn, hipproto.Frame{Type: hipproto.FrameUploadChunk, Payload: payload}, c.maxFrame); err != nil {
		t.Fatalf("write chunk: %v", err)
	}
}

// recv reads until it gets a frame of type want, discarding PINGs it needs to answer.
func (c *hipClient) recv(t *testing.T, want hipproto.FrameType) hipproto.Frame {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	_ = c.conn.SetReadDeadline(deadline)
	for {
		f, err := hipproto.ReadFrame(c.conn, c.maxFrame)
		if err != nil {
			t.Fatalf("read frame (want %s): %v", want, err)
		}
		if f.Type == hipproto.FramePing {
			_ = hipproto.WriteFrame(c.conn, hipproto.Frame{Type: hipproto.FramePong}, c.maxFrame)
			continue
		}
		if f.Type != want {
			t.Fatalf("got frame %s, want %s", f.Type, want)
		}
		return f
	}
}

// ---- fixture ----

type fixture struct {
	httpBase string
	hipAddr  string
	filer    *filerMock
}

func setUp(t *testing.T) *fixture {
	t.Helper()

	// SeaweedFS mock
	filer := newFilerMock()
	filerSrv := httptest.NewServer(http.HandlerFunc(filer.handle))
	t.Cleanup(filerSrv.Close)

	// SQLite in-memory
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	sc := storage.New(filerSrv.URL)
	idx := index.New()
	if err := idx.Rebuild(context.Background(), db); err != nil {
		t.Fatalf("rebuild idx: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	// HIP server on an ephemeral port
	hipSrv := hipserver.New(hipserver.Config{
		Addr:               "127.0.0.1:0",
		BearerToken:        "test-token",
		MaxFrame:           hipproto.DefaultMaxFrameBytes,
		MaxInFlightUploads: 4,
		AllowedServers:     map[string]struct{}{"jp": {}, "en": {}, "tw": {}, "kr": {}, "cn": {}},
		ServerVersion:      "test-gw/1",
	}, db, sc, idx, nil, log)

	hipCtx, hipCancel := context.WithCancel(context.Background())
	go func() { _ = hipSrv.ListenAndServe(hipCtx) }()
	// wait for listener
	deadline := time.Now().Add(2 * time.Second)
	for hipSrv.Addr() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hipSrv.Addr() == nil {
		t.Fatal("hip server did not bind")
	}
	t.Cleanup(func() {
		hipCancel()
		_ = hipSrv.Shutdown()
	})

	// HTTP read path on an ephemeral port
	proxy := &httpapi.ProxyHandler{
		Idx:            idx,
		Storage:        sc,
		Log:            log,
		AllowedServers: map[string]struct{}{"jp": {}, "en": {}, "tw": {}, "kr": {}, "cn": {}},
	}
	router := &httpapi.Router{Proxy: proxy}
	httpSrv := httptest.NewServer(router.Handler())
	t.Cleanup(httpSrv.Close)

	return &fixture{
		httpBase: httpSrv.URL,
		hipAddr:  hipSrv.Addr().String(),
		filer:    filer,
	}
}

// ---- helper: run a full session that uploads (path, bytes) for (region, version) ----

type uploadItem struct {
	path        string
	bundlePath  string
	fingerprint string
	body        []byte
	// mutateSha: if true, send an intentionally bogus sha256 in UPLOAD_END.
	corruptSha bool
	// wantAction: expected CHECK_ACK.action ("SKIP" or "UPLOAD")
	wantAction string
	// wantPlacement (only when wantAction=="UPLOAD"): "SHARED" or "OVERRIDE"
	wantPlacement string
	// wantUploadStatus: expected UPLOAD_ACK.status ("OK", "SHA_MISMATCH", ...)
	wantUploadStatus string
}

func runSession(t *testing.T, fx *fixture, region, appVersion, assetVersion, assetHash string, items []uploadItem) {
	t.Helper()
	c := dial(t, fx.hipAddr)

	// HELLO
	c.send(t, hipproto.FrameHello, &hipproto.Hello{
		Proto:            "hip",
		Version:          1,
		BearerToken:      "test-token",
		Region:           region,
		AppVersion:       appVersion,
		AssetVersion:     assetVersion,
		AssetHash:        assetHash,
		RunID:            fmt.Sprintf("run-%s-%d", region, time.Now().UnixNano()),
		UnpackerVersion:  "test/0.1",
		ExpectedMaxFrame: hipproto.DefaultMaxFrameBytes,
	})
	ack := c.recv(t, hipproto.FrameHelloAck)
	var helloAck hipproto.HelloAck
	if err := hipproto.Decode(ack.Payload, &helloAck); err != nil {
		t.Fatalf("decode helloack: %v", err)
	}
	if !helloAck.Sha256Required {
		t.Fatalf("expected sha256_required=true")
	}

	// CHECK_BATCH
	batch := hipproto.CheckBatch{BatchID: 1}
	for _, it := range items {
		batch.Items = append(batch.Items, hipproto.CheckBatchItem{
			Path:        it.bundlePath,
			Fingerprint: it.fingerprint,
			Size:        uint64(len(it.body)),
			Provider:    region,
		})
	}
	c.send(t, hipproto.FrameCheckBatch, &batch)
	ckAck := c.recv(t, hipproto.FrameCheckAck)
	var ckResult hipproto.CheckResult
	if err := hipproto.Decode(ckAck.Payload, &ckResult); err != nil {
		t.Fatalf("decode checkack: %v", err)
	}
	if len(ckResult.Results) != len(items) {
		t.Fatalf("check results len: %d", len(ckResult.Results))
	}
	for i, r := range ckResult.Results {
		it := items[i]
		if r.Action != it.wantAction {
			t.Fatalf("item[%d] action=%q, want %q (path=%q)", i, r.Action, it.wantAction, it.path)
		}
		if it.wantAction == hipproto.ActionUpload && r.Placement != it.wantPlacement {
			t.Fatalf("item[%d] placement=%q, want %q", i, r.Placement, it.wantPlacement)
		}
	}

	// UPLOAD for each item whose CHECK said UPLOAD
	stats := hipproto.CommitStats{}
	stream := uint32(1)
	for i, it := range items {
		if items[i].wantAction != hipproto.ActionUpload {
			stats.SkippedByCheck++
			continue
		}
		// UPLOAD_BEGIN
		c.send(t, hipproto.FrameUploadBegin, &hipproto.UploadBegin{
			StreamID:    stream,
			BundlePath:  it.bundlePath,
			Path:        it.path,
			Fingerprint: it.fingerprint,
			Size:        uint64(len(it.body)),
			ContentType: "application/octet-stream",
		})
		// UPLOAD_CHUNK (in 2 pieces just to exercise multi-chunk)
		half := len(it.body) / 2
		if half > 0 {
			c.sendChunk(t, stream, it.body[:half])
		}
		c.sendChunk(t, stream, it.body[half:])
		// UPLOAD_END
		sum := sha256.Sum256(it.body)
		shaHex := hex.EncodeToString(sum[:])
		if it.corruptSha {
			shaHex = strings.Repeat("0", 64)
		}
		c.send(t, hipproto.FrameUploadEnd, &hipproto.UploadEnd{
			StreamID:   stream,
			TotalBytes: uint64(len(it.body)),
			Sha256:     shaHex,
		})
		upAckFrame := c.recv(t, hipproto.FrameUploadAck)
		var upAck hipproto.UploadAck
		if err := hipproto.Decode(upAckFrame.Payload, &upAck); err != nil {
			t.Fatalf("decode upack: %v", err)
		}
		if upAck.Status != it.wantUploadStatus {
			t.Fatalf("item[%d] upload status=%q, want %q, msg=%q", i, upAck.Status, it.wantUploadStatus, upAck.Message)
		}
		if upAck.Status == hipproto.UploadStatusOK {
			if it.wantPlacement == hipproto.PlacementShared {
				stats.UploadedShared++
			} else {
				stats.UploadedOverride++
			}
		}
		stream++
	}

	// COMMIT
	c.send(t, hipproto.FrameCommit, &hipproto.Commit{
		BundleCount: uint64(len(items)),
		Stats:       stats,
	})
	commitAck := c.recv(t, hipproto.FrameCommitAck)
	var cAck hipproto.CommitAck
	if err := hipproto.Decode(commitAck.Payload, &cAck); err != nil {
		t.Fatalf("decode commitack: %v", err)
	}
	if !cAck.OverrideIndexRebuilt {
		t.Fatalf("override_index_rebuilt=false")
	}
	c.send(t, hipproto.FrameBye, nil)
	// Give server a moment to close
	time.Sleep(50 * time.Millisecond)
}

// ---- tests ----

func TestEndToEndSharedThenOverride(t *testing.T) {
	fx := setUp(t)

	body1 := []byte("hello jp world " + strings.Repeat("x", 1024))
	body2 := []byte("EN OVERRIDE variant " + strings.Repeat("y", 512))

	const bundlePath = "character/member_small/res026_no048"
	const assetPath = "card_normal.webp"
	const publicPath = bundlePath + "/" + assetPath

	// JP session: uploads a fresh shared asset.
	runSession(t, fx, "jp", "6.0.0", "6.0.0.1", "hash-jp-1", []uploadItem{
		{
			path:             assetPath,
			bundlePath:       bundlePath,
			fingerprint:      "111",
			body:             body1,
			wantAction:       hipproto.ActionUpload,
			wantPlacement:    hipproto.PlacementShared,
			wantUploadStatus: hipproto.UploadStatusOK,
		},
	})

	// Read from JP using the real public path: bundle_path + asset_path.
	respBody, headers := httpGet(t, fx.httpBase+"/sekai-jp-assets/"+publicPath)
	if !bytes.Equal(respBody, body1) {
		t.Fatalf("body mismatch, got %d bytes want %d", len(respBody), len(body1))
	}
	if headers.Get("X-Serve-From") != "shared" {
		t.Fatalf("X-Serve-From=%q, want shared", headers.Get("X-Serve-From"))
	}
	if headers.Get("ETag") != `"111"` {
		t.Fatalf("ETag=%q", headers.Get("ETag"))
	}

	// Cross-region SKIP+reuse: EN checks same bundle fp → should get SKIP.
	runSession(t, fx, "en", "6.0.0", "6.0.0.1", "hash-en-1", []uploadItem{
		{
			path:        assetPath,
			bundlePath:  bundlePath,
			fingerprint: "111",
			body:        body1,
			wantAction:  hipproto.ActionSkip,
		},
	})
	// EN read must still succeed and serve the shared bytes.
	respBody, headers = httpGet(t, fx.httpBase+"/sekai-en-assets/"+publicPath)
	if !bytes.Equal(respBody, body1) {
		t.Fatalf("en shared reuse body mismatch")
	}
	if headers.Get("X-Serve-From") != "shared" {
		t.Fatalf("en shared reuse serve-from=%q", headers.Get("X-Serve-From"))
	}

	// EN OVERRIDE: same public path, different fp.
	runSession(t, fx, "en", "6.0.0", "6.0.0.2", "hash-en-2", []uploadItem{
		{
			path:             assetPath,
			bundlePath:       bundlePath,
			fingerprint:      "222",
			body:             body2,
			wantAction:       hipproto.ActionUpload,
			wantPlacement:    hipproto.PlacementOverride,
			wantUploadStatus: hipproto.UploadStatusOK,
		},
	})

	respBody, headers = httpGet(t, fx.httpBase+"/sekai-en-assets/"+publicPath)
	if !bytes.Equal(respBody, body2) {
		t.Fatalf("en override body mismatch")
	}
	if headers.Get("X-Serve-From") != "override" {
		t.Fatalf("en override serve-from=%q", headers.Get("X-Serve-From"))
	}
	if headers.Get("ETag") != `"222"` {
		t.Fatalf("en override ETag=%q", headers.Get("ETag"))
	}

	// JP path must remain shared (unchanged).
	_, headers = httpGet(t, fx.httpBase+"/sekai-jp-assets/"+publicPath)
	if headers.Get("X-Serve-From") != "shared" {
		t.Fatalf("jp after en override serve-from=%q", headers.Get("X-Serve-From"))
	}

	// Regression guard for the production bug: the asset must not be indexed or
	// stored under the bare asset filename.
	code, missHeaders := httpGetCode(t, fx.httpBase+"/sekai-jp-assets/"+assetPath)
	if code != 404 || missHeaders.Get("X-Miss") != "not-indexed" {
		t.Fatalf("bare asset path should miss, got code=%d X-Miss=%q", code, missHeaders.Get("X-Miss"))
	}
}

func TestShaMismatchRejected(t *testing.T) {
	fx := setUp(t)
	body := []byte("payload that will be corrupted at end frame")

	runSession(t, fx, "jp", "6.0.0", "6.0.0.9", "hash-corrupt", []uploadItem{
		{
			path:             "test/corrupt.bin",
			bundlePath:       "bundle/corrupt",
			fingerprint:      "999",
			body:             body,
			corruptSha:       true,
			wantAction:       hipproto.ActionUpload,
			wantPlacement:    hipproto.PlacementShared,
			wantUploadStatus: hipproto.UploadStatusShaMismatch,
		},
	})

	// A GET should 404 because the corrupt upload was rejected before COMMIT
	// mutated the assets table. But COMMIT still ran for zero good rows —
	// which is legal (no assets to commit); index has no entry.
	code, _ := httpGetCode(t, fx.httpBase+"/sekai-jp-assets/bundle/corrupt/test/corrupt.bin")
	if code != 404 {
		t.Fatalf("expected 404 after corrupt, got %d", code)
	}
}

func TestReadPathUnknownRegion(t *testing.T) {
	fx := setUp(t)
	code, _ := httpGetCode(t, fx.httpBase+"/sekai-xx-assets/foo")
	if code != 404 {
		t.Fatalf("expected 404 for unknown region, got %d", code)
	}
}

// ---- HTTP helpers ----

func httpGet(t *testing.T, url string) ([]byte, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return body, resp.Header
}

func httpGetCode(t *testing.T, url string) (int, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return 0, nil
		}
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header
}

// TestMain lets us disable logs from stdlib.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	os.Exit(m.Run())
}
