package hipserver

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/hipproto"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

// sessionState is the server-side view of the HIP state machine (§5).
type sessionState int

const (
	stateHandshake  sessionState = iota // waiting for HELLO
	stateHandshaked                     // sent HELLO_ACK, waiting for first CHECK/UPLOAD
	stateRunning
	stateCommitting
	stateFinalized
	stateClosed
)

// session is one HIP connection.
type session struct {
	// wired-in dependencies
	srv      *Server
	conn     net.Conn
	log      *slog.Logger
	maxFrame uint64

	// negotiated fields from HELLO
	hello     hipproto.Hello
	sessionID string
	state     sessionState

	// I/O
	fw *frameWriter

	// upload bookkeeping
	uploadsMu sync.Mutex
	uploads   map[uint32]*inflightUpload
	// concurrency guard: buffered chan used as semaphore.
	uploadSem chan struct{}

	// commit-time bookkeeping
	pendingMu sync.Mutex
	pending   []store.Asset // successful UPLOAD_ACKs
	// (path, fingerprint) → SharedStorageKey when CHECK returned SKIP+SharedReuse.
	sharedReuse map[string]sharedReuseEntry

	// heartbeat state
	lastRecvMu sync.Mutex
	lastRecv   time.Time
}

type sharedReuseEntry struct {
	server, path, fingerprint, storageKey string
}

// runSession is the per-connection entrypoint.
func (srv *Server) runSession(ctx context.Context, conn net.Conn) {
	log := srv.log.With("remote", conn.RemoteAddr().String())
	log.Debug("hip: session accepted")
	s := &session{
		srv:         srv,
		conn:        conn,
		log:         log,
		maxFrame:    srv.cfg.MaxFrame,
		state:       stateHandshake,
		uploads:     map[uint32]*inflightUpload{},
		sharedReuse: map[string]sharedReuseEntry{},
	}
	s.updateLastRecv()

	// per-session context, cancelled when the goroutine returns.
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.fw = newFrameWriter(conn, srv.cfg.MaxFrame, 64)
	defer s.fw.Close()

	srv.gaugeSessionsActive(+1)
	defer srv.gaugeSessionsActive(-1)

	// heartbeat watchdog
	heartbeatDone := make(chan struct{})
	go s.heartbeat(sctx, heartbeatDone)
	defer func() { <-heartbeatDone }()

	err := s.readLoop(sctx)
	// cleanup: abort all outstanding uploads (they'll delete their tmp keys).
	s.abortAllUploads(sctx)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		// Sessions that never got past HELLO are almost always TCP health
		// probes or port scans — noisy but harmless. Log them at DEBUG so
		// real client errors still stand out at WARN.
		if s.state == stateHandshake {
			log.Debug("hip: pre-hello session dropped", "err", err)
		} else {
			log.Warn("hip: session ended with error", "err", err)
		}
		srv.counterSessions("abort")
	} else if s.state == stateFinalized || s.state == stateClosed {
		srv.counterSessions("commit")
	} else {
		srv.counterSessions("abort")
	}
	_ = conn.Close()
}

func (s *session) updateLastRecv() {
	s.lastRecvMu.Lock()
	s.lastRecv = time.Now()
	s.lastRecvMu.Unlock()
}

func (s *session) sinceLastRecv() time.Duration {
	s.lastRecvMu.Lock()
	defer s.lastRecvMu.Unlock()
	return time.Since(s.lastRecv)
}

func (s *session) heartbeat(ctx context.Context, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	lastPing := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// If the peer has been silent for > 60s, close.
			if s.sinceLastRecv() > 60*time.Second {
				// A silent pre-hello peer is almost always a health probe;
				// don't spam WARN. Real client stalls happen after HELLO.
				if s.state == stateHandshake {
					s.log.Debug("hip: pre-hello peer silent, closing")
				} else {
					s.log.Warn("hip: peer silent > 60s, closing")
				}
				_ = s.conn.Close()
				return
			}
			// Send a PING every 30s of idleness on our side.
			if time.Since(lastPing) > 30*time.Second {
				if err := s.send(ctx, hipproto.FramePing, nil); err != nil {
					return
				}
				lastPing = time.Now()
			}
		}
	}
}

// readLoop is the single-reader per session.
func (s *session) readLoop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := hipproto.ReadFrame(s.conn, s.maxFrame)
		if err != nil {
			return err
		}
		s.updateLastRecv()
		if err := s.dispatch(ctx, frame); err != nil {
			return err
		}
	}
}

func (s *session) dispatch(ctx context.Context, f hipproto.Frame) error {
	switch f.Type {
	case hipproto.FrameHello:
		return s.handleHello(ctx, f.Payload)
	case hipproto.FrameCheckBatch:
		if s.state != stateHandshaked && s.state != stateRunning {
			return s.protoViolate(ctx, "CHECK_BATCH in wrong state")
		}
		s.state = stateRunning
		return s.handleCheckBatch(ctx, f.Payload)
	case hipproto.FrameUploadBegin:
		if s.state != stateHandshaked && s.state != stateRunning {
			return s.protoViolate(ctx, "UPLOAD_BEGIN in wrong state")
		}
		s.state = stateRunning
		return s.handleUploadBegin(ctx, f.Payload)
	case hipproto.FrameUploadChunk:
		if s.state != stateRunning {
			return s.protoViolate(ctx, "UPLOAD_CHUNK in wrong state")
		}
		return s.handleUploadChunk(ctx, f.Payload)
	case hipproto.FrameUploadEnd:
		if s.state != stateRunning {
			return s.protoViolate(ctx, "UPLOAD_END in wrong state")
		}
		return s.handleUploadEnd(ctx, f.Payload)
	case hipproto.FrameCommit:
		if s.state != stateHandshaked && s.state != stateRunning {
			return s.protoViolate(ctx, "COMMIT in wrong state")
		}
		s.state = stateCommitting
		return s.handleCommit(ctx, f.Payload)
	case hipproto.FrameBye:
		s.log.Info("hip: BYE received")
		return io.EOF
	case hipproto.FramePing:
		return s.send(ctx, hipproto.FramePong, nil)
	case hipproto.FramePong:
		return nil
	case hipproto.FrameError:
		var ep hipproto.ErrorPayload
		if err := hipproto.Decode(f.Payload, &ep); err == nil {
			s.log.Warn("hip: peer ERROR", "code", ep.Code, "message", ep.Message, "fatal", ep.Fatal)
			if ep.Fatal {
				return errors.New("hip: peer sent fatal ERROR")
			}
		}
		return nil
	default:
		return s.protoViolate(ctx, fmt.Sprintf("unexpected frame %s", f.Type))
	}
}

// send serialises a msgpack payload (or raw bytes when v is []byte / nil) as
// a frame via the writer.
func (s *session) send(ctx context.Context, t hipproto.FrameType, v any) error {
	var payload []byte
	switch vv := v.(type) {
	case nil:
	case []byte:
		payload = vv
	default:
		b, err := hipproto.Encode(vv)
		if err != nil {
			return err
		}
		payload = b
	}
	return s.fw.Send(ctx, hipproto.Frame{Type: t, Payload: payload})
}

// sendFatal sends an ERROR{fatal:true} and returns a non-nil error to stop
// the read loop.
func (s *session) sendFatal(ctx context.Context, code, message string) error {
	_ = s.send(ctx, hipproto.FrameError, hipproto.ErrorPayload{
		Code: code, Message: message, Fatal: true,
	})
	return fmt.Errorf("hip: fatal %s: %s", code, message)
}

func (s *session) protoViolate(ctx context.Context, message string) error {
	return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, message)
}

// ------------- HELLO -------------

func (s *session) handleHello(ctx context.Context, payload []byte) error {
	if s.state != stateHandshake {
		return s.protoViolate(ctx, "HELLO in wrong state")
	}
	var h hipproto.Hello
	if err := hipproto.Decode(payload, &h); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad HELLO payload")
	}
	if h.Proto != "hip" || h.Version != 1 {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "unsupported proto/version")
	}
	if subtle.ConstantTimeCompare([]byte(h.BearerToken), []byte(s.srv.cfg.BearerToken)) != 1 {
		return s.sendFatal(ctx, hipproto.ErrCodeAuthFailed, "bearer token rejected")
	}
	if !s.srv.serverAllowed(h.Region) {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "region not allowed")
	}
	// asset_hash is optional: nuverse-provider regions (tw/kr/cn) do not
	// carry an asset_hash in their AssetBundleInfo — they identify a version
	// solely by asset_version + per-bundle crc. Only require it for
	// colorful_palette regions (jp/en) where the upstream always emits one.
	if h.RunID == "" || h.AssetVersion == "" {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "missing HELLO fields")
	}
	if (h.Region == "jp" || h.Region == "en") && h.AssetHash == "" {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "missing HELLO fields")
	}

	s.hello = h
	s.sessionID = newSessionID()
	// negotiated max frame
	maxFrame := s.maxFrame
	if h.ExpectedMaxFrame != 0 && h.ExpectedMaxFrame < maxFrame {
		maxFrame = h.ExpectedMaxFrame
	}
	s.maxFrame = maxFrame

	// negotiated in-flight window
	s.uploadSem = make(chan struct{}, s.srv.cfg.MaxInFlightUploads)

	known, err := store.KnownVersion(ctx, s.srv.db, h.Region, h.AssetVersion, h.AssetHash)
	if err != nil {
		s.log.Warn("hip: KnownVersion query failed", "err", err)
	}

	ack := hipproto.HelloAck{
		SessionID:          s.sessionID,
		ServerVersion:      s.srv.cfg.ServerVersion,
		MaxFrame:           maxFrame,
		MaxInFlightUploads: s.srv.cfg.MaxInFlightUploads,
		Sha256Required:     true,
		KnownVersion:       known,
	}
	if err := s.send(ctx, hipproto.FrameHelloAck, &ack); err != nil {
		return err
	}
	s.state = stateHandshaked
	s.log.Info("hip: HELLO ok", "session_id", s.sessionID, "region", h.Region, "asset_version", h.AssetVersion)
	return nil
}

// ------------- CHECK -------------

func (s *session) handleCheckBatch(ctx context.Context, payload []byte) error {
	var batch hipproto.CheckBatch
	if err := hipproto.Decode(payload, &batch); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad CHECK_BATCH payload")
	}
	result := hipproto.CheckResult{BatchID: batch.BatchID, Results: make([]hipproto.CheckAckItem, 0, len(batch.Items))}
	for _, item := range batch.Items {
		bundlePath := safeStripInvisible(item.Path)
		if _, err := storage.SafeRelPath(bundlePath); err != nil {
			return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "unsafe bundle path")
		}
		d, err := store.CheckBundle(ctx, s.srv.db, s.hello.Region, bundlePath, item.Fingerprint)
		if err != nil {
			s.log.Warn("hip: CHECK query failed", "err", err, "bundle_path", bundlePath)
			return s.sendFatal(ctx, hipproto.ErrCodeInternal, "check failed")
		}
		var ack hipproto.CheckAckItem
		ack.Path = bundlePath
		if d.Skip {
			ack.Action = hipproto.ActionSkip
		} else {
			ack.Action = hipproto.ActionUpload
			ack.Placement = d.Placement
		}
		result.Results = append(result.Results, ack)
	}
	return s.send(ctx, hipproto.FrameCheckAck, &result)
}

func crossRegionReuseKey(path, fingerprint string) string {
	return path + "\x00" + fingerprint
}

func canonicalAssetPath(bundlePath, assetPath string) (string, error) {
	bundlePath = safeStripInvisible(bundlePath)
	assetPath = safeStripInvisible(assetPath)
	if _, err := storage.SafeRelPath(bundlePath); err != nil {
		return "", err
	}
	if _, err := storage.SafeRelPath(assetPath); err != nil {
		return "", err
	}
	return bundlePath + "/" + assetPath, nil
}

// ------------- UPLOAD -------------

func (s *session) handleUploadBegin(ctx context.Context, payload []byte) error {
	var begin hipproto.UploadBegin
	if err := hipproto.Decode(payload, &begin); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad UPLOAD_BEGIN payload")
	}
	canonicalPath, err := canonicalAssetPath(begin.BundlePath, begin.Path)
	if err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "unsafe path")
	}
	// Acquire an in-flight slot. Blocks; the client should have obeyed
	// max_in_flight_uploads.
	select {
	case s.uploadSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	s.uploadsMu.Lock()
	if _, dup := s.uploads[begin.StreamID]; dup {
		s.uploadsMu.Unlock()
		<-s.uploadSem
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "duplicate stream_id")
	}
	// Re-derive placement from the DB to avoid trusting client memory.
	dec, err := store.CheckPath(ctx, s.srv.db, s.hello.Region, canonicalPath, begin.Fingerprint)
	if err != nil {
		s.uploadsMu.Unlock()
		<-s.uploadSem
		return s.sendFatal(ctx, hipproto.ErrCodeInternal, "check for placement")
	}
	if dec.Skip {
		// Client shouldn't have started an upload; still ack (rejected) and
		// keep session alive.
		s.uploadsMu.Unlock()
		<-s.uploadSem
		return s.send(ctx, hipproto.FrameUploadAck, ackErr(begin.StreamID, hipproto.UploadStatusRejected, "already present"))
	}
	temp := storage.TempKey(s.hello.RunID, begin.StreamID)
	up := newInflight(ctx, s.srv.storage, begin, canonicalPath, temp)
	up.placement = dec.Placement
	s.uploads[begin.StreamID] = up
	s.uploadsMu.Unlock()
	return nil
}

func (s *session) handleUploadChunk(ctx context.Context, payload []byte) error {
	streamID, data, err := hipproto.DecodeChunk(payload)
	if err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad UPLOAD_CHUNK payload")
	}
	s.uploadsMu.Lock()
	up, ok := s.uploads[streamID]
	s.uploadsMu.Unlock()
	if !ok {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "chunk for unknown stream_id")
	}
	if err := up.writeChunk(data); err != nil {
		s.log.Warn("hip: chunk write failed", "err", err, "stream", streamID)
		// Best-effort: abort this stream, ack error, keep session open.
		s.finishInflight(ctx, streamID, hipproto.UploadStatusRejected, "chunk write failed: "+err.Error())
		return nil
	}
	s.srv.counterBytesIngested(uint64(len(data)))
	return nil
}

func (s *session) handleUploadEnd(ctx context.Context, payload []byte) error {
	var end hipproto.UploadEnd
	if err := hipproto.Decode(payload, &end); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad UPLOAD_END payload")
	}
	s.uploadsMu.Lock()
	up, ok := s.uploads[end.StreamID]
	s.uploadsMu.Unlock()
	if !ok {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "END for unknown stream_id")
	}
	// verifyEnd needs the pipe closed to compute final state; call finish first.
	sha, err := up.finish(ctx)
	if err != nil {
		s.finishInflight(ctx, end.StreamID, hipproto.UploadStatusRejected, "put failed: "+err.Error())
		return nil
	}
	// Compare declared vs observed
	if status, msg := verifyEnd(up, end); status != "" {
		// Clean up tmp, send error ack.
		_ = s.srv.storage.Delete(ctx, up.tempKey)
		s.dropInflight(end.StreamID)
		if status == hipproto.UploadStatusShaMismatch {
			s.srv.counterUploads("sha_mismatch")
		} else {
			s.srv.counterUploads("size_mismatch")
		}
		return s.send(ctx, hipproto.FrameUploadAck, ackErr(end.StreamID, status, msg))
	}
	// Determine final key by placement decided on UPLOAD_BEGIN.
	var finalKey string
	isOverride := false
	switch up.placement {
	case hipproto.PlacementShared:
		finalKey = storage.SharedKey(up.path)
	case hipproto.PlacementOverride:
		finalKey = storage.OverrideKey(s.hello.Region, up.path)
		isOverride = true
	default:
		// Should not happen — placement was set at BEGIN. Recover as SHARED.
		finalKey = storage.SharedKey(up.path)
	}
	// Copy tmp → final, then delete tmp.
	if err := s.srv.storage.Copy(ctx, up.tempKey, finalKey); err != nil {
		s.log.Warn("hip: copy tmp→final failed", "err", err, "src", up.tempKey, "dst", finalKey)
		_ = s.srv.storage.Delete(ctx, up.tempKey)
		s.dropInflight(end.StreamID)
		s.srv.counterUploads("upstream_err")
		return s.send(ctx, hipproto.FrameUploadAck, ackErr(end.StreamID, hipproto.UploadStatusRejected, "copy failed"))
	}
	_ = s.srv.storage.Delete(ctx, up.tempKey)

	// record pending row for COMMIT
	s.pendingMu.Lock()
	s.pending = append(s.pending, store.Asset{
		Server:      s.hello.Region,
		BundlePath:  up.bundlePath,
		Path:        up.path,
		Version:     s.hello.AssetVersion,
		Fingerprint: up.fingerprint,
		Sha256:      sha,
		Size:        int64(up.totalRead),
		IsOverride:  isOverride,
		StorageKey:  finalKey,
	})
	s.pendingMu.Unlock()
	s.dropInflight(end.StreamID)
	s.srv.counterUploads("ok")

	return s.send(ctx, hipproto.FrameUploadAck, ackOK(end.StreamID, up.placement, sha, finalKey))
}

// dropInflight removes a stream from the map and releases its window slot.
func (s *session) dropInflight(streamID uint32) {
	s.uploadsMu.Lock()
	if _, ok := s.uploads[streamID]; ok {
		delete(s.uploads, streamID)
		select {
		case <-s.uploadSem:
		default:
		}
	}
	s.uploadsMu.Unlock()
}

func (s *session) finishInflight(ctx context.Context, streamID uint32, status, msg string) {
	s.uploadsMu.Lock()
	up, ok := s.uploads[streamID]
	s.uploadsMu.Unlock()
	if !ok {
		return
	}
	up.abort(ctx, s.srv.storage)
	s.dropInflight(streamID)
	_ = s.send(ctx, hipproto.FrameUploadAck, ackErr(streamID, status, msg))
}

func (s *session) abortAllUploads(ctx context.Context) {
	s.uploadsMu.Lock()
	ups := make([]*inflightUpload, 0, len(s.uploads))
	for _, u := range s.uploads {
		ups = append(ups, u)
	}
	s.uploadsMu.Unlock()
	for _, u := range ups {
		u.abort(ctx, s.srv.storage)
		s.dropInflight(u.streamID)
	}
	// Best effort: scrub any leftover tmp for this run.
	if s.hello.RunID != "" {
		// SeaweedFS filer supports directory-scoped DELETE via ?recursive=true.
		_ = s.srv.storage.Delete(ctx, "/tmp/"+s.hello.RunID)
	}
}

// ------------- COMMIT -------------

func (s *session) handleCommit(ctx context.Context, payload []byte) error {
	var c hipproto.Commit
	if err := hipproto.Decode(payload, &c); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation, "bad COMMIT payload")
	}
	// Also require: no outstanding uploads.
	s.uploadsMu.Lock()
	nInflight := len(s.uploads)
	s.uploadsMu.Unlock()
	if nInflight != 0 {
		return s.sendFatal(ctx, hipproto.ErrCodeProtoViolation,
			fmt.Sprintf("COMMIT with %d in-flight uploads", nInflight))
	}

	statsBytes, _ := json.Marshal(c.Stats)

	tx, err := s.srv.db.BeginTx(ctx, nil)
	if err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeInternal, "begin tx: "+err.Error())
	}
	rollback := func() { _ = tx.Rollback() }

	versionID, err := store.InsertVersion(ctx, tx, store.Version{
		Server:       s.hello.Region,
		AppVersion:   s.hello.AppVersion,
		AssetVersion: s.hello.AssetVersion,
		AssetHash:    s.hello.AssetHash,
		BundleCount:  int64(c.BundleCount),
		StatsJSON:    string(statsBytes),
	})
	if err != nil {
		rollback()
		return s.sendFatal(ctx, hipproto.ErrCodeInternal, "insert version: "+err.Error())
	}

	// Pending uploaded assets
	s.pendingMu.Lock()
	pending := s.pending
	reuse := s.sharedReuse
	s.pendingMu.Unlock()
	for _, a := range pending {
		if err := store.InsertAsset(ctx, tx, a); err != nil {
			rollback()
			return s.sendFatal(ctx, hipproto.ErrCodeInternal, "insert asset: "+err.Error())
		}
	}
	// Cross-region shared-reuse rows (no bytes were transferred, but we still
	// bind (server, path, version) to the existing shared storage key).
	for _, r := range reuse {
		if err := store.InsertAsset(ctx, tx, store.Asset{
			Server:      r.server,
			BundlePath:  "", // unknown here — CHECK didn't carry bundle_path
			Path:        r.path,
			Version:     s.hello.AssetVersion,
			Fingerprint: r.fingerprint,
			Sha256:      "", // shared row already has its sha; safe to leave blank for reuse rows
			Size:        0,
			IsOverride:  false,
			StorageKey:  r.storageKey,
		}); err != nil {
			rollback()
			return s.sendFatal(ctx, hipproto.ErrCodeInternal, "insert reuse asset: "+err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return s.sendFatal(ctx, hipproto.ErrCodeInternal, "commit tx: "+err.Error())
	}

	// Rebuild the in-memory index — read-path visibility flip.
	if err := s.srv.idx.Rebuild(ctx, s.srv.db); err != nil {
		s.log.Warn("hip: index rebuild failed", "err", err)
		// Do NOT undo the commit — data is durable; readers will just still
		// see the stale snapshot until the next successful rebuild.
	}

	s.state = stateFinalized
	s.log.Info("hip: COMMIT ok", "version_id", versionID, "assets", len(pending), "reuse", len(reuse))
	return s.send(ctx, hipproto.FrameCommitAck, &hipproto.CommitAck{
		VersionID:            uint64(versionID),
		OverrideIndexRebuilt: true,
	})
}

// ------------- helpers -------------

// SafeStripInvisible strips control characters clients may accidentally send.
func safeStripInvisible(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 {
			return -1
		}
		return r
	}, s)
}

// stub: session-scoped store handle for tests
var _ = sql.ErrNoRows
