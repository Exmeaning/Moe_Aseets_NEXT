package hipserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/Team-Haruki/moe-assets-gateway/internal/hipproto"
	"github.com/Team-Haruki/moe-assets-gateway/internal/storage"
)

// inflightUpload tracks one active UPLOAD_BEGIN..UPLOAD_END stream.
type inflightUpload struct {
	streamID     uint32
	bundlePath   string
	path         string
	fingerprint  string
	contentType  string
	expectedSize uint64
	// placement decided by prior CHECK. Empty means we look it up at
	// UPLOAD_END time (defensive fallback).
	placement string

	// running state
	hasher    hash.Hash
	totalRead uint64
	pipeW     *io.PipeWriter // client → filer streaming pipe
	pipeR     *io.PipeReader

	// PUT result signalling
	putErr chan error

	// destination decided when the stream finishes cleanly.
	tempKey  string
	finalKey string

	mu     sync.Mutex
	closed bool
}

// newInflight builds an inflight upload and kicks off the concurrent PUT
// to the SeaweedFS filer. Chunks arrive via pipeW.Write() from the reader
// goroutine.
func newInflight(ctx context.Context, sc *storage.Client, begin hipproto.UploadBegin, tempKey string) *inflightUpload {
	pr, pw := io.Pipe()
	up := &inflightUpload{
		streamID:     begin.StreamID,
		bundlePath:   begin.BundlePath,
		path:         begin.Path,
		fingerprint:  begin.Fingerprint,
		contentType:  begin.ContentType,
		expectedSize: begin.Size,
		hasher:       sha256.New(),
		pipeR:        pr,
		pipeW:        pw,
		putErr:       make(chan error, 1),
		tempKey:      tempKey,
	}
	go func() {
		defer close(up.putErr)
		ct := up.contentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		// We do not know the exact size here (could be wrong if client lies).
		// Pass -1 to force chunked transfer-encoding.
		err := sc.Put(ctx, tempKey, pr, -1, ct)
		up.putErr <- err
	}()
	return up
}

// writeChunk feeds bytes into the hasher and the pipe writer.
func (u *inflightUpload) writeChunk(data []byte) error {
	if _, err := u.hasher.Write(data); err != nil {
		return err
	}
	u.totalRead += uint64(len(data))
	if _, err := u.pipeW.Write(data); err != nil {
		return err
	}
	return nil
}

// finish closes the pipe writer and waits for the PUT to complete. Returns
// (serverSha, err). If ctx cancels first, err is ctx.Err().
func (u *inflightUpload) finish(ctx context.Context) (string, error) {
	u.mu.Lock()
	if !u.closed {
		u.closed = true
		_ = u.pipeW.Close()
	}
	u.mu.Unlock()
	select {
	case err := <-u.putErr:
		if err != nil {
			return "", err
		}
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return hex.EncodeToString(u.hasher.Sum(nil)), nil
}

// abort tears down the pipe and best-effort deletes the temp key. Safe to
// call multiple times.
func (u *inflightUpload) abort(ctx context.Context, sc *storage.Client) {
	u.mu.Lock()
	if !u.closed {
		u.closed = true
		_ = u.pipeW.CloseWithError(errAborted)
	}
	u.mu.Unlock()
	// Drain PUT result; if it succeeded before we aborted, delete the temp.
	<-u.putErr
	if u.tempKey != "" {
		_ = sc.Delete(ctx, u.tempKey)
	}
}

var errAborted = errors.New("hipserver: upload aborted")

// ackOK builds an UPLOAD_ACK{status:OK, ...} frame.
func ackOK(streamID uint32, placement, sha, storageKey string) hipproto.UploadAck {
	return hipproto.UploadAck{
		StreamID:     streamID,
		Status:       hipproto.UploadStatusOK,
		Placement:    placement,
		ServerSha256: sha,
		StorageKey:   storageKey,
	}
}

// ackErr builds an UPLOAD_ACK with the given non-OK status.
func ackErr(streamID uint32, status, msg string) hipproto.UploadAck {
	return hipproto.UploadAck{StreamID: streamID, Status: status, Message: msg}
}

// verifyEnd cross-checks the client-declared bytes/sha against server-observed.
// Returns "" on success or one of the UPLOAD_ACK status strings on mismatch.
func verifyEnd(u *inflightUpload, end hipproto.UploadEnd) (status, msg string) {
	if end.TotalBytes != u.totalRead {
		return hipproto.UploadStatusSizeMismatch,
			fmt.Sprintf("client=%d observed=%d", end.TotalBytes, u.totalRead)
	}
	if u.expectedSize != 0 && u.totalRead != u.expectedSize {
		return hipproto.UploadStatusSizeMismatch,
			fmt.Sprintf("begin=%d observed=%d", u.expectedSize, u.totalRead)
	}
	serverSha := hex.EncodeToString(u.hasher.Sum(nil))
	if end.Sha256 != "" && end.Sha256 != serverSha {
		return hipproto.UploadStatusShaMismatch,
			fmt.Sprintf("client=%s server=%s", end.Sha256, serverSha)
	}
	return "", ""
}
