// Package hipproto implements the HIP/1 wire format.
//
// Spec: docs/hip.md in the Haruki-Sekai-Asset-Updater repo. This package is
// intentionally free of any network / storage / DB dependency so it can be
// exhaustively unit-tested in isolation.
package hipproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FrameType is the one-byte HIP frame discriminator.
type FrameType uint8

const (
	FrameHello       FrameType = 0x01
	FrameHelloAck    FrameType = 0x02
	FrameCheckBatch  FrameType = 0x03
	FrameCheckAck    FrameType = 0x04
	FrameUploadBegin FrameType = 0x05
	FrameUploadChunk FrameType = 0x06
	FrameUploadEnd   FrameType = 0x07
	FrameUploadAck   FrameType = 0x08
	FrameCommit      FrameType = 0x09
	FrameCommitAck   FrameType = 0x0A
	FrameBye         FrameType = 0x0B
	FrameWindow      FrameType = 0x0C
	FramePing        FrameType = 0x0E
	FramePong        FrameType = 0x0F
	FrameError       FrameType = 0x1F
)

// DefaultMaxFrameBytes is the default MAX_FRAME (16 MiB, matches the Rust
// client's DEFAULT_MAX_FRAME_BYTES).
const DefaultMaxFrameBytes uint64 = 16 * 1024 * 1024

// HardMaxFrameBytes is the absolute cap enforced regardless of config, to
// prevent a malicious peer from causing a huge allocation.
const HardMaxFrameBytes uint64 = 64 * 1024 * 1024

// Frame is a decoded HIP frame.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// ErrFrameTooLarge is returned when the length prefix exceeds max.
var ErrFrameTooLarge = errors.New("hipproto: frame exceeds max_frame")

// ErrShortFrame is returned when the payload has zero length (must contain
// the type byte at minimum).
var ErrShortFrame = errors.New("hipproto: frame length must be >= 1")

// String helps logging.
func (t FrameType) String() string {
	switch t {
	case FrameHello:
		return "HELLO"
	case FrameHelloAck:
		return "HELLO_ACK"
	case FrameCheckBatch:
		return "CHECK_BATCH"
	case FrameCheckAck:
		return "CHECK_ACK"
	case FrameUploadBegin:
		return "UPLOAD_BEGIN"
	case FrameUploadChunk:
		return "UPLOAD_CHUNK"
	case FrameUploadEnd:
		return "UPLOAD_END"
	case FrameUploadAck:
		return "UPLOAD_ACK"
	case FrameCommit:
		return "COMMIT"
	case FrameCommitAck:
		return "COMMIT_ACK"
	case FrameBye:
		return "BYE"
	case FrameWindow:
		return "WINDOW"
	case FramePing:
		return "PING"
	case FramePong:
		return "PONG"
	case FrameError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(t))
	}
}

// ReadFrame reads exactly one HIP frame from r. `maxFrame` is the negotiated
// upper bound on `length` (i.e. type byte + payload).
func ReadFrame(r io.Reader, maxFrame uint64) (Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return Frame{}, ErrShortFrame
	}
	if uint64(length) > maxFrame {
		return Frame{}, fmt.Errorf("%w: got %d, max %d", ErrFrameTooLarge, length, maxFrame)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}
	return Frame{Type: FrameType(buf[0]), Payload: buf[1:]}, nil
}

// WriteFrame writes exactly one HIP frame to w. `maxFrame` is the negotiated
// upper bound on `length`.
func WriteFrame(w io.Writer, f Frame, maxFrame uint64) error {
	total := 1 + len(f.Payload)
	if uint64(total) > maxFrame {
		return fmt.Errorf("%w: got %d, max %d", ErrFrameTooLarge, total, maxFrame)
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(total))
	hdr[4] = uint8(f.Type)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}
