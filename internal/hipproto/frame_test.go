package hipproto

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("hello world payload bytes")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Frame{Type: FrameHello, Payload: payload}, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadFrame(&buf, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Type != FrameHello {
		t.Fatalf("type mismatch: %v", got.Type)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Frame{Type: FramePing, Payload: nil}, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadFrame(&buf, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Type != FramePing || len(got.Payload) != 0 {
		t.Fatalf("bad frame: %+v", got)
	}
}

func TestFrameTooLarge(t *testing.T) {
	// length prefix = 3, but maxFrame = 2 — reader should reject.
	buf := bytes.NewBuffer([]byte{0, 0, 0, 3, 0x01, 0xaa, 0xbb})
	_, err := ReadFrame(buf, 2)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

func TestFrameShort(t *testing.T) {
	// length prefix = 0 is illegal.
	buf := bytes.NewBuffer([]byte{0, 0, 0, 0})
	_, err := ReadFrame(buf, 16)
	if !errors.Is(err, ErrShortFrame) {
		t.Fatalf("want ErrShortFrame, got %v", err)
	}
}

func TestFrameTruncated(t *testing.T) {
	// length prefix = 8 but stream ends after 3 bytes.
	buf := bytes.NewBuffer([]byte{0, 0, 0, 8, 0x01, 0xaa})
	_, err := ReadFrame(buf, 16)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestChunkRoundTrip(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	payload := EncodeChunk(0xdeadbeef, data)
	id, back, err := DecodeChunk(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if id != 0xdeadbeef {
		t.Fatalf("stream id mismatch: %x", id)
	}
	if !bytes.Equal(back, data) {
		t.Fatalf("data mismatch: %v", back)
	}
}

func TestChunkShort(t *testing.T) {
	if _, _, err := DecodeChunk([]byte{1, 2}); !errors.Is(err, ErrChunkTooShort) {
		t.Fatalf("want ErrChunkTooShort, got %v", err)
	}
}
