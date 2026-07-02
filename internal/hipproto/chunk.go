package hipproto

import (
	"encoding/binary"
	"errors"
)

// UPLOAD_CHUNK has a raw wire layout, NOT msgpack:
//
//   +-----------------+-------------------------+
//   | stream_id (u32) |     raw bytes...        |
//   +-----------------+-------------------------+
//         4 bytes            remaining bytes

// ErrChunkTooShort indicates a chunk payload lacked the 4-byte stream id prefix.
var ErrChunkTooShort = errors.New("hipproto: UPLOAD_CHUNK payload shorter than 4 bytes")

// DecodeChunk returns the stream id and a reference into the payload's data
// bytes (no copy). The returned data slice aliases the input payload.
func DecodeChunk(payload []byte) (streamID uint32, data []byte, err error) {
	if len(payload) < 4 {
		return 0, nil, ErrChunkTooShort
	}
	return binary.BigEndian.Uint32(payload[:4]), payload[4:], nil
}

// EncodeChunk allocates a new payload buffer with the u32 prefix + data.
// Prefer EncodeChunkInto for hot paths.
func EncodeChunk(streamID uint32, data []byte) []byte {
	out := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(out[:4], streamID)
	copy(out[4:], data)
	return out
}
