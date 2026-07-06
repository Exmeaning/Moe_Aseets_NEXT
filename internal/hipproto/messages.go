package hipproto

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Encode a message struct to msgpack. Field names use `msgpack:"snake_case"`
// tags so the wire is compatible with rmp-serde `write_named` on the Rust
// client side.
func Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode msgpack payload into v.
func Decode(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

// ---- Hello / HelloAck ----

type Hello struct {
	Proto            string `msgpack:"proto"`
	Version          uint32 `msgpack:"version"`
	BearerToken      string `msgpack:"bearer_token"`
	Region           string `msgpack:"region"`
	AppVersion       string `msgpack:"app_version"`
	AssetVersion     string `msgpack:"asset_version"`
	AssetHash        string `msgpack:"asset_hash"`
	RunID            string `msgpack:"run_id"`
	UnpackerVersion  string `msgpack:"unpacker_version"`
	ExpectedMaxFrame uint64 `msgpack:"expected_max_frame"`
}

type HelloAck struct {
	SessionID          string `msgpack:"session_id"`
	ServerVersion      string `msgpack:"server_version"`
	MaxFrame           uint64 `msgpack:"max_frame"`
	MaxInFlightUploads uint32 `msgpack:"max_in_flight_uploads"`
	Sha256Required     bool   `msgpack:"sha256_required"`
	KnownVersion       bool   `msgpack:"known_version"`
}

// ---- CheckBatch / CheckAck ----

type CheckBatchItem struct {
	Path        string `msgpack:"path"`
	Fingerprint string `msgpack:"fingerprint"`
	Size        uint64 `msgpack:"size"`
	Provider    string `msgpack:"provider"`
}

type CheckBatch struct {
	BatchID uint64           `msgpack:"batch_id"`
	Items   []CheckBatchItem `msgpack:"items"`
}

// CheckAction is a string enum: "SKIP" or "UPLOAD". We keep it as a Go string
// to match the SCREAMING_SNAKE_CASE serde encoding on the Rust side.
type CheckAction = string

const (
	ActionSkip   CheckAction = "SKIP"
	ActionUpload CheckAction = "UPLOAD"
)

type Placement = string

const (
	PlacementShared   Placement = "SHARED"
	PlacementOverride Placement = "OVERRIDE"
)

type CheckAckItem struct {
	Path      string    `msgpack:"path"`
	Action    string    `msgpack:"action"`
	Placement Placement `msgpack:"placement,omitempty"`
}

type CheckResult struct {
	BatchID uint64         `msgpack:"batch_id"`
	Results []CheckAckItem `msgpack:"results"`
}

// ---- Upload ----

type UploadBegin struct {
	StreamID    uint32 `msgpack:"stream_id"`
	BundlePath  string `msgpack:"bundle_path"`
	Path        string `msgpack:"path"`
	Fingerprint string `msgpack:"fingerprint"`
	Size        uint64 `msgpack:"size"`
	ContentType string `msgpack:"content_type,omitempty"`
}

type UploadEnd struct {
	StreamID   uint32 `msgpack:"stream_id"`
	TotalBytes uint64 `msgpack:"total_bytes"`
	Sha256     string `msgpack:"sha256"`
}

// UploadAckStatus values, matching the Rust client's expected strings.
const (
	UploadStatusOK           = "OK"
	UploadStatusShaMismatch  = "SHA_MISMATCH"
	UploadStatusSizeMismatch = "SIZE_MISMATCH"
	UploadStatusRejected     = "REJECTED"
)

type UploadAck struct {
	StreamID     uint32    `msgpack:"stream_id"`
	Status       string    `msgpack:"status"`
	Placement    Placement `msgpack:"placement,omitempty"`
	ServerSha256 string    `msgpack:"server_sha256,omitempty"`
	StorageKey   string    `msgpack:"storage_key,omitempty"`
	Message      string    `msgpack:"message,omitempty"`
}

// ---- Commit ----

type CommitStats struct {
	SkippedByLayer1  uint64 `msgpack:"skipped_by_layer1"`
	SkippedByCheck   uint64 `msgpack:"skipped_by_check"`
	UploadedShared   uint64 `msgpack:"uploaded_shared"`
	UploadedOverride uint64 `msgpack:"uploaded_override"`
}

type CommitBundleCompletion struct {
	Path        string `msgpack:"path"`
	Fingerprint string `msgpack:"fingerprint"`
	Source      string `msgpack:"source,omitempty"`
}

type Commit struct {
	BundleCount      uint64                   `msgpack:"bundle_count"`
	Stats            CommitStats              `msgpack:"stats"`
	CompletedBundles []CommitBundleCompletion `msgpack:"completed_bundles,omitempty"`
}

type CommitAck struct {
	VersionID            uint64 `msgpack:"version_id"`
	OverrideIndexRebuilt bool   `msgpack:"override_index_rebuilt"`
}

// ---- Window / Error ----

type Window struct {
	MaxInFlightUploads uint32 `msgpack:"max_in_flight_uploads"`
}

// Error codes (§4.14).
const (
	ErrCodeAuthFailed     = "AUTH_FAILED"
	ErrCodeProtoViolation = "PROTO_VIOLATION"
	ErrCodeShaMismatch    = "SHA_MISMATCH"
	ErrCodeStorageFull    = "STORAGE_FULL"
	ErrCodeInternal       = "INTERNAL"
)

type ErrorPayload struct {
	Code    string `msgpack:"code"`
	Message string `msgpack:"message"`
	Fatal   bool   `msgpack:"fatal"`
}
