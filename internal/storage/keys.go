package storage

import (
	"errors"
	"path"
	"strings"
)

// ErrBadPath signals a path that is unsafe (traversal, absolute, backslash, ...).
var ErrBadPath = errors.New("storage: unsafe path")

// SafeRelPath validates a client-supplied relative path. It must:
//   - be non-empty
//   - not start with '/', '\\' or '.'
//   - not contain '..'
//   - not contain '\\' (Windows separator)
//   - normalise cleanly (path.Clean must equal the input)
func SafeRelPath(p string) (string, error) {
	if p == "" {
		return "", ErrBadPath
	}
	if strings.Contains(p, "\\") || strings.Contains(p, "\x00") {
		return "", ErrBadPath
	}
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, ".") {
		return "", ErrBadPath
	}
	// Reject '..' segments anywhere.
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", ErrBadPath
		}
	}
	// Ensure normalisation is a no-op.
	if path.Clean(p) != p {
		return "", ErrBadPath
	}
	return p, nil
}

// SharedKey returns the SeaweedFS filer key for a shared baseline asset.
func SharedKey(relPath string) string {
	return "/shared-assets/" + relPath
}

// OverrideKey returns the SeaweedFS filer key for a per-server override.
func OverrideKey(server, relPath string) string {
	return "/overrides/" + server + "/" + relPath
}

// TempKey returns the SeaweedFS filer key used for in-flight upload buffers.
func TempKey(runID string, streamID uint32) string {
	return "/tmp/" + runID + "/" + itoa(streamID)
}

func itoa(u uint32) string {
	// Small stdlib-free itoa to avoid pulling strconv into a hot path.
	if u == 0 {
		return "0"
	}
	var buf [10]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	return string(buf[i:])
}
