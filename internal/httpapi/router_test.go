package httpapi

import "testing"

func TestParseSekaiPath(t *testing.T) {
	cases := []struct {
		in     string
		server string
		rel    string
		ok     bool
	}{
		{"/sekai-jp-assets/event/foo/bar.webp", "jp", "event/foo/bar.webp", true},
		{"/sekai-cn-assets/a", "cn", "a", true},
		{"/sekai-jp-assets/", "", "", false},
		{"/sekai--assets/x", "", "", false},
		{"/sekai-JP-assets/x", "", "", false},
		{"/other/x", "", "", false},
		{"/sekai-jp-assetsx/y", "", "", false},
		{"/healthz", "", "", false},
	}
	for _, c := range cases {
		s, r, ok := parseSekaiPath(c.in)
		if ok != c.ok || s != c.server || r != c.rel {
			t.Errorf("parse %q → (%q, %q, %v), want (%q, %q, %v)",
				c.in, s, r, ok, c.server, c.rel, c.ok)
		}
	}
}
