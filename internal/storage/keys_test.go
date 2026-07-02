package storage

import "testing"

func TestSafeRelPath(t *testing.T) {
	ok := []string{"a", "a/b", "event/foo/bar.webp", "a-b/c_d.e"}
	bad := []string{"", "/a", "./a", "a/../b", "..", "a//b", "a/./b", "a\\b", "a\x00b"}
	for _, p := range ok {
		if _, err := SafeRelPath(p); err != nil {
			t.Errorf("wanted OK %q, got %v", p, err)
		}
	}
	for _, p := range bad {
		if _, err := SafeRelPath(p); err == nil {
			t.Errorf("wanted err for %q", p)
		}
	}
}

func TestKeyBuilders(t *testing.T) {
	if got := SharedKey("event/foo/bar.webp"); got != "/shared-assets/event/foo/bar.webp" {
		t.Fatal(got)
	}
	if got := OverrideKey("jp", "event/foo/bar.webp"); got != "/overrides/jp/event/foo/bar.webp" {
		t.Fatal(got)
	}
	if got := TempKey("run-1", 7); got != "/tmp/run-1/7" {
		t.Fatal(got)
	}
}
