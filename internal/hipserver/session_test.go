package hipserver

import "testing"

func TestCanonicalAssetPathJoinsBundleRelativeAssetPath(t *testing.T) {
	got, err := canonicalAssetPath("sound/scenario/bgm/bgm001", "bgm001.mp3")
	if err != nil {
		t.Fatalf("canonicalAssetPath failed: %v", err)
	}
	want := "sound/scenario/bgm/bgm001/bgm001.mp3"
	if got != want {
		t.Fatalf("canonical path = %q, want %q", got, want)
	}
}

func TestCanonicalAssetPathKeepsAlreadyCanonicalAssetPath(t *testing.T) {
	got, err := canonicalAssetPath(
		"mysekai/fixture/mdl_foo",
		"mysekai/fixture/mdl_foo/mdl_foo.obj",
	)
	if err != nil {
		t.Fatalf("canonicalAssetPath failed: %v", err)
	}
	want := "mysekai/fixture/mdl_foo/mdl_foo.obj"
	if got != want {
		t.Fatalf("canonical path = %q, want %q", got, want)
	}
}

func TestCanonicalAssetPathDoesNotOvermatchSiblingPrefix(t *testing.T) {
	got, err := canonicalAssetPath(
		"mysekai/fixture/mdl_foo",
		"mysekai/fixture/mdl_foobar/mdl_foobar.obj",
	)
	if err != nil {
		t.Fatalf("canonicalAssetPath failed: %v", err)
	}
	want := "mysekai/fixture/mdl_foo/mysekai/fixture/mdl_foobar/mdl_foobar.obj"
	if got != want {
		t.Fatalf("canonical path = %q, want %q", got, want)
	}
}
