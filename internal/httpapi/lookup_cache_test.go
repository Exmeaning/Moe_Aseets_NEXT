package httpapi

import (
	"testing"
	"time"

	"github.com/Team-Haruki/moe-assets-gateway/internal/store"
)

func TestLookupCacheInvalidatePathsRemovesAllServersForPath(t *testing.T) {
	cache := NewLookupCache(10, time.Hour)
	a := store.Placement{Found: true, StorageKey: "/shared-assets/a"}
	b := store.Placement{Found: true, StorageKey: "/shared-assets/b"}

	cache.Set("jp\x00img/a.png", a)
	cache.Set("en\x00img/a.png", a)
	cache.Set("jp\x00img/b.png", b)

	cache.InvalidatePaths([]string{"img/a.png"})

	if _, ok := cache.Get("jp\x00img/a.png"); ok {
		t.Fatal("jp img/a.png survived invalidation")
	}
	if _, ok := cache.Get("en\x00img/a.png"); ok {
		t.Fatal("en img/a.png survived invalidation")
	}
	if got, ok := cache.Get("jp\x00img/b.png"); !ok || got.StorageKey != b.StorageKey {
		t.Fatalf("unrelated cache entry missing: got=%+v ok=%v", got, ok)
	}
}

func TestLookupCacheDoesNotStoreMisses(t *testing.T) {
	cache := NewLookupCache(10, time.Hour)

	cache.Set("jp\x00missing.png", store.Placement{})

	if _, ok := cache.Get("jp\x00missing.png"); ok {
		t.Fatal("negative lookup was cached")
	}
}
