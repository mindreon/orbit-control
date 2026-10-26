package auth

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Positive IAM lookups stay valid for a minute; revocation at baize therefore
// takes up to 60s to reach orbit. Negative results are never cached.
const (
	iamLookupTTL     = 60 * time.Second
	iamCacheCapacity = 10_000
	iamLookupTimeout = 8 * time.Second

	// A remembered (principal, project) -> workspace mapping skips the upsert for a
	// minute. A rename at baize therefore reaches orbit within 60s.
	iamProjectMemoTTL = 60 * time.Second
)

// cachedIAMLookup wraps a lookup with a bounded in-process cache keyed by the
// SHA-256 of the bearer token, so the token itself is never retained as a map
// key. The shape follows netflow's cachedIAMLookup (apps/api/internal/auth).
type cachedIAMLookup struct {
	lookup iamLookup
	cache  *ttlCache[[sha256.Size]byte, iamIdentity]
	group  singleflight.Group
	now    func() time.Time
}

func newCachedLookup(lookup iamLookup) *cachedIAMLookup {
	return &cachedIAMLookup{
		lookup: lookup,
		cache:  newTTLCache[[sha256.Size]byte, iamIdentity](iamCacheCapacity),
		now:    time.Now,
	}
}

func (c *cachedIAMLookup) Lookup(ctx context.Context, token string) (iamIdentity, error) {
	key := sha256.Sum256([]byte(token))
	if identity, ok := c.cache.get(key, c.now()); ok {
		return identity, nil
	}
	result := c.group.DoChan(string(key[:]), func() (any, error) {
		if identity, ok := c.cache.get(key, c.now()); ok {
			return identity, nil
		}
		lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), iamLookupTimeout)
		defer cancel()
		identity, err := c.lookup.Lookup(lookupCtx, token)
		if err != nil {
			return iamIdentity{}, err
		}
		now := c.now()
		c.cache.set(key, identity, now.Add(iamLookupTTL), now)
		return identity, nil
	})
	select {
	case <-ctx.Done():
		return iamIdentity{}, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return iamIdentity{}, r.Err
		}
		return r.Val.(iamIdentity), nil
	}
}

type ttlEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type ttlCache[K comparable, V any] struct {
	mu       sync.Mutex
	entries  map[K]ttlEntry[V]
	capacity int
}

func newTTLCache[K comparable, V any](capacity int) *ttlCache[K, V] {
	return &ttlCache[K, V]{entries: make(map[K]ttlEntry[V]), capacity: capacity}
}

func (c *ttlCache[K, V]) get(key K, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if ok && now.Before(entry.expiresAt) {
		return entry.value, true
	}
	delete(c.entries, key)
	var zero V
	return zero, false
}

// set evicts expired entries once the cache is full, then one arbitrary live
// entry, so the bound holds.
func (c *ttlCache[K, V]) set(key K, value V, expiresAt, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !now.Before(expiresAt) {
		return
	}
	if len(c.entries) >= c.capacity {
		for k, entry := range c.entries {
			if !now.Before(entry.expiresAt) {
				delete(c.entries, k)
			}
		}
		if _, exists := c.entries[key]; !exists && len(c.entries) >= c.capacity {
			for k := range c.entries {
				delete(c.entries, k)
				break
			}
		}
	}
	c.entries[key] = ttlEntry[V]{value: value, expiresAt: expiresAt}
}

// projectMemoKey remembers one workspace upsert per (principal, project).
type projectMemoKey struct{ principal, project string }

// iamProjectMemo caches the workspace id an upsert produced, so repeat requests
// for the same project do not write. It does not cache failures.
type iamProjectMemo struct {
	cache *ttlCache[projectMemoKey, string]
	now   func() time.Time
}

func newProjectMemo() *iamProjectMemo {
	return &iamProjectMemo{cache: newTTLCache[projectMemoKey, string](iamCacheCapacity), now: time.Now}
}

func (m *iamProjectMemo) get(principal, project string) (string, bool) {
	return m.cache.get(projectMemoKey{principal, project}, m.now())
}

func (m *iamProjectMemo) set(principal, project, workspace string) {
	now := m.now()
	m.cache.set(projectMemoKey{principal, project}, workspace, now.Add(iamProjectMemoTTL), now)
}
