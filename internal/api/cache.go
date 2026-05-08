package api

import (
	"container/list"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"traffic-go/internal/model"
)

type lruEntry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

type lruCache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	now      func() time.Time
	items    map[K]*list.Element
	order    *list.List
}

func newLRUCache[K comparable, V any](capacity int) *lruCache[K, V] {
	if capacity < 0 {
		capacity = 0
	}
	return &lruCache[K, V]{
		capacity: capacity,
		now:      time.Now,
		items:    make(map[K]*list.Element),
		order:    list.New(),
	}
}

func (c *lruCache[K, V]) Get(key K) (V, bool) {
	var zero V
	if c == nil || c.capacity == 0 {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return zero, false
	}
	entry := elem.Value.(*lruEntry[K, V])
	if !entry.expiresAt.IsZero() && c.now().After(entry.expiresAt) {
		c.removeLocked(elem)
		return zero, false
	}
	c.order.MoveToFront(elem)
	return entry.value, true
}

func (c *lruCache[K, V]) Set(key K, value V, ttl time.Duration) {
	if c == nil || c.capacity == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = c.now().Add(ttl)
	}
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lruEntry[K, V])
		entry.value = value
		entry.expiresAt = expiresAt
		c.order.MoveToFront(elem)
		return
	}
	entry := &lruEntry[K, V]{key: key, value: value, expiresAt: expiresAt}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	if c.order.Len() > c.capacity {
		c.removeLocked(c.order.Back())
	}
}

func (c *lruCache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *lruCache[K, V]) removeLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	entry := elem.Value.(*lruEntry[K, V])
	c.order.Remove(elem)
	delete(c.items, entry.key)
}

// cachedTop holds the full grouped result for a top-N endpoint (processes or
// remotes) keyed by query window. The handler sorts and pages this slice in
// memory so different sort_by/page combinations all reuse one cache entry.
type cachedTop[T any] struct {
	rows   []T
	source string
}

// loadCachedCount returns the cached integer for key, or runs load (deduped
// via singleflight) and stores the result. Concurrent misses for the same key
// share one underlying store call so we never run two identical COUNT(*)
// queries side-by-side on the single CPU we have.
func loadCachedCount(ctx context.Context, cache *lruCache[string, int], group *singleflight.Group, key string, ttl time.Duration, load func(context.Context) (int, error)) (int, error) {
	if v, ok := cache.Get(key); ok {
		return v, nil
	}
	value, err, _ := group.Do(key, func() (any, error) {
		if v, ok := cache.Get(key); ok {
			return v, nil
		}
		v, err := load(ctx)
		if err != nil {
			return 0, err
		}
		cache.Set(key, v, ttl)
		return v, nil
	})
	if err != nil {
		return 0, err
	}
	return value.(int), nil
}

// loadCachedTop returns the cached top-N entry for key, or runs load (deduped
// via singleflight) and stores the result. See loadCachedCount for the
// rationale.
func loadCachedTop[T any](ctx context.Context, cache *lruCache[string, cachedTop[T]], group *singleflight.Group, key string, ttl time.Duration, load func(context.Context) (cachedTop[T], error)) (cachedTop[T], error) {
	if v, ok := cache.Get(key); ok {
		return v, nil
	}
	value, err, _ := group.Do(key, func() (any, error) {
		if v, ok := cache.Get(key); ok {
			return v, nil
		}
		v, err := load(ctx)
		if err != nil {
			return cachedTop[T]{}, err
		}
		cache.Set(key, v, ttl)
		return v, nil
	})
	if err != nil {
		return cachedTop[T]{}, err
	}
	return value.(cachedTop[T]), nil
}

func quantizeWindow(rangeKey string, startUnix int64, endUnix int64) (int64, int64, bool) {
	switch rangeKey {
	case "this_month":
		return startUnix, endUnix, false
	case "last_month", "two_months_ago":
		return startUnix, endUnix, true
	}
	gran := windowGranularitySeconds(rangeKey, endUnix-startUnix)
	q := func(v int64) int64 { return (v / gran) * gran }
	return q(startUnix), q(endUnix), false
}

func windowGranularitySeconds(rangeKey string, duration int64) int64 {
	switch rangeKey {
	case "1h", "24h":
		return 60
	case "7d":
		return 300
	}
	switch {
	case duration <= 3600:
		return 60
	case duration <= 7*24*3600:
		return 300
	default:
		return 3600
	}
}

func usageFilterFingerprint(q model.UsageQuery) string {
	pidStr := ""
	if q.PID != nil {
		pidStr = fmt.Sprintf("%d", *q.PID)
	}
	localPortStr := ""
	if q.LocalPort != nil {
		localPortStr = fmt.Sprintf("%d", *q.LocalPort)
	}
	remotePortStr := ""
	if q.RemotePort != nil {
		remotePortStr = fmt.Sprintf("%d", *q.RemotePort)
	}
	parts := []string{
		"attr=" + string(q.Attribution),
		"comm=" + q.Comm,
		"dir=" + string(q.Direction),
		"exe=" + q.Exe,
		"lport=" + localPortStr,
		"pid=" + pidStr,
		"proto=" + q.Proto,
		"rip=" + q.RemoteIP,
		"rport=" + remotePortStr,
	}
	return strings.Join(parts, "|")
}

func forwardFilterFingerprint(q model.ForwardQuery) string {
	return strings.Join([]string{
		"proto=" + q.Proto,
		"src=" + q.OrigSrcIP,
		"dst=" + q.OrigDstIP,
	}, "|")
}

func normalizedRangeKey(r *http.Request) string {
	query := r.URL.Query()
	if query.Get("start") != "" {
		return "explicit"
	}
	value := strings.ToLower(strings.TrimSpace(query.Get("range")))
	switch value {
	case "":
		return "24h"
	case "current_month", "month":
		return "this_month"
	case "previous_month":
		return "last_month"
	case "month_before_last":
		return "two_months_ago"
	default:
		return value
	}
}

func (s *Server) cacheTTL(archived bool) time.Duration {
	if archived && s.archivedCacheTTL > 0 {
		return s.archivedCacheTTL
	}
	return s.slidingCacheTTL
}

func usageCountCacheKey(source string, rangeKey string, startBucket int64, endBucket int64, query model.UsageQuery) string {
	return fmt.Sprintf("usage_count|src=%s|rng=%s|t=%d-%d|%s",
		source, rangeKey, startBucket, endBucket, usageFilterFingerprint(query))
}

func forwardCountCacheKey(source string, rangeKey string, startBucket int64, endBucket int64, query model.ForwardQuery) string {
	return fmt.Sprintf("forward_count|src=%s|rng=%s|t=%d-%d|%s",
		source, rangeKey, startBucket, endBucket, forwardFilterFingerprint(query))
}
