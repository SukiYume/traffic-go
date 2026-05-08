package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"

	"traffic-go/internal/model"
)

const testCacheTTL = 10 * time.Second

func TestLRUCacheGetSet(t *testing.T) {
	c := newLRUCache[string, int](2)
	c.Set("a", 1, testCacheTTL)
	c.Set("b", 2, testCacheTTL)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("get a = %v, %v", v, ok)
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("get b = %v, %v", v, ok)
	}
}

func TestLRUCacheEvictsOldest(t *testing.T) {
	c := newLRUCache[string, int](2)
	c.Set("a", 1, testCacheTTL)
	c.Set("b", 2, testCacheTTL)
	c.Set("c", 3, testCacheTTL)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should still be cached")
	}
}

func TestLRUCacheTTLExpiry(t *testing.T) {
	c := newLRUCache[string, int](4)
	c.Set("a", 1, 20*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should have expired")
	}
}

func TestLRUCacheGetPromotes(t *testing.T) {
	c := newLRUCache[string, int](2)
	c.Set("a", 1, testCacheTTL)
	c.Set("b", 2, testCacheTTL)
	_, _ = c.Get("a")
	c.Set("c", 3, testCacheTTL)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should still be cached after promotion")
	}
}

func TestLRUCacheZeroCapacityIsNoop(t *testing.T) {
	c := newLRUCache[string, int](0)
	c.Set("a", 1, testCacheTTL)
	if _, ok := c.Get("a"); ok {
		t.Fatal("zero-capacity cache should not retain entries")
	}
}

func TestQuantizeWindow(t *testing.T) {
	tests := []struct {
		name      string
		rangeKey  string
		startUnix int64
		endUnix   int64
		wantStart int64
		wantEnd   int64
		archived  bool
	}{
		{
			name:      "1h is quantized to 60s",
			rangeKey:  "1h",
			startUnix: 1715140861,
			endUnix:   1715144461,
			wantStart: 1715140860,
			wantEnd:   1715144460,
			archived:  false,
		},
		{
			name:      "7d quantized to 5min",
			rangeKey:  "7d",
			startUnix: 1714540861,
			endUnix:   1715145661,
			wantStart: 1714540800,
			wantEnd:   1715145600,
			archived:  false,
		},
		{
			name:      "this_month is not quantized",
			rangeKey:  "this_month",
			startUnix: 1714521600,
			endUnix:   1717113600,
			wantStart: 1714521600,
			wantEnd:   1717113600,
			archived:  false,
		},
		{
			name:      "last_month is archived",
			rangeKey:  "last_month",
			startUnix: 1711929600,
			endUnix:   1714521600,
			wantStart: 1711929600,
			wantEnd:   1714521600,
			archived:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, gotArchived := quantizeWindow(tt.rangeKey, tt.startUnix, tt.endUnix)
			if gotStart != tt.wantStart || gotEnd != tt.wantEnd || gotArchived != tt.archived {
				t.Fatalf("quantizeWindow(%s) = (%d, %d, %v), want (%d, %d, %v)",
					tt.rangeKey, gotStart, gotEnd, gotArchived, tt.wantStart, tt.wantEnd, tt.archived)
			}
		})
	}
}

func TestUsageFilterFingerprintStable(t *testing.T) {
	localPort := 443
	q1 := model.UsageQuery{Comm: "ss-server", Direction: model.DirectionOut, Proto: "tcp", LocalPort: &localPort}
	q2 := model.UsageQuery{Proto: "tcp", LocalPort: &localPort, Direction: model.DirectionOut, Comm: "ss-server"}
	if usageFilterFingerprint(q1) != usageFilterFingerprint(q2) {
		t.Fatal("fingerprint should be stable regardless of struct field assignment order")
	}
}

func TestUsageFilterFingerprintDistinguishesAllFields(t *testing.T) {
	base := model.UsageQuery{}
	pid := 123
	localPort := 80
	remotePort := 443
	variants := []model.UsageQuery{
		{Comm: "x"},
		{PID: &pid},
		{Exe: "/bin/x"},
		{RemoteIP: "1.1.1.1"},
		{LocalPort: &localPort},
		{Direction: model.DirectionIn},
		{Proto: "udp"},
		{Attribution: model.AttributionExact},
		{RemotePort: &remotePort},
	}
	seen := map[string]struct{}{usageFilterFingerprint(base): {}}
	for i, q := range variants {
		fp := usageFilterFingerprint(q)
		if _, dup := seen[fp]; dup {
			t.Fatalf("variant %d collided with another fingerprint: %q", i, fp)
		}
		seen[fp] = struct{}{}
	}
}

func TestForwardFilterFingerprintStable(t *testing.T) {
	q1 := model.ForwardQuery{Proto: "tcp", OrigSrcIP: "10.0.0.1", OrigDstIP: "1.1.1.1"}
	q2 := model.ForwardQuery{OrigDstIP: "1.1.1.1", OrigSrcIP: "10.0.0.1", Proto: "tcp"}
	if forwardFilterFingerprint(q1) != forwardFilterFingerprint(q2) {
		t.Fatal("forward fingerprint should be deterministic")
	}
}

func TestLoadCachedCountDedupesConcurrentMisses(t *testing.T) {
	cache := newLRUCache[string, int](4)
	var group singleflight.Group
	var calls atomic.Int32
	gate := make(chan struct{})
	load := func(ctx context.Context) (int, error) {
		calls.Add(1)
		<-gate
		return 42, nil
	}

	const goroutines = 10
	results := make([]int, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			v, err := loadCachedCount(context.Background(), cache, &group, "key", testCacheTTL, load)
			if err != nil {
				t.Errorf("loadCachedCount: %v", err)
				return
			}
			results[idx] = v
		}(i)
	}
	// Give the goroutines a moment to enter singleflight before unblocking the loader.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly one underlying load, got %d", got)
	}
	for i, v := range results {
		if v != 42 {
			t.Fatalf("goroutine %d got value %d, want 42", i, v)
		}
	}
	// Subsequent reads should hit the cache, not increment calls.
	if v, err := loadCachedCount(context.Background(), cache, &group, "key", testCacheTTL, load); err != nil || v != 42 {
		t.Fatalf("post-load fetch = (%d, %v), want (42, nil)", v, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("cached read should not invoke load again, calls=%d", got)
	}
}
