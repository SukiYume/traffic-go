package api

import (
	"testing"
	"time"

	"traffic-go/internal/model"
)

func TestLRUCacheGetSet(t *testing.T) {
	c := newLRUCache[string, int](2, 10*time.Second)
	c.Set("a", 1)
	c.Set("b", 2)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("get a = %v, %v", v, ok)
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("get b = %v, %v", v, ok)
	}
}

func TestLRUCacheEvictsOldest(t *testing.T) {
	c := newLRUCache[string, int](2, 10*time.Second)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should still be cached")
	}
}

func TestLRUCacheTTLExpiry(t *testing.T) {
	c := newLRUCache[string, int](4, 20*time.Millisecond)
	c.Set("a", 1)
	time.Sleep(50 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should have expired")
	}
}

func TestLRUCacheGetPromotes(t *testing.T) {
	c := newLRUCache[string, int](2, 10*time.Second)
	c.Set("a", 1)
	c.Set("b", 2)
	_, _ = c.Get("a")
	c.Set("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should still be cached after promotion")
	}
}

func TestLRUCacheZeroCapacityIsNoop(t *testing.T) {
	c := newLRUCache[string, int](0, 10*time.Second)
	c.Set("a", 1)
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
