package collector

import (
	"context"
	"testing"
	"time"

	"traffic-go/internal/model"
)

func TestProcessResolverRefreshesCacheFromFullScan(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	resolver := newProcessResolver("/proc")
	resolver.now = func() time.Time { return now }

	scans := []map[uint64]model.ProcessInfo{
		{
			1: processInfo(100, "ss-server"),
			2: processInfo(200, "nginx"),
		},
		{
			1: processInfo(300, "v2ray"),
		},
	}
	scanCalls := 0
	resolver.scan = func(context.Context) (map[uint64]model.ProcessInfo, bool) {
		scan := scans[scanCalls]
		scanCalls++
		return scan, true
	}

	first := resolver.Resolve(context.Background(), map[uint64]struct{}{1: {}, 2: {}})
	if scanCalls != 1 {
		t.Fatalf("expected initial full scan, got %d calls", scanCalls)
	}
	if first[1].PID != 100 || first[2].PID != 200 {
		t.Fatalf("unexpected first resolution: %+v", first)
	}

	now = now.Add(resolver.positiveTTL + time.Second)
	second := resolver.Resolve(context.Background(), map[uint64]struct{}{1: {}, 2: {}})
	if scanCalls != 2 {
		t.Fatalf("expected cache refresh scan, got %d calls", scanCalls)
	}
	if second[1].PID != 300 {
		t.Fatalf("expected refreshed PID for inode 1, got %+v", second[1])
	}
	if _, ok := second[2]; ok {
		t.Fatalf("expected stale inode 2 to be dropped after refresh")
	}
}

func TestProcessResolverNegativeCacheAvoidsRepeatedScans(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	resolver := newProcessResolver("/proc")
	resolver.now = func() time.Time { return now }

	scanCalls := 0
	resolver.scan = func(context.Context) (map[uint64]model.ProcessInfo, bool) {
		scanCalls++
		return map[uint64]model.ProcessInfo{}, true
	}

	requested := map[uint64]struct{}{99: {}}
	first := resolver.Resolve(context.Background(), requested)
	if len(first) != 0 || scanCalls != 1 {
		t.Fatalf("expected one miss scan, got results=%v scans=%d", first, scanCalls)
	}

	now = now.Add(5 * time.Second)
	second := resolver.Resolve(context.Background(), requested)
	if len(second) != 0 || scanCalls != 1 {
		t.Fatalf("expected negative cache to suppress rescan, got results=%v scans=%d", second, scanCalls)
	}

	now = now.Add(resolver.negativeTTL + time.Second)
	third := resolver.Resolve(context.Background(), requested)
	if len(third) != 0 || scanCalls != 2 {
		t.Fatalf("expected rescan after negative cache expiry, got results=%v scans=%d", third, scanCalls)
	}
}

func processInfo(pid int, comm string) model.ProcessInfo {
	return model.ProcessInfo{
		PID:  pid,
		Comm: comm,
		Exe:  "/usr/bin/" + comm,
	}
}
