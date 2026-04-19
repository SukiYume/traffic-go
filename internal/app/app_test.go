package app

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"traffic-go/internal/api"
	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func TestNewDerivesProcessLogDirsFromLegacyFields(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	cfg.NginxLogDir = "/legacy/nginx"

	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() {
		_ = application.Close()
	})

	if application.cfg.ProcessLogDirs["nginx"] != "/legacy/nginx" {
		t.Fatalf("expected derived nginx process log dir, got %v", application.cfg.ProcessLogDirs)
	}
}

func TestRunAggregationRefreshesLatestCompletedHour(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() {
		_ = application.Close()
	})

	ctx := context.Background()
	latestCompleteHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	minuteA := latestCompleteHour.Add(10 * time.Minute).Unix()
	minuteB := latestCompleteHour.Add(40 * time.Minute).Unix()

	firstUsage := map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minuteA,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   100,
			BytesDown: 40,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}
	if err := application.store.FlushMinute(ctx, minuteA, firstUsage, nil); err != nil {
		t.Fatalf("flush first minute: %v", err)
	}
	if err := application.store.AggregateHour(ctx, latestCompleteHour); err != nil {
		t.Fatalf("aggregate initial hour: %v", err)
	}
	if err := application.store.SetLastAggregatedHour(ctx, latestCompleteHour); err != nil {
		t.Fatalf("set aggregation cursor: %v", err)
	}

	lateUsage := map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minuteB,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   200,
			BytesDown: 60,
			PktsUp:    2,
			PktsDown:  2,
			FlowCount: 1,
		},
	}
	if err := application.store.FlushMinute(ctx, minuteB, lateUsage, nil); err != nil {
		t.Fatalf("flush late minute: %v", err)
	}

	application.runAggregation(ctx)

	stats, err := application.store.QueryOverview(ctx, latestCompleteHour, latestCompleteHour.Add(time.Hour), store.DataSourceHour)
	if err != nil {
		t.Fatalf("query hourly overview: %v", err)
	}
	if stats.BytesUp != 300 || stats.BytesDown != 100 {
		t.Fatalf("expected re-aggregated hourly totals, got %+v", stats)
	}
}

func TestRunPrefetchWarmsConfiguredProcessLogs(t *testing.T) {
	logDir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	cfg.ProcessLogDirs = map[string]string{"ss-server": logDir}

	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() {
		_ = application.Close()
	})

	ctx := context.Background()
	minuteTime := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	minute := minuteTime.Unix()
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte("2026-04-18T12:08:10Z /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443\n"), 0o644); err != nil {
		t.Fatalf("write ss log: %v", err)
	}

	pid := 1088
	if err := application.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 4096,
			PktsUp:    4,
			PktsDown:  7,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed usage row: %v", err)
	}

	application.apiServer.RunBackgroundPrefetch(ctx, api.BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 minuteTime.Add(30 * time.Second),
		EvidenceLookback:    application.cfg.Prefetch.EvidenceLookback,
		ChainLookback:       application.cfg.Prefetch.ChainLookback,
		ScanBudget:          application.cfg.Prefetch.ScanBudget,
		MaxScanFiles:        application.cfg.Prefetch.MaxScanFiles,
		MaxScanLinesPerFile: application.cfg.Prefetch.MaxScanLinesPerFile,
	})

	chains, err := application.store.QueryUsageChainsForProcess(ctx, minute, &pid, "ss-server", "/usr/bin/ss-server", store.DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query prefetched chains: %v", err)
	}
	if len(chains) == 0 {
		t.Fatalf("expected prefetched chain rows, got %+v", chains)
	}
}
