package app

import (
	"context"
	"database/sql"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"traffic-go/internal/api"
	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func TestNewLimitsHTTPHeaderBytes(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() { _ = application.Close() })
	if application.server.MaxHeaderBytes != maxHTTPHeaderBytes {
		t.Fatalf("expected max header bytes %d, got %d", maxHTTPHeaderBytes, application.server.MaxHeaderBytes)
	}
}

func TestScheduledCleanupNeverRunsVacuum(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	probeDB, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		t.Fatalf("open vacuum probe database: %v", err)
	}
	t.Cleanup(func() { _ = probeDB.Close() })
	if _, err := probeDB.Exec(`
CREATE TABLE vacuum_probe (payload BLOB);
INSERT INTO vacuum_probe (payload) VALUES (zeroblob(1048576));
DELETE FROM vacuum_probe;
`); err != nil {
		t.Fatalf("create free pages: %v", err)
	}
	var freeBefore int64
	if err := probeDB.QueryRow(`PRAGMA freelist_count`).Scan(&freeBefore); err != nil {
		t.Fatalf("query initial free pages: %v", err)
	}
	if freeBefore == 0 {
		t.Fatalf("expected deleted probe data to leave free pages")
	}

	ctx := context.Background()
	if err := application.cleanup(ctx, false, false); err != nil {
		t.Fatalf("scheduled cleanup: %v", err)
	}
	if _, ok, err := application.store.LastVacuum(ctx); err != nil || ok {
		t.Fatalf("scheduled cleanup must not create a vacuum marker, ok=%v err=%v", ok, err)
	}
	var freeAfterBaseline int64
	if err := probeDB.QueryRow(`PRAGMA freelist_count`).Scan(&freeAfterBaseline); err != nil {
		t.Fatalf("query free pages after baseline: %v", err)
	}
	if freeAfterBaseline == 0 {
		t.Fatalf("first scheduled cleanup unexpectedly ran VACUUM")
	}

	if err := application.RunMaintenanceOnce(ctx, true); err != nil {
		t.Fatalf("explicit vacuum maintenance: %v", err)
	}
	var freeAfterExplicit int64
	if err := probeDB.QueryRow(`PRAGMA freelist_count`).Scan(&freeAfterExplicit); err != nil {
		t.Fatalf("query free pages after explicit vacuum: %v", err)
	}
	if freeAfterExplicit != 0 {
		t.Fatalf("explicit maintenance should vacuum free pages, got %d", freeAfterExplicit)
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

	application.runAggregationAt(ctx, latestCompleteHour.Add(time.Hour).Add(2*time.Minute))

	stats, err := application.store.QueryOverview(ctx, latestCompleteHour, latestCompleteHour.Add(time.Hour), store.DataSourceHour)
	if err != nil {
		t.Fatalf("query hourly overview: %v", err)
	}
	if stats.BytesUp != 300 || stats.BytesDown != 100 {
		t.Fatalf("expected re-aggregated hourly totals, got %+v", stats)
	}
}

func TestRunAggregationDoesNotRepeatLatestCompletedHourOutsideGrace(t *testing.T) {
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
	now := time.Date(2026, 4, 16, 10, 30, 0, 0, time.UTC)
	latestCompleteHour := now.Truncate(time.Hour).Add(-time.Hour)
	minuteA := latestCompleteHour.Add(10 * time.Minute).Unix()
	minuteB := latestCompleteHour.Add(40 * time.Minute).Unix()
	key := model.UsageKey{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         42,
		Comm:        "curl",
		Exe:         "/usr/bin/curl",
		LocalPort:   50500,
		RemoteIP:    "1.1.1.1",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}

	key.MinuteTS = minuteA
	if err := application.store.FlushMinute(ctx, minuteA, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 100, BytesDown: 40, PktsUp: 1, PktsDown: 1, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush first minute: %v", err)
	}

	application.runAggregationAt(ctx, now)

	key.MinuteTS = minuteB
	if err := application.store.FlushMinute(ctx, minuteB, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 200, BytesDown: 60, PktsUp: 2, PktsDown: 2, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush late minute: %v", err)
	}

	application.runAggregationAt(ctx, now.Add(time.Minute))

	stats, err := application.store.QueryOverview(ctx, latestCompleteHour, latestCompleteHour.Add(time.Hour), store.DataSourceHour)
	if err != nil {
		t.Fatalf("query hourly overview: %v", err)
	}
	if stats.BytesUp != 100 || stats.BytesDown != 40 {
		t.Fatalf("expected second run outside grace to skip latest-hour refresh, got %+v", stats)
	}
}

func TestRunAggregationGraceRefreshesHourButFinalizesDayOnceAfterGrace(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() { _ = application.Close() })

	ctx := context.Background()
	hourStart := time.Date(2026, 4, 16, 9, 0, 0, 0, time.UTC)
	dayStart := hourStart.Truncate(24 * time.Hour)
	key := model.UsageKey{
		Proto: "tcp", Direction: model.DirectionOut, PID: 42, Comm: "curl", Exe: "/usr/bin/curl",
		LocalPort: 50500, RemoteIP: "1.1.1.1", RemotePort: 443, Attribution: model.AttributionExact,
	}
	firstMinute := hourStart.Add(10 * time.Minute).Unix()
	key.MinuteTS = firstMinute
	if err := application.store.FlushMinute(ctx, firstMinute, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 100, BytesDown: 40, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush first minute: %v", err)
	}

	application.runAggregationAt(ctx, hourStart.Add(time.Hour+2*time.Minute))
	hourStats, err := application.store.QueryOverview(ctx, hourStart, hourStart.Add(time.Hour), store.DataSourceHour)
	if err != nil {
		t.Fatalf("query grace hour: %v", err)
	}
	dayStats, err := application.store.QueryOverview(ctx, dayStart, dayStart.Add(24*time.Hour), store.DataSourceDay)
	if err != nil {
		t.Fatalf("query day during grace: %v", err)
	}
	if hourStats.BytesUp != 100 || dayStats.BytesUp != 0 {
		t.Fatalf("grace should refresh only the hour, hour=%+v day=%+v", hourStats, dayStats)
	}

	lateMinute := hourStart.Add(40 * time.Minute).Unix()
	key.MinuteTS = lateMinute
	if err := application.store.FlushMinute(ctx, lateMinute, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 200, BytesDown: 60, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush late minute: %v", err)
	}
	application.runAggregationAt(ctx, hourStart.Add(time.Hour+3*time.Minute))
	hourStats, err = application.store.QueryOverview(ctx, hourStart, hourStart.Add(time.Hour), store.DataSourceHour)
	if err != nil {
		t.Fatalf("query refreshed grace hour: %v", err)
	}
	if hourStats.BytesUp != 300 {
		t.Fatalf("expected second grace refresh to include the late minute, got %+v", hourStats)
	}

	application.runAggregationAt(ctx, hourStart.Add(time.Hour+latestCompletedHourRefreshGrace+time.Minute))
	dayStats, err = application.store.QueryOverview(ctx, dayStart, dayStart.Add(24*time.Hour), store.DataSourceDay)
	if err != nil {
		t.Fatalf("query finalized day: %v", err)
	}
	if dayStats.BytesUp != 300 || dayStats.BytesDown != 100 {
		t.Fatalf("expected one post-grace day finalization with complete totals, got %+v", dayStats)
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
		Now:                 minuteTime.Add(2*time.Minute + 30*time.Second),
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

func TestRunAggregationReplaysDirtyBackfilledChainHours(t *testing.T) {
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
	lateMinute := time.Date(2026, 4, 16, 2, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := application.store.SetLastAggregatedHour(ctx, lateMinute.Add(4*time.Hour).Truncate(time.Hour)); err != nil {
		t.Fatalf("set aggregation cursor: %v", err)
	}

	if err := application.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        lateMinute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         3,
			EvidenceCount:     2,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "chain-fp-1",
			SampleMessage:     "sample",
			SampleTime:        lateMinute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert late chain: %v", err)
	}

	application.runAggregation(ctx)

	rows, err := application.store.QueryUsageChainsForProcess(ctx, lateMinute.Truncate(time.Hour).Unix(), &pid, "ss-server", exe, store.DataSourceHourChain)
	if err != nil {
		t.Fatalf("query aggregated dirty chains: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one replayable hourly chain row, got %+v", rows)
	}
}

func TestRunWaitsForCollectorShutdownBeforeReturning(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	cfg.Listen = freeListenAddress(t)

	application, err := New(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	t.Cleanup(func() {
		_ = application.Close()
	})

	collector := &blockingCollector{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	application.collector = collector
	application.apiServer = api.NewServer(application.store, collector, log.New(io.Discard, "", 0), nil, nil, api.BasicAuthConfig{}, true, config.Default().Cache)
	application.server = &http.Server{
		Addr:    cfg.Listen,
		Handler: http.NewServeMux(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- application.Run(ctx)
	}()

	select {
	case <-collector.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("collector did not start")
	}

	cancel()

	select {
	case err := <-runDone:
		t.Fatalf("Run returned before collector finished shutdown: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(collector.release)

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not wait for collector shutdown")
	}

	select {
	case <-collector.returned:
	case <-time.After(2 * time.Second):
		t.Fatalf("collector did not finish shutdown")
	}
}

func freeListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve test listen address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release test listen address: %v", err)
	}
	return address
}

type blockingCollector struct {
	started  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func (c *blockingCollector) Start(ctx context.Context) error {
	close(c.started)
	<-ctx.Done()
	<-c.release
	close(c.returned)
	return nil
}

func (c *blockingCollector) ActiveProcesses() []model.ProcessListItem {
	return nil
}

func (c *blockingCollector) ActiveStats() model.ActiveStats {
	return model.ActiveStats{}
}

func (c *blockingCollector) ReadyStatus() model.CollectorReadiness {
	return model.CollectorReadiness{}
}

func (c *blockingCollector) Diagnostics() model.CollectorDiagnostics {
	return model.CollectorDiagnostics{
		AttributionCounts: make(map[string]int64),
	}
}
