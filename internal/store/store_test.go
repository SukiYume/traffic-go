package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
)

func TestOpenCreatesPrivateDatabaseDirectoryAndFile(t *testing.T) {
	cfg := config.Default()
	dbDir := filepath.Join(t.TempDir(), "private", "state")
	cfg.DBPath = filepath.Join(dbDir, "traffic.db")

	trafficStore, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = trafficStore.Close() })

	if _, err := os.Stat(cfg.DBPath); err != nil {
		t.Fatalf("database was not created: %v", err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	dirInfo, err := os.Stat(dbDir)
	if err != nil {
		t.Fatalf("stat database directory: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("expected database directory mode 0700, got %04o", got)
	}
	for _, path := range []string{cfg.DBPath, cfg.DBPath + "-wal", cfg.DBPath + "-shm"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat sqlite file %s: %v", filepath.Base(path), err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("expected %s mode 0600, got %04o", filepath.Base(path), got)
		}
	}
}

func TestFlushMinuteBatchDeduplicatesKnownProcessUpserts(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	if _, err := trafficStore.db.ExecContext(ctx, `
CREATE TABLE known_process_audit (id INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE TRIGGER audit_known_process_insert AFTER INSERT ON known_processes BEGIN
    INSERT INTO known_process_audit (id) VALUES (NULL);
END;
CREATE TRIGGER audit_known_process_update AFTER UPDATE ON known_processes BEGIN
    INSERT INTO known_process_audit (id) VALUES (NULL);
END;
`); err != nil {
		t.Fatalf("create known process audit: %v", err)
	}
	minute := time.Date(2026, 4, 20, 10, 15, 0, 0, time.UTC).Unix()
	common := model.UsageKey{
		MinuteTS: minute, Proto: "tcp", Direction: model.DirectionOut, PID: 42,
		Comm: "curl", Exe: "/usr/bin/curl", LocalPort: 50500, RemotePort: 443,
		Attribution: model.AttributionExact,
	}
	first := common
	first.RemoteIP = "1.1.1.1"
	second := common
	second.RemoteIP = "8.8.8.8"
	if err := trafficStore.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		first:  {BytesUp: 1},
		second: {BytesUp: 2},
	}, nil); err != nil {
		t.Fatalf("flush process batch: %v", err)
	}
	var writes int
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM known_process_audit`).Scan(&writes); err != nil {
		t.Fatalf("count known process writes: %v", err)
	}
	if writes != 1 {
		t.Fatalf("expected one known-process upsert for duplicate process dimensions, got %d", writes)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func TestSchemaCreatesLogEvidenceHostPortIndexes(t *testing.T) {
	store := newTestStore(t)

	for _, indexName := range []string{"idx_log_evidence_source_entry_ts_v2", "idx_log_evidence_source_host_port_ts_v2"} {
		row := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, indexName)
		var count int
		if err := row.Scan(&count); err != nil {
			t.Fatalf("count %s: %v", indexName, err)
		}
		if count != 1 {
			t.Fatalf("expected %s to exist, found %d", indexName, count)
		}
	}
	for _, legacyName := range []string{"idx_log_evidence_lookup", "idx_log_evidence_client_lookup", "idx_log_evidence_target_lookup", "idx_log_evidence_created_at", "idx_log_evidence_entry_port", "idx_log_evidence_host_port"} {
		row := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, legacyName)
		var count int
		if err := row.Scan(&count); err != nil {
			t.Fatalf("count legacy %s: %v", legacyName, err)
		}
		if count != 0 {
			t.Fatalf("obsolete evidence index %s should be removed", legacyName)
		}
	}
}

func TestFlushAndQueryInterfaceTimeseries(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 5, 1, 10, 3, 0, 0, time.UTC)
	if err := store.FlushInterfaceMinute(ctx, minute.Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 1024, TxBytes: 2048},
	}); err != nil {
		t.Fatalf("flush interface minute: %v", err)
	}
	if err := store.FlushInterfaceMinute(ctx, minute.Add(time.Minute).Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 4096, TxBytes: 8192},
		"ens3": {RxBytes: 512, TxBytes: 256},
	}); err != nil {
		t.Fatalf("flush second interface minute: %v", err)
	}

	points, err := store.QueryInterfaceTimeseries(ctx, minute, minute.Add(10*time.Minute), 5*time.Minute, DataSourceMinute)
	if err != nil {
		t.Fatalf("query interface timeseries: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("expected two interface points, got %+v", points)
	}
	if points[0].Interface != "ens3" || points[0].RxBytes != 512 || points[0].TxBytes != 256 {
		t.Fatalf("unexpected ens3 point: %+v", points[0])
	}
	if points[1].Interface != "eth0" || points[1].RxBytes != 5120 || points[1].TxBytes != 10240 {
		t.Fatalf("unexpected eth0 point: %+v", points[1])
	}
}

func TestFlushMinuteBatchRollsBackAllCounterFamiliesWhenInterfaceWriteFails(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 20, 10, 15, 0, 0, time.UTC).Unix()
	usage := map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 200, PktsUp: 1, PktsDown: 2, FlowCount: 1},
	}
	forward := map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  minute,
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 50500,
			OrigDPort: 443,
		}: {BytesUp: 300, BytesDown: 400, PktsUp: 3, PktsDown: 4, FlowCount: 1},
	}
	interfaces := map[string]model.InterfaceUsageDelta{"eth0": {RxBytes: 600, TxBytes: 500}}

	if _, err := trafficStore.db.ExecContext(ctx, `
CREATE TRIGGER fail_interface_insert
BEFORE INSERT ON interface_1m
BEGIN
    SELECT RAISE(ABORT, 'injected interface failure');
END`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	if err := trafficStore.FlushMinuteBatch(ctx, minute, usage, forward, interfaces); err == nil {
		t.Fatalf("expected injected interface write failure")
	}

	for _, table := range []string{"usage_1m", "usage_1m_forward", "interface_1m", "known_processes"} {
		var rows int
		if err := trafficStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&rows); err != nil {
			t.Fatalf("count %s after rollback: %v", table, err)
		}
		if rows != 0 {
			t.Fatalf("expected %s to roll back with the failed batch, found %d rows", table, rows)
		}
	}

	if _, err := trafficStore.db.ExecContext(ctx, `DROP TRIGGER fail_interface_insert`); err != nil {
		t.Fatalf("drop failure trigger: %v", err)
	}
	if err := trafficStore.FlushMinuteBatch(ctx, minute, usage, forward, interfaces); err != nil {
		t.Fatalf("retry atomic minute batch: %v", err)
	}

	var usageUp, forwardOrig, interfaceRX int64
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT bytes_up FROM usage_1m`).Scan(&usageUp); err != nil {
		t.Fatalf("query usage retry: %v", err)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT bytes_orig FROM usage_1m_forward`).Scan(&forwardOrig); err != nil {
		t.Fatalf("query forward retry: %v", err)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT rx_bytes FROM interface_1m`).Scan(&interfaceRX); err != nil {
		t.Fatalf("query interface retry: %v", err)
	}
	if usageUp != 100 || forwardOrig != 300 || interfaceRX != 600 {
		t.Fatalf("retry must commit each family exactly once, usage=%d forward=%d interface=%d", usageUp, forwardOrig, interfaceRX)
	}
}

func TestInterfaceUsageAggregatesToHourAndDay(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hour := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	if err := store.FlushInterfaceMinute(ctx, hour.Add(2*time.Minute).Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 1000, TxBytes: 2000},
	}); err != nil {
		t.Fatalf("flush first interface minute: %v", err)
	}
	if err := store.FlushInterfaceMinute(ctx, hour.Add(37*time.Minute).Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 3000, TxBytes: 4000},
	}); err != nil {
		t.Fatalf("flush second interface minute: %v", err)
	}

	if err := store.AggregateHour(ctx, hour); err != nil {
		t.Fatalf("aggregate interface hour: %v", err)
	}

	hourPoints, err := store.QueryInterfaceTimeseries(ctx, hour, hour.Add(time.Hour), time.Hour, DataSourceHour)
	if err != nil {
		t.Fatalf("query interface hour: %v", err)
	}
	if len(hourPoints) != 1 || hourPoints[0].DataSource != DataSourceInterfaceHour || hourPoints[0].RxBytes != 4000 || hourPoints[0].TxBytes != 6000 {
		t.Fatalf("unexpected hourly interface points: %+v", hourPoints)
	}

	day := hour.Truncate(24 * time.Hour)
	dayPoints, err := store.QueryInterfaceTimeseries(ctx, day, day.Add(24*time.Hour), 24*time.Hour, DataSourceDay)
	if err != nil {
		t.Fatalf("query interface day: %v", err)
	}
	if len(dayPoints) != 1 || dayPoints[0].DataSource != DataSourceInterfaceDay || dayPoints[0].RxBytes != 4000 || dayPoints[0].TxBytes != 6000 {
		t.Fatalf("unexpected daily interface points: %+v", dayPoints)
	}
}

func TestQueryInterfaceTimeseriesSummaryIncludesMinuteTail(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 8, 12, 35, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	oldHour := now.Add(-8 * 24 * time.Hour).Truncate(time.Hour)
	if err := store.FlushInterfaceMinute(ctx, oldHour.Add(5*time.Minute).Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 1000, TxBytes: 2000},
	}); err != nil {
		t.Fatalf("flush old interface minute: %v", err)
	}
	if err := store.AggregateHour(ctx, oldHour); err != nil {
		t.Fatalf("aggregate old interface hour: %v", err)
	}

	currentMinute := now.Truncate(time.Minute)
	if err := store.FlushInterfaceMinute(ctx, currentMinute.Unix(), map[string]model.InterfaceUsageDelta{
		"eth0": {RxBytes: 300, TxBytes: 700},
	}); err != nil {
		t.Fatalf("flush current interface minute: %v", err)
	}

	points, source, err := store.QueryInterfaceTimeseriesSummary(ctx, oldHour, now.Add(time.Minute), time.Hour)
	if err != nil {
		t.Fatalf("query interface summary: %v", err)
	}
	if source != DataSourceInterfaceHour {
		t.Fatalf("expected hourly summary source, got %s", source)
	}
	byBucket := make(map[int64]model.InterfaceTimeseriesPoint)
	for _, point := range points {
		if point.Interface == "eth0" {
			byBucket[point.BucketTS] = point
		}
	}
	oldPoint, ok := byBucket[oldHour.Unix()]
	if !ok || oldPoint.RxBytes != 1000 || oldPoint.TxBytes != 2000 {
		t.Fatalf("expected old hourly point, got %+v", points)
	}
	tailPoint, ok := byBucket[now.Truncate(time.Hour).Unix()]
	if !ok || tailPoint.RxBytes != 300 || tailPoint.TxBytes != 700 {
		t.Fatalf("expected current minute tail point, got %+v", points)
	}
}

func TestResolveUsageSource(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	start := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(31 * 24 * time.Hour)

	source, err := store.ResolveUsageSource(start, end, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceDay {
		t.Fatalf("unexpected source: %s", source)
	}

	if _, err := store.ResolveUsageSource(start, end, true); err != ErrDimensionUnavailable {
		t.Fatalf("expected dimension unavailable, got %v", err)
	}
}

func TestResolveUsageSourceRejectsWindowBeforeDailyRetention(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	start := dayRetentionStartUTC(now, store.retention).Add(-time.Second)
	if _, err := store.ResolveUsageSource(start, now, false); err != ErrRangeUnavailable {
		t.Fatalf("expected range unavailable before daily retention, got %v", err)
	}
}

func TestResolveUsageSourceUsesLongestActuallyAvailableRetentionLayer(t *testing.T) {
	trafficStore := newTestStore(t)
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }
	trafficStore.retention.MinuteDays = 7
	trafficStore.retention.HourMonths = 4
	trafficStore.retention.DayMonths = 1
	start := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)
	source, err := trafficStore.ResolveUsageSource(start, start.Add(time.Hour), false)
	if err != nil || source != DataSourceHour {
		t.Fatalf("expected still-retained hour data despite shorter day retention, source=%q err=%v", source, err)
	}

	trafficStore.retention.MinuteDays = 120
	trafficStore.retention.HourMonths = 1
	start = now.Add(-60 * 24 * time.Hour)
	source, err = trafficStore.ResolveUsageSource(start, start.Add(time.Hour), false)
	if err != nil || source != DataSourceMinute {
		t.Fatalf("expected longest minute retention to win over shorter aggregates, source=%q err=%v", source, err)
	}
}

func TestQueryInterfaceTimeseriesEnforcesPointLimit(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	start := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)
	_, err := trafficStore.db.ExecContext(ctx, `
WITH RECURSIVE a(x) AS (
    SELECT 0 UNION ALL SELECT x + 1 FROM a WHERE x < 100
), b(y) AS (
    SELECT 0 UNION ALL SELECT y + 1 FROM b WHERE y < 99
)
INSERT INTO interface_1m (minute_ts, interface, rx_bytes, tx_bytes)
SELECT ?, printf('if-%05d', x * 100 + y), 1, 1 FROM a CROSS JOIN b
`, start.Unix())
	if err != nil {
		t.Fatalf("seed interface points: %v", err)
	}

	_, err = trafficStore.QueryInterfaceTimeseries(ctx, start, start.Add(time.Minute), time.Minute, DataSourceMinute)
	if err != ErrTimeseriesPointLimit {
		t.Fatalf("expected interface point limit, got %v", err)
	}
}

func TestOpenSplitsSQLiteReadAndWriteConnections(t *testing.T) {
	store := newTestStore(t)
	if store.db.Stats().MaxOpenConnections != 1 {
		t.Fatalf("expected sqlite write pool to use one open connection, got %+v", store.db.Stats())
	}
	if store.readDB == nil {
		t.Fatalf("expected sqlite read pool")
	}
	if store.readDB.Stats().MaxOpenConnections <= 1 {
		t.Fatalf("expected sqlite read pool to allow concurrent readers, got %+v", store.readDB.Stats())
	}
	var queryOnly int
	if err := store.readDB.QueryRow(`PRAGMA query_only`).Scan(&queryOnly); err != nil {
		t.Fatalf("read query_only pragma: %v", err)
	}
	if queryOnly != 1 {
		t.Fatalf("expected read pool to be query_only, got %d", queryOnly)
	}
}

func TestFlushAndAggregate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 15, 1, 2, 0, 0, time.UTC).Unix()
	err := store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
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
			BytesDown: 200,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	if err := store.AggregateHour(ctx, time.Unix(minute, 0)); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	stats, err := store.QueryOverview(ctx, time.Unix(minute, 0), time.Unix(minute, 0).Add(time.Hour), DataSourceMinute)
	if err != nil {
		t.Fatalf("query overview: %v", err)
	}
	if stats.BytesUp != 100 || stats.BytesDown != 200 {
		t.Fatalf("unexpected overview: %+v", stats)
	}

	hourStart := time.Unix(minute, 0).Truncate(time.Hour)
	dayStart := time.Unix(minute, 0).UTC().Truncate(24 * time.Hour)
	dayStats, err := store.QueryOverview(ctx, dayStart, dayStart.Add(24*time.Hour), DataSourceDay)
	if err != nil {
		t.Fatalf("query daily overview: %v", err)
	}
	if dayStats.BytesUp != 100 || dayStats.BytesDown != 200 || dayStats.DataSource != DataSourceDay {
		t.Fatalf("unexpected daily overview: %+v", dayStats)
	}

	top, totalRows, err := store.QueryTopProcesses(ctx, hourStart, hourStart.Add(time.Hour), DataSourceHour, "comm", "total", "desc", 10, 0, true)
	if err != nil {
		t.Fatalf("query top: %v", err)
	}
	if totalRows != 1 {
		t.Fatalf("unexpected total rows: %d", totalRows)
	}
	if len(top) != 1 || top[0].Comm != "curl" {
		t.Fatalf("unexpected top rows: %+v", top)
	}
}

func TestCleanupSummarizesExpiredCalendarMonthBeforeDeletingDetails(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	january := time.Date(2026, 1, 20, 10, 15, 0, 0, time.UTC)
	february := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC) }

	if err := store.FlushMinute(ctx, january.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    january.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 200, PktsUp: 1, PktsDown: 2, FlowCount: 1},
	}, map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  january.Unix(),
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 50000,
			OrigDPort: 443,
		}: {BytesUp: 300, BytesDown: 400, PktsUp: 3, PktsDown: 4, FlowCount: 2},
	}); err != nil {
		t.Fatalf("flush january details: %v", err)
	}
	if err := store.FlushMinute(ctx, february.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    february.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50501,
			RemoteIP:    "8.8.8.8",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 900, BytesDown: 1000, PktsUp: 9, PktsDown: 10, FlowCount: 9},
	}, nil); err != nil {
		t.Fatalf("flush february details: %v", err)
	}
	if err := store.AggregateHour(ctx, january); err != nil {
		t.Fatalf("aggregate january details: %v", err)
	}
	if err := store.AggregateHour(ctx, february); err != nil {
		t.Fatalf("aggregate february details: %v", err)
	}

	if err := store.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{{
		Source:      "nginx",
		EventTS:     january.Add(time.Minute).Unix(),
		ClientIP:    "203.0.113.24",
		TargetIP:    "198.51.100.44",
		Host:        "example.test",
		Path:        "/",
		Method:      "GET",
		Message:     "sample",
		Fingerprint: "monthly-evidence-jan",
	}}); err != nil {
		t.Fatalf("upsert january evidence: %v", err)
	}
	pid := 42
	exe := "/usr/bin/curl"
	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{{
		TimeBucket:        january.Unix(),
		PID:               &pid,
		Comm:              "curl",
		Exe:               &exe,
		SourceIP:          "203.0.113.24",
		TargetIP:          "198.51.100.44",
		BytesTotal:        300,
		FlowCount:         1,
		EvidenceCount:     1,
		EvidenceSource:    "nginx",
		Confidence:        "high",
		SampleFingerprint: "monthly-evidence-jan",
		SampleMessage:     "sample",
		SampleTime:        january.Unix(),
	}}); err != nil {
		t.Fatalf("upsert january chain: %v", err)
	}
	if err := store.AggregateHour(ctx, january); err != nil {
		t.Fatalf("aggregate january chain details: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	var summary model.MonthlyUsageSummary
	err := store.db.QueryRow(`
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, updated_at
FROM usage_monthly
WHERE month_ts = ?
`, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(
		&summary.MonthTS,
		&summary.BytesUp,
		&summary.BytesDown,
		&summary.FlowCount,
		&summary.ForwardBytesOrig,
		&summary.ForwardBytesReply,
		&summary.ForwardFlowCount,
		&summary.EvidenceCount,
		&summary.ChainCount,
		&summary.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("query monthly summary: %v", err)
	}
	if summary.BytesUp != 100 || summary.BytesDown != 200 || summary.FlowCount != 1 {
		t.Fatalf("unexpected normal monthly summary: %+v", summary)
	}
	if summary.ForwardBytesOrig != 300 || summary.ForwardBytesReply != 400 || summary.ForwardFlowCount != 2 {
		t.Fatalf("unexpected forward monthly summary: %+v", summary)
	}
	if summary.EvidenceCount != 1 || summary.ChainCount != 1 {
		t.Fatalf("unexpected evidence/chain monthly summary: %+v", summary)
	}

	var remainingJanuary, remainingFebruary int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m WHERE minute_ts < ?`, february.Unix()).Scan(&remainingJanuary); err != nil {
		t.Fatalf("count remaining january detail: %v", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m WHERE minute_ts >= ?`, february.Unix()).Scan(&remainingFebruary); err != nil {
		t.Fatalf("count remaining february detail: %v", err)
	}
	if remainingJanuary != 0 || remainingFebruary != 0 {
		t.Fatalf("unexpected remaining detail rows: january=%d february=%d", remainingJanuary, remainingFebruary)
	}
}

func TestCleanupSummarizesClosedMonthBeforeItExpires(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	february := time.Date(2026, 2, 18, 10, 15, 0, 0, time.UTC)
	april := time.Date(2026, 4, 20, 12, 30, 0, 0, time.UTC)
	store.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }

	if err := store.FlushMinute(ctx, february.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    february.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 200, PktsUp: 1, PktsDown: 2, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush february details: %v", err)
	}
	if err := store.FlushMinute(ctx, april.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    april.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50501,
			RemoteIP:    "8.8.8.8",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 300, BytesDown: 400, PktsUp: 3, PktsDown: 4, FlowCount: 2},
	}, nil); err != nil {
		t.Fatalf("flush april details: %v", err)
	}
	if err := store.AggregateHour(ctx, february); err != nil {
		t.Fatalf("aggregate february details: %v", err)
	}
	if err := store.AggregateHour(ctx, april); err != nil {
		t.Fatalf("aggregate april details: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	var aprilSummary model.MonthlyUsageSummary
	if err := store.db.QueryRow(`
SELECT month_ts, bytes_up, bytes_down, flow_count
FROM usage_monthly
WHERE month_ts = ?
`, time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(
		&aprilSummary.MonthTS,
		&aprilSummary.BytesUp,
		&aprilSummary.BytesDown,
		&aprilSummary.FlowCount,
	); err != nil {
		t.Fatalf("query april monthly summary: %v", err)
	}
	if aprilSummary.BytesUp != 300 || aprilSummary.BytesDown != 400 || aprilSummary.FlowCount != 2 {
		t.Fatalf("unexpected april monthly summary: %+v", aprilSummary)
	}

	var aprilDetails, februaryDetails, februarySummaryBytes int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m WHERE minute_ts >= ? AND minute_ts < ?`, april.Unix(), april.Add(time.Minute).Unix()).Scan(&aprilDetails); err != nil {
		t.Fatalf("count april details: %v", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m WHERE minute_ts < ?`, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&februaryDetails); err != nil {
		t.Fatalf("count february details: %v", err)
	}
	if err := store.db.QueryRow(`SELECT bytes_up FROM usage_monthly WHERE month_ts = ?`, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&februarySummaryBytes); err != nil {
		t.Fatalf("query february monthly summary: %v", err)
	}
	if aprilDetails != 0 || februaryDetails != 0 || februarySummaryBytes != 100 {
		t.Fatalf("unexpected cleanup result: aprilDetails=%d februaryDetails=%d februarySummaryBytes=%d", aprilDetails, februaryDetails, februarySummaryBytes)
	}
}

func TestCleanupRetainsCurrentMonthShortTTLCountsForMonthlyArchive(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	expired := time.Date(2026, 4, 2, 10, 15, 0, 0, time.UTC)
	live := time.Date(2026, 4, 19, 11, 20, 0, 0, time.UTC)
	pid := 42
	exe := "/usr/bin/curl"

	if err := store.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{
		{
			Source:      "nginx",
			EventTS:     expired.Unix(),
			ClientIP:    "203.0.113.24",
			TargetIP:    "198.51.100.44",
			Host:        "old.example.test",
			Path:        "/",
			Method:      "GET",
			Message:     "expired",
			Fingerprint: "monthly-retained-expired-evidence",
		},
		{
			Source:      "nginx",
			EventTS:     live.Unix(),
			ClientIP:    "203.0.113.25",
			TargetIP:    "198.51.100.45",
			Host:        "live.example.test",
			Path:        "/",
			Method:      "GET",
			Message:     "live",
			Fingerprint: "monthly-retained-live-evidence",
		},
	}); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}

	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        expired.Unix(),
			PID:               &pid,
			Comm:              "curl",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			TargetIP:          "198.51.100.44",
			BytesTotal:        300,
			FlowCount:         1,
			EvidenceCount:     1,
			EvidenceSource:    "nginx",
			Confidence:        "high",
			SampleFingerprint: "monthly-retained-expired-evidence",
			SampleMessage:     "expired",
			SampleTime:        expired.Unix(),
		},
		{
			TimeBucket:        live.Unix(),
			PID:               &pid,
			Comm:              "curl",
			Exe:               &exe,
			SourceIP:          "203.0.113.25",
			TargetIP:          "198.51.100.45",
			BytesTotal:        400,
			FlowCount:         1,
			EvidenceCount:     1,
			EvidenceSource:    "nginx",
			Confidence:        "high",
			SampleFingerprint: "monthly-retained-live-evidence",
			SampleMessage:     "live",
			SampleTime:        live.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert chains: %v", err)
	}
	if err := store.AggregateHour(ctx, expired); err != nil {
		t.Fatalf("aggregate expired chain hour: %v", err)
	}

	store.now = func() time.Time { return time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC) }
	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup current month: %v", err)
	}

	summaries, err := store.QueryMonthlyUsage(ctx)
	if err != nil {
		t.Fatalf("query monthly usage: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected one monthly summary, got %+v", summaries)
	}
	if summaries[0].EvidenceCount != 2 || summaries[0].ChainCount != 2 {
		t.Fatalf("expected retained plus live counts, got %+v", summaries[0])
	}

	var expiredEvidenceRows, expiredChainRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM log_evidence WHERE event_ts = ?`, expired.Unix()).Scan(&expiredEvidenceRows); err != nil {
		t.Fatalf("count expired evidence: %v", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_chain_1h WHERE hour_ts = ?`, expired.Truncate(time.Hour).Unix()).Scan(&expiredChainRows); err != nil {
		t.Fatalf("count expired chain hour: %v", err)
	}
	if expiredEvidenceRows != 0 || expiredChainRows != 0 {
		t.Fatalf("expected expired short-TTL rows deleted, evidence=%d chain=%d", expiredEvidenceRows, expiredChainRows)
	}

	store.invalidateMonthlyCache()
	store.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }
	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup closed month: %v", err)
	}

	var evidenceCount, chainCount, retainedRows int
	if err := store.db.QueryRow(`SELECT evidence_count, chain_count FROM usage_monthly WHERE month_ts = ?`, time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&evidenceCount, &chainCount); err != nil {
		t.Fatalf("query archived monthly counts: %v", err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM monthly_retained_counts`).Scan(&retainedRows); err != nil {
		t.Fatalf("count retained rows: %v", err)
	}
	if evidenceCount != 2 || chainCount != 2 || retainedRows != 0 {
		t.Fatalf("unexpected archived counts evidence=%d chain=%d retainedRows=%d", evidenceCount, chainCount, retainedRows)
	}
}

func TestCleanupFallbackSummarizesMissingMonthBeforeDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	february := time.Date(2026, 2, 18, 10, 15, 0, 0, time.UTC)
	now := time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC)
	cutoff := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()
	if err := store.FlushMinute(ctx, february.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    february.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 500, BytesDown: 600, PktsUp: 5, PktsDown: 6, FlowCount: 7},
	}, nil); err != nil {
		t.Fatalf("flush february details: %v", err)
	}
	if err := store.AggregateHour(ctx, february); err != nil {
		t.Fatalf("aggregate february details: %v", err)
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	if err := store.summarizeCleanupMonthsIfMissing(ctx, tx, cutoff, now); err != nil {
		t.Fatalf("summarize cleanup months if missing: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM usage_1m WHERE minute_ts < ?`, cutoff); err != nil {
		t.Fatalf("delete expired details: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit cleanup fallback: %v", err)
	}

	var summary model.MonthlyUsageSummary
	if err := store.db.QueryRow(`
SELECT month_ts, bytes_up, bytes_down, flow_count
FROM usage_monthly
WHERE month_ts = ?
`, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(
		&summary.MonthTS,
		&summary.BytesUp,
		&summary.BytesDown,
		&summary.FlowCount,
	); err != nil {
		t.Fatalf("query february fallback summary: %v", err)
	}
	if summary.BytesUp != 500 || summary.BytesDown != 600 || summary.FlowCount != 7 {
		t.Fatalf("unexpected fallback summary: %+v", summary)
	}

	var detailRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m WHERE minute_ts < ?`, cutoff).Scan(&detailRows); err != nil {
		t.Fatalf("count expired details: %v", err)
	}
	if detailRows != 0 {
		t.Fatalf("expected expired details to be deleted after fallback summary, got %d", detailRows)
	}
}

func TestCleanupFallbackSummarizesEvidenceOnlyMonthBeforeDelete(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }

	february := time.Date(2026, 2, 18, 10, 15, 0, 0, time.UTC)
	if err := store.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{{
		Source:      "nginx",
		EventTS:     february.Unix(),
		ClientIP:    "203.0.113.24",
		TargetIP:    "198.51.100.44",
		Host:        "example.test",
		Path:        "/",
		Method:      "GET",
		Message:     "evidence-only",
		Fingerprint: "monthly-evidence-only-feb",
	}}); err != nil {
		t.Fatalf("upsert february evidence: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	var evidenceCount int64
	if err := store.db.QueryRow(`
SELECT evidence_count
FROM usage_monthly
WHERE month_ts = ?
`, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&evidenceCount); err != nil {
		t.Fatalf("query evidence-only monthly summary: %v", err)
	}
	if evidenceCount != 1 {
		t.Fatalf("unexpected evidence-only monthly summary count: %d", evidenceCount)
	}

	var remainingEvidence int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM log_evidence WHERE event_ts < ?`, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(&remainingEvidence); err != nil {
		t.Fatalf("count expired evidence: %v", err)
	}
	if remainingEvidence != 0 {
		t.Fatalf("expected expired evidence to be deleted after fallback summary, got %d", remainingEvidence)
	}
}

func TestCleanupSummarizesExpiredHourOnlyMonth(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC) }

	decemberDay := time.Date(2025, 12, 15, 0, 0, 0, 0, time.UTC).Unix()
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO usage_1d (
	day_ts, proto, direction, comm, local_port, remote_ip,
	bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 50500, '1.1.1.1', 700, 800, 7, 8, 9);
INSERT INTO usage_1d_forward (
	day_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
	bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
) VALUES (?, 'tcp', '10.0.0.2', '1.1.1.1', 50000, 443, 300, 400, 3, 4, 5);
`, decemberDay, decemberDay); err != nil {
		t.Fatalf("seed day-only month: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	var summary model.MonthlyUsageSummary
	if err := store.db.QueryRow(`
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply, forward_flow_count
FROM usage_monthly
WHERE month_ts = ?
`, time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC).Unix()).Scan(
		&summary.MonthTS,
		&summary.BytesUp,
		&summary.BytesDown,
		&summary.FlowCount,
		&summary.ForwardBytesOrig,
		&summary.ForwardBytesReply,
		&summary.ForwardFlowCount,
	); err != nil {
		t.Fatalf("query hour-only monthly summary: %v", err)
	}
	if summary.BytesUp != 700 || summary.BytesDown != 800 || summary.FlowCount != 9 {
		t.Fatalf("unexpected hour-only normal summary: %+v", summary)
	}
	if summary.ForwardBytesOrig != 300 || summary.ForwardBytesReply != 400 || summary.ForwardFlowCount != 5 {
		t.Fatalf("unexpected hour-only forward summary: %+v", summary)
	}
}

func TestQueryMonthlyUsageCombinesArchiveAndLiveMonths(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC) }

	january := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	march := time.Date(2026, 3, 12, 8, 15, 0, 0, time.UTC)
	april := time.Date(2026, 4, 24, 11, 0, 0, 0, time.UTC)
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO usage_monthly (
	month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
	forward_flow_count, evidence_count, chain_count, updated_at
) VALUES
	(?, 100, 200, 3, 400, 500, 6, 7, 8, 9),
	(?, 999, 999, 99, 0, 0, 0, 0, 0, 10)
`, january, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()); err != nil {
		t.Fatalf("seed monthly archive: %v", err)
	}

	if err := store.FlushMinute(ctx, march.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    march.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 11, BytesDown: 22, PktsUp: 1, PktsDown: 2, FlowCount: 1},
	}, map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  march.Unix(),
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 50000,
			OrigDPort: 443,
		}: {BytesUp: 33, BytesDown: 44, PktsUp: 3, PktsDown: 4, FlowCount: 2},
	}); err != nil {
		t.Fatalf("flush march details: %v", err)
	}
	if err := store.AggregateHour(ctx, march); err != nil {
		t.Fatalf("aggregate march details: %v", err)
	}
	if err := store.FlushMinute(ctx, april.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    april.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         99,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "2.2.2.2",
			RemotePort:  55000,
			Attribution: model.AttributionExact,
		}: {BytesUp: 55, BytesDown: 66, PktsUp: 5, PktsDown: 6, FlowCount: 7},
	}, nil); err != nil {
		t.Fatalf("flush april details: %v", err)
	}

	summaries, err := store.QueryMonthlyUsage(ctx)
	if err != nil {
		t.Fatalf("query monthly usage: %v", err)
	}
	if len(summaries) != 3 {
		t.Fatalf("unexpected summary count: %+v", summaries)
	}
	if summaries[0].MonthTS != time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("expected april first, got %+v", summaries[0])
	}
	if summaries[0].Archived || !summaries[0].DetailAvailable || summaries[0].DetailRange != "this_month" {
		t.Fatalf("unexpected april detail status: %+v", summaries[0])
	}
	if summaries[0].BytesUp != 55 || summaries[0].BytesDown != 66 || summaries[0].FlowCount != 7 {
		t.Fatalf("unexpected april live summary: %+v", summaries[0])
	}
	if summaries[1].MonthTS != time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("expected march second, got %+v", summaries[1])
	}
	if !summaries[1].Archived || !summaries[1].DetailAvailable || summaries[1].DetailRange != "last_month" {
		t.Fatalf("unexpected march archive status: %+v", summaries[1])
	}
	if summaries[1].BytesUp != 1010 || summaries[1].BytesDown != 1021 || summaries[1].FlowCount != 100 ||
		summaries[1].ForwardBytesOrig != 33 || summaries[1].ForwardBytesReply != 44 || summaries[1].ForwardFlowCount != 2 {
		t.Fatalf("late retained detail should increment the archived month exactly once: %+v", summaries[1])
	}
	if !summaries[2].Archived || !summaries[2].DetailAvailable || summaries[2].DetailRange != "" {
		t.Fatalf("unexpected january archive status: %+v", summaries[2])
	}
	if summaries[2].BytesUp != 100 || summaries[2].BytesDown != 200 || summaries[2].EvidenceCount != 7 || summaries[2].ChainCount != 8 {
		t.Fatalf("unexpected january archive summary: %+v", summaries[2])
	}
}

func TestQueryMonthlyUsageDoesNotDoubleCountCutoffHour(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 5, 31, 7, 44, 30, 0, time.UTC) }

	cutoffHour := time.Date(2026, 5, 24, 7, 0, 0, 0, time.UTC)
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO usage_1h (
	hour_ts, proto, direction, comm, local_port, remote_ip,
	bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 50500, '1.1.1.1', 100, 200, 1, 2, 3)
`, cutoffHour.Unix()); err != nil {
		t.Fatalf("seed cutoff hour aggregate: %v", err)
	}
	if err := store.FlushMinute(ctx, cutoffHour.Add(45*time.Minute).Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    cutoffHour.Add(45 * time.Minute).Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 10, BytesDown: 20, PktsUp: 1, PktsDown: 1, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("seed retained minute in cutoff hour: %v", err)
	}

	summaries, err := store.QueryMonthlyUsage(ctx)
	if err != nil {
		t.Fatalf("query monthly usage: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("expected one monthly summary, got %+v", summaries)
	}
	if summaries[0].BytesUp != 100 || summaries[0].BytesDown != 200 || summaries[0].FlowCount != 3 {
		t.Fatalf("cutoff hour should be counted once from hourly aggregate, got %+v", summaries[0])
	}
}

func TestCleanupDoesNotOverwriteExistingMonthlyArchiveWithExpiredDetail(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC) }

	aprilMonth := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix()
	aprilDay := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC).Unix()
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO usage_monthly (
	month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
	forward_flow_count, evidence_count, chain_count, updated_at
) VALUES (?, 100, 200, 3, 0, 0, 0, 7, 8, 9);
INSERT INTO usage_1d (
	day_ts, proto, direction, comm, local_port, remote_ip,
	bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 50500, '1.1.1.1', 100, 200, 1, 2, 3);
`, aprilMonth, aprilDay); err != nil {
		t.Fatalf("seed archived month and retained day detail: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	var evidenceCount, chainCount int64
	if err := store.db.QueryRow(`SELECT evidence_count, chain_count FROM usage_monthly WHERE month_ts = ?`, aprilMonth).Scan(&evidenceCount, &chainCount); err != nil {
		t.Fatalf("query monthly archive: %v", err)
	}
	if evidenceCount != 7 || chainCount != 8 {
		t.Fatalf("existing monthly archive counts should be preserved, evidence=%d chain=%d", evidenceCount, chainCount)
	}
}

func TestArchivedMonthAcceptsLateTrafficEvidenceAndChainWithoutDoubleCountingUpserts(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	initial := time.Date(2026, 4, 20, 10, 15, 0, 0, time.UTC)
	late := time.Date(2026, 4, 10, 8, 25, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }

	usageKey := model.UsageKey{
		MinuteTS: initial.Unix(), Proto: "tcp", Direction: model.DirectionOut, PID: 42,
		Comm: "curl", Exe: "/usr/bin/curl", LocalPort: 50500, RemoteIP: "1.1.1.1",
		RemotePort: 443, Attribution: model.AttributionExact,
	}
	forwardKey := model.ForwardUsageKey{
		MinuteTS: initial.Unix(), Proto: "tcp", OrigSrcIP: "10.0.0.2", OrigDstIP: "1.1.1.1",
		OrigSPort: 50500, OrigDPort: 443,
	}
	if err := trafficStore.FlushMinute(ctx, initial.Unix(), map[model.UsageKey]model.UsageDelta{
		usageKey: {BytesUp: 100, BytesDown: 200, FlowCount: 1},
	}, map[model.ForwardUsageKey]model.UsageDelta{
		forwardKey: {BytesUp: 300, BytesDown: 400, FlowCount: 1},
	}); err != nil {
		t.Fatalf("flush initial traffic: %v", err)
	}
	if err := trafficStore.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{{
		Source: "nginx", EventTS: initial.Unix(), Fingerprint: "archive-initial-evidence",
	}}); err != nil {
		t.Fatalf("insert initial evidence: %v", err)
	}
	pid := 42
	exe := "/usr/bin/curl"
	initialChain := model.UsageChainRecord{
		TimeBucket: initial.Unix(), PID: &pid, Comm: "curl", Exe: &exe,
		SourceIP: "203.0.113.10", TargetIP: "198.51.100.10", BytesTotal: 300,
		FlowCount: 1, EvidenceCount: 1, SampleFingerprint: "archive-initial-chain",
	}
	if err := trafficStore.UpsertUsageChains(ctx, []model.UsageChainRecord{initialChain}); err != nil {
		t.Fatalf("insert initial chain: %v", err)
	}
	if err := trafficStore.AggregateHour(ctx, initial); err != nil {
		t.Fatalf("aggregate initial hour: %v", err)
	}
	if err := trafficStore.Cleanup(ctx); err != nil {
		t.Fatalf("archive initial month: %v", err)
	}

	usageKey.MinuteTS = late.Unix()
	forwardKey.MinuteTS = late.Unix()
	if err := trafficStore.FlushMinute(ctx, late.Unix(), map[model.UsageKey]model.UsageDelta{
		usageKey: {BytesUp: 11, BytesDown: 22, FlowCount: 2},
	}, map[model.ForwardUsageKey]model.UsageDelta{
		forwardKey: {BytesUp: 33, BytesDown: 44, FlowCount: 2},
	}); err != nil {
		t.Fatalf("flush late archived traffic: %v", err)
	}
	lateEvidence := model.LogEvidence{
		Source: "nginx", EventTS: late.Unix(), Fingerprint: "archive-late-evidence", Message: "late",
	}
	if err := trafficStore.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{lateEvidence}); err != nil {
		t.Fatalf("insert late evidence: %v", err)
	}
	lateChain := model.UsageChainRecord{
		TimeBucket: late.Unix(), PID: &pid, Comm: "curl", Exe: &exe,
		SourceIP: "203.0.113.11", TargetIP: "198.51.100.11", BytesTotal: 55,
		FlowCount: 1, EvidenceCount: 1, SampleFingerprint: "archive-late-chain",
	}
	if err := trafficStore.UpsertUsageChains(ctx, []model.UsageChainRecord{lateChain}); err != nil {
		t.Fatalf("insert late chain: %v", err)
	}
	if err := trafficStore.AggregateHour(ctx, late); err != nil {
		t.Fatalf("aggregate late chain hour: %v", err)
	}

	// Conflict updates must not look like additional late rows to the archive.
	lateEvidence.Message = "late-updated"
	if err := trafficStore.UpsertLogEvidenceBatch(ctx, []model.LogEvidence{lateEvidence}); err != nil {
		t.Fatalf("update late evidence: %v", err)
	}
	lateChain.BytesTotal = 66
	if err := trafficStore.UpsertUsageChains(ctx, []model.UsageChainRecord{lateChain}); err != nil {
		t.Fatalf("update late chain: %v", err)
	}
	if err := trafficStore.AggregateHour(ctx, late); err != nil {
		t.Fatalf("reaggregate updated late chain: %v", err)
	}
	if err := trafficStore.Cleanup(ctx); err != nil {
		t.Fatalf("refresh archived month: %v", err)
	}

	var bytesUp, bytesDown, flows, forwardOrig, forwardReply, forwardFlows, evidence, chains int64
	monthTS := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix()
	if err := trafficStore.db.QueryRowContext(ctx, `
SELECT bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count
FROM usage_monthly WHERE month_ts = ?
`, monthTS).Scan(&bytesUp, &bytesDown, &flows, &forwardOrig, &forwardReply, &forwardFlows, &evidence, &chains); err != nil {
		t.Fatalf("query refreshed archive: %v", err)
	}
	if bytesUp != 111 || bytesDown != 222 || flows != 3 || forwardOrig != 333 || forwardReply != 444 || forwardFlows != 3 {
		t.Fatalf("unexpected late traffic totals: up=%d down=%d flows=%d fwd=%d/%d/%d", bytesUp, bytesDown, flows, forwardOrig, forwardReply, forwardFlows)
	}
	if evidence != 2 || chains != 2 {
		t.Fatalf("late row upserts must count once, evidence=%d chains=%d", evidence, chains)
	}
}

func TestCleanupPreservesExpiredDirtyChainUntilItCanBeAggregated(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	late := time.Date(2026, 4, 1, 8, 25, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }
	pid := 42
	exe := "/usr/bin/curl"
	if err := trafficStore.UpsertUsageChains(ctx, []model.UsageChainRecord{{
		TimeBucket: late.Unix(), PID: &pid, Comm: "curl", Exe: &exe,
		SourceIP: "203.0.113.11", TargetIP: "198.51.100.11", BytesTotal: 55,
		FlowCount: 1, EvidenceCount: 1, SampleFingerprint: "expired-dirty-chain",
	}}); err != nil {
		t.Fatalf("insert expired dirty chain: %v", err)
	}
	if err := trafficStore.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup before dirty aggregation: %v", err)
	}
	var minuteRows, dirtyRows int
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_chain_1m`).Scan(&minuteRows); err != nil {
		t.Fatalf("count preserved minute chain: %v", err)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dirty_chain_hours`).Scan(&dirtyRows); err != nil {
		t.Fatalf("count preserved dirty hour: %v", err)
	}
	if minuteRows != 1 || dirtyRows != 1 {
		t.Fatalf("cleanup must preserve dirty source rows, minute=%d dirty=%d", minuteRows, dirtyRows)
	}

	if err := trafficStore.AggregateHour(ctx, late); err != nil {
		t.Fatalf("aggregate preserved dirty hour: %v", err)
	}
	if err := trafficStore.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup after dirty aggregation: %v", err)
	}
	var archiveChains int64
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT chain_count FROM usage_monthly WHERE month_ts = ?`, monthStartUTC(late).Unix()).Scan(&archiveChains); err != nil {
		t.Fatalf("query archived late chain: %v", err)
	}
	if archiveChains != 1 {
		t.Fatalf("expected preserved dirty chain in monthly archive, got %d", archiveChains)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_chain_1m`).Scan(&minuteRows); err != nil {
		t.Fatalf("count cleaned minute chain: %v", err)
	}
	if minuteRows != 0 {
		t.Fatalf("aggregated expired minute chain should be cleaned, got %d", minuteRows)
	}
}

func TestAggregateHoursRecomputesEachAffectedDayOnce(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	day := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	if _, err := trafficStore.db.ExecContext(ctx, `
CREATE TABLE day_aggregation_audit (id INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE TRIGGER audit_usage_day_insert AFTER INSERT ON usage_1d BEGIN
    INSERT INTO day_aggregation_audit (id) VALUES (NULL);
END;
CREATE TRIGGER audit_usage_day_update AFTER UPDATE ON usage_1d BEGIN
    INSERT INTO day_aggregation_audit (id) VALUES (NULL);
END;
`); err != nil {
		t.Fatalf("create day aggregation audit: %v", err)
	}

	hours := []time.Time{day.Add(time.Hour), day.Add(2 * time.Hour), day.Add(3 * time.Hour)}
	for index, hour := range hours {
		minute := hour.Add(5 * time.Minute).Unix()
		if err := trafficStore.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
			{
				MinuteTS: minute, Proto: "tcp", Direction: model.DirectionOut, Comm: "curl",
				LocalPort: 50500, RemoteIP: "1.1.1.1", RemotePort: 443, Attribution: model.AttributionExact,
			}: {BytesUp: int64(index + 1), FlowCount: 1},
		}, nil); err != nil {
			t.Fatalf("seed hour %s: %v", hour, err)
		}
	}
	if err := trafficStore.AggregateHours(ctx, hours); err != nil {
		t.Fatalf("aggregate catch-up hours: %v", err)
	}
	var dayWrites, bytesUp int64
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM day_aggregation_audit`).Scan(&dayWrites); err != nil {
		t.Fatalf("count day writes: %v", err)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT bytes_up FROM usage_1d WHERE day_ts = ?`, day.Unix()).Scan(&bytesUp); err != nil {
		t.Fatalf("query day total: %v", err)
	}
	if dayWrites != 1 || bytesUp != 6 {
		t.Fatalf("expected one day recomputation with full total, writes=%d bytes=%d", dayWrites, bytesUp)
	}
}

func TestCleanupRemovesStaleKnownProcessesButKeepsRecentEntries(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }
	if _, err := trafficStore.db.ExecContext(ctx, `
INSERT INTO known_processes (pid, comm, exe, seen_ts) VALUES
    (1, 'stale', '/usr/bin/stale', ?),
    (2, 'recent', '/usr/bin/recent', ?)
`, now.Add(-knownProcessRetention-time.Hour).Unix(), now.Add(-time.Hour).Unix()); err != nil {
		t.Fatalf("seed known processes: %v", err)
	}
	if err := trafficStore.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	var stale, recent int
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM known_processes WHERE comm = 'stale'`).Scan(&stale); err != nil {
		t.Fatalf("count stale process: %v", err)
	}
	if err := trafficStore.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM known_processes WHERE comm = 'recent'`).Scan(&recent); err != nil {
		t.Fatalf("count recent process: %v", err)
	}
	if stale != 0 || recent != 1 {
		t.Fatalf("unexpected known process retention result stale=%d recent=%d", stale, recent)
	}
}

func TestPassiveWALCheckpointDoesNotWaitForActiveReader(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 20, 10, 15, 0, 0, time.UTC).Unix()
	key := model.UsageKey{
		MinuteTS: minute, Proto: "tcp", Direction: model.DirectionOut, Comm: "curl",
		LocalPort: 50500, RemoteIP: "1.1.1.1", RemotePort: 443, Attribution: model.AttributionExact,
	}
	if err := trafficStore.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 1},
	}, nil); err != nil {
		t.Fatalf("seed first WAL frame: %v", err)
	}

	readTx, err := trafficStore.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read snapshot: %v", err)
	}
	defer readTx.Rollback()
	var rows int
	if err := readTx.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_1m`).Scan(&rows); err != nil {
		t.Fatalf("establish read snapshot: %v", err)
	}
	key.MinuteTS = minute + 60
	if err := trafficStore.FlushMinute(ctx, minute+60, map[model.UsageKey]model.UsageDelta{
		key: {BytesUp: 2},
	}, nil); err != nil {
		t.Fatalf("seed WAL frame after read snapshot: %v", err)
	}

	checkpointCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	started := time.Now()
	if err := trafficStore.CheckpointWAL(checkpointCtx); err != nil {
		t.Fatalf("passive checkpoint with active reader: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("passive checkpoint waited for active reader: %v", elapsed)
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("release read snapshot: %v", err)
	}
	if err := trafficStore.CheckpointWALTruncate(ctx); err != nil {
		t.Fatalf("truncate checkpoint after reader release: %v", err)
	}
}

func TestNextPendingAggregationHourIncludesForwardOnlyMinutes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 15, 2, 10, 0, 0, time.UTC).Unix()
	err := store.FlushMinute(ctx, minute, nil, map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  minute,
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 51122,
			OrigDPort: 443,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    4,
			PktsDown:  8,
			FlowCount: 1,
		},
	})
	if err != nil {
		t.Fatalf("flush forward minute: %v", err)
	}

	nextHour, ok, err := store.NextPendingAggregationHour(ctx, nil, time.Unix(minute, 0).Add(2*time.Hour))
	if err != nil {
		t.Fatalf("next pending aggregation hour: %v", err)
	}
	if !ok {
		t.Fatalf("expected forward-only minute to schedule aggregation")
	}
	expectedHour := time.Unix(minute, 0).UTC().Truncate(time.Hour)
	if !nextHour.Equal(expectedHour) {
		t.Fatalf("unexpected next pending hour: got %s want %s", nextHour, expectedHour)
	}
}

func TestUpsertUsageChainsMarksDirtyHourOutsideAggregationCursor(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	lateMinute := time.Date(2026, 4, 15, 2, 10, 0, 0, time.UTC)
	laterHour := lateMinute.Add(4 * time.Hour).Truncate(time.Hour)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := store.SetLastAggregatedHour(ctx, laterHour); err != nil {
		t.Fatalf("set aggregation cursor: %v", err)
	}

	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{
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
		t.Fatalf("upsert usage chains: %v", err)
	}

	dirtyHour, ok, err := store.NextDirtyChainHour(ctx, laterHour.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("next dirty chain hour: %v", err)
	}
	if !ok {
		t.Fatalf("expected late chain backfill to mark a dirty hour")
	}
	if !dirtyHour.Equal(lateMinute.Truncate(time.Hour)) {
		t.Fatalf("unexpected dirty chain hour: got %s want %s", dirtyHour, lateMinute.Truncate(time.Hour))
	}

	if err := store.AggregateHour(ctx, dirtyHour); err != nil {
		t.Fatalf("aggregate dirty chain hour: %v", err)
	}

	if _, ok, err := store.NextDirtyChainHour(ctx, laterHour.Add(2*time.Hour)); err != nil {
		t.Fatalf("query dirty chain hour after aggregate: %v", err)
	} else if ok {
		t.Fatalf("expected dirty chain hour to be cleared after aggregation")
	}
}

func TestQueryTimeseriesUsesTimeColumnForBucketing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 15, 1, 2, 0, 0, time.UTC)
	err := store.FlushMinute(ctx, minute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute.Unix(),
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
			BytesDown: 200,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	points, err := store.QueryTimeseries(ctx, model.TimeseriesQuery{
		Start:   minute.Truncate(time.Hour),
		End:     minute.Truncate(time.Hour).Add(time.Hour),
		Bucket:  time.Minute,
		GroupBy: "direction",
	}, DataSourceMinute)
	if err != nil {
		t.Fatalf("query timeseries: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: %d", len(points))
	}
	if points[0].BucketTS != minute.Unix() || points[0].Group != string(model.DirectionOut) {
		t.Fatalf("unexpected point: %+v", points[0])
	}
	if points[0].BytesUp != 100 || points[0].BytesDown != 200 || points[0].FlowCount != 1 {
		t.Fatalf("unexpected aggregate values: %+v", points[0])
	}
}

func TestResolveUsageSourceUsesRetainedDetailBeforeDailyFallback(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

	source, err := store.ResolveUsageSource(start, end, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceHour {
		t.Fatalf("expected long window inside hourly retention to use hour source, got %s", source)
	}

	_, err = store.ResolveUsageSource(start, end, true)
	if err != ErrDimensionUnavailable {
		t.Fatalf("expected minute-required long window outside 7d retention to fail, got %v", err)
	}

	fullMinuteStart := now.Add(-7 * 24 * time.Hour).Truncate(time.Minute)
	source, err = store.ResolveUsageSource(fullMinuteStart, now, false)
	if err != nil {
		t.Fatalf("resolve minute retention source: %v", err)
	}
	if source != DataSourceMinute {
		t.Fatalf("expected full retained minute window to keep minute source, got %s", source)
	}
}

func TestResolveUsageSourceFallsBackForOldAbsoluteWindow(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	start := time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)

	source, err := store.ResolveUsageSource(start, end, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceDay {
		t.Fatalf("expected old window to use day source, got %s", source)
	}

	if _, err := store.ResolveUsageSource(start, end, true); err != ErrDimensionUnavailable {
		t.Fatalf("expected minute-only dimensions to be unavailable, got %v", err)
	}
}

func TestQueryUsageHourlyReturnsNullForMinuteOnlyDimensions(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 15, 1, 2, 0, 0, time.UTC)
	if err := store.FlushMinute(ctx, minute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute.Unix(),
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
			BytesDown: 200,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("flush minute: %v", err)
	}
	if err := store.AggregateHour(ctx, minute); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	rows, _, _, err := store.QueryUsage(ctx, model.UsageQuery{
		Start: minute.Truncate(time.Hour),
		End:   minute.Truncate(time.Hour).Add(time.Hour),
		Limit: 10,
	}, DataSourceHour)
	if err != nil {
		t.Fatalf("query usage: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected row count: %d", len(rows))
	}
	if rows[0].PID != nil || rows[0].Exe != nil || rows[0].RemotePort != nil || rows[0].Attribution != nil {
		t.Fatalf("expected null hourly-only dimensions, got %+v", rows[0])
	}
}

func TestQueryUsageHourlyCollapsesOutboundEphemeralLocalPorts(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hour := time.Date(2026, 4, 30, 23, 0, 0, 0, time.UTC)
	if err := store.FlushMinute(ctx, hour.Add(2*time.Minute).Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    hour.Add(2 * time.Minute).Unix(),
			Proto:       "udp",
			Direction:   model.DirectionOut,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   60997,
			RemoteIP:    "2001:4860:4860::8888",
			RemotePort:  53,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   160,
			BytesDown: 350,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
		{
			MinuteTS:    hour.Add(8 * time.Minute).Unix(),
			Proto:       "udp",
			Direction:   model.DirectionOut,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   60900,
			RemoteIP:    "2001:4860:4860::8888",
			RemotePort:  53,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   170,
			BytesDown: 262,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
		{
			MinuteTS:    hour.Add(12 * time.Minute).Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   80,
			RemoteIP:    "198.51.100.24",
			RemotePort:  50000,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   100,
			BytesDown: 200,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
		{
			MinuteTS:    hour.Add(18 * time.Minute).Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "198.51.100.24",
			RemotePort:  50001,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   300,
			BytesDown: 400,
			PktsUp:    2,
			PktsDown:  2,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("flush minute: %v", err)
	}
	if err := store.AggregateHour(ctx, hour); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	rows, _, totalRows, err := store.QueryUsage(ctx, model.UsageQuery{
		Start:        hour,
		End:          hour.Add(time.Hour),
		UsePage:      true,
		Page:         1,
		PageSize:     10,
		IncludeTotal: true,
		SortBy:       "bytes_total",
		SortOrder:    "desc",
	}, DataSourceHour)
	if err != nil {
		t.Fatalf("query hourly usage: %v", err)
	}
	if totalRows != 3 || len(rows) != 3 {
		t.Fatalf("expected one collapsed outbound row plus two inbound ports, total=%d rows=%+v", totalRows, rows)
	}
	rowTotal := func(row model.UsageRecord) int64 {
		return row.BytesUp + row.BytesDown
	}
	if rows[0].RemoteIP != "2001:4860:4860::8888" || rowTotal(rows[0]) != 942 {
		t.Fatalf("expected largest collapsed DNS row first after total-byte sort, got %+v", rows)
	}
	if rows[1].LocalPort != 443 || rowTotal(rows[1]) != 700 {
		t.Fatalf("expected second largest inbound 443 row after total-byte sort, got %+v", rows)
	}
	if rows[2].LocalPort != 80 || rowTotal(rows[2]) != 300 {
		t.Fatalf("expected smallest inbound 80 row after total-byte sort, got %+v", rows)
	}

	var dnsRows int
	for _, row := range rows {
		if row.Direction == model.DirectionOut && row.RemoteIP == "2001:4860:4860::8888" {
			dnsRows++
			if row.LocalPort != 0 {
				t.Fatalf("expected collapsed outbound local port to be zero, got %+v", row)
			}
			if row.BytesUp != 330 || row.BytesDown != 612 || row.FlowCount != 2 {
				t.Fatalf("unexpected collapsed dns totals: %+v", row)
			}
		}
	}
	if dnsRows != 1 {
		t.Fatalf("expected exactly one collapsed dns row, got %d rows=%+v", dnsRows, rows)
	}
}

func TestQueryUsageSupportsExeBasenameFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 16, 1, 8, 0, 0, time.UTC).Unix()
	err := store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "obfs-server",
			Exe:         "/usr/local/bin/obfs-server",
			LocalPort:   12345,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   100,
			BytesDown: 200,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	rows, _, _, err := store.QueryUsage(ctx, model.UsageQuery{
		Start: time.Unix(minute, 0).Add(-time.Minute),
		End:   time.Unix(minute, 0).Add(time.Minute),
		Exe:   "obfs-server",
		Limit: 10,
	}, DataSourceMinute)
	if err != nil {
		t.Fatalf("query usage with basename exe: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Exe == nil || *rows[0].Exe != "/usr/local/bin/obfs-server" {
		t.Fatalf("unexpected exe value: %+v", rows[0].Exe)
	}
}

func TestQueryTimeseriesSupportsExeBasenameFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 16, 2, 2, 0, 0, time.UTC)
	err := store.FlushMinute(ctx, minute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "obfs-server",
			Exe:         "/usr/local/bin/obfs-server",
			LocalPort:   12345,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    4,
			PktsDown:  6,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	points, err := store.QueryTimeseries(ctx, model.TimeseriesQuery{
		Start:  minute.Truncate(time.Hour),
		End:    minute.Truncate(time.Hour).Add(time.Hour),
		Bucket: time.Minute,
		Exe:    "obfs-server",
	}, DataSourceMinute)
	if err != nil {
		t.Fatalf("query timeseries with basename exe: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}
	if points[0].BytesUp != 1024 || points[0].BytesDown != 2048 {
		t.Fatalf("unexpected point values: %+v", points[0])
	}
}

func TestUsageChainsUpsertAggregateAndCleanup(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 1, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "159.226.171.34",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "www.google.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         3,
			EvidenceCount:     2,
			EvidenceSource:    "ss-log",
			Confidence:        "medium",
			SampleFingerprint: "chain-fp-1",
			SampleMessage:     "sample",
			SampleTime:        minute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert usage chains: %v", err)
	}

	rows, err := store.QueryUsageChainsForProcess(ctx, minute.Unix(), &pid, "ss-server", exe, DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query minute chains: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 minute chain row, got %+v", rows)
	}
	if rows[0].TargetHostNormalized != "www.google.com" || rows[0].ChainID == "" {
		t.Fatalf("unexpected minute chain row: %+v", rows[0])
	}

	if err := store.AggregateHour(ctx, minute); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}
	hourRows, err := store.QueryUsageChainsForProcess(ctx, minute.Truncate(time.Hour).Unix(), &pid, "ss-server", exe, DataSourceHourChain)
	if err != nil {
		t.Fatalf("query hour chains: %v", err)
	}
	if len(hourRows) != 1 {
		t.Fatalf("expected 1 hour chain row, got %+v", hourRows)
	}
	if hourRows[0].BytesTotal != 4096 || hourRows[0].FlowCount != 3 {
		t.Fatalf("unexpected hour chain aggregate: %+v", hourRows[0])
	}
	if !strings.HasPrefix(hourRows[0].ChainID, DataSourceHourChain+"|") {
		t.Fatalf("expected hourly chain id prefix, got %+v", hourRows[0])
	}

	store.now = func() time.Time { return time.Date(2026, 5, 1, 0, 5, 0, 0, time.UTC) }
	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	row := store.db.QueryRow(`SELECT COUNT(*) FROM usage_chain_1m`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count minute chains after cleanup: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected minute chains to be cleaned up, found %d", count)
	}

	var monthTS, chainCount int64
	if err := store.db.QueryRow(`SELECT month_ts, chain_count FROM usage_monthly`).Scan(&monthTS, &chainCount); err != nil {
		t.Fatalf("query monthly summary after cleanup: %v", err)
	}
	if monthTS != time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix() || chainCount != 1 {
		t.Fatalf("unexpected monthly chain summary: month=%d chain_count=%d", monthTS, chainCount)
	}
}

func TestAggregateHourKeepsUsageChainSampleFieldsFromLatestMinuteRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 1, 0, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        1024,
			FlowCount:         1,
			EvidenceCount:     1,
			EvidenceSource:    "older-log",
			Confidence:        "medium",
			SampleFingerprint: "zz-older",
			SampleMessage:     "older-message",
			SampleTime:        minute.Unix(),
		},
		{
			TimeBucket:        minute.Add(10 * time.Minute).Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        2048,
			FlowCount:         2,
			EvidenceCount:     1,
			EvidenceSource:    "newer-log",
			Confidence:        "high",
			SampleFingerprint: "aa-newer",
			SampleMessage:     "newer-message",
			SampleTime:        minute.Add(10 * time.Minute).Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert chain rows: %v", err)
	}

	if err := store.AggregateHour(ctx, minute); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	rows, err := store.QueryUsageChainsForProcess(ctx, minute.Truncate(time.Hour).Unix(), &pid, "ss-server", exe, DataSourceHourChain)
	if err != nil {
		t.Fatalf("query aggregated chains: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one aggregated chain row, got %+v", rows)
	}
	if rows[0].SampleTime != minute.Add(10*time.Minute).Unix() {
		t.Fatalf("expected latest sample time to win, got %+v", rows[0])
	}
	if rows[0].SampleFingerprint != "aa-newer" || rows[0].SampleMessage != "newer-message" || rows[0].EvidenceSource != "newer-log" {
		t.Fatalf("expected sample fields from latest minute row, got %+v", rows[0])
	}
}

func TestQueryUsageChainsSupportsExeBasenameFilter(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 1, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/local/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
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
			SampleTime:        minute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert usage chains: %v", err)
	}

	rows, err := store.QueryUsageChainsForProcess(ctx, minute.Unix(), &pid, "ss-server", "ss-server", DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query minute chains by basename exe: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected basename exe filter to match chain row, got %+v", rows)
	}
}

func TestQueryTopRemotesCanExcludeLoopback(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix()
	err := store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "127.0.0.1",
			RemotePort:  8388,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1000,
			BytesDown: 2000,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   12096,
			RemoteIP:    "203.0.113.24",
			RemotePort:  52144,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   500,
			BytesDown: 4000,
			PktsUp:    1,
			PktsDown:  5,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	rows, totalRows, err := store.QueryTopRemotes(
		ctx,
		time.Unix(minute, 0).Add(-time.Minute),
		time.Unix(minute, 0).Add(time.Minute),
		DataSourceMinute,
		"",
		false,
		"total",
		"desc",
		10,
		0,
		true,
	)
	if err != nil {
		t.Fatalf("query top remotes: %v", err)
	}
	if totalRows != 1 || len(rows) != 1 {
		t.Fatalf("unexpected remote rows: total=%d rows=%+v", totalRows, rows)
	}
	if rows[0].RemoteIP != "203.0.113.24" || rows[0].Direction != model.DirectionIn {
		t.Fatalf("unexpected remote entry: %+v", rows[0])
	}
}

func TestQueryTopProcessesSeparatesPidAndEmptyComm(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	minute := time.Date(2026, 4, 16, 2, 0, 0, 0, time.UTC).Unix()
	err := store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         1045,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   80,
			RemoteIP:    "198.51.100.17",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   122880,
			BytesDown: 896000,
			PktsUp:    250,
			PktsDown:  530,
			FlowCount: 5,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   12096,
			RemoteIP:    "203.0.113.77",
			RemotePort:  52144,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   400,
			BytesDown: 3200,
			PktsUp:    2,
			PktsDown:  9,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	rows, totalRows, err := store.QueryTopProcesses(
		ctx,
		time.Unix(minute, 0).Add(-time.Minute),
		time.Unix(minute, 0).Add(time.Minute),
		DataSourceMinute,
		"pid",
		"total",
		"desc",
		10,
		0,
		true,
	)
	if err != nil {
		t.Fatalf("query top processes: %v", err)
	}
	if totalRows != 2 || len(rows) != 2 {
		t.Fatalf("unexpected process rows: total=%d rows=%+v", totalRows, rows)
	}
	if rows[0].Comm != "nginx" || rows[0].PID == nil || *rows[0].PID != 1045 {
		t.Fatalf("unexpected first process entry: %+v", rows[0])
	}
	if rows[1].PID == nil || *rows[1].PID != 0 || rows[1].Comm != "" {
		t.Fatalf("unexpected second process entry: %+v", rows[1])
	}
}

func TestQueryOverviewRespectsRequestedWindow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	olderMinute := time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC).Unix()
	recentMinute := time.Date(2026, 4, 15, 12, 30, 0, 0, time.UTC).Unix()

	err := store.FlushMinute(ctx, recentMinute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    recentMinute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50500,
			RemoteIP:    "1.1.1.1",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 200, PktsUp: 1, PktsDown: 2, FlowCount: 1},
		{
			MinuteTS:    olderMinute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   50501,
			RemoteIP:    "8.8.8.8",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 300, BytesDown: 400, PktsUp: 3, PktsDown: 4, FlowCount: 1},
	}, nil)
	if err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	recentStats, err := store.QueryOverview(
		ctx,
		time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 15, 13, 0, 0, 0, time.UTC),
		DataSourceMinute,
	)
	if err != nil {
		t.Fatalf("query recent overview: %v", err)
	}
	if recentStats.BytesUp != 100 || recentStats.BytesDown != 200 {
		t.Fatalf("unexpected recent stats: %+v", recentStats)
	}

	fullStats, err := store.QueryOverview(
		ctx,
		time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 15, 13, 0, 0, 0, time.UTC),
		DataSourceMinute,
	)
	if err != nil {
		t.Fatalf("query full overview: %v", err)
	}
	if fullStats.BytesUp != 400 || fullStats.BytesDown != 600 {
		t.Fatalf("unexpected full stats: %+v", fullStats)
	}
}

func TestUsageSummaryQueriesPreserveMinuteEdgeTotals(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 8, 12, 35, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	start := time.Date(2026, 5, 8, 9, 15, 0, 0, time.UTC)
	end := now

	usage := map[model.UsageKey]model.UsageDelta{}
	addRow := func(ts time.Time, localPort int, remoteIP string, up int64, down int64) {
		usage[model.UsageKey{
			MinuteTS:    ts.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         42,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   localPort,
			RemoteIP:    remoteIP,
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}] = model.UsageDelta{BytesUp: up, BytesDown: down, PktsUp: 1, PktsDown: 1, FlowCount: 1}
	}
	addRow(time.Date(2026, 5, 8, 9, 20, 0, 0, time.UTC), 50001, "198.51.100.10", 100, 10)
	addRow(time.Date(2026, 5, 8, 10, 20, 0, 0, time.UTC), 50002, "198.51.100.10", 200, 20)
	addRow(time.Date(2026, 5, 8, 11, 20, 0, 0, time.UTC), 50003, "203.0.113.20", 300, 30)
	addRow(time.Date(2026, 5, 8, 12, 10, 0, 0, time.UTC), 50004, "203.0.113.20", 400, 40)
	if err := store.FlushMinute(ctx, start.Unix(), usage, nil); err != nil {
		t.Fatalf("flush minute usage: %v", err)
	}
	for _, hour := range []time.Time{
		time.Date(2026, 5, 8, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 8, 11, 0, 0, 0, time.UTC),
	} {
		if err := store.AggregateHour(ctx, hour); err != nil {
			t.Fatalf("aggregate hour %s: %v", hour, err)
		}
	}

	exact, err := store.QueryOverview(ctx, start, end, DataSourceMinute)
	if err != nil {
		t.Fatalf("query exact overview: %v", err)
	}
	summary, err := store.QueryOverviewSummary(ctx, start, end)
	if err != nil {
		t.Fatalf("query summary overview: %v", err)
	}
	if summary.BytesUp != exact.BytesUp || summary.BytesDown != exact.BytesDown || summary.FlowCount != exact.FlowCount {
		t.Fatalf("summary overview changed totals: exact=%+v summary=%+v", exact, summary)
	}

	remotes, _, remoteSource, err := store.QueryTopRemotesSummary(ctx, start, end, model.DirectionOut, true, "bytes_total", "desc", 10, 0, true)
	if err != nil {
		t.Fatalf("query summary remotes: %v", err)
	}
	if remoteSource != DataSourceHour {
		t.Fatalf("expected mixed summary to report hourly data source, got %s", remoteSource)
	}
	if len(remotes) != 2 {
		t.Fatalf("expected two remote groups, got %+v", remotes)
	}
	var remoteTotal int64
	for _, row := range remotes {
		remoteTotal += row.BytesUp + row.BytesDown
	}
	if remoteTotal != exact.BytesUp+exact.BytesDown {
		t.Fatalf("summary remote totals changed: exact=%d remoteTotal=%d rows=%+v", exact.BytesUp+exact.BytesDown, remoteTotal, remotes)
	}

	processes, _, processSource, err := store.QueryTopProcessesSummary(ctx, start, end, "pid", "bytes_total", "desc", 10, 0, true)
	if err != nil {
		t.Fatalf("query summary processes: %v", err)
	}
	if processSource != DataSourceHour {
		t.Fatalf("expected process summary to report hourly data source, got %s", processSource)
	}
	if len(processes) != 1 || processes[0].PID != nil || processes[0].Comm != "curl" {
		t.Fatalf("expected aggregated process summary to fall back to comm, got %+v", processes)
	}
	if processes[0].BytesUp != exact.BytesUp || processes[0].BytesDown != exact.BytesDown || processes[0].FlowCount != exact.FlowCount {
		t.Fatalf("summary process totals changed: exact=%+v processes=%+v", exact, processes)
	}
}

func TestUpsertAndQueryLogEvidence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	status := 200
	rows := []model.LogEvidence{
		{
			Source:      "nginx",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "74.7.227.153",
			TargetIP:    "",
			Host:        "paris.escape.ac.cn",
			Path:        "/cloud/",
			Method:      "GET",
			Status:      &status,
			Message:     "sample",
			Fingerprint: "fp-1",
		},
	}

	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert log evidence: %v", err)
	}

	updatedStatus := 401
	rows[0].Status = &updatedStatus
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert log evidence update: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:   "nginx",
		StartTS:  time.Date(2026, 4, 16, 0, 50, 0, 0, time.UTC).Unix(),
		EndTS:    time.Date(2026, 4, 16, 1, 10, 0, 0, time.UTC).Unix(),
		ClientIP: "74.7.227.153",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("query log evidence: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("unexpected evidence count: %d", len(fetched))
	}
	if fetched[0].Status == nil || *fetched[0].Status != 401 {
		t.Fatalf("unexpected evidence status: %+v", fetched[0])
	}
}

func TestQueryLogEvidenceSupportsAnyIP(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	status := 200
	rows := []model.LogEvidence{
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "203.0.113.10",
			TargetIP:    "142.250.72.14",
			Host:        "chatgpt.com",
			Path:        "443",
			Method:      "connect",
			Status:      &status,
			Message:     "sample-1",
			Fingerprint: "fp-any-1",
		},
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 30, 0, time.UTC).Unix(),
			ClientIP:    "198.51.100.22",
			TargetIP:    "1.1.1.1",
			Host:        "one.one.one.one",
			Path:        "443",
			Method:      "connect",
			Status:      &status,
			Message:     "sample-2",
			Fingerprint: "fp-any-2",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:  "ss",
		StartTS: time.Date(2026, 4, 16, 0, 59, 0, 0, time.UTC).Unix(),
		EndTS:   time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix(),
		AnyIP:   "142.250.72.14",
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("query any ip evidence: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one evidence row, got %d", len(fetched))
	}
	if fetched[0].TargetIP != "142.250.72.14" {
		t.Fatalf("unexpected evidence row: %+v", fetched[0])
	}
}

func TestQueryLogEvidenceAnyIPDoesNotDuplicateRows(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rows := []model.LogEvidence{
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "203.0.113.10",
			TargetIP:    "203.0.113.10",
			Host:        "example.com",
			Path:        "443",
			Method:      "connect",
			Message:     "self ip",
			Fingerprint: "fp-any-dup-1",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:  "ss",
		StartTS: time.Date(2026, 4, 16, 0, 59, 0, 0, time.UTC).Unix(),
		EndTS:   time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix(),
		AnyIP:   "203.0.113.10",
		Limit:   50,
	})
	if err != nil {
		t.Fatalf("query any ip evidence: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one deduped evidence row, got %d", len(fetched))
	}
}

func TestQueryLogEvidenceAnyIPRespectsHostPortFilters(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rows := []model.LogEvidence{
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "203.0.113.10",
			TargetIP:    "142.250.72.14",
			Host:        "chatgpt.com",
			Path:        "443",
			Method:      "connect",
			Message:     "match",
			Fingerprint: "fp-any-host-port-match",
		},
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 30, 0, time.UTC).Unix(),
			ClientIP:    "203.0.113.10",
			TargetIP:    "1.1.1.1",
			Host:        "one.one.one.one",
			Path:        "443",
			Method:      "connect",
			Message:     "mismatch host",
			Fingerprint: "fp-any-host-port-mismatch",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:         "ss",
		StartTS:        time.Date(2026, 4, 16, 0, 59, 0, 0, time.UTC).Unix(),
		EndTS:          time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix(),
		AnyIP:          "203.0.113.10",
		HostNormalized: "chatgpt.com",
		TargetPort:     443,
		Limit:          50,
	})
	if err != nil {
		t.Fatalf("query any ip with host/port: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one filtered evidence row, got %d", len(fetched))
	}
	if fetched[0].HostNormalized != "chatgpt.com" || fetched[0].TargetPort != 443 {
		t.Fatalf("unexpected filtered row: %+v", fetched[0])
	}
}

func TestQueryLogEvidenceRespectsEntryPort(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rows := []model.LogEvidence{
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "203.0.113.24",
			EntryPort:   12096,
			Method:      "accept",
			Message:     "[12096] accepted connection from 203.0.113.24",
			Fingerprint: "fp-entry-12096",
		},
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 30, 0, time.UTC).Unix(),
			ClientIP:    "198.51.100.77",
			EntryPort:   12098,
			Method:      "accept",
			Message:     "[12098] accepted connection from 198.51.100.77",
			Fingerprint: "fp-entry-12098",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:    "ss",
		StartTS:   time.Date(2026, 4, 16, 0, 59, 0, 0, time.UTC).Unix(),
		EndTS:     time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix(),
		EntryPort: 12096,
		Limit:     20,
	})
	if err != nil {
		t.Fatalf("query entry-port evidence: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one entry-port evidence row, got %d", len(fetched))
	}
	if fetched[0].EntryPort != 12096 || fetched[0].ClientIP != "203.0.113.24" {
		t.Fatalf("unexpected entry-port evidence row: %+v", fetched[0])
	}
}

func TestCleanupRemovesExpiredLogEvidence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	status := 200
	rows := []model.LogEvidence{
		{
			Source:      "nginx",
			EventTS:     time.Date(2026, 3, 31, 23, 59, 0, 0, time.UTC).Unix(),
			ClientIP:    "74.7.227.153",
			TargetIP:    "",
			Host:        "paris.escape.ac.cn",
			Path:        "/old",
			Method:      "GET",
			Status:      &status,
			Message:     "old",
			Fingerprint: "fp-clean-old",
		},
		{
			Source:      "nginx",
			EventTS:     time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC).Unix(),
			ClientIP:    "74.7.227.154",
			TargetIP:    "",
			Host:        "paris.escape.ac.cn",
			Path:        "/new",
			Method:      "GET",
			Status:      &status,
			Message:     "new",
			Fingerprint: "fp-clean-new",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert evidence: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE log_evidence SET created_at = ? WHERE fingerprint = ?`, now.Add(-48*time.Hour).Unix(), "fp-clean-old"); err != nil {
		t.Fatalf("set created_at for old row: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE log_evidence SET created_at = ? WHERE fingerprint = ?`, now.Add(-2*time.Hour).Unix(), "fp-clean-new"); err != nil {
		t.Fatalf("set created_at for new row: %v", err)
	}

	if err := store.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:  "nginx",
		StartTS: now.Add(-72 * time.Hour).Unix(),
		EndTS:   now.Unix(),
		Limit:   20,
	})
	if err != nil {
		t.Fatalf("query evidence after cleanup: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one recent evidence row after cleanup, got %d", len(fetched))
	}
	if fetched[0].Fingerprint != "fp-clean-new" {
		t.Fatalf("unexpected remaining evidence row: %+v", fetched[0])
	}
}

func TestQueryLogEvidenceMatchesHostOnlyRowsByTargetPort(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	rows := []model.LogEvidence{
		{
			Source:      "ss",
			EventTS:     time.Date(2026, 4, 16, 1, 0, 20, 0, time.UTC).Unix(),
			ClientIP:    "",
			TargetIP:    "",
			Host:        "chatgpt.com",
			Path:        "443",
			Method:      "connect",
			Message:     "connect to chatgpt.com:443",
			Fingerprint: "fp-host-only-443",
		},
	}
	if err := store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		t.Fatalf("upsert host-only evidence: %v", err)
	}

	fetched, err := store.QueryLogEvidence(ctx, LogEvidenceQuery{
		Source:         "ss",
		StartTS:        time.Date(2026, 4, 16, 0, 59, 0, 0, time.UTC).Unix(),
		EndTS:          time.Date(2026, 4, 16, 1, 2, 0, 0, time.UTC).Unix(),
		HostNormalized: "chatgpt.com",
		TargetPort:     443,
		Limit:          20,
	})
	if err != nil {
		t.Fatalf("query host-only evidence: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected one host-only evidence row, got %d", len(fetched))
	}
	if fetched[0].TargetPort != 443 || fetched[0].HostNormalized != "chatgpt.com" {
		t.Fatalf("unexpected normalized evidence row: %+v", fetched[0])
	}
}

func TestQueryUsageCountMatchesPagedTotal(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 5, 8, 9, 0, 0, 0, time.UTC)
	usage := map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1001,
			Comm:        "alpha",
			Exe:         "/usr/bin/alpha",
			LocalPort:   40001,
			RemoteIP:    "198.51.100.10",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 20, FlowCount: 1},
		{
			MinuteTS:    minute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1002,
			Comm:        "beta",
			Exe:         "/usr/bin/beta",
			LocalPort:   40002,
			RemoteIP:    "198.51.100.11",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 200, BytesDown: 40, FlowCount: 1},
	}
	if err := store.FlushMinute(ctx, minute.Unix(), usage, nil); err != nil {
		t.Fatalf("flush minute: %v", err)
	}
	q := model.UsageQuery{
		Start:        minute.Add(-time.Minute),
		End:          minute.Add(time.Minute),
		Page:         1,
		PageSize:     1,
		UsePage:      true,
		IncludeTotal: true,
	}
	_, _, fullTotal, err := store.QueryUsage(ctx, q, DataSourceMinute)
	if err != nil {
		t.Fatalf("paged query: %v", err)
	}
	got, err := store.QueryUsageCount(ctx, q, DataSourceMinute)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if got != fullTotal {
		t.Fatalf("QueryUsageCount=%d, paged total=%d", got, fullTotal)
	}
}

func TestQueryForwardUsageCountMatchesPagedTotal(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 5, 8, 9, 0, 0, 0, time.UTC)
	forward := map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  minute.Unix(),
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "203.0.113.10",
			OrigSPort: 52000,
			OrigDPort: 443,
		}: {BytesUp: 100, BytesDown: 20, FlowCount: 1},
		{
			MinuteTS:  minute.Unix(),
			Proto:     "udp",
			OrigSrcIP: "10.0.0.3",
			OrigDstIP: "203.0.113.11",
			OrigSPort: 53000,
			OrigDPort: 53,
		}: {BytesUp: 200, BytesDown: 40, FlowCount: 1},
	}
	if err := store.FlushMinute(ctx, minute.Unix(), nil, forward); err != nil {
		t.Fatalf("flush minute: %v", err)
	}
	q := model.ForwardQuery{
		Start:        minute.Add(-time.Minute),
		End:          minute.Add(time.Minute),
		Page:         1,
		PageSize:     1,
		UsePage:      true,
		IncludeTotal: true,
	}
	_, _, fullTotal, err := store.QueryForwardUsage(ctx, q, DataSourceMinute)
	if err != nil {
		t.Fatalf("paged query: %v", err)
	}
	got, err := store.QueryForwardUsageCount(ctx, q, DataSourceMinute)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if got != fullTotal {
		t.Fatalf("QueryForwardUsageCount=%d, paged total=%d", got, fullTotal)
	}
}

func TestQueryTopProcessesAllReturnsAllGroups(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	usage := map[model.UsageKey]model.UsageDelta{}
	for i := 0; i < 5; i++ {
		usage[model.UsageKey{
			MinuteTS:    minute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         6000 + i,
			Comm:        "worker",
			Exe:         "/usr/bin/worker",
			LocalPort:   43000 + i,
			RemoteIP:    "198.51.100.50",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}] = model.UsageDelta{BytesUp: int64(10 + i), FlowCount: 1}
	}
	if err := store.FlushMinute(ctx, minute.Unix(), usage, nil); err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	all, err := store.QueryTopProcessesAllLimited(ctx, minute.Add(-time.Minute), minute.Add(time.Minute), DataSourceMinute, "pid", 0)
	if err != nil {
		t.Fatalf("query all top processes: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("expected all 5 pid groups, got %d: %+v", len(all), all)
	}

	byComm, err := store.QueryTopProcessesAllLimited(ctx, minute.Add(-time.Minute), minute.Add(time.Minute), DataSourceMinute, "comm", 0)
	if err != nil {
		t.Fatalf("query all top processes by comm: %v", err)
	}
	if len(byComm) != 1 || byComm[0].BytesUp != 60 {
		t.Fatalf("expected one comm group with 60 bytes_up, got %+v", byComm)
	}
}

func TestQueryTopRemotesAllReturnsAllGroups(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	minute := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	usage := map[model.UsageKey]model.UsageDelta{}
	for i := 0; i < 4; i++ {
		usage[model.UsageKey{
			MinuteTS:    minute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         7000 + i,
			Comm:        "remote-worker",
			Exe:         "/usr/bin/remote-worker",
			LocalPort:   44000 + i,
			RemoteIP:    fmt.Sprintf("203.0.113.%d", 50+i),
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}] = model.UsageDelta{BytesUp: int64(10 + i), FlowCount: 1}
	}
	if err := store.FlushMinute(ctx, minute.Unix(), usage, nil); err != nil {
		t.Fatalf("flush minute: %v", err)
	}

	all, err := store.QueryTopRemotesAllLimited(ctx, minute.Add(-time.Minute), minute.Add(time.Minute), DataSourceMinute, model.DirectionOut, true, 0)
	if err != nil {
		t.Fatalf("query all top remotes: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("expected all 4 remote groups, got %d: %+v", len(all), all)
	}

	inbound, err := store.QueryTopRemotesAllLimited(ctx, minute.Add(-time.Minute), minute.Add(time.Minute), DataSourceMinute, model.DirectionIn, true, 0)
	if err != nil {
		t.Fatalf("query inbound top remotes: %v", err)
	}
	if len(inbound) != 0 {
		t.Fatalf("expected no inbound groups, got %+v", inbound)
	}
}
