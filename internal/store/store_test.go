package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
)

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

func TestResolveUsageSource(t *testing.T) {
	store := newTestStore(t)
	start := time.Unix(0, 0)
	end := start.Add(31 * 24 * time.Hour)

	source, err := store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceHour {
		t.Fatalf("unexpected source: %s", source)
	}

	if _, err := store.ResolveUsageSource(start, end, true, false); err != ErrDimensionUnavailable {
		t.Fatalf("expected dimension unavailable, got %v", err)
	}
}

func TestOpenUsesSingleSQLiteConnection(t *testing.T) {
	store := newTestStore(t)
	if store.db.Stats().MaxOpenConnections != 1 {
		t.Fatalf("expected sqlite store to use one open connection, got %+v", store.db.Stats())
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
	top, totalRows, err := store.QueryTopProcesses(ctx, hourStart, hourStart.Add(time.Hour), DataSourceHour, "comm", "total", "desc", 10, 0)
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
	store.retention.Months = 3

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
	if remainingJanuary != 0 || remainingFebruary != 1 {
		t.Fatalf("unexpected remaining detail rows: january=%d february=%d", remainingJanuary, remainingFebruary)
	}
}

func TestCleanupSummarizesExpiredHourOnlyMonth(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	store.now = func() time.Time { return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC) }
	store.retention.Months = 3

	decemberHour := time.Date(2025, 12, 15, 3, 0, 0, 0, time.UTC).Unix()
	if _, err := store.db.ExecContext(ctx, `
INSERT INTO usage_1h (
	hour_ts, proto, direction, comm, local_port, remote_ip,
	bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 50500, '1.1.1.1', 700, 800, 7, 8, 9);
INSERT INTO usage_1h_forward (
	hour_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
	bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
) VALUES (?, 'tcp', '10.0.0.2', '1.1.1.1', 50000, 443, 300, 400, 3, 4, 5);
`, decemberHour, decemberHour); err != nil {
		t.Fatalf("seed hour-only month: %v", err)
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

func TestResolveUsageSourceUsesConfiguredMonthRetention(t *testing.T) {
	cfg := config.Default()
	cfg.Retention.Months = 3
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

	source, err := store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceMinute {
		t.Fatalf("expected minute source, got %s", source)
	}
}

func TestResolveUsageSourceFallsBackForOldAbsoluteWindow(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	start := time.Date(2026, 1, 31, 23, 30, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)

	source, err := store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceHour {
		t.Fatalf("expected old window to use hour source, got %s", source)
	}

	if _, err := store.ResolveUsageSource(start, end, true, false); err != ErrDimensionUnavailable {
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

func TestMigrateDropsLegacyFlowSnapshotTable(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE flows_snapshot (
    ct_id INTEGER PRIMARY KEY,
    proto TEXT NOT NULL
);
`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	_ = db.Close()

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	row := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'flows_snapshot'`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected legacy flows_snapshot table to be dropped, found %d", count)
	}
}

func TestMigrateLegacyLogEvidenceAddsHostPortColumnsAndIndex(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	_, err = db.Exec(`
CREATE TABLE log_evidence (
    source TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    client_ip TEXT NOT NULL,
    target_ip TEXT NOT NULL,
    host TEXT NOT NULL,
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    status INTEGER,
    message TEXT NOT NULL,
    fingerprint TEXT NOT NULL PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_log_evidence_lookup ON log_evidence (source, event_ts, client_ip, target_ip);
CREATE INDEX IF NOT EXISTS idx_log_evidence_created_at ON log_evidence (created_at);
`)
	if err != nil {
		t.Fatalf("create legacy log_evidence table: %v", err)
	}
	_ = db.Close()

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	hasHostNormalized, err := store.tableHasColumn(context.Background(), "log_evidence", "host_normalized")
	if err != nil {
		t.Fatalf("check host_normalized column: %v", err)
	}
	if !hasHostNormalized {
		t.Fatalf("expected host_normalized column to be added")
	}

	hasEntryPort, err := store.tableHasColumn(context.Background(), "log_evidence", "entry_port")
	if err != nil {
		t.Fatalf("check entry_port column: %v", err)
	}
	if !hasEntryPort {
		t.Fatalf("expected entry_port column to be added")
	}

	hasTargetPort, err := store.tableHasColumn(context.Background(), "log_evidence", "target_port")
	if err != nil {
		t.Fatalf("check target_port column: %v", err)
	}
	if !hasTargetPort {
		t.Fatalf("expected target_port column to be added")
	}

	row := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_log_evidence_host_port'`)
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count host/port index: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected idx_log_evidence_host_port to exist, found %d", count)
	}
}

func TestMigratePrunesHistoricalZeroUsageRows(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`
INSERT INTO usage_1m (
	minute_ts, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port, attribution,
	bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES
	(1710000000, 'tcp', 'out', 0, '', '', 0, '203.0.113.8', 443, 'unknown', 0, 0, 0, 0, 0),
	(1710000060, 'tcp', 'out', 1088, 'ss-server', '/usr/bin/ss-server', 8388, '203.0.113.9', 443, 'exact', 10, 20, 1, 2, 1);
INSERT INTO usage_1m_forward (
	minute_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
	bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
) VALUES
	(1710000000, 'tcp', '10.0.0.2', '203.0.113.8', 51000, 443, 0, 0, 0, 0, 0),
	(1710000060, 'tcp', '10.0.0.3', '203.0.113.9', 51001, 443, 10, 20, 1, 2, 1);
`); err != nil {
		t.Fatalf("seed zero usage rows: %v", err)
	}
	_ = db.Close()

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	var usageRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m`).Scan(&usageRows); err != nil {
		t.Fatalf("count usage rows after migrate: %v", err)
	}
	if usageRows != 1 {
		t.Fatalf("expected zero-valued usage rows to be pruned, found %d rows", usageRows)
	}

	var forwardRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM usage_1m_forward`).Scan(&forwardRows); err != nil {
		t.Fatalf("count forward rows after migrate: %v", err)
	}
	if forwardRows != 1 {
		t.Fatalf("expected zero-valued forward rows to be pruned, found %d rows", forwardRows)
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

	store.retention.Months = 1
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

func TestQueryTopRemotesExcludesLoopbackByDefault(t *testing.T) {
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

	store.retention.Months = 1
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
