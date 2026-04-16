package store

import (
	"context"
	"database/sql"
	"path/filepath"
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
	top, totalRows, err := store.QueryTopProcesses(ctx, hourStart, hourStart.Add(time.Hour), DataSourceHour, "total", "desc", 10, 0)
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

func TestResolveUsageSourceUsesConfiguredRetention(t *testing.T) {
	cfg := config.Default()
	cfg.Retention.MinuteDays = 60
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	start := time.Unix(0, 0)
	end := start.Add(45 * 24 * time.Hour)

	source, err := store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		t.Fatalf("resolve source: %v", err)
	}
	if source != DataSourceMinute {
		t.Fatalf("expected minute source, got %s", source)
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

func TestCleanupRemovesExpiredLogEvidence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	status := 200
	rows := []model.LogEvidence{
		{
			Source:      "nginx",
			EventTS:     now.Add(-48 * time.Hour).Unix(),
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
			EventTS:     now.Add(-2 * time.Hour).Unix(),
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

	store.retention.MinuteDays = 1
	store.retention.HourlyDays = 7
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
