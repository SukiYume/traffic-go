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
