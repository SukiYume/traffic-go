package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/model"
)

func TestAppendSummaryPagination(t *testing.T) {
	tests := []struct {
		name         string
		usePage      bool
		includeTotal bool
		page         int
		pageSize     int
		cursorTS     int64
		cursorRowID  int64
		limit        int
		wantSQL      string
		wantArgs     []any
	}{
		{
			name:         "page with exact total",
			usePage:      true,
			includeTotal: true,
			page:         2,
			pageSize:     5,
			wantSQL:      " ORDER BY bytes_total DESC LIMIT ? OFFSET ?",
			wantArgs:     []any{5, 5},
		},
		{
			name:     "page with lookahead",
			usePage:  true,
			page:     1,
			pageSize: 5,
			wantSQL:  " ORDER BY bytes_total DESC LIMIT ? OFFSET ?",
			wantArgs: []any{6, 0},
		},
		{
			name:        "cursor",
			cursorTS:    120,
			cursorRowID: 8,
			limit:       10,
			wantSQL:     " WHERE (time_bucket < ? OR (time_bucket = ? AND row_id < ?)) ORDER BY time_bucket DESC, row_id DESC LIMIT ?",
			wantArgs:    []any{int64(120), int64(120), int64(8), 11},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var builder strings.Builder
			var args []any
			appendSummaryPagination(&builder, &args, " ORDER BY bytes_total DESC", test.usePage, test.includeTotal, test.page, test.pageSize, test.cursorTS, test.cursorRowID, test.limit)
			if got := builder.String(); got != test.wantSQL {
				t.Fatalf("unexpected SQL: got %q want %q", got, test.wantSQL)
			}
			if !reflect.DeepEqual(args, test.wantArgs) {
				t.Fatalf("unexpected args: got %#v want %#v", args, test.wantArgs)
			}
		})
	}
}

func TestDetailAndTimeseriesSummariesIncludeCurrentMinuteTail(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 5, 8, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }
	start := time.Date(2026, 5, 8, 9, 15, 0, 0, time.UTC)

	usage := make(map[model.UsageKey]model.UsageDelta)
	forward := make(map[model.ForwardUsageKey]model.UsageDelta)
	for index, at := range []time.Time{
		time.Date(2026, 5, 8, 9, 20, 0, 0, time.UTC),
		time.Date(2026, 5, 8, 10, 20, 0, 0, time.UTC),
		time.Date(2026, 5, 8, 11, 20, 0, 0, time.UTC),
		time.Date(2026, 5, 8, 12, 10, 0, 0, time.UTC),
	} {
		value := int64((index + 1) * 100)
		usage[model.UsageKey{
			MinuteTS: at.Unix(), Proto: "tcp", Direction: model.DirectionOut,
			PID: 42, Comm: "curl", Exe: "/usr/bin/curl", LocalPort: 50000 + index,
			RemoteIP: "198.51.100.10", RemotePort: 443, Attribution: model.AttributionExact,
		}] = model.UsageDelta{BytesUp: value, BytesDown: value / 10, PktsUp: 1, PktsDown: 1, FlowCount: 1}
		forward[model.ForwardUsageKey{
			MinuteTS: at.Unix(), Proto: "tcp", OrigSrcIP: "10.0.0.2",
			OrigDstIP: "198.51.100.10", OrigSPort: 51000 + index, OrigDPort: 443,
		}] = model.UsageDelta{BytesUp: value, BytesDown: value / 10, PktsUp: 1, PktsDown: 1, FlowCount: 1}
	}
	if err := trafficStore.FlushMinute(ctx, start.Unix(), usage, forward); err != nil {
		t.Fatalf("flush minute: %v", err)
	}
	for hour := start.Truncate(time.Hour); hour.Before(now.Truncate(time.Hour)); hour = hour.Add(time.Hour) {
		if err := trafficStore.AggregateHour(ctx, hour); err != nil {
			t.Fatalf("aggregate hour %s: %v", hour, err)
		}
	}

	usageQuery := model.UsageQuery{Start: start, End: now, Page: 1, PageSize: 20, UsePage: true, IncludeTotal: true}
	rows, _, total, source, err := trafficStore.QueryUsageSummary(ctx, usageQuery)
	if err != nil {
		t.Fatalf("query usage summary: %v", err)
	}
	if total != 4 || len(rows) != 4 || source != DataSourceHour {
		t.Fatalf("unexpected usage summary: total=%d source=%s rows=%+v", total, source, rows)
	}
	var usageBytes int64
	var sawMinuteTail bool
	for _, row := range rows {
		usageBytes += row.BytesUp
		if row.TimeBucket == time.Date(2026, 5, 8, 12, 10, 0, 0, time.UTC).Unix() && row.DataSource == DataSourceMinute {
			sawMinuteTail = true
		}
	}
	if usageBytes != 1000 || !sawMinuteTail {
		t.Fatalf("usage summary lost current-hour tail: bytes=%d tail=%v rows=%+v", usageBytes, sawMinuteTail, rows)
	}

	forwardQuery := model.ForwardQuery{Start: start, End: now, Page: 1, PageSize: 20, UsePage: true, IncludeTotal: true}
	forwardRows, _, forwardTotal, forwardSource, err := trafficStore.QueryForwardUsageSummary(ctx, forwardQuery)
	if err != nil {
		t.Fatalf("query forward summary: %v", err)
	}
	if forwardTotal != 4 || len(forwardRows) != 4 || forwardSource != DataSourceHourForward {
		t.Fatalf("unexpected forward summary: total=%d source=%s rows=%+v", forwardTotal, forwardSource, forwardRows)
	}
	var forwardBytes int64
	for _, row := range forwardRows {
		forwardBytes += row.BytesOrig
	}
	if forwardBytes != 1000 || forwardRows[0].DataSource != DataSourceMinuteForward {
		t.Fatalf("forward summary lost current-hour tail: bytes=%d rows=%+v", forwardBytes, forwardRows)
	}

	points, pointSource, err := trafficStore.QueryTimeseriesSummary(ctx, model.TimeseriesQuery{
		Start: start, End: now, Bucket: time.Hour, GroupBy: "direction",
	})
	if err != nil {
		t.Fatalf("query timeseries summary: %v", err)
	}
	if pointSource != DataSourceHour || len(points) != 4 {
		t.Fatalf("unexpected timeseries summary: source=%s points=%+v", pointSource, points)
	}
	var pointBytes int64
	for _, point := range points {
		pointBytes += point.BytesUp
	}
	if pointBytes != 1000 {
		t.Fatalf("timeseries summary lost bytes: %d", pointBytes)
	}
}

func TestSummaryCoverageRoundsOldPartialHoursOutward(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }

	start := time.Date(2026, 6, 10, 10, 20, 0, 0, time.UTC)
	end := time.Date(2026, 6, 10, 11, 20, 0, 0, time.UTC)
	effectiveStart := start.Truncate(time.Hour)
	effectiveEnd := end.Truncate(time.Hour).Add(time.Hour)

	for index, hour := range []time.Time{effectiveStart, effectiveStart.Add(time.Hour)} {
		if _, err := trafficStore.db.ExecContext(ctx, `
INSERT INTO usage_1h (
    hour_ts, proto, direction, comm, local_port, remote_ip,
    bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 443, '198.51.100.10', ?, 0, 1, 0, 1)
`, hour.Unix(), int64((index+1)*100)); err != nil {
			t.Fatalf("insert hourly aggregate: %v", err)
		}
	}

	coverage := trafficStore.SummaryCoverage(start, end)
	if !coverage.Approximate || coverage.EffectiveStart != effectiveStart.Unix() || coverage.EffectiveEnd != effectiveEnd.Unix() {
		t.Fatalf("unexpected partial-hour coverage: %+v", coverage)
	}
	parts := trafficStore.usageSummaryParts(start, end)
	if len(parts) != 1 || parts[0].info.DataLabel != DataSourceHour || parts[0].start != effectiveStart.Unix() || parts[0].end != effectiveEnd.Unix() {
		t.Fatalf("unexpected partial-hour plan: %+v", parts)
	}

	stats, err := trafficStore.QueryOverviewSummary(ctx, start, end)
	if err != nil {
		t.Fatalf("query partial-hour overview: %v", err)
	}
	if stats.BytesUp != 300 || stats.FlowCount != 2 || stats.DataSource != DataSourceHour {
		t.Fatalf("partial-hour summary omitted a boundary bucket: %+v", stats)
	}
}

func TestSummaryCoverageIncludesSingleOldPartialHour(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }

	start := time.Date(2026, 6, 11, 10, 20, 0, 0, time.UTC)
	end := time.Date(2026, 6, 11, 10, 50, 0, 0, time.UTC)
	effectiveStart := start.Truncate(time.Hour)
	effectiveEnd := effectiveStart.Add(time.Hour)
	if _, err := trafficStore.db.ExecContext(ctx, `
INSERT INTO usage_1h (
    hour_ts, proto, direction, comm, local_port, remote_ip,
    bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 443, '198.51.100.11', 700, 0, 1, 0, 1)
`, effectiveStart.Unix()); err != nil {
		t.Fatalf("insert single hourly aggregate: %v", err)
	}

	coverage := trafficStore.SummaryCoverage(start, end)
	if !coverage.Approximate || coverage.EffectiveStart != effectiveStart.Unix() || coverage.EffectiveEnd != effectiveEnd.Unix() {
		t.Fatalf("unexpected single-hour coverage: %+v", coverage)
	}
	stats, err := trafficStore.QueryOverviewSummary(ctx, start, end)
	if err != nil {
		t.Fatalf("query single partial hour: %v", err)
	}
	if stats.BytesUp != 700 || stats.FlowCount != 1 || stats.DataSource != DataSourceHour {
		t.Fatalf("single partial hour was silently omitted: %+v", stats)
	}
}

func TestSummaryCoverageRoundsOldPartialDaysOutward(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }

	start := time.Date(2026, 4, 10, 10, 20, 0, 0, time.UTC)
	end := time.Date(2026, 4, 11, 5, 30, 0, 0, time.UTC)
	effectiveStart := time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC)
	effectiveEnd := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)

	for index, day := range []time.Time{effectiveStart, effectiveStart.Add(24 * time.Hour)} {
		if _, err := trafficStore.db.ExecContext(ctx, `
INSERT INTO usage_1d (
    day_ts, proto, direction, comm, local_port, remote_ip,
    bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, 'tcp', 'out', 'curl', 443, '198.51.100.12', ?, 0, 1, 0, 1)
`, day.Unix(), int64((index+1)*1000)); err != nil {
			t.Fatalf("insert daily aggregate: %v", err)
		}
	}

	coverage := trafficStore.SummaryCoverage(start, end)
	if !coverage.Approximate || coverage.EffectiveStart != effectiveStart.Unix() || coverage.EffectiveEnd != effectiveEnd.Unix() {
		t.Fatalf("unexpected partial-day coverage: %+v", coverage)
	}
	parts := trafficStore.usageSummaryParts(start, end)
	if len(parts) != 1 || parts[0].info.DataLabel != DataSourceDay || parts[0].start != effectiveStart.Unix() || parts[0].end != effectiveEnd.Unix() {
		t.Fatalf("unexpected partial-day plan: %+v", parts)
	}

	stats, err := trafficStore.QueryOverviewSummary(ctx, start, end)
	if err != nil {
		t.Fatalf("query partial-day overview: %v", err)
	}
	if stats.BytesUp != 3000 || stats.FlowCount != 2 || stats.DataSource != DataSourceDay {
		t.Fatalf("partial-day summary omitted a boundary bucket: %+v", stats)
	}
}

func TestSummaryCoverageKeepsAlignedRecentWindowExact(t *testing.T) {
	trafficStore := newTestStore(t)
	now := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }
	start := time.Date(2026, 7, 10, 9, 15, 0, 0, time.UTC)
	end := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)

	coverage := trafficStore.SummaryCoverage(start, end)
	if coverage.Approximate || coverage.EffectiveStart != start.Unix() || coverage.EffectiveEnd != end.Unix() {
		t.Fatalf("minute-aligned retained window should remain exact: %+v", coverage)
	}
}

func TestSummaryPlanContinuouslyCoversEffectiveWindowAcrossRetentionEdges(t *testing.T) {
	trafficStore := newTestStore(t)
	now := time.Date(2026, 7, 10, 12, 35, 0, 0, time.UTC)
	trafficStore.now = func() time.Time { return now }

	tests := []struct {
		name  string
		start time.Time
		end   time.Time
	}{
		{
			name:  "single recent partial hour",
			start: time.Date(2026, 7, 10, 10, 20, 10, 0, time.UTC),
			end:   time.Date(2026, 7, 10, 10, 50, 20, 0, time.UTC),
		},
		{
			name:  "minute cutoff splits requested hour",
			start: time.Date(2026, 7, 3, 12, 20, 0, 0, time.UTC),
			end:   time.Date(2026, 7, 3, 12, 40, 0, 0, time.UTC),
		},
		{
			name:  "retained minute edges around complete hours",
			start: time.Date(2026, 7, 3, 12, 36, 0, 0, time.UTC),
			end:   time.Date(2026, 7, 3, 14, 20, 0, 0, time.UTC),
		},
		{
			name:  "day hour and minute layers",
			start: time.Date(2026, 4, 30, 10, 20, 0, 0, time.UTC),
			end:   time.Date(2026, 7, 10, 12, 20, 0, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parts, coverage := trafficStore.summaryPlan(test.start, test.end, usageSourceInfo)
			if len(parts) == 0 {
				t.Fatal("expected a non-empty summary plan")
			}
			cursor := coverage.EffectiveStart
			for index, part := range parts {
				if part.start != cursor {
					t.Fatalf("plan has a gap or overlap before part %d: cursor=%d part=%+v coverage=%+v", index, cursor, part, coverage)
				}
				if part.end <= part.start {
					t.Fatalf("plan has an empty part %d: %+v", index, part)
				}
				cursor = part.end
			}
			if cursor != coverage.EffectiveEnd {
				t.Fatalf("plan does not reach effective end: cursor=%d coverage=%+v parts=%+v", cursor, coverage, parts)
			}
			if coverage.EffectiveStart > test.start.Unix() || coverage.EffectiveEnd < test.end.Unix() {
				t.Fatalf("coverage was not rounded outward: coverage=%+v requested=%d..%d", coverage, test.start.Unix(), test.end.Unix())
			}
		})
	}
}

func TestLogEvidenceIndexesUseEqualityBeforeRangeAndRetentionIndex(t *testing.T) {
	trafficStore := newTestStore(t)
	ctx := context.Background()

	assertPlanUses := func(query string, args []any, expected string) {
		t.Helper()
		rows, err := trafficStore.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatalf("explain query plan: %v", err)
		}
		defer rows.Close()
		var plan string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatalf("scan plan: %v", err)
			}
			plan += detail
		}
		if !strings.Contains(strings.ToLower(plan), strings.ToLower(expected)) {
			t.Fatalf("expected plan to use %s, got %s", expected, plan)
		}
	}

	assertPlanUses("SELECT fingerprint FROM log_evidence WHERE source=? AND client_ip=? AND event_ts>=? AND event_ts<=?", []any{"ss", "198.51.100.1", 1, 2}, "idx_log_evidence_source_client_ts_v2")
	assertPlanUses("DELETE FROM log_evidence WHERE event_ts < ?", []any{1}, "idx_log_evidence_event_ts_v2")
}
