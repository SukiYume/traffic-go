package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
)

var ErrTimeseriesPointLimit = errors.New("timeseries point limit exceeded")

const maxTimeseriesPoints = 10000

type sourceInfo struct {
	Table     string
	TimeCol   string
	DataLabel string
}

// Readiness performs a cheap real query against the read pool. It deliberately
// avoids the heavyweight diagnostics table counts so readiness probes remain
// safe to run frequently on small servers.
func (s *Store) Readiness(ctx context.Context) error {
	if s == nil || s.queryDB() == nil {
		return errors.New("store unavailable")
	}
	var value int
	if err := s.queryDB().QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil {
		return fmt.Errorf("store readiness: %w", err)
	}
	if value != 1 {
		return errors.New("store readiness returned unexpected value")
	}
	return nil
}

func usageSourceInfo(source string) sourceInfo {
	switch source {
	case DataSourceDay:
		return sourceInfo{Table: DataSourceDay, TimeCol: "day_ts", DataLabel: DataSourceDay}
	case DataSourceHour:
		return sourceInfo{Table: DataSourceHour, TimeCol: "hour_ts", DataLabel: DataSourceHour}
	default:
		return sourceInfo{Table: DataSourceMinute, TimeCol: "minute_ts", DataLabel: DataSourceMinute}
	}
}

func forwardSourceInfo(source string) sourceInfo {
	switch source {
	case DataSourceDay:
		return sourceInfo{Table: DataSourceDayForward, TimeCol: "day_ts", DataLabel: DataSourceDayForward}
	case DataSourceHour:
		return sourceInfo{Table: DataSourceHourForward, TimeCol: "hour_ts", DataLabel: DataSourceHourForward}
	default:
		return sourceInfo{Table: DataSourceMinuteForward, TimeCol: "minute_ts", DataLabel: DataSourceMinuteForward}
	}
}

func interfaceSourceInfo(source string) sourceInfo {
	switch source {
	case DataSourceDay:
		return sourceInfo{Table: DataSourceInterfaceDay, TimeCol: "day_ts", DataLabel: DataSourceInterfaceDay}
	case DataSourceHour:
		return sourceInfo{Table: DataSourceInterfaceHour, TimeCol: "hour_ts", DataLabel: DataSourceInterfaceHour}
	default:
		return sourceInfo{Table: DataSourceInterfaceMinute, TimeCol: "minute_ts", DataLabel: DataSourceInterfaceMinute}
	}
}

func EncodeCursor(ts, rowID int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d:%d", ts, rowID)))
}

func DecodeCursor(cursor string) (int64, int64, error) {
	if cursor == "" {
		return 0, 0, nil
	}
	if len(cursor) > 96 {
		return 0, 0, errors.New("invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, 0, err
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid cursor")
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	rowID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if ts <= 0 || rowID <= 0 {
		return 0, 0, errors.New("invalid cursor")
	}
	return ts, rowID, nil
}

func clampLimit(limit int) int {
	if limit <= 0 || limit > 500 {
		return 200
	}
	return limit
}

func clampPage(page int) int {
	if page <= 0 {
		return 1
	}
	if page > 1_000_000 {
		return 1_000_000
	}
	return page
}

func clampPageSize(size int) int {
	if size <= 0 {
		return 50
	}
	if size > 200 {
		return 200
	}
	return size
}

func normalizeSortOrder(order string) string {
	if strings.EqualFold(order, "asc") {
		return "ASC"
	}
	return "DESC"
}

func isAggregatedUsageSource(source string) bool {
	return source == DataSourceHour || source == DataSourceDay
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func trimPage[T any](records []T, limit int, timeBucket func(T) int64, rowID func(T) int64) ([]T, string) {
	if len(records) <= limit {
		return records, ""
	}
	last := records[limit-1]
	return records[:limit], EncodeCursor(timeBucket(last), rowID(last))
}

func countRows(ctx context.Context, db *sql.DB, statement string, args []any, label string) (int, error) {
	var totalRows int
	if err := db.QueryRowContext(ctx, statement, args...).Scan(&totalRows); err != nil {
		return 0, fmt.Errorf("count %s: %w", label, err)
	}
	return totalRows, nil
}

type summaryPart struct {
	info  sourceInfo
	start int64
	end   int64
}

// SummaryCoverage describes the time interval actually covered by a mixed
// retention query. Hour and day rows represent whole buckets, so a request
// whose boundary is older than the finer retention layer must be rounded
// outward instead of silently dropping the partial bucket.
//
// EffectiveStart and EffectiveEnd use the same half-open Unix-second interval
// convention as the summary queries: [effective_start, effective_end).
type SummaryCoverage struct {
	Approximate    bool  `json:"approximate"`
	EffectiveStart int64 `json:"effective_start"`
	EffectiveEnd   int64 `json:"effective_end"`
}

func floorUnix(value int64, unit int64) int64 {
	return (value / unit) * unit
}

func ceilUnix(value int64, unit int64) int64 {
	floored := floorUnix(value, unit)
	if value == floored {
		return value
	}
	return floored + unit
}

func (s *Store) summaryPlan(start, end time.Time, sourceInfo func(string) sourceInfo) ([]summaryPart, SummaryCoverage) {
	startUnix := start.UTC().Unix()
	endUnix := end.UTC().Unix()
	coverage := SummaryCoverage{EffectiveStart: startUnix, EffectiveEnd: endUnix}
	if endUnix <= startUnix {
		return nil, coverage
	}

	now := s.now().UTC()
	minuteStart := minuteRetentionStartUTC(now, s.retention).Unix()
	hourStart := hourRetentionStartUTC(now, s.retention).Unix()
	currentHour := now.Truncate(time.Hour).Unix()

	// Even the finest persisted rows represent complete minutes. Start with
	// minute-aligned outward bounds, then widen them further when only hour or
	// day buckets are retained at a request edge.
	effectiveStart := floorUnix(startUnix, 60)
	effectiveEnd := ceilUnix(endUnix, 60)
	startBucketSeconds := int64(0)
	switch {
	case effectiveStart < hourStart:
		effectiveStart = floorUnix(effectiveStart, 86400)
		startBucketSeconds = 86400
	case effectiveStart < minuteStart:
		effectiveStart = floorUnix(effectiveStart, 3600)
		startBucketSeconds = 3600
	}

	// Resolve the last included persisted minute rather than the exclusive end
	// itself. An end exactly on a retention boundary still belongs to the older
	// layer on its left.
	endProbe := effectiveEnd - 1
	switch {
	case endProbe < hourStart:
		effectiveEnd = ceilUnix(effectiveEnd, 86400)
	case endProbe < minuteStart:
		effectiveEnd = ceilUnix(effectiveEnd, 3600)
	default:
		// The minute cutoff can split an hour. If the requested interval also
		// reaches into the expired part of that same hour, using only the retained
		// minute tail would leave a gap. Include the complete hour instead.
		if effectiveStart < minuteStart &&
			effectiveEnd != floorUnix(effectiveEnd, 3600) &&
			floorUnix(effectiveEnd, 3600) < minuteStart {
			effectiveEnd = ceilUnix(effectiveEnd, 3600)
		}
	}
	if startBucketSeconds > 0 {
		effectiveEnd = maxInt64(effectiveEnd, effectiveStart+startBucketSeconds)
	}

	coverage.EffectiveStart = effectiveStart
	coverage.EffectiveEnd = effectiveEnd
	coverage.Approximate = effectiveStart != startUnix || effectiveEnd != endUnix

	parts := make([]summaryPart, 0, 5)
	appendPart := func(source string, partStart int64, partEnd int64) {
		if partStart >= partEnd {
			return
		}
		info := sourceInfo(source)
		if len(parts) > 0 {
			last := &parts[len(parts)-1]
			if last.end == partStart && last.info.Table == info.Table && last.info.TimeCol == info.TimeCol && last.info.DataLabel == info.DataLabel {
				last.end = partEnd
				return
			}
		}
		parts = append(parts, summaryPart{info: info, start: partStart, end: partEnd})
	}

	// The day/hour retention boundary is a UTC month boundary, hence also a day
	// boundary. Outward-rounded day rows therefore join the hour region without
	// overlap.
	appendPart(DataSourceDay, effectiveStart, minInt64(effectiveEnd, hourStart))

	// Between the day boundary and the current hour, use hour rows for aligned
	// middle buckets. Exact partial-hour edges are served by retained minute rows.
	regionStart := maxInt64(effectiveStart, hourStart)
	regionEnd := minInt64(effectiveEnd, currentHour)
	if regionStart < regionEnd {
		middleStart := regionStart
		if middleStart != floorUnix(middleStart, 3600) {
			leadingEnd := minInt64(regionEnd, ceilUnix(middleStart, 3600))
			appendPart(DataSourceMinute, middleStart, leadingEnd)
			middleStart = leadingEnd
		}

		middleEnd := regionEnd
		trailingStart := regionEnd
		if regionEnd != floorUnix(regionEnd, 3600) {
			trailingStart = maxInt64(middleStart, floorUnix(regionEnd, 3600))
			middleEnd = trailingStart
		}
		appendPart(DataSourceHour, middleStart, middleEnd)
		appendPart(DataSourceMinute, trailingStart, regionEnd)
	}

	// The current hour has not been aggregated yet and is always read from the
	// minute table. This also covers entirely future/empty windows consistently.
	appendPart(DataSourceMinute, maxInt64(effectiveStart, currentHour), effectiveEnd)

	return parts, coverage
}

func (s *Store) summaryParts(start, end time.Time, sourceInfo func(string) sourceInfo) []summaryPart {
	parts, _ := s.summaryPlan(start, end, sourceInfo)
	return parts
}

// SummaryCoverage returns API-ready metadata for the same mixed-retention plan
// used by all summary query families.
func (s *Store) SummaryCoverage(start, end time.Time) SummaryCoverage {
	_, coverage := s.summaryPlan(start, end, usageSourceInfo)
	return coverage
}

func (s *Store) usageSummaryParts(start, end time.Time) []summaryPart {
	return s.summaryParts(start, end, usageSourceInfo)
}

func (s *Store) forwardSummaryParts(start, end time.Time) []summaryPart {
	return s.summaryParts(start, end, forwardSourceInfo)
}

func (s *Store) interfaceSummaryParts(start, end time.Time) []summaryPart {
	return s.summaryParts(start, end, interfaceSourceInfo)
}

func summaryDataLabel(parts []summaryPart, minuteLabel, hourLabel, dayLabel string) string {
	label := minuteLabel
	for _, part := range parts {
		switch part.info.DataLabel {
		case dayLabel:
			return dayLabel
		case hourLabel:
			label = hourLabel
		}
	}
	return label
}

func usageSummaryDataLabel(parts []summaryPart) string {
	return summaryDataLabel(parts, DataSourceMinute, DataSourceHour, DataSourceDay)
}

func interfaceSummaryDataLabel(parts []summaryPart) string {
	return summaryDataLabel(parts, DataSourceInterfaceMinute, DataSourceInterfaceHour, DataSourceInterfaceDay)
}

func forwardSummaryDataLabel(parts []summaryPart) string {
	return summaryDataLabel(parts, DataSourceMinuteForward, DataSourceHourForward, DataSourceDayForward)
}

func buildSummaryUnion(parts []summaryPart, emptySelect string, selectPart func(summaryPart) string) (string, []any) {
	partSQL := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts)*2)
	for _, part := range parts {
		partSQL = append(partSQL, selectPart(part))
		args = append(args, part.start, part.end)
	}
	if len(partSQL) == 0 {
		partSQL = append(partSQL, emptySelect)
	}
	return strings.Join(partSQL, "\nUNION ALL\n"), args
}

func appendOffsetPagination(builder *strings.Builder, args *[]any, orderClause string, page int, pageSize int) {
	appendOffsetPaginationWithLimitExtra(builder, args, orderClause, page, pageSize, 0)
}

func appendOffsetPaginationWithLimitExtra(builder *strings.Builder, args *[]any, orderClause string, page int, pageSize int, extra int) {
	resolvedPage := clampPage(page)
	resolvedPageSize := clampPageSize(pageSize)
	offset := (resolvedPage - 1) * resolvedPageSize
	builder.WriteString(orderClause)
	builder.WriteString(" LIMIT ? OFFSET ?")
	*args = append(*args, resolvedPageSize+extra, offset)
}

func trimOffsetPage[T any](records []T, page int, pageSize int) ([]T, int) {
	resolvedPage := clampPage(page)
	resolvedPageSize := clampPageSize(pageSize)
	offset := (resolvedPage - 1) * resolvedPageSize
	if len(records) <= resolvedPageSize {
		return records, offset + len(records)
	}
	return records[:resolvedPageSize], offset + resolvedPageSize + 1
}

func appendCursorPagination(builder *strings.Builder, args *[]any, timeCol string, cursorTS int64, cursorRowID int64, limit int) int {
	if cursorTS > 0 {
		builder.WriteString(fmt.Sprintf(" AND (%s < ? OR (%s = ? AND rowid < ?))", timeCol, timeCol))
		*args = append(*args, cursorTS, cursorTS, cursorRowID)
	}
	resolvedLimit := clampLimit(limit)
	builder.WriteString(fmt.Sprintf(" ORDER BY %s DESC, rowid DESC LIMIT ?", timeCol))
	*args = append(*args, resolvedLimit+1)
	return resolvedLimit
}

func appendSummaryPagination(builder *strings.Builder, args *[]any, orderClause string, usePage bool, includeTotal bool, page int, pageSize int, cursorTS int64, cursorRowID int64, limit int) {
	if usePage {
		if includeTotal {
			appendOffsetPagination(builder, args, orderClause, page, pageSize)
		} else {
			appendOffsetPaginationWithLimitExtra(builder, args, orderClause, page, pageSize, 1)
		}
		return
	}
	if cursorTS > 0 {
		builder.WriteString(" WHERE (time_bucket < ? OR (time_bucket = ? AND row_id < ?))")
		*args = append(*args, cursorTS, cursorTS, cursorRowID)
	}
	builder.WriteString(" ORDER BY time_bucket DESC, row_id DESC LIMIT ?")
	*args = append(*args, clampLimit(limit)+1)
}

func (s *Store) QueryOverview(ctx context.Context, start, end time.Time, source string) (model.OverviewStats, error) {
	info := usageSourceInfo(source)
	db := s.queryDB()
	row := db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol), start.Unix(), end.Unix())

	var stats model.OverviewStats
	stats.DataSource = info.DataLabel
	if err := row.Scan(&stats.BytesUp, &stats.BytesDown, &stats.FlowCount); err != nil {
		return stats, fmt.Errorf("query overview totals: %w", err)
	}
	return stats, nil
}

func (s *Store) QueryOverviewSummary(ctx context.Context, start, end time.Time) (model.OverviewStats, error) {
	parts := s.usageSummaryParts(start, end)
	partSQL, args := buildSummaryUnion(
		parts,
		`SELECT 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count WHERE 0`,
		func(part summaryPart) string {
			return fmt.Sprintf(`
SELECT bytes_up, bytes_down, flow_count
FROM %[1]s
WHERE %[2]s >= ? AND %[2]s < ?`, part.info.Table, part.info.TimeCol)
		},
	)

	db := s.queryDB()
	row := db.QueryRowContext(ctx, `
WITH summary_usage AS (
`+partSQL+`
)
SELECT COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
FROM summary_usage
`, args...)

	var stats model.OverviewStats
	stats.DataSource = usageSummaryDataLabel(parts)
	if err := row.Scan(&stats.BytesUp, &stats.BytesDown, &stats.FlowCount); err != nil {
		return stats, fmt.Errorf("query overview summary totals: %w", err)
	}
	return stats, nil
}

func (s *Store) QueryMonthlyUsage(ctx context.Context) ([]model.MonthlyUsageSummary, error) {
	now := s.now().UTC()
	s.cacheMu.RLock()
	if s.monthlyCacheUntil.After(now) {
		summaries := cloneMonthlySummaries(s.monthlyCache)
		s.cacheMu.RUnlock()
		return summaries, nil
	}
	s.cacheMu.RUnlock()

	liveMonth := monthStartUTC(now)
	liveStart := liveMonth.Unix()
	hourBoundary := hourRetentionStartUTC(now, s.retention).Unix()
	minuteBoundary := monthlyMinuteBoundaryUTC(now, s.retention).Unix()
	evidenceCutoff := evidenceRetentionStartUTC(now, s.retention).Unix()
	chainBoundary := monthlyChainBoundaryUTC(now, s.retention).Unix()
	liveUpdatedAt := now.Unix()
	cte, args := monthlySingleMonthAggregateCTE(monthlyAggregateWindow{
		start:          liveStart,
		hourBoundary:   hourBoundary,
		minuteBoundary: minuteBoundary,
		evidenceStart:  evidenceCutoff,
		chainBoundary:  chainBoundary,
	}, liveStart)
	query := cte + `,
combined AS (
    SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
           forward_flow_count, evidence_count, chain_count, updated_at, 1 AS archived
    FROM usage_monthly
    UNION ALL
    SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
           forward_flow_count, evidence_count, chain_count, ? AS updated_at, 0 AS archived
    FROM monthly_detail
    WHERE month_ts NOT IN (SELECT month_ts FROM usage_monthly)
)
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, updated_at, archived
FROM combined
ORDER BY month_ts DESC
`
	args = append(args, liveUpdatedAt)
	db := s.queryDB()
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query monthly usage: %w", err)
	}
	defer rows.Close()

	summaries := make([]model.MonthlyUsageSummary, 0)
	for rows.Next() {
		var summary model.MonthlyUsageSummary
		var archived int
		if err := rows.Scan(
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
			&archived,
		); err != nil {
			return nil, fmt.Errorf("scan monthly usage: %w", err)
		}
		summary.Archived = archived != 0
		summary.DetailRange, summary.DetailAvailable = s.detailRangeForMonth(summary.MonthTS)
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate monthly usage: %w", err)
	}
	s.cacheMu.Lock()
	s.monthlyCache = cloneMonthlySummaries(summaries)
	s.monthlyCacheUntil = now.Add(storeShortCacheTTL)
	s.cacheMu.Unlock()
	return summaries, nil
}

func (s *Store) detailRangeForMonth(monthTS int64) (string, bool) {
	now := s.now().UTC()
	month := monthStartUTC(time.Unix(monthTS, 0))
	current := monthStartUTC(now)
	if month.Before(dayRetentionStartUTC(now, s.retention)) || !month.Before(current.AddDate(0, 1, 0)) {
		return "", false
	}

	diff := (current.Year()-month.Year())*12 + int(current.Month()-month.Month())
	switch diff {
	case 0:
		return "this_month", true
	case 1:
		return "last_month", true
	case 2:
		return "two_months_ago", true
	default:
		return "", true
	}
}

func (s *Store) QueryTimeseries(ctx context.Context, query model.TimeseriesQuery, source string) ([]model.TimeseriesPoint, error) {
	info := usageSourceInfo(source)
	bucketSeconds := int64(query.Bucket / time.Second)
	if bucketSeconds <= 0 {
		bucketSeconds = 60
	}
	groupExpr := "direction"
	switch query.GroupBy {
	case "comm":
		groupExpr = "comm"
	case "remote_ip":
		groupExpr = "remote_ip"
	}

	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf(`
SELECT ((%[1]s / ?) * ?) AS bucket_ts,
       %[2]s AS group_value,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM %[3]s
WHERE %[1]s >= ? AND %[1]s < ?
`, info.TimeCol, groupExpr, info.Table))

	args := []any{bucketSeconds, bucketSeconds, query.Start.Unix(), query.End.Unix()}
	appendUsageFilters(&builder, &args, query.Comm, query.RemoteIP, query.Direction, query.Proto)
	if !isAggregatedUsageSource(source) && query.Exe != "" {
		appendExeFilter(&builder, &args, query.Exe)
	}
	if !isAggregatedUsageSource(source) && query.PID != nil {
		builder.WriteString(" AND pid = ?")
		args = append(args, *query.PID)
	}
	builder.WriteString(" GROUP BY bucket_ts, group_value ORDER BY bucket_ts ASC, group_value ASC LIMIT ?")
	args = append(args, maxTimeseriesPoints+1)

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query timeseries: %w", err)
	}
	defer rows.Close()

	points := make([]model.TimeseriesPoint, 0)
	for rows.Next() {
		var point model.TimeseriesPoint
		point.DataSource = info.DataLabel
		if err := rows.Scan(&point.BucketTS, &point.Group, &point.BytesUp, &point.BytesDown, &point.FlowCount); err != nil {
			return nil, fmt.Errorf("scan timeseries: %w", err)
		}
		points = append(points, point)
		if len(points) > maxTimeseriesPoints {
			return nil, ErrTimeseriesPointLimit
		}
	}
	return points, rows.Err()
}

// QueryTimeseriesSummary stitches non-overlapping day/hour/minute retention
// segments together. In particular, the current (not-yet-aggregated) hour is
// always read from usage_1m, so long windows do not silently lose their tail.
func (s *Store) QueryTimeseriesSummary(ctx context.Context, query model.TimeseriesQuery) ([]model.TimeseriesPoint, string, error) {
	parts := s.usageSummaryParts(query.Start, query.End)
	label := usageSummaryDataLabel(parts)
	type pointKey struct {
		ts    int64
		group string
	}
	merged := make(map[pointKey]model.TimeseriesPoint)
	for _, part := range parts {
		partQuery := query
		partQuery.Start = time.Unix(part.start, 0).UTC()
		partQuery.End = time.Unix(part.end, 0).UTC()
		points, err := s.QueryTimeseries(ctx, partQuery, part.info.DataLabel)
		if err != nil {
			return nil, "", err
		}
		for _, point := range points {
			key := pointKey{ts: point.BucketTS, group: point.Group}
			existing := merged[key]
			existing.BucketTS = point.BucketTS
			existing.Group = point.Group
			existing.BytesUp += point.BytesUp
			existing.BytesDown += point.BytesDown
			existing.FlowCount += point.FlowCount
			existing.DataSource = label
			merged[key] = existing
			if len(merged) > maxTimeseriesPoints {
				return nil, "", ErrTimeseriesPointLimit
			}
		}
	}
	points := make([]model.TimeseriesPoint, 0, len(merged))
	for _, point := range merged {
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].BucketTS == points[j].BucketTS {
			return points[i].Group < points[j].Group
		}
		return points[i].BucketTS < points[j].BucketTS
	})
	return points, label, nil
}

func (s *Store) queryInterfaceTimeseries(ctx context.Context, startUnix, endUnix int64, bucketSeconds int64, info sourceInfo) ([]model.InterfaceTimeseriesPoint, error) {
	db := s.queryDB()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT ((%[1]s / ?) * ?) AS bucket_ts,
       interface,
       COALESCE(SUM(rx_bytes), 0),
       COALESCE(SUM(tx_bytes), 0)
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
GROUP BY bucket_ts, interface
ORDER BY bucket_ts ASC, interface ASC
LIMIT ?
`, info.TimeCol, info.Table), bucketSeconds, bucketSeconds, startUnix, endUnix, maxTimeseriesPoints+1)
	if err != nil {
		return nil, fmt.Errorf("query interface timeseries: %w", err)
	}
	defer rows.Close()

	points := make([]model.InterfaceTimeseriesPoint, 0)
	for rows.Next() {
		var point model.InterfaceTimeseriesPoint
		point.DataSource = info.DataLabel
		if err := rows.Scan(&point.BucketTS, &point.Interface, &point.RxBytes, &point.TxBytes); err != nil {
			return nil, fmt.Errorf("scan interface timeseries: %w", err)
		}
		points = append(points, point)
		if len(points) > maxTimeseriesPoints {
			return nil, ErrTimeseriesPointLimit
		}
	}
	return points, rows.Err()
}

func (s *Store) QueryInterfaceTimeseries(ctx context.Context, start, end time.Time, bucket time.Duration, source string) ([]model.InterfaceTimeseriesPoint, error) {
	bucketSeconds := int64(bucket / time.Second)
	if bucketSeconds <= 0 {
		bucketSeconds = 60
	}
	return s.queryInterfaceTimeseries(ctx, start.Unix(), end.Unix(), bucketSeconds, interfaceSourceInfo(source))
}

func (s *Store) QueryInterfaceTimeseriesSummary(ctx context.Context, start, end time.Time, bucket time.Duration) ([]model.InterfaceTimeseriesPoint, string, error) {
	bucketSeconds := int64(bucket / time.Second)
	if bucketSeconds <= 0 {
		bucketSeconds = 60
	}
	parts := s.interfaceSummaryParts(start, end)
	label := interfaceSummaryDataLabel(parts)
	type bucketKey struct {
		ts        int64
		ifaceName string
	}
	merged := make(map[bucketKey]model.InterfaceTimeseriesPoint)
	for _, part := range parts {
		points, err := s.queryInterfaceTimeseries(ctx, part.start, part.end, bucketSeconds, part.info)
		if err != nil {
			return nil, "", err
		}
		for _, point := range points {
			key := bucketKey{ts: point.BucketTS, ifaceName: point.Interface}
			existing := merged[key]
			existing.BucketTS = point.BucketTS
			existing.Interface = point.Interface
			existing.RxBytes += point.RxBytes
			existing.TxBytes += point.TxBytes
			existing.DataSource = label
			merged[key] = existing
			if len(merged) > maxTimeseriesPoints {
				return nil, "", ErrTimeseriesPointLimit
			}
		}
	}
	points := make([]model.InterfaceTimeseriesPoint, 0, len(merged))
	for _, point := range merged {
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].BucketTS == points[j].BucketTS {
			return points[i].Interface < points[j].Interface
		}
		return points[i].BucketTS < points[j].BucketTS
	})
	return points, label, nil
}

func usageOrderClause(source string, sortBy string, sortOrder string, timeCol string) string {
	order := normalizeSortOrder(sortOrder)
	bytesUpExpr := "bytes_up"
	bytesDownExpr := "bytes_down"
	flowCountExpr := "flow_count"
	if isAggregatedUsageSource(source) {
		bytesUpExpr = "COALESCE(SUM(bytes_up), 0)"
		bytesDownExpr = "COALESCE(SUM(bytes_down), 0)"
		flowCountExpr = "COALESCE(SUM(flow_count), 0)"
	}
	switch sortBy {
	case "bytes_up":
		return fmt.Sprintf(" ORDER BY %s %s, %s DESC, rowid DESC", bytesUpExpr, order, timeCol)
	case "bytes_down":
		return fmt.Sprintf(" ORDER BY %s %s, %s DESC, rowid DESC", bytesDownExpr, order, timeCol)
	case "bytes_total":
		return fmt.Sprintf(" ORDER BY (%s + %s) %s, %s DESC, rowid DESC", bytesUpExpr, bytesDownExpr, order, timeCol)
	case "flow_count":
		return fmt.Sprintf(" ORDER BY %s %s, %s DESC, rowid DESC", flowCountExpr, order, timeCol)
	case "remote_ip":
		return fmt.Sprintf(" ORDER BY remote_ip %s, %s DESC, rowid DESC", order, timeCol)
	case "direction":
		return fmt.Sprintf(" ORDER BY direction %s, %s DESC, rowid DESC", order, timeCol)
	case "proto":
		return fmt.Sprintf(" ORDER BY proto %s, %s DESC, rowid DESC", order, timeCol)
	case "local_port":
		return fmt.Sprintf(" ORDER BY local_port %s, %s DESC, rowid DESC", order, timeCol)
	case "comm":
		return fmt.Sprintf(" ORDER BY comm COLLATE NOCASE %s, %s DESC, rowid DESC", order, timeCol)
	case "pid":
		if isAggregatedUsageSource(source) {
			return fmt.Sprintf(" ORDER BY %s DESC, rowid DESC", timeCol)
		}
		return fmt.Sprintf(" ORDER BY pid %s, %s DESC, rowid DESC", order, timeCol)
	default:
		return fmt.Sprintf(" ORDER BY %s %s, rowid %s", timeCol, order, order)
	}
}

func aggregatedUsageLocalPortExpr(query model.UsageQuery) string {
	if query.LocalPort != nil {
		return "local_port"
	}
	return "CASE WHEN direction = 'in' THEN local_port ELSE 0 END"
}

func appendGroupedCursorPagination(builder *strings.Builder, args *[]any, timeCol string, cursorTS int64, cursorRowID int64, limit int) int {
	if cursorTS > 0 {
		builder.WriteString(fmt.Sprintf(" HAVING (%s < ? OR (%s = ? AND MIN(rowid) < ?))", timeCol, timeCol))
		*args = append(*args, cursorTS, cursorTS, cursorRowID)
	}
	resolvedLimit := clampLimit(limit)
	builder.WriteString(fmt.Sprintf(" ORDER BY %s DESC, rowid DESC LIMIT ?", timeCol))
	*args = append(*args, resolvedLimit+1)
	return resolvedLimit
}

func forwardOrderClause(sortBy string, sortOrder string, timeCol string) string {
	order := normalizeSortOrder(sortOrder)
	switch sortBy {
	case "bytes_orig":
		return fmt.Sprintf(" ORDER BY bytes_orig %s, %s DESC, rowid DESC", order, timeCol)
	case "bytes_reply":
		return fmt.Sprintf(" ORDER BY bytes_reply %s, %s DESC, rowid DESC", order, timeCol)
	case "bytes_total":
		return fmt.Sprintf(" ORDER BY (bytes_orig + bytes_reply) %s, %s DESC, rowid DESC", order, timeCol)
	case "flow_count":
		return fmt.Sprintf(" ORDER BY flow_count %s, %s DESC, rowid DESC", order, timeCol)
	case "orig_src_ip":
		return fmt.Sprintf(" ORDER BY orig_src_ip %s, %s DESC, rowid DESC", order, timeCol)
	case "orig_dst_ip":
		return fmt.Sprintf(" ORDER BY orig_dst_ip %s, %s DESC, rowid DESC", order, timeCol)
	default:
		return fmt.Sprintf(" ORDER BY %s %s, rowid %s", timeCol, order, order)
	}
}

func buildUsageCountSQL(query model.UsageQuery, source string) (string, []any, error) {
	info := usageSourceInfo(source)
	aggregatedSource := isAggregatedUsageSource(source)
	var builder strings.Builder
	args := []any{query.Start.Unix(), query.End.Unix()}
	if aggregatedSource {
		if query.Attribution != "" {
			return "", nil, ErrDimensionUnavailable
		}
		if query.RemotePort != nil {
			return "", nil, ErrDimensionUnavailable
		}
		localPortExpr := aggregatedUsageLocalPortExpr(query)
		aggregatedGroupBy := fmt.Sprintf("%s, proto, direction, comm, %s, remote_ip", info.TimeCol, localPortExpr)
		builder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM (
SELECT 1
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
		appendUsageFiltersDetailed(&builder, &args, query, true)
		builder.WriteString(" GROUP BY " + aggregatedGroupBy + ") AS grouped_usage")
		return builder.String(), args, nil
	}
	builder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
	appendUsageFiltersDetailed(&builder, &args, query, false)
	return builder.String(), args, nil
}

func (s *Store) QueryUsageCount(ctx context.Context, query model.UsageQuery, source string) (int, error) {
	statement, args, err := buildUsageCountSQL(query, source)
	if err != nil {
		return 0, err
	}
	return countRows(ctx, s.queryDB(), statement, args, "usage")
}

func (s *Store) QueryUsage(ctx context.Context, query model.UsageQuery, source string) ([]model.UsageRecord, string, int, error) {
	info := usageSourceInfo(source)
	builder := strings.Builder{}
	aggregatedSource := isAggregatedUsageSource(source)
	aggregatedGroupBy := ""
	if aggregatedSource {
		if query.Attribution != "" {
			return nil, "", 0, ErrDimensionUnavailable
		}
		if query.RemotePort != nil {
			return nil, "", 0, ErrDimensionUnavailable
		}
		localPortExpr := aggregatedUsageLocalPortExpr(query)
		aggregatedGroupBy = fmt.Sprintf("%s, proto, direction, comm, %s, remote_ip", info.TimeCol, localPortExpr)
		builder.WriteString(fmt.Sprintf(`
SELECT MIN(rowid) AS rowid, %[1]s, proto, direction, NULL AS pid, comm, NULL AS exe, %[3]s AS local_port, remote_ip, NULL AS remote_port,
       NULL AS attribution,
       COALESCE(SUM(bytes_up), 0) AS bytes_up,
       COALESCE(SUM(bytes_down), 0) AS bytes_down,
       COALESCE(SUM(pkts_up), 0) AS pkts_up,
       COALESCE(SUM(pkts_down), 0) AS pkts_down,
       COALESCE(SUM(flow_count), 0) AS flow_count
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
`, info.TimeCol, info.Table, localPortExpr))
	} else {
		builder.WriteString(fmt.Sprintf(`
SELECT rowid, %s, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port,
       attribution, bytes_up, bytes_down, pkts_up, pkts_down, flow_count
FROM %s
WHERE %s >= ? AND %s < ?
`, info.TimeCol, info.Table, info.TimeCol, info.TimeCol))
	}
	args := []any{query.Start.Unix(), query.End.Unix()}
	appendUsageFiltersDetailed(&builder, &args, query, aggregatedSource)
	if aggregatedSource {
		builder.WriteString(" GROUP BY " + aggregatedGroupBy)
	}
	if query.UsePage {
		db := s.queryDB()
		var totalRows int
		if query.IncludeTotal {
			var err error
			totalRows, err = s.QueryUsageCount(ctx, query, source)
			if err != nil {
				return nil, "", 0, err
			}
			appendOffsetPagination(&builder, &args, usageOrderClause(source, query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize)
		} else {
			appendOffsetPaginationWithLimitExtra(&builder, &args, usageOrderClause(source, query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize, 1)
		}

		rows, err := db.QueryContext(ctx, builder.String(), args...)
		if err != nil {
			return nil, "", 0, fmt.Errorf("query usage: %w", err)
		}
		defer rows.Close()

		records, err := scanUsageRows(rows, info.DataLabel)
		if err != nil {
			return nil, "", 0, err
		}
		if !query.IncludeTotal {
			records, totalRows = trimOffsetPage(records, query.Page, query.PageSize)
		}
		return records, "", totalRows, nil
	}
	limit := 0
	if aggregatedSource {
		limit = appendGroupedCursorPagination(&builder, &args, info.TimeCol, query.CursorTS, query.CursorRowID, query.Limit)
	} else {
		limit = appendCursorPagination(&builder, &args, info.TimeCol, query.CursorTS, query.CursorRowID, query.Limit)
	}

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("query usage: %w", err)
	}
	defer rows.Close()

	records, err := scanUsageRows(rows, info.DataLabel)
	if err != nil {
		return nil, "", 0, err
	}

	records, nextCursor := trimPage(records, limit, func(record model.UsageRecord) int64 {
		return record.TimeBucket
	}, func(record model.UsageRecord) int64 {
		return record.RowID
	})
	return records, nextCursor, 0, nil
}

func buildUsageSummaryRows(parts []summaryPart, query model.UsageQuery) (string, []any, error) {
	if query.Attribution != "" || query.RemotePort != nil || query.PID != nil || strings.TrimSpace(query.Exe) != "" {
		return "", nil, ErrDimensionUnavailable
	}
	selects := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts)*4)
	for _, part := range parts {
		var builder strings.Builder
		if isAggregatedUsageSource(part.info.DataLabel) {
			localPortExpr := aggregatedUsageLocalPortExpr(query)
			builder.WriteString(fmt.Sprintf(`
SELECT MIN(rowid) AS row_id, %[1]s AS time_bucket, proto, direction,
       NULL AS pid, comm, NULL AS exe, %[3]s AS local_port, remote_ip,
       NULL AS remote_port, NULL AS attribution,
       COALESCE(SUM(bytes_up), 0) AS bytes_up,
       COALESCE(SUM(bytes_down), 0) AS bytes_down,
       COALESCE(SUM(pkts_up), 0) AS pkts_up,
       COALESCE(SUM(pkts_down), 0) AS pkts_down,
       COALESCE(SUM(flow_count), 0) AS flow_count,
       '%[4]s' AS data_source
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
`, part.info.TimeCol, part.info.Table, localPortExpr, part.info.DataLabel))
			partArgs := []any{part.start, part.end}
			appendUsageFiltersDetailed(&builder, &partArgs, query, true)
			builder.WriteString(fmt.Sprintf(" GROUP BY %s, proto, direction, comm, %s, remote_ip", part.info.TimeCol, localPortExpr))
			selects = append(selects, builder.String())
			args = append(args, partArgs...)
			continue
		}
		builder.WriteString(fmt.Sprintf(`
SELECT rowid AS row_id, %[1]s AS time_bucket, proto, direction, pid, comm, exe,
       local_port, remote_ip, remote_port, attribution,
       bytes_up, bytes_down, pkts_up, pkts_down, flow_count,
       '%[3]s' AS data_source
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
`, part.info.TimeCol, part.info.Table, part.info.DataLabel))
		partArgs := []any{part.start, part.end}
		appendUsageFiltersDetailed(&builder, &partArgs, query, false)
		selects = append(selects, builder.String())
		args = append(args, partArgs...)
	}
	if len(selects) == 0 {
		return `SELECT 0 AS row_id, 0 AS time_bucket, '' AS proto, '' AS direction,
NULL AS pid, '' AS comm, NULL AS exe, 0 AS local_port, '' AS remote_ip,
NULL AS remote_port, NULL AS attribution, 0 AS bytes_up, 0 AS bytes_down,
0 AS pkts_up, 0 AS pkts_down, 0 AS flow_count, '' AS data_source WHERE 0`, nil, nil
	}
	return strings.Join(selects, "\nUNION ALL\n"), args, nil
}

func usageSummaryOrderClause(sortBy, sortOrder string) string {
	order := normalizeSortOrder(sortOrder)
	switch sortBy {
	case "bytes_up":
		return fmt.Sprintf(" ORDER BY bytes_up %s, time_bucket DESC, row_id DESC", order)
	case "bytes_down":
		return fmt.Sprintf(" ORDER BY bytes_down %s, time_bucket DESC, row_id DESC", order)
	case "bytes_total":
		return fmt.Sprintf(" ORDER BY (bytes_up + bytes_down) %s, time_bucket DESC, row_id DESC", order)
	case "flow_count":
		return fmt.Sprintf(" ORDER BY flow_count %s, time_bucket DESC, row_id DESC", order)
	case "remote_ip":
		return fmt.Sprintf(" ORDER BY remote_ip %s, time_bucket DESC, row_id DESC", order)
	case "direction":
		return fmt.Sprintf(" ORDER BY direction %s, time_bucket DESC, row_id DESC", order)
	case "proto":
		return fmt.Sprintf(" ORDER BY proto %s, time_bucket DESC, row_id DESC", order)
	case "local_port":
		return fmt.Sprintf(" ORDER BY local_port %s, time_bucket DESC, row_id DESC", order)
	case "comm":
		return fmt.Sprintf(" ORDER BY comm COLLATE NOCASE %s, time_bucket DESC, row_id DESC", order)
	default:
		return fmt.Sprintf(" ORDER BY time_bucket %s, row_id %s", order, order)
	}
}

func (s *Store) QueryUsageSummaryCount(ctx context.Context, query model.UsageQuery) (int, string, error) {
	parts := s.usageSummaryParts(query.Start, query.End)
	unionSQL, args, err := buildUsageSummaryRows(parts, query)
	if err != nil {
		return 0, "", err
	}
	total, err := countRows(ctx, s.queryDB(), "SELECT COUNT(*) FROM ("+unionSQL+") AS summary_usage", args, "usage summary")
	return total, usageSummaryDataLabel(parts), err
}

// QueryUsageSummary returns a globally sorted/paged union across the
// retention layers. summaryParts guarantees that the layers do not overlap.
func (s *Store) QueryUsageSummary(ctx context.Context, query model.UsageQuery) ([]model.UsageRecord, string, int, string, error) {
	parts := s.usageSummaryParts(query.Start, query.End)
	dataSource := usageSummaryDataLabel(parts)
	unionSQL, args, err := buildUsageSummaryRows(parts, query)
	if err != nil {
		return nil, "", 0, "", err
	}
	var builder strings.Builder
	builder.WriteString("SELECT * FROM (" + unionSQL + ") AS summary_usage")
	var totalRows int
	if query.UsePage && query.IncludeTotal {
		totalRows, _, err = s.QueryUsageSummaryCount(ctx, query)
		if err != nil {
			return nil, "", 0, "", err
		}
	}
	appendSummaryPagination(&builder, &args, usageSummaryOrderClause(query.SortBy, query.SortOrder), query.UsePage, query.IncludeTotal, query.Page, query.PageSize, query.CursorTS, query.CursorRowID, query.Limit)
	rows, err := s.queryDB().QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, "", 0, "", fmt.Errorf("query usage summary: %w", err)
	}
	defer rows.Close()
	records, err := scanUsageSummaryRows(rows)
	if err != nil {
		return nil, "", 0, "", err
	}
	if query.UsePage {
		if !query.IncludeTotal {
			records, totalRows = trimOffsetPage(records, query.Page, query.PageSize)
		}
		return records, "", totalRows, dataSource, nil
	}
	records, nextCursor := trimPage(records, clampLimit(query.Limit), func(record model.UsageRecord) int64 { return record.TimeBucket }, func(record model.UsageRecord) int64 { return record.RowID })
	return records, nextCursor, 0, dataSource, nil
}

func scanUsageSummaryRows(rows *sql.Rows) ([]model.UsageRecord, error) {
	records := make([]model.UsageRecord, 0)
	for rows.Next() {
		var record model.UsageRecord
		var pid sql.NullInt64
		var exe sql.NullString
		var remotePort sql.NullInt64
		var attribution sql.NullString
		if err := rows.Scan(&record.RowID, &record.TimeBucket, &record.Proto, &record.Direction,
			&pid, &record.Comm, &exe, &record.LocalPort, &record.RemoteIP, &remotePort,
			&attribution, &record.BytesUp, &record.BytesDown, &record.PktsUp, &record.PktsDown,
			&record.FlowCount, &record.DataSource); err != nil {
			return nil, fmt.Errorf("scan usage summary: %w", err)
		}
		record.PID = nullableInt(pid)
		record.Exe = nullableString(exe)
		record.RemotePort = nullableInt(remotePort)
		record.Attribution = nullableAttribution(attribution)
		records = append(records, record)
	}
	return records, rows.Err()
}

func buildForwardUsageCountSQL(query model.ForwardQuery, source string) (string, []any) {
	info := forwardSourceInfo(source)
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
	args := []any{query.Start.Unix(), query.End.Unix()}
	if query.Proto != "" {
		builder.WriteString(" AND proto = ?")
		args = append(args, query.Proto)
	}
	if query.OrigSrcIP != "" {
		builder.WriteString(" AND orig_src_ip = ?")
		args = append(args, query.OrigSrcIP)
	}
	if query.OrigDstIP != "" {
		builder.WriteString(" AND orig_dst_ip = ?")
		args = append(args, query.OrigDstIP)
	}
	return builder.String(), args
}

func (s *Store) QueryForwardUsageCount(ctx context.Context, query model.ForwardQuery, source string) (int, error) {
	statement, args := buildForwardUsageCountSQL(query, source)
	return countRows(ctx, s.queryDB(), statement, args, "forward usage")
}

func scanUsageRows(rows *sql.Rows, dataSource string) ([]model.UsageRecord, error) {
	records := make([]model.UsageRecord, 0)
	for rows.Next() {
		var record model.UsageRecord
		var (
			pidValue        sql.NullInt64
			exeValue        sql.NullString
			remotePortValue sql.NullInt64
			attrValue       sql.NullString
		)
		record.DataSource = dataSource
		if err := rows.Scan(
			&record.RowID,
			&record.TimeBucket,
			&record.Proto,
			&record.Direction,
			&pidValue,
			&record.Comm,
			&exeValue,
			&record.LocalPort,
			&record.RemoteIP,
			&remotePortValue,
			&attrValue,
			&record.BytesUp,
			&record.BytesDown,
			&record.PktsUp,
			&record.PktsDown,
			&record.FlowCount,
		); err != nil {
			return nil, fmt.Errorf("scan usage: %w", err)
		}
		record.PID = nullableInt(pidValue)
		record.Exe = nullableString(exeValue)
		record.RemotePort = nullableInt(remotePortValue)
		record.Attribution = nullableAttribution(attrValue)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) QueryTopProcesses(ctx context.Context, start, end time.Time, source string, groupBy, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.ProcessSummary, int, error) {
	info := usageSourceInfo(source)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	resolvedGroupBy := "pid"
	if isAggregatedUsageSource(source) || strings.EqualFold(groupBy, "comm") {
		resolvedGroupBy = "comm"
	}

	groupExpr := "pid, comm, exe"
	selectExpr := "pid, comm, exe"
	if resolvedGroupBy == "comm" {
		groupExpr = "comm"
		selectExpr = "NULL AS pid, comm, NULL AS exe"
	}

	countSQL := fmt.Sprintf(`
SELECT COUNT(*) FROM (
    SELECT 1
    FROM %s
    WHERE %s >= ? AND %s < ?
    GROUP BY %s
)`, info.Table, info.TimeCol, info.TimeCol, groupExpr)
	var totalRows int
	db := s.queryDB()
	if includeTotal {
		if err := db.QueryRowContext(ctx, countSQL, start.Unix(), end.Unix()).Scan(&totalRows); err != nil {
			return nil, 0, fmt.Errorf("count top processes: %w", err)
		}
	}

	sortExpr := "(SUM(bytes_up) + SUM(bytes_down))"
	switch sortBy {
	case "bytes_total", "total", "":
		// Default total sort expression.
	case "bytes_up":
		sortExpr = "SUM(bytes_up)"
	case "bytes_down":
		sortExpr = "SUM(bytes_down)"
	case "flow_count":
		sortExpr = "SUM(flow_count)"
	case "comm":
		sortExpr = "comm COLLATE NOCASE"
	case "pid":
		if resolvedGroupBy == "pid" {
			sortExpr = "pid"
		} else {
			sortExpr = "comm COLLATE NOCASE"
		}
	}
	order := normalizeSortOrder(sortOrder)

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM %s
WHERE %s >= ? AND %s < ?
GROUP BY %s
ORDER BY %s %s, comm COLLATE NOCASE ASC, comm ASC, COALESCE(pid, -1) ASC, COALESCE(exe, '') ASC
LIMIT ? OFFSET ?
`, selectExpr, info.Table, info.TimeCol, info.TimeCol, groupExpr, sortExpr, order), start.Unix(), end.Unix(), pageSize+boolInt(!includeTotal), pageOffset)
	if err != nil {
		return nil, 0, fmt.Errorf("query top processes: %w", err)
	}
	defer rows.Close()

	var entries []model.ProcessSummary
	for rows.Next() {
		var (
			entry    model.ProcessSummary
			pidValue sql.NullInt64
			exeValue sql.NullString
		)
		entry.DataSource = info.DataLabel
		if err := rows.Scan(&pidValue, &entry.Comm, &exeValue, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, 0, fmt.Errorf("scan top processes: %w", err)
		}
		entry.PID = nullableInt(pidValue)
		entry.Exe = nullableString(exeValue)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if !includeTotal {
		entries, totalRows = trimOffsetPage(entries, 1+(pageOffset/pageSize), pageSize)
	}
	return entries, totalRows, nil
}

func (s *Store) QueryTopProcessesAllLimited(ctx context.Context, start, end time.Time, source string, groupBy string, limit int) ([]model.ProcessSummary, error) {
	info := usageSourceInfo(source)
	resolvedGroupBy := "pid"
	if isAggregatedUsageSource(source) || strings.EqualFold(groupBy, "comm") {
		resolvedGroupBy = "comm"
	}
	groupExpr := "pid, comm, exe"
	selectExpr := "pid, comm, exe"
	if resolvedGroupBy == "comm" {
		groupExpr = "comm"
		selectExpr = "NULL AS pid, comm, NULL AS exe"
	}

	statement := fmt.Sprintf(`
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM %s
WHERE %s >= ? AND %s < ?
GROUP BY %s
`, selectExpr, info.Table, info.TimeCol, info.TimeCol, groupExpr)
	args := []any{start.Unix(), end.Unix()}
	if limit > 0 {
		statement += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.queryDB().QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("query all top processes: %w", err)
	}
	defer rows.Close()
	return scanProcessSummaries(rows, info.DataLabel)
}

func scanProcessSummaries(rows *sql.Rows, dataSource string) ([]model.ProcessSummary, error) {
	var entries []model.ProcessSummary
	for rows.Next() {
		var (
			entry    model.ProcessSummary
			pidValue sql.NullInt64
			exeValue sql.NullString
		)
		entry.DataSource = dataSource
		if err := rows.Scan(&pidValue, &entry.Comm, &exeValue, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, fmt.Errorf("scan top processes: %w", err)
		}
		entry.PID = nullableInt(pidValue)
		entry.Exe = nullableString(exeValue)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func processSummarySortExpr(sortBy string, resolvedGroupBy string) string {
	sortExpr := "(SUM(bytes_up) + SUM(bytes_down))"
	switch sortBy {
	case "bytes_total", "total", "":
	case "bytes_up":
		sortExpr = "SUM(bytes_up)"
	case "bytes_down":
		sortExpr = "SUM(bytes_down)"
	case "flow_count":
		sortExpr = "SUM(flow_count)"
	case "comm":
		sortExpr = "comm COLLATE NOCASE"
	case "pid":
		if resolvedGroupBy == "pid" {
			sortExpr = "pid"
		} else {
			sortExpr = "comm COLLATE NOCASE"
		}
	}
	return sortExpr
}

type processSummarySource struct {
	dataSource      string
	partSQL         string
	args            []any
	resolvedGroupBy string
	groupExpr       string
	selectExpr      string
}

func (s *Store) buildProcessSummarySource(start, end time.Time, groupBy string) processSummarySource {
	parts := s.usageSummaryParts(start, end)
	source := processSummarySource{
		dataSource:      usageSummaryDataLabel(parts),
		resolvedGroupBy: "pid",
		groupExpr:       "pid, comm, exe",
		selectExpr:      "pid, comm, exe",
	}
	if source.dataSource != DataSourceMinute || strings.EqualFold(groupBy, "comm") {
		source.resolvedGroupBy = "comm"
		source.groupExpr = "comm"
		source.selectExpr = "NULL AS pid, comm, NULL AS exe"
	}
	source.partSQL, source.args = buildSummaryUnion(
		parts,
		`SELECT NULL AS pid, '' AS comm, NULL AS exe, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count WHERE 0`,
		func(part summaryPart) string {
			if source.resolvedGroupBy == "pid" && part.info.DataLabel == DataSourceMinute {
				return fmt.Sprintf(`
SELECT pid, comm, exe, bytes_up, bytes_down, flow_count
FROM %[1]s
WHERE %[2]s >= ? AND %[2]s < ?`, part.info.Table, part.info.TimeCol)
			}
			return fmt.Sprintf(`
SELECT NULL AS pid, comm, NULL AS exe, bytes_up, bytes_down, flow_count
FROM %[1]s
WHERE %[2]s >= ? AND %[2]s < ?`, part.info.Table, part.info.TimeCol)
		},
	)
	return source
}

func (s *Store) QueryTopProcessesSummary(ctx context.Context, start, end time.Time, groupBy, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.ProcessSummary, int, string, error) {
	source := s.buildProcessSummarySource(start, end, groupBy)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}
	cte := `
WITH summary_usage AS (
` + source.partSQL + `
)`
	countSQL := fmt.Sprintf(`
%s
SELECT COUNT(*) FROM (
    SELECT 1
    FROM summary_usage
    GROUP BY %s
)`, cte, source.groupExpr)

	db := s.queryDB()
	var totalRows int
	if includeTotal {
		if err := db.QueryRowContext(ctx, countSQL, source.args...).Scan(&totalRows); err != nil {
			return nil, 0, "", fmt.Errorf("count top process summary: %w", err)
		}
	}

	sortExpr := processSummarySortExpr(sortBy, source.resolvedGroupBy)
	order := normalizeSortOrder(sortOrder)
	querySQL := fmt.Sprintf(`
%s
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM summary_usage
GROUP BY %s
ORDER BY %s %s, comm COLLATE NOCASE ASC, comm ASC, COALESCE(pid, -1) ASC, COALESCE(exe, '') ASC
LIMIT ? OFFSET ?
`, cte, source.selectExpr, source.groupExpr, sortExpr, order)
	queryArgs := append(append([]any{}, source.args...), pageSize+boolInt(!includeTotal), pageOffset)
	rows, err := db.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, 0, "", fmt.Errorf("query top process summary: %w", err)
	}
	defer rows.Close()

	var entries []model.ProcessSummary
	for rows.Next() {
		var (
			entry    model.ProcessSummary
			pidValue sql.NullInt64
			exeValue sql.NullString
		)
		entry.DataSource = source.dataSource
		if err := rows.Scan(&pidValue, &entry.Comm, &exeValue, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, 0, "", fmt.Errorf("scan top process summary: %w", err)
		}
		entry.PID = nullableInt(pidValue)
		entry.Exe = nullableString(exeValue)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}
	if !includeTotal {
		entries, totalRows = trimOffsetPage(entries, 1+(pageOffset/pageSize), pageSize)
	}
	return entries, totalRows, source.dataSource, nil
}

func (s *Store) QueryTopProcessesSummaryAllLimited(ctx context.Context, start, end time.Time, groupBy string, limit int) ([]model.ProcessSummary, string, error) {
	source := s.buildProcessSummarySource(start, end, groupBy)
	statement := fmt.Sprintf(`
WITH summary_usage AS (
%s
)
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM summary_usage
GROUP BY %s
`, source.partSQL, source.selectExpr, source.groupExpr)
	if limit > 0 {
		statement += " LIMIT ?"
		source.args = append(source.args, limit)
	}
	rows, err := s.queryDB().QueryContext(ctx, statement, source.args...)
	if err != nil {
		return nil, "", fmt.Errorf("query all top process summary: %w", err)
	}
	defer rows.Close()
	entries, err := scanProcessSummaries(rows, source.dataSource)
	return entries, source.dataSource, err
}

func (s *Store) QueryTopRemotes(ctx context.Context, start, end time.Time, source string, direction model.Direction, includeLoopback bool, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.RemoteSummary, int, error) {
	info := usageSourceInfo(source)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	filterBuilder := strings.Builder{}
	filterBuilder.WriteString(fmt.Sprintf("FROM %s WHERE %s >= ? AND %s < ?", info.Table, info.TimeCol, info.TimeCol))
	args := []any{start.Unix(), end.Unix()}
	if direction != "" {
		filterBuilder.WriteString(" AND direction = ?")
		args = append(args, direction)
	}
	if !includeLoopback {
		filterBuilder.WriteString(" AND remote_ip <> '' AND remote_ip NOT LIKE '127.%' AND remote_ip <> '::1'")
	}

	countSQL := "SELECT COUNT(*) FROM (SELECT 1 " + filterBuilder.String() + " GROUP BY direction, remote_ip)"
	var totalRows int
	db := s.queryDB()
	if includeTotal {
		if err := db.QueryRowContext(ctx, countSQL, args...).Scan(&totalRows); err != nil {
			return nil, 0, fmt.Errorf("count top remotes: %w", err)
		}
	}

	sortExpr := remoteSummarySortExpr(sortBy)
	order := normalizeSortOrder(sortOrder)

	querySQL := fmt.Sprintf(`
SELECT direction, remote_ip, COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
%s
GROUP BY direction, remote_ip
ORDER BY %s %s, remote_ip ASC, direction ASC
LIMIT ? OFFSET ?
`, filterBuilder.String(), sortExpr, order)
	queryArgs := append(append([]any{}, args...), pageSize+boolInt(!includeTotal), pageOffset)
	rows, err := db.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query top remotes: %w", err)
	}
	defer rows.Close()

	var entries []model.RemoteSummary
	for rows.Next() {
		var entry model.RemoteSummary
		entry.DataSource = info.DataLabel
		if err := rows.Scan(&entry.Direction, &entry.RemoteIP, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, 0, fmt.Errorf("scan top remotes: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if !includeTotal {
		entries, totalRows = trimOffsetPage(entries, 1+(pageOffset/pageSize), pageSize)
	}
	return entries, totalRows, nil
}

func (s *Store) QueryTopRemotesAllLimited(ctx context.Context, start, end time.Time, source string, direction model.Direction, includeLoopback bool, limit int) ([]model.RemoteSummary, error) {
	info := usageSourceInfo(source)
	filterBuilder := strings.Builder{}
	filterBuilder.WriteString(fmt.Sprintf("FROM %s WHERE %s >= ? AND %s < ?", info.Table, info.TimeCol, info.TimeCol))
	args := []any{start.Unix(), end.Unix()}
	if direction != "" {
		filterBuilder.WriteString(" AND direction = ?")
		args = append(args, direction)
	}
	if !includeLoopback {
		filterBuilder.WriteString(" AND remote_ip <> '' AND remote_ip NOT LIKE '127.%' AND remote_ip <> '::1'")
	}
	statement := `
SELECT direction, remote_ip, COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
` + filterBuilder.String() + `
GROUP BY direction, remote_ip
`
	if limit > 0 {
		statement += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.queryDB().QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("query all top remotes: %w", err)
	}
	defer rows.Close()
	return scanRemoteSummaries(rows, info.DataLabel)
}

func scanRemoteSummaries(rows *sql.Rows, dataSource string) ([]model.RemoteSummary, error) {
	var entries []model.RemoteSummary
	for rows.Next() {
		var entry model.RemoteSummary
		entry.DataSource = dataSource
		if err := rows.Scan(&entry.Direction, &entry.RemoteIP, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, fmt.Errorf("scan top remotes: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func remoteSummarySortExpr(sortBy string) string {
	sortExpr := "(SUM(bytes_up) + SUM(bytes_down))"
	switch sortBy {
	case "bytes_total", "total", "":
	case "bytes_up":
		sortExpr = "SUM(bytes_up)"
	case "bytes_down":
		sortExpr = "SUM(bytes_down)"
	case "flow_count":
		sortExpr = "SUM(flow_count)"
	case "remote_ip":
		sortExpr = "remote_ip"
	case "direction":
		sortExpr = "direction"
	}
	return sortExpr
}

type remoteSummarySource struct {
	dataSource string
	partSQL    string
	args       []any
	filterSQL  string
	filterArgs []any
}

func (s *Store) buildRemoteSummarySource(start, end time.Time, direction model.Direction, includeLoopback bool) remoteSummarySource {
	parts := s.usageSummaryParts(start, end)
	source := remoteSummarySource{dataSource: usageSummaryDataLabel(parts)}
	source.partSQL, source.args = buildSummaryUnion(
		parts,
		`SELECT '' AS direction, '' AS remote_ip, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count WHERE 0`,
		func(part summaryPart) string {
			return fmt.Sprintf(`
SELECT direction, remote_ip, bytes_up, bytes_down, flow_count
FROM %[1]s
WHERE %[2]s >= ? AND %[2]s < ?`, part.info.Table, part.info.TimeCol)
		},
	)
	filterBuilder := strings.Builder{}
	filterBuilder.WriteString("WHERE 1=1")
	if direction != "" {
		filterBuilder.WriteString(" AND direction = ?")
		source.filterArgs = append(source.filterArgs, direction)
	}
	if !includeLoopback {
		filterBuilder.WriteString(" AND remote_ip <> '' AND remote_ip NOT LIKE '127.%' AND remote_ip <> '::1'")
	}
	source.filterSQL = filterBuilder.String()
	return source
}

func (s *Store) QueryTopRemotesSummary(ctx context.Context, start, end time.Time, direction model.Direction, includeLoopback bool, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.RemoteSummary, int, string, error) {
	source := s.buildRemoteSummarySource(start, end, direction, includeLoopback)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	cte := `
WITH summary_usage AS (
` + source.partSQL + `
)`
	countSQL := fmt.Sprintf(`
%s
SELECT COUNT(*) FROM (
    SELECT 1
    FROM summary_usage
    %s
    GROUP BY direction, remote_ip
)`, cte, source.filterSQL)

	db := s.queryDB()
	var totalRows int
	countArgs := append(append([]any{}, source.args...), source.filterArgs...)
	if includeTotal {
		if err := db.QueryRowContext(ctx, countSQL, countArgs...).Scan(&totalRows); err != nil {
			return nil, 0, "", fmt.Errorf("count top remote summary: %w", err)
		}
	}

	sortExpr := remoteSummarySortExpr(sortBy)
	order := normalizeSortOrder(sortOrder)
	querySQL := fmt.Sprintf(`
%s
SELECT direction, remote_ip, COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
FROM summary_usage
%s
GROUP BY direction, remote_ip
ORDER BY %s %s, remote_ip ASC, direction ASC
LIMIT ? OFFSET ?
`, cte, source.filterSQL, sortExpr, order)
	queryArgs := append(append(append([]any{}, source.args...), source.filterArgs...), pageSize+boolInt(!includeTotal), pageOffset)
	rows, err := db.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, 0, "", fmt.Errorf("query top remote summary: %w", err)
	}
	defer rows.Close()

	var entries []model.RemoteSummary
	for rows.Next() {
		var entry model.RemoteSummary
		entry.DataSource = source.dataSource
		if err := rows.Scan(&entry.Direction, &entry.RemoteIP, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, 0, "", fmt.Errorf("scan top remote summary: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, "", err
	}
	if !includeTotal {
		entries, totalRows = trimOffsetPage(entries, 1+(pageOffset/pageSize), pageSize)
	}
	return entries, totalRows, source.dataSource, nil
}

func (s *Store) QueryTopRemotesSummaryAllLimited(ctx context.Context, start, end time.Time, direction model.Direction, includeLoopback bool, limit int) ([]model.RemoteSummary, string, error) {
	source := s.buildRemoteSummarySource(start, end, direction, includeLoopback)
	queryArgs := append(append([]any{}, source.args...), source.filterArgs...)
	statement := fmt.Sprintf(`
WITH summary_usage AS (
%s
)
SELECT direction, remote_ip, COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
FROM summary_usage
%s
GROUP BY direction, remote_ip
`, source.partSQL, source.filterSQL)
	if limit > 0 {
		statement += " LIMIT ?"
		queryArgs = append(queryArgs, limit)
	}
	rows, err := s.queryDB().QueryContext(ctx, statement, queryArgs...)
	if err != nil {
		return nil, "", fmt.Errorf("query all top remote summary: %w", err)
	}
	defer rows.Close()
	entries, err := scanRemoteSummaries(rows, source.dataSource)
	return entries, source.dataSource, err
}

func (s *Store) QueryTopPorts(ctx context.Context, start, end time.Time, source string, orderBy string) ([]model.TopEntry, error) {
	return s.queryTop(ctx, start, end, source, "CAST(local_port AS TEXT)", orderBy)
}

func topEntrySortExpr(orderBy string) string {
	sortExpr := "SUM(bytes_up + bytes_down)"
	switch orderBy {
	case "bytes_up":
		sortExpr = "SUM(bytes_up)"
	case "bytes_down":
		sortExpr = "SUM(bytes_down)"
	}
	return sortExpr
}

func (s *Store) QueryTopPortsSummary(ctx context.Context, start, end time.Time, orderBy string) ([]model.TopEntry, string, error) {
	parts := s.usageSummaryParts(start, end)
	dataSource := usageSummaryDataLabel(parts)
	partSQL, args := buildSummaryUnion(
		parts,
		`SELECT '' AS item_key, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count WHERE 0`,
		func(part summaryPart) string {
			return fmt.Sprintf(`
SELECT CAST(local_port AS TEXT) AS item_key, bytes_up, bytes_down, flow_count
FROM %[1]s
WHERE %[2]s >= ? AND %[2]s < ?`, part.info.Table, part.info.TimeCol)
		},
	)

	sortExpr := topEntrySortExpr(orderBy)

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
WITH summary_usage AS (
%s
)
SELECT item_key,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM summary_usage
GROUP BY item_key
ORDER BY %s DESC
LIMIT 10
`, partSQL, sortExpr), args...)
	if err != nil {
		return nil, "", fmt.Errorf("query top port summary: %w", err)
	}
	defer rows.Close()

	var entries []model.TopEntry
	for rows.Next() {
		var entry model.TopEntry
		entry.DataSource = dataSource
		if err := rows.Scan(&entry.Key, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, "", fmt.Errorf("scan top port summary: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, dataSource, rows.Err()
}

func (s *Store) queryTop(ctx context.Context, start, end time.Time, source string, groupExpr string, orderBy string) ([]model.TopEntry, error) {
	info := usageSourceInfo(source)
	sortExpr := topEntrySortExpr(orderBy)

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT %s AS item_key,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM %s
WHERE %s >= ? AND %s < ?
GROUP BY item_key
ORDER BY %s DESC
LIMIT 10
`, groupExpr, info.Table, info.TimeCol, info.TimeCol, sortExpr), start.Unix(), end.Unix())
	if err != nil {
		return nil, fmt.Errorf("query top: %w", err)
	}
	defer rows.Close()

	var entries []model.TopEntry
	for rows.Next() {
		var entry model.TopEntry
		entry.DataSource = info.DataLabel
		if err := rows.Scan(&entry.Key, &entry.BytesUp, &entry.BytesDown, &entry.FlowCount); err != nil {
			return nil, fmt.Errorf("scan top: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func (s *Store) QueryForwardUsage(ctx context.Context, query model.ForwardQuery, source string) ([]model.ForwardUsageRecord, string, int, error) {
	info := forwardSourceInfo(source)
	builder := strings.Builder{}
	countBuilder := strings.Builder{}
	builder.WriteString(fmt.Sprintf(`
SELECT rowid, %s, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
       bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
FROM %s
WHERE %s >= ? AND %s < ?
`, info.TimeCol, info.Table, info.TimeCol, info.TimeCol))
	countBuilder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
	args := []any{query.Start.Unix(), query.End.Unix()}
	countArgs := []any{query.Start.Unix(), query.End.Unix()}
	if query.Proto != "" {
		builder.WriteString(" AND proto = ?")
		args = append(args, query.Proto)
		countBuilder.WriteString(" AND proto = ?")
		countArgs = append(countArgs, query.Proto)
	}
	if query.OrigSrcIP != "" {
		builder.WriteString(" AND orig_src_ip = ?")
		args = append(args, query.OrigSrcIP)
		countBuilder.WriteString(" AND orig_src_ip = ?")
		countArgs = append(countArgs, query.OrigSrcIP)
	}
	if query.OrigDstIP != "" {
		builder.WriteString(" AND orig_dst_ip = ?")
		args = append(args, query.OrigDstIP)
		countBuilder.WriteString(" AND orig_dst_ip = ?")
		countArgs = append(countArgs, query.OrigDstIP)
	}
	if query.UsePage {
		db := s.queryDB()
		var totalRows int
		if query.IncludeTotal {
			var err error
			totalRows, err = countRows(ctx, db, countBuilder.String(), countArgs, "forward usage")
			if err != nil {
				return nil, "", 0, err
			}
			appendOffsetPagination(&builder, &args, forwardOrderClause(query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize)
		} else {
			appendOffsetPaginationWithLimitExtra(&builder, &args, forwardOrderClause(query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize, 1)
		}

		rows, err := db.QueryContext(ctx, builder.String(), args...)
		if err != nil {
			return nil, "", 0, fmt.Errorf("query forward usage: %w", err)
		}
		defer rows.Close()

		records, err := scanForwardRows(rows, info.DataLabel)
		if err != nil {
			return nil, "", 0, err
		}
		if !query.IncludeTotal {
			records, totalRows = trimOffsetPage(records, query.Page, query.PageSize)
		}
		return records, "", totalRows, nil
	}
	limit := appendCursorPagination(&builder, &args, info.TimeCol, query.CursorTS, query.CursorRowID, query.Limit)

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("query forward usage: %w", err)
	}
	defer rows.Close()

	records, err := scanForwardRows(rows, info.DataLabel)
	if err != nil {
		return nil, "", 0, err
	}

	records, nextCursor := trimPage(records, limit, func(record model.ForwardUsageRecord) int64 {
		return record.TimeBucket
	}, func(record model.ForwardUsageRecord) int64 {
		return record.RowID
	})
	return records, nextCursor, 0, nil
}

func buildForwardSummaryRows(parts []summaryPart, query model.ForwardQuery) (string, []any) {
	selects := make([]string, 0, len(parts))
	args := make([]any, 0, len(parts)*4)
	for _, part := range parts {
		var builder strings.Builder
		builder.WriteString(fmt.Sprintf(`
SELECT rowid AS row_id, %[1]s AS time_bucket, proto, orig_src_ip, orig_dst_ip,
       orig_sport, orig_dport, bytes_orig, bytes_reply, pkts_orig, pkts_reply,
       flow_count, '%[3]s' AS data_source
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
`, part.info.TimeCol, part.info.Table, part.info.DataLabel))
		partArgs := []any{part.start, part.end}
		if query.Proto != "" {
			builder.WriteString(" AND proto = ?")
			partArgs = append(partArgs, query.Proto)
		}
		if query.OrigSrcIP != "" {
			builder.WriteString(" AND orig_src_ip = ?")
			partArgs = append(partArgs, query.OrigSrcIP)
		}
		if query.OrigDstIP != "" {
			builder.WriteString(" AND orig_dst_ip = ?")
			partArgs = append(partArgs, query.OrigDstIP)
		}
		selects = append(selects, builder.String())
		args = append(args, partArgs...)
	}
	if len(selects) == 0 {
		return `SELECT 0 AS row_id, 0 AS time_bucket, '' AS proto, '' AS orig_src_ip,
'' AS orig_dst_ip, 0 AS orig_sport, 0 AS orig_dport, 0 AS bytes_orig,
0 AS bytes_reply, 0 AS pkts_orig, 0 AS pkts_reply, 0 AS flow_count,
'' AS data_source WHERE 0`, nil
	}
	return strings.Join(selects, "\nUNION ALL\n"), args
}

func forwardSummaryOrderClause(sortBy, sortOrder string) string {
	order := normalizeSortOrder(sortOrder)
	switch sortBy {
	case "bytes_orig":
		return fmt.Sprintf(" ORDER BY bytes_orig %s, time_bucket DESC, row_id DESC", order)
	case "bytes_reply":
		return fmt.Sprintf(" ORDER BY bytes_reply %s, time_bucket DESC, row_id DESC", order)
	case "bytes_total":
		return fmt.Sprintf(" ORDER BY (bytes_orig + bytes_reply) %s, time_bucket DESC, row_id DESC", order)
	case "flow_count":
		return fmt.Sprintf(" ORDER BY flow_count %s, time_bucket DESC, row_id DESC", order)
	case "orig_src_ip":
		return fmt.Sprintf(" ORDER BY orig_src_ip %s, time_bucket DESC, row_id DESC", order)
	case "orig_dst_ip":
		return fmt.Sprintf(" ORDER BY orig_dst_ip %s, time_bucket DESC, row_id DESC", order)
	default:
		return fmt.Sprintf(" ORDER BY time_bucket %s, row_id %s", order, order)
	}
}

func (s *Store) QueryForwardUsageSummaryCount(ctx context.Context, query model.ForwardQuery) (int, string, error) {
	parts := s.forwardSummaryParts(query.Start, query.End)
	unionSQL, args := buildForwardSummaryRows(parts, query)
	total, err := countRows(ctx, s.queryDB(), "SELECT COUNT(*) FROM ("+unionSQL+") AS summary_forward", args, "forward usage summary")
	return total, forwardSummaryDataLabel(parts), err
}

func (s *Store) QueryForwardUsageSummary(ctx context.Context, query model.ForwardQuery) ([]model.ForwardUsageRecord, string, int, string, error) {
	parts := s.forwardSummaryParts(query.Start, query.End)
	dataSource := forwardSummaryDataLabel(parts)
	unionSQL, args := buildForwardSummaryRows(parts, query)
	var builder strings.Builder
	builder.WriteString("SELECT * FROM (" + unionSQL + ") AS summary_forward")
	var totalRows int
	var err error
	if query.UsePage && query.IncludeTotal {
		totalRows, _, err = s.QueryForwardUsageSummaryCount(ctx, query)
		if err != nil {
			return nil, "", 0, "", err
		}
	}
	appendSummaryPagination(&builder, &args, forwardSummaryOrderClause(query.SortBy, query.SortOrder), query.UsePage, query.IncludeTotal, query.Page, query.PageSize, query.CursorTS, query.CursorRowID, query.Limit)
	rows, err := s.queryDB().QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, "", 0, "", fmt.Errorf("query forward usage summary: %w", err)
	}
	defer rows.Close()
	records, err := scanForwardSummaryRows(rows)
	if err != nil {
		return nil, "", 0, "", err
	}
	if query.UsePage {
		if !query.IncludeTotal {
			records, totalRows = trimOffsetPage(records, query.Page, query.PageSize)
		}
		return records, "", totalRows, dataSource, nil
	}
	records, nextCursor := trimPage(records, clampLimit(query.Limit), func(record model.ForwardUsageRecord) int64 { return record.TimeBucket }, func(record model.ForwardUsageRecord) int64 { return record.RowID })
	return records, nextCursor, 0, dataSource, nil
}

func scanForwardSummaryRows(rows *sql.Rows) ([]model.ForwardUsageRecord, error) {
	records := make([]model.ForwardUsageRecord, 0)
	for rows.Next() {
		var record model.ForwardUsageRecord
		if err := rows.Scan(&record.RowID, &record.TimeBucket, &record.Proto, &record.OrigSrcIP,
			&record.OrigDstIP, &record.OrigSPort, &record.OrigDPort, &record.BytesOrig,
			&record.BytesReply, &record.PktsOrig, &record.PktsReply, &record.FlowCount,
			&record.DataSource); err != nil {
			return nil, fmt.Errorf("scan forward usage summary: %w", err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func scanForwardRows(rows *sql.Rows, dataSource string) ([]model.ForwardUsageRecord, error) {
	var records []model.ForwardUsageRecord
	for rows.Next() {
		var record model.ForwardUsageRecord
		record.DataSource = dataSource
		if err := rows.Scan(
			&record.RowID,
			&record.TimeBucket,
			&record.Proto,
			&record.OrigSrcIP,
			&record.OrigDstIP,
			&record.OrigSPort,
			&record.OrigDPort,
			&record.BytesOrig,
			&record.BytesReply,
			&record.PktsOrig,
			&record.PktsReply,
			&record.FlowCount,
		); err != nil {
			return nil, fmt.Errorf("scan forward usage: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func appendUsageFilters(builder *strings.Builder, args *[]any, comm, remoteIP string, direction model.Direction, proto string) {
	if comm != "" {
		builder.WriteString(" AND comm = ?")
		*args = append(*args, comm)
	}
	if remoteIP != "" {
		builder.WriteString(" AND remote_ip = ?")
		*args = append(*args, remoteIP)
	}
	if direction != "" {
		builder.WriteString(" AND direction = ?")
		*args = append(*args, direction)
	}
	if proto != "" {
		builder.WriteString(" AND proto = ?")
		*args = append(*args, proto)
	}
}

func appendUsageFiltersDetailed(builder *strings.Builder, args *[]any, query model.UsageQuery, hourSource bool) {
	appendUsageFilters(builder, args, query.Comm, query.RemoteIP, query.Direction, query.Proto)
	if !hourSource && query.Exe != "" {
		appendExeFilter(builder, args, query.Exe)
	}
	if !hourSource && query.PID != nil {
		builder.WriteString(" AND pid = ?")
		*args = append(*args, *query.PID)
	}
	if query.LocalPort != nil {
		builder.WriteString(" AND local_port = ?")
		*args = append(*args, *query.LocalPort)
	}
	if !hourSource && query.RemotePort != nil {
		builder.WriteString(" AND remote_port = ?")
		*args = append(*args, *query.RemotePort)
	}
	if !hourSource && query.Attribution != "" {
		builder.WriteString(" AND attribution = ?")
		*args = append(*args, query.Attribution)
	}
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, "!", "!!")
	value = strings.ReplaceAll(value, "%", "!%")
	value = strings.ReplaceAll(value, "_", "!_")
	return value
}

func appendExeFilter(builder *strings.Builder, args *[]any, exe string) {
	trimmed := strings.TrimSpace(exe)
	if trimmed == "" {
		return
	}
	escaped := escapeLike(trimmed)
	builder.WriteString(" AND (exe = ? OR exe LIKE ? ESCAPE '!' OR exe LIKE ? ESCAPE '!')")
	*args = append(*args, trimmed, "%/"+escaped, `%\`+escaped)
}

func nullableInt(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func nullableAttribution(value sql.NullString) *model.Attribution {
	if !value.Valid || value.String == "" {
		return nil
	}
	result := model.Attribution(value.String)
	return &result
}
