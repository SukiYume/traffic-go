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

type sourceInfo struct {
	Table     string
	TimeCol   string
	DataLabel string
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

func (s *Store) summaryParts(start, end time.Time, sourceInfo func(string) sourceInfo) []summaryPart {
	startUnix := start.UTC().Unix()
	endUnix := end.UTC().Unix()
	if endUnix <= startUnix {
		return nil
	}

	now := s.now().UTC()
	minuteStart := minuteRetentionStartUTC(now, s.retention).Unix()
	hourStart := hourRetentionStartUTC(now, s.retention).Unix()
	currentHour := now.Truncate(time.Hour).Unix()

	parts := make([]summaryPart, 0, 4)
	appendPart := func(source string, partStart int64, partEnd int64) {
		if partStart >= partEnd {
			return
		}
		parts = append(parts, summaryPart{info: sourceInfo(source), start: partStart, end: partEnd})
	}

	dayStart := ceilUnix(startUnix, 86400)
	dayEnd := minInt64(floorUnix(endUnix, 86400), hourStart)
	appendPart(DataSourceDay, dayStart, dayEnd)

	fullHourStart := ceilUnix(maxInt64(startUnix, hourStart), 3600)
	fullHourEnd := minInt64(floorUnix(endUnix, 3600), currentHour)
	appendPart(DataSourceHour, fullHourStart, fullHourEnd)

	if startUnix >= minuteStart {
		appendPart(DataSourceMinute, startUnix, minInt64(endUnix, ceilUnix(startUnix, 3600)))
	}

	tailStart := maxInt64(ceilUnix(startUnix, 3600), minInt64(floorUnix(endUnix, 3600), currentHour))
	tailStart = maxInt64(tailStart, minuteStart)
	appendPart(DataSourceMinute, tailStart, endUnix)

	if len(parts) == 0 {
		source, err := s.ResolveUsageSource(start, end, false)
		if err != nil {
			return nil
		}
		appendPart(source, startUnix, endUnix)
	}
	return parts
}

func (s *Store) usageSummaryParts(start, end time.Time) []summaryPart {
	return s.summaryParts(start, end, usageSourceInfo)
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
	builder.WriteString(" GROUP BY bucket_ts, group_value ORDER BY bucket_ts ASC, group_value ASC")

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
	}
	return points, rows.Err()
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
`, info.TimeCol, info.Table), bucketSeconds, bucketSeconds, startUnix, endUnix)
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

func (s *Store) QueryUsage(ctx context.Context, query model.UsageQuery, source string) ([]model.UsageRecord, string, int, error) {
	info := usageSourceInfo(source)
	builder := strings.Builder{}
	countBuilder := strings.Builder{}
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
		countBuilder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM (
SELECT 1
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
	} else {
		builder.WriteString(fmt.Sprintf(`
SELECT rowid, %s, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port,
       attribution, bytes_up, bytes_down, pkts_up, pkts_down, flow_count
FROM %s
WHERE %s >= ? AND %s < ?
`, info.TimeCol, info.Table, info.TimeCol, info.TimeCol))
		countBuilder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
FROM %s
WHERE %s >= ? AND %s < ?
`, info.Table, info.TimeCol, info.TimeCol))
	}
	args := []any{query.Start.Unix(), query.End.Unix()}
	countArgs := []any{query.Start.Unix(), query.End.Unix()}
	appendUsageFiltersDetailed(&builder, &args, query, aggregatedSource)
	appendUsageFiltersDetailed(&countBuilder, &countArgs, query, aggregatedSource)
	if aggregatedSource {
		builder.WriteString(" GROUP BY " + aggregatedGroupBy)
		countBuilder.WriteString(" GROUP BY " + aggregatedGroupBy + ") AS grouped_usage")
	}
	if query.UsePage {
		db := s.queryDB()
		var totalRows int
		if query.IncludeTotal {
			var err error
			totalRows, err = countRows(ctx, db, countBuilder.String(), countArgs, "usage")
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
ORDER BY %s %s, comm COLLATE NOCASE ASC
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
		}
	}
	return sortExpr
}

func (s *Store) QueryTopProcessesSummary(ctx context.Context, start, end time.Time, groupBy, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.ProcessSummary, int, string, error) {
	parts := s.usageSummaryParts(start, end)
	dataSource := usageSummaryDataLabel(parts)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	resolvedGroupBy := "pid"
	if dataSource != DataSourceMinute || strings.EqualFold(groupBy, "comm") {
		resolvedGroupBy = "comm"
	}

	partSQL, args := buildSummaryUnion(
		parts,
		`SELECT NULL AS pid, '' AS comm, NULL AS exe, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count WHERE 0`,
		func(part summaryPart) string {
			if resolvedGroupBy == "pid" && part.info.DataLabel == DataSourceMinute {
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

	groupExpr := "pid, comm, exe"
	selectExpr := "pid, comm, exe"
	if resolvedGroupBy == "comm" {
		groupExpr = "comm"
		selectExpr = "NULL AS pid, comm, NULL AS exe"
	}
	cte := `
WITH summary_usage AS (
` + partSQL + `
)`
	countSQL := fmt.Sprintf(`
%s
SELECT COUNT(*) FROM (
    SELECT 1
    FROM summary_usage
    GROUP BY %s
)`, cte, groupExpr)

	db := s.queryDB()
	var totalRows int
	if includeTotal {
		if err := db.QueryRowContext(ctx, countSQL, args...).Scan(&totalRows); err != nil {
			return nil, 0, "", fmt.Errorf("count top process summary: %w", err)
		}
	}

	sortExpr := processSummarySortExpr(sortBy, resolvedGroupBy)
	order := normalizeSortOrder(sortOrder)
	querySQL := fmt.Sprintf(`
%s
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM summary_usage
GROUP BY %s
ORDER BY %s %s, comm COLLATE NOCASE ASC
LIMIT ? OFFSET ?
`, cte, selectExpr, groupExpr, sortExpr, order)
	queryArgs := append(append([]any{}, args...), pageSize+boolInt(!includeTotal), pageOffset)
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
		entry.DataSource = dataSource
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
	return entries, totalRows, dataSource, nil
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
ORDER BY %s %s, remote_ip ASC
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

func (s *Store) QueryTopRemotesSummary(ctx context.Context, start, end time.Time, direction model.Direction, includeLoopback bool, sortBy, sortOrder string, limit, offset int, includeTotal bool) ([]model.RemoteSummary, int, string, error) {
	parts := s.usageSummaryParts(start, end)
	dataSource := usageSummaryDataLabel(parts)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	partSQL, args := buildSummaryUnion(
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
	filterArgs := make([]any, 0, 1)
	filterBuilder.WriteString("WHERE 1=1")
	if direction != "" {
		filterBuilder.WriteString(" AND direction = ?")
		filterArgs = append(filterArgs, direction)
	}
	if !includeLoopback {
		filterBuilder.WriteString(" AND remote_ip <> '' AND remote_ip NOT LIKE '127.%' AND remote_ip <> '::1'")
	}
	cte := `
WITH summary_usage AS (
` + partSQL + `
)`
	countSQL := fmt.Sprintf(`
%s
SELECT COUNT(*) FROM (
    SELECT 1
    FROM summary_usage
    %s
    GROUP BY direction, remote_ip
)`, cte, filterBuilder.String())

	db := s.queryDB()
	var totalRows int
	countArgs := append(append([]any{}, args...), filterArgs...)
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
ORDER BY %s %s, remote_ip ASC
LIMIT ? OFFSET ?
`, cte, filterBuilder.String(), sortExpr, order)
	queryArgs := append(append(append([]any{}, args...), filterArgs...), pageSize+boolInt(!includeTotal), pageOffset)
	rows, err := db.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, 0, "", fmt.Errorf("query top remote summary: %w", err)
	}
	defer rows.Close()

	var entries []model.RemoteSummary
	for rows.Next() {
		var entry model.RemoteSummary
		entry.DataSource = dataSource
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
	return entries, totalRows, dataSource, nil
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
