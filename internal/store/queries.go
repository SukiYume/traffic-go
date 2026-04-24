package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
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
	case DataSourceHour:
		return sourceInfo{Table: "usage_1h", TimeCol: "hour_ts", DataLabel: DataSourceHour}
	default:
		return sourceInfo{Table: "usage_1m", TimeCol: "minute_ts", DataLabel: DataSourceMinute}
	}
}

func forwardSourceInfo(source string) sourceInfo {
	switch source {
	case DataSourceHour:
		return sourceInfo{Table: DataSourceHourForward, TimeCol: "hour_ts", DataLabel: DataSourceHourForward}
	default:
		return sourceInfo{Table: DataSourceMinuteForward, TimeCol: "minute_ts", DataLabel: DataSourceMinuteForward}
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

func appendOffsetPagination(builder *strings.Builder, args *[]any, orderClause string, page int, pageSize int) {
	resolvedPage := clampPage(page)
	resolvedPageSize := clampPageSize(pageSize)
	offset := (resolvedPage - 1) * resolvedPageSize
	builder.WriteString(orderClause)
	builder.WriteString(" LIMIT ? OFFSET ?")
	*args = append(*args, resolvedPageSize, offset)
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
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`
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

func (s *Store) QueryMonthlyUsage(ctx context.Context) ([]model.MonthlyUsageSummary, error) {
	now := s.now().UTC()
	liveStart := retentionStartUTC(now, s.retention).Unix()
	liveUpdatedAt := now.Unix()
	query := monthlyDetailAggregateCTE(monthlyDetailFromInclusive) + `,
combined AS (
    SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
           forward_flow_count, evidence_count, chain_count, updated_at, 1 AS archived
    FROM usage_monthly
    WHERE month_ts NOT IN (SELECT month_ts FROM monthly_detail)
    UNION ALL
    SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
           forward_flow_count, evidence_count, chain_count, ? AS updated_at, 0 AS archived
    FROM monthly_detail
)
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, updated_at, archived
FROM combined
ORDER BY month_ts DESC
`
	args := append(monthlyDetailAggregateArgs(liveStart), liveUpdatedAt)
	rows, err := s.db.QueryContext(ctx, query, args...)
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
		if !summary.Archived {
			summary.DetailRange, summary.DetailAvailable = s.detailRangeForMonth(summary.MonthTS)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate monthly usage: %w", err)
	}
	return summaries, nil
}

func (s *Store) detailRangeForMonth(monthTS int64) (string, bool) {
	now := s.now().UTC()
	month := monthStartUTC(time.Unix(monthTS, 0))
	current := monthStartUTC(now)
	if month.Before(retentionStartUTC(now, s.retention)) || !month.Before(current.AddDate(0, 1, 0)) {
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
	if source != DataSourceHour && query.Exe != "" {
		appendExeFilter(&builder, &args, query.Exe)
	}
	if source != DataSourceHour && query.PID != nil {
		builder.WriteString(" AND pid = ?")
		args = append(args, *query.PID)
	}
	builder.WriteString(" GROUP BY bucket_ts, group_value ORDER BY bucket_ts ASC, group_value ASC")

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
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

func usageOrderClause(source string, sortBy string, sortOrder string, timeCol string) string {
	order := normalizeSortOrder(sortOrder)
	switch sortBy {
	case "bytes_up":
		return fmt.Sprintf(" ORDER BY bytes_up %s, %s DESC, rowid DESC", order, timeCol)
	case "bytes_down":
		return fmt.Sprintf(" ORDER BY bytes_down %s, %s DESC, rowid DESC", order, timeCol)
	case "bytes_total":
		return fmt.Sprintf(" ORDER BY (bytes_up + bytes_down) %s, %s DESC, rowid DESC", order, timeCol)
	case "flow_count":
		return fmt.Sprintf(" ORDER BY flow_count %s, %s DESC, rowid DESC", order, timeCol)
	case "remote_ip":
		return fmt.Sprintf(" ORDER BY remote_ip %s, %s DESC, rowid DESC", order, timeCol)
	case "direction":
		return fmt.Sprintf(" ORDER BY direction %s, %s DESC, rowid DESC", order, timeCol)
	case "local_port":
		return fmt.Sprintf(" ORDER BY local_port %s, %s DESC, rowid DESC", order, timeCol)
	case "comm":
		return fmt.Sprintf(" ORDER BY comm COLLATE NOCASE %s, %s DESC, rowid DESC", order, timeCol)
	case "pid":
		if source == DataSourceHour {
			return fmt.Sprintf(" ORDER BY %s DESC, rowid DESC", timeCol)
		}
		return fmt.Sprintf(" ORDER BY pid %s, %s DESC, rowid DESC", order, timeCol)
	default:
		return fmt.Sprintf(" ORDER BY %s %s, rowid %s", timeCol, order, order)
	}
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
	if source == DataSourceHour {
		if query.Attribution != "" {
			return nil, "", 0, ErrDimensionUnavailable
		}
		if query.RemotePort != nil {
			return nil, "", 0, ErrDimensionUnavailable
		}
		builder.WriteString(fmt.Sprintf(`
SELECT rowid, %s, proto, direction, NULL AS pid, comm, NULL AS exe, local_port, remote_ip, NULL AS remote_port,
       NULL AS attribution, bytes_up, bytes_down, pkts_up, pkts_down, flow_count
FROM %s
WHERE %s >= ? AND %s < ?
`, info.TimeCol, info.Table, info.TimeCol, info.TimeCol))
		countBuilder.WriteString(fmt.Sprintf(`
SELECT COUNT(*)
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
	appendUsageFiltersDetailed(&builder, &args, query, source == DataSourceHour)
	appendUsageFiltersDetailed(&countBuilder, &countArgs, query, source == DataSourceHour)
	if query.UsePage {
		totalRows, err := countRows(ctx, s.db, countBuilder.String(), countArgs, "usage")
		if err != nil {
			return nil, "", 0, err
		}
		appendOffsetPagination(&builder, &args, usageOrderClause(source, query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize)

		rows, err := s.db.QueryContext(ctx, builder.String(), args...)
		if err != nil {
			return nil, "", 0, fmt.Errorf("query usage: %w", err)
		}
		defer rows.Close()

		records, err := scanUsageRows(rows, info.DataLabel)
		if err != nil {
			return nil, "", 0, err
		}
		return records, "", totalRows, nil
	}
	limit := appendCursorPagination(&builder, &args, info.TimeCol, query.CursorTS, query.CursorRowID, query.Limit)

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
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

func (s *Store) QueryTopProcesses(ctx context.Context, start, end time.Time, source string, groupBy, sortBy, sortOrder string, limit, offset int) ([]model.ProcessSummary, int, error) {
	info := usageSourceInfo(source)
	pageSize := clampPageSize(limit)
	pageOffset := offset
	if pageOffset < 0 {
		pageOffset = 0
	}

	resolvedGroupBy := "pid"
	if source == DataSourceHour || strings.EqualFold(groupBy, "comm") {
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
	if err := s.db.QueryRowContext(ctx, countSQL, start.Unix(), end.Unix()).Scan(&totalRows); err != nil {
		return nil, 0, fmt.Errorf("count top processes: %w", err)
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

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
SELECT %s,
       COALESCE(SUM(bytes_up), 0),
       COALESCE(SUM(bytes_down), 0),
       COALESCE(SUM(flow_count), 0)
FROM %s
WHERE %s >= ? AND %s < ?
GROUP BY %s
ORDER BY %s %s, comm COLLATE NOCASE ASC
LIMIT ? OFFSET ?
`, selectExpr, info.Table, info.TimeCol, info.TimeCol, groupExpr, sortExpr, order), start.Unix(), end.Unix(), pageSize, pageOffset)
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
	return entries, totalRows, rows.Err()
}

func (s *Store) QueryTopRemotes(ctx context.Context, start, end time.Time, source string, direction model.Direction, includeLoopback bool, sortBy, sortOrder string, limit, offset int) ([]model.RemoteSummary, int, error) {
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
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&totalRows); err != nil {
		return nil, 0, fmt.Errorf("count top remotes: %w", err)
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
	case "remote_ip":
		sortExpr = "remote_ip"
	case "direction":
		sortExpr = "direction"
	}
	order := normalizeSortOrder(sortOrder)

	querySQL := fmt.Sprintf(`
SELECT direction, remote_ip, COALESCE(SUM(bytes_up), 0), COALESCE(SUM(bytes_down), 0), COALESCE(SUM(flow_count), 0)
%s
GROUP BY direction, remote_ip
ORDER BY %s %s, remote_ip ASC
LIMIT ? OFFSET ?
`, filterBuilder.String(), sortExpr, order)
	queryArgs := append(append([]any{}, args...), pageSize, pageOffset)
	rows, err := s.db.QueryContext(ctx, querySQL, queryArgs...)
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
	return entries, totalRows, rows.Err()
}

func (s *Store) QueryTopPorts(ctx context.Context, start, end time.Time, source string, orderBy string) ([]model.TopEntry, error) {
	return s.queryTop(ctx, start, end, source, "CAST(local_port AS TEXT)", orderBy)
}

func (s *Store) queryTop(ctx context.Context, start, end time.Time, source string, groupExpr string, orderBy string) ([]model.TopEntry, error) {
	info := usageSourceInfo(source)
	sortExpr := "SUM(bytes_up + bytes_down)"
	switch orderBy {
	case "bytes_up":
		sortExpr = "SUM(bytes_up)"
	case "bytes_down":
		sortExpr = "SUM(bytes_down)"
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
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
		totalRows, err := countRows(ctx, s.db, countBuilder.String(), countArgs, "forward usage")
		if err != nil {
			return nil, "", 0, err
		}
		appendOffsetPagination(&builder, &args, forwardOrderClause(query.SortBy, query.SortOrder, info.TimeCol), query.Page, query.PageSize)

		rows, err := s.db.QueryContext(ctx, builder.String(), args...)
		if err != nil {
			return nil, "", 0, fmt.Errorf("query forward usage: %w", err)
		}
		defer rows.Close()

		records, err := scanForwardRows(rows, info.DataLabel)
		if err != nil {
			return nil, "", 0, err
		}
		return records, "", totalRows, nil
	}
	limit := appendCursorPagination(&builder, &args, info.TimeCol, query.CursorTS, query.CursorRowID, query.Limit)

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
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

func appendExeFilter(builder *strings.Builder, args *[]any, exe string) {
	trimmed := strings.TrimSpace(exe)
	if trimmed == "" {
		return
	}
	builder.WriteString(" AND (exe = ? OR exe LIKE ? OR exe LIKE ?)")
	*args = append(*args, trimmed, "%/"+trimmed, "%\\"+trimmed)
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
