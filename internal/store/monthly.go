package store

import (
	"fmt"
	"strings"
)

type monthlyAggregateWindow struct {
	start          int64
	upper          *int64
	hourBoundary   int64
	minuteBoundary int64
	evidenceStart  int64
	chainBoundary  int64
}

type monthlyMetricKind int

const (
	monthlyMetricUsage monthlyMetricKind = iota
	monthlyMetricForward
)

type monthlyCountMetric int

const (
	monthlyCountEvidence monthlyCountMetric = iota
	monthlyCountChain
)

type monthlyWhere struct {
	sql  string
	args []any
}

func monthlyDetailAggregateCTE(window monthlyAggregateWindow) (string, []any) {
	parts := make([]string, 0, 10)
	args := make([]any, 0, 16)

	appendMonthlyTrafficPart(&parts, &args, monthlyMetricUsage, "usage_1d", "day_ts", window.start, minMonthlyEnd(window.upper, window.hourBoundary), nil)
	appendMonthlyTrafficPart(&parts, &args, monthlyMetricUsage, "usage_1h", "hour_ts", maxMonthlyStart(window.start, window.hourBoundary), minMonthlyEnd(window.upper, window.minuteBoundary), nil)
	appendMonthlyTrafficPart(&parts, &args, monthlyMetricUsage, "usage_1m", "minute_ts", maxMonthlyStart(window.start, window.minuteBoundary), window.upper, nil)

	appendMonthlyTrafficPart(&parts, &args, monthlyMetricForward, "usage_1d_forward", "day_ts", window.start, minMonthlyEnd(window.upper, window.hourBoundary), nil)
	appendMonthlyTrafficPart(&parts, &args, monthlyMetricForward, "usage_1h_forward", "hour_ts", maxMonthlyStart(window.start, window.hourBoundary), minMonthlyEnd(window.upper, window.minuteBoundary), nil)
	appendMonthlyTrafficPart(&parts, &args, monthlyMetricForward, "usage_1m_forward", "minute_ts", maxMonthlyStart(window.start, window.minuteBoundary), window.upper, nil)

	appendMonthlyCountPart(&parts, &args, monthlyCountEvidence, "log_evidence", "event_ts", window.evidenceStart, window.upper, nil)
	appendMonthlyCountPart(&parts, &args, monthlyCountChain, "usage_chain_1h", "hour_ts", window.start, minMonthlyEnd(window.upper, window.chainBoundary), nil)
	appendMonthlyCountPart(&parts, &args, monthlyCountChain, "usage_chain_1m", "minute_ts", maxMonthlyStart(window.start, window.chainBoundary), window.upper, nil)
	appendMonthlyRetainedCountsPart(&parts, &args, "monthly_retained_counts", "month_ts", window.start, window.upper, nil)

	if len(parts) == 0 {
		parts = append(parts, `
SELECT NULL AS month_ts, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count,
       0 AS forward_bytes_orig, 0 AS forward_bytes_reply, 0 AS forward_flow_count,
       0 AS evidence_count, 0 AS chain_count
WHERE 0`)
	}

	return `
WITH monthly_detail_raw AS (
` + strings.Join(parts, "\n    UNION ALL\n") + `
),
monthly_detail AS (
    SELECT month_ts,
           COALESCE(SUM(bytes_up), 0) AS bytes_up,
           COALESCE(SUM(bytes_down), 0) AS bytes_down,
           COALESCE(SUM(flow_count), 0) AS flow_count,
           COALESCE(SUM(forward_bytes_orig), 0) AS forward_bytes_orig,
           COALESCE(SUM(forward_bytes_reply), 0) AS forward_bytes_reply,
           COALESCE(SUM(forward_flow_count), 0) AS forward_flow_count,
           COALESCE(SUM(evidence_count), 0) AS evidence_count,
           COALESCE(SUM(chain_count), 0) AS chain_count
    FROM monthly_detail_raw
    WHERE month_ts IS NOT NULL
    GROUP BY month_ts
)`, args
}

func monthlySingleMonthAggregateCTE(window monthlyAggregateWindow, monthTS int64) (string, []any) {
	parts := make([]string, 0, 10)
	args := make([]any, 0, 24)

	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricUsage, "usage_1d", "day_ts", window.start, minMonthlyEnd(window.upper, window.hourBoundary), nil)
	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricUsage, "usage_1h", "hour_ts", maxMonthlyStart(window.start, window.hourBoundary), minMonthlyEnd(window.upper, window.minuteBoundary), nil)
	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricUsage, "usage_1m", "minute_ts", maxMonthlyStart(window.start, window.minuteBoundary), window.upper, nil)

	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricForward, "usage_1d_forward", "day_ts", window.start, minMonthlyEnd(window.upper, window.hourBoundary), nil)
	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricForward, "usage_1h_forward", "hour_ts", maxMonthlyStart(window.start, window.hourBoundary), minMonthlyEnd(window.upper, window.minuteBoundary), nil)
	appendSingleMonthTrafficPart(&parts, &args, monthTS, monthlyMetricForward, "usage_1m_forward", "minute_ts", maxMonthlyStart(window.start, window.minuteBoundary), window.upper, nil)

	appendSingleMonthCountPart(&parts, &args, monthTS, monthlyCountEvidence, "log_evidence", "event_ts", window.evidenceStart, window.upper, nil)
	appendSingleMonthCountPart(&parts, &args, monthTS, monthlyCountChain, "usage_chain_1h", "hour_ts", window.start, minMonthlyEnd(window.upper, window.chainBoundary), nil)
	appendSingleMonthCountPart(&parts, &args, monthTS, monthlyCountChain, "usage_chain_1m", "minute_ts", maxMonthlyStart(window.start, window.chainBoundary), window.upper, nil)
	appendSingleMonthRetainedCountsPart(&parts, &args, monthTS)

	if len(parts) == 0 {
		parts = append(parts, `
SELECT NULL AS month_ts, 0 AS bytes_up, 0 AS bytes_down, 0 AS flow_count,
       0 AS forward_bytes_orig, 0 AS forward_bytes_reply, 0 AS forward_flow_count,
       0 AS evidence_count, 0 AS chain_count
WHERE 0`)
	}

	return `
WITH monthly_detail_raw AS (
` + strings.Join(parts, "\n    UNION ALL\n") + `
),
monthly_detail AS (
    SELECT month_ts,
           COALESCE(SUM(bytes_up), 0) AS bytes_up,
           COALESCE(SUM(bytes_down), 0) AS bytes_down,
           COALESCE(SUM(flow_count), 0) AS flow_count,
           COALESCE(SUM(forward_bytes_orig), 0) AS forward_bytes_orig,
           COALESCE(SUM(forward_bytes_reply), 0) AS forward_bytes_reply,
           COALESCE(SUM(forward_flow_count), 0) AS forward_flow_count,
           COALESCE(SUM(evidence_count), 0) AS evidence_count,
           COALESCE(SUM(chain_count), 0) AS chain_count
    FROM monthly_detail_raw
    WHERE month_ts IS NOT NULL
    GROUP BY month_ts
)`, args
}

func appendMonthlyTrafficPart(parts *[]string, args *[]any, kind monthlyMetricKind, table string, timeCol string, start int64, upper *int64, extraWhere []monthlyWhere) {
	if !validMonthlyRange(start, upper) {
		return
	}

	metricSelect := monthlyTrafficMetricSelect(kind)
	whereSQL, whereArgs := monthlyRangeWhere("src."+timeCol, start, upper, extraWhere)
	*args = append(*args, whereArgs...)
	*parts = append(*parts, fmt.Sprintf(`
    SELECT %s AS month_ts,
           %s
    FROM %s src
    WHERE %s
    GROUP BY month_ts`, monthExpr("src."+timeCol), metricSelect, table, whereSQL))
}

func appendSingleMonthTrafficPart(parts *[]string, args *[]any, monthTS int64, kind monthlyMetricKind, table string, timeCol string, start int64, upper *int64, extraWhere []monthlyWhere) {
	if !validMonthlyRange(start, upper) {
		return
	}

	metricSelect := monthlyTrafficMetricSelect(kind)
	whereSQL, whereArgs := monthlyRangeWhere("src."+timeCol, start, upper, extraWhere)
	*args = append(*args, monthTS)
	*args = append(*args, whereArgs...)
	*parts = append(*parts, fmt.Sprintf(`
    SELECT ? AS month_ts,
           %s
    FROM %s src
    WHERE %s
    HAVING COUNT(*) > 0`, metricSelect, table, whereSQL))
}

func monthlyTrafficMetricSelect(kind monthlyMetricKind) string {
	switch kind {
	case monthlyMetricForward:
		return `0 AS bytes_up,
           0 AS bytes_down,
           0 AS flow_count,
           COALESCE(SUM(bytes_orig), 0) AS forward_bytes_orig,
           COALESCE(SUM(bytes_reply), 0) AS forward_bytes_reply,
           COALESCE(SUM(flow_count), 0) AS forward_flow_count,
           0 AS evidence_count,
           0 AS chain_count`
	default:
		return `COALESCE(SUM(bytes_up), 0) AS bytes_up,
           COALESCE(SUM(bytes_down), 0) AS bytes_down,
           COALESCE(SUM(flow_count), 0) AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           0 AS evidence_count,
           0 AS chain_count`
	}
}

func appendMonthlyCountPart(parts *[]string, args *[]any, metric monthlyCountMetric, table string, timeCol string, start int64, upper *int64, extraWhere []monthlyWhere) {
	if !validMonthlyRange(start, upper) {
		return
	}

	evidenceExpr, chainExpr := monthlyCountMetricSelect(metric)
	whereSQL, whereArgs := monthlyRangeWhere("src."+timeCol, start, upper, extraWhere)
	*args = append(*args, whereArgs...)
	*parts = append(*parts, fmt.Sprintf(`
    SELECT %s AS month_ts,
           0 AS bytes_up,
           0 AS bytes_down,
           0 AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           %s AS evidence_count,
           %s AS chain_count
    FROM %s src
    WHERE %s
    GROUP BY month_ts`, monthExpr("src."+timeCol), evidenceExpr, chainExpr, table, whereSQL))
}

func appendSingleMonthCountPart(parts *[]string, args *[]any, monthTS int64, metric monthlyCountMetric, table string, timeCol string, start int64, upper *int64, extraWhere []monthlyWhere) {
	if !validMonthlyRange(start, upper) {
		return
	}

	evidenceExpr, chainExpr := monthlyCountMetricSelect(metric)
	whereSQL, whereArgs := monthlyRangeWhere("src."+timeCol, start, upper, extraWhere)
	*args = append(*args, monthTS)
	*args = append(*args, whereArgs...)
	*parts = append(*parts, fmt.Sprintf(`
    SELECT ? AS month_ts,
           0 AS bytes_up,
           0 AS bytes_down,
           0 AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           %s AS evidence_count,
           %s AS chain_count
    FROM %s src
    WHERE %s
    HAVING COUNT(*) > 0`, evidenceExpr, chainExpr, table, whereSQL))
}

func monthlyCountMetricSelect(metric monthlyCountMetric) (string, string) {
	if metric == monthlyCountChain {
		return "0", "COUNT(*)"
	}
	return "COUNT(*)", "0"
}

func appendMonthlyRetainedCountsPart(parts *[]string, args *[]any, table string, timeCol string, start int64, upper *int64, extraWhere []monthlyWhere) {
	if !validMonthlyRange(start, upper) {
		return
	}

	whereSQL, whereArgs := monthlyRangeWhere("src."+timeCol, start, upper, extraWhere)
	*args = append(*args, whereArgs...)
	*parts = append(*parts, fmt.Sprintf(`
    SELECT src.month_ts AS month_ts,
           0 AS bytes_up,
           0 AS bytes_down,
           0 AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           COALESCE(SUM(src.evidence_count), 0) AS evidence_count,
           COALESCE(SUM(src.chain_count), 0) AS chain_count
    FROM %s src
    WHERE %s
    GROUP BY src.month_ts`, table, whereSQL))
}

func appendSingleMonthRetainedCountsPart(parts *[]string, args *[]any, monthTS int64) {
	*args = append(*args, monthTS, monthTS)
	*parts = append(*parts, `
    SELECT ? AS month_ts,
           0 AS bytes_up,
           0 AS bytes_down,
           0 AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           COALESCE(SUM(evidence_count), 0) AS evidence_count,
           COALESCE(SUM(chain_count), 0) AS chain_count
    FROM monthly_retained_counts
    WHERE month_ts = ?
    HAVING COUNT(*) > 0`)
}

func monthExpr(timeExpr string) string {
	return fmt.Sprintf("CAST(strftime('%%s', datetime(%s, 'unixepoch', 'start of month')) AS INTEGER)", timeExpr)
}

func monthlyRangeWhere(timeExpr string, start int64, upper *int64, extra []monthlyWhere) (string, []any) {
	clauses := []string{timeExpr + " >= ?"}
	args := []any{start}
	if upper != nil {
		clauses = append(clauses, timeExpr+" < ?")
		args = append(args, *upper)
	}
	for _, clause := range extra {
		clauses = append(clauses, clause.sql)
		args = append(args, clause.args...)
	}
	return strings.Join(clauses, "\n      AND "), args
}

func validMonthlyRange(start int64, upper *int64) bool {
	return upper == nil || start < *upper
}

func minMonthlyEnd(upper *int64, boundary int64) *int64 {
	if upper != nil && *upper < boundary {
		value := *upper
		return &value
	}
	value := boundary
	return &value
}

func maxMonthlyStart(start int64, boundary int64) int64 {
	if boundary > start {
		return boundary
	}
	return start
}
