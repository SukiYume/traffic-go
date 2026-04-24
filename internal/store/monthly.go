package store

import "strings"

type monthlyDetailBound string

const (
	monthlyDetailBeforeExclusive monthlyDetailBound = "<"
	monthlyDetailFromInclusive   monthlyDetailBound = ">="
	monthlyDetailBoundArgCount                      = 10
)

const monthlyDetailAggregateCTETemplate = `
WITH minute_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_1m
    WHERE minute_ts {{BOUND}} ?
),
forward_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_1m_forward
    WHERE minute_ts {{BOUND}} ?
),
chain_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_chain_1m
    WHERE minute_ts {{BOUND}} ?
),
monthly_detail_raw AS (
    SELECT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           COALESCE(SUM(bytes_up), 0) AS bytes_up,
           COALESCE(SUM(bytes_down), 0) AS bytes_down,
           COALESCE(SUM(flow_count), 0) AS flow_count,
           0 AS forward_bytes_orig,
           0 AS forward_bytes_reply,
           0 AS forward_flow_count,
           0 AS evidence_count,
           0 AS chain_count
    FROM usage_1m
    WHERE minute_ts {{BOUND}} ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           COALESCE(SUM(bytes_up), 0),
           COALESCE(SUM(bytes_down), 0),
           COALESCE(SUM(flow_count), 0),
           0, 0, 0, 0, 0
    FROM usage_1h
    WHERE hour_ts {{BOUND}} ?
      AND CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) NOT IN (
          SELECT month_ts FROM minute_detail_months
      )
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0,
           COALESCE(SUM(bytes_orig), 0),
           COALESCE(SUM(bytes_reply), 0),
           COALESCE(SUM(flow_count), 0),
           0,
           0
    FROM usage_1m_forward
    WHERE minute_ts {{BOUND}} ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0,
           COALESCE(SUM(bytes_orig), 0),
           COALESCE(SUM(bytes_reply), 0),
           COALESCE(SUM(flow_count), 0),
           0,
           0
    FROM usage_1h_forward
    WHERE hour_ts {{BOUND}} ?
      AND CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) NOT IN (
          SELECT month_ts FROM forward_detail_months
      )
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(event_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0, 0, 0, 0,
           COUNT(*),
           0
    FROM log_evidence
    WHERE event_ts {{BOUND}} ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0, 0, 0, 0, 0,
           COUNT(*)
    FROM usage_chain_1m
    WHERE minute_ts {{BOUND}} ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0, 0, 0, 0, 0,
           COUNT(*)
    FROM usage_chain_1h
    WHERE hour_ts {{BOUND}} ?
      AND CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) NOT IN (
          SELECT month_ts FROM chain_detail_months
      )
    GROUP BY month_ts
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
)`

func monthlyDetailAggregateCTE(bound monthlyDetailBound) string {
	switch bound {
	case monthlyDetailBeforeExclusive, monthlyDetailFromInclusive:
	default:
		panic("unsupported monthly detail bound " + string(bound))
	}
	return strings.ReplaceAll(monthlyDetailAggregateCTETemplate, "{{BOUND}}", string(bound))
}

func monthlyDetailAggregateArgs(boundTS int64) []any {
	args := make([]any, monthlyDetailBoundArgCount)
	for i := range args {
		args[i] = boundTS
	}
	return args
}
