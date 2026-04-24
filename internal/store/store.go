package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "modernc.org/sqlite"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
)

const (
	DataSourceMinute = "usage_1m"
	DataSourceHour   = "usage_1h"

	DataSourceMinuteForward = "usage_1m_forward"
	DataSourceHourForward   = "usage_1h_forward"
	DataSourceMinuteChain   = "usage_chain_1m"
	DataSourceHourChain     = "usage_chain_1h"
)

var ErrDimensionUnavailable = errors.New("dimension_unavailable")
var ErrCursorSortUnsupported = errors.New("cursor pagination only supports time-desc sort")

type Store struct {
	db        *sql.DB
	retention config.Retention
	now       func() time.Time
}

func effectiveRetentionMonths(retention config.Retention) int {
	if retention.Months <= 0 {
		return 3
	}
	return retention.Months
}

func monthStartUTC(value time.Time) time.Time {
	year, month, _ := value.UTC().Date()
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
}

func retentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return monthStartUTC(now).AddDate(0, -(effectiveRetentionMonths(retention) - 1), 0)
}

func Open(cfg config.Config) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil && filepath.Dir(cfg.DBPath) != "." {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Keep the application on a single SQLite connection so WAL/busy-timeout
	// settings stay consistent across HTTP queries, collector flushes, and
	// background maintenance jobs.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{
		db:        db,
		retention: cfg.Retention,
		now:       time.Now,
	}
	if err := store.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := s.ensureLogEvidenceColumns(ctx); err != nil {
		return err
	}
	if err := s.pruneZeroUsageRows(ctx); err != nil {
		return err
	}
	if err := s.seedDirtyChainHours(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureLogEvidenceColumns(ctx context.Context) error {
	type columnSpec struct {
		name string
		sql  string
	}

	columns := []columnSpec{
		{name: "host_normalized", sql: `ALTER TABLE log_evidence ADD COLUMN host_normalized TEXT NOT NULL DEFAULT ''`},
		{name: "entry_port", sql: `ALTER TABLE log_evidence ADD COLUMN entry_port INTEGER NOT NULL DEFAULT 0`},
		{name: "target_port", sql: `ALTER TABLE log_evidence ADD COLUMN target_port INTEGER NOT NULL DEFAULT 0`},
	}
	for _, column := range columns {
		exists, err := s.tableHasColumn(ctx, "log_evidence", column.name)
		if err != nil {
			return fmt.Errorf("inspect log_evidence.%s: %w", column.name, err)
		}
		if exists {
			continue
		}
		if _, err := s.db.ExecContext(ctx, column.sql); err != nil {
			return fmt.Errorf("add log_evidence.%s: %w", column.name, err)
		}
	}

	statements := []string{
		`UPDATE log_evidence SET host_normalized = lower(trim(host)) WHERE host_normalized = '' AND host <> ''`,
		`UPDATE log_evidence SET entry_port = 0 WHERE entry_port IS NULL`,
		`UPDATE log_evidence SET target_port = CAST(path AS INTEGER) WHERE target_port = 0 AND path GLOB '[0-9]*'`,
		`CREATE INDEX IF NOT EXISTS idx_log_evidence_client_lookup ON log_evidence (source, event_ts, client_ip)`,
		`CREATE INDEX IF NOT EXISTS idx_log_evidence_target_lookup ON log_evidence (source, event_ts, target_ip)`,
		`CREATE INDEX IF NOT EXISTS idx_log_evidence_entry_port ON log_evidence (source, event_ts, entry_port)`,
		`CREATE INDEX IF NOT EXISTS idx_log_evidence_host_port ON log_evidence (source, event_ts, host_normalized, target_port)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate log_evidence: %w", err)
		}
	}
	return nil
}

func (s *Store) pruneZeroUsageRows(ctx context.Context) error {
	statements := []string{
		`DELETE FROM usage_1m WHERE bytes_up = 0 AND bytes_down = 0 AND pkts_up = 0 AND pkts_down = 0 AND flow_count = 0`,
		`DELETE FROM usage_1h WHERE bytes_up = 0 AND bytes_down = 0 AND pkts_up = 0 AND pkts_down = 0 AND flow_count = 0`,
		`DELETE FROM usage_1m_forward WHERE bytes_orig = 0 AND bytes_reply = 0 AND pkts_orig = 0 AND pkts_reply = 0 AND flow_count = 0`,
		`DELETE FROM usage_1h_forward WHERE bytes_orig = 0 AND bytes_reply = 0 AND pkts_orig = 0 AND pkts_reply = 0 AND flow_count = 0`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("prune zero-usage rows: %w", err)
		}
	}
	return nil
}

func (s *Store) seedDirtyChainHours(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO dirty_chain_hours (hour_ts)
SELECT DISTINCT (minute_ts / 3600) * 3600
FROM usage_chain_1m
WHERE minute_ts > 0
  AND ((minute_ts / 3600) * 3600) NOT IN (
      SELECT hour_ts FROM usage_chain_1h
  )
ON CONFLICT(hour_ts) DO NOTHING
`); err != nil {
		return fmt.Errorf("seed dirty chain hours: %w", err)
	}
	return nil
}

func (s *Store) tableHasColumn(ctx context.Context, table string, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			kind       string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Store) FlushMinute(
	ctx context.Context,
	minuteTS int64,
	usage map[model.UsageKey]model.UsageDelta,
	forward map[model.ForwardUsageKey]model.UsageDelta,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin flush minute: %w", err)
	}
	defer tx.Rollback()

	if len(usage) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
INSERT INTO usage_1m (
    minute_ts, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port,
    attribution, bytes_up, bytes_down, pkts_up, pkts_down, flow_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(minute_ts, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port, attribution)
DO UPDATE SET
    bytes_up = usage_1m.bytes_up + excluded.bytes_up,
    bytes_down = usage_1m.bytes_down + excluded.bytes_down,
    pkts_up = usage_1m.pkts_up + excluded.pkts_up,
    pkts_down = usage_1m.pkts_down + excluded.pkts_down,
    flow_count = usage_1m.flow_count + excluded.flow_count`)
		if err != nil {
			return fmt.Errorf("prepare minute flush: %w", err)
		}
		defer stmt.Close()

		for key, delta := range usage {
			if key.MinuteTS == 0 {
				key.MinuteTS = minuteTS
			}
			if _, err := stmt.ExecContext(
				ctx,
				key.MinuteTS,
				key.Proto,
				key.Direction,
				key.PID,
				key.Comm,
				key.Exe,
				key.LocalPort,
				key.RemoteIP,
				key.RemotePort,
				key.Attribution,
				delta.BytesUp,
				delta.BytesDown,
				delta.PktsUp,
				delta.PktsDown,
				delta.FlowCount,
			); err != nil {
				return fmt.Errorf("flush minute usage: %w", err)
			}
		}
	}

	if len(forward) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
INSERT INTO usage_1m_forward (
    minute_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
    bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(minute_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport)
DO UPDATE SET
    bytes_orig = usage_1m_forward.bytes_orig + excluded.bytes_orig,
    bytes_reply = usage_1m_forward.bytes_reply + excluded.bytes_reply,
    pkts_orig = usage_1m_forward.pkts_orig + excluded.pkts_orig,
    pkts_reply = usage_1m_forward.pkts_reply + excluded.pkts_reply,
    flow_count = usage_1m_forward.flow_count + excluded.flow_count`)
		if err != nil {
			return fmt.Errorf("prepare forward minute flush: %w", err)
		}
		defer stmt.Close()

		for key, delta := range forward {
			if key.MinuteTS == 0 {
				key.MinuteTS = minuteTS
			}
			if _, err := stmt.ExecContext(
				ctx,
				key.MinuteTS,
				key.Proto,
				key.OrigSrcIP,
				key.OrigDstIP,
				key.OrigSPort,
				key.OrigDPort,
				delta.BytesUp,
				delta.BytesDown,
				delta.PktsUp,
				delta.PktsDown,
				delta.FlowCount,
			); err != nil {
				return fmt.Errorf("flush forward minute usage: %w", err)
			}
		}
	}

	return tx.Commit()
}

func (s *Store) AggregateHour(ctx context.Context, hour time.Time) error {
	hourTS := hour.Truncate(time.Hour).Unix()
	nextHourTS := hour.Truncate(time.Hour).Add(time.Hour).Unix()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin aggregate hour: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO usage_1h (
    hour_ts, proto, direction, comm, local_port, remote_ip,
    bytes_up, bytes_down, pkts_up, pkts_down, flow_count
)
SELECT ?, proto, direction, comm, local_port, remote_ip,
       SUM(bytes_up), SUM(bytes_down), SUM(pkts_up), SUM(pkts_down), SUM(flow_count)
FROM usage_1m
WHERE minute_ts >= ? AND minute_ts < ?
GROUP BY proto, direction, comm, local_port, remote_ip
ON CONFLICT(hour_ts, proto, direction, comm, local_port, remote_ip) DO UPDATE SET
    bytes_up = excluded.bytes_up,
    bytes_down = excluded.bytes_down,
    pkts_up = excluded.pkts_up,
    pkts_down = excluded.pkts_down,
    flow_count = excluded.flow_count
`, hourTS, hourTS, nextHourTS); err != nil {
		return fmt.Errorf("aggregate usage_1h: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
WITH aggregated AS (
    SELECT pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized, target_port,
           SUM(bytes_total) AS bytes_total,
           SUM(flow_count) AS flow_count,
           SUM(evidence_count) AS evidence_count,
           MAX(confidence_rank) AS confidence_rank
    FROM usage_chain_1m
    WHERE minute_ts >= ? AND minute_ts < ?
    GROUP BY pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized, target_port
),
latest_samples AS (
    SELECT pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized, target_port,
           evidence_source, sample_fingerprint, sample_message, sample_time,
           ROW_NUMBER() OVER (
               PARTITION BY pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized, target_port
               ORDER BY sample_time DESC, rowid DESC
           ) AS rn
    FROM usage_chain_1m
    WHERE minute_ts >= ? AND minute_ts < ?
)
INSERT INTO usage_chain_1h (
    hour_ts, chain_id, pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized,
    target_port, bytes_total, flow_count, evidence_count, evidence_source, confidence, confidence_rank,
    sample_fingerprint, sample_message, sample_time
)
SELECT ?, 
       (? || '|' || ? || '|' || aggregated.pid || '|' || aggregated.comm || '|' || aggregated.exe || '|' || aggregated.source_ip || '|' || aggregated.entry_port || '|' || aggregated.target_ip || '|' || aggregated.target_host_normalized || '|' || aggregated.target_port),
       aggregated.pid, aggregated.comm, aggregated.exe, aggregated.source_ip, aggregated.entry_port, aggregated.target_ip, aggregated.target_host, aggregated.target_host_normalized,
       aggregated.target_port, aggregated.bytes_total, aggregated.flow_count, aggregated.evidence_count,
       COALESCE(latest_samples.evidence_source, ''),
       CASE aggregated.confidence_rank WHEN 3 THEN 'high' WHEN 2 THEN 'medium' ELSE 'low' END,
       aggregated.confidence_rank,
       COALESCE(latest_samples.sample_fingerprint, ''),
       COALESCE(latest_samples.sample_message, ''),
       COALESCE(latest_samples.sample_time, 0)
FROM aggregated
LEFT JOIN latest_samples
    ON latest_samples.rn = 1
   AND latest_samples.pid = aggregated.pid
   AND latest_samples.comm = aggregated.comm
   AND latest_samples.exe = aggregated.exe
   AND latest_samples.source_ip = aggregated.source_ip
   AND latest_samples.entry_port = aggregated.entry_port
   AND latest_samples.target_ip = aggregated.target_ip
   AND latest_samples.target_host = aggregated.target_host
   AND latest_samples.target_host_normalized = aggregated.target_host_normalized
   AND latest_samples.target_port = aggregated.target_port
ON CONFLICT(chain_id) DO UPDATE SET
    bytes_total = excluded.bytes_total,
    flow_count = excluded.flow_count,
    evidence_count = excluded.evidence_count,
    evidence_source = excluded.evidence_source,
    confidence = excluded.confidence,
    confidence_rank = excluded.confidence_rank,
    sample_fingerprint = excluded.sample_fingerprint,
    sample_message = excluded.sample_message,
    sample_time = excluded.sample_time
`, hourTS, nextHourTS, hourTS, nextHourTS, hourTS, DataSourceHourChain, hourTS); err != nil {
		return fmt.Errorf("aggregate usage_chain_1h: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO usage_1h_forward (
    hour_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
    bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
)
SELECT ?, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
       SUM(bytes_orig), SUM(bytes_reply), SUM(pkts_orig), SUM(pkts_reply), SUM(flow_count)
FROM usage_1m_forward
WHERE minute_ts >= ? AND minute_ts < ?
GROUP BY proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport
ON CONFLICT(hour_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport) DO UPDATE SET
    bytes_orig = excluded.bytes_orig,
    bytes_reply = excluded.bytes_reply,
    pkts_orig = excluded.pkts_orig,
    pkts_reply = excluded.pkts_reply,
    flow_count = excluded.flow_count
`, hourTS, hourTS, nextHourTS); err != nil {
		return fmt.Errorf("aggregate usage_1h_forward: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM dirty_chain_hours WHERE hour_ts = ?`, hourTS); err != nil {
		return fmt.Errorf("clear dirty chain hour: %w", err)
	}

	return tx.Commit()
}

func (s *Store) Cleanup(ctx context.Context) error {
	cutoff := retentionStartUTC(s.now(), s.retention).Unix()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cleanup: %w", err)
	}
	defer tx.Rollback()

	if err := s.summarizeExpiredMonths(ctx, tx, cutoff); err != nil {
		return err
	}

	statements := []struct {
		query string
		arg   int64
	}{
		{`DELETE FROM usage_1m WHERE minute_ts < ?`, cutoff},
		{`DELETE FROM usage_1h WHERE hour_ts < ?`, cutoff},
		{`DELETE FROM usage_1m_forward WHERE minute_ts < ?`, cutoff},
		{`DELETE FROM usage_1h_forward WHERE hour_ts < ?`, cutoff},
		{`DELETE FROM usage_chain_1m WHERE minute_ts < ?`, cutoff},
		{`DELETE FROM usage_chain_1h WHERE hour_ts < ?`, cutoff},
		{`DELETE FROM log_evidence WHERE event_ts < ?`, cutoff},
		{`DELETE FROM dirty_chain_hours WHERE hour_ts < ?`, cutoff},
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt.query, stmt.arg); err != nil {
			return fmt.Errorf("cleanup query failed: %w", err)
		}
	}

	return tx.Commit()
}

func (s *Store) summarizeExpiredMonths(ctx context.Context, tx *sql.Tx, cutoff int64) error {
	updatedAt := s.now().UTC().Unix()
	_, err := tx.ExecContext(ctx, `
WITH minute_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_1m
    WHERE minute_ts < ?
),
forward_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_1m_forward
    WHERE minute_ts < ?
),
chain_detail_months AS (
    SELECT DISTINCT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts
    FROM usage_chain_1m
    WHERE minute_ts < ?
),
raw_months AS (
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
    WHERE minute_ts < ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           COALESCE(SUM(bytes_up), 0),
           COALESCE(SUM(bytes_down), 0),
           COALESCE(SUM(flow_count), 0),
           0, 0, 0, 0, 0
    FROM usage_1h
    WHERE hour_ts < ?
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
    WHERE minute_ts < ?
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
    WHERE hour_ts < ?
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
    WHERE event_ts < ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(minute_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0, 0, 0, 0, 0,
           COUNT(*)
    FROM usage_chain_1m
    WHERE minute_ts < ?
    GROUP BY month_ts
    UNION ALL
    SELECT CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) AS month_ts,
           0, 0, 0, 0, 0, 0, 0,
           COUNT(*)
    FROM usage_chain_1h
    WHERE hour_ts < ?
      AND CAST(strftime('%s', datetime(hour_ts, 'unixepoch', 'start of month')) AS INTEGER) NOT IN (
          SELECT month_ts FROM chain_detail_months
      )
    GROUP BY month_ts
),
monthly AS (
    SELECT month_ts,
           COALESCE(SUM(bytes_up), 0) AS bytes_up,
           COALESCE(SUM(bytes_down), 0) AS bytes_down,
           COALESCE(SUM(flow_count), 0) AS flow_count,
           COALESCE(SUM(forward_bytes_orig), 0) AS forward_bytes_orig,
           COALESCE(SUM(forward_bytes_reply), 0) AS forward_bytes_reply,
           COALESCE(SUM(forward_flow_count), 0) AS forward_flow_count,
           COALESCE(SUM(evidence_count), 0) AS evidence_count,
           COALESCE(SUM(chain_count), 0) AS chain_count
    FROM raw_months
    WHERE month_ts IS NOT NULL
    GROUP BY month_ts
)
INSERT OR REPLACE INTO usage_monthly (
    month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
    forward_flow_count, evidence_count, chain_count, updated_at
)
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, ?
FROM monthly
`, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, updatedAt)
	if err != nil {
		return fmt.Errorf("summarize expired months: %w", err)
	}
	return nil
}

func (s *Store) LastAggregatedHour(ctx context.Context) (time.Time, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'last_aggregated_hour'`)
	var value string
	if err := row.Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	unixValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(unixValue, 0).UTC(), true, nil
}

func (s *Store) SetLastAggregatedHour(ctx context.Context, hour time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO meta (key, value) VALUES ('last_aggregated_hour', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, strconv.FormatInt(hour.UTC().Truncate(time.Hour).Unix(), 10))
	return err
}

func (s *Store) NextPendingAggregationHour(ctx context.Context, after *time.Time, before time.Time) (time.Time, bool, error) {
	minMinute := int64(0)
	if after != nil {
		minMinute = after.UTC().Add(time.Hour).Unix()
	}
	row := s.db.QueryRowContext(ctx, `
SELECT MIN(hour_ts)
FROM (
    SELECT (minute_ts / 3600) * 3600 AS hour_ts
    FROM usage_1m
    WHERE minute_ts >= ? AND minute_ts < ?
    UNION
    SELECT (minute_ts / 3600) * 3600 AS hour_ts
    FROM usage_1m_forward
    WHERE minute_ts >= ? AND minute_ts < ?
)
`, minMinute, before.UTC().Truncate(time.Hour).Unix(), minMinute, before.UTC().Truncate(time.Hour).Unix())

	var value sql.NullInt64
	if err := row.Scan(&value); err != nil {
		return time.Time{}, false, err
	}
	if !value.Valid {
		return time.Time{}, false, nil
	}
	return time.Unix(value.Int64, 0).UTC(), true, nil
}

func (s *Store) NextDirtyChainHour(ctx context.Context, before time.Time) (time.Time, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT MIN(hour_ts)
FROM dirty_chain_hours
WHERE hour_ts < ?
`, before.UTC().Truncate(time.Hour).Unix())

	var value sql.NullInt64
	if err := row.Scan(&value); err != nil {
		return time.Time{}, false, err
	}
	if !value.Valid {
		return time.Time{}, false, nil
	}
	return time.Unix(value.Int64, 0).UTC(), true, nil
}

func (s *Store) LastVacuum(ctx context.Context) (time.Time, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'last_vacuum_at'`)
	var value string
	if err := row.Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	unixValue, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(unixValue, 0).UTC(), true, nil
}

func (s *Store) SetLastVacuum(ctx context.Context, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO meta (key, value) VALUES ('last_vacuum_at', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, strconv.FormatInt(at.UTC().Unix(), 10))
	return err
}

func (s *Store) Vacuum(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	return nil
}

func (s *Store) ResolveUsageSource(start, end time.Time, pidFilter bool, exeFilter bool) (string, error) {
	if end.Before(start) {
		return "", fmt.Errorf("end before start")
	}
	if start.UTC().Before(retentionStartUTC(s.now(), s.retention)) {
		if pidFilter || exeFilter {
			return "", ErrDimensionUnavailable
		}
		return DataSourceHour, nil
	}
	return DataSourceMinute, nil
}

func ForwardDataSource(source string) string {
	switch source {
	case DataSourceHour:
		return DataSourceHourForward
	default:
		return DataSourceMinuteForward
	}
}

func (s *Store) QueryKnownProcesses(ctx context.Context, limit int) ([]model.ProcessListItem, error) {
	resolvedLimit := clampPageSize(limit)
	rows, err := s.db.QueryContext(ctx, `
WITH minute_processes AS (
    SELECT pid, comm, exe, MAX(minute_ts) AS seen_ts
    FROM usage_1m
    WHERE pid > 0 OR comm <> '' OR exe <> ''
    GROUP BY pid, comm, exe
),
hour_processes AS (
    SELECT 0 AS pid, comm, '' AS exe, MAX(hour_ts) AS seen_ts
    FROM usage_1h
    WHERE comm <> ''
    GROUP BY comm
),
combined AS (
    SELECT pid, comm, exe, seen_ts FROM minute_processes
    UNION ALL
    SELECT pid, comm, exe, seen_ts FROM hour_processes
),
deduped AS (
    SELECT pid, comm, exe, MAX(seen_ts) AS seen_ts
    FROM combined
    GROUP BY pid, comm, exe
)
SELECT pid, comm, exe
FROM deduped
ORDER BY seen_ts DESC, comm COLLATE NOCASE ASC, pid DESC
LIMIT ?
`, resolvedLimit)
	if err != nil {
		return nil, fmt.Errorf("query known processes: %w", err)
	}
	defer rows.Close()

	processes := make([]model.ProcessListItem, 0, resolvedLimit)
	for rows.Next() {
		var item model.ProcessListItem
		if err := rows.Scan(&item.PID, &item.Comm, &item.Exe); err != nil {
			return nil, fmt.Errorf("scan known processes: %w", err)
		}
		processes = append(processes, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate known processes: %w", err)
	}
	return processes, nil
}
