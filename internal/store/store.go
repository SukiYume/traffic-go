package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
)

const (
	DataSourceMinute = "usage_1m"
	DataSourceHour   = "usage_1h"
	DataSourceDay    = "usage_1d"

	DataSourceMinuteForward = "usage_1m_forward"
	DataSourceHourForward   = "usage_1h_forward"
	DataSourceDayForward    = "usage_1d_forward"
	DataSourceMinuteChain   = "usage_chain_1m"
	DataSourceHourChain     = "usage_chain_1h"
)

var ErrDimensionUnavailable = errors.New("dimension_unavailable")
var ErrCursorSortUnsupported = errors.New("cursor pagination only supports time-desc sort")

const hourlyQueryMinWindow = 24 * time.Hour
const dailyQueryMinWindow = 14 * 24 * time.Hour
const storeShortCacheTTL = 30 * time.Second

type Store struct {
	db        *sql.DB
	readDB    *sql.DB
	dbPath    string
	retention config.Retention
	now       func() time.Time

	cacheMu           sync.RWMutex
	monthlyCache      []model.MonthlyUsageSummary
	monthlyCacheUntil time.Time
	processCache      map[int]cachedProcessList
}

type cachedProcessList struct {
	items     []model.ProcessListItem
	expiresAt time.Time
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

	db, err := openSQLiteDB(cfg.DBPath, 1, false)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	store := &Store{
		db:        db,
		dbPath:    cfg.DBPath,
		retention: cfg.Retention,
		now:       time.Now,
	}
	if err := store.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	readDB, err := openSQLiteDB(cfg.DBPath, 4, true)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite read pool: %w", err)
	}
	store.readDB = readDB
	return store, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var firstErr error
	if s.readDB != nil {
		if err := s.readDB.Close(); err != nil {
			firstErr = err
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func openSQLiteDB(path string, maxOpen int, queryOnly bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path, queryOnly))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	return db, nil
}

func sqliteDSN(path string, queryOnly bool) string {
	values := url.Values{}
	values.Add("_pragma", "busy_timeout=5000")
	values.Add("_pragma", "journal_mode(WAL)")
	values.Add("_pragma", "synchronous(NORMAL)")
	if queryOnly {
		values.Add("_pragma", "query_only(1)")
	}
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + values.Encode()
}

func (s *Store) queryDB() *sql.DB {
	if s != nil && s.readDB != nil {
		return s.readDB
	}
	if s == nil {
		return nil
	}
	return s.db
}

func (s *Store) invalidateCaches() {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.monthlyCache = nil
	s.monthlyCacheUntil = time.Time{}
	s.processCache = nil
}

func cloneMonthlySummaries(values []model.MonthlyUsageSummary) []model.MonthlyUsageSummary {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]model.MonthlyUsageSummary, len(values))
	copy(cloned, values)
	return cloned
}

func cloneProcessItems(values []model.ProcessListItem) []model.ProcessListItem {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]model.ProcessListItem, len(values))
	copy(cloned, values)
	return cloned
}

func (s *Store) Diagnostics(ctx context.Context) (model.StoreDiagnostics, error) {
	diag := model.StoreDiagnostics{
		DBBytes:  fileSizeOrZero(s.dbPath),
		WALBytes: fileSizeOrZero(s.dbPath + "-wal"),
		SHMBytes: fileSizeOrZero(s.dbPath + "-shm"),
	}
	if s.db != nil {
		diag.WritePool = poolDiagnostics(s.db.Stats())
	}
	if s.readDB != nil {
		diag.ReadPool = poolDiagnostics(s.readDB.Stats())
	}

	tableNames := []string{
		DataSourceMinute,
		DataSourceHour,
		DataSourceDay,
		DataSourceMinuteForward,
		DataSourceHourForward,
		DataSourceDayForward,
		DataSourceMinuteChain,
		DataSourceHourChain,
		"usage_monthly",
		"log_evidence",
		"dirty_chain_hours",
	}
	db := s.queryDB()
	diag.Tables = make([]model.StoreTableDiagnostics, 0, len(tableNames))
	for _, table := range tableNames {
		var count int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
			return diag, fmt.Errorf("diagnostics count %s: %w", table, err)
		}
		diag.Tables = append(diag.Tables, model.StoreTableDiagnostics{Name: table, Rows: count})
	}

	if value, ok, err := s.LastAggregatedHour(ctx); err != nil {
		return diag, err
	} else if ok {
		unix := value.Unix()
		diag.LastAggregatedHourTS = &unix
	}
	if value, ok, err := s.LastVacuum(ctx); err != nil {
		return diag, err
	} else if ok {
		unix := value.Unix()
		diag.LastVacuumTS = &unix
	}
	return diag, nil
}

func fileSizeOrZero(path string) int64 {
	if strings.TrimSpace(path) == "" {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func poolDiagnostics(stats sql.DBStats) model.StorePoolDiagnostics {
	return model.StorePoolDiagnostics{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:              stats.InUse,
		Idle:               stats.Idle,
		WaitCount:          stats.WaitCount,
		WaitDurationMS:     stats.WaitDuration.Milliseconds(),
	}
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
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

	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateCaches()
	return nil
}

func (s *Store) AggregateHour(ctx context.Context, hour time.Time) error {
	hourTS := hour.Truncate(time.Hour).Unix()
	nextHourTS := hour.Truncate(time.Hour).Add(time.Hour).Unix()
	dayTS := (hourTS / 86400) * 86400
	nextDayTS := dayTS + 86400

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

	if _, err := tx.ExecContext(ctx, `
INSERT INTO usage_1d (
    day_ts, proto, direction, comm, local_port, remote_ip,
    bytes_up, bytes_down, pkts_up, pkts_down, flow_count
)
SELECT ?, proto, direction, comm, local_port, remote_ip,
       SUM(bytes_up), SUM(bytes_down), SUM(pkts_up), SUM(pkts_down), SUM(flow_count)
FROM usage_1h
WHERE hour_ts >= ? AND hour_ts < ?
GROUP BY proto, direction, comm, local_port, remote_ip
ON CONFLICT(day_ts, proto, direction, comm, local_port, remote_ip) DO UPDATE SET
    bytes_up = excluded.bytes_up,
    bytes_down = excluded.bytes_down,
    pkts_up = excluded.pkts_up,
    pkts_down = excluded.pkts_down,
    flow_count = excluded.flow_count
`, dayTS, dayTS, nextDayTS); err != nil {
		return fmt.Errorf("aggregate usage_1d: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO usage_1d_forward (
    day_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
    bytes_orig, bytes_reply, pkts_orig, pkts_reply, flow_count
)
SELECT ?, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport,
       SUM(bytes_orig), SUM(bytes_reply), SUM(pkts_orig), SUM(pkts_reply), SUM(flow_count)
FROM usage_1h_forward
WHERE hour_ts >= ? AND hour_ts < ?
GROUP BY proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport
ON CONFLICT(day_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport) DO UPDATE SET
    bytes_orig = excluded.bytes_orig,
    bytes_reply = excluded.bytes_reply,
    pkts_orig = excluded.pkts_orig,
    pkts_reply = excluded.pkts_reply,
    flow_count = excluded.flow_count
`, dayTS, dayTS, nextDayTS); err != nil {
		return fmt.Errorf("aggregate usage_1d_forward: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM dirty_chain_hours WHERE hour_ts = ?`, hourTS); err != nil {
		return fmt.Errorf("clear dirty chain hour: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateCaches()
	return nil
}

func (s *Store) Cleanup(ctx context.Context) error {
	now := s.now().UTC()
	cutoff := retentionStartUTC(now, s.retention).Unix()
	closedMonthCutoff := monthStartUTC(now).Unix()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cleanup: %w", err)
	}
	defer tx.Rollback()

	if err := s.summarizeClosedMonths(ctx, tx, closedMonthCutoff, now); err != nil {
		return err
	}
	if err := s.summarizeCleanupMonthsIfMissing(ctx, tx, cutoff, now); err != nil {
		return err
	}

	statements := []struct {
		query string
		arg   int64
	}{
		{`DELETE FROM usage_1m WHERE minute_ts < ?`, cutoff},
		{`DELETE FROM usage_1h WHERE hour_ts < ?`, cutoff},
		{`DELETE FROM usage_1d WHERE day_ts < ?`, cutoff},
		{`DELETE FROM usage_1m_forward WHERE minute_ts < ?`, cutoff},
		{`DELETE FROM usage_1h_forward WHERE hour_ts < ?`, cutoff},
		{`DELETE FROM usage_1d_forward WHERE day_ts < ?`, cutoff},
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

	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateCaches()
	return nil
}

func (s *Store) summarizeCleanupMonthsIfMissing(ctx context.Context, tx *sql.Tx, cutoff int64, now time.Time) error {
	missingMonths, err := s.missingCleanupSummaryMonths(ctx, tx, cutoff)
	if err != nil {
		return err
	}
	if len(missingMonths) == 0 {
		return nil
	}

	if err := s.summarizeClosedMonths(ctx, tx, cutoff, now); err != nil {
		return fmt.Errorf("summarize missing cleanup months: %w", err)
	}
	missingMonths, err = s.missingCleanupSummaryMonths(ctx, tx, cutoff)
	if err != nil {
		return err
	}
	if len(missingMonths) > 0 {
		labels := make([]string, 0, len(missingMonths))
		for _, monthTS := range missingMonths {
			labels = append(labels, time.Unix(monthTS, 0).UTC().Format("2006-01"))
		}
		return fmt.Errorf("monthly summary missing before cleanup after fallback for months: %s", strings.Join(labels, ", "))
	}
	return nil
}

func (s *Store) missingCleanupSummaryMonths(ctx context.Context, tx *sql.Tx, cutoff int64) ([]int64, error) {
	query := monthlyDetailAggregateCTE(monthlyDetailBeforeExclusive) + `
SELECT month_ts
FROM monthly_detail
WHERE month_ts NOT IN (SELECT month_ts FROM usage_monthly)
ORDER BY month_ts ASC
`
	rows, err := tx.QueryContext(ctx, query, monthlyDetailAggregateArgs(cutoff)...)
	if err != nil {
		return nil, fmt.Errorf("verify monthly summaries before cleanup: %w", err)
	}
	defer rows.Close()

	missingMonths := make([]int64, 0)
	for rows.Next() {
		var monthTS int64
		if err := rows.Scan(&monthTS); err != nil {
			return nil, fmt.Errorf("scan missing monthly summaries: %w", err)
		}
		missingMonths = append(missingMonths, monthTS)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate missing monthly summaries: %w", err)
	}
	return missingMonths, nil
}

func (s *Store) summarizeClosedMonths(ctx context.Context, tx *sql.Tx, before int64, now time.Time) error {
	updatedAt := now.UTC().Unix()
	query := monthlyDetailAggregateCTE(monthlyDetailBeforeExclusive) + `
INSERT OR REPLACE INTO usage_monthly (
    month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
    forward_flow_count, evidence_count, chain_count, updated_at
)
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, ?
FROM monthly_detail
`
	args := append(monthlyDetailAggregateArgs(before), updatedAt)
	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("summarize closed months: %w", err)
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

func (s *Store) Optimize(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA optimize`); err != nil {
		return fmt.Errorf("optimize: %w", err)
	}
	return nil
}

func (s *Store) ResolveUsageSource(start, end time.Time, requiresMinute bool) (string, error) {
	if end.Before(start) {
		return "", fmt.Errorf("end before start")
	}
	if start.UTC().Before(retentionStartUTC(s.now(), s.retention)) {
		if requiresMinute {
			return "", ErrDimensionUnavailable
		}
		return DataSourceHour, nil
	}
	if requiresMinute {
		return DataSourceMinute, nil
	}
	if end.Sub(start) > dailyQueryMinWindow {
		return DataSourceDay, nil
	}
	if end.Sub(start) > hourlyQueryMinWindow {
		return DataSourceHour, nil
	}
	return DataSourceMinute, nil
}

func ForwardDataSource(source string) string {
	switch source {
	case DataSourceDay:
		return DataSourceDayForward
	case DataSourceHour:
		return DataSourceHourForward
	default:
		return DataSourceMinuteForward
	}
}

func (s *Store) QueryKnownProcesses(ctx context.Context, limit int) ([]model.ProcessListItem, error) {
	resolvedLimit := clampPageSize(limit)
	now := s.now().UTC()
	s.cacheMu.RLock()
	if cached, ok := s.processCache[resolvedLimit]; ok && cached.expiresAt.After(now) {
		items := cloneProcessItems(cached.items)
		s.cacheMu.RUnlock()
		return items, nil
	}
	s.cacheMu.RUnlock()

	db := s.queryDB()
	rows, err := db.QueryContext(ctx, `
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
	s.cacheMu.Lock()
	if s.processCache == nil {
		s.processCache = make(map[int]cachedProcessList)
	}
	s.processCache[resolvedLimit] = cachedProcessList{
		items:     cloneProcessItems(processes),
		expiresAt: now.Add(storeShortCacheTTL),
	}
	s.cacheMu.Unlock()
	return processes, nil
}
