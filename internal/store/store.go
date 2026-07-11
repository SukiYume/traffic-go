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

	DataSourceInterfaceMinute = "interface_1m"
	DataSourceInterfaceHour   = "interface_1h"
	DataSourceInterfaceDay    = "interface_1d"

	DataSourceMinuteForward = "usage_1m_forward"
	DataSourceHourForward   = "usage_1h_forward"
	DataSourceDayForward    = "usage_1d_forward"
	DataSourceMinuteChain   = "usage_chain_1m"
	DataSourceHourChain     = "usage_chain_1h"
)

var ErrDimensionUnavailable = errors.New("dimension_unavailable")
var ErrRangeUnavailable = errors.New("range_unavailable")
var ErrCursorSortUnsupported = errors.New("cursor pagination only supports time-desc sort")

const (
	storeShortCacheTTL    = 30 * time.Second
	knownProcessRetention = 90 * 24 * time.Hour
)

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

func effectiveMinuteRetentionDays(retention config.Retention) int {
	if retention.MinuteDays <= 0 {
		return 7
	}
	return retention.MinuteDays
}

func effectiveHourRetentionMonths(retention config.Retention) int {
	if retention.HourMonths <= 0 {
		return 3
	}
	return retention.HourMonths
}

func effectiveDayRetentionMonths(retention config.Retention) int {
	if retention.DayMonths <= 0 {
		return 12
	}
	return retention.DayMonths
}

func effectiveEvidenceRetentionDays(retention config.Retention) int {
	if retention.EvidenceDays <= 0 {
		return 14
	}
	return retention.EvidenceDays
}

func effectiveChainRetentionDays(retention config.Retention) int {
	if retention.ChainDays <= 0 {
		return 14
	}
	return retention.ChainDays
}

func monthStartUTC(value time.Time) time.Time {
	year, month, _ := value.UTC().Date()
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
}

func minuteRetentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return now.UTC().AddDate(0, 0, -effectiveMinuteRetentionDays(retention)).Truncate(time.Minute)
}

func hourRetentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return monthStartUTC(now).AddDate(0, -(effectiveHourRetentionMonths(retention) - 1), 0)
}

func dayRetentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return monthStartUTC(now).AddDate(0, -(effectiveDayRetentionMonths(retention) - 1), 0)
}

func evidenceRetentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return now.UTC().AddDate(0, 0, -effectiveEvidenceRetentionDays(retention)).Truncate(time.Minute)
}

func chainRetentionStartUTC(now time.Time, retention config.Retention) time.Time {
	return now.UTC().AddDate(0, 0, -effectiveChainRetentionDays(retention)).Truncate(time.Minute)
}

func ceilHourUTC(value time.Time) time.Time {
	truncated := value.UTC().Truncate(time.Hour)
	if value.UTC().Equal(truncated) {
		return truncated
	}
	return truncated.Add(time.Hour)
}

func monthlyMinuteBoundaryUTC(now time.Time, retention config.Retention) time.Time {
	return ceilHourUTC(minuteRetentionStartUTC(now, retention))
}

func monthlyChainBoundaryUTC(now time.Time, retention config.Retention) time.Time {
	return ceilHourUTC(chainRetentionStartUTC(now, retention))
}

func Open(cfg config.Config) (*Store, error) {
	if err := ensurePrivateDBDirectory(cfg.DBPath); err != nil {
		return nil, err
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
	if err := store.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := secureSQLiteFiles(cfg.DBPath); err != nil {
		_ = db.Close()
		return nil, err
	}

	readDB, err := openSQLiteDB(cfg.DBPath, 4, true)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite read pool: %w", err)
	}
	store.readDB = readDB
	if err := secureSQLiteFiles(cfg.DBPath); err != nil {
		_ = readDB.Close()
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func ensurePrivateDBDirectory(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	_, statErr := os.Stat(dir)
	if statErr == nil {
		return nil
	}
	if !os.IsNotExist(statErr) {
		return fmt.Errorf("inspect db directory: %w", statErr)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure db directory: %w", err)
	}
	return nil
}

func secureSQLiteFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(candidate); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect sqlite file %q: %w", candidate, err)
		}
		if err := os.Chmod(candidate, 0o600); err != nil {
			return fmt.Errorf("secure sqlite file %q: %w", candidate, err)
		}
	}
	return nil
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
	values.Add("_pragma", "journal_size_limit(67108864)")
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

func (s *Store) invalidateMonthlyCache() {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.monthlyCache = nil
	s.monthlyCacheUntil = time.Time{}
}

func (s *Store) invalidateProcessCache() {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.processCache = nil
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
		DataSourceInterfaceMinute,
		DataSourceInterfaceHour,
		DataSourceInterfaceDay,
		"usage_monthly",
		"monthly_retained_counts",
		"known_processes",
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

func (s *Store) ensureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, monthlyLateWriteTriggersSQL); err != nil {
		return fmt.Errorf("apply monthly late-write triggers: %w", err)
	}
	return nil
}

const monthlyLateWriteTriggersSQL = `
CREATE TRIGGER IF NOT EXISTS trg_log_evidence_monthly_late_insert
AFTER INSERT ON log_evidence
BEGIN
    UPDATE usage_monthly
    SET evidence_count = evidence_count + 1,
        updated_at = MAX(updated_at, CAST(strftime('%s', 'now') AS INTEGER))
    WHERE month_ts = CAST(strftime('%s', datetime(NEW.event_ts, 'unixepoch', 'start of month')) AS INTEGER);
END;

CREATE TRIGGER IF NOT EXISTS trg_log_evidence_monthly_late_move
AFTER UPDATE OF event_ts ON log_evidence
WHEN OLD.event_ts != NEW.event_ts
BEGIN
    UPDATE usage_monthly
    SET evidence_count = MAX(0, evidence_count - 1),
        updated_at = MAX(updated_at, CAST(strftime('%s', 'now') AS INTEGER))
    WHERE month_ts = CAST(strftime('%s', datetime(OLD.event_ts, 'unixepoch', 'start of month')) AS INTEGER);
    UPDATE usage_monthly
    SET evidence_count = evidence_count + 1,
        updated_at = MAX(updated_at, CAST(strftime('%s', 'now') AS INTEGER))
    WHERE month_ts = CAST(strftime('%s', datetime(NEW.event_ts, 'unixepoch', 'start of month')) AS INTEGER);
END;

CREATE TRIGGER IF NOT EXISTS trg_usage_chain_hourly_monthly_late_insert
AFTER INSERT ON usage_chain_1h
BEGIN
    UPDATE usage_monthly
    SET chain_count = chain_count + 1,
        updated_at = MAX(updated_at, CAST(strftime('%s', 'now') AS INTEGER))
    WHERE month_ts = CAST(strftime('%s', datetime(NEW.hour_ts, 'unixepoch', 'start of month')) AS INTEGER);
END;
`

func (s *Store) FlushMinute(
	ctx context.Context,
	minuteTS int64,
	usage map[model.UsageKey]model.UsageDelta,
	forward map[model.ForwardUsageKey]model.UsageDelta,
) error {
	return s.FlushMinuteBatch(ctx, minuteTS, usage, forward, nil)
}

// FlushMinuteBatch commits every counter family for one collector bucket in a
// single transaction. The collector keeps its in-memory bucket on error, so a
// split transaction here would make a successful usage commit repeat when a
// later interface commit failed and the bucket was retried.
func (s *Store) FlushMinuteBatch(
	ctx context.Context,
	minuteTS int64,
	usage map[model.UsageKey]model.UsageDelta,
	forward map[model.ForwardUsageKey]model.UsageDelta,
	interfaces map[string]model.InterfaceUsageDelta,
) error {
	monthlyUsage := make(map[int64]model.UsageDelta)
	monthlyForward := make(map[int64]model.UsageDelta)
	archiveUpdated := false
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin flush minute: %w", err)
	}
	defer tx.Rollback()

	if len(usage) > 0 {
		type processIdentity struct {
			pid  int
			comm string
			exe  string
		}
		knownProcesses := make(map[processIdentity]int64)
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

		processStmt, err := tx.PrepareContext(ctx, `
INSERT INTO known_processes (pid, comm, exe, seen_ts)
VALUES (?, ?, ?, ?)
ON CONFLICT(pid, comm, exe) DO UPDATE SET
    seen_ts = MAX(known_processes.seen_ts, excluded.seen_ts)`)
		if err != nil {
			return fmt.Errorf("prepare known process upsert: %w", err)
		}
		defer processStmt.Close()

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
			monthTS := monthStartUTC(time.Unix(key.MinuteTS, 0)).Unix()
			monthDelta := monthlyUsage[monthTS]
			monthDelta.BytesUp += delta.BytesUp
			monthDelta.BytesDown += delta.BytesDown
			monthDelta.FlowCount += delta.FlowCount
			monthlyUsage[monthTS] = monthDelta
			if key.PID > 0 || strings.TrimSpace(key.Comm) != "" || strings.TrimSpace(key.Exe) != "" {
				identity := processIdentity{pid: key.PID, comm: key.Comm, exe: key.Exe}
				if seenTS, exists := knownProcesses[identity]; !exists || key.MinuteTS > seenTS {
					knownProcesses[identity] = key.MinuteTS
				}
			}
		}
		for identity, seenTS := range knownProcesses {
			if _, err := processStmt.ExecContext(ctx, identity.pid, identity.comm, identity.exe, seenTS); err != nil {
				return fmt.Errorf("upsert known process: %w", err)
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
			monthTS := monthStartUTC(time.Unix(key.MinuteTS, 0)).Unix()
			monthDelta := monthlyForward[monthTS]
			monthDelta.BytesUp += delta.BytesUp
			monthDelta.BytesDown += delta.BytesDown
			monthDelta.FlowCount += delta.FlowCount
			monthlyForward[monthTS] = monthDelta
		}
	}

	updatedAt := s.now().UTC().Unix()
	for monthTS, delta := range monthlyUsage {
		result, err := tx.ExecContext(ctx, `
UPDATE usage_monthly
SET bytes_up = bytes_up + ?,
    bytes_down = bytes_down + ?,
    flow_count = flow_count + ?,
    updated_at = MAX(updated_at, ?)
WHERE month_ts = ?
`, delta.BytesUp, delta.BytesDown, delta.FlowCount, updatedAt, monthTS)
		if err != nil {
			return fmt.Errorf("refresh archived monthly usage: %w", err)
		}
		if affected, err := result.RowsAffected(); err == nil && affected > 0 {
			archiveUpdated = true
		}
	}
	for monthTS, delta := range monthlyForward {
		result, err := tx.ExecContext(ctx, `
UPDATE usage_monthly
SET forward_bytes_orig = forward_bytes_orig + ?,
    forward_bytes_reply = forward_bytes_reply + ?,
    forward_flow_count = forward_flow_count + ?,
    updated_at = MAX(updated_at, ?)
WHERE month_ts = ?
`, delta.BytesUp, delta.BytesDown, delta.FlowCount, updatedAt, monthTS)
		if err != nil {
			return fmt.Errorf("refresh archived monthly forward usage: %w", err)
		}
		if affected, err := result.RowsAffected(); err == nil && affected > 0 {
			archiveUpdated = true
		}
	}

	if len(interfaces) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
INSERT INTO interface_1m (
    minute_ts, interface, rx_bytes, tx_bytes
) VALUES (?, ?, ?, ?)
ON CONFLICT(minute_ts, interface)
DO UPDATE SET
    rx_bytes = interface_1m.rx_bytes + excluded.rx_bytes,
    tx_bytes = interface_1m.tx_bytes + excluded.tx_bytes`)
		if err != nil {
			return fmt.Errorf("prepare interface minute flush: %w", err)
		}
		defer stmt.Close()

		for name, delta := range interfaces {
			name = strings.TrimSpace(name)
			if name == "" || (delta.RxBytes == 0 && delta.TxBytes == 0) {
				continue
			}
			if _, err := stmt.ExecContext(ctx, minuteTS, name, delta.RxBytes, delta.TxBytes); err != nil {
				return fmt.Errorf("flush interface minute usage: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateProcessCache()
	if archiveUpdated {
		s.invalidateMonthlyCache()
	}
	return nil
}

func (s *Store) FlushInterfaceMinute(ctx context.Context, minuteTS int64, interfaces map[string]model.InterfaceUsageDelta) error {
	return s.FlushMinuteBatch(ctx, minuteTS, nil, nil, interfaces)
}

func (s *Store) AggregateHour(ctx context.Context, hour time.Time) error {
	return s.aggregateHour(ctx, hour, true)
}

// AggregateHourOnly refreshes the hour-level tables without rescanning the
// entire UTC day. It is used during the short post-hour grace window, where
// the just-finished hour may receive one last collector flush.
func (s *Store) AggregateHourOnly(ctx context.Context, hour time.Time) error {
	return s.aggregateHour(ctx, hour, false)
}

// AggregateHours refreshes an ordered catch-up set and recomputes each affected
// UTC day once, after that day's final hour. All operations are idempotent; the
// caller advances its durable cursor only after this method succeeds.
func (s *Store) AggregateHours(ctx context.Context, hours []time.Time) error {
	for index, hour := range hours {
		hour = hour.UTC().Truncate(time.Hour)
		includeDay := index == len(hours)-1 || !sameUTCDay(hour, hours[index+1])
		if err := s.aggregateHour(ctx, hour, includeDay); err != nil {
			return err
		}
	}
	return nil
}

func sameUTCDay(left, right time.Time) bool {
	return left.UTC().Truncate(24 * time.Hour).Equal(right.UTC().Truncate(24 * time.Hour))
}

func (s *Store) aggregateHour(ctx context.Context, hour time.Time, includeDay bool) error {
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
INSERT INTO interface_1h (
    hour_ts, interface, rx_bytes, tx_bytes
)
SELECT ?, interface, SUM(rx_bytes), SUM(tx_bytes)
FROM interface_1m
WHERE minute_ts >= ? AND minute_ts < ?
GROUP BY interface
ON CONFLICT(hour_ts, interface) DO UPDATE SET
    rx_bytes = excluded.rx_bytes,
    tx_bytes = excluded.tx_bytes
`, hourTS, hourTS, nextHourTS); err != nil {
		return fmt.Errorf("aggregate interface_1h: %w", err)
	}

	if includeDay {
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

		if _, err := tx.ExecContext(ctx, `
INSERT INTO interface_1d (
    day_ts, interface, rx_bytes, tx_bytes
)
SELECT ?, interface, SUM(rx_bytes), SUM(tx_bytes)
FROM interface_1h
WHERE hour_ts >= ? AND hour_ts < ?
GROUP BY interface
ON CONFLICT(day_ts, interface) DO UPDATE SET
    rx_bytes = excluded.rx_bytes,
    tx_bytes = excluded.tx_bytes
	`, dayTS, dayTS, nextDayTS); err != nil {
			return fmt.Errorf("aggregate interface_1d: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM dirty_chain_hours WHERE hour_ts = ?`, hourTS); err != nil {
		return fmt.Errorf("clear dirty chain hour: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.invalidateMonthlyCache()
	return nil
}

func (s *Store) Cleanup(ctx context.Context) error {
	now := s.now().UTC()
	minuteCutoff := minuteRetentionStartUTC(now, s.retention).Unix()
	hourCutoff := hourRetentionStartUTC(now, s.retention).Unix()
	dayCutoff := dayRetentionStartUTC(now, s.retention).Unix()
	evidenceCutoff := evidenceRetentionStartUTC(now, s.retention).Unix()
	chainCutoff := chainRetentionStartUTC(now, s.retention).Unix()
	knownProcessCutoff := now.Add(-knownProcessRetention).Unix()
	closedMonthCutoff := monthStartUTC(now).Unix()
	summaryCutoff := minInt64(maxInt64(minuteCutoff, hourCutoff, dayCutoff, evidenceCutoff, chainCutoff), closedMonthCutoff)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cleanup: %w", err)
	}
	defer tx.Rollback()

	if err := s.summarizeClosedMonths(ctx, tx, closedMonthCutoff, now); err != nil {
		return err
	}
	if err := s.retainExpiringMonthlyCounts(ctx, tx, now, evidenceCutoff, chainCutoff); err != nil {
		return err
	}
	if err := s.summarizeCleanupMonthsIfMissing(ctx, tx, summaryCutoff, now); err != nil {
		return err
	}

	statements := []struct {
		query string
		arg   int64
	}{
		{`DELETE FROM usage_1m WHERE minute_ts < ?`, minuteCutoff},
		{`DELETE FROM interface_1m WHERE minute_ts < ?`, minuteCutoff},
		{`DELETE FROM interface_1h WHERE hour_ts < ?`, hourCutoff},
		{`DELETE FROM interface_1d WHERE day_ts < ?`, dayCutoff},
		{`DELETE FROM usage_1m_forward WHERE minute_ts < ?`, minuteCutoff},
		{`DELETE FROM usage_1h WHERE hour_ts < ?`, hourCutoff},
		{`DELETE FROM usage_1h_forward WHERE hour_ts < ?`, hourCutoff},
		{`DELETE FROM usage_1d WHERE day_ts < ?`, dayCutoff},
		{`DELETE FROM usage_1d_forward WHERE day_ts < ?`, dayCutoff},
		{`DELETE FROM usage_chain_1m
          WHERE minute_ts < ?
            AND NOT EXISTS (
                SELECT 1 FROM dirty_chain_hours dirty
                WHERE dirty.hour_ts = (usage_chain_1m.minute_ts / 3600) * 3600
            )`, chainCutoff},
		{`DELETE FROM usage_chain_1h WHERE hour_ts < ?`, chainCutoff},
		{`DELETE FROM log_evidence WHERE event_ts < ?`, evidenceCutoff},
		{`DELETE FROM dirty_chain_hours
          WHERE hour_ts < ?
            AND NOT EXISTS (
                SELECT 1 FROM usage_chain_1m minute
                WHERE minute.minute_ts >= dirty_chain_hours.hour_ts
                  AND minute.minute_ts < dirty_chain_hours.hour_ts + 3600
            )`, chainCutoff},
		{`DELETE FROM monthly_retained_counts WHERE month_ts < ?`, closedMonthCutoff},
		{`DELETE FROM known_processes WHERE seen_ts < ?`, knownProcessCutoff},
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

func (s *Store) retainExpiringMonthlyCounts(ctx context.Context, tx *sql.Tx, now time.Time, evidenceCutoff int64, chainCutoff int64) error {
	currentMonth := monthStartUTC(now).Unix()
	updatedAt := now.UTC().Unix()
	if evidenceCutoff > currentMonth {
		if err := s.retainExpiringEvidenceCounts(ctx, tx, currentMonth, evidenceCutoff, updatedAt); err != nil {
			return err
		}
	}
	if chainCutoff > currentMonth {
		if err := s.retainExpiringChainCounts(ctx, tx, currentMonth, chainCutoff, updatedAt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) retainExpiringEvidenceCounts(ctx context.Context, tx *sql.Tx, currentMonth int64, cutoff int64, updatedAt int64) error {
	eventMonth := monthExpr("event_ts")
	query := fmt.Sprintf(`
WITH candidates AS (
    SELECT %[1]s AS month_ts, COUNT(*) AS evidence_count
    FROM log_evidence
    LEFT JOIN monthly_retained_counts retained
        ON retained.month_ts = %[1]s
    WHERE event_ts >= ?
      AND event_ts < ?
      AND event_ts >= COALESCE(NULLIF(retained.evidence_until, 0), %[1]s)
    GROUP BY month_ts
)
INSERT INTO monthly_retained_counts (
    month_ts, evidence_count, chain_count, evidence_until, chain_until, updated_at
)
SELECT month_ts, evidence_count, 0, ?, 0, ?
FROM candidates
WHERE 1
ON CONFLICT(month_ts) DO UPDATE SET
    evidence_count = monthly_retained_counts.evidence_count + excluded.evidence_count,
    evidence_until = MAX(monthly_retained_counts.evidence_until, excluded.evidence_until),
    updated_at = excluded.updated_at
`, eventMonth)
	if _, err := tx.ExecContext(ctx, query, currentMonth, cutoff, cutoff, updatedAt); err != nil {
		return fmt.Errorf("retain expiring monthly evidence counts: %w", err)
	}
	return nil
}

func (s *Store) retainExpiringChainCounts(ctx context.Context, tx *sql.Tx, currentMonth int64, cutoff int64, updatedAt int64) error {
	hourMonth := monthExpr("hour_ts")
	minuteMonth := monthExpr("minute_ts")
	query := fmt.Sprintf(`
WITH retained_window AS (
    SELECT ? AS current_month, ? AS cutoff
),
hour_counts AS (
    SELECT %[1]s AS month_ts, COUNT(*) AS chain_count
    FROM usage_chain_1h
    LEFT JOIN monthly_retained_counts retained
        ON retained.month_ts = %[1]s
    JOIN retained_window
    WHERE hour_ts >= current_month
      AND hour_ts < cutoff
      AND hour_ts >= COALESCE(NULLIF(retained.chain_until, 0), %[1]s)
    GROUP BY month_ts
),
minute_fallback_counts AS (
    SELECT %[2]s AS month_ts, COUNT(*) AS chain_count
    FROM usage_chain_1m
    LEFT JOIN monthly_retained_counts retained
        ON retained.month_ts = %[2]s
    JOIN retained_window
    WHERE minute_ts >= current_month
      AND minute_ts < cutoff
      AND minute_ts >= COALESCE(NULLIF(retained.chain_until, 0), %[2]s)
      AND NOT EXISTS (
          SELECT 1
          FROM usage_chain_1h h
          WHERE h.hour_ts = (usage_chain_1m.minute_ts / 3600) * 3600
          LIMIT 1
      )
    GROUP BY month_ts
),
candidates AS (
    SELECT month_ts, SUM(chain_count) AS chain_count
    FROM (
        SELECT month_ts, chain_count FROM hour_counts
        UNION ALL
        SELECT month_ts, chain_count FROM minute_fallback_counts
    )
    GROUP BY month_ts
)
INSERT INTO monthly_retained_counts (
    month_ts, evidence_count, chain_count, evidence_until, chain_until, updated_at
)
SELECT month_ts, 0, chain_count, 0, ?, ?
FROM candidates
WHERE 1
ON CONFLICT(month_ts) DO UPDATE SET
    chain_count = monthly_retained_counts.chain_count + excluded.chain_count,
    chain_until = MAX(monthly_retained_counts.chain_until, excluded.chain_until),
    updated_at = excluded.updated_at
`, hourMonth, minuteMonth)
	if _, err := tx.ExecContext(ctx, query, currentMonth, cutoff, cutoff, updatedAt); err != nil {
		return fmt.Errorf("retain expiring monthly chain counts: %w", err)
	}
	return nil
}

func maxInt64(values ...int64) int64 {
	var max int64
	for index, value := range values {
		if index == 0 || value > max {
			max = value
		}
	}
	return max
}

func minInt64(values ...int64) int64 {
	var min int64
	for index, value := range values {
		if index == 0 || value < min {
			min = value
		}
	}
	return min
}

func (s *Store) summarizeCleanupMonthsIfMissing(ctx context.Context, tx *sql.Tx, cutoff int64, now time.Time) error {
	missingMonths, err := s.missingCleanupSummaryMonths(ctx, tx, cutoff, now)
	if err != nil {
		return err
	}
	if len(missingMonths) == 0 {
		return nil
	}

	if err := s.summarizeClosedMonths(ctx, tx, cutoff, now); err != nil {
		return fmt.Errorf("summarize missing cleanup months: %w", err)
	}
	missingMonths, err = s.missingCleanupSummaryMonths(ctx, tx, cutoff, now)
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

func (s *Store) missingCleanupSummaryMonths(ctx context.Context, tx *sql.Tx, cutoff int64, now time.Time) ([]int64, error) {
	cte, args := monthlyDetailAggregateCTE(s.monthlyArchiveWindow(cutoff, now))
	query := cte + `
SELECT month_ts
FROM monthly_detail
WHERE month_ts NOT IN (SELECT month_ts FROM usage_monthly)
ORDER BY month_ts ASC
`
	rows, err := tx.QueryContext(ctx, query, args...)
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
	cte, args := monthlyDetailAggregateCTE(s.monthlyArchiveWindow(before, now))
	query := cte + `
INSERT INTO usage_monthly (
    month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
    forward_flow_count, evidence_count, chain_count, updated_at
)
SELECT month_ts, bytes_up, bytes_down, flow_count, forward_bytes_orig, forward_bytes_reply,
       forward_flow_count, evidence_count, chain_count, ?
FROM monthly_detail
WHERE 1
ON CONFLICT(month_ts) DO UPDATE SET
    bytes_up = MAX(usage_monthly.bytes_up, excluded.bytes_up),
    bytes_down = MAX(usage_monthly.bytes_down, excluded.bytes_down),
    flow_count = MAX(usage_monthly.flow_count, excluded.flow_count),
    forward_bytes_orig = MAX(usage_monthly.forward_bytes_orig, excluded.forward_bytes_orig),
    forward_bytes_reply = MAX(usage_monthly.forward_bytes_reply, excluded.forward_bytes_reply),
    forward_flow_count = MAX(usage_monthly.forward_flow_count, excluded.forward_flow_count),
    evidence_count = MAX(usage_monthly.evidence_count, excluded.evidence_count),
    chain_count = MAX(usage_monthly.chain_count, excluded.chain_count),
    updated_at = MAX(usage_monthly.updated_at, excluded.updated_at)
`
	args = append(args, updatedAt)
	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("summarize closed months: %w", err)
	}
	return nil
}

func (s *Store) monthlyArchiveWindow(before int64, now time.Time) monthlyAggregateWindow {
	return monthlyAggregateWindow{
		start:          0,
		upper:          &before,
		hourBoundary:   hourRetentionStartUTC(now, s.retention).Unix(),
		minuteBoundary: monthlyMinuteBoundaryUTC(now, s.retention).Unix(),
		evidenceStart:  0,
		chainBoundary:  monthlyChainBoundaryUTC(now, s.retention).Unix(),
	}
}

func (s *Store) lastMetaTime(ctx context.Context, key string) (time.Time, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key)
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

func (s *Store) setMetaTime(ctx context.Context, key string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO meta (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, key, strconv.FormatInt(at.UTC().Unix(), 10))
	return err
}

func (s *Store) LastAggregatedHour(ctx context.Context) (time.Time, bool, error) {
	return s.lastMetaTime(ctx, "last_aggregated_hour")
}

func (s *Store) SetLastAggregatedHour(ctx context.Context, hour time.Time) error {
	return s.setMetaTime(ctx, "last_aggregated_hour", hour.Truncate(time.Hour))
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
    UNION
    SELECT (minute_ts / 3600) * 3600 AS hour_ts
    FROM interface_1m
    WHERE minute_ts >= ? AND minute_ts < ?
)
`, minMinute, before.UTC().Truncate(time.Hour).Unix(), minMinute, before.UTC().Truncate(time.Hour).Unix(), minMinute, before.UTC().Truncate(time.Hour).Unix())

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

func (s *Store) DirtyChainHours(ctx context.Context, before time.Time) ([]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT hour_ts
FROM dirty_chain_hours
WHERE hour_ts < ?
ORDER BY hour_ts ASC
`, before.UTC().Truncate(time.Hour).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hours := make([]time.Time, 0)
	for rows.Next() {
		var hourTS int64
		if err := rows.Scan(&hourTS); err != nil {
			return nil, err
		}
		hours = append(hours, time.Unix(hourTS, 0).UTC())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return hours, nil
}

func (s *Store) LastVacuum(ctx context.Context) (time.Time, bool, error) {
	return s.lastMetaTime(ctx, "last_vacuum_at")
}

func (s *Store) SetLastVacuum(ctx context.Context, at time.Time) error {
	return s.setMetaTime(ctx, "last_vacuum_at", at)
}

func (s *Store) Vacuum(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	return nil
}

func (s *Store) CheckpointWAL(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		return fmt.Errorf("wal checkpoint passive: %w", err)
	}
	return nil
}

func (s *Store) CheckpointWALTruncate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("wal checkpoint truncate: %w", err)
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
	now := s.now()
	startUTC := start.UTC()
	if !startUTC.Before(minuteRetentionStartUTC(now, s.retention)) {
		return DataSourceMinute, nil
	}
	if requiresMinute {
		if !startUTC.Before(hourRetentionStartUTC(now, s.retention)) || !startUTC.Before(dayRetentionStartUTC(now, s.retention)) {
			return "", ErrDimensionUnavailable
		}
		return "", ErrRangeUnavailable
	}
	if !startUTC.Before(hourRetentionStartUTC(now, s.retention)) {
		return DataSourceHour, nil
	}
	if !startUTC.Before(dayRetentionStartUTC(now, s.retention)) {
		return DataSourceDay, nil
	}
	return "", ErrRangeUnavailable
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

func InterfaceDataSource(source string) string {
	return interfaceSourceInfo(source).DataLabel
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
SELECT pid, comm, exe
FROM known_processes
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
