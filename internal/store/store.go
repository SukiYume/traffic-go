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

	dataSourceMinuteForward = "usage_1m_forward"
	dataSourceHourForward   = "usage_1h_forward"
)

var ErrDimensionUnavailable = errors.New("dimension_unavailable")

type Store struct {
	db        *sql.DB
	retention config.Retention
	now       func() time.Time
}

func Open(cfg config.Config) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil && filepath.Dir(cfg.DBPath) != "." {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

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

	return tx.Commit()
}

func (s *Store) Cleanup(ctx context.Context) error {
	minuteCutoff := s.now().Add(-time.Duration(s.retention.MinuteDays) * 24 * time.Hour).Unix()
	hourCutoff := s.now().Add(-time.Duration(s.retention.HourlyDays) * 24 * time.Hour).Unix()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cleanup: %w", err)
	}
	defer tx.Rollback()

	statements := []struct {
		query string
		arg   int64
	}{
		{`DELETE FROM usage_1m WHERE minute_ts < ?`, minuteCutoff},
		{`DELETE FROM usage_1h WHERE hour_ts < ?`, hourCutoff},
		{`DELETE FROM usage_1m_forward WHERE minute_ts < ?`, minuteCutoff},
		{`DELETE FROM usage_1h_forward WHERE hour_ts < ?`, hourCutoff},
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt.query, stmt.arg); err != nil {
			return fmt.Errorf("cleanup query failed: %w", err)
		}
	}

	return tx.Commit()
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
SELECT MIN((minute_ts / 3600) * 3600)
FROM usage_1m
WHERE minute_ts >= ? AND minute_ts < ?
`, minMinute, before.UTC().Truncate(time.Hour).Unix())

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
	minuteWindow := time.Duration(s.retention.MinuteDays) * 24 * time.Hour
	if end.Sub(start) > minuteWindow {
		if pidFilter || exeFilter {
			return "", ErrDimensionUnavailable
		}
		return DataSourceHour, nil
	}
	return DataSourceMinute, nil
}
