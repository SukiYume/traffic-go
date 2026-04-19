package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"traffic-go/internal/model"
)

type LogEvidenceQuery struct {
	Source         string
	StartTS        int64
	EndTS          int64
	ClientIP       string
	TargetIP       string
	AnyIP          string
	HostNormalized string
	EntryPort      int
	TargetPort     int
	Limit          int
}

func clampEvidenceLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 2000 {
		return 2000
	}
	return limit
}

func (s *Store) UpsertLogEvidenceBatch(ctx context.Context, rows []model.LogEvidence) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert log evidence: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO log_evidence (
    source, event_ts, client_ip, target_ip, host, host_normalized, path, method, entry_port, target_port, status, message, fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    source = excluded.source,
    event_ts = excluded.event_ts,
    client_ip = excluded.client_ip,
    target_ip = excluded.target_ip,
    host = excluded.host,
    host_normalized = excluded.host_normalized,
    path = excluded.path,
    method = excluded.method,
    entry_port = excluded.entry_port,
    target_port = excluded.target_port,
    status = excluded.status,
    message = excluded.message
`)
	if err != nil {
		return fmt.Errorf("prepare upsert log evidence: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		row = normalizeLogEvidenceRow(row)
		status := any(nil)
		if row.Status != nil {
			status = *row.Status
		}
		if _, err := stmt.ExecContext(
			ctx,
			row.Source,
			row.EventTS,
			row.ClientIP,
			row.TargetIP,
			row.Host,
			row.HostNormalized,
			row.Path,
			row.Method,
			row.EntryPort,
			row.TargetPort,
			status,
			row.Message,
			row.Fingerprint,
		); err != nil {
			return fmt.Errorf("upsert log evidence: %w", err)
		}
	}

	return tx.Commit()
}

func (s *Store) QueryLogEvidence(ctx context.Context, query LogEvidenceQuery) ([]model.LogEvidence, error) {
	limit := clampEvidenceLimit(query.Limit)

	if query.AnyIP != "" && query.ClientIP == "" && query.TargetIP == "" {
		return s.queryLogEvidenceAnyIP(ctx, query, limit)
	}

	sqlText := `
SELECT source, event_ts, client_ip, target_ip, host, host_normalized, path, method, entry_port, target_port, status, message, fingerprint
FROM log_evidence
WHERE source = ? AND event_ts >= ? AND event_ts <= ?
`
	args := []any{query.Source, query.StartTS, query.EndTS}
	if query.ClientIP != "" {
		sqlText += " AND client_ip = ?"
		args = append(args, query.ClientIP)
	}
	if query.TargetIP != "" {
		sqlText += " AND target_ip = ?"
		args = append(args, query.TargetIP)
	}
	if query.AnyIP != "" {
		sqlText += " AND (client_ip = ? OR target_ip = ?)"
		args = append(args, query.AnyIP, query.AnyIP)
	}
	if query.HostNormalized != "" {
		sqlText += " AND host_normalized = ?"
		args = append(args, query.HostNormalized)
	}
	if query.EntryPort > 0 {
		sqlText += " AND entry_port = ?"
		args = append(args, query.EntryPort)
	}
	if query.TargetPort > 0 {
		sqlText += " AND target_port = ?"
		args = append(args, query.TargetPort)
	}
	sqlText += " ORDER BY event_ts DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("query log evidence: %w", err)
	}
	defer rows.Close()

	result, err := scanLogEvidenceRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan log evidence rows: %w", err)
	}
	return result, nil
}

func (s *Store) queryLogEvidenceAnyIP(ctx context.Context, query LogEvidenceQuery, limit int) ([]model.LogEvidence, error) {
	extraSQL := ""
	extraArgs := make([]any, 0, 2)
	if query.HostNormalized != "" {
		extraSQL += " AND host_normalized = ?"
		extraArgs = append(extraArgs, query.HostNormalized)
	}
	if query.EntryPort > 0 {
		extraSQL += " AND entry_port = ?"
		extraArgs = append(extraArgs, query.EntryPort)
	}
	if query.TargetPort > 0 {
		extraSQL += " AND target_port = ?"
		extraArgs = append(extraArgs, query.TargetPort)
	}

	sqlText := `
SELECT source, event_ts, client_ip, target_ip, host, host_normalized, path, method, entry_port, target_port, status, message, fingerprint
FROM (
    SELECT source, event_ts, client_ip, target_ip, host, host_normalized, path, method, entry_port, target_port, status, message, fingerprint
    FROM log_evidence
    WHERE source = ? AND event_ts >= ? AND event_ts <= ?` + extraSQL + ` AND client_ip = ?
    UNION
    SELECT source, event_ts, client_ip, target_ip, host, host_normalized, path, method, entry_port, target_port, status, message, fingerprint
    FROM log_evidence
    WHERE source = ? AND event_ts >= ? AND event_ts <= ?` + extraSQL + ` AND target_ip = ?
)
ORDER BY event_ts DESC
LIMIT ?`

	args := make([]any, 0, 10+len(extraArgs)*2)
	args = append(args, query.Source, query.StartTS, query.EndTS)
	args = append(args, extraArgs...)
	args = append(args, query.AnyIP)
	args = append(args, query.Source, query.StartTS, query.EndTS)
	args = append(args, extraArgs...)
	args = append(args, query.AnyIP)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("query any-ip log evidence: %w", err)
	}
	defer rows.Close()

	result, err := scanLogEvidenceRows(rows)
	if err != nil {
		return nil, fmt.Errorf("scan any-ip log evidence rows: %w", err)
	}
	return result, nil
}

func scanLogEvidenceRows(rows *sql.Rows) ([]model.LogEvidence, error) {

	result := make([]model.LogEvidence, 0)
	for rows.Next() {
		var item model.LogEvidence
		var status sql.NullInt64
		if err := rows.Scan(
			&item.Source,
			&item.EventTS,
			&item.ClientIP,
			&item.TargetIP,
			&item.Host,
			&item.HostNormalized,
			&item.Path,
			&item.Method,
			&item.EntryPort,
			&item.TargetPort,
			&status,
			&item.Message,
			&item.Fingerprint,
		); err != nil {
			return nil, fmt.Errorf("scan log evidence: %w", err)
		}
		if status.Valid {
			statusValue := int(status.Int64)
			item.Status = &statusValue
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func normalizeLogEvidenceRow(row model.LogEvidence) model.LogEvidence {
	row.HostNormalized = strings.ToLower(strings.TrimSpace(row.HostNormalized))
	if row.HostNormalized == "" {
		row.HostNormalized = strings.ToLower(strings.TrimSpace(row.Host))
	}
	if row.TargetPort <= 0 {
		if port, err := strconv.Atoi(strings.TrimSpace(row.Path)); err == nil && port > 0 {
			row.TargetPort = port
		}
	}
	return row
}
