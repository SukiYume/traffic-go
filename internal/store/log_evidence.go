package store

import (
	"context"
	"database/sql"
	"fmt"

	"traffic-go/internal/model"
)

type LogEvidenceQuery struct {
	Source   string
	StartTS  int64
	EndTS    int64
	ClientIP string
	TargetIP string
	AnyIP    string
	Limit    int
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
    source, event_ts, client_ip, target_ip, host, path, method, status, message, fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    source = excluded.source,
    event_ts = excluded.event_ts,
    client_ip = excluded.client_ip,
    target_ip = excluded.target_ip,
    host = excluded.host,
    path = excluded.path,
    method = excluded.method,
    status = excluded.status,
    message = excluded.message
`)
	if err != nil {
		return fmt.Errorf("prepare upsert log evidence: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
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
			row.Path,
			row.Method,
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

	sqlText := `
SELECT source, event_ts, client_ip, target_ip, host, path, method, status, message, fingerprint
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
	sqlText += " ORDER BY event_ts DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, fmt.Errorf("query log evidence: %w", err)
	}
	defer rows.Close()

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
			&item.Path,
			&item.Method,
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
		return nil, fmt.Errorf("iterate log evidence: %w", err)
	}
	return result, nil
}
