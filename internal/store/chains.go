package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"traffic-go/internal/model"
)

type chainSourceInfo struct {
	Table     string
	TimeCol   string
	DataLabel string
}

func usageChainSourceInfo(source string) chainSourceInfo {
	switch source {
	case DataSourceHourChain:
		return chainSourceInfo{Table: DataSourceHourChain, TimeCol: "hour_ts", DataLabel: DataSourceHourChain}
	default:
		return chainSourceInfo{Table: DataSourceMinuteChain, TimeCol: "minute_ts", DataLabel: DataSourceMinuteChain}
	}
}

func BuildUsageChainID(bucketTS int64, source string, record model.UsageChainRecord) string {
	return fmt.Sprintf(
		"%s|%d|%d|%s|%s|%s|%d|%s|%s|%d",
		source,
		bucketTS,
		intValue(record.PID),
		strings.TrimSpace(record.Comm),
		strings.TrimSpace(stringValue(record.Exe)),
		strings.TrimSpace(record.SourceIP),
		intValue(record.EntryPort),
		strings.TrimSpace(record.TargetIP),
		normalizeChainHost(record.TargetHost),
		intValue(record.TargetPort),
	)
}

func (s *Store) UpsertUsageChains(ctx context.Context, records []model.UsageChainRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert usage chains: %w", err)
	}
	defer tx.Rollback()

	dirtyHours := make(map[int64]struct{}, len(records))

	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO usage_chain_1m (
    minute_ts, chain_id, pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized,
    target_port, bytes_total, flow_count, evidence_count, evidence_source, confidence, confidence_rank,
    sample_fingerprint, sample_message, sample_time
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(chain_id) DO UPDATE SET
    pid = excluded.pid,
    comm = excluded.comm,
    exe = excluded.exe,
    source_ip = excluded.source_ip,
    entry_port = excluded.entry_port,
    target_ip = excluded.target_ip,
    target_host = excluded.target_host,
    target_host_normalized = excluded.target_host_normalized,
    target_port = excluded.target_port,
    bytes_total = excluded.bytes_total,
    flow_count = excluded.flow_count,
    evidence_count = excluded.evidence_count,
    evidence_source = excluded.evidence_source,
    confidence = CASE WHEN excluded.confidence_rank >= usage_chain_1m.confidence_rank THEN excluded.confidence ELSE usage_chain_1m.confidence END,
    confidence_rank = CASE WHEN excluded.confidence_rank >= usage_chain_1m.confidence_rank THEN excluded.confidence_rank ELSE usage_chain_1m.confidence_rank END,
    sample_fingerprint = CASE WHEN excluded.sample_time >= usage_chain_1m.sample_time THEN excluded.sample_fingerprint ELSE usage_chain_1m.sample_fingerprint END,
    sample_message = CASE WHEN excluded.sample_time >= usage_chain_1m.sample_time THEN excluded.sample_message ELSE usage_chain_1m.sample_message END,
    sample_time = CASE WHEN excluded.sample_time >= usage_chain_1m.sample_time THEN excluded.sample_time ELSE usage_chain_1m.sample_time END
`)
	if err != nil {
		return fmt.Errorf("prepare upsert usage chains: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		record = normalizeUsageChainRecord(record, DataSourceMinuteChain)
		dirtyHours[timeBucketToHour(record.TimeBucket)] = struct{}{}
		if _, err := stmt.ExecContext(
			ctx,
			record.TimeBucket,
			record.ChainID,
			intValue(record.PID),
			record.Comm,
			stringValue(record.Exe),
			record.SourceIP,
			intValue(record.EntryPort),
			record.TargetIP,
			record.TargetHost,
			record.TargetHostNormalized,
			intValue(record.TargetPort),
			record.BytesTotal,
			record.FlowCount,
			record.EvidenceCount,
			record.EvidenceSource,
			record.Confidence,
			confidenceRank(record.Confidence),
			record.SampleFingerprint,
			record.SampleMessage,
			record.SampleTime,
		); err != nil {
			return fmt.Errorf("upsert usage chain: %w", err)
		}
	}

	if err := upsertDirtyHours(ctx, tx, dirtyHours); err != nil {
		return err
	}

	return tx.Commit()
}

func upsertDirtyHours(ctx context.Context, tx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, hours map[int64]struct{}) error {
	for hourTS := range hours {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO dirty_chain_hours (hour_ts) VALUES (?)
ON CONFLICT(hour_ts) DO NOTHING
`, hourTS); err != nil {
			return fmt.Errorf("mark dirty chain hour: %w", err)
		}
	}
	return nil
}

func timeBucketToHour(bucketTS int64) int64 {
	return (bucketTS / 3600) * 3600
}

func (s *Store) QueryUsageChainsForProcess(ctx context.Context, bucketTS int64, pid *int, comm string, exe string, source string) ([]model.UsageChainRecord, error) {
	info := usageChainSourceInfo(source)
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf(`
SELECT chain_id, %[1]s, pid, comm, exe, source_ip, entry_port, target_ip, target_host, target_host_normalized,
       target_port, bytes_total, flow_count, evidence_count, evidence_source, confidence,
       sample_fingerprint, sample_message, sample_time
FROM %[2]s
WHERE %[1]s = ?
`, info.TimeCol, info.Table))

	args := []any{bucketTS}
	if pid != nil {
		builder.WriteString(" AND pid = ?")
		args = append(args, *pid)
	}
	if strings.TrimSpace(comm) != "" {
		builder.WriteString(" AND comm = ?")
		args = append(args, strings.TrimSpace(comm))
	}
	if strings.TrimSpace(exe) != "" {
		appendExeFilter(&builder, &args, exe)
	}
	builder.WriteString(" ORDER BY bytes_total DESC, evidence_count DESC, chain_id ASC")

	rows, err := s.db.QueryContext(ctx, builder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query usage chains: %w", err)
	}
	defer rows.Close()

	result := make([]model.UsageChainRecord, 0)
	for rows.Next() {
		var (
			record         model.UsageChainRecord
			pidValue       int
			exeValue       string
			entryPortValue int
			targetPort     int
		)
		record.DataSource = info.DataLabel
		if err := rows.Scan(
			&record.ChainID,
			&record.TimeBucket,
			&pidValue,
			&record.Comm,
			&exeValue,
			&record.SourceIP,
			&entryPortValue,
			&record.TargetIP,
			&record.TargetHost,
			&record.TargetHostNormalized,
			&targetPort,
			&record.BytesTotal,
			&record.FlowCount,
			&record.EvidenceCount,
			&record.EvidenceSource,
			&record.Confidence,
			&record.SampleFingerprint,
			&record.SampleMessage,
			&record.SampleTime,
		); err != nil {
			return nil, fmt.Errorf("scan usage chain: %w", err)
		}
		record.PID = nullableIntFromInt(pidValue)
		record.Exe = nullableStringFromString(exeValue)
		record.EntryPort = nullableIntFromInt(entryPortValue)
		record.TargetPort = nullableIntFromInt(targetPort)
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func normalizeUsageChainRecord(record model.UsageChainRecord, source string) model.UsageChainRecord {
	record.Comm = strings.TrimSpace(record.Comm)
	record.SourceIP = strings.TrimSpace(record.SourceIP)
	record.TargetIP = strings.TrimSpace(record.TargetIP)
	record.TargetHost = strings.TrimSpace(record.TargetHost)
	record.TargetHostNormalized = normalizeChainHost(record.TargetHost)
	record.EvidenceSource = strings.TrimSpace(record.EvidenceSource)
	record.Confidence = normalizeConfidence(record.Confidence)
	record.SampleFingerprint = strings.TrimSpace(record.SampleFingerprint)
	record.SampleMessage = strings.TrimSpace(record.SampleMessage)

	if record.Exe != nil {
		exe := strings.TrimSpace(*record.Exe)
		record.Exe = &exe
	}
	if record.ChainID == "" {
		record.ChainID = BuildUsageChainID(record.TimeBucket, source, record)
	}
	return record
}

func normalizeConfidence(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return "high"
	case "medium":
		return "medium"
	default:
		return "low"
	}
}

func confidenceRank(value string) int {
	switch normalizeConfidence(value) {
	case "high":
		return 3
	case "medium":
		return 2
	default:
		return 1
	}
}

func intValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullableIntFromInt(value int) *int {
	if value <= 0 {
		return nil
	}
	result := value
	return &result
}

func nullableStringFromString(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	result := value
	return &result
}

func normalizeChainHost(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
