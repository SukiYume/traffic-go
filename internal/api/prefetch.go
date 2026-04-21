package api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

const (
	// Background prefetch is a cache warmer, not the main request path. Keep it
	// focused on the newest activity so a busy host does not re-run thousands of
	// explain queries every minute.
	backgroundPrefetchReplayWindow = 3 * time.Minute
	backgroundPrefetchMaxUsageRows = 256
)

type BackgroundPrefetchOptions struct {
	Enabled             bool
	Now                 time.Time
	EvidenceLookback    time.Duration
	ChainLookback       time.Duration
	ScanBudget          time.Duration
	MaxScanFiles        int
	MaxScanLinesPerFile int
}

type BackgroundPrefetchSummary struct {
	Sources        int
	EvidenceRows   int
	UsageRows      int
	ChainRows      int
	PartialSources int
	Errors         int
}

type processLogDescriptor struct {
	LookupKey       string
	LogDir          string
	EvidenceSource  string
	FileNameMatcher func(string) bool
	Parser          evidence.Parser
}

func (s *Server) RunBackgroundPrefetch(ctx context.Context, options BackgroundPrefetchOptions) BackgroundPrefetchSummary {
	summary := BackgroundPrefetchSummary{}
	if !options.Enabled || len(s.processLogDirs) == 0 {
		return summary
	}

	now := options.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	descriptors := s.processLogDescriptors()
	if len(descriptors) == 0 {
		return summary
	}

	startTS := now.Add(-options.EvidenceLookback).Unix()
	endTS := now.Add(time.Minute).Unix()
	for _, descriptor := range descriptors {
		result, err := evidence.PrefetchWindow(ctx, s.store, evidence.PrefetchOptions{
			Source:              descriptor.EvidenceSource,
			LogDir:              descriptor.LogDir,
			StartTS:             startTS,
			EndTS:               endTS,
			ReferenceTS:         endTS,
			FileNameMatcher:     descriptor.FileNameMatcher,
			Parser:              descriptor.Parser,
			ScanBudget:          options.ScanBudget,
			MaxScanFiles:        options.MaxScanFiles,
			MaxScanLinesPerFile: options.MaxScanLinesPerFile,
		})
		summary.Sources++
		if err != nil {
			summary.Errors++
			s.logPrefetchf("prefetch source %s failed: %v", descriptor.LookupKey, err)
			continue
		}
		summary.EvidenceRows += result.RowsImported
		if result.Partial {
			summary.PartialSources++
		}
		if strings.TrimSpace(result.Note) != "" {
			s.logPrefetchf("prefetch source %s: %s", descriptor.LookupKey, result.Note)
		}
		if descriptor.EvidenceSource == evidenceSourceSS && !result.Partial && result.RowsImported == 0 {
			journalRows, journalNote, err := s.loadShadowsocksJournalEvidence(
				ctx,
				startTS,
				endTS,
				endTS,
				backgroundPrefetchMaxEvidenceRows,
				nil,
				options.ScanBudget,
			)
			if err != nil {
				summary.Errors++
				s.logPrefetchf("prefetch source %s journal fallback failed: %v", descriptor.LookupKey, err)
				continue
			}
			if journalNote != "" {
				s.logPrefetchf("prefetch source %s: %s", descriptor.LookupKey, journalNote)
			}
			if len(journalRows) > 0 {
				summary.EvidenceRows += len(journalRows)
				s.logPrefetchf("prefetch source %s: imported %d rows from systemd journal", descriptor.LookupKey, len(journalRows))
			}
		}
	}

	chainStart := now.Add(-options.ChainLookback)
	replayFloor := now.Add(-backgroundPrefetchReplayWindow)
	if chainStart.Before(replayFloor) {
		chainStart = replayFloor
	}

	usageRows, truncated, err := s.queryUsageRowsForPrefetch(ctx, chainStart, now.Add(time.Minute), backgroundPrefetchMaxUsageRows)
	if err != nil {
		summary.Errors++
		s.logPrefetchf("query prefetch usage rows failed: %v", err)
		return summary
	}
	if truncated {
		s.logPrefetchf("prefetch usage replay capped at %d rows within %s", backgroundPrefetchMaxUsageRows, backgroundPrefetchReplayWindow)
	}
	queries := dedupePrefetchExplainQueries(usageRows)
	for _, query := range queries {
		if _, _, ok := s.lookupConfiguredProcessLogDir(query.Comm, query.Exe); !ok {
			continue
		}
		summary.UsageRows++
		response, err := s.analyzeUsageExplainWithOptions(ctx, query, usageExplainOptions{allowFileScan: false})
		if err != nil {
			summary.Errors++
			s.logPrefetchf("precompute chain for %s/%s %s:%d -> %s:%d failed: %v",
				query.Comm,
				query.Exe,
				query.Proto,
				queryLocalPort(query),
				query.RemoteIP,
				nullablePortValue(query.RemotePort),
				err,
			)
			continue
		}
		summary.ChainRows += len(response.Chains)
	}
	return summary
}

func dedupePrefetchExplainQueries(rows []model.UsageRecord) []usageExplainQuery {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(rows))
	result := make([]usageExplainQuery, 0, len(rows))
	for _, row := range rows {
		query, ok := usageExplainQueryFromRecord(row)
		if !ok {
			continue
		}
		key := usageExplainQueryKey(query)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, query)
	}
	return result
}

func (s *Server) processLogDescriptors() []processLogDescriptor {
	if len(s.processLogDirs) == 0 {
		return nil
	}
	result := make([]processLogDescriptor, 0, len(s.processLogDirs))
	seen := make(map[string]struct{}, len(s.processLogDirs))
	for key, logDir := range s.processLogDirs {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		normalizedDir := strings.TrimSpace(logDir)
		if normalizedKey == "" || normalizedDir == "" {
			continue
		}
		descriptor := buildProcessLogDescriptor(normalizedKey, normalizedDir)
		dedupeKey := fmt.Sprintf("%s|%s", descriptor.EvidenceSource, descriptor.LogDir)
		if _, ok := seen[dedupeKey]; ok {
			continue
		}
		seen[dedupeKey] = struct{}{}
		result = append(result, descriptor)
	}
	return result
}

func buildProcessLogDescriptor(lookupKey string, logDir string) processLogDescriptor {
	switch {
	case isNginxProcess(lookupKey, ""):
		return processLogDescriptor{
			LookupKey:       lookupKey,
			LogDir:          logDir,
			EvidenceSource:  evidenceSourceNginx,
			FileNameMatcher: isNginxLogFileName,
			Parser:          parseNginxEvidenceLine,
		}
	case isShadowsocksProcess(lookupKey, ""):
		return processLogDescriptor{
			LookupKey:       lookupKey,
			LogDir:          logDir,
			EvidenceSource:  evidenceSourceSS,
			FileNameMatcher: isShadowsocksLogFileName,
			Parser:          parseSSEvidenceLine,
		}
	default:
		return processLogDescriptor{
			LookupKey:       lookupKey,
			LogDir:          logDir,
			EvidenceSource:  customEvidenceSource(lookupKey),
			FileNameMatcher: isGenericLogFileName,
			Parser:          parseGenericEvidenceLine,
		}
	}
}

func (s *Server) queryUsageRowsForPrefetch(ctx context.Context, start time.Time, end time.Time, maxRows int) ([]model.UsageRecord, bool, error) {
	query := model.UsageQuery{
		Start:     start.UTC(),
		End:       end.UTC(),
		SortBy:    "minute_ts",
		SortOrder: "desc",
	}
	result := make([]model.UsageRecord, 0, 256)
	for {
		pageLimit := 200
		if maxRows > 0 {
			remaining := maxRows - len(result)
			if remaining <= 0 {
				return result, true, nil
			}
			if remaining < pageLimit {
				pageLimit = remaining
			}
		}
		query.Limit = pageLimit

		rows, nextCursor, _, err := s.store.QueryUsage(ctx, query, store.DataSourceMinute)
		if err != nil {
			return nil, false, err
		}
		result = append(result, rows...)
		if maxRows > 0 && len(result) >= maxRows {
			return result, nextCursor != "", nil
		}
		if nextCursor == "" {
			return result, false, nil
		}
		cursorTS, cursorRowID, err := store.DecodeCursor(nextCursor)
		if err != nil {
			return nil, false, fmt.Errorf("decode usage cursor: %w", err)
		}
		query.CursorTS = cursorTS
		query.CursorRowID = cursorRowID
	}
}

func usageExplainQueryFromRecord(record model.UsageRecord) (usageExplainQuery, bool) {
	if record.TimeBucket <= 0 || record.Direction == "" || record.Proto == "" {
		return usageExplainQuery{}, false
	}
	return usageExplainQuery{
		TimeBucket: record.TimeBucket,
		DataSource: store.DataSourceMinute,
		Proto:      record.Proto,
		Direction:  record.Direction,
		PID:        record.PID,
		Comm:       strings.TrimSpace(record.Comm),
		Exe:        nullableStringValue(record.Exe),
		LocalPort:  nullablePort(record.LocalPort),
		RemoteIP:   strings.TrimSpace(record.RemoteIP),
		RemotePort: record.RemotePort,
	}, true
}

func usageExplainQueryKey(query usageExplainQuery) string {
	return fmt.Sprintf(
		"%d|%s|%s|%d|%s|%s|%d|%s|%d",
		query.TimeBucket,
		strings.TrimSpace(query.Proto),
		strings.TrimSpace(string(query.Direction)),
		nullablePortValue(query.PID),
		strings.TrimSpace(query.Comm),
		strings.TrimSpace(query.Exe),
		queryLocalPort(query),
		strings.TrimSpace(query.RemoteIP),
		nullablePortValue(query.RemotePort),
	)
}

func (s *Server) logPrefetchf(format string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Printf(format, args...)
}
