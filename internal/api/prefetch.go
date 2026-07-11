package api

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

const (
	// Background prefetch is a cache warmer, not the main request path. Keep row
	// count bounded so a busy host does not re-run thousands of explain queries
	// every minute.
	// Each explain replay can perform several evidence/chain lookups. A 64-row
	// cap keeps the warmer bounded to one usage query. A saved usage cursor then
	// drains the next backlog page instead of repeatedly replaying the newest 64.
	backgroundPrefetchMaxUsageRows = 64

	backgroundEvidenceWatermarkPrefix = "\x00background-evidence-watermark|"
	backgroundChainWatermarkKey       = "\x00background-chain-watermark"
	backgroundChainWindowStartKey     = "\x00background-chain-window-start"
	backgroundChainWindowEndKey       = "\x00background-chain-window-end"
	backgroundChainCursorTSKey        = "\x00background-chain-cursor-ts"
	backgroundChainCursorRowIDKey     = "\x00background-chain-cursor-row-id"
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

type backgroundChainPrefetchState struct {
	Watermark   int64
	WindowStart int64
	WindowEnd   int64
	CursorTS    int64
	CursorRowID int64
	Active      bool
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
	defaultEvidenceStartTS := now.Add(-options.EvidenceLookback).Unix()
	evidenceEndTS := backgroundPrefetchClosedEnd(now).Unix()
	if defaultEvidenceStartTS > evidenceEndTS {
		defaultEvidenceStartTS = evidenceEndTS
	}
	select {
	case s.scanGate <- struct{}{}:
	default:
		// An interactive scan has priority; the next scheduled prefetch can
		// catch up from the frozen low watermarks without concurrent disk I/O.
		for _, descriptor := range descriptors {
			s.ensureEvidencePrefetchWatermark(descriptor, defaultEvidenceStartTS)
		}
		s.ensureBackgroundChainPrefetchState(now, options.ChainLookback)
		summary.PartialSources = len(descriptors)
		return summary
	}
	scanGateHeld := true
	defer func() {
		if scanGateHeld {
			<-s.scanGate
		}
	}()

	for _, descriptor := range descriptors {
		startTS := defaultEvidenceStartTS
		if watermark := s.ensureEvidencePrefetchWatermark(descriptor, defaultEvidenceStartTS); watermark > 0 && watermark < startTS {
			startTS = watermark
		}
		scanCtx, cancelScan := context.WithCancel(ctx)
		if options.ScanBudget > 0 {
			scanCtx, cancelScan = context.WithTimeout(ctx, options.ScanBudget)
		}
		journalCtx := withExplainScanContext(ctx, scanCtx)
		result, err := evidence.PrefetchWindow(ctx, s.store, evidence.PrefetchOptions{
			Source:              descriptor.EvidenceSource,
			LogDir:              descriptor.LogDir,
			StartTS:             startTS,
			EndTS:               evidenceEndTS,
			ReferenceTS:         evidenceEndTS,
			FileNameMatcher:     descriptor.FileNameMatcher,
			Parser:              descriptor.Parser,
			ScanBudget:          options.ScanBudget,
			MaxScanFiles:        options.MaxScanFiles,
			MaxScanLinesPerFile: options.MaxScanLinesPerFile,
			CursorLookup:        s.lookupPrefetchCursor,
			CursorCommit:        s.commitPrefetchCursor,
			ScanContext:         scanCtx,
			PersistContext:      ctx,
		})
		summary.Sources++
		if err != nil {
			cancelScan()
			summary.Errors++
			s.logPrefetchf("prefetch source %s failed: %v", descriptor.LookupKey, err)
			continue
		}
		sourcePartial := result.Partial
		summary.EvidenceRows += result.RowsImported
		if result.Partial {
			summary.PartialSources++
		}
		if strings.TrimSpace(result.Note) != "" {
			s.logPrefetchf("prefetch source %s: %s", descriptor.LookupKey, result.Note)
		}
		if descriptor.EvidenceSource == evidenceSourceSS && !sourcePartial && result.RowsImported == 0 {
			journalRows, journalNote, err := s.loadShadowsocksJournalEvidence(
				journalCtx,
				startTS,
				evidenceEndTS,
				evidenceEndTS,
				backgroundPrefetchMaxEvidenceRows,
				nil,
				options.ScanBudget,
			)
			if err != nil {
				cancelScan()
				summary.Errors++
				s.logPrefetchf("prefetch source %s journal fallback failed: %v", descriptor.LookupKey, err)
				continue
			}
			if journalNote != "" {
				if !sourcePartial {
					summary.PartialSources++
				}
				sourcePartial = true
				s.logPrefetchf("prefetch source %s: %s", descriptor.LookupKey, journalNote)
			}
			if len(journalRows) > 0 {
				summary.EvidenceRows += len(journalRows)
				s.logPrefetchf("prefetch source %s: imported %d rows from systemd journal", descriptor.LookupKey, len(journalRows))
			}
		}
		cancelScan()
		if !sourcePartial && ctx.Err() == nil {
			s.commitEvidencePrefetchWatermark(descriptor, evidenceEndTS)
		}
	}
	<-s.scanGate
	scanGateHeld = false

	chainState := s.ensureBackgroundChainPrefetchState(now, options.ChainLookback)
	if chainState.WindowStart >= chainState.WindowEnd {
		if chainState.WindowEnd > chainState.Watermark {
			chainState.Watermark = chainState.WindowEnd
		}
		chainState.Active = false
		s.commitBackgroundChainPrefetchState(chainState)
		return summary
	}
	if summary.Errors > 0 || summary.PartialSources > 0 {
		// Do not mark usage rows processed while their evidence window is known
		// to be incomplete. The frozen chain window will be retried after a fully
		// successful evidence pass.
		return summary
	}
	usageRows, nextUsageCursor, err := s.queryUsageRowsForPrefetchFromCursor(
		ctx,
		time.Unix(chainState.WindowStart, 0),
		time.Unix(chainState.WindowEnd, 0),
		backgroundPrefetchMaxUsageRows,
		chainState.CursorTS,
		chainState.CursorRowID,
	)
	if err != nil {
		summary.Errors++
		s.logPrefetchf("query prefetch usage rows failed: %v", err)
		return summary
	}
	if nextUsageCursor != "" {
		s.logPrefetchf("prefetch usage replay capped at %d rows within %s", backgroundPrefetchMaxUsageRows, options.ChainLookback)
	}
	queries := dedupePrefetchExplainQueries(usageRows)
	chainRecords := make([]model.UsageChainRecord, 0, len(queries))
	chainPageFailed := false
	for _, query := range queries {
		if _, _, ok := s.lookupConfiguredProcessLogDir(query.Comm, query.Exe); !ok {
			continue
		}
		summary.UsageRows++
		response, err := s.analyzeUsageExplainWithOptions(ctx, query, usageExplainOptions{allowFileScan: false, deferChainPersist: true})
		if err != nil {
			summary.Errors++
			chainPageFailed = true
			s.logPrefetchf("precompute chain for %s/%s %s:%d -> %s:%d failed: %v",
				query.Comm,
				query.Exe,
				query.Proto,
				nullablePortValue(query.LocalPort),
				query.RemoteIP,
				nullablePortValue(query.RemotePort),
				err,
			)
			continue
		}
		summary.ChainRows += len(response.Chains)
		chainRecords = append(chainRecords, canonicalChainRecords(query.TimeBucket, query, response.Chains)...)
	}
	chainRecords = dedupeUsageChainRecords(chainRecords)
	if len(chainRecords) > 0 {
		if err := s.store.UpsertUsageChains(ctx, chainRecords); err != nil {
			summary.Errors++
			chainPageFailed = true
			s.logPrefetchf("persist prefetched chains failed: %v", err)
		}
	}
	if chainPageFailed {
		return summary
	}
	if nextUsageCursor != "" {
		cursorTS, cursorRowID, err := store.DecodeCursor(nextUsageCursor)
		if err != nil {
			summary.Errors++
			s.logPrefetchf("decode prefetch usage cursor failed: %v", err)
			return summary
		}
		chainState.CursorTS = cursorTS
		chainState.CursorRowID = cursorRowID
		s.commitBackgroundChainPrefetchState(chainState)
	} else {
		if chainState.WindowEnd > chainState.Watermark {
			chainState.Watermark = chainState.WindowEnd
		}
		chainState.Active = false
		chainState.CursorTS = 0
		chainState.CursorRowID = 0
		s.commitBackgroundChainPrefetchState(chainState)
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
	keys := make([]string, 0, len(s.processLogDirs))
	for key := range s.processLogDirs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		logDir := s.processLogDirs[key]
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		normalizedDir := strings.TrimSpace(logDir)
		if normalizedKey == "" || normalizedDir == "" {
			continue
		}
		descriptor := buildProcessLogDescriptor(normalizedKey, normalizedDir)
		dedupeKey := fmt.Sprintf("%s|%s", descriptor.EvidenceSource, canonicalLogPathSpec(descriptor.LogDir))
		if _, ok := seen[dedupeKey]; ok {
			continue
		}
		seen[dedupeKey] = struct{}{}
		result = append(result, descriptor)
	}
	return result
}

func canonicalLogPathSpec(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.ContainsAny(trimmed, "*?[") {
		return filepath.Clean(trimmed)
	}
	if abs, err := filepath.Abs(trimmed); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(trimmed)
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
	rows, nextCursor, err := s.queryUsageRowsForPrefetchFromCursor(ctx, start, end, maxRows, 0, 0)
	return rows, nextCursor != "", err
}

func (s *Server) queryUsageRowsForPrefetchFromCursor(
	ctx context.Context,
	start time.Time,
	end time.Time,
	maxRows int,
	cursorTS int64,
	cursorRowID int64,
) ([]model.UsageRecord, string, error) {
	query := model.UsageQuery{
		Start:       start.UTC(),
		End:         end.UTC(),
		SortBy:      "minute_ts",
		SortOrder:   "desc",
		CursorTS:    cursorTS,
		CursorRowID: cursorRowID,
	}
	initialCapacity := 200
	if maxRows > 0 && maxRows < initialCapacity {
		initialCapacity = maxRows
	}
	result := make([]model.UsageRecord, 0, initialCapacity)
	for {
		pageLimit := 200
		if maxRows > 0 {
			remaining := maxRows - len(result)
			if remaining <= 0 {
				return result, "", nil
			}
			if remaining < pageLimit {
				pageLimit = remaining
			}
		}
		query.Limit = pageLimit

		rows, nextCursor, _, err := s.store.QueryUsage(ctx, query, store.DataSourceMinute)
		if err != nil {
			return nil, "", err
		}
		result = append(result, rows...)
		if maxRows > 0 && len(result) >= maxRows {
			return result, nextCursor, nil
		}
		if nextCursor == "" {
			return result, "", nil
		}
		cursorTS, cursorRowID, err := store.DecodeCursor(nextCursor)
		if err != nil {
			return nil, "", fmt.Errorf("decode usage cursor: %w", err)
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
		nullablePortValue(query.LocalPort),
		strings.TrimSpace(query.RemoteIP),
		nullablePortValue(query.RemotePort),
	)
}

func dedupeUsageChainRecords(records []model.UsageChainRecord) []model.UsageChainRecord {
	if len(records) == 0 {
		return nil
	}
	result := make([]model.UsageChainRecord, 0, len(records))
	seen := make(map[string]int, len(records))
	for _, record := range records {
		key := strings.TrimSpace(record.ChainID)
		if key == "" {
			continue
		}
		if existingIdx, ok := seen[key]; ok {
			if record.SampleTime >= result[existingIdx].SampleTime {
				result[existingIdx] = record
			}
			continue
		}
		seen[key] = len(result)
		result = append(result, record)
	}
	return result
}

func (s *Server) lookupPrefetchCursor(source string, path string) (evidence.PrefetchCursor, bool) {
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if len(s.prefetchCursors) == 0 {
		return evidence.PrefetchCursor{}, false
	}
	cursor, ok := s.prefetchCursors[prefetchCursorKey(source, path)]
	return cursor, ok
}

func (s *Server) commitPrefetchCursor(source string, path string, cursor evidence.PrefetchCursor) {
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if s.prefetchCursors == nil {
		s.prefetchCursors = make(map[string]evidence.PrefetchCursor)
	}
	s.prefetchCursors[prefetchCursorKey(source, path)] = cursor
}

func (s *Server) ensureEvidencePrefetchWatermark(descriptor processLogDescriptor, initialStart int64) int64 {
	key := backgroundEvidenceWatermarkPrefix + prefetchCursorKey(descriptor.EvidenceSource, descriptor.LogDir)
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if cursor, exists := s.prefetchCursors[key]; exists {
		return cursor.Offset
	}
	if s.prefetchCursors == nil {
		s.prefetchCursors = make(map[string]evidence.PrefetchCursor)
	}
	s.prefetchCursors[key] = evidence.PrefetchCursor{
		Identity: "background-evidence-watermark-v1",
		Offset:   initialStart,
	}
	return initialStart
}

func (s *Server) commitEvidencePrefetchWatermark(descriptor processLogDescriptor, watermark int64) {
	if watermark <= 0 {
		return
	}
	key := backgroundEvidenceWatermarkPrefix + prefetchCursorKey(descriptor.EvidenceSource, descriptor.LogDir)
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if s.prefetchCursors == nil {
		s.prefetchCursors = make(map[string]evidence.PrefetchCursor)
	}
	current := s.prefetchCursors[key]
	if watermark > current.Offset {
		current.Offset = watermark
		current.Identity = "background-evidence-watermark-v1"
		s.prefetchCursors[key] = current
	}
}

func (s *Server) lookupBackgroundChainPrefetchState() backgroundChainPrefetchState {
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if len(s.prefetchCursors) == 0 {
		return backgroundChainPrefetchState{}
	}
	_, active := s.prefetchCursors[backgroundChainWindowStartKey]
	return backgroundChainPrefetchState{
		Watermark:   s.prefetchCursors[backgroundChainWatermarkKey].Offset,
		WindowStart: s.prefetchCursors[backgroundChainWindowStartKey].Offset,
		WindowEnd:   s.prefetchCursors[backgroundChainWindowEndKey].Offset,
		CursorTS:    s.prefetchCursors[backgroundChainCursorTSKey].Offset,
		CursorRowID: s.prefetchCursors[backgroundChainCursorRowIDKey].Offset,
		Active:      active,
	}
}

func (s *Server) ensureBackgroundChainPrefetchState(now time.Time, lookback time.Duration) backgroundChainPrefetchState {
	state := s.lookupBackgroundChainPrefetchState()
	if state.Active {
		return state
	}
	state.WindowStart = now.Add(-lookback).Unix()
	if state.Watermark > 0 {
		state.WindowStart = state.Watermark
	}
	state.WindowEnd = backgroundPrefetchClosedEnd(now).Unix()
	if state.WindowStart > state.WindowEnd {
		state.WindowStart = state.WindowEnd
	}
	state.CursorTS = 0
	state.CursorRowID = 0
	state.Active = true
	// Freeze the backlog boundary before querying it. A skipped run or a query,
	// analysis, or persistence failure must retry the same page instead of
	// letting newer rows continually push it out of the 64-row cap.
	s.commitBackgroundChainPrefetchState(state)
	return state
}

func backgroundPrefetchClosedEnd(now time.Time) time.Time {
	// Keep one fully closed minute between collection and attribution. The
	// collector flushes a minute only after the clock advances, and log writers
	// can append the final records shortly afterward.
	return now.UTC().Truncate(time.Minute).Add(-time.Minute)
}

func (s *Server) commitBackgroundChainPrefetchState(state backgroundChainPrefetchState) {
	s.prefetchCursorMu.Lock()
	defer s.prefetchCursorMu.Unlock()
	if s.prefetchCursors == nil {
		s.prefetchCursors = make(map[string]evidence.PrefetchCursor)
	}
	s.prefetchCursors[backgroundChainWatermarkKey] = evidence.PrefetchCursor{
		Identity: "background-chain-watermark-v1",
		Offset:   state.Watermark,
	}
	if !state.Active {
		delete(s.prefetchCursors, backgroundChainWindowStartKey)
		delete(s.prefetchCursors, backgroundChainWindowEndKey)
		delete(s.prefetchCursors, backgroundChainCursorTSKey)
		delete(s.prefetchCursors, backgroundChainCursorRowIDKey)
		return
	}
	s.prefetchCursors[backgroundChainWindowStartKey] = evidence.PrefetchCursor{Identity: "background-chain-state-v1", Offset: state.WindowStart}
	s.prefetchCursors[backgroundChainWindowEndKey] = evidence.PrefetchCursor{Identity: "background-chain-state-v1", Offset: state.WindowEnd}
	s.prefetchCursors[backgroundChainCursorTSKey] = evidence.PrefetchCursor{Identity: "background-chain-state-v1", Offset: state.CursorTS}
	s.prefetchCursors[backgroundChainCursorRowIDKey] = evidence.PrefetchCursor{Identity: "background-chain-state-v1", Offset: state.CursorRowID}
}

func prefetchCursorKey(source string, path string) string {
	return strings.TrimSpace(source) + "|" + canonicalLogPathSpec(path)
}

func (s *Server) logPrefetchf(format string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Printf(format, args...)
}
