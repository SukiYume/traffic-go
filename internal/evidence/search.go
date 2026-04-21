package evidence

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

var fileDatePattern = regexp.MustCompile(`(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)`)

const tailScanChunkSize = 64 * 1024

type LogFileCandidate struct {
	Name    string
	Path    string
	ModTime time.Time
}

type Matcher func(model.LogEvidence) bool

type Parser func(source string, line string, reference time.Time) (model.LogEvidence, bool)

type QueryHints struct {
	ClientIP       string
	TargetIP       string
	AnyIP          string
	HostNormalized string
	EntryPort      int
	TargetPort     int
}

type Store interface {
	QueryLogEvidence(context.Context, store.LogEvidenceQuery) ([]model.LogEvidence, error)
	UpsertLogEvidenceBatch(context.Context, []model.LogEvidence) error
}

type SearchOptions struct {
	Source               string
	LogDir               string
	BucketTS             int64
	Limit                int
	QueryHints           QueryHints
	FileNameMatcher      func(string) bool
	Parser               Parser
	Matcher              Matcher
	StrictWindow         int64
	FallbackWindow       int64
	ScanBudget           time.Duration
	MaxScanFilesStrict   int
	MaxScanFilesFallback int
	MaxScanLinesPerFile  int
	CacheOnly            bool
}

type PrefetchOptions struct {
	Source              string
	LogDir              string
	StartTS             int64
	EndTS               int64
	ReferenceTS         int64
	FileNameMatcher     func(string) bool
	Parser              Parser
	Matcher             Matcher
	ScanBudget          time.Duration
	MaxScanFiles        int
	MaxScanLinesPerFile int
}

type PrefetchResult struct {
	FilesConsidered int
	RowsImported    int
	Partial         bool
	Note            string
}

type lineEvidenceCollector struct {
	source        string
	parser        Parser
	matcher       Matcher
	startTS       int64
	endTS         int64
	reference     time.Time
	limit         int
	collected     []model.LogEvidence
	seen          map[string]struct{}
	overEndStreak int
}

func newLineEvidenceCollector(
	source string,
	parser Parser,
	matcher Matcher,
	startTS int64,
	endTS int64,
	referenceTS int64,
	limit int,
	initialCapacity int,
) *lineEvidenceCollector {
	if initialCapacity < 0 {
		initialCapacity = 0
	}
	return &lineEvidenceCollector{
		source:    source,
		parser:    parser,
		matcher:   matcher,
		startTS:   startTS,
		endTS:     endTS,
		reference: time.Unix(referenceTS, 0).In(time.Local),
		limit:     limit,
		collected: make([]model.LogEvidence, 0, initialCapacity),
		seen:      make(map[string]struct{}, initialCapacity),
	}
}

func (c *lineEvidenceCollector) StartStream() {
	c.overEndStreak = 0
}

func (c *lineEvidenceCollector) AddLine(line string) bool {
	row, ok := c.parser(c.source, line, c.reference)
	if !ok {
		return false
	}
	row = Normalize(row)
	if row.EventTS > c.endTS {
		c.overEndStreak++
		return c.overEndStreak >= 200
	}
	c.overEndStreak = 0
	if row.EventTS < c.startTS || row.EventTS > c.endTS {
		return false
	}
	if c.matcher != nil && !c.matcher(row) {
		return false
	}
	if row.Fingerprint == "" {
		row.Fingerprint = Fingerprint(row)
	}
	if _, exists := c.seen[row.Fingerprint]; exists {
		return false
	}
	c.seen[row.Fingerprint] = struct{}{}
	c.collected = append(c.collected, row)
	return c.limit > 0 && len(c.collected) >= c.limit
}

func (c *lineEvidenceCollector) AddLineReverse(line string) bool {
	row, ok := c.parser(c.source, line, c.reference)
	if !ok {
		return false
	}
	row = Normalize(row)
	if row.EventTS < c.startTS || row.EventTS > c.endTS {
		return false
	}
	if c.matcher != nil && !c.matcher(row) {
		return false
	}
	if row.Fingerprint == "" {
		row.Fingerprint = Fingerprint(row)
	}
	if _, exists := c.seen[row.Fingerprint]; exists {
		return false
	}
	c.seen[row.Fingerprint] = struct{}{}
	c.collected = append(c.collected, row)
	return c.limit > 0 && len(c.collected) >= c.limit
}

func (c *lineEvidenceCollector) Rows() []model.LogEvidence {
	return c.collected
}

func (c *lineEvidenceCollector) Full() bool {
	return c.limit > 0 && len(c.collected) >= c.limit
}

func LookupOrScan(ctx context.Context, evidenceStore Store, options SearchOptions) ([]model.LogEvidence, string, error) {
	strictStart := options.BucketTS - options.StrictWindow
	strictEnd := options.BucketTS + options.StrictWindow
	fallbackStart := options.BucketTS - options.FallbackWindow
	fallbackEnd := options.BucketTS + options.FallbackWindow
	cacheLimit := CacheEvidenceLimit(options.Limit)

	rows, err := queryCachedEvidence(ctx, evidenceStore, options.Source, strictStart, strictEnd, options.QueryHints, cacheLimit)
	if err != nil {
		return nil, "", fmt.Errorf("query cached evidence: %w", err)
	}
	rows = Filter(rows, options.Matcher)
	rows = SortAndTrim(rows, options.BucketTS, options.Limit)
	if len(rows) > 0 {
		return rows, "", nil
	}

	rows, err = queryCachedEvidence(ctx, evidenceStore, options.Source, fallbackStart, fallbackEnd, options.QueryHints, cacheLimit)
	if err != nil {
		return nil, "", fmt.Errorf("query fallback evidence: %w", err)
	}
	rows = Filter(rows, options.Matcher)
	rows = SortAndTrim(rows, options.BucketTS, options.Limit)
	if len(rows) > 0 {
		return rows, "在缓存中命中 ±15 分钟窗口日志。", nil
	}

	if options.QueryHints.TargetPort > 0 && (options.QueryHints.AnyIP != "" || options.QueryHints.ClientIP != "" || options.QueryHints.TargetIP != "") {
		relaxedHints := options.QueryHints
		relaxedHints.AnyIP = ""
		relaxedHints.ClientIP = ""
		relaxedHints.TargetIP = ""

		rows, err = queryCachedEvidence(ctx, evidenceStore, options.Source, strictStart, strictEnd, relaxedHints, cacheLimit)
		if err != nil {
			return nil, "", fmt.Errorf("query relaxed cached evidence: %w", err)
		}
		rows = Filter(rows, options.Matcher)
		rows = SortAndTrim(rows, options.BucketTS, options.Limit)
		if len(rows) > 0 {
			return rows, "在缓存中按 host/port 命中日志候选。", nil
		}

		rows, err = queryCachedEvidence(ctx, evidenceStore, options.Source, fallbackStart, fallbackEnd, relaxedHints, cacheLimit)
		if err != nil {
			return nil, "", fmt.Errorf("query relaxed fallback evidence: %w", err)
		}
		rows = Filter(rows, options.Matcher)
		rows = SortAndTrim(rows, options.BucketTS, options.Limit)
		if len(rows) > 0 {
			return rows, "在缓存中按 host/port 命中 ±15 分钟窗口日志。", nil
		}
	}
	if options.CacheOnly {
		return nil, "", nil
	}

	strictFiles, err := ListLogFiles(options.LogDir, options.FileNameMatcher, strictStart, strictEnd, options.MaxScanFilesStrict)
	if err != nil {
		return nil, "", err
	}
	scanCtx, cancel := deriveScanContext(ctx, options.ScanBudget)
	defer cancel()
	if len(strictFiles) > 0 {
		scannedStrict, err := ScanFiles(scanCtx, options.Source, strictFiles, options.Parser, options.Matcher, strictStart, strictEnd, options.BucketTS, options.Limit*3, options.MaxScanLinesPerFile)
		if isBudgetTimeout(ctx, scanCtx, err) {
			if len(scannedStrict) > 0 {
				if persistErr := evidenceStore.UpsertLogEvidenceBatch(ctx, scannedStrict); persistErr != nil {
					return nil, "", fmt.Errorf("persist partial strict evidence: %w", persistErr)
				}
				return SortAndTrim(scannedStrict, options.BucketTS, options.Limit), formatScanBudgetNote(options.ScanBudget, true), nil
			}
			return nil, formatScanBudgetNote(options.ScanBudget, false), nil
		}
		if err != nil {
			return nil, "", err
		}
		if len(scannedStrict) > 0 {
			if err := evidenceStore.UpsertLogEvidenceBatch(ctx, scannedStrict); err != nil {
				return nil, "", fmt.Errorf("persist strict evidence: %w", err)
			}
			return SortAndTrim(scannedStrict, options.BucketTS, options.Limit), "", nil
		}
	}

	fallbackFiles, err := ListLogFiles(options.LogDir, options.FileNameMatcher, fallbackStart, fallbackEnd, options.MaxScanFilesFallback)
	if err != nil {
		return nil, "", err
	}
	if len(fallbackFiles) == 0 {
		return nil, "", nil
	}

	scannedFallback, err := ScanFiles(scanCtx, options.Source, fallbackFiles, options.Parser, options.Matcher, fallbackStart, fallbackEnd, options.BucketTS, options.Limit*3, options.MaxScanLinesPerFile)
	if isBudgetTimeout(ctx, scanCtx, err) {
		if len(scannedFallback) > 0 {
			if persistErr := evidenceStore.UpsertLogEvidenceBatch(ctx, scannedFallback); persistErr != nil {
				return nil, "", fmt.Errorf("persist partial fallback evidence: %w", persistErr)
			}
			return SortAndTrim(scannedFallback, options.BucketTS, options.Limit), formatScanBudgetNote(options.ScanBudget, true), nil
		}
		return nil, formatScanBudgetNote(options.ScanBudget, false), nil
	}
	if err != nil {
		return nil, "", err
	}
	if len(scannedFallback) > 0 {
		if err := evidenceStore.UpsertLogEvidenceBatch(ctx, scannedFallback); err != nil {
			return nil, "", fmt.Errorf("persist fallback evidence: %w", err)
		}
		return SortAndTrim(scannedFallback, options.BucketTS, options.Limit), "在 ±2 分钟窗口未命中，已回退到 ±15 分钟窗口匹配日志。", nil
	}

	return nil, "", nil
}

func PrefetchWindow(ctx context.Context, evidenceStore Store, options PrefetchOptions) (PrefetchResult, error) {
	if strings.TrimSpace(options.Source) == "" || strings.TrimSpace(options.LogDir) == "" {
		return PrefetchResult{}, nil
	}
	if options.EndTS < options.StartTS {
		options.StartTS, options.EndTS = options.EndTS, options.StartTS
	}
	if options.ReferenceTS <= 0 {
		options.ReferenceTS = options.EndTS
	}

	files, err := ListLogFiles(options.LogDir, options.FileNameMatcher, options.StartTS, options.EndTS, options.MaxScanFiles)
	if err != nil {
		return PrefetchResult{}, err
	}
	result := PrefetchResult{FilesConsidered: len(files)}
	if len(files) == 0 {
		return result, nil
	}

	scanCtx, cancel := deriveScanContext(ctx, options.ScanBudget)
	defer cancel()

	rows, err := ScanFiles(
		scanCtx,
		options.Source,
		files,
		options.Parser,
		options.Matcher,
		options.StartTS,
		options.EndTS,
		options.ReferenceTS,
		0,
		options.MaxScanLinesPerFile,
	)
	if isBudgetTimeout(ctx, scanCtx, err) {
		result.Partial = true
		result.Note = formatScanBudgetNote(options.ScanBudget, len(rows) > 0)
		if len(rows) == 0 {
			return result, nil
		}
		if persistErr := evidenceStore.UpsertLogEvidenceBatch(ctx, rows); persistErr != nil {
			return PrefetchResult{}, fmt.Errorf("persist partial prefetch evidence: %w", persistErr)
		}
		result.RowsImported = len(rows)
		return result, nil
	}
	if err != nil {
		return PrefetchResult{}, err
	}
	if len(rows) == 0 {
		return result, nil
	}
	if err := evidenceStore.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		return PrefetchResult{}, fmt.Errorf("persist prefetch evidence: %w", err)
	}
	result.RowsImported = len(rows)
	return result, nil
}

func ScanLines(
	ctx context.Context,
	source string,
	lines []string,
	parser Parser,
	matcher Matcher,
	startTS int64,
	endTS int64,
	referenceTS int64,
	limit int,
) ([]model.LogEvidence, error) {
	if len(lines) == 0 {
		return nil, nil
	}
	initialCapacity := len(lines)
	if limit > 0 && limit < initialCapacity {
		initialCapacity = limit
	}
	collector := newLineEvidenceCollector(source, parser, matcher, startTS, endTS, referenceTS, limit, initialCapacity)
	collector.StartStream()
	for _, line := range lines {
		if ctx.Err() != nil {
			return collector.Rows(), ctx.Err()
		}
		if collector.AddLine(line) {
			break
		}
	}
	return collector.Rows(), nil
}

func queryCachedEvidence(ctx context.Context, evidenceStore Store, source string, startTS int64, endTS int64, hints QueryHints, limit int) ([]model.LogEvidence, error) {
	return evidenceStore.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source:         source,
		StartTS:        startTS,
		EndTS:          endTS,
		ClientIP:       hints.ClientIP,
		TargetIP:       hints.TargetIP,
		AnyIP:          hints.AnyIP,
		HostNormalized: hints.HostNormalized,
		EntryPort:      hints.EntryPort,
		TargetPort:     hints.TargetPort,
		Limit:          limit,
	})
}

func CacheEvidenceLimit(limit int) int {
	if limit <= 0 {
		return 200
	}
	computed := limit * 10
	if computed < limit+64 {
		computed = limit + 64
	}
	if computed > 2000 {
		return 2000
	}
	return computed
}

func ResolvedLogDir(value string, fallback string) string {
	resolved := strings.TrimSpace(value)
	if resolved == "" {
		return fallback
	}
	return resolved
}

func ListLogFiles(logDir string, nameMatcher func(string) bool, startTS int64, endTS int64, maxCandidates int) ([]LogFileCandidate, error) {
	logPathSpec := ResolvedLogDir(logDir, "")
	if logPathSpec == "" {
		return nil, nil
	}
	pathSpecHasGlob := hasGlobMeta(logPathSpec)
	paths, err := resolveLogPaths(logPathSpec)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}

	files := make([]LogFileCandidate, 0, len(paths)*4)
	seen := make(map[string]struct{}, len(paths)*4)
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("读取日志路径失败：%s（%w）", path, err)
		}
		if info.IsDir() {
			entries, err := os.ReadDir(path)
			if err != nil {
				return nil, fmt.Errorf("读取日志目录失败：%s（%w）", path, err)
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				lower := strings.ToLower(entry.Name())
				if nameMatcher != nil && !nameMatcher(lower) {
					continue
				}
				entryInfo, err := entry.Info()
				if err != nil {
					continue
				}
				if !IsLikelyFileInTimeRange(lower, entryInfo.ModTime(), startTS, endTS) {
					continue
				}
				candidatePath := filepath.Join(path, entry.Name())
				if _, ok := seen[candidatePath]; ok {
					continue
				}
				seen[candidatePath] = struct{}{}
				files = append(files, LogFileCandidate{
					Name:    entry.Name(),
					Path:    candidatePath,
					ModTime: entryInfo.ModTime(),
				})
			}
			continue
		}

		name := filepath.Base(path)
		lower := strings.ToLower(name)
		// Keep explicit glob matches usable even when file names are custom.
		if !pathSpecHasGlob && nameMatcher != nil && !nameMatcher(lower) {
			continue
		}
		if !IsLikelyFileInTimeRange(lower, info.ModTime(), startTS, endTS) {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		files = append(files, LogFileCandidate{
			Name:    name,
			Path:    path,
			ModTime: info.ModTime(),
		})
	}

	if len(files) == 0 {
		return nil, nil
	}

	centerTS := (startTS + endTS) / 2
	sort.Slice(files, func(i, j int) bool {
		di := AbsInt64(files[i].ModTime.Unix() - centerTS)
		dj := AbsInt64(files[j].ModTime.Unix() - centerTS)
		if di == dj {
			if files[i].ModTime.Equal(files[j].ModTime) {
				return files[i].Path > files[j].Path
			}
			return files[i].ModTime.After(files[j].ModTime)
		}
		return di < dj
	})
	if maxCandidates > 0 && len(files) > maxCandidates {
		files = files[:maxCandidates]
	}
	return files, nil
}

func resolveLogPaths(logPathSpec string) ([]string, error) {
	trimmed := strings.TrimSpace(logPathSpec)
	if trimmed == "" {
		return nil, nil
	}
	if !hasGlobMeta(trimmed) {
		return []string{filepath.Clean(trimmed)}, nil
	}
	matches, err := filepath.Glob(trimmed)
	if err != nil {
		return nil, fmt.Errorf("解析日志通配路径失败：%s（%w）", trimmed, err)
	}
	if len(matches) == 0 {
		return nil, nil
	}

	result := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		cleaned := filepath.Clean(match)
		if _, ok := seen[cleaned]; ok {
			continue
		}
		seen[cleaned] = struct{}{}
		result = append(result, cleaned)
	}
	return result, nil
}

func hasGlobMeta(path string) bool {
	return strings.ContainsAny(path, "*?[")
}

func IsLikelyFileInTimeRange(lowerName string, modTime time.Time, startTS int64, endTS int64) bool {
	if startTS <= 0 || endTS <= 0 {
		return true
	}
	if endTS < startTS {
		startTS, endTS = endTS, startTS
	}
	start := time.Unix(startTS, 0).UTC()
	end := time.Unix(endTS, 0).UTC()
	dayStart := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC).Add(-24 * time.Hour)
	dayEnd := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 0, time.UTC).Add(24 * time.Hour)

	dates := extractDateCandidatesFromFileName(lowerName)
	if len(dates) > 0 {
		for _, date := range dates {
			if !date.Before(dayStart) && !date.After(dayEnd) {
				return true
			}
		}
		return false
	}

	if modTime.IsZero() {
		return true
	}
	mod := modTime.UTC()
	if mod.Before(dayStart.Add(-24 * time.Hour)) {
		return false
	}
	if mod.After(dayEnd.Add(72 * time.Hour)) {
		return false
	}
	return true
}

func extractDateCandidatesFromFileName(lowerName string) []time.Time {
	matches := fileDatePattern.FindAllStringSubmatch(lowerName, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[int64]struct{})
	result := make([]time.Time, 0, len(matches))
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		year, errYear := strconv.Atoi(match[1])
		month, errMonth := strconv.Atoi(match[2])
		day, errDay := strconv.Atoi(match[3])
		if errYear != nil || errMonth != nil || errDay != nil {
			continue
		}
		candidate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		if candidate.Year() != year || int(candidate.Month()) != month || candidate.Day() != day {
			continue
		}
		unix := candidate.Unix()
		if _, exists := seen[unix]; exists {
			continue
		}
		seen[unix] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func ScanFiles(
	ctx context.Context,
	source string,
	files []LogFileCandidate,
	parser Parser,
	matcher Matcher,
	startTS int64,
	endTS int64,
	referenceTS int64,
	limit int,
	maxLinesPerFile int,
) ([]model.LogEvidence, error) {
	if len(files) == 0 {
		return nil, nil
	}
	collector := newLineEvidenceCollector(source, parser, matcher, startTS, endTS, referenceTS, limit, 0)

	for _, file := range files {
		if ctx.Err() != nil {
			return collector.Rows(), ctx.Err()
		}
		if shouldTailScanPlainFile(file.Path, maxLinesPerFile) {
			plainFile, err := os.Open(file.Path)
			if err != nil {
				continue
			}
			err = scanPlainFileTail(ctx, plainFile, collector, maxLinesPerFile)
			_ = plainFile.Close()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return collector.Rows(), err
				}
				return nil, fmt.Errorf("scan evidence file %s: %w", file.Path, err)
			}
			if collector.Full() {
				break
			}
			continue
		}

		reader, err := openMaybeGzip(file.Path)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		buffer := make([]byte, 0, 64*1024)
		scanner.Buffer(buffer, 1024*1024)
		linesRead := 0
		collector.StartStream()
		for scanner.Scan() {
			if ctx.Err() != nil {
				_ = reader.Close()
				return collector.Rows(), ctx.Err()
			}
			linesRead++
			if maxLinesPerFile > 0 && linesRead > maxLinesPerFile {
				break
			}
			if collector.AddLine(scanner.Text()) {
				break
			}
		}
		if err := scanner.Err(); err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("scan evidence file %s: %w", file.Path, err)
		}
		_ = reader.Close()
		if collector.Full() {
			break
		}
	}
	return collector.Rows(), nil
}

func shouldTailScanPlainFile(path string, maxLinesPerFile int) bool {
	return maxLinesPerFile > 0 && !strings.HasSuffix(strings.ToLower(path), ".gz")
}

func scanPlainFileTail(ctx context.Context, file *os.File, collector *lineEvidenceCollector, maxLinesPerFile int) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	remaining := info.Size()
	if remaining <= 0 {
		return nil
	}

	leftover := make([]byte, 0, tailScanChunkSize)
	linesRead := 0

	for remaining > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		chunkSize := int64(tailScanChunkSize)
		if remaining < chunkSize {
			chunkSize = remaining
		}
		remaining -= chunkSize

		if _, err := file.Seek(remaining, io.SeekStart); err != nil {
			return err
		}

		chunk := make([]byte, chunkSize)
		if _, err := io.ReadFull(file, chunk); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return err
		}

		segment := append(append(make([]byte, 0, len(chunk)+len(leftover)), chunk...), leftover...)
		parts := bytes.Split(segment, []byte{'\n'})
		startIdx := 0
		if remaining > 0 {
			leftover = append(leftover[:0], parts[0]...)
			startIdx = 1
		} else {
			leftover = leftover[:0]
		}

		for i := len(parts) - 1; i >= startIdx; i-- {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			line := strings.TrimRight(string(parts[i]), "\r")
			if strings.TrimSpace(line) == "" {
				continue
			}
			linesRead++
			if collector.AddLineReverse(line) {
				return nil
			}
			if maxLinesPerFile > 0 && linesRead >= maxLinesPerFile {
				return nil
			}
		}
	}

	return nil
}

func deriveScanContext(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if budget <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, budget)
}

func isBudgetTimeout(parentCtx context.Context, scanCtx context.Context, err error) bool {
	return err != nil && errors.Is(err, context.DeadlineExceeded) && parentCtx.Err() == nil && scanCtx.Err() != nil
}

func formatScanBudgetNote(budget time.Duration, partial bool) string {
	if budget <= 0 {
		if partial {
			return "日志深度扫描超时，已返回部分候选。"
		}
		return "日志深度扫描超时，已跳过文件扫描。"
	}
	if partial {
		return fmt.Sprintf("日志深度扫描超过 %s，已返回部分候选。", budget.Truncate(100*time.Millisecond))
	}
	return fmt.Sprintf("日志深度扫描超过 %s，已跳过文件扫描。", budget.Truncate(100*time.Millisecond))
}

func Filter(rows []model.LogEvidence, matcher Matcher) []model.LogEvidence {
	if matcher == nil || len(rows) == 0 {
		return rows
	}
	filtered := make([]model.LogEvidence, 0, len(rows))
	for _, row := range rows {
		if matcher(row) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func SortAndTrim(rows []model.LogEvidence, bucketTS int64, limit int) []model.LogEvidence {
	if len(rows) == 0 {
		return rows
	}
	sort.Slice(rows, func(i, j int) bool {
		di := AbsInt64(rows[i].EventTS - bucketTS)
		dj := AbsInt64(rows[j].EventTS - bucketTS)
		if di == dj {
			return rows[i].EventTS > rows[j].EventTS
		}
		return di < dj
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func Normalize(row model.LogEvidence) model.LogEvidence {
	row.HostNormalized = NormalizeHost(row.Host)
	if row.TargetPort <= 0 {
		row.TargetPort, _ = ParsePort(row.Path)
	}
	if row.EntryPort < 0 {
		row.EntryPort = 0
	}
	return row
}

func NormalizeHost(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func ParsePort(value string) (int, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	port, err := strconv.Atoi(trimmed)
	if err != nil || port <= 0 {
		return 0, false
	}
	return port, true
}

func Fingerprint(row model.LogEvidence) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf(
		"%s|%d|%s|%s|%s|%s|%s|%d|%d|%v|%s",
		row.Source,
		row.EventTS,
		row.ClientIP,
		row.TargetIP,
		row.HostNormalized,
		row.Path,
		row.Method,
		row.EntryPort,
		row.TargetPort,
		statusValue(row.Status),
		row.Message,
	)))
	return fmt.Sprintf("%x", h.Sum64())
}

func openMaybeGzip(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(strings.ToLower(path), ".gz") {
		return file, nil
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &stackedCloser{Reader: reader, closers: []io.Closer{reader, file}}, nil
}

type stackedCloser struct {
	io.Reader
	closers []io.Closer
}

func (s *stackedCloser) Close() error {
	var firstErr error
	for _, closer := range s.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func statusValue(status *int) int {
	if status == nil {
		return -1
	}
	return *status
}

func AbsInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
