package evidence

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

var fileDatePattern = regexp.MustCompile(`(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)`)

const (
	tailScanChunkSize       = 64 * 1024
	maxPlainLogLineBytes    = 1 << 20
	defaultMaxLogCandidates = 256
	maxLogDirectoryEntries  = 100_000
)

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
	// ScanContext may carry a shared deadline across multiple log directories.
	// PersistContext remains the request context so partial rows can still be
	// committed after the scan budget itself expires.
	ScanContext    context.Context
	PersistContext context.Context
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
	CursorLookup        PrefetchCursorLookup
	CursorCommit        PrefetchCursorCommit
	ScanContext         context.Context
	PersistContext      context.Context
}

type PrefetchResult struct {
	FilesConsidered int
	RowsImported    int
	Partial         bool
	Note            string
}

type PrefetchCursor struct {
	Identity string
	Offset   int64
	// LineOffset is used only for compressed files, where a byte offset cannot
	// be resumed safely. It records how many decompressed lines were completely
	// processed before a max-lines or future-row boundary.
	LineOffset int64
	Incomplete bool
	HeadHash   uint64
	HeadBytes  int
	TailHash   uint64
	TailBytes  int
}

type PrefetchCursorLookup func(source string, path string) (PrefetchCursor, bool)

// PrefetchCursorCommit receives a cursor progress proposal. It may point to a
// max-lines boundary rather than EOF; callers must not publish the proposal
// until all rows returned with that progress have persisted successfully.
type PrefetchCursorCommit func(source string, path string, cursor PrefetchCursor)

type prefetchCursorTarget struct {
	source string
	path   string
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
	stop, _ := c.addLine(line, false)
	return stop
}

func (c *lineEvidenceCollector) AddLineForPrefetch(line string) (stop bool, future bool) {
	return c.addLine(line, true)
}

func (c *lineEvidenceCollector) addLine(line string, stopAtFuture bool) (stop bool, future bool) {
	row, ok := c.parser(c.source, line, c.reference)
	if !ok {
		return false, false
	}
	row = Normalize(row)
	if row.EventTS > c.endTS {
		if stopAtFuture {
			return true, true
		}
		c.overEndStreak++
		return c.overEndStreak >= 200, true
	}
	c.overEndStreak = 0
	if row.EventTS < c.startTS || row.EventTS > c.endTS {
		return false, false
	}
	if c.matcher != nil && !c.matcher(row) {
		return false, false
	}
	if row.Fingerprint == "" {
		row.Fingerprint = Fingerprint(row)
	}
	if _, exists := c.seen[row.Fingerprint]; exists {
		return false, false
	}
	c.seen[row.Fingerprint] = struct{}{}
	c.collected = append(c.collected, row)
	return c.limit > 0 && len(c.collected) >= c.limit, false
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

	scanParent := options.ScanContext
	if scanParent == nil {
		scanParent = ctx
	}
	persistCtx := options.PersistContext
	if persistCtx == nil {
		persistCtx = ctx
	}
	scanCtx, cancel := deriveScanContext(scanParent, options.ScanBudget)
	defer cancel()
	strictFiles, err := ListLogFilesContext(scanCtx, options.LogDir, options.FileNameMatcher, strictStart, strictEnd, options.MaxScanFilesStrict)
	if err != nil {
		if isBudgetTimeout(ctx, scanCtx, err) {
			return nil, formatScanBudgetNote(options.ScanBudget, false), nil
		}
		return nil, "", err
	}
	if len(strictFiles) > 0 {
		scannedStrict, err := ScanFiles(scanCtx, options.Source, strictFiles, options.Parser, options.Matcher, strictStart, strictEnd, options.BucketTS, options.Limit*3, options.MaxScanLinesPerFile)
		if isBudgetTimeout(ctx, scanCtx, err) {
			if len(scannedStrict) > 0 {
				if persistErr := evidenceStore.UpsertLogEvidenceBatch(persistCtx, scannedStrict); persistErr != nil {
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
			if err := evidenceStore.UpsertLogEvidenceBatch(persistCtx, scannedStrict); err != nil {
				return nil, "", fmt.Errorf("persist strict evidence: %w", err)
			}
			return SortAndTrim(scannedStrict, options.BucketTS, options.Limit), "", nil
		}
	}

	fallbackFiles, err := ListLogFilesContext(scanCtx, options.LogDir, options.FileNameMatcher, fallbackStart, fallbackEnd, options.MaxScanFilesFallback)
	if err != nil {
		if isBudgetTimeout(ctx, scanCtx, err) {
			return nil, formatScanBudgetNote(options.ScanBudget, false), nil
		}
		return nil, "", err
	}
	if len(fallbackFiles) == 0 {
		return nil, "", nil
	}

	scannedFallback, err := ScanFiles(scanCtx, options.Source, fallbackFiles, options.Parser, options.Matcher, fallbackStart, fallbackEnd, options.BucketTS, options.Limit*3, options.MaxScanLinesPerFile)
	if isBudgetTimeout(ctx, scanCtx, err) {
		if len(scannedFallback) > 0 {
			if persistErr := evidenceStore.UpsertLogEvidenceBatch(persistCtx, scannedFallback); persistErr != nil {
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
		if err := evidenceStore.UpsertLogEvidenceBatch(persistCtx, scannedFallback); err != nil {
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

	scanParent := options.ScanContext
	if scanParent == nil {
		scanParent = ctx
	}
	persistCtx := options.PersistContext
	if persistCtx == nil {
		persistCtx = ctx
	}
	scanCtx, cancel := deriveScanContext(scanParent, options.ScanBudget)
	defer cancel()
	files, err := ListLogFilesContext(scanCtx, options.LogDir, options.FileNameMatcher, options.StartTS, options.EndTS, options.MaxScanFiles)
	if err != nil {
		if isBudgetTimeout(ctx, scanCtx, err) {
			return PrefetchResult{Partial: true, Note: formatScanBudgetNote(options.ScanBudget, false)}, nil
		}
		return PrefetchResult{}, err
	}
	result := PrefetchResult{FilesConsidered: len(files)}
	if len(files) == 0 {
		return result, nil
	}

	var pendingOrder []prefetchCursorTarget
	pendingCursors := make(map[prefetchCursorTarget]PrefetchCursor)
	cursorCommit := options.CursorCommit
	if options.CursorLookup != nil && options.CursorCommit != nil {
		cursorCommit = func(source string, path string, cursor PrefetchCursor) {
			target := prefetchCursorTarget{source: source, path: path}
			if _, exists := pendingCursors[target]; !exists {
				pendingOrder = append(pendingOrder, target)
			}
			pendingCursors[target] = cursor
		}
	}
	commitPendingCursors := func() {
		if options.CursorCommit == nil {
			return
		}
		for _, target := range pendingOrder {
			options.CursorCommit(target.source, target.path, pendingCursors[target])
		}
	}

	rows, truncated, err := ScanPrefetchFiles(
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
		options.CursorLookup,
		cursorCommit,
	)
	if isBudgetTimeout(ctx, scanCtx, err) {
		result.Partial = true
		result.Note = formatScanBudgetNote(options.ScanBudget, len(rows) > 0)
		if len(rows) == 0 {
			return result, nil
		}
		if persistErr := PersistLogEvidenceBatches(persistCtx, evidenceStore, rows); persistErr != nil {
			return PrefetchResult{}, fmt.Errorf("persist partial prefetch evidence: %w", persistErr)
		}
		commitPendingCursors()
		result.RowsImported = len(rows)
		return result, nil
	}
	if err != nil {
		return PrefetchResult{}, err
	}
	if truncated {
		result.Partial = true
		result.Note = "日志文件仍有未扫描行，将在下一轮继续导入。"
	}
	if len(rows) == 0 {
		commitPendingCursors()
		return result, nil
	}
	if err := PersistLogEvidenceBatches(persistCtx, evidenceStore, rows); err != nil {
		return PrefetchResult{}, fmt.Errorf("persist prefetch evidence: %w", err)
	}
	commitPendingCursors()
	result.RowsImported = len(rows)
	return result, nil
}

const prefetchEvidencePersistBatchSize = 500

// PersistLogEvidenceBatches bounds each SQLite write transaction while keeping
// cursor publication under the caller's control. Callers may safely retry all
// rows after an intermediate batch failure because fingerprints are upserted.
func PersistLogEvidenceBatches(ctx context.Context, evidenceStore Store, rows []model.LogEvidence) error {
	for start := 0; start < len(rows); start += prefetchEvidencePersistBatchSize {
		end := start + prefetchEvidencePersistBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := evidenceStore.UpsertLogEvidenceBatch(ctx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
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

func ScanPrefetchFiles(
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
	cursorLookup PrefetchCursorLookup,
	cursorCommit PrefetchCursorCommit,
) ([]model.LogEvidence, bool, error) {
	if cursorLookup == nil || cursorCommit == nil {
		rows, err := ScanFiles(ctx, source, files, parser, matcher, startTS, endTS, referenceTS, limit, maxLinesPerFile)
		return rows, false, err
	}
	if len(files) == 0 {
		return nil, false, nil
	}
	collector := newLineEvidenceCollector(source, parser, matcher, startTS, endTS, referenceTS, limit, 0)
	truncated := false

	for _, candidate := range files {
		if ctx.Err() != nil {
			return collector.Rows(), truncated, ctx.Err()
		}
		file, err := openRegularNoFollow(candidate.Path)
		if err != nil {
			continue
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			continue
		}
		identity := fileCursorIdentity(candidate.Path, info)
		previous, hasPrevious := cursorLookup(source, candidate.Path)
		sameFile := hasPrevious && previous.Identity == identity && !prefetchFileContentChanged(file, previous, info.Size())

		if shouldTailScanPlainFile(candidate.Path, maxLinesPerFile) {
			completeOffset, err := lastCompleteLineOffset(file, info.Size())
			if err != nil {
				_ = file.Close()
				continue
			}
			if sameFile && previous.Offset == completeOffset {
				cursorCommit(source, candidate.Path, buildPrefetchCursor(file, identity, completeOffset))
				_ = file.Close()
				continue
			}
			startOffset := int64(0)
			if sameFile {
				startOffset = previous.Offset
				if startOffset < 0 || startOffset > completeOffset {
					startOffset = 0
				}
			}
			nextOffset, hitLimit, err := scanPlainFileForwardFromOffset(ctx, file, collector, startOffset, maxLinesPerFile)
			if err != nil {
				_ = file.Close()
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return collector.Rows(), truncated, err
				}
				return nil, truncated, fmt.Errorf("scan evidence file %s: %w", candidate.Path, err)
			}
			truncated = truncated || (hitLimit && nextOffset < completeOffset)
			cursorCommit(source, candidate.Path, buildPrefetchCursor(file, identity, nextOffset))
			_ = file.Close()
			if collector.Full() {
				break
			}
			continue
		}
		cursor := buildPrefetchCursor(file, identity, info.Size())
		if sameFile && previous.Offset == info.Size() && !previous.Incomplete {
			cursorCommit(source, candidate.Path, cursor)
			_ = file.Close()
			continue
		}
		startLine := int64(0)
		if sameFile && previous.Incomplete {
			startLine = previous.LineOffset
		}
		_ = file.Close()

		reader, err := openMaybeGzip(candidate.Path)
		if err != nil {
			continue
		}
		nextLine, fileTruncated, fileComplete, err := scanReaderForwardFromLine(ctx, reader, collector, startLine, maxLinesPerFile)
		if err != nil {
			_ = reader.Close()
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return collector.Rows(), truncated, err
			}
			return nil, truncated, fmt.Errorf("scan evidence file %s: %w", candidate.Path, err)
		}
		_ = reader.Close()
		if !fileComplete {
			cursor.LineOffset = nextLine
			cursor.Incomplete = true
			cursorCommit(source, candidate.Path, cursor)
		}
		if fileTruncated {
			truncated = true
		} else if fileComplete {
			cursorCommit(source, candidate.Path, cursor)
		}
		if collector.Full() {
			break
		}
	}
	return collector.Rows(), truncated, nil
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
	return ListLogFilesContext(context.Background(), logDir, nameMatcher, startTS, endTS, maxCandidates)
}

func ListLogFilesContext(ctx context.Context, logDir string, nameMatcher func(string) bool, startTS int64, endTS int64, maxCandidates int) ([]LogFileCandidate, error) {
	logPathSpec := ResolvedLogDir(logDir, "")
	if logPathSpec == "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pathSpecHasGlob := hasGlobMeta(logPathSpec)
	configuredRoot, err := configuredLogRoot(logPathSpec)
	if err != nil {
		return nil, err
	}
	centerTS := (startTS + endTS) / 2
	if maxCandidates <= 0 {
		maxCandidates = defaultMaxLogCandidates
	}
	candidates := newLogFileCandidateTopK(centerTS, maxCandidates)
	directoryEntriesVisited := 0

	addCandidate := func(path string, info os.FileInfo, applyNameMatcher bool) {
		name := filepath.Base(path)
		lower := strings.ToLower(name)
		if applyNameMatcher && nameMatcher != nil && !nameMatcher(lower) {
			return
		}
		if !IsLikelyFileInTimeRange(lower, info.ModTime(), startTS, endTS) {
			return
		}
		if !pathWithinConfiguredRoot(configuredRoot, path) {
			return
		}
		candidates.Add(LogFileCandidate{Name: name, Path: path, ModTime: info.ModTime()})
	}

	processResolvedPath := func(path string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("读取日志路径失败：%s（%w）", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("拒绝符号链接日志路径：%s", path)
		}
		if info.IsDir() {
			return enumerateLogDirectory(ctx, path, &directoryEntriesVisited, func(candidatePath string, candidateInfo os.FileInfo) {
				addCandidate(candidatePath, candidateInfo, true)
			})
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("拒绝非普通日志文件：%s", path)
		}
		if !pathWithinConfiguredRoot(configuredRoot, path) {
			return fmt.Errorf("日志文件超出配置根目录：%s", path)
		}
		// Keep explicit glob matches usable even when file names are custom.
		addCandidate(path, info, !pathSpecHasGlob)
		return nil
	}

	if err := forEachResolvedLogPath(ctx, logPathSpec, processResolvedPath); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return candidates.Sorted(), nil
}

type logFileCandidateHeap struct {
	centerTS int64
	items    []LogFileCandidate
}

func (h logFileCandidateHeap) Len() int { return len(h.items) }

func (h logFileCandidateHeap) Less(i int, j int) bool {
	// container/heap keeps the "least" element at the root. Define the least
	// element as the worst candidate so replacement remains O(log K).
	return logFileCandidateBetter(h.items[j], h.items[i], h.centerTS)
}

func (h logFileCandidateHeap) Swap(i int, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *logFileCandidateHeap) Push(value any) {
	h.items = append(h.items, value.(LogFileCandidate))
}

func (h *logFileCandidateHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	h.items = h.items[:last]
	return value
}

type logFileCandidateTopK struct {
	limit int
	heap  logFileCandidateHeap
}

func newLogFileCandidateTopK(centerTS int64, limit int) *logFileCandidateTopK {
	initialCapacity := limit
	if initialCapacity > defaultMaxLogCandidates {
		initialCapacity = defaultMaxLogCandidates
	}
	return &logFileCandidateTopK{
		limit: limit,
		heap:  logFileCandidateHeap{centerTS: centerTS, items: make([]LogFileCandidate, 0, initialCapacity)},
	}
}

func (c *logFileCandidateTopK) Add(candidate LogFileCandidate) {
	if c.limit <= 0 {
		return
	}
	if c.heap.Len() < c.limit {
		heap.Push(&c.heap, candidate)
		return
	}
	if logFileCandidateBetter(candidate, c.heap.items[0], c.heap.centerTS) {
		c.heap.items[0] = candidate
		heap.Fix(&c.heap, 0)
	}
}

func (c *logFileCandidateTopK) Sorted() []LogFileCandidate {
	if c.heap.Len() == 0 {
		return nil
	}
	result := append([]LogFileCandidate(nil), c.heap.items...)
	sort.Slice(result, func(i int, j int) bool {
		return logFileCandidateBetter(result[i], result[j], c.heap.centerTS)
	})
	return result
}

func logFileCandidateBetter(left LogFileCandidate, right LogFileCandidate, centerTS int64) bool {
	leftDistance := AbsInt64(left.ModTime.Unix() - centerTS)
	rightDistance := AbsInt64(right.ModTime.Unix() - centerTS)
	if leftDistance != rightDistance {
		return leftDistance < rightDistance
	}
	if !left.ModTime.Equal(right.ModTime) {
		return left.ModTime.After(right.ModTime)
	}
	return left.Path > right.Path
}

func enumerateLogDirectory(ctx context.Context, path string, entriesVisited *int, add func(string, os.FileInfo)) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("读取日志目录失败：%s（%w）", path, err)
	}
	defer directory.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := directory.Readdir(128)
		for _, entryInfo := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			(*entriesVisited)++
			if *entriesVisited > maxLogDirectoryEntries {
				return fmt.Errorf("日志目录枚举超过 %d 个目录项：%s", maxLogDirectoryEntries, path)
			}
			if entryInfo.IsDir() || entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.Mode().IsRegular() {
				continue
			}
			add(filepath.Join(path, entryInfo.Name()), entryInfo)
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("读取日志目录失败：%s（%w）", path, readErr)
		}
	}
}

func configuredLogRoot(pathSpec string) (string, error) {
	trimmed := filepath.Clean(strings.TrimSpace(pathSpec))
	if trimmed == "" {
		return "", errors.New("empty log path")
	}
	root := trimmed
	if index := strings.IndexAny(trimmed, "*?["); index >= 0 {
		root = filepath.Dir(trimmed[:index])
	} else if info, err := os.Lstat(trimmed); err == nil && !info.IsDir() {
		root = filepath.Dir(trimmed)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("解析日志根目录失败：%w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("解析日志根目录失败：%s（%w）", abs, err)
	}
	return filepath.Clean(resolved), nil
}

func pathWithinConfiguredRoot(root string, candidate string) bool {
	abs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, filepath.Clean(resolved))
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func forEachResolvedLogPath(ctx context.Context, logPathSpec string, visit func(string) error) error {
	trimmed := strings.TrimSpace(logPathSpec)
	if trimmed == "" {
		return nil
	}
	cleaned := filepath.Clean(trimmed)
	if !hasGlobMeta(cleaned) {
		return visit(cleaned)
	}

	firstMeta := strings.IndexAny(cleaned, "*?[")
	base := filepath.Dir(cleaned[:firstMeta])
	relativePattern, err := filepath.Rel(base, cleaned)
	if err != nil {
		return fmt.Errorf("解析日志通配路径失败：%s（%w）", cleaned, err)
	}
	components := strings.Split(relativePattern, string(filepath.Separator))
	for _, component := range components {
		if hasGlobMeta(component) {
			if _, err := filepath.Match(component, ""); err != nil {
				return fmt.Errorf("解析日志通配路径失败：%s（%w）", cleaned, err)
			}
		}
	}

	enumerator := globPathEnumerator{ctx: ctx, pattern: cleaned, visit: visit}
	return enumerator.walk(base, components, 0)
}

type globPathEnumerator struct {
	ctx            context.Context
	pattern        string
	entriesVisited int
	visit          func(string) error
}

func (e *globPathEnumerator) walk(current string, components []string, index int) error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	if index >= len(components) {
		if err := e.visit(filepath.Clean(current)); err != nil {
			return err
		}
		return e.ctx.Err()
	}

	component := components[index]
	if !hasGlobMeta(component) {
		next := filepath.Join(current, component)
		info, err := os.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("读取日志通配路径失败：%s（%w）", next, err)
		}
		if index+1 < len(components) {
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("拒绝符号链接日志路径：%s", next)
			}
			if !info.IsDir() {
				return nil
			}
		}
		return e.walk(next, components, index+1)
	}

	directory, err := os.Open(current)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("读取日志通配目录失败：%s（%w）", current, err)
	}
	defer directory.Close()
	for {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		entries, readErr := directory.Readdir(128)
		for _, entryInfo := range entries {
			if err := e.ctx.Err(); err != nil {
				return err
			}
			e.entriesVisited++
			if e.entriesVisited > maxLogDirectoryEntries {
				return fmt.Errorf("日志通配路径枚举超过 %d 个目录项：%s", maxLogDirectoryEntries, e.pattern)
			}
			matched, matchErr := filepath.Match(component, entryInfo.Name())
			if matchErr != nil {
				return fmt.Errorf("解析日志通配路径失败：%s（%w）", e.pattern, matchErr)
			}
			if !matched {
				continue
			}
			if index+1 < len(components) {
				if entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.IsDir() {
					continue
				}
			}
			if err := e.walk(filepath.Join(current, entryInfo.Name()), components, index+1); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("读取日志通配目录失败：%s（%w）", current, readErr)
		}
	}
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

	if isActiveUndatedLogFile(lowerName) {
		return true
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

func isActiveUndatedLogFile(lowerName string) bool {
	return strings.HasSuffix(strings.TrimSpace(lowerName), ".log")
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
			plainFile, err := openRegularNoFollow(file.Path)
			if err != nil {
				continue
			}
			_, err = scanPlainFileTail(ctx, plainFile, collector, maxLinesPerFile)
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

		_, err = scanReaderForward(ctx, reader, collector, maxLinesPerFile)
		if err != nil {
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

func scanReaderForward(ctx context.Context, reader io.Reader, collector *lineEvidenceCollector, maxLinesPerFile int) (bool, error) {
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)
	linesRead := 0
	collector.StartStream()
	for scanner.Scan() {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		linesRead++
		if maxLinesPerFile > 0 && linesRead > maxLinesPerFile {
			return true, nil
		}
		if collector.AddLine(scanner.Text()) {
			return false, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func scanReaderForwardFromLine(
	ctx context.Context,
	reader io.Reader,
	collector *lineEvidenceCollector,
	startLine int64,
	maxLinesPerFile int,
) (nextLine int64, truncated bool, complete bool, err error) {
	if startLine < 0 {
		startLine = 0
	}
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 1024*1024)
	lineNumber := int64(0)
	linesRead := 0
	collector.StartStream()
	for scanner.Scan() {
		if ctx.Err() != nil {
			return startLine + int64(linesRead), false, false, ctx.Err()
		}
		lineNumber++
		if lineNumber <= startLine {
			continue
		}
		if maxLinesPerFile > 0 && linesRead >= maxLinesPerFile {
			return startLine + int64(linesRead), true, false, nil
		}
		stop, future := collector.AddLineForPrefetch(scanner.Text())
		if future {
			return startLine + int64(linesRead), false, false, nil
		}
		linesRead++
		if stop {
			return startLine + int64(linesRead), true, false, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return startLine + int64(linesRead), false, false, err
	}
	return startLine + int64(linesRead), false, true, nil
}

func shouldTailScanPlainFile(path string, maxLinesPerFile int) bool {
	return maxLinesPerFile > 0 && !strings.HasSuffix(strings.ToLower(path), ".gz")
}

func scanPlainFileForwardFromOffset(ctx context.Context, file *os.File, collector *lineEvidenceCollector, offset int64, maxLinesPerFile int) (int64, bool, error) {
	if offset < 0 {
		offset = 0
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return offset, false, err
	}
	reader := bufio.NewReaderSize(file, 64*1024)
	linesRead := 0
	currentOffset := offset
	lineStartOffset := offset
	lineBuffer := make([]byte, 0, 64*1024)
	lineOversized := false
	collector.StartStream()
	for {
		if ctx.Err() != nil {
			return currentOffset, false, ctx.Err()
		}
		fragment, err := reader.ReadSlice('\n')
		currentOffset += int64(len(fragment))
		if !lineOversized {
			if len(lineBuffer)+len(fragment) > maxPlainLogLineBytes {
				lineBuffer = lineBuffer[:0]
				lineOversized = true
			} else {
				lineBuffer = append(lineBuffer, fragment...)
			}
		}
		if err == nil {
			if !lineOversized {
				trimmed := strings.TrimRight(string(lineBuffer), "\r\n")
				if strings.TrimSpace(trimmed) != "" {
					stop, future := collector.AddLineForPrefetch(trimmed)
					if future {
						return lineStartOffset, false, nil
					}
					if stop {
						return currentOffset, true, nil
					}
				}
			}
			linesRead++
			if maxLinesPerFile > 0 && linesRead >= maxLinesPerFile {
				return currentOffset, true, nil
			}
			lineBuffer = lineBuffer[:0]
			lineOversized = false
			lineStartOffset = currentOffset
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			// Keep the cursor at the beginning of an unterminated fragment.
			// The next append will then present the complete line exactly once.
			return lineStartOffset, false, nil
		}
		return lineStartOffset, false, err
	}
}

func scanPlainFileTail(ctx context.Context, file *os.File, collector *lineEvidenceCollector, maxLinesPerFile int) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	completeOffset, err := lastCompleteLineOffset(file, info.Size())
	if err != nil {
		return 0, err
	}
	remaining := completeOffset
	if remaining <= 0 {
		return 0, nil
	}

	leftover := make([]byte, 0, tailScanChunkSize)
	skippingOversizedLine := false
	linesRead := 0

	for remaining > 0 {
		if ctx.Err() != nil {
			return completeOffset, ctx.Err()
		}

		chunkSize := int64(tailScanChunkSize)
		if remaining < chunkSize {
			chunkSize = remaining
		}
		remaining -= chunkSize

		if _, err := file.Seek(remaining, io.SeekStart); err != nil {
			return completeOffset, err
		}

		chunk := make([]byte, chunkSize)
		if _, err := io.ReadFull(file, chunk); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return completeOffset, err
		}

		segment := append(append(make([]byte, 0, len(chunk)+len(leftover)), chunk...), leftover...)
		parts := bytes.Split(segment, []byte{'\n'})
		startIdx := 0
		endIdx := len(parts) - 1
		if skippingOversizedLine {
			if len(parts) == 1 {
				continue
			}
			// The rightmost fragment completes the oversized line whose suffix
			// was deliberately discarded on the previous reverse chunk.
			endIdx--
			skippingOversizedLine = false
		}
		if remaining > 0 {
			if len(parts[0]) > maxPlainLogLineBytes {
				leftover = leftover[:0]
				skippingOversizedLine = true
			} else {
				leftover = append(leftover[:0], parts[0]...)
			}
			startIdx = 1
		} else {
			leftover = leftover[:0]
		}

		for i := endIdx; i >= startIdx; i-- {
			if ctx.Err() != nil {
				return completeOffset, ctx.Err()
			}
			if len(parts[i]) > maxPlainLogLineBytes {
				continue
			}
			line := strings.TrimRight(string(parts[i]), "\r")
			if strings.TrimSpace(line) == "" {
				continue
			}
			linesRead++
			if collector.AddLineReverse(line) {
				return completeOffset, nil
			}
			if maxLinesPerFile > 0 && linesRead >= maxLinesPerFile {
				return completeOffset, nil
			}
		}
	}

	return completeOffset, nil
}

func lastCompleteLineOffset(file *os.File, size int64) (int64, error) {
	if size <= 0 {
		return 0, nil
	}
	last := []byte{0}
	if _, err := file.ReadAt(last, size-1); err != nil {
		return 0, err
	}
	if last[0] == '\n' {
		return size, nil
	}
	buffer := make([]byte, 4096)
	position := size
	for position > 0 {
		chunk := int64(len(buffer))
		if position < chunk {
			chunk = position
		}
		position -= chunk
		read, err := file.ReadAt(buffer[:chunk], position)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if index := bytes.LastIndexByte(buffer[:read], '\n'); index >= 0 {
			return position + int64(index) + 1, nil
		}
	}
	return 0, nil
}

func fileCursorIdentity(path string, info os.FileInfo) string {
	if info != nil {
		if identity, ok := statDeviceInode(info.Sys()); ok {
			return identity
		}
	}
	return filepath.Clean(path)
}

const prefetchSignatureBytes = 256

func buildPrefetchCursor(file *os.File, identity string, offset int64) PrefetchCursor {
	if offset < 0 {
		offset = 0
	}
	cursor := PrefetchCursor{Identity: identity, Offset: offset}
	cursor.HeadBytes = int(minInt64(offset, prefetchSignatureBytes))
	if cursor.HeadBytes > 0 {
		cursor.HeadHash, _ = hashFileRegion(file, 0, cursor.HeadBytes)
	}
	cursor.TailBytes = int(minInt64(offset, prefetchSignatureBytes))
	if cursor.TailBytes > 0 {
		cursor.TailHash, _ = hashFileRegion(file, offset-int64(cursor.TailBytes), cursor.TailBytes)
	}
	return cursor
}

func prefetchFileContentChanged(file *os.File, previous PrefetchCursor, currentSize int64) bool {
	if previous.Offset < 0 || previous.Offset > currentSize {
		return true
	}
	if previous.HeadBytes > 0 {
		if int64(previous.HeadBytes) > currentSize {
			return true
		}
		hash, err := hashFileRegion(file, 0, previous.HeadBytes)
		if err != nil || hash != previous.HeadHash {
			return true
		}
	}
	if previous.TailBytes > 0 {
		if int64(previous.TailBytes) > previous.Offset {
			return true
		}
		hash, err := hashFileRegion(file, previous.Offset-int64(previous.TailBytes), previous.TailBytes)
		if err != nil || hash != previous.TailHash {
			return true
		}
	}
	return false
}

func hashFileRegion(file *os.File, offset int64, length int) (uint64, error) {
	if length <= 0 {
		return 0, nil
	}
	buffer := make([]byte, length)
	read, err := file.ReadAt(buffer, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if read != length {
		return 0, io.ErrUnexpectedEOF
	}
	hash := fnv.New64a()
	_, _ = hash.Write(buffer)
	return hash.Sum64(), nil
}

func minInt64(left int64, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func statDeviceInode(sys any) (string, bool) {
	if sys == nil {
		return "", false
	}
	value := reflect.ValueOf(sys)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", false
	}
	dev, okDev := numericStructField(value, "Dev")
	ino, okIno := numericStructField(value, "Ino")
	if !okDev || !okIno {
		return "", false
	}
	return fmt.Sprintf("%d:%d", dev, ino), true
}

func numericStructField(value reflect.Value, name string) (uint64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		raw := field.Int()
		if raw < 0 {
			return 0, false
		}
		return uint64(raw), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return field.Uint(), true
	default:
		return 0, false
	}
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
	file, err := openRegularNoFollow(path)
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
