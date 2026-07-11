package evidence

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type fakeEvidenceStore struct {
	upserted   []model.LogEvidence
	upsertErr  error
	batchSizes []int
	batchCalls int
	failBatch  int
}

func (f *fakeEvidenceStore) QueryLogEvidence(context.Context, store.LogEvidenceQuery) ([]model.LogEvidence, error) {
	return nil, nil
}

func (f *fakeEvidenceStore) UpsertLogEvidenceBatch(_ context.Context, rows []model.LogEvidence) error {
	f.batchCalls++
	f.batchSizes = append(f.batchSizes, len(rows))
	if f.upsertErr != nil {
		return f.upsertErr
	}
	if f.failBatch > 0 && f.batchCalls == f.failBatch {
		return errors.New("injected batch failure")
	}
	f.upserted = append(f.upserted, rows...)
	return nil
}

func TestLookupOrScanReturnsPartialRowsWhenScanBudgetExpires(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	lines := strings.Join([]string{
		"2026-04-18T12:00:00Z connect to chatgpt.com:443 #1",
		"2026-04-18T12:00:01Z connect to chatgpt.com:443 #2",
		"2026-04-18T12:00:02Z connect to chatgpt.com:443 #3",
		"2026-04-18T12:00:03Z connect to chatgpt.com:443 #4",
		"2026-04-18T12:00:04Z connect to chatgpt.com:443 #5",
		"2026-04-18T12:00:05Z connect to chatgpt.com:443 #6",
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(lines), 0o644); err != nil {
		t.Fatalf("write test log: %v", err)
	}

	store := &fakeEvidenceStore{}
	startTS := time.Date(2026, 4, 18, 11, 59, 0, 0, time.UTC).Unix()
	endTS := time.Date(2026, 4, 18, 12, 1, 0, 0, time.UTC).Unix()
	bucketTS := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC).Unix()

	parserCalls := 0
	parser := func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		parserCalls++
		if parserCalls > 1 {
			time.Sleep(15 * time.Millisecond)
		}
		return Normalize(model.LogEvidence{
			Source:  source,
			EventTS: bucketTS,
			Host:    "chatgpt.com",
			Path:    "443",
			Method:  "connect",
			Message: strings.TrimSpace(line),
		}), true
	}

	persistCtx := context.Background()
	sharedScanCtx, cancel := context.WithTimeout(persistCtx, 40*time.Millisecond)
	defer cancel()
	rows, note, err := LookupOrScan(persistCtx, store, SearchOptions{
		Source:               "ss",
		LogDir:               logPath,
		BucketTS:             bucketTS,
		Limit:                8,
		Parser:               parser,
		StrictWindow:         120,
		FallbackWindow:       900,
		ScanBudget:           40 * time.Millisecond,
		MaxScanFilesStrict:   1,
		MaxScanFilesFallback: 1,
		MaxScanLinesPerFile:  100,
		ScanContext:          sharedScanCtx,
		PersistContext:       persistCtx,
	})
	if err != nil {
		t.Fatalf("lookup or scan: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected partial evidence rows on timeout")
	}
	if len(store.upserted) == 0 {
		t.Fatalf("expected partial evidence rows to be persisted")
	}
	if !strings.Contains(note, "已返回部分候选") {
		t.Fatalf("expected partial timeout note, got %q", note)
	}
	if rows[0].EventTS < startTS || rows[0].EventTS > endTS {
		t.Fatalf("unexpected event ts in partial row: %+v", rows[0])
	}
}

func TestLookupOrScanCacheOnlySkipsFileScan(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	if err := os.WriteFile(logPath, []byte("2026-04-18T12:00:00Z connect to chatgpt.com:443\n"), 0o644); err != nil {
		t.Fatalf("write test log: %v", err)
	}

	store := &fakeEvidenceStore{}
	bucketTS := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC).Unix()
	parserCalls := 0

	rows, note, err := LookupOrScan(context.Background(), store, SearchOptions{
		Source:   "ss",
		LogDir:   logPath,
		BucketTS: bucketTS,
		Limit:    8,
		Parser: func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
			parserCalls++
			return Normalize(model.LogEvidence{
				Source:  source,
				EventTS: bucketTS,
				Host:    "chatgpt.com",
				Path:    "443",
				Method:  "connect",
				Message: strings.TrimSpace(line),
			}), true
		},
		StrictWindow:         120,
		FallbackWindow:       900,
		MaxScanFilesStrict:   1,
		MaxScanFilesFallback: 1,
		MaxScanLinesPerFile:  100,
		CacheOnly:            true,
	})
	if err != nil {
		t.Fatalf("lookup or scan cache-only: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows in cache-only mode without cached evidence, got %+v", rows)
	}
	if note != "" {
		t.Fatalf("expected empty note in cache-only mode, got %q", note)
	}
	if parserCalls != 0 {
		t.Fatalf("expected parser to never be called in cache-only mode, got %d", parserCalls)
	}
}

func TestPrefetchWindowPersistsScannedRows(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	content := strings.Join([]string{
		"2026-04-18T12:00:00Z connect to chatgpt.com:443",
		"2026-04-18T12:00:01Z connect to openai.com:443",
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write test log: %v", err)
	}

	store := &fakeEvidenceStore{}
	startTS := time.Date(2026, 4, 18, 11, 59, 0, 0, time.UTC).Unix()
	endTS := time.Date(2026, 4, 18, 12, 2, 0, 0, time.UTC).Unix()

	result, err := PrefetchWindow(context.Background(), store, PrefetchOptions{
		Source:  "ss",
		LogDir:  logPath,
		StartTS: startTS,
		EndTS:   endTS,
		Parser: func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				return model.LogEvidence{}, false
			}
			ts, parseErr := time.Parse(time.RFC3339, fields[0])
			if parseErr != nil {
				return model.LogEvidence{}, false
			}
			hostPort := strings.TrimSpace(fields[len(fields)-1])
			return Normalize(model.LogEvidence{
				Source:  source,
				EventTS: ts.Unix(),
				Host:    strings.TrimSuffix(hostPort, ":443"),
				Path:    "443",
				Method:  "connect",
				Message: strings.TrimSpace(line),
			}), true
		},
		MaxScanFiles:        2,
		MaxScanLinesPerFile: 100,
	})
	if err != nil {
		t.Fatalf("prefetch window: %v", err)
	}
	if result.FilesConsidered != 1 {
		t.Fatalf("expected 1 file considered, got %+v", result)
	}
	if result.RowsImported != 2 {
		t.Fatalf("expected 2 imported rows, got %+v", result)
	}
	if len(store.upserted) != 2 {
		t.Fatalf("expected 2 persisted rows, got %+v", store.upserted)
	}
}

func TestPrefetchWindowUsesCursorForAppendedPlainLogs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	initialContent := strings.Join([]string{
		"2026-04-18T12:00:00Z connect to chatgpt.com:443",
		"2026-04-18T12:01:00Z connect to openai.com:443",
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(initialContent), 0o644); err != nil {
		t.Fatalf("write test log: %v", err)
	}

	store := &fakeEvidenceStore{}
	cursors := make(map[string]PrefetchCursor)
	cursorKey := func(source string, path string) string {
		return source + "|" + path
	}
	lookupCursor := func(source string, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[cursorKey(source, path)]
		return cursor, ok
	}
	commitCursor := func(source string, path string, cursor PrefetchCursor) {
		cursors[cursorKey(source, path)] = cursor
	}
	parseLine := func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return model.LogEvidence{}, false
		}
		ts, parseErr := time.Parse(time.RFC3339, fields[0])
		if parseErr != nil {
			return model.LogEvidence{}, false
		}
		hostPort := strings.TrimSpace(fields[len(fields)-1])
		return Normalize(model.LogEvidence{
			Source:  source,
			EventTS: ts.Unix(),
			Host:    strings.TrimSuffix(hostPort, ":443"),
			Path:    "443",
			Method:  "connect",
			Message: strings.TrimSpace(line),
		}), true
	}
	options := PrefetchOptions{
		Source:              "ss",
		LogDir:              logPath,
		StartTS:             time.Date(2026, 4, 18, 11, 59, 0, 0, time.UTC).Unix(),
		EndTS:               time.Date(2026, 4, 18, 12, 4, 0, 0, time.UTC).Unix(),
		Parser:              parseLine,
		MaxScanFiles:        1,
		MaxScanLinesPerFile: 100,
		CursorLookup:        lookupCursor,
		CursorCommit:        commitCursor,
	}

	first, err := PrefetchWindow(context.Background(), store, options)
	if err != nil {
		t.Fatalf("first prefetch window: %v", err)
	}
	if first.RowsImported != 2 {
		t.Fatalf("expected first prefetch to import 2 rows, got %+v", first)
	}
	if len(store.upserted) != 2 {
		t.Fatalf("expected 2 upserted rows after first prefetch, got %+v", store.upserted)
	}

	appendContent := "2026-04-18T12:02:00Z connect to example.com:443\n"
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open test log for append: %v", err)
	}
	if _, err := file.WriteString(appendContent); err != nil {
		_ = file.Close()
		t.Fatalf("append test log: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close appended test log: %v", err)
	}

	second, err := PrefetchWindow(context.Background(), store, options)
	if err != nil {
		t.Fatalf("second prefetch window: %v", err)
	}
	if second.RowsImported != 1 {
		t.Fatalf("expected second prefetch to import only appended row, got %+v", second)
	}
	if got := store.upserted[len(store.upserted)-1].Host; got != "example.com" {
		t.Fatalf("expected appended host to be imported, got %q", got)
	}

	third, err := PrefetchWindow(context.Background(), store, options)
	if err != nil {
		t.Fatalf("third prefetch window: %v", err)
	}
	if third.RowsImported != 0 {
		t.Fatalf("expected third prefetch to skip unchanged file, got %+v", third)
	}
}

func TestPrefetchWindowDoesNotAdvanceCursorWhenPersistenceFails(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	if err := os.WriteFile(logPath, []byte("2026-04-18T12:00:00Z connect to first.example:443\n"), 0o644); err != nil {
		t.Fatalf("write initial log: %v", err)
	}

	evidenceStore := &fakeEvidenceStore{}
	cursors := make(map[string]PrefetchCursor)
	cursorKey := func(source string, path string) string { return source + "|" + path }
	lookupCursor := func(source string, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[cursorKey(source, path)]
		return cursor, ok
	}
	commitCursor := func(source string, path string, cursor PrefetchCursor) {
		cursors[cursorKey(source, path)] = cursor
	}
	parseLine := func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return model.LogEvidence{}, false
		}
		ts, err := time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return model.LogEvidence{}, false
		}
		hostPort := fields[len(fields)-1]
		return Normalize(model.LogEvidence{
			Source: source, EventTS: ts.Unix(), Host: strings.TrimSuffix(hostPort, ":443"),
			Method: "connect", Message: line,
		}), true
	}
	options := PrefetchOptions{
		Source:              "ss",
		LogDir:              logPath,
		StartTS:             time.Date(2026, 4, 18, 11, 59, 0, 0, time.UTC).Unix(),
		EndTS:               time.Date(2026, 4, 18, 12, 5, 0, 0, time.UTC).Unix(),
		Parser:              parseLine,
		MaxScanFiles:        1,
		MaxScanLinesPerFile: 100,
		CursorLookup:        lookupCursor,
		CursorCommit:        commitCursor,
	}
	if _, err := PrefetchWindow(context.Background(), evidenceStore, options); err != nil {
		t.Fatalf("initial prefetch: %v", err)
	}
	key := cursorKey("ss", logPath)
	previous, ok := cursors[key]
	if !ok || previous.Offset == 0 {
		t.Fatalf("initial cursor was not committed: %+v", cursors)
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := file.WriteString("2026-04-18T12:01:00Z connect to retry.example:443\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append retry row: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close append: %v", err)
	}

	evidenceStore.upsertErr = errors.New("injected upsert failure")
	if _, err := PrefetchWindow(context.Background(), evidenceStore, options); err == nil {
		t.Fatal("expected persistence failure")
	}
	if got := cursors[key]; got != previous {
		t.Fatalf("cursor advanced across failed persistence: before=%+v after=%+v", previous, got)
	}

	evidenceStore.upsertErr = nil
	result, err := PrefetchWindow(context.Background(), evidenceStore, options)
	if err != nil {
		t.Fatalf("retry prefetch: %v", err)
	}
	if result.RowsImported != 1 {
		t.Fatalf("retry skipped unpersisted appended row: %+v", result)
	}
	if got := evidenceStore.upserted[len(evidenceStore.upserted)-1].Host; got != "retry.example" {
		t.Fatalf("unexpected retried evidence row: %+v", evidenceStore.upserted)
	}
}

func TestPrefetchWindowPersistsAtMostFiveHundredRowsPerBatchBeforeCursorPublish(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "service.log")
	var content strings.Builder
	for index := 0; index < 501; index++ {
		fmt.Fprintf(&content, "row-%04d.example\n", index)
	}
	if err := os.WriteFile(logPath, []byte(content.String()), 0o600); err != nil {
		t.Fatalf("write batch log: %v", err)
	}

	evidenceStore := &fakeEvidenceStore{failBatch: 2}
	cursors := make(map[string]PrefetchCursor)
	lookupCursor := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+"|"+path]
		return cursor, ok
	}
	commitCursor := func(source, path string, cursor PrefetchCursor) {
		cursors[source+"|"+path] = cursor
	}
	reference := time.Now().UTC().Truncate(time.Second)
	parser := func(source string, line string, reference time.Time) (model.LogEvidence, bool) {
		return Normalize(model.LogEvidence{
			Source: source, EventTS: reference.Unix(), Host: line,
			Method: "test", Message: line, Fingerprint: line,
		}), true
	}
	options := PrefetchOptions{
		Source: "test", LogDir: logPath,
		StartTS: reference.Add(-time.Minute).Unix(), EndTS: reference.Add(time.Minute).Unix(), ReferenceTS: reference.Unix(),
		Parser: parser, MaxScanFiles: 1, MaxScanLinesPerFile: 1000,
		CursorLookup: lookupCursor, CursorCommit: commitCursor,
	}

	if _, err := PrefetchWindow(context.Background(), evidenceStore, options); err == nil {
		t.Fatal("expected second persistence batch to fail")
	}
	if len(cursors) != 0 {
		t.Fatalf("cursor published after intermediate batch failure: %+v", cursors)
	}
	if len(evidenceStore.batchSizes) != 2 || evidenceStore.batchSizes[0] != 500 || evidenceStore.batchSizes[1] != 1 {
		t.Fatalf("unexpected failed-attempt batch sizes: %+v", evidenceStore.batchSizes)
	}

	evidenceStore.failBatch = 0
	evidenceStore.batchCalls = 0
	evidenceStore.batchSizes = nil
	result, err := PrefetchWindow(context.Background(), evidenceStore, options)
	if err != nil {
		t.Fatalf("retry batched prefetch: %v", err)
	}
	if result.RowsImported != 501 {
		t.Fatalf("retry imported %d rows, want 501", result.RowsImported)
	}
	if len(evidenceStore.batchSizes) != 2 || evidenceStore.batchSizes[0] != 500 || evidenceStore.batchSizes[1] != 1 {
		t.Fatalf("unexpected retry batch sizes: %+v", evidenceStore.batchSizes)
	}
	for _, size := range evidenceStore.batchSizes {
		if size > prefetchEvidencePersistBatchSize {
			t.Fatalf("persistence batch exceeded cap: %d", size)
		}
	}
	if got := cursors["test|"+logPath].Offset; got != int64(len(content.String())) {
		t.Fatalf("cursor=%d, want EOF=%d after all batches succeeded", got, len(content.String()))
	}
}

func TestListLogFilesKeepsActiveUndatedLogDespiteMtime(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "ss-server.log")
	rotated := filepath.Join(dir, "ss-server.log-20260301.gz")
	if err := os.WriteFile(active, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write active log: %v", err)
	}
	if err := os.WriteFile(rotated, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write rotated log: %v", err)
	}
	mtime := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(active, mtime, mtime); err != nil {
		t.Fatalf("set active mtime: %v", err)
	}
	if err := os.Chtimes(rotated, mtime, mtime); err != nil {
		t.Fatalf("set rotated mtime: %v", err)
	}

	start := time.Date(2026, 4, 18, 11, 59, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 4, 18, 12, 1, 0, 0, time.UTC).Unix()
	files, err := ListLogFiles(dir, func(lowerName string) bool {
		return strings.Contains(lowerName, ".log")
	}, start, end, 10)
	if err != nil {
		t.Fatalf("list log files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected only active undated log, got %+v", files)
	}
	if filepath.Base(files[0].Path) != "ss-server.log" {
		t.Fatalf("expected active ss-server.log, got %+v", files[0])
	}
}

func TestScanFilesPrefersTailOfLargePlainTextLogs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ss-server.log")
	content := strings.Join([]string{
		"2026-04-18T11:58:00Z connect to old-1.example:443",
		"2026-04-18T11:58:30Z connect to old-2.example:443",
		"2026-04-18T12:00:00Z connect to recent-1.example:443",
		"2026-04-18T12:00:10Z connect to recent-2.example:443",
	}, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write test log: %v", err)
	}

	startTS := time.Date(2026, 4, 18, 11, 59, 30, 0, time.UTC).Unix()
	endTS := time.Date(2026, 4, 18, 12, 0, 30, 0, time.UTC).Unix()
	referenceTS := time.Date(2026, 4, 18, 12, 0, 10, 0, time.UTC).Unix()

	rows, err := ScanFiles(
		context.Background(),
		"ss",
		[]LogFileCandidate{{Path: logPath}},
		func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				return model.LogEvidence{}, false
			}
			ts, parseErr := time.Parse(time.RFC3339, fields[0])
			if parseErr != nil {
				return model.LogEvidence{}, false
			}
			hostPort := strings.TrimSpace(fields[len(fields)-1])
			return Normalize(model.LogEvidence{
				Source:  source,
				EventTS: ts.Unix(),
				Host:    strings.TrimSuffix(hostPort, ":443"),
				Path:    "443",
				Method:  "connect",
				Message: strings.TrimSpace(line),
			}), true
		},
		nil,
		startTS,
		endTS,
		referenceTS,
		10,
		2,
	)
	if err != nil {
		t.Fatalf("scan files: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected tail scan to keep 2 recent rows, got %+v", rows)
	}
	hosts := rows[0].Host + "," + rows[1].Host
	if !strings.Contains(hosts, "recent-1.example") || !strings.Contains(hosts, "recent-2.example") {
		t.Fatalf("expected recent tail rows, got %+v", rows)
	}
}

func TestScanLinesDedupesAndFiltersRows(t *testing.T) {
	startTS := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC).Unix()
	endTS := time.Date(2026, 4, 18, 12, 2, 0, 0, time.UTC).Unix()
	referenceTS := time.Date(2026, 4, 18, 12, 1, 0, 0, time.UTC).Unix()

	rows, err := ScanLines(
		context.Background(),
		"ss",
		[]string{
			"2026-04-18T12:01:00Z connect to chatgpt.com:443 #dup",
			"2026-04-18T12:01:00Z connect to chatgpt.com:443 #dup",
			"2026-04-18T12:01:10Z connect to openai.com:443 #keep",
			"2026-04-18T11:58:00Z connect to skipped.example:443 #old",
		},
		func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				return model.LogEvidence{}, false
			}
			ts, parseErr := time.Parse(time.RFC3339, fields[0])
			if parseErr != nil {
				return model.LogEvidence{}, false
			}
			hostPort := fields[len(fields)-2]
			host := strings.TrimSuffix(hostPort, ":443")
			return Normalize(model.LogEvidence{
				Source:      source,
				EventTS:     ts.Unix(),
				Host:        host,
				Path:        "443",
				Method:      "connect",
				Fingerprint: fields[len(fields)-1],
				Message:     strings.TrimSpace(line),
			}), true
		},
		func(row model.LogEvidence) bool {
			return row.Host != "openai.com"
		},
		startTS,
		endTS,
		referenceTS,
		10,
	)
	if err != nil {
		t.Fatalf("scan lines: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected deduped and filtered rows, got %+v", rows)
	}
	if rows[0].Host != "chatgpt.com" {
		t.Fatalf("expected chatgpt.com row, got %+v", rows[0])
	}
}
