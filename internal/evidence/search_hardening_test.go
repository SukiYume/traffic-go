package evidence

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/model"
)

func hardeningParser(source string, line string, reference time.Time) (model.LogEvidence, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return model.LogEvidence{}, false
	}
	return Normalize(model.LogEvidence{
		Source: source, EventTS: reference.Unix(), Host: line,
		Method: "test", Message: line, Fingerprint: line,
	}), true
}

func TestPrefetchCursorDoesNotCommitUnterminatedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.log")
	complete := "first.example\n"
	partial := "second.example"
	if err := os.WriteFile(path, []byte(complete+partial), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}
	cursors := map[string]PrefetchCursor{}
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	files, err := ListLogFiles(path, nil, 0, 0, 1)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	rows, _, err := ScanPrefetchFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 100, lookup, commit)
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if len(rows) != 1 || rows[0].Message != "first.example" {
		t.Fatalf("unterminated line was parsed: %+v", rows)
	}
	if got := cursors["test"+path].Offset; got != int64(len(complete)) {
		t.Fatalf("cursor committed partial line: got=%d want=%d", got, len(complete))
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	if _, err := file.WriteString("-finished\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append line: %v", err)
	}
	_ = file.Close()
	rows, _, err = ScanPrefetchFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 100, lookup, commit)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(rows) != 1 || rows[0].Message != "second.example-finished" {
		t.Fatalf("completed line was not imported exactly once: %+v", rows)
	}
}

func TestPrefetchCursorDetectsCopytruncateRegrow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.log")
	oldContent := "old-a.example\nold-b.example\n"
	if err := os.WriteFile(path, []byte(oldContent), 0o600); err != nil {
		t.Fatalf("write old log: %v", err)
	}
	cursors := map[string]PrefetchCursor{}
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	files, err := ListLogFiles(path, nil, 0, 0, 1)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	if _, _, err := ScanPrefetchFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 100, lookup, commit); err != nil {
		t.Fatalf("first scan: %v", err)
	}
	newContent := "new-a.example\nnew-b.example\nnew-c.example\n"
	if len(newContent) <= len(oldContent) {
		t.Fatal("test requires regrown file to exceed previous offset")
	}
	if err := os.WriteFile(path, []byte(newContent), 0o600); err != nil {
		t.Fatalf("copytruncate rewrite: %v", err)
	}
	rows, _, err := ScanPrefetchFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 100, lookup, commit)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("copytruncate regrow was treated as append: %+v", rows)
	}
}

func TestListLogFilesRejectsSymlinkAndHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	link := filepath.Join(dir, "link.log")
	if err := os.WriteFile(target, []byte("test\n"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, link); err == nil {
		if _, err := ListLogFiles(link, nil, 0, 0, 1); err == nil {
			t.Fatal("expected explicit symlink to be rejected")
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ListLogFilesContext(canceled, dir, nil, 0, 0, 1); err == nil {
		t.Fatal("expected canceled directory enumeration to stop")
	}
}

func TestListLogFilesGlobKeepsBoundedBestCandidatesInLargeDirectory(t *testing.T) {
	dir := t.TempDir()
	center := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)
	const fileCount = 2048
	const candidateLimit = 7
	for index := 0; index < fileCount; index++ {
		name := fmt.Sprintf("candidate-%04d.log", index)
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write candidate %d: %v", index, err)
		}
		modified := center.Add(time.Duration(index) * time.Second)
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatalf("set candidate mtime %d: %v", index, err)
		}
	}

	files, err := ListLogFilesContext(
		context.Background(),
		filepath.Join(dir, "*.log"),
		nil,
		center.Add(-time.Minute).Unix(),
		center.Add(time.Minute).Unix(),
		candidateLimit,
	)
	if err != nil {
		t.Fatalf("list large glob directory: %v", err)
	}
	if len(files) != candidateLimit {
		t.Fatalf("candidate count = %d, want %d", len(files), candidateLimit)
	}
	for index, candidate := range files {
		want := fmt.Sprintf("candidate-%04d.log", index)
		if candidate.Name != want {
			t.Fatalf("candidate[%d] = %q, want %q; all=%+v", index, candidate.Name, want, files)
		}
	}
}

func TestGlobEnumerationStopsAfterContextCancellation(t *testing.T) {
	dir := t.TempDir()
	for index := 0; index < 16; index++ {
		path := filepath.Join(dir, fmt.Sprintf("candidate-%02d.log", index))
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("write candidate %d: %v", index, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	visited := 0
	err := forEachResolvedLogPath(ctx, filepath.Join(dir, "*.log"), func(string) error {
		visited++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("glob enumeration error = %v, want context cancellation", err)
	}
	if visited != 1 {
		t.Fatalf("glob enumeration visited %d matches after cancellation, want 1", visited)
	}
}

func TestGlobConfiguredRootUsesDirectoryBeforeWildcardPrefix(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "nginx"), 0o700); err != nil {
		t.Fatalf("create colliding prefix directory: %v", err)
	}
	path := filepath.Join(dir, "nginx-access.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write wildcard match: %v", err)
	}
	files, err := ListLogFiles(filepath.Join(dir, "nginx*.log"), nil, 0, 0, 4)
	if err != nil {
		t.Fatalf("list wildcard prefix: %v", err)
	}
	if len(files) != 1 || files[0].Path != path {
		t.Fatalf("wildcard match was constrained to colliding prefix directory: %+v", files)
	}
}

func TestPrefetchMaxLinesContinuesPlainFileAcrossRounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.log")
	content := strings.Join([]string{
		"one.example",
		"two.example",
		"three.example",
		"four.example",
		"five.example",
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write plain log: %v", err)
	}

	store := &fakeEvidenceStore{}
	cursors := make(map[string]PrefetchCursor)
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	reference := time.Now().UTC().Truncate(time.Second)
	options := PrefetchOptions{
		Source: "test", LogDir: path,
		StartTS: reference.Add(-time.Minute).Unix(), EndTS: reference.Add(time.Minute).Unix(), ReferenceTS: reference.Unix(),
		Parser: hardeningParser, MaxScanFiles: 1, MaxScanLinesPerFile: 2,
		CursorLookup: lookup, CursorCommit: commit,
	}

	wantRows := []int{2, 2, 1}
	wantPartial := []bool{true, true, false}
	previousOffset := int64(0)
	for index := range wantRows {
		result, err := PrefetchWindow(context.Background(), store, options)
		if err != nil {
			t.Fatalf("prefetch round %d: %v", index+1, err)
		}
		if result.RowsImported != wantRows[index] || result.Partial != wantPartial[index] {
			t.Fatalf("prefetch round %d = %+v, want rows=%d partial=%v", index+1, result, wantRows[index], wantPartial[index])
		}
		cursor := cursors["test"+path]
		if cursor.Offset <= previousOffset {
			t.Fatalf("plain cursor did not advance in round %d: before=%d after=%d", index+1, previousOffset, cursor.Offset)
		}
		previousOffset = cursor.Offset
	}
	if previousOffset != int64(len(content)) {
		t.Fatalf("plain cursor=%d, want EOF=%d", previousOffset, len(content))
	}
	if len(store.upserted) != 5 {
		t.Fatalf("plain multi-round import=%d rows, want 5: %+v", len(store.upserted), store.upserted)
	}
}

func TestPrefetchMaxLinesContinuesGzipFileAcrossRounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.log.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create gzip log: %v", err)
	}
	writer := gzip.NewWriter(file)
	for _, line := range []string{"one.example", "two.example", "three.example", "four.example", "five.example"} {
		if _, err := fmt.Fprintln(writer, line); err != nil {
			t.Fatalf("write gzip line: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close gzip file: %v", err)
	}

	store := &fakeEvidenceStore{}
	cursors := make(map[string]PrefetchCursor)
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	reference := time.Now().UTC().Truncate(time.Second)
	options := PrefetchOptions{
		Source: "test", LogDir: path,
		StartTS: reference.Add(-time.Minute).Unix(), EndTS: reference.Add(time.Minute).Unix(), ReferenceTS: reference.Unix(),
		Parser: hardeningParser, MaxScanFiles: 1, MaxScanLinesPerFile: 2,
		CursorLookup: lookup, CursorCommit: commit,
	}

	wantRows := []int{2, 2, 1, 0}
	wantPartial := []bool{true, true, false, false}
	wantLineOffsets := []int64{2, 4, 0, 0}
	wantIncomplete := []bool{true, true, false, false}
	for index := range wantRows {
		result, err := PrefetchWindow(context.Background(), store, options)
		if err != nil {
			t.Fatalf("gzip prefetch round %d: %v", index+1, err)
		}
		if result.RowsImported != wantRows[index] || result.Partial != wantPartial[index] {
			t.Fatalf("gzip prefetch round %d = %+v, want rows=%d partial=%v", index+1, result, wantRows[index], wantPartial[index])
		}
		if got := cursors["test"+path].LineOffset; got != wantLineOffsets[index] {
			t.Fatalf("gzip line cursor round %d=%d, want %d", index+1, got, wantLineOffsets[index])
		}
		if got := cursors["test"+path].Incomplete; got != wantIncomplete[index] {
			t.Fatalf("gzip incomplete cursor round %d=%v, want %v", index+1, got, wantIncomplete[index])
		}
	}
	if len(store.upserted) != 5 {
		t.Fatalf("gzip multi-round import=%d rows, want 5: %+v", len(store.upserted), store.upserted)
	}
}

func TestPrefetchGzipDefersFutureRowsWithoutMarkingWindowPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "future.log.gz")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create future gzip log: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Minute)
	future := base.Add(30 * time.Second)
	writer := gzip.NewWriter(file)
	if _, err := fmt.Fprintln(writer, future.Format(time.RFC3339)); err != nil {
		t.Fatalf("write future gzip row: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close future gzip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close future gzip file: %v", err)
	}
	if err := os.Chtimes(path, base, base); err != nil {
		t.Fatalf("set future gzip mtime: %v", err)
	}

	parser := func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		eventTime, err := time.Parse(time.RFC3339, strings.TrimSpace(line))
		if err != nil {
			return model.LogEvidence{}, false
		}
		return Normalize(model.LogEvidence{
			Source: source, EventTS: eventTime.Unix(), Host: "future.example",
			Method: "test", Message: line, Fingerprint: line,
		}), true
	}
	store := &fakeEvidenceStore{}
	cursors := make(map[string]PrefetchCursor)
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	options := PrefetchOptions{
		Source: "test", LogDir: path,
		StartTS: base.Add(-time.Minute).Unix(), EndTS: base.Unix(), ReferenceTS: base.Unix(),
		Parser: parser, MaxScanFiles: 1, MaxScanLinesPerFile: 100,
		CursorLookup: lookup, CursorCommit: commit,
	}

	first, err := PrefetchWindow(context.Background(), store, options)
	if err != nil {
		t.Fatalf("first future gzip prefetch: %v", err)
	}
	if first.Partial || first.RowsImported != 0 {
		t.Fatalf("future row should be deferred without making the closed window partial: %+v", first)
	}
	if cursor := cursors["test"+path]; !cursor.Incomplete || cursor.LineOffset != 0 {
		t.Fatalf("future gzip cursor must remain before the first row: %+v", cursor)
	}

	options.EndTS = base.Add(time.Minute).Unix()
	options.ReferenceTS = options.EndTS
	second, err := PrefetchWindow(context.Background(), store, options)
	if err != nil {
		t.Fatalf("second future gzip prefetch: %v", err)
	}
	if second.Partial || second.RowsImported != 1 {
		t.Fatalf("deferred gzip row was not imported when its window closed: %+v", second)
	}
	if cursor := cursors["test"+path]; cursor.Incomplete || cursor.LineOffset != 0 {
		t.Fatalf("completed gzip cursor remained incomplete: %+v", cursor)
	}
}

func TestPlainScansSkipOversizedLineAndContinueWithinBound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.log")
	oversized := strings.Repeat("x", maxPlainLogLineBytes+tailScanChunkSize)
	content := oversized + "\nkept.example\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write oversized log: %v", err)
	}
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	files := []LogFileCandidate{{Name: filepath.Base(path), Path: path, ModTime: time.Now()}}

	rows, err := ScanFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 10)
	if err != nil {
		t.Fatalf("tail scan oversized plain log: %v", err)
	}
	if len(rows) != 1 || rows[0].Message != "kept.example" {
		t.Fatalf("tail scan did not skip oversized line cleanly: %+v", rows)
	}

	cursors := make(map[string]PrefetchCursor)
	lookup := func(source, path string) (PrefetchCursor, bool) {
		cursor, ok := cursors[source+path]
		return cursor, ok
	}
	commit := func(source, path string, cursor PrefetchCursor) { cursors[source+path] = cursor }
	rows, truncated, err := ScanPrefetchFiles(context.Background(), "test", files, hardeningParser, nil, reference-1, reference+1, reference, 0, 10, lookup, commit)
	if err != nil {
		t.Fatalf("forward scan oversized plain log: %v", err)
	}
	if truncated || len(rows) != 1 || rows[0].Message != "kept.example" {
		t.Fatalf("forward scan did not skip oversized line cleanly: truncated=%v rows=%+v", truncated, rows)
	}
	if got := cursors["test"+path].Offset; got != int64(len(content)) {
		t.Fatalf("oversized line cursor=%d, want EOF=%d", got, len(content))
	}
}
