package evidence

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type fakeEvidenceStore struct {
	upserted []model.LogEvidence
}

func (f *fakeEvidenceStore) QueryLogEvidence(context.Context, store.LogEvidenceQuery) ([]model.LogEvidence, error) {
	return nil, nil
}

func (f *fakeEvidenceStore) UpsertLogEvidenceBatch(_ context.Context, rows []model.LogEvidence) error {
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

	parser := func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		time.Sleep(10 * time.Millisecond)
		return Normalize(model.LogEvidence{
			Source:  source,
			EventTS: bucketTS,
			Host:    "chatgpt.com",
			Path:    "443",
			Method:  "connect",
			Message: strings.TrimSpace(line),
		}), true
	}

	rows, note, err := LookupOrScan(context.Background(), store, SearchOptions{
		Source:               "ss",
		LogDir:               logPath,
		BucketTS:             bucketTS,
		Limit:                8,
		Parser:               parser,
		StrictWindow:         120,
		FallbackWindow:       900,
		ScanBudget:           25 * time.Millisecond,
		MaxScanFilesStrict:   1,
		MaxScanFilesFallback: 1,
		MaxScanLinesPerFile:  100,
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
