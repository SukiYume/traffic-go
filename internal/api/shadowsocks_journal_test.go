package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func TestLoadShadowsocksJournalEvidenceUsesSharedScanDeadline(t *testing.T) {
	server := newTestServer(t)
	server.readShadowsocksJournal = func(ctx context.Context, _ time.Time, _ time.Time) ([]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	requestCtx := context.Background()
	sharedScanCtx, cancel := context.WithTimeout(requestCtx, 20*time.Millisecond)
	defer cancel()
	ctx := withExplainScanContext(requestCtx, sharedScanCtx)
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	started := time.Now()
	rows, note, err := server.loadShadowsocksJournalEvidence(
		ctx,
		reference-60,
		reference+60,
		reference,
		10,
		nil,
		time.Second,
	)
	if err != nil {
		t.Fatalf("load journal with shared deadline: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("journal ignored shared deadline and used a fresh budget: %s", elapsed)
	}
	if len(rows) != 0 || !strings.Contains(note, "扫描超过") {
		t.Fatalf("shared deadline result rows=%+v note=%q", rows, note)
	}
}

func TestLoadShadowsocksJournalEvidencePersistsRowsParsedBeforeDeadline(t *testing.T) {
	server := newTestServer(t)
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		return []string{"one", "two", "three"}, nil
	}

	requestCtx := context.Background()
	sharedScanCtx, cancel := context.WithTimeout(requestCtx, 50*time.Millisecond)
	defer cancel()
	ctx := withExplainScanContext(requestCtx, sharedScanCtx)
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	parserCalls := 0
	parser := evidence.Parser(func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		parserCalls++
		if parserCalls == 2 {
			<-sharedScanCtx.Done()
		}
		return evidence.Normalize(model.LogEvidence{
			Source: source, EventTS: reference, Host: line,
			Method: "test", Message: line, Fingerprint: "journal-" + line,
		}), true
	})

	rows, note, err := server.loadShadowsocksJournalEvidenceWithParser(
		ctx,
		reference-60,
		reference+60,
		reference,
		10,
		nil,
		time.Second,
		parser,
	)
	if err != nil {
		t.Fatalf("load partial journal rows: %v", err)
	}
	if len(rows) != 2 || !strings.Contains(note, "扫描超过") {
		t.Fatalf("partial journal result rows=%+v note=%q", rows, note)
	}
	persisted, err := server.store.QueryLogEvidence(requestCtx, store.LogEvidenceQuery{
		Source: evidenceSourceSS, StartTS: reference - 60, EndTS: reference + 60, Limit: 10,
	})
	if err != nil {
		t.Fatalf("query partial journal evidence: %v", err)
	}
	if len(persisted) != 2 {
		t.Fatalf("persisted %d partial journal rows, want 2: %+v", len(persisted), persisted)
	}
}

func TestLoadShadowsocksJournalEvidencePersistsReaderRowsReturnedWithTimeout(t *testing.T) {
	server := newTestServer(t)
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		return []string{"one", "two"}, context.DeadlineExceeded
	}
	reference := time.Now().UTC().Truncate(time.Second).Unix()
	parser := evidence.Parser(func(source string, line string, _ time.Time) (model.LogEvidence, bool) {
		return evidence.Normalize(model.LogEvidence{
			Source: source, EventTS: reference, Host: line,
			Method: "test", Message: line, Fingerprint: "timed-reader-" + line,
		}), true
	})

	rows, note, err := server.loadShadowsocksJournalEvidenceWithParser(
		context.Background(),
		reference-60,
		reference+60,
		reference,
		10,
		nil,
		time.Second,
		parser,
	)
	if err != nil {
		t.Fatalf("load timed-out reader rows: %v", err)
	}
	if len(rows) != 2 || !strings.Contains(note, "扫描超过") {
		t.Fatalf("timed reader result rows=%+v note=%q", rows, note)
	}
	persisted, err := server.store.QueryLogEvidence(context.Background(), store.LogEvidenceQuery{
		Source: evidenceSourceSS, StartTS: reference - 60, EndTS: reference + 60, Limit: 10,
	})
	if err != nil {
		t.Fatalf("query timed reader evidence: %v", err)
	}
	if len(persisted) != 2 {
		t.Fatalf("persisted %d timed reader rows, want 2: %+v", len(persisted), persisted)
	}
}
