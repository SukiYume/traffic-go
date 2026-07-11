package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func TestProcessLogDescriptorsDedupesEquivalentShadowsocksDirs(t *testing.T) {
	dir := t.TempDir()
	server := &Server{
		processLogDirs: map[string]string{
			"ss-server":   dir,
			"ss-manager":  filepath.Join(dir, "."),
			"obfs-server": filepath.Join(dir, "subdir", ".."),
		},
	}

	descriptors := server.processLogDescriptors()
	if len(descriptors) != 1 {
		t.Fatalf("expected one deduped descriptor, got %+v", descriptors)
	}
	if descriptors[0].EvidenceSource != evidenceSourceSS {
		t.Fatalf("expected shadowsocks evidence source, got %+v", descriptors[0])
	}
	if descriptors[0].LookupKey != "obfs-server" {
		t.Fatalf("expected deterministic first sorted lookup key, got %+v", descriptors[0])
	}
}

func TestRunBackgroundPrefetchUsesSuccessfulWatermarkAcrossSkippedWindow(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)
	logPath := filepath.Join(logDir, "ss-server.log")
	base := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)
	line := func(ts time.Time, host string) string {
		return fmt.Sprintf("%s /usr/bin/ss-server[27896]: [12096] connect to %s:443", ts.Format(time.RFC3339), host)
	}
	if err := os.WriteFile(logPath, []byte(line(base, "first.example")+"\n"), 0o644); err != nil {
		t.Fatalf("write initial evidence: %v", err)
	}
	options := BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 base.Add(2*time.Minute + 30*time.Second),
		EvidenceLookback:    5 * time.Minute,
		ChainLookback:       5 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        2,
		MaxScanLinesPerFile: 100,
	}
	first := server.RunBackgroundPrefetch(context.Background(), options)
	if first.Errors != 0 || first.EvidenceRows != 1 {
		t.Fatalf("initial prefetch failed: %+v", first)
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open evidence append: %v", err)
	}
	if _, err := file.WriteString(line(base.Add(10*time.Minute), "gap.example") + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("append gap evidence: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close evidence append: %v", err)
	}
	gapMinute := base.Add(10 * time.Minute)
	if err := server.store.FlushMinute(context.Background(), gapMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS: gapMinute.Unix(), Proto: "tcp", Direction: model.DirectionOut,
			PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server",
			LocalPort: 47920, RemoteIP: "203.0.113.10", RemotePort: 443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 1024, BytesDown: 2048, PktsUp: 2, PktsDown: 4, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("seed usage inside skipped window: %v", err)
	}

	// This run is 30 minutes after the prior run, while the configured lookback
	// is only five minutes. The prior successful watermark must bridge the gap.
	options.Now = base.Add(30 * time.Minute)
	second := server.RunBackgroundPrefetch(context.Background(), options)
	if second.Errors != 0 || second.EvidenceRows != 1 || second.UsageRows != 1 {
		t.Fatalf("watermark did not bridge skipped window: %+v", second)
	}
	rows, err := server.store.QueryLogEvidence(context.Background(), store.LogEvidenceQuery{
		Source:         evidenceSourceSS,
		StartTS:        base.Unix(),
		EndTS:          base.Add(31 * time.Minute).Unix(),
		HostNormalized: "gap.example",
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query gap evidence: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("gap evidence was permanently skipped: %+v", rows)
	}
}

func TestRunBackgroundPrefetchFreezesWindowsWhenScanGateIsBusy(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)
	base := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)
	options := BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 base,
		EvidenceLookback:    5 * time.Minute,
		ChainLookback:       5 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        2,
		MaxScanLinesPerFile: 100,
	}
	server.scanGate <- struct{}{}
	skipped := server.RunBackgroundPrefetch(context.Background(), options)
	<-server.scanGate
	if skipped.PartialSources != 1 || skipped.Errors != 0 {
		t.Fatalf("busy scan gate did not report a skipped source: %+v", skipped)
	}
	if state := server.lookupBackgroundChainPrefetchState(); !state.Active {
		t.Fatalf("busy scan gate did not freeze the pending chain window: %+v", state)
	}

	gapMinute := base.Add(10 * time.Minute)
	logLine := fmt.Sprintf(
		"%s /usr/bin/ss-server[27896]: [12096] connect to gated.example:443\n",
		gapMinute.Format(time.RFC3339),
	)
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte(logLine), 0o644); err != nil {
		t.Fatalf("write gated evidence: %v", err)
	}
	if err := server.store.FlushMinute(context.Background(), gapMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS: gapMinute.Unix(), Proto: "tcp", Direction: model.DirectionOut,
			PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server",
			LocalPort: 47920, RemoteIP: "203.0.113.20", RemotePort: 443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 1024, BytesDown: 2048, PktsUp: 2, PktsDown: 4, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("seed gated usage: %v", err)
	}

	options.Now = base.Add(30 * time.Minute)
	catchup := server.RunBackgroundPrefetch(context.Background(), options)
	if catchup.Errors != 0 || catchup.EvidenceRows != 1 {
		t.Fatalf("evidence window skipped while gate was busy: %+v", catchup)
	}
	// The first healthy run drains the already-frozen empty chain window. The
	// next page opens from that watermark and must still include the gap row.
	chainCatchup := server.RunBackgroundPrefetch(context.Background(), options)
	if chainCatchup.Errors != 0 || chainCatchup.UsageRows != 1 {
		t.Fatalf("chain window skipped while gate was busy: %+v", chainCatchup)
	}
}

func TestRunBackgroundPrefetchAdvancesThroughUsageBacklog(t *testing.T) {
	server := newTestServer(t)
	setProcessLogDir(server, "ss-server", t.TempDir())
	ctx := context.Background()
	base := time.Date(2026, 4, 18, 9, 0, 0, 0, time.UTC)
	const rowCount = backgroundPrefetchMaxUsageRows*2 + 2
	for index := 0; index < rowCount; index++ {
		minute := base.Add(time.Duration(index) * time.Minute)
		if err := server.store.FlushMinute(ctx, minute.Unix(), map[model.UsageKey]model.UsageDelta{
			{
				MinuteTS:    minute.Unix(),
				Proto:       "tcp",
				Direction:   model.DirectionOut,
				PID:         1000 + index,
				Comm:        "ss-server",
				Exe:         "/usr/bin/ss-server",
				LocalPort:   40000 + index,
				RemoteIP:    "203.0.113.10",
				RemotePort:  443,
				Attribution: model.AttributionExact,
			}: {
				BytesUp: 1024, BytesDown: 2048, PktsUp: 2, PktsDown: 4, FlowCount: 1,
			},
		}, nil); err != nil {
			t.Fatalf("seed backlog row %d: %v", index, err)
		}
	}

	options := BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 base.Add(rowCount*time.Minute + 90*time.Second),
		EvidenceLookback:    5 * time.Minute,
		ChainLookback:       4 * time.Hour,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        2,
		MaxScanLinesPerFile: 100,
	}
	wantUsageRows := []int{backgroundPrefetchMaxUsageRows, backgroundPrefetchMaxUsageRows, 2, 0}
	var previousCursor int64
	for run, want := range wantUsageRows {
		summary := server.RunBackgroundPrefetch(ctx, options)
		if summary.Errors != 0 {
			t.Fatalf("backlog run %d returned errors: %+v", run+1, summary)
		}
		if summary.UsageRows != want {
			t.Fatalf("backlog run %d usage rows = %d, want %d; summary=%+v", run+1, summary.UsageRows, want, summary)
		}
		state := server.lookupBackgroundChainPrefetchState()
		if run < 2 {
			if !state.Active || state.CursorTS == 0 {
				t.Fatalf("backlog run %d did not retain a continuation cursor: %+v", run+1, state)
			}
			if previousCursor != 0 && state.CursorTS >= previousCursor {
				t.Fatalf("backlog cursor did not move toward older rows: before=%d after=%d", previousCursor, state.CursorTS)
			}
			previousCursor = state.CursorTS
		} else if state.Active {
			t.Fatalf("backlog should be drained after run %d: %+v", run+1, state)
		}
	}
}

func TestRunBackgroundPrefetchRetriesMinuteFlushedAfterClosedBoundaryPass(t *testing.T) {
	server := newTestServer(t)
	server.enableSSJournalFallback = false
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)
	logPath := filepath.Join(logDir, "ss-server.log")
	firstNow := time.Date(2026, 4, 18, 12, 30, 30, 0, time.UTC)
	lateMinute := firstNow.Truncate(time.Minute).Add(-time.Minute)
	line := fmt.Sprintf(
		"%s /usr/bin/ss-server[27896]: [12096] connect to late.example:443\n",
		lateMinute.Add(45*time.Second).Format(time.RFC3339),
	)
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatalf("create active log: %v", err)
	}

	ctx := context.Background()
	options := BackgroundPrefetchOptions{
		Enabled: true, Now: firstNow,
		EvidenceLookback: time.Minute, ChainLookback: time.Minute,
		ScanBudget: time.Second, MaxScanFiles: 1, MaxScanLinesPerFile: 100,
	}

	first := server.RunBackgroundPrefetch(ctx, options)
	if first.Errors != 0 || first.PartialSources != 0 || first.EvidenceRows != 0 || first.UsageRows != 0 {
		t.Fatalf("unexpected first closed-boundary pass: %+v", first)
	}
	wantFirstWatermark := backgroundPrefetchClosedEnd(firstNow).Unix()
	descriptor := server.processLogDescriptors()[0]
	if got := server.ensureEvidencePrefetchWatermark(descriptor, 1); got != wantFirstWatermark {
		t.Fatalf("evidence watermark=%d, want closed boundary %d", got, wantFirstWatermark)
	}
	if got := server.lookupBackgroundChainPrefetchState().Watermark; got != wantFirstWatermark {
		t.Fatalf("chain watermark=%d, want closed boundary %d", got, wantFirstWatermark)
	}

	if err := server.store.FlushMinute(ctx, lateMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS: lateMinute.Unix(), Proto: "tcp", Direction: model.DirectionOut,
			PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server",
			LocalPort: 47920, RemoteIP: "203.0.113.45", RemotePort: 443,
			Attribution: model.AttributionExact,
		}: {BytesUp: 1024, BytesDown: 2048, FlowCount: 1},
	}, nil); err != nil {
		t.Fatalf("flush late closed-minute usage: %v", err)
	}

	options.Now = firstNow.Add(2 * time.Minute)
	second := server.RunBackgroundPrefetch(ctx, options)
	if second.Errors != 0 || second.PartialSources != 0 || second.EvidenceRows != 1 || second.UsageRows != 1 {
		t.Fatalf("late closed-minute data was skipped on second pass: %+v", second)
	}
	rows, err := server.store.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source: evidenceSourceSS, StartTS: lateMinute.Unix(), EndTS: lateMinute.Add(time.Minute).Unix(),
		HostNormalized: "late.example", Limit: 10,
	})
	if err != nil {
		t.Fatalf("query late evidence: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("late evidence was not persisted exactly once: %+v", rows)
	}
}

func TestRunBackgroundPrefetchKeepsWatermarkFrozenUntilMaxLinesBacklogDrains(t *testing.T) {
	server := newTestServer(t)
	server.enableSSJournalFallback = false
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)
	base := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)
	content := fmt.Sprintf(
		"%s /usr/bin/ss-server[1]: [1001] connect to first.example:443\n%s /usr/bin/ss-server[1]: [1002] connect to second.example:443\n",
		base.Format(time.RFC3339), base.Add(10*time.Second).Format(time.RFC3339),
	)
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte(content), 0o600); err != nil {
		t.Fatalf("write truncated backlog: %v", err)
	}

	options := BackgroundPrefetchOptions{
		Enabled: true, Now: base.Add(3*time.Minute + 30*time.Second),
		EvidenceLookback: 5 * time.Minute, ChainLookback: 5 * time.Minute,
		ScanBudget: time.Second, MaxScanFiles: 1, MaxScanLinesPerFile: 1,
	}
	descriptor := server.processLogDescriptors()[0]
	initialWatermark := options.Now.Add(-options.EvidenceLookback).Unix()
	first := server.RunBackgroundPrefetch(context.Background(), options)
	if first.Errors != 0 || first.PartialSources != 1 || first.EvidenceRows != 1 {
		t.Fatalf("first truncated pass = %+v", first)
	}
	if got := server.ensureEvidencePrefetchWatermark(descriptor, 1); got != initialWatermark {
		t.Fatalf("truncated pass advanced watermark: got=%d want frozen=%d", got, initialWatermark)
	}

	second := server.RunBackgroundPrefetch(context.Background(), options)
	if second.Errors != 0 || second.PartialSources != 0 || second.EvidenceRows != 1 {
		t.Fatalf("second backlog pass = %+v", second)
	}
	wantWatermark := backgroundPrefetchClosedEnd(options.Now).Unix()
	if got := server.ensureEvidencePrefetchWatermark(descriptor, 1); got != wantWatermark {
		t.Fatalf("drained pass watermark=%d, want %d", got, wantWatermark)
	}
	rows, err := server.store.QueryLogEvidence(context.Background(), store.LogEvidenceQuery{
		Source: evidenceSourceSS, StartTS: base.Add(-time.Minute).Unix(), EndTS: base.Add(time.Minute).Unix(), Limit: 10,
	})
	if err != nil {
		t.Fatalf("query drained evidence: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("max-lines backlog imported %d rows, want 2: %+v", len(rows), rows)
	}
}

func TestRunBackgroundPrefetchDoesNotAdvanceWatermarkAfterJournalTimeout(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)
	server.readShadowsocksJournal = func(ctx context.Context, _ time.Time, _ time.Time) ([]string, error) {
		<-ctx.Done()
		return []string{
			"2026-04-18T12:28:10Z /usr/bin/ss-server[27896]: [12096] connect to partial.example:443",
		}, ctx.Err()
	}

	now := time.Date(2026, 4, 18, 12, 30, 30, 0, time.UTC)
	options := BackgroundPrefetchOptions{
		Enabled: true, Now: now,
		EvidenceLookback: 5 * time.Minute, ChainLookback: 5 * time.Minute,
		ScanBudget: 20 * time.Millisecond, MaxScanFiles: 1, MaxScanLinesPerFile: 100,
	}
	started := time.Now()
	summary := server.RunBackgroundPrefetch(context.Background(), options)
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("journal exceeded shared background scan budget: %s", elapsed)
	}
	if summary.Errors != 0 || summary.PartialSources != 1 || summary.EvidenceRows != 1 {
		t.Fatalf("journal timeout was not reported as partial: %+v", summary)
	}
	descriptor := server.processLogDescriptors()[0]
	wantWatermark := now.Add(-options.EvidenceLookback).Unix()
	if got := server.ensureEvidencePrefetchWatermark(descriptor, 1); got != wantWatermark {
		t.Fatalf("journal timeout advanced watermark: got=%d want frozen=%d", got, wantWatermark)
	}
	if state := server.lookupBackgroundChainPrefetchState(); !state.Active || state.Watermark != 0 {
		t.Fatalf("journal timeout must keep chain window frozen: %+v", state)
	}
	rows, err := server.store.QueryLogEvidence(context.Background(), store.LogEvidenceQuery{
		Source: evidenceSourceSS, StartTS: now.Add(-options.EvidenceLookback).Unix(), EndTS: now.Unix(),
		HostNormalized: "partial.example", Limit: 10,
	})
	if err != nil {
		t.Fatalf("query partial journal evidence: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("journal timeout did not persist captured partial row: %+v", rows)
	}
}
