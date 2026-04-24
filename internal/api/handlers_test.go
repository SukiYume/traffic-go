package api

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/embed"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func writeGzipFile(path string, text string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := gzip.NewWriter(file)
	if _, err := writer.Write([]byte(text)); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

type stubRuntime struct {
	processes []model.ProcessListItem
	stats     model.ActiveStats
}

func (s stubRuntime) ActiveProcesses() []model.ProcessListItem {
	return s.processes
}

func (s stubRuntime) ActiveStats() model.ActiveStats {
	return s.stats
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	trafficStore, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = trafficStore.Close()
	})
	return NewServer(trafficStore, stubRuntime{
		processes: []model.ProcessListItem{
			{PID: 42, Comm: "ss-server", Exe: "/usr/bin/ss-server"},
		},
		stats: model.ActiveStats{Connections: 3, Processes: 1},
	}, nil, embed.StaticFS(), cfg.ProcessLogDirs, true)
}

func setProcessLogDir(server *Server, processKey string, dir string) {
	if server.processLogDirs == nil {
		server.processLogDirs = make(map[string]string)
	}
	server.processLogDirs[processKey] = dir
}

func TestUsageDimensionUnavailable(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?range=90d&pid=12", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "dimension_unavailable") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestHealthz(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/healthz", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
}

func TestTimeseriesDimensionUnavailable(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats/timeseries?range=90d&pid=12", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "dimension_unavailable") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageRejectsInvalidPageParam(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?range=24h&page=abc", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_usage_query") || !strings.Contains(string(body), "invalid page") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageRejectsCursorSortOverrides(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?range=24h&sort_by=bytes_up&sort_order=asc", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_usage_query") || !strings.Contains(string(body), "cursor pagination only supports time-desc sort") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageRejectsRemotePortForHourlySource(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?range=90d&remote_port=443", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "dimension_unavailable") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageRejectsNonPositiveRange(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage?range=0h", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_usage_query") || !strings.Contains(string(body), "range must be positive") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageRejectsInvertedExplicitWindow(t *testing.T) {
	server := newTestServer(t)
	start := url.QueryEscape("2026-04-16T10:00:00Z")
	end := url.QueryEscape("2026-04-16T09:00:00Z")
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/usage?start=%s&end=%s", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_usage_query") || !strings.Contains(string(body), "end must be after start") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestTopProcessesRejectsInvalidPageParam(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/top/processes?range=24h&page=abc", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_query") || !strings.Contains(string(body), "invalid page") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestForwardUsageRejectsCursorSortOverrides(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/forward/usage?range=24h&sort_by=bytes_total", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_forward_query") || !strings.Contains(string(body), "cursor pagination only supports time-desc sort") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestForwardUsageReturnsForwardDataSourceLabel(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, nil, map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  minute,
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 51000,
			OrigDPort: 443,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}); err != nil {
		t.Fatalf("seed forward row: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/forward/usage?start=%s&end=%s&page=1&page_size=10", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"data_source":"usage_1m_forward"`) {
		t.Fatalf("expected forward data source label in body: %s", bodyText)
	}
}

func TestUsageResponseNormalizesPageMetadata(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1045,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "203.0.113.24",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed usage row: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/usage?start=%s&end=%s&page=0&page_size=999", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"page":1`) || !strings.Contains(bodyText, `"page_size":200`) {
		t.Fatalf("expected normalized page metadata, got %s", bodyText)
	}
}

func TestUsageFiltersByRemotePort(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "198.51.100.44",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "198.51.100.44",
			RemotePort:  8443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   256,
			BytesDown: 512,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed usage rows: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/usage?start=%s&end=%s&page=1&page_size=10&remote_ip=198.51.100.44&remote_port=443", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"remote_port":443`) {
		t.Fatalf("expected filtered remote_port=443 row in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `"remote_port":8443`) {
		t.Fatalf("expected remote_port filter to exclude 8443 row: %s", bodyText)
	}
}

func TestForwardUsageResponseNormalizesPageMetadata(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, nil, map[model.ForwardUsageKey]model.UsageDelta{
		{
			MinuteTS:  minute,
			Proto:     "tcp",
			OrigSrcIP: "10.0.0.2",
			OrigDstIP: "1.1.1.1",
			OrigSPort: 51000,
			OrigDPort: 443,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}); err != nil {
		t.Fatalf("seed forward row: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/forward/usage?start=%s&end=%s&page=-1&page_size=999", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"page":1`) || !strings.Contains(bodyText, `"page_size":200`) {
		t.Fatalf("expected normalized forward page metadata, got %s", bodyText)
	}
}

func TestTopProcessesSupportsGroupByComm(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1045,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "203.0.113.24",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         2048,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "198.51.100.77",
			RemotePort:  50210,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   4096,
			BytesDown: 8192,
			PktsUp:    7,
			PktsDown:  9,
			FlowCount: 2,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed top processes rows: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Hour).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Hour).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/top/processes?start=%s&end=%s&group_by=comm&page=1&page_size=10&sort_by=bytes_total&sort_order=desc", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, "\"total_rows\":1") {
		t.Fatalf("expected one grouped row: %s", bodyText)
	}
	if !strings.Contains(bodyText, "\"comm\":\"nginx\"") {
		t.Fatalf("expected nginx grouped row: %s", bodyText)
	}
	if !strings.Contains(bodyText, "\"pid\":null") {
		t.Fatalf("expected pid null in comm grouping: %s", bodyText)
	}
}

func TestTopProcessesResponseNormalizesPageMetadata(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1045,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "203.0.113.24",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed top process row: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/top/processes?start=%s&end=%s&page=0&page_size=999", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"page":1`) || !strings.Contains(bodyText, `"page_size":200`) {
		t.Fatalf("expected normalized top-process page metadata, got %s", bodyText)
	}
}

func TestTopRemotesResponseNormalizesPageMetadata(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1045,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "203.0.113.24",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed top remote row: %v", err)
	}

	start := url.QueryEscape(time.Unix(minute, 0).Add(-time.Minute).UTC().Format(time.RFC3339))
	end := url.QueryEscape(time.Unix(minute, 0).Add(time.Minute).UTC().Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/top/remotes?start=%s&end=%s&page=-1&page_size=999", start, end), nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"page":1`) || !strings.Contains(bodyText, `"page_size":200`) {
		t.Fatalf("expected normalized top-remote page metadata, got %s", bodyText)
	}
}

func TestRunBackgroundPrefetchWarmsEvidenceAndChains(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)

	minuteTime := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	minute := minuteTime.Unix()
	logLine := "2026-04-18T12:08:10Z /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443"
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte(logLine+"\n"), 0o644); err != nil {
		t.Fatalf("write ss log: %v", err)
	}

	pid := 1088
	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2048,
			BytesDown: 8192,
			PktsUp:    8,
			PktsDown:  11,
			FlowCount: 2,
		},
	}, nil); err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	summary := server.RunBackgroundPrefetch(ctx, BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 minuteTime.Add(30 * time.Second),
		EvidenceLookback:    20 * time.Minute,
		ChainLookback:       20 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        4,
		MaxScanLinesPerFile: 1000,
	})
	if summary.EvidenceRows == 0 {
		t.Fatalf("expected prefetched evidence rows, got %+v", summary)
	}
	if summary.UsageRows == 0 {
		t.Fatalf("expected prefetch to evaluate usage rows, got %+v", summary)
	}

	evidenceRows, err := server.store.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source:         evidenceSourceSS,
		StartTS:        minute - 120,
		EndTS:          minute + 120,
		HostNormalized: "chatgpt.com",
		TargetPort:     443,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query prefetched evidence: %v", err)
	}
	if len(evidenceRows) != 1 {
		t.Fatalf("expected 1 prefetched evidence row, got %+v", evidenceRows)
	}
	if evidenceRows[0].EntryPort != 12096 {
		t.Fatalf("expected entry port 12096 in evidence row, got %+v", evidenceRows[0])
	}

	chains, err := server.store.QueryUsageChainsForProcess(ctx, minute, &pid, "ss-server", "/usr/bin/ss-server", store.DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query prefetched chains: %v", err)
	}
	if len(chains) == 0 {
		t.Fatalf("expected prefetched chain rows, got %+v", chains)
	}
	if chains[0].TargetHostNormalized != "chatgpt.com" || chains[0].TargetPort == nil || *chains[0].TargetPort != 443 {
		t.Fatalf("unexpected prefetched chain row: %+v", chains[0])
	}
	if chains[0].TargetIP != "104.26.8.78" || chains[0].BytesTotal <= 0 || chains[0].FlowCount <= 0 {
		t.Fatalf("expected hydrated prefetched chain metrics, got %+v", chains[0])
	}
}

func TestRunBackgroundPrefetchDedupesEquivalentUsageQueries(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)

	minuteTime := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	minute := minuteTime.Unix()
	logLine := "2026-04-18T12:08:10Z /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443"
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte(logLine+"\n"), 0o644); err != nil {
		t.Fatalf("write ss log: %v", err)
	}

	pid := 1088
	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2048,
			BytesDown: 8192,
			PktsUp:    8,
			PktsDown:  11,
			FlowCount: 2,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionHeuristic,
		}: {
			BytesUp:   512,
			BytesDown: 1024,
			PktsUp:    2,
			PktsDown:  4,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed duplicated ss usage rows: %v", err)
	}

	summary := server.RunBackgroundPrefetch(ctx, BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 minuteTime.Add(30 * time.Second),
		EvidenceLookback:    20 * time.Minute,
		ChainLookback:       20 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        4,
		MaxScanLinesPerFile: 1000,
	})
	if summary.UsageRows != 1 {
		t.Fatalf("expected duplicated usage rows to prefetch once, got %+v", summary)
	}
}

func TestRunBackgroundPrefetchHonorsChainLookback(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	logDir := t.TempDir()
	setProcessLogDir(server, "ss-server", logDir)

	now := time.Date(2026, 4, 18, 12, 10, 30, 0, time.UTC)
	outsideMinute := now.Add(-21 * time.Minute).Truncate(time.Minute)
	lookbackMinute := now.Add(-10 * time.Minute).Truncate(time.Minute)
	recentMinute := now.Add(-time.Minute).Truncate(time.Minute)
	logLines := []string{
		fmt.Sprintf("%s /usr/bin/ss-server[27896]: [12096] connect to outside.example.com:443", outsideMinute.Add(10*time.Second).Format(time.RFC3339)),
		fmt.Sprintf("%s /usr/bin/ss-server[27896]: [12096] connect to lookback.example.com:443", lookbackMinute.Add(10*time.Second).Format(time.RFC3339)),
		fmt.Sprintf("%s /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443", recentMinute.Add(10*time.Second).Format(time.RFC3339)),
	}
	if err := os.WriteFile(filepath.Join(logDir, "ss-server.log"), []byte(strings.Join(logLines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write ss log: %v", err)
	}

	pid := 1088
	if err := server.store.FlushMinute(ctx, outsideMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    outsideMinute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "203.0.113.10",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 4096,
			PktsUp:    4,
			PktsDown:  8,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed outside usage row: %v", err)
	}
	if err := server.store.FlushMinute(ctx, lookbackMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    lookbackMinute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "203.0.113.20",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1536,
			BytesDown: 6144,
			PktsUp:    6,
			PktsDown:  9,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed lookback usage row: %v", err)
	}
	if err := server.store.FlushMinute(ctx, recentMinute.Unix(), map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    recentMinute.Unix(),
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2048,
			BytesDown: 8192,
			PktsUp:    8,
			PktsDown:  11,
			FlowCount: 2,
		},
	}, nil); err != nil {
		t.Fatalf("seed recent usage row: %v", err)
	}

	summary := server.RunBackgroundPrefetch(ctx, BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 now,
		EvidenceLookback:    20 * time.Minute,
		ChainLookback:       20 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        4,
		MaxScanLinesPerFile: 1000,
	})
	if summary.UsageRows != 2 {
		t.Fatalf("expected chain lookback to prefetch recent and lookback rows, got %+v", summary)
	}

	recentChains, err := server.store.QueryUsageChainsForProcess(ctx, recentMinute.Unix(), &pid, "ss-server", "/usr/bin/ss-server", store.DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query recent chains: %v", err)
	}
	if len(recentChains) == 0 {
		t.Fatalf("expected recent usage row to be prefetched into chains")
	}

	lookbackChains, err := server.store.QueryUsageChainsForProcess(ctx, lookbackMinute.Unix(), &pid, "ss-server", "/usr/bin/ss-server", store.DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query lookback chains: %v", err)
	}
	if len(lookbackChains) == 0 {
		t.Fatalf("expected usage row inside chain lookback to be prefetched into chains")
	}

	outsideChains, err := server.store.QueryUsageChainsForProcess(ctx, outsideMinute.Unix(), &pid, "ss-server", "/usr/bin/ss-server", store.DataSourceMinuteChain)
	if err != nil {
		t.Fatalf("query outside chains: %v", err)
	}
	if len(outsideChains) != 0 {
		t.Fatalf("expected usage row outside chain lookback to be skipped, got %+v", outsideChains)
	}
}

func TestRunBackgroundPrefetchFallsBackToShadowsocksJournal(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	setProcessLogDir(server, "ss-server", t.TempDir())
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		return []string{
			"2026-04-18T20:08:10+08:00 /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443",
			"2026-04-18T20:08:09+08:00 /usr/bin/ss-server[27896]: [12096] [udp] cache miss: chatgpt.com:443 <-> 203.0.113.24:52144",
		}, nil
	}

	minuteTime := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	minute := minuteTime.Unix()
	pid := 1088
	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         pid,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2048,
			BytesDown: 8192,
			PktsUp:    8,
			PktsDown:  11,
			FlowCount: 2,
		},
	}, nil); err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	summary := server.RunBackgroundPrefetch(ctx, BackgroundPrefetchOptions{
		Enabled:             true,
		Now:                 minuteTime.Add(30 * time.Second),
		EvidenceLookback:    20 * time.Minute,
		ChainLookback:       20 * time.Minute,
		ScanBudget:          2 * time.Second,
		MaxScanFiles:        4,
		MaxScanLinesPerFile: 1000,
	})
	if summary.EvidenceRows == 0 {
		t.Fatalf("expected journal fallback to import evidence rows, got %+v", summary)
	}
	if summary.ChainRows == 0 {
		t.Fatalf("expected journal fallback to hydrate chains, got %+v", summary)
	}

	evidenceRows, err := server.store.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source:         evidenceSourceSS,
		StartTS:        minute - 120,
		EndTS:          minute + 120,
		HostNormalized: "chatgpt.com",
		TargetPort:     443,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query prefetched journal evidence: %v", err)
	}
	if len(evidenceRows) == 0 {
		t.Fatalf("expected prefetched journal evidence rows")
	}
}

func TestLookupShadowsocksJournalEvidenceByEntryPortsBatchesStrictRead(t *testing.T) {
	server := newTestServer(t)
	journalCalls := 0
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		journalCalls++
		return []string{
			`2026-04-16T01:00:18+08:00 /usr/bin/ss-server[27896]: [12096] [udp] cache miss: chatgpt.com:443 <-> 74.7.227.153:52144`,
			`2026-04-16T01:00:19+08:00 /usr/bin/ss-server[27896]: [12097] [udp] cache miss: openai.com:443 <-> 74.7.227.154:52145`,
		}, nil
	}

	cst := time.FixedZone("CST", 8*3600)
	bucketTS := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()
	rowsByPort, notes, err := server.lookupShadowsocksJournalEvidenceByEntryPorts(context.Background(), bucketTS, []int{12096, 12097}, maxRelatedPeers*4)
	if err != nil {
		t.Fatalf("lookup batched shadowsocks journal evidence: %v", err)
	}
	if journalCalls != 1 {
		t.Fatalf("expected a single strict journal read for multiple ports, got %d", journalCalls)
	}
	if len(rowsByPort[12096]) == 0 || len(rowsByPort[12097]) == 0 {
		t.Fatalf("expected both entry ports to be hydrated, got %+v", rowsByPort)
	}
	notesText := strings.Join(notes, "\n")
	if !strings.Contains(notesText, "systemd journal") {
		t.Fatalf("expected journal fallback note, got %s", notesText)
	}
}

func TestLookupShadowsocksJournalEvidenceByEntryPortsFallsBackForRemainingPorts(t *testing.T) {
	server := newTestServer(t)
	journalCalls := 0
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		journalCalls++
		switch journalCalls {
		case 1:
			return []string{
				`2026-04-16T01:00:18+08:00 /usr/bin/ss-server[27896]: [12096] [udp] cache miss: chatgpt.com:443 <-> 74.7.227.153:52144`,
			}, nil
		case 2:
			return []string{
				`2026-04-16T01:12:18+08:00 /usr/bin/ss-server[27896]: [12097] [udp] cache miss: openai.com:443 <-> 74.7.227.154:52145`,
			}, nil
		default:
			return nil, nil
		}
	}

	cst := time.FixedZone("CST", 8*3600)
	bucketTS := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()
	rowsByPort, notes, err := server.lookupShadowsocksJournalEvidenceByEntryPorts(context.Background(), bucketTS, []int{12096, 12097}, maxRelatedPeers*4)
	if err != nil {
		t.Fatalf("lookup batched shadowsocks journal evidence with fallback: %v", err)
	}
	if journalCalls != 2 {
		t.Fatalf("expected strict and fallback journal reads, got %d", journalCalls)
	}
	if len(rowsByPort[12096]) == 0 || len(rowsByPort[12097]) == 0 {
		t.Fatalf("expected strict+fallback rows for both ports, got %+v", rowsByPort)
	}
	notesText := strings.Join(notes, "\n")
	if !strings.Contains(notesText, "±15 分钟窗口") {
		t.Fatalf("expected fallback window note, got %s", notesText)
	}
}

func TestQueryUsageRowsForPrefetchCapsCandidateScan(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	start := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		minute := start.Add(time.Duration(i) * time.Minute)
		if err := server.store.FlushMinute(ctx, minute.Unix(), map[model.UsageKey]model.UsageDelta{
			{
				MinuteTS:    minute.Unix(),
				Proto:       "tcp",
				Direction:   model.DirectionOut,
				PID:         1000 + i,
				Comm:        "ss-server",
				Exe:         "/usr/bin/ss-server",
				LocalPort:   40000 + i,
				RemoteIP:    fmt.Sprintf("203.0.113.%d", 10+i),
				RemotePort:  443,
				Attribution: model.AttributionExact,
			}: {
				BytesUp:   int64(1024 + i),
				BytesDown: int64(2048 + i),
				PktsUp:    2,
				PktsDown:  4,
				FlowCount: 1,
			},
		}, nil); err != nil {
			t.Fatalf("seed usage row %d: %v", i, err)
		}
	}

	rows, truncated, err := server.queryUsageRowsForPrefetch(ctx, start.Add(-time.Minute), start.Add(6*time.Minute), 3)
	if err != nil {
		t.Fatalf("query prefetch rows: %v", err)
	}
	if !truncated {
		t.Fatalf("expected candidate scan to report truncation")
	}
	if len(rows) != 3 {
		t.Fatalf("expected prefetch row cap to stop at 3 rows, got %d", len(rows))
	}
	if rows[0].TimeBucket <= rows[1].TimeBucket || rows[1].TimeBucket <= rows[2].TimeBucket {
		t.Fatalf("expected rows ordered from newest to oldest, got %+v", rows)
	}
}

func TestProcessesAndOverviewUseRuntimeView(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()
	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "203.0.113.24",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1024,
			BytesDown: 2048,
			PktsUp:    3,
			PktsDown:  5,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed process suggestion row: %v", err)
	}

	processReq := httptest.NewRequest(http.MethodGet, "/api/v1/processes", nil)
	processRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(processRec, processReq)
	if processRec.Code != http.StatusOK {
		t.Fatalf("unexpected process status: %d", processRec.Code)
	}
	processBody, _ := io.ReadAll(processRec.Body)
	if !strings.Contains(string(processBody), "\"pid\":42") {
		t.Fatalf("unexpected process body: %s", string(processBody))
	}
	if !strings.Contains(string(processBody), "\"comm\":\"nginx\"") {
		t.Fatalf("expected historical process suggestion in body: %s", string(processBody))
	}

	overviewReq := httptest.NewRequest(http.MethodGet, "/api/v1/stats/overview?range=1h", nil)
	overviewRec := httptest.NewRecorder()
	server.Handler().ServeHTTP(overviewRec, overviewReq)
	if overviewRec.Code != http.StatusOK {
		t.Fatalf("unexpected overview status: %d", overviewRec.Code)
	}
	overviewBody, _ := io.ReadAll(overviewRec.Body)
	if !strings.Contains(string(overviewBody), "\"active_connections\":3") || !strings.Contains(string(overviewBody), "\"active_processes\":1") {
		t.Fatalf("unexpected overview body: %s", string(overviewBody))
	}
}

func TestUsageExplainCorrelatesShadowsocksPeers(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 16, 8, 30, 0, 0, time.UTC).Unix()

	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   8388,
			RemoteIP:    "203.0.113.24",
			RemotePort:  52144,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   182000,
			BytesDown: 1240000,
			PktsUp:    320,
			PktsDown:  960,
			FlowCount: 3,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1918000,
			BytesDown: 6144000,
			PktsUp:    980,
			PktsDown:  1620,
			FlowCount: 4,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed usage rows: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, "\"source_ips\":[\"203.0.113.24\"]") {
		t.Fatalf("expected source ip in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, "\"target_ips\":[\"142.250.72.14\"]") {
		t.Fatalf("expected target ip in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, "Shadowsocks") {
		t.Fatalf("expected shadowsocks note in body: %s", bodyText)
	}
}

func TestUsageExplainValidatesInput(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/usage/explain?direction=out", nil)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), "invalid_usage_explain_query") {
		t.Fatalf("unexpected body: %s", string(body))
	}
}

func TestUsageExplainReadsConfiguredNginxLogDir(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "nginx", logDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 0, 20, 0, cst).Unix()
	logLines := strings.Join([]string{
		`127.0.0.1 - - [16/Apr/2026:01:00:20 +0800] "GET /cloud HTTP/2.0" 301 162 "https://paris.escape.ac.cn/" "Mozilla/5.0" "-"`,
		`74.7.227.153 - - [16/Apr/2026:01:00:20 +0800] "GET /cloud/ HTTP/2.0" 401 12 "https://paris.escape.ac.cn" "Mozilla/5.0" "-"`,
		`127.0.0.1 - - [16/Apr/2026:01:00:22 +0800] "GET /en/ HTTP/2.0" 200 8058 "https://paris.escape.ac.cn/" "Mozilla/5.0" "-"`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(logDir, "blog_access.log-20260416"), []byte(logLines+"\n"), 0o644); err != nil {
		t.Fatalf("write nginx log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   80,
			RemoteIP:    "74.7.227.153",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   122880,
			BytesDown: 896000,
			PktsUp:    250,
			PktsDown:  530,
			FlowCount: 5,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed nginx usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=80&remote_ip=74.7.227.153&remote_port=41220&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"nginx_requests":[{`) {
		t.Fatalf("expected nginx_requests in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"path":"/cloud/"`) {
		t.Fatalf("expected nginx path in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `"path":"/cloud"`) {
		t.Fatalf("unexpected localhost path matched: %s", bodyText)
	}
}

func TestUsageExplainReadsCompressedRotatedNginxLog(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "nginx", logDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 20, 0, 0, cst).Unix()
	logLine := `74.7.227.153 - - [16/Apr/2026:01:10:20 +0800] "GET /cloud/ HTTP/2.0" 401 12 "https://paris.escape.ac.cn" "Mozilla/5.0" "-"`
	if err := writeGzipFile(filepath.Join(logDir, "blog_access.log-20260416.gz"), logLine+"\n"); err != nil {
		t.Fatalf("write nginx gzip log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "74.7.227.153",
			RemotePort:  36892,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   100,
			BytesDown: 100,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed nginx usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=443&remote_ip=74.7.227.153&remote_port=36892&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"path":"/cloud/"`) {
		t.Fatalf("expected nginx path in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `已回退到 ±15 分钟窗口`) {
		t.Fatalf("expected fallback note in body: %s", bodyText)
	}
}

func TestUsageExplainSummarizesNginxBotRequests(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "nginx", logDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 8, 0, 0, cst).Unix()
	logLines := strings.Join([]string{
		`127.0.0.1 - - [16/Apr/2026:01:07:56 +0800] "GET /apod/2023/12/AstroPH-2023-12 HTTP/2.0" 200 54614 "https://paris.escape.ac.cn/sitemap.xml" "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
		`127.0.0.1 - - [16/Apr/2026:01:08:12 +0800] "GET /apod/2023/12/AstroPH-2023-12 HTTP/2.0" 200 54614 "https://paris.escape.ac.cn/sitemap.xml" "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
		`127.0.0.1 - - [16/Apr/2026:01:08:15 +0800] "GET /en/apod/2023/12/AstroPH-2023-12 HTTP/2.0" 404 1039 "https://paris.escape.ac.cn/apod/2023/12/AstroPH-2023-12" "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(logDir, "blog_access.log-20260416"), []byte(logLines+"\n"), 0o644); err != nil {
		t.Fatalf("write nginx log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         3312,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   443,
			RemoteIP:    "74.7.227.153",
			RemotePort:  36892,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   100,
			BytesDown: 100,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed nginx usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=443&remote_ip=74.7.227.153&remote_port=36892&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"bot":"GPTBot"`) {
		t.Fatalf("expected GPTBot in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"count":2`) {
		t.Fatalf("expected aggregated count in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `访问端识别`) {
		t.Fatalf("expected bot summary note in body: %s", bodyText)
	}
}

func TestUsageExplainReadsShadowsocksLogEvidence(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()
	ssLine := `2026-04-16T01:00:20+08:00 level=info msg="relay" client=74.7.227.153 target=104.26.8.78:443`
	if err := writeGzipFile(filepath.Join(ssLogDir, "ss-server.log-20260416.gz"), ssLine+"\n"); err != nil {
		t.Fatalf("write ss gzip log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   8388,
			RemoteIP:    "74.7.227.153",
			RemotePort:  52144,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   50,
			BytesDown: 80,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=8388&remote_ip=74.7.227.153&remote_port=52144&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"target_ips":["104.26.8.78"]`) {
		t.Fatalf("expected ss target ip from logs in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"chains":[{`) || !strings.Contains(bodyText, `"source_ip":"74.7.227.153"`) || !strings.Contains(bodyText, `"target_ip":"104.26.8.78"`) {
		t.Fatalf("expected ss chain summary in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"chain_id":"usage_chain_1m|`) {
		t.Fatalf("expected canonical chain id in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("expected ss hit note in body: %s", bodyText)
	}
}

func TestUsageExplainFallsBackToShadowsocksJournal(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		return []string{
			`2026-04-16T01:00:18+08:00 /usr/bin/ss-server[27896]: [12096] [udp] cache miss: chatgpt.com:443 <-> 74.7.227.153:52144`,
			`2026-04-16T01:00:20+08:00 /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443`,
		}, nil
	}

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   50,
			BytesDown: 80,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=104.26.8.78&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"source_ips":["74.7.227.153"]`) {
		t.Fatalf("expected source ip from journal fallback in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `systemd journal`) {
		t.Fatalf("expected systemd journal fallback note in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"target_host":"chatgpt.com"`) {
		t.Fatalf("expected target host from journal fallback in body: %s", bodyText)
	}
}

func TestUsageExplainSkipsShadowsocksJournalWhenFallbackDisabled(t *testing.T) {
	server := newTestServer(t)
	server.enableSSJournalFallback = false
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	journalCalls := 0
	server.readShadowsocksJournal = func(context.Context, time.Time, time.Time) ([]string, error) {
		journalCalls++
		return []string{
			`2026-04-16T01:00:20+08:00 /usr/bin/ss-server[27896]: [12096] connect to chatgpt.com:443`,
		}, nil
	}

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()
	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   50,
			BytesDown: 80,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=104.26.8.78&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if journalCalls != 0 {
		t.Fatalf("expected disabled shadowsocks journal fallback to skip journal reads, got %d", journalCalls)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, "systemd journal") {
		t.Fatalf("expected disabled shadowsocks journal fallback to omit journal notes: %s", bodyText)
	}
}

func TestUsageExplainShadowsocksDoesNotFallbackToOtherDirs(t *testing.T) {
	server := newTestServer(t)
	configuredDir := t.TempDir()
	otherDir := t.TempDir()
	setProcessLogDir(server, "ss-server", configuredDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 0, 0, 0, cst).Unix()
	ssLine := `2026-04-16T01:00:20+08:00 level=info msg="relay" client=74.7.227.153 target=104.26.8.78:443`
	if err := writeGzipFile(filepath.Join(otherDir, "ss-server.log-20260416.gz"), ssLine+"\n"); err != nil {
		t.Fatalf("write ss gzip log in non-configured dir: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   8388,
			RemoteIP:    "74.7.227.153",
			RemotePort:  52144,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   50,
			BytesDown: 80,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=8388&remote_ip=74.7.227.153&remote_port=52144&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("did not expect ss hit note via non-configured directory fallback: %s", bodyText)
	}
	if !strings.Contains(bodyText, `中未命中 SS 相关日志`) {
		t.Fatalf("expected strict no-hit note for configured path: %s", bodyText)
	}
}

func TestUsageExplainOpenrestyUsesGenericProcessLogs(t *testing.T) {
	server := newTestServer(t)
	logDir := t.TempDir()
	setProcessLogDir(server, "openresty", logDir)

	cst := time.FixedZone("CST", 8*3600)
	minute := time.Date(2026, 4, 16, 1, 5, 0, 0, cst).Unix()
	line := `2026-04-16T01:05:20+08:00 level=info msg="proxy" client=74.7.227.153 target=104.26.8.78:443`
	if err := os.WriteFile(filepath.Join(logDir, "openresty.log-20260416"), []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write openresty log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         2233,
			Comm:        "openresty",
			Exe:         "/usr/bin/openresty",
			LocalPort:   443,
			RemoteIP:    "104.26.8.78",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   1200,
			BytesDown: 2400,
			PktsUp:    10,
			PktsDown:  16,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed openresty usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=2233&comm=openresty&exe=/usr/bin/openresty&local_port=443&remote_ip=104.26.8.78&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `openresty 日志命中`) {
		t.Fatalf("expected generic process log hit note for openresty: %s", bodyText)
	}
	if strings.Contains(bodyText, `"evidence":"ss-log"`) {
		t.Fatalf("did not expect ss-log chain evidence for openresty process: %s", bodyText)
	}
	if !strings.Contains(bodyText, `"evidence":"openresty-log"`) {
		t.Fatalf("expected openresty-specific chain evidence label: %s", bodyText)
	}
	if strings.Contains(bodyText, `Nginx 日志命中`) {
		t.Fatalf("did not expect nginx parser note for openresty process: %s", bodyText)
	}
}

func TestUsageExplainUnknownInboundDoesNotUseShadowsocksFallback(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	now := time.Now().In(time.Local).Add(-30 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	line := fmt.Sprintf("%s level=info msg=\"relay\" client=159.226.171.226 target=172.217.22.170:443", now.Format(time.RFC3339))
	logPath := filepath.Join(ssLogDir, fmt.Sprintf("ss-server.log-%s", now.Format("20060102")))
	if err := os.WriteFile(logPath, []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write ss relay log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   12096,
			RemoteIP:    "159.226.171.226",
			RemotePort:  44598,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   0,
			BytesDown: 0,
			PktsUp:    0,
			PktsDown:  0,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed unknown inbound usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=0&local_port=12096&remote_ip=159.226.171.226&remote_port=44598&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, `"target_ips":["172.217.22.170"]`) {
		t.Fatalf("did not expect fallback target ip in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `当前进程归因缺失`) {
		t.Fatalf("did not expect unknown-process ss fallback note in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("did not expect ss log hit note in body: %s", bodyText)
	}
}

func TestUsageExplainUsesConfiguredProcessLogDirForFrps(t *testing.T) {
	server := newTestServer(t)
	frpsLogDir := t.TempDir()
	server.processLogDirs = map[string]string{"frps": frpsLogDir}

	now := time.Now().In(time.Local).Add(-20 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	line := fmt.Sprintf("%s [I] [proxy.go:123] [7f3f] frps bridge src=213.209.159.228:6010 dst=172.217.22.170:443", now.Format(time.RFC3339))
	logPath := filepath.Join(frpsLogDir, fmt.Sprintf("frps.log-%s", now.Format("20060102")))
	if err := os.WriteFile(logPath, []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write frps log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         936,
			Comm:        "frps",
			Exe:         "/usr/local/bin/frps",
			LocalPort:   6010,
			RemoteIP:    "213.209.159.228",
			RemotePort:  55352,
			Attribution: model.AttributionHeuristic,
		}: {
			BytesUp:   0,
			BytesDown: 0,
			PktsUp:    0,
			PktsDown:  0,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed frps usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=936&comm=frps&exe=/usr/local/bin/frps&local_port=6010&remote_ip=213.209.159.228&remote_port=55352&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"target_ips":["172.217.22.170"]`) {
		t.Fatalf("expected frps target ip from configured log in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `frps 日志命中`) {
		t.Fatalf("expected frps log hit note in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("did not expect ss log note for frps process in body: %s", bodyText)
	}
}

func TestUsageExplainFallsBackToCachedEvidenceWhenProcessLogDirIsUnconfigured(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 17, 8, 0, 0, 0, time.UTC).Unix()

	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         936,
			Comm:        "frps",
			Exe:         "/usr/local/bin/frps",
			LocalPort:   6010,
			RemoteIP:    "213.209.159.228",
			RemotePort:  55352,
			Attribution: model.AttributionHeuristic,
		}: {
			BytesUp:   0,
			BytesDown: 0,
			PktsUp:    0,
			PktsDown:  0,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed frps usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=936&comm=frps&exe=/usr/local/bin/frps&local_port=6010&remote_ip=213.209.159.228&remote_port=55352&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `进程 frps 未配置日志目录，当前仅回放已缓存证据，无法同步扫描文件。`) {
		t.Fatalf("expected cached-evidence fallback note for unconfigured process: %s", bodyText)
	}
	if strings.Contains(bodyText, `日志命中`) {
		t.Fatalf("did not expect log hit note for unconfigured process: %s", bodyText)
	}
}

func TestUsageExplainUnknownInboundSelectsCounterpartFromUsage(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 17, 7, 10, 0, 0, time.UTC).Unix()

	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         0,
			Comm:        "",
			Exe:         "",
			LocalPort:   12096,
			RemoteIP:    "159.226.171.226",
			RemotePort:  44598,
			Attribution: model.AttributionUnknown,
		}: {
			BytesUp:   0,
			BytesDown: 0,
			PktsUp:    0,
			PktsDown:  0,
			FlowCount: 1,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   52098,
			RemoteIP:    "172.217.22.170",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   210000,
			BytesDown: 360000,
			PktsUp:    220,
			PktsDown:  280,
			FlowCount: 3,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         2010,
			Comm:        "curl",
			Exe:         "/usr/bin/curl",
			LocalPort:   43300,
			RemoteIP:    "203.0.113.11",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2000,
			BytesDown: 2200,
			PktsUp:    8,
			PktsDown:  10,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed usage rows: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=0&local_port=12096&remote_ip=159.226.171.226&remote_port=44598&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"target_ips":["172.217.22.170"`) {
		t.Fatalf("expected counterpart target ip from usage in body: %s", bodyText)
	}
}

func TestUsageExplainReadsShadowsocksConnectHostFromConfiguredGlobPath(t *testing.T) {
	server := newTestServer(t)
	ssRootDir := t.TempDir()
	ssLogDir := filepath.Join(ssRootDir, "shadowsocks")
	if err := os.MkdirAll(ssLogDir, 0o755); err != nil {
		t.Fatalf("make ss log subdir: %v", err)
	}
	setProcessLogDir(server, "ss-server", filepath.Join(ssLogDir, "server*"))

	now := time.Now().In(time.Local).Add(-30 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	ssLine := fmt.Sprintf("%s 217 /usr/local/bin/ss-server[27896]: [12096] connect to chatgpt.com:443", now.Format("Jan _2 15:04:05"))
	ssFile := filepath.Join(ssLogDir, fmt.Sprintf("server.log-%s", now.Format("20060102")))
	if err := os.WriteFile(ssFile, []byte(ssLine+"\n"), 0o644); err != nil {
		t.Fatalf("write ss connect log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   30,
			BytesDown: 60,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `SS 目标主机候选`) {
		t.Fatalf("expected ss host candidates in notes: %s", bodyText)
	}
	if !strings.Contains(bodyText, `chatgpt.com:443`) {
		t.Fatalf("expected chatgpt host candidate: %s", bodyText)
	}
	if !strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("expected ss log hit note: %s", bodyText)
	}
}

func TestUsageExplainBuildsShadowsocksChainFromSharedServerAndObfsLogs(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "obfs-server", ssLogDir)

	now := time.Now().In(time.Local).Add(-20 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	serverLine := fmt.Sprintf("%s 217 /usr/local/bin/ss-server[27896]: [12096] connect to clients3.google.com:443", now.Format("Jan _2 15:04:05"))
	obfsLine := fmt.Sprintf("%s 217 /usr/local/bin/obfs-server[27898]: [12096] accepted connection from 203.0.113.24", now.Format("Jan _2 15:04:05"))
	if err := os.WriteFile(filepath.Join(ssLogDir, fmt.Sprintf("server.log-%s", now.Format("20060102"))), []byte(serverLine+"\n"), 0o644); err != nil {
		t.Fatalf("write ss connect log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ssLogDir, fmt.Sprintf("obfs.log-%s", now.Format("20060102"))), []byte(obfsLine+"\n"), 0o644); err != nil {
		t.Fatalf("write obfs source log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   512,
			BytesDown: 2048,
			PktsUp:    2,
			PktsDown:  4,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `clients3.google.com`) {
		t.Fatalf("expected target host from shared server log: %s", bodyText)
	}
	if !strings.Contains(bodyText, `203.0.113.24`) {
		t.Fatalf("expected source ip from obfs entry-port log: %s", bodyText)
	}
	if !strings.Contains(bodyText, `SS/obfs 入口日志命中`) {
		t.Fatalf("expected entry-port source note: %s", bodyText)
	}
}

func TestUsageExplainReadsLegacyShadowsocksEvidenceSourcesFromCacheWithoutConfiguredLogDir(t *testing.T) {
	server := newTestServer(t)

	now := time.Now().UTC().Truncate(time.Minute)
	minute := now.Unix()
	if err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   256,
			BytesDown: 1024,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	status := 200
	if err := server.store.UpsertLogEvidenceBatch(context.Background(), []model.LogEvidence{
		{
			Source:      "proc:obfs-server",
			EventTS:     minute + 5,
			Host:        "chatgpt.com",
			Path:        "443",
			Method:      "connect",
			EntryPort:   12096,
			TargetPort:  443,
			Status:      &status,
			Message:     "connect to chatgpt.com:443",
			Fingerprint: "legacy-ss-connect",
		},
		{
			Source:      "proc:obfs-server",
			EventTS:     minute + 6,
			ClientIP:    "203.0.113.24",
			Method:      "accept",
			EntryPort:   12096,
			Message:     "[12096] accepted connection from 203.0.113.24",
			Fingerprint: "legacy-ss-source",
		},
	}); err != nil {
		t.Fatalf("seed legacy shadowsocks evidence: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `chatgpt.com`) {
		t.Fatalf("expected target host from legacy cache source: %s", bodyText)
	}
	if !strings.Contains(bodyText, `203.0.113.24`) {
		t.Fatalf("expected source ip from legacy cache source: %s", bodyText)
	}
	if !strings.Contains(bodyText, `SS 日志命中`) || !strings.Contains(bodyText, `SS/obfs 入口日志命中`) {
		t.Fatalf("expected shadowsocks cache notes in body: %s", bodyText)
	}
	if strings.Contains(bodyText, `已跳过日志检索`) {
		t.Fatalf("expected cached evidence replay without skip note: %s", bodyText)
	}
}

func TestUsageExplainSkipsSynchronousFileScanByDefault(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	now := time.Now().In(time.Local).Add(-30 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	line := fmt.Sprintf("%s 217 /usr/local/bin/ss-server[27896]: [12096] connect to chatgpt.com:443", now.Format("Jan _2 15:04:05"))
	logPath := filepath.Join(ssLogDir, fmt.Sprintf("ss-server.log-%s", now.Format("20060102")))
	if err := os.WriteFile(logPath, []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write ss connect log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   30,
			BytesDown: 60,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	url := fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `已跳过日志缓存回放`) {
		t.Fatalf("expected default explain request to skip sync scan: %s", bodyText)
	}
	if strings.Contains(bodyText, `chatgpt.com:443`) || strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("expected no live log scan on default request: %s", bodyText)
	}
}

func TestUsageExplainReplaysHourlyPersistedChains(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	hour := time.Date(2026, 4, 16, 8, 0, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := server.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        hour.Add(8 * time.Minute).Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         3,
			EvidenceCount:     2,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "chain-fp-1",
			SampleMessage:     "connect to chatgpt.com:443",
			SampleTime:        hour.Add(8 * time.Minute).Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert minute chain: %v", err)
	}
	if err := server.store.AggregateHour(ctx, hour.Add(8*time.Minute)); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	url := fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1h&proto=tcp&direction=out&comm=ss-server&local_port=12096&remote_ip=142.250.72.14", hour.Unix())
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"chain_id":"usage_chain_1h|`) {
		t.Fatalf("expected hourly chain replay in body: %s", bodyText)
	}
	if !strings.Contains(bodyText, `小时聚合数据`) {
		t.Fatalf("expected hourly replay note in body: %s", bodyText)
	}
}

func TestUsageExplainFallsBackAcrossPersistedChainSourcesAfterFiltering(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	hour := time.Date(2026, 4, 16, 8, 0, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := server.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        hour.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "203.0.113.99",
			TargetHost:        "elsewhere.example",
			TargetPort:        &targetPort,
			BytesTotal:        1024,
			FlowCount:         1,
			EvidenceCount:     1,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "minute-irrelevant",
			SampleMessage:     "connect to elsewhere.example:443",
			SampleTime:        hour.Unix(),
		},
		{
			TimeBucket:        hour.Add(8 * time.Minute).Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         3,
			EvidenceCount:     2,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "hour-relevant",
			SampleMessage:     "connect to chatgpt.com:443",
			SampleTime:        hour.Add(8 * time.Minute).Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert persisted chains: %v", err)
	}
	if err := server.store.AggregateHour(ctx, hour.Add(8*time.Minute)); err != nil {
		t.Fatalf("aggregate hour: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=12096&remote_ip=142.250.72.14&remote_port=443", hour.Unix()),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"chain_id":"usage_chain_1h|`) {
		t.Fatalf("expected explain to fall back to filtered hourly persisted chain: %s", bodyText)
	}
	if strings.Contains(bodyText, `elsewhere.example`) {
		t.Fatalf("did not expect unrelated minute chain to block later fallback: %s", bodyText)
	}
}

func TestUsageExplainDoesNotReplayPersistedOutboundChainOnlyBySharedTargetPort(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := server.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "203.0.113.99",
			TargetHost:        "elsewhere.example",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         2,
			EvidenceCount:     1,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "fp-mismatch",
			SampleMessage:     "connect to elsewhere.example:443",
			SampleTime:        minute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert mismatched persisted chain: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute.Unix()),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, `"target_ip":"203.0.113.99"`) || strings.Contains(bodyText, `elsewhere.example`) {
		t.Fatalf("did not expect shared-target-port persisted chain replay: %s", bodyText)
	}
}

func TestUsageExplainDoesNotReplayPersistedOutboundChainWithMismatchedLocalPort(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := server.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.24",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "chatgpt.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         2,
			EvidenceCount:     1,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "fp-wrong-entry-port",
			SampleMessage:     "connect to chatgpt.com:443",
			SampleTime:        minute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert mismatched local-port persisted chain: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute.Unix()),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, `"target_ip":"142.250.72.14"`) || strings.Contains(bodyText, `chatgpt.com`) {
		t.Fatalf("did not expect persisted outbound chain replay with mismatched local port: %s", bodyText)
	}
}

func TestUsageExplainDoesNotReplayPersistedInboundChainOnlyBySharedEntryPort(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 18, 12, 8, 0, 0, time.UTC)
	pid := 1088
	exe := "/usr/bin/ss-server"
	entryPort := 12096
	targetPort := 443

	if err := server.store.UpsertUsageChains(ctx, []model.UsageChainRecord{
		{
			TimeBucket:        minute.Unix(),
			PID:               &pid,
			Comm:              "ss-server",
			Exe:               &exe,
			SourceIP:          "203.0.113.77",
			EntryPort:         &entryPort,
			TargetIP:          "142.250.72.14",
			TargetHost:        "www.google.com",
			TargetPort:        &targetPort,
			BytesTotal:        4096,
			FlowCount:         2,
			EvidenceCount:     1,
			EvidenceSource:    "ss-log",
			Confidence:        "high",
			SampleFingerprint: "fp-wrong-source",
			SampleMessage:     "relay client=203.0.113.77 target=www.google.com:443",
			SampleTime:        minute.Unix(),
		},
	}); err != nil {
		t.Fatalf("upsert mismatched inbound persisted chain: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=in&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=12096&remote_ip=159.226.171.226&remote_port=44598", minute.Unix()),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if strings.Contains(bodyText, `"source_ip":"203.0.113.77"`) || strings.Contains(bodyText, `"target_ip":"142.250.72.14"`) {
		t.Fatalf("did not expect shared-entry-port persisted chain replay: %s", bodyText)
	}
}

func TestUsageExplainFiltersCurrentTupleByRemotePort(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 17, 7, 10, 0, 0, time.UTC).Unix()

	err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "198.51.100.44",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   2048,
			BytesDown: 8192,
			PktsUp:    8,
			PktsDown:  11,
			FlowCount: 2,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "198.51.100.44",
			RemotePort:  8443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   512,
			BytesDown: 1024,
			PktsUp:    2,
			PktsDown:  4,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed usage rows: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=198.51.100.44&remote_port=443", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `"remote_port":443`) {
		t.Fatalf("expected explain response to include the anchored remote port: %s", bodyText)
	}
	if strings.Contains(bodyText, `"remote_port":8443`) {
		t.Fatalf("did not expect same-IP different-port usage to leak into related peers: %s", bodyText)
	}
}

func TestUsageExplainReusesCachedHostOnlyShadowsocksEvidence(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	now := time.Now().In(time.Local).Add(-30 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	line := fmt.Sprintf("%s 217 /usr/local/bin/ss-server[27896]: [12096] connect to chatgpt.com:443", now.Format("Jan _2 15:04:05"))
	logPath := filepath.Join(ssLogDir, fmt.Sprintf("ss-server.log-%s", now.Format("20060102")))
	if err := os.WriteFile(logPath, []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write ss connect log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "142.250.72.14",
			RemotePort:  443,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   30,
			BytesDown: 60,
			PktsUp:    1,
			PktsDown:  1,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	url := fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443&scan=1", minute)
	first := httptest.NewRecorder()
	server.Handler().ServeHTTP(first, httptest.NewRequest(http.MethodGet, url, nil))
	if first.Code != http.StatusOK {
		t.Fatalf("unexpected first status: %d", first.Code)
	}
	if err := os.Remove(logPath); err != nil {
		t.Fatalf("remove ss log after cache seed: %v", err)
	}

	second := httptest.NewRecorder()
	server.Handler().ServeHTTP(second, httptest.NewRequest(http.MethodGet, url, nil))
	if second.Code != http.StatusOK {
		t.Fatalf("unexpected second status: %d", second.Code)
	}
	body, _ := io.ReadAll(second.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `chatgpt.com:443`) {
		t.Fatalf("expected host-only evidence to come from cache: %s", bodyText)
	}
}

func TestUsageExplainConfirmsShadowsocksSourceIPFromUDPCacheMiss(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	setProcessLogDir(server, "ss-server", ssLogDir)

	now := time.Now().In(time.Local).Add(-20 * time.Second).Truncate(time.Second)
	minute := now.Unix()
	line := fmt.Sprintf("%s 217 /usr/local/bin/ss-server[27896]: [12096] [udp] cache miss: time.apple.com:123 <-> 203.0.113.24:42683", now.Format("Jan _2 15:04:05"))
	if err := os.WriteFile(filepath.Join(ssLogDir, fmt.Sprintf("ss-server.log-%s", now.Format("20060102"))), []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write ss udp log: %v", err)
	}

	err := server.store.FlushMinute(context.Background(), minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "udp",
			Direction:   model.DirectionOut,
			PID:         1088,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   47920,
			RemoteIP:    "17.253.144.10",
			RemotePort:  123,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   120,
			BytesDown: 220,
			PktsUp:    2,
			PktsDown:  3,
			FlowCount: 1,
		},
	}, nil)
	if err != nil {
		t.Fatalf("seed ss usage row: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=udp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=17.253.144.10&remote_port=123&scan=1", minute),
		nil,
	)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyText := string(body)
	if !strings.Contains(bodyText, `SS 来源IP确认`) {
		t.Fatalf("expected ss source confirmation note: %s", bodyText)
	}
	if !strings.Contains(bodyText, `203.0.113.24`) {
		t.Fatalf("expected confirmed source ip in body: %s", bodyText)
	}
}
