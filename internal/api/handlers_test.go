package api

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	}, nil, embed.StaticFS(), cfg.NginxLogDir, cfg.SSLogDir)
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

func TestProcessesAndOverviewUseRuntimeView(t *testing.T) {
	server := newTestServer(t)

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute),
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
	server.nginxLogDir = logDir

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=80&remote_ip=74.7.227.153&remote_port=41220", minute),
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
	server.nginxLogDir = logDir

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=443&remote_ip=74.7.227.153&remote_port=36892", minute),
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
	server.nginxLogDir = logDir

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=3312&comm=nginx&exe=/usr/sbin/nginx&local_port=443&remote_ip=74.7.227.153&remote_port=36892", minute),
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
	server.ssLogDir = ssLogDir

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=in&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=8388&remote_ip=74.7.227.153&remote_port=52144", minute),
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
	if !strings.Contains(bodyText, `SS 日志命中`) {
		t.Fatalf("expected ss hit note in body: %s", bodyText)
	}
}

func TestUsageExplainReadsShadowsocksConnectHostFromSubdir(t *testing.T) {
	server := newTestServer(t)
	ssRootDir := t.TempDir()
	server.ssLogDir = ssRootDir
	ssLogDir := filepath.Join(ssRootDir, "shadowsocks")
	if err := os.MkdirAll(ssLogDir, 0o755); err != nil {
		t.Fatalf("make ss log subdir: %v", err)
	}

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443", minute),
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
	if !strings.Contains(bodyText, `SS 日志目录自动回退到`) {
		t.Fatalf("expected ss log subdir fallback note: %s", bodyText)
	}
}

func TestUsageExplainConfirmsShadowsocksSourceIPFromUDPCacheMiss(t *testing.T) {
	server := newTestServer(t)
	ssLogDir := t.TempDir()
	server.ssLogDir = ssLogDir

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
		fmt.Sprintf("/api/v1/usage/explain?ts=%d&proto=udp&direction=out&pid=1088&comm=ss-server&exe=/usr/bin/ss-server&local_port=47920&remote_ip=17.253.144.10&remote_port=123", minute),
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
