package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"traffic-go/internal/config"
	"traffic-go/internal/embed"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

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
	}, nil, embed.StaticFS())
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
