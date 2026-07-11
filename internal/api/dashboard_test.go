package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"traffic-go/internal/model"
)

func TestDashboardEndpointBuildsAllPanelsFromSharedSummary(t *testing.T) {
	server := newTestServer(t)
	minute := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Minute)
	usage := map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS: minute.Unix(), Proto: "tcp", Direction: model.DirectionOut,
			PID: 42, Comm: "curl", Exe: "/usr/bin/curl", LocalPort: 50500,
			RemoteIP: "1.1.1.1", RemotePort: 443, Attribution: model.AttributionExact,
		}: {BytesUp: 100, BytesDown: 20, FlowCount: 1},
		{
			MinuteTS: minute.Add(time.Minute).Unix(), Proto: "tcp", Direction: model.DirectionIn,
			PID: 7, Comm: "nginx", Exe: "/usr/sbin/nginx", LocalPort: 443,
			RemoteIP: "2.2.2.2", RemotePort: 50100, Attribution: model.AttributionExact,
		}: {BytesUp: 30, BytesDown: 200, FlowCount: 2},
	}
	if err := server.store.FlushMinute(context.Background(), minute.Unix(), usage, nil); err != nil {
		t.Fatalf("seed dashboard usage: %v", err)
	}

	url := fmt.Sprintf("/api/v1/dashboard?start=%d&end=%d&bucket=5m", minute.Unix(), minute.Add(2*time.Minute).Unix())
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var body struct {
		DataSource string `json:"data_source"`
		Data       struct {
			Overview           model.OverviewStats     `json:"overview"`
			Timeseries         []model.TimeseriesPoint `json:"timeseries"`
			TopProcesses       []model.ProcessSummary  `json:"top_processes"`
			TopInboundRemotes  []model.RemoteSummary   `json:"top_inbound_remotes"`
			TopOutboundRemotes []model.RemoteSummary   `json:"top_outbound_remotes"`
			TopPorts           []model.TopEntry        `json:"top_ports"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode dashboard response: %v", err)
	}
	if body.DataSource != "usage_1m" {
		t.Fatalf("unexpected data source: %s", body.DataSource)
	}
	if body.Data.Overview.BytesUp != 130 || body.Data.Overview.BytesDown != 220 || body.Data.Overview.FlowCount != 3 {
		t.Fatalf("unexpected overview: %+v", body.Data.Overview)
	}
	if body.Data.Overview.ActiveConnections != 3 || body.Data.Overview.ActiveProcesses != 1 {
		t.Fatalf("unexpected live stats: %+v", body.Data.Overview)
	}
	if len(body.Data.Timeseries) != 2 || len(body.Data.TopProcesses) != 2 || len(body.Data.TopPorts) != 2 {
		t.Fatalf("unexpected shared panels: %+v", body.Data)
	}
	if len(body.Data.TopInboundRemotes) != 1 || body.Data.TopInboundRemotes[0].RemoteIP != "2.2.2.2" {
		t.Fatalf("unexpected inbound remotes: %+v", body.Data.TopInboundRemotes)
	}
	if len(body.Data.TopOutboundRemotes) != 1 || body.Data.TopOutboundRemotes[0].RemoteIP != "1.1.1.1" {
		t.Fatalf("unexpected outbound remotes: %+v", body.Data.TopOutboundRemotes)
	}
}

func TestArchivedDashboardCacheOutlivesRoutineRefreshes(t *testing.T) {
	server := &Server{
		slidingCacheTTL:  time.Minute,
		archivedCacheTTL: time.Hour,
	}
	if got := server.dashboardCacheTTL(true); got != 24*time.Hour {
		t.Fatalf("archived dashboard cache TTL = %s, want 24h", got)
	}
	if got := server.dashboardCacheTTL(false); got != time.Minute {
		t.Fatalf("sliding dashboard cache TTL = %s, want 1m", got)
	}
}
