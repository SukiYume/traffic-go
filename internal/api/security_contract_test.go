package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type readyStatusOnlyRuntime struct{}

func (readyStatusOnlyRuntime) ActiveProcesses() []model.ProcessListItem { return nil }
func (readyStatusOnlyRuntime) ActiveStats() model.ActiveStats           { return model.ActiveStats{} }
func (readyStatusOnlyRuntime) ReadyStatus() model.CollectorReadiness {
	return model.CollectorReadiness{SnapshotReady: true, CollectionFresh: true}
}
func (readyStatusOnlyRuntime) Diagnostics() model.CollectorDiagnostics {
	panic("readyz must not build full collector diagnostics")
}

func TestAPIRejectsUnsafePaginationAndBooleanValues(t *testing.T) {
	server := newTestServer(t)
	for _, target := range []string{
		"/api/v1/usage?range=1h&include_total=1",
		"/api/v1/usage?range=1h&page=1&cursor=MTox",
		"/api/v1/usage?range=1h&page=" + strings.Repeat("9", 40),
		"/api/v1/top/remotes?range=1h&include_loopback=maybe",
		"/api/v1/stats/overview?range=1h&summary=maybe",
	} {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d body=%s", target, recorder.Code, recorder.Body.String())
		}
	}
}

func TestAPIMethodGuardsAndScanContract(t *testing.T) {
	server := newTestServer(t)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/usage?range=1h&page=1", nil))
	if recorder.Code != http.StatusMethodNotAllowed || !strings.Contains(recorder.Header().Get("Allow"), http.MethodGet) {
		t.Fatalf("unexpected usage method response: status=%d allow=%q body=%s", recorder.Code, recorder.Header().Get("Allow"), recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	target := "/api/v1/usage/explain?ts=1710000000&direction=out&proto=tcp&remote_ip=198.51.100.1&scan=1"
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET scan must be rejected: status=%d allow=%q body=%s", recorder.Code, recorder.Header().Get("Allow"), recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, target, nil)
	request.Header.Set("Origin", "https://attacker.invalid")
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("cross-origin scan must be rejected: status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, target, strings.NewReader("unexpected"))
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("non-empty scan body must be rejected: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestInteractiveScanGateAllowsImmediateNextScanAfterRelease(t *testing.T) {
	server := newTestServer(t)
	release, ok := server.beginInteractiveScan()
	if !ok {
		t.Fatal("expected first interactive scan to acquire the gate")
	}
	if _, ok := server.beginInteractiveScan(); ok {
		t.Fatal("concurrent interactive scan must be rejected")
	}
	release()

	releaseNext, ok := server.beginInteractiveScan()
	if !ok {
		t.Fatal("completed scan must not impose an additional cooldown")
	}
	releaseNext()
}

func TestInternalErrorsAreGenericAndCarryRequestID(t *testing.T) {
	server := newTestServer(t)
	var serverLog bytes.Buffer
	server.logger = log.New(&serverLog, "", 0)
	if err := server.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/processes", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	requestID := recorder.Header().Get("X-Request-ID")
	if requestID == "" || !strings.Contains(recorder.Body.String(), `"message":"internal server error"`) || !strings.Contains(recorder.Body.String(), `"request_id":"`+requestID+`"`) {
		t.Fatalf("generic error/request id contract failed: id=%q body=%s", requestID, recorder.Body.String())
	}
	if strings.Contains(strings.ToLower(recorder.Body.String()), "database") || strings.Contains(strings.ToLower(recorder.Body.String()), "closed") {
		t.Fatalf("internal error leaked implementation detail: %s", recorder.Body.String())
	}
	logText := serverLog.String()
	if !strings.Contains(logText, "request failed") || !strings.Contains(logText, "request_id="+requestID) || !strings.Contains(strings.ToLower(logText), "closed") {
		t.Fatalf("server log did not preserve the correlated root cause: id=%q log=%s", requestID, logText)
	}
}

func TestReadinessUsesConstantTimeReadyStatus(t *testing.T) {
	server := newTestServer(t)
	server.runtime = readyStatusOnlyRuntime{}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/readyz", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"collector_ready":true`) {
		t.Fatalf("unexpected readiness response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestSummaryResponsesExposeEffectiveCoverage(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.Retention.MinuteDays = 1
		cfg.Retention.HourMonths = 1
		cfg.Retention.DayMonths = 12
	})
	start := time.Now().UTC().Add(-60 * 24 * time.Hour).Truncate(24 * time.Hour).Add(17 * time.Minute)
	end := start.Add(25*time.Hour + 13*time.Minute)
	window := fmt.Sprintf("start=%d&end=%d", start.Unix(), end.Unix())
	targets := []string{
		"/api/v1/stats/overview?" + window + "&summary=1",
		"/api/v1/stats/timeseries?" + window + "&bucket=1d",
		"/api/v1/stats/interfaces/timeseries?" + window + "&bucket=1d",
		"/api/v1/usage?" + window + "&page=1&page_size=1",
		"/api/v1/top/processes?" + window + "&summary=1&page=1&page_size=1",
		"/api/v1/top/remotes?" + window + "&summary=1&page=1&page_size=1",
		"/api/v1/top/ports?" + window + "&summary=1",
		"/api/v1/forward/usage?" + window + "&page=1&page_size=1",
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
			if recorder.Code != http.StatusOK {
				t.Fatalf("unexpected status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			var payload struct {
				Coverage *store.SummaryCoverage `json:"coverage"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if payload.Coverage == nil || !payload.Coverage.Approximate {
				t.Fatalf("summary response omitted approximate coverage: %s", recorder.Body.String())
			}
			if payload.Coverage.EffectiveStart > start.Unix() || payload.Coverage.EffectiveEnd < end.Unix() {
				t.Fatalf("coverage did not contain requested interval: %+v", payload.Coverage)
			}
		})
	}
}

func TestAPISecurityHeadersReadinessAndRedactedAccessLog(t *testing.T) {
	server := newTestServer(t)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/readyz", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"collector_ready":true`) || !strings.Contains(recorder.Body.String(), `"build":`) {
		t.Fatalf("unexpected readiness response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for key, expected := range map[string]string{
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	} {
		if got := recorder.Header().Get(key); got != expected {
			t.Fatalf("header %s=%q want %q", key, got, expected)
		}
	}

	var output bytes.Buffer
	handler := loggingMiddleware(log.New(&output, "", 0), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/usage?remote_ip=203.0.113.9&exe=/secret/path", nil))
	if strings.Contains(output.String(), "203.0.113.9") || strings.Contains(output.String(), "secret") || !strings.Contains(output.String(), "/api/v1/usage") {
		t.Fatalf("access log leaked query parameters: %s", output.String())
	}
}

func TestPageSliceDefendsAgainstNativeIntOverflow(t *testing.T) {
	rows := []int{1, 2, 3}
	if got := pageSlice(rows, math.MaxInt, math.MaxInt); len(got) != 0 {
		t.Fatalf("expected empty overflow page, got %+v", got)
	}
}

func TestStaticCachePolicySeparatesHashedAssetsFromHTML(t *testing.T) {
	server := newTestServer(t)
	server.static = fstest.MapFS{
		"index.html":         &fstest.MapFile{Data: []byte("<html></html>")},
		"assets/app-ab12.js": &fstest.MapFile{Data: []byte("console.log('ok')")},
	}
	asset := httptest.NewRecorder()
	server.Handler().ServeHTTP(asset, httptest.NewRequest(http.MethodGet, "/assets/app-ab12.js", nil))
	if asset.Code != http.StatusOK || asset.Header().Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Fatalf("unexpected asset cache policy: status=%d cache=%q", asset.Code, asset.Header().Get("Cache-Control"))
	}
	index := httptest.NewRecorder()
	server.Handler().ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/", nil))
	if index.Code != http.StatusOK || index.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("unexpected index cache policy: status=%d cache=%q", index.Code, index.Header().Get("Cache-Control"))
	}
	missingAsset := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingAsset, httptest.NewRequest(http.MethodGet, "/assets/missing-deadbeef.js", nil))
	if missingAsset.Code != http.StatusNotFound || strings.Contains(missingAsset.Body.String(), "<html") {
		t.Fatalf("missing assets must return a real 404, got status=%d body=%q", missingAsset.Code, missingAsset.Body.String())
	}
}
