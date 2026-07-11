package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

const minArchivedDashboardCacheTTL = 24 * time.Hour

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	window, ok := s.resolveWindowSource(w, r, false)
	if !ok {
		return
	}
	bucket, err := resolveTimeseriesBucket(r.URL.Query().Get("bucket"), window.End.Sub(window.Start), window.Source)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_dashboard_query", err)
		return
	}

	rangeKey := normalizedRangeKey(r)
	startBucket, endBucket, archived := quantizeWindow(rangeKey, window.Start.Unix(), window.End.Unix())
	cacheKey := fmt.Sprintf("dashboard|rng=%s|t=%d-%d|bucket=%d", rangeKey, startBucket, endBucket, int64(bucket/time.Second))
	snapshot, err := s.loadDashboardSnapshot(r.Context(), cacheKey, archived, func(ctx context.Context) (model.DashboardSnapshot, error) {
		return s.store.QueryDashboardSummary(ctx, window.Start, window.End, bucket)
	})
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}

	active := s.runtime.ActiveStats()
	overview := model.OverviewStats{
		Range:             window.RangeLabel,
		DataSource:        snapshot.DataSource,
		BytesUp:           snapshot.BytesUp,
		BytesDown:         snapshot.BytesDown,
		FlowCount:         snapshot.FlowCount,
		ActiveConnections: active.Connections,
		ActiveProcesses:   active.Processes,
	}
	payload := envelope{
		"data_source": snapshot.DataSource,
		"bucket":      strings.TrimSpace(r.URL.Query().Get("bucket")),
		"data": envelope{
			"overview":             overview,
			"timeseries":           snapshot.Timeseries,
			"top_processes":        snapshot.TopProcesses,
			"top_inbound_remotes":  snapshot.TopInboundRemotes,
			"top_outbound_remotes": snapshot.TopOutboundRemotes,
			"top_ports":            snapshot.TopPorts,
		},
	}
	addSummaryCoverage(payload, s.summaryCoverage(window.Start, window.End, window.Source != store.DataSourceMinute))
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) loadDashboardSnapshot(ctx context.Context, key string, archived bool, load func(context.Context) (model.DashboardSnapshot, error)) (model.DashboardSnapshot, error) {
	if value, ok := s.dashboardCache.Get(key); ok {
		return value, nil
	}
	value, err, _ := s.dashboardSF.Do(key, func() (any, error) {
		if cached, ok := s.dashboardCache.Get(key); ok {
			return cached, nil
		}
		loaded, err := load(ctx)
		if err != nil {
			return model.DashboardSnapshot{}, err
		}
		s.dashboardCache.Set(key, loaded, s.dashboardCacheTTL(archived))
		return loaded, nil
	})
	if err != nil {
		return model.DashboardSnapshot{}, err
	}
	return value.(model.DashboardSnapshot), nil
}

func (s *Server) dashboardCacheTTL(archived bool) time.Duration {
	ttl := s.cacheTTL(archived)
	if archived && ttl < minArchivedDashboardCacheTTL {
		return minArchivedDashboardCacheTTL
	}
	return ttl
}
