package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/buildinfo"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type envelope map[string]any

const (
	processSuggestionLimit   = 200
	maxCachedTopRows         = 5000
	maxCachedTopBytes        = 8 << 20
	maxCombinedTopCacheBytes = 16 << 20
	maxPageNumber            = 1_000_000
	maxPageSize              = 200
	maxCursorLimit           = 500
	maxQueryWindow           = 370 * 24 * time.Hour
)

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"ok": true, "status": "alive", "build": buildinfo.Current()})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	readiness := s.runtime.ReadyStatus()
	storeReady := s.store.Readiness(ctx) == nil
	collectorReady := readiness.SnapshotReady && readiness.CollectionFresh
	status := http.StatusOK
	if !storeReady || !collectorReady {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, envelope{
		"ok":              status == http.StatusOK,
		"status":          map[bool]string{true: "ready", false: "not_ready"}[status == http.StatusOK],
		"store_ready":     storeReady,
		"collector_ready": collectorReady,
		"collector_fresh": readiness.CollectionFresh,
		"build":           buildinfo.Current(),
	})
}

func (s *Server) handleCollectorDiagnostics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"data": s.runtime.Diagnostics()})
}

func (s *Server) handleStoreDiagnostics(w http.ResponseWriter, r *http.Request) {
	diagnostics, err := s.store.Diagnostics(r.Context())
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{"data": diagnostics})
}

func (s *Server) handleProcesses(w http.ResponseWriter, r *http.Request) {
	processes, err := s.store.QueryKnownProcesses(r.Context(), processSuggestionLimit)
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	processes = mergeProcessListItems(s.runtime.ActiveProcesses(), processes)
	writeJSON(w, http.StatusOK, envelope{"data": processes})
}

type resolvedUsageWindow struct {
	Start      time.Time
	End        time.Time
	RangeLabel string
	Source     string
}

type resolvedPagedUsageWindow struct {
	resolvedUsageWindow
	Page         int
	PageSize     int
	Offset       int
	IncludeTotal bool
}

func (s *Server) summaryCoverage(start, end time.Time, useSummary bool) *store.SummaryCoverage {
	if !useSummary {
		return nil
	}
	coverage := s.store.SummaryCoverage(start, end)
	return &coverage
}

func addSummaryCoverage(payload envelope, coverage *store.SummaryCoverage) {
	if coverage != nil {
		payload["coverage"] = *coverage
	}
}

func (s *Server) resolveWindowSource(w http.ResponseWriter, r *http.Request, requiresMinute bool) (resolvedUsageWindow, bool) {
	start, end, rangeLabel, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return resolvedUsageWindow{}, false
	}
	source, ok := s.resolveSourceForWindow(w, start, end, requiresMinute)
	if !ok {
		return resolvedUsageWindow{}, false
	}
	return resolvedUsageWindow{
		Start:      start,
		End:        end,
		RangeLabel: rangeLabel,
		Source:     source,
	}, true
}

func (s *Server) resolveSourceForWindow(w http.ResponseWriter, start time.Time, end time.Time, requiresMinute bool) (string, bool) {
	source, err := s.store.ResolveUsageSource(start, end, requiresMinute)
	if err != nil {
		writeDimensionError(w, err)
		return "", false
	}
	return source, true
}

func (s *Server) resolvePagedWindowSource(w http.ResponseWriter, r *http.Request, requiresMinute bool) (resolvedPagedUsageWindow, bool) {
	window, ok := s.resolveWindowSource(w, r, requiresMinute)
	if !ok {
		return resolvedPagedUsageWindow{}, false
	}
	page, pageSize, includeTotal, err := parsePageParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return resolvedPagedUsageWindow{}, false
	}
	return resolvedPagedUsageWindow{
		resolvedUsageWindow: window,
		Page:                page,
		PageSize:            pageSize,
		Offset:              (page - 1) * pageSize,
		IncludeTotal:        includeTotal,
	}, true
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	start, end, rangeLabel, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	var stats model.OverviewStats
	_, err = summaryRequested(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	source, ok := s.resolveSourceForWindow(w, start, end, false)
	if !ok {
		return
	}
	useSummary := source != store.DataSourceMinute
	if useSummary {
		stats, err = s.store.QueryOverviewSummary(r.Context(), start, end)
	} else {
		stats, err = s.store.QueryOverview(r.Context(), start, end, source)
	}
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	activeStats := s.runtime.ActiveStats()
	stats.ActiveConnections = activeStats.Connections
	stats.ActiveProcesses = activeStats.Processes
	stats.Range = rangeLabel
	payload := envelope{"data": stats}
	addSummaryCoverage(payload, s.summaryCoverage(start, end, useSummary))
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleMonthlyUsage(w http.ResponseWriter, r *http.Request) {
	summaries, err := s.store.QueryMonthlyUsage(r.Context())
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{"data": summaries})
}

func (s *Server) handleTimeseries(w http.ResponseWriter, r *http.Request) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	var pidFilter *int
	if pidValue := r.URL.Query().Get("pid"); pidValue != "" {
		pid, err := strconv.Atoi(pidValue)
		if err != nil || pid < 0 || pid > int(^uint32(0)>>1) {
			writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("invalid pid"))
			return
		}
		pidFilter = &pid
	}
	exeFilter := strings.TrimSpace(r.URL.Query().Get("exe"))
	commFilter := strings.TrimSpace(r.URL.Query().Get("comm"))
	if len(commFilter) > 128 || len(exeFilter) > 4096 {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("process filter is too long"))
		return
	}
	groupBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("group_by")))
	if !oneOf(groupBy, "", "direction", "comm", "remote_ip") {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("invalid group_by"))
		return
	}
	direction := model.Direction(strings.TrimSpace(r.URL.Query().Get("direction")))
	if !validDirection(direction) {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("invalid direction"))
		return
	}
	proto := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("proto")))
	if !validProto(proto) {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("invalid proto"))
		return
	}
	remoteIP := strings.TrimSpace(r.URL.Query().Get("remote_ip"))
	if err := validateOptionalIP(remoteIP, "remote_ip"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", err)
		return
	}
	requiresMinute := pidFilter != nil || exeFilter != ""
	source, ok := s.resolveSourceForWindow(w, start, end, requiresMinute)
	if !ok {
		return
	}
	bucket, err := resolveTimeseriesBucket(r.URL.Query().Get("bucket"), end.Sub(start), source)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", err)
		return
	}
	timeseriesQuery := model.TimeseriesQuery{
		Start:     start,
		End:       end,
		Bucket:    bucket,
		GroupBy:   groupBy,
		Comm:      commFilter,
		Exe:       exeFilter,
		RemoteIP:  remoteIP,
		Direction: direction,
		Proto:     proto,
		PID:       pidFilter,
	}
	var points []model.TimeseriesPoint
	dataSource := source
	if source == store.DataSourceMinute {
		points, err = s.store.QueryTimeseries(r.Context(), timeseriesQuery, source)
	} else {
		points, dataSource, err = s.store.QueryTimeseriesSummary(r.Context(), timeseriesQuery)
	}
	if err != nil {
		if errors.Is(err, store.ErrTimeseriesPointLimit) {
			writeError(w, http.StatusUnprocessableEntity, "timeseries_too_large", err)
			return
		}
		s.writeInternalError(w, r, err)
		return
	}
	payload := envelope{"data_source": dataSource, "data": points}
	addSummaryCoverage(payload, s.summaryCoverage(start, end, source != store.DataSourceMinute))
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleInterfaceTimeseries(w http.ResponseWriter, r *http.Request) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	source, ok := s.resolveSourceForWindow(w, start, end, false)
	if !ok {
		return
	}
	bucket, err := resolveTimeseriesBucket(r.URL.Query().Get("bucket"), end.Sub(start), source)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_timeseries_query", err)
		return
	}
	var points []model.InterfaceTimeseriesPoint
	dataSource := store.InterfaceDataSource(source)
	if source == store.DataSourceMinute {
		points, err = s.store.QueryInterfaceTimeseries(r.Context(), start, end, bucket, source)
	} else {
		points, dataSource, err = s.store.QueryInterfaceTimeseriesSummary(r.Context(), start, end, bucket)
	}
	if err != nil {
		if errors.Is(err, store.ErrTimeseriesPointLimit) {
			writeError(w, http.StatusUnprocessableEntity, "timeseries_too_large", err)
			return
		}
		s.writeInternalError(w, r, err)
		return
	}
	payload := envelope{"data_source": dataSource, "data": points}
	addSummaryCoverage(payload, s.summaryCoverage(start, end, source != store.DataSourceMinute))
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	query, err := parseUsageQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_usage_query", err)
		return
	}
	source, ok := s.resolveSourceForWindow(w, query.Start, query.End, usageQueryRequiresMinuteSource(query))
	if !ok {
		return
	}
	totalRowsExact := query.IncludeTotal
	totalRows := 0
	fetchQuery := query
	useSummary := source != store.DataSourceMinute
	if query.UsePage && query.IncludeTotal {
		if useSummary {
			totalRows, err = s.lookupUsageSummaryCount(r.Context(), query, source, normalizedRangeKey(r))
		} else {
			totalRows, err = s.lookupUsageCount(r.Context(), query, source, normalizedRangeKey(r))
		}
		if err != nil {
			if errors.Is(err, store.ErrDimensionUnavailable) {
				writeDimensionError(w, err)
				return
			}
			s.writeInternalError(w, r, err)
			return
		}
		fetchQuery.IncludeTotal = false
	}
	var records []model.UsageRecord
	var nextCursor string
	var pageTotalRows int
	dataSource := source
	if useSummary {
		records, nextCursor, pageTotalRows, dataSource, err = s.store.QueryUsageSummary(r.Context(), fetchQuery)
	} else {
		records, nextCursor, pageTotalRows, err = s.store.QueryUsage(r.Context(), fetchQuery, source)
	}
	if err != nil {
		if errors.Is(err, store.ErrDimensionUnavailable) {
			writeDimensionError(w, err)
			return
		}
		if errors.Is(err, store.ErrCursorSortUnsupported) {
			writeError(w, http.StatusBadRequest, "invalid_usage_query", err)
			return
		}
		s.writeInternalError(w, r, err)
		return
	}
	if !totalRowsExact {
		totalRows = pageTotalRows
	}
	writePagedData(w, dataSource, records, nextCursor, totalRows, query.UsePage, query.Page, query.PageSize, totalRowsExact, s.summaryCoverage(query.Start, query.End, useSummary))
}

func (s *Server) lookupUsageCount(ctx context.Context, query model.UsageQuery, source string, rangeKey string) (int, error) {
	startBucket, endBucket, archived := quantizeWindow(rangeKey, query.Start.Unix(), query.End.Unix())
	key := usageCountCacheKey(source, rangeKey, startBucket, endBucket, query)
	return loadCachedCount(ctx, s.countCache, &s.countSF, key, s.cacheTTL(archived), func(ctx context.Context) (int, error) {
		return s.store.QueryUsageCount(ctx, query, source)
	})
}

func (s *Server) handleTopProcesses(w http.ResponseWriter, r *http.Request) {
	window, ok := s.resolvePagedWindowSource(w, r, false)
	if !ok {
		return
	}
	groupBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("group_by")))
	sortBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_by")))
	sortOrder := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_order")))
	if !oneOf(groupBy, "", "pid", "comm") ||
		!oneOf(sortBy, "", "bytes_total", "total", "bytes_up", "bytes_down", "flow_count", "comm", "pid") ||
		!validSortOrder(sortOrder) {
		writeError(w, http.StatusBadRequest, "invalid_query", fmt.Errorf("invalid top-process query"))
		return
	}
	_, err := summaryRequested(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	useSummary := window.Source != store.DataSourceMinute
	rangeKey := normalizedRangeKey(r)
	startBucket, endBucket, archived := quantizeWindow(rangeKey, window.Start.Unix(), window.End.Unix())
	cacheKey := topProcessesCacheKey(useSummary, window.Source, rangeKey, startBucket, endBucket, groupBy)
	entry, err := s.lookupOrLoadTopProcesses(r.Context(), cacheKey, archived, useSummary, window, groupBy)
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	if entry.tooLarge {
		var rows []model.ProcessSummary
		var total int
		source := window.Source
		if useSummary {
			rows, total, source, err = s.store.QueryTopProcessesSummary(r.Context(), window.Start, window.End, groupBy, sortBy, sortOrder, window.PageSize, window.Offset, window.IncludeTotal)
		} else {
			rows, total, err = s.store.QueryTopProcesses(r.Context(), window.Start, window.End, window.Source, groupBy, sortBy, sortOrder, window.PageSize, window.Offset, window.IncludeTotal)
		}
		if err != nil {
			s.writeInternalError(w, r, err)
			return
		}
		writePagedData(w, source, rows, "", total, true, window.Page, window.PageSize, window.IncludeTotal, s.summaryCoverage(window.Start, window.End, useSummary))
		return
	}
	sorted := sortProcessSummaries(entry.rows, sortBy, sortOrder)
	page := pageSlice(sorted, window.Page, window.PageSize)
	writePagedData(w, entry.source, page, "", len(sorted), true, window.Page, window.PageSize, true, s.summaryCoverage(window.Start, window.End, useSummary))
}

func (s *Server) lookupUsageSummaryCount(ctx context.Context, query model.UsageQuery, source string, rangeKey string) (int, error) {
	startBucket, endBucket, archived := quantizeWindow(rangeKey, query.Start.Unix(), query.End.Unix())
	key := usageCountCacheKey("summary:"+source, rangeKey, startBucket, endBucket, query)
	return loadCachedCount(ctx, s.countCache, &s.countSF, key, s.cacheTTL(archived), func(ctx context.Context) (int, error) {
		total, _, err := s.store.QueryUsageSummaryCount(ctx, query)
		return total, err
	})
}

func (s *Server) lookupOrLoadTopProcesses(ctx context.Context, key string, archived bool, useSummary bool, window resolvedPagedUsageWindow, groupBy string) (cachedTop[model.ProcessSummary], error) {
	entry, err := loadCachedTop(ctx, s.processCache, &s.processSF, key, s.cacheTTL(archived), func(ctx context.Context) (cachedTop[model.ProcessSummary], error) {
		var result cachedTop[model.ProcessSummary]
		if useSummary {
			rows, source, err := s.store.QueryTopProcessesSummaryAllLimited(ctx, window.Start, window.End, groupBy, maxCachedTopRows+1)
			result = cachedTop[model.ProcessSummary]{rows: rows, source: source}
			if err != nil {
				return result, err
			}
		} else {
			rows, err := s.store.QueryTopProcessesAllLimited(ctx, window.Start, window.End, window.Source, groupBy, maxCachedTopRows+1)
			result = cachedTop[model.ProcessSummary]{rows: rows, source: window.Source}
			if err != nil {
				return result, err
			}
		}
		result.weight = topProcessesCacheBytes(result.rows)
		if len(result.rows) > maxCachedTopRows || result.weight > maxCachedTopBytes {
			result.rows = nil
			result.tooLarge = true
			result.weight = 64
		}
		return result, nil
	})
	return entry, err
}

func topProcessesCacheKey(summary bool, source string, rangeKey string, startBucket int64, endBucket int64, groupBy string) string {
	return fmt.Sprintf("top_processes|sum=%v|src=%s|rng=%s|t=%d-%d|gb=%s",
		summary, source, rangeKey, startBucket, endBucket, strings.ToLower(strings.TrimSpace(groupBy)))
}

func (s *Server) handleTopRemotes(w http.ResponseWriter, r *http.Request) {
	window, ok := s.resolvePagedWindowSource(w, r, false)
	if !ok {
		return
	}
	includeLoopback := true
	var err error
	if r.URL.Query().Has("include_loopback") {
		includeLoopback, err = parseOptionalBool(r.URL.Query(), "include_loopback")
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_query", err)
			return
		}
	}
	excludeLoopback, err := parseOptionalBool(r.URL.Query(), "exclude_loopback")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	if excludeLoopback {
		includeLoopback = false
	}
	direction := model.Direction(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("direction"))))
	sortBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_by")))
	sortOrder := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_order")))
	if !validDirection(direction) ||
		!oneOf(sortBy, "", "bytes_total", "total", "bytes_up", "bytes_down", "flow_count", "remote_ip", "direction") ||
		!validSortOrder(sortOrder) {
		writeError(w, http.StatusBadRequest, "invalid_query", fmt.Errorf("invalid top-remote query"))
		return
	}
	_, err = summaryRequested(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	useSummary := window.Source != store.DataSourceMinute
	rangeKey := normalizedRangeKey(r)
	startBucket, endBucket, archived := quantizeWindow(rangeKey, window.Start.Unix(), window.End.Unix())
	cacheKey := topRemotesCacheKey(useSummary, window.Source, rangeKey, startBucket, endBucket, direction, includeLoopback)
	entry, err := s.lookupOrLoadTopRemotes(r.Context(), cacheKey, archived, useSummary, window, direction, includeLoopback)
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	if entry.tooLarge {
		var rows []model.RemoteSummary
		var total int
		source := window.Source
		if useSummary {
			rows, total, source, err = s.store.QueryTopRemotesSummary(r.Context(), window.Start, window.End, direction, includeLoopback, sortBy, sortOrder, window.PageSize, window.Offset, window.IncludeTotal)
		} else {
			rows, total, err = s.store.QueryTopRemotes(r.Context(), window.Start, window.End, window.Source, direction, includeLoopback, sortBy, sortOrder, window.PageSize, window.Offset, window.IncludeTotal)
		}
		if err != nil {
			s.writeInternalError(w, r, err)
			return
		}
		writePagedData(w, source, rows, "", total, true, window.Page, window.PageSize, window.IncludeTotal, s.summaryCoverage(window.Start, window.End, useSummary))
		return
	}
	sorted := sortRemoteSummaries(entry.rows, sortBy, sortOrder)
	page := pageSlice(sorted, window.Page, window.PageSize)
	writePagedData(w, entry.source, page, "", len(sorted), true, window.Page, window.PageSize, true, s.summaryCoverage(window.Start, window.End, useSummary))
}

func (s *Server) lookupOrLoadTopRemotes(ctx context.Context, key string, archived bool, useSummary bool, window resolvedPagedUsageWindow, direction model.Direction, includeLoopback bool) (cachedTop[model.RemoteSummary], error) {
	entry, err := loadCachedTop(ctx, s.remoteCache, &s.remoteSF, key, s.cacheTTL(archived), func(ctx context.Context) (cachedTop[model.RemoteSummary], error) {
		var result cachedTop[model.RemoteSummary]
		if useSummary {
			rows, source, err := s.store.QueryTopRemotesSummaryAllLimited(ctx, window.Start, window.End, direction, includeLoopback, maxCachedTopRows+1)
			result = cachedTop[model.RemoteSummary]{rows: rows, source: source}
			if err != nil {
				return result, err
			}
		} else {
			rows, err := s.store.QueryTopRemotesAllLimited(ctx, window.Start, window.End, window.Source, direction, includeLoopback, maxCachedTopRows+1)
			result = cachedTop[model.RemoteSummary]{rows: rows, source: window.Source}
			if err != nil {
				return result, err
			}
		}
		result.weight = topRemotesCacheBytes(result.rows)
		if len(result.rows) > maxCachedTopRows || result.weight > maxCachedTopBytes {
			result.rows = nil
			result.tooLarge = true
			result.weight = 64
		}
		return result, nil
	})
	return entry, err
}

func topProcessesCacheBytes(rows []model.ProcessSummary) int64 {
	bytes := int64(len(rows) * 96)
	for _, row := range rows {
		bytes += int64(len(row.Comm))
		if row.Exe != nil {
			bytes += int64(len(*row.Exe))
		}
	}
	return bytes
}

func topRemotesCacheBytes(rows []model.RemoteSummary) int64 {
	bytes := int64(len(rows) * 72)
	for _, row := range rows {
		bytes += int64(len(row.RemoteIP))
	}
	return bytes
}

func topRemotesCacheKey(summary bool, source string, rangeKey string, startBucket int64, endBucket int64, direction model.Direction, includeLoopback bool) string {
	return fmt.Sprintf("top_remotes|sum=%v|src=%s|rng=%s|t=%d-%d|dir=%s|loop=%v",
		summary, source, rangeKey, startBucket, endBucket, string(direction), includeLoopback)
}

func (s *Server) handleTopPorts(w http.ResponseWriter, r *http.Request) {
	s.handleTop(w, r, s.store.QueryTopPorts)
}

func (s *Server) handleTop(
	w http.ResponseWriter,
	r *http.Request,
	queryFunc func(context.Context, time.Time, time.Time, string, string) ([]model.TopEntry, error),
) {
	by := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("by")))
	if !oneOf(by, "", "total", "bytes_up", "bytes_down") {
		writeError(w, http.StatusBadRequest, "invalid_query", fmt.Errorf("invalid top-port query"))
		return
	}
	window, ok := s.resolveWindowSource(w, r, false)
	if !ok {
		return
	}
	_, err := summaryRequested(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	useSummary := window.Source != store.DataSourceMinute
	if useSummary {
		entries, source, err := s.store.QueryTopPortsSummary(r.Context(), window.Start, window.End, by)
		if err != nil {
			s.writeInternalError(w, r, err)
			return
		}
		payload := envelope{"data_source": source, "data": entries}
		addSummaryCoverage(payload, s.summaryCoverage(window.Start, window.End, true))
		writeJSON(w, http.StatusOK, payload)
		return
	}
	entries, err := queryFunc(r.Context(), window.Start, window.End, window.Source, by)
	if err != nil {
		s.writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{"data_source": window.Source, "data": entries})
}

func (s *Server) handleForwardUsage(w http.ResponseWriter, r *http.Request) {
	query, err := parseForwardQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_forward_query", err)
		return
	}
	source, ok := s.resolveSourceForWindow(w, query.Start, query.End, false)
	if !ok {
		return
	}
	totalRowsExact := query.IncludeTotal
	totalRows := 0
	fetchQuery := query
	useSummary := source != store.DataSourceMinute
	if query.UsePage && query.IncludeTotal {
		if useSummary {
			totalRows, err = s.lookupForwardUsageSummaryCount(r.Context(), query, source, normalizedRangeKey(r))
		} else {
			totalRows, err = s.lookupForwardUsageCount(r.Context(), query, source, normalizedRangeKey(r))
		}
		if err != nil {
			s.writeInternalError(w, r, err)
			return
		}
		fetchQuery.IncludeTotal = false
	}
	var records []model.ForwardUsageRecord
	var nextCursor string
	var pageTotalRows int
	dataSource := store.ForwardDataSource(source)
	if useSummary {
		records, nextCursor, pageTotalRows, dataSource, err = s.store.QueryForwardUsageSummary(r.Context(), fetchQuery)
	} else {
		records, nextCursor, pageTotalRows, err = s.store.QueryForwardUsage(r.Context(), fetchQuery, source)
	}
	if err != nil {
		if errors.Is(err, store.ErrCursorSortUnsupported) {
			writeError(w, http.StatusBadRequest, "invalid_forward_query", err)
			return
		}
		s.writeInternalError(w, r, err)
		return
	}
	if !totalRowsExact {
		totalRows = pageTotalRows
	}
	writePagedData(w, dataSource, records, nextCursor, totalRows, query.UsePage, query.Page, query.PageSize, totalRowsExact, s.summaryCoverage(query.Start, query.End, useSummary))
}

func (s *Server) lookupForwardUsageSummaryCount(ctx context.Context, query model.ForwardQuery, source string, rangeKey string) (int, error) {
	return s.lookupForwardUsageCountCached(ctx, query, "summary:"+source, rangeKey, func(ctx context.Context) (int, error) {
		total, _, err := s.store.QueryForwardUsageSummaryCount(ctx, query)
		return total, err
	})
}

func (s *Server) lookupForwardUsageCount(ctx context.Context, query model.ForwardQuery, source string, rangeKey string) (int, error) {
	return s.lookupForwardUsageCountCached(ctx, query, source, rangeKey, func(ctx context.Context) (int, error) {
		return s.store.QueryForwardUsageCount(ctx, query, source)
	})
}

func (s *Server) lookupForwardUsageCountCached(ctx context.Context, query model.ForwardQuery, cacheSource string, rangeKey string, load func(context.Context) (int, error)) (int, error) {
	startBucket, endBucket, archived := quantizeWindow(rangeKey, query.Start.Unix(), query.End.Unix())
	key := forwardCountCacheKey(cacheSource, rangeKey, startBucket, endBucket, query)
	return loadCachedCount(ctx, s.countCache, &s.countSF, key, s.cacheTTL(archived), load)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writePagedData(w http.ResponseWriter, source string, data any, nextCursor string, totalRows int, usePage bool, page int, pageSize int, totalRowsExact bool, coverage *store.SummaryCoverage) {
	payload := envelope{
		"data_source":      source,
		"data":             data,
		"next_cursor":      nextCursor,
		"total_rows":       totalRows,
		"total_rows_exact": totalRowsExact,
		"has_more":         pagedHasMore(usePage, totalRows, page, pageSize, nextCursor),
		"page":             page,
		"page_size":        pageSize,
	}
	addSummaryCoverage(payload, coverage)
	writeJSON(w, http.StatusOK, payload)
}

func pagedHasMore(usePage bool, totalRows int, page int, pageSize int, nextCursor string) bool {
	if !usePage {
		return nextCursor != ""
	}
	if pageSize <= 0 {
		return false
	}
	return totalRows > page*pageSize
}

func (s *Server) writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	requestID := strings.TrimSpace(w.Header().Get("X-Request-ID"))
	if s.logger != nil {
		method := "unknown"
		path := "unknown"
		if r != nil {
			method = r.Method
			path = r.URL.EscapedPath()
			if len(path) > 256 {
				path = path[:256]
			}
		}
		errorText := "<nil>"
		if err != nil {
			errorText = strings.NewReplacer("\r", `\r`, "\n", `\n`).Replace(err.Error())
			if len(errorText) > 2048 {
				errorText = errorText[:2048]
			}
		}
		s.logger.Printf(
			"request failed method=%s path=%s status=%d code=internal_error request_id=%s error=%s",
			method,
			path,
			http.StatusInternalServerError,
			requestID,
			errorText,
		)
	}
	writeError(w, http.StatusInternalServerError, "internal_error", err)
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	message := "internal server error"
	if status < http.StatusInternalServerError {
		message = err.Error()
	}
	payload := envelope{
		"error":   code,
		"message": message,
	}
	if status >= http.StatusInternalServerError {
		if requestID := strings.TrimSpace(w.Header().Get("X-Request-ID")); requestID != "" {
			payload["request_id"] = requestID
		}
	}
	writeJSON(w, status, payload)
}

func writeDimensionError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrRangeUnavailable) {
		writeJSON(w, http.StatusUnprocessableEntity, envelope{
			"error":   "range_unavailable",
			"message": "the requested start time is older than the retained daily traffic data",
		})
		return
	}
	if errors.Is(err, store.ErrDimensionUnavailable) {
		writeJSON(w, http.StatusBadRequest, envelope{
			"error":   "dimension_unavailable",
			"message": "the selected filters require a time range within the minute-retention window",
		})
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_query", err)
}

func parseWindow(r *http.Request) (time.Time, time.Time, string, error) {
	return parseWindowAt(r, time.Now().UTC())
}

func parseWindowAt(r *http.Request, now time.Time) (time.Time, time.Time, string, error) {
	query := r.URL.Query()
	if startValue := query.Get("start"); startValue != "" {
		return parseExplicitWindow(query, startValue)
	}

	rangeLabel := strings.TrimSpace(query.Get("range"))
	if rangeLabel == "" {
		rangeLabel = "24h"
	}
	if start, end, normalizedLabel, ok := naturalMonthWindow(rangeLabel, now); ok {
		return normalizeWindow(start, end, normalizedLabel)
	}
	duration, err := parseRange(rangeLabel)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	// Relative windows are defined on minute buckets, matching the finest
	// persisted resolution. Ceil to the current minute boundary so identical
	// requests share exact cache keys while still including the in-progress
	// minute bucket.
	end := now.UTC().Truncate(time.Minute).Add(time.Minute)
	start := end.Add(-duration)
	return normalizeWindow(start, end, rangeLabel)
}

func naturalMonthWindow(value string, now time.Time) (time.Time, time.Time, string, bool) {
	monthStart := func(offset int) (time.Time, time.Time) {
		current := now.UTC()
		start := time.Date(current.Year(), current.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, offset, 0)
		return start, start.AddDate(0, 1, 0)
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "this_month", "current_month", "month":
		start, end := monthStart(0)
		return start, end, "this_month", true
	case "last_month", "previous_month":
		start, end := monthStart(-1)
		return start, end, "last_month", true
	case "two_months_ago", "month_before_last":
		start, end := monthStart(-2)
		return start, end, "two_months_ago", true
	default:
		return time.Time{}, time.Time{}, "", false
	}
}

func parseUsageQuery(r *http.Request) (model.UsageQuery, error) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		return model.UsageQuery{}, err
	}
	listParams, err := parseListQueryParams(r.URL.Query())
	if err != nil {
		return model.UsageQuery{}, err
	}
	query := model.UsageQuery{
		Start:        start,
		End:          end,
		Comm:         strings.TrimSpace(r.URL.Query().Get("comm")),
		Exe:          strings.TrimSpace(r.URL.Query().Get("exe")),
		RemoteIP:     strings.TrimSpace(r.URL.Query().Get("remote_ip")),
		Direction:    model.Direction(strings.TrimSpace(r.URL.Query().Get("direction"))),
		Proto:        strings.ToLower(strings.TrimSpace(r.URL.Query().Get("proto"))),
		Attribution:  model.Attribution(strings.TrimSpace(r.URL.Query().Get("attribution"))),
		Limit:        listParams.Limit,
		Page:         listParams.Page,
		PageSize:     listParams.PageSize,
		SortBy:       listParams.SortBy,
		SortOrder:    listParams.SortOrder,
		CursorTS:     listParams.CursorTS,
		CursorRowID:  listParams.CursorRowID,
		UsePage:      listParams.UsePage,
		IncludeTotal: listParams.IncludeTotal,
	}
	if len(query.Comm) > 128 || len(query.Exe) > 4096 {
		return query, fmt.Errorf("process filter is too long")
	}
	if err := validateOptionalIP(query.RemoteIP, "remote_ip"); err != nil {
		return query, err
	}
	if !validDirection(query.Direction) || !validProto(query.Proto) || !validAttribution(query.Attribution) {
		return query, fmt.Errorf("invalid direction, proto, or attribution")
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(query.SortBy)), "", "time", "minute_ts", "hour_ts", "bytes_up", "bytes_down", "bytes_total", "flow_count", "remote_ip", "direction", "proto", "local_port", "comm", "pid") || !validSortOrder(query.SortOrder) {
		return query, fmt.Errorf("invalid sort")
	}
	if pidValue := r.URL.Query().Get("pid"); pidValue != "" {
		pid, err := strconv.Atoi(pidValue)
		if err != nil || pid < 0 || pid > int(^uint32(0)>>1) {
			return query, fmt.Errorf("invalid pid")
		}
		query.PID = &pid
	}
	if portValue := r.URL.Query().Get("local_port"); portValue != "" {
		port, err := strconv.Atoi(portValue)
		if err != nil || port < 1 || port > 65535 {
			return query, fmt.Errorf("invalid local_port")
		}
		query.LocalPort = &port
	}
	if portValue := r.URL.Query().Get("remote_port"); portValue != "" {
		port, err := strconv.Atoi(portValue)
		if err != nil || port < 1 || port > 65535 {
			return query, fmt.Errorf("invalid remote_port")
		}
		query.RemotePort = &port
	}
	if !query.UsePage {
		if err := validateCursorSort(query.SortBy, query.SortOrder); err != nil {
			return query, err
		}
	}
	return query, nil
}

func usageQueryRequiresMinuteSource(query model.UsageQuery) bool {
	return query.PID != nil ||
		strings.TrimSpace(query.Exe) != "" ||
		query.RemotePort != nil ||
		query.Attribution != ""
}

func parseForwardQuery(r *http.Request) (model.ForwardQuery, error) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		return model.ForwardQuery{}, err
	}
	listParams, err := parseListQueryParams(r.URL.Query())
	if err != nil {
		return model.ForwardQuery{}, err
	}
	query := model.ForwardQuery{
		Start:        start,
		End:          end,
		Proto:        strings.ToLower(strings.TrimSpace(r.URL.Query().Get("proto"))),
		OrigSrcIP:    strings.TrimSpace(r.URL.Query().Get("orig_src_ip")),
		OrigDstIP:    strings.TrimSpace(r.URL.Query().Get("orig_dst_ip")),
		Limit:        listParams.Limit,
		Page:         listParams.Page,
		PageSize:     listParams.PageSize,
		SortBy:       listParams.SortBy,
		SortOrder:    listParams.SortOrder,
		CursorTS:     listParams.CursorTS,
		CursorRowID:  listParams.CursorRowID,
		UsePage:      listParams.UsePage,
		IncludeTotal: listParams.IncludeTotal,
	}
	if !validProto(query.Proto) {
		return query, fmt.Errorf("invalid proto")
	}
	if err := validateOptionalIP(query.OrigSrcIP, "orig_src_ip"); err != nil {
		return query, err
	}
	if err := validateOptionalIP(query.OrigDstIP, "orig_dst_ip"); err != nil {
		return query, err
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(query.SortBy)), "", "time", "minute_ts", "hour_ts", "bytes_orig", "bytes_reply", "bytes_total", "flow_count", "orig_src_ip", "orig_dst_ip") || !validSortOrder(query.SortOrder) {
		return query, fmt.Errorf("invalid sort")
	}
	if !query.UsePage {
		if err := validateCursorSort(query.SortBy, query.SortOrder); err != nil {
			return query, err
		}
	}
	return query, nil
}

func validateCursorSort(sortBy string, sortOrder string) error {
	// Cursor tokens are derived from (time_bucket, rowid), so allowing arbitrary
	// sort orders would make "next page" semantics unstable and non-repeatable.
	normalizedSortBy := strings.TrimSpace(sortBy)
	switch normalizedSortBy {
	case "", "time", "minute_ts", "hour_ts":
	default:
		return store.ErrCursorSortUnsupported
	}

	normalizedSortOrder := strings.ToLower(strings.TrimSpace(sortOrder))
	switch normalizedSortOrder {
	case "", "desc":
		return nil
	default:
		return store.ErrCursorSortUnsupported
	}
}

func parseRange(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 32)
		if err != nil {
			return 0, err
		}
		if days <= 0 || days > 370 {
			return 0, fmt.Errorf("range must be between 1d and 370d")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if duration <= 0 || duration > maxQueryWindow {
		return 0, fmt.Errorf("range must be positive and no greater than %s", maxQueryWindow)
	}
	return duration, nil
}

func parseBucket(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Minute, nil
	}
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 32)
		if err == nil && days > 0 && days <= 370 {
			return time.Duration(days) * 24 * time.Hour, nil
		}
		return 0, fmt.Errorf("invalid bucket")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < time.Minute || duration > maxQueryWindow {
		return 0, fmt.Errorf("invalid bucket")
	}
	return duration, nil
}

func resolveTimeseriesBucket(value string, window time.Duration, source string) (time.Duration, error) {
	bucket, err := parseBucket(value)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(value) == "" {
		bucket = defaultBucketForWindow(window, source)
	}
	if source == store.DataSourceDay && bucket < 24*time.Hour {
		return 24 * time.Hour, nil
	}
	if source == store.DataSourceHour && bucket < time.Hour {
		return time.Hour, nil
	}
	return bucket, nil
}

func defaultBucketForWindow(window time.Duration, source string) time.Duration {
	switch {
	case window <= time.Hour:
		return time.Minute
	case window <= 24*time.Hour:
		return 5 * time.Minute
	case window <= 7*24*time.Hour:
		return time.Hour
	case window <= 30*24*time.Hour:
		return 6 * time.Hour
	default:
		if source == store.DataSourceHour || source == store.DataSourceDay {
			return 24 * time.Hour
		}
		return 6 * time.Hour
	}
}

func parseTimeValue(value string) (time.Time, error) {
	if unixValue, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(unixValue, 0), nil
	}
	return time.Parse(time.RFC3339, value)
}

func parseExplicitWindow(query url.Values, startValue string) (time.Time, time.Time, string, error) {
	start, err := parseTimeValue(startValue)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	endValue := query.Get("end")
	if endValue == "" {
		return time.Time{}, time.Time{}, "", fmt.Errorf("end is required when start is provided")
	}
	end, err := parseTimeValue(endValue)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	return normalizeWindow(start.UTC(), end.UTC(), fmt.Sprintf("%s..%s", startValue, endValue))
}

func normalizeWindow(start time.Time, end time.Time, label string) (time.Time, time.Time, string, error) {
	if !end.After(start) {
		return time.Time{}, time.Time{}, "", fmt.Errorf("end must be after start")
	}
	if end.Sub(start) > maxQueryWindow {
		return time.Time{}, time.Time{}, "", fmt.Errorf("time window exceeds %s", maxQueryWindow)
	}
	return start.UTC(), end.UTC(), label, nil
}

type listQueryParams struct {
	Limit        int
	Page         int
	PageSize     int
	SortBy       string
	SortOrder    string
	CursorTS     int64
	CursorRowID  int64
	UsePage      bool
	IncludeTotal bool
}

func parseListQueryParams(query url.Values) (listQueryParams, error) {
	limit, err := parseIntWithDefault(query.Get("limit"), 200, "limit")
	if err != nil {
		return listQueryParams{}, err
	}
	page, err := parseIntWithDefault(query.Get("page"), 1, "page")
	if err != nil {
		return listQueryParams{}, err
	}
	pageSize, err := parseIntWithDefault(query.Get("page_size"), 50, "page_size")
	if err != nil {
		return listQueryParams{}, err
	}

	usePage := query.Has("page") || query.Has("page_size")
	if limit < 1 || limit > maxCursorLimit {
		return listQueryParams{}, fmt.Errorf("limit must be between 1 and %d", maxCursorLimit)
	}
	if page < 1 || page > maxPageNumber {
		return listQueryParams{}, fmt.Errorf("page must be between 1 and %d", maxPageNumber)
	}
	if pageSize < 1 || pageSize > maxPageSize {
		return listQueryParams{}, fmt.Errorf("page_size must be between 1 and %d", maxPageSize)
	}
	includeTotal, err := parseOptionalBool(query, "include_total")
	if err != nil {
		return listQueryParams{}, err
	}
	if includeTotal && !usePage {
		return listQueryParams{}, fmt.Errorf("include_total requires page pagination")
	}
	if query.Get("cursor") != "" && usePage {
		return listQueryParams{}, fmt.Errorf("cursor cannot be combined with page pagination")
	}
	params := listQueryParams{
		Limit:        limit,
		Page:         page,
		PageSize:     pageSize,
		SortBy:       strings.ToLower(strings.TrimSpace(query.Get("sort_by"))),
		SortOrder:    strings.ToLower(strings.TrimSpace(query.Get("sort_order"))),
		UsePage:      usePage,
		IncludeTotal: includeTotal,
	}
	if cursor := query.Get("cursor"); cursor != "" {
		ts, rowID, err := store.DecodeCursor(cursor)
		if err != nil {
			return listQueryParams{}, fmt.Errorf("invalid cursor: %w", err)
		}
		params.CursorTS = ts
		params.CursorRowID = rowID
	}
	return params, nil
}

func parseIntWithDefault(value string, fallback int, field string) (int, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", field, err)
	}
	return parsed, nil
}

func parsePageParams(r *http.Request) (int, int, bool, error) {
	if strings.TrimSpace(r.URL.Query().Get("cursor")) != "" {
		return 0, 0, false, fmt.Errorf("cursor pagination is not supported by this endpoint")
	}
	page, err := parseIntWithDefault(r.URL.Query().Get("page"), 1, "page")
	if err != nil {
		return 0, 0, false, err
	}
	if page < 1 || page > maxPageNumber {
		return 0, 0, false, fmt.Errorf("page must be between 1 and %d", maxPageNumber)
	}
	pageSize, err := parseIntWithDefault(r.URL.Query().Get("page_size"), 25, "page_size")
	if err != nil {
		return 0, 0, false, err
	}
	if pageSize < 1 || pageSize > maxPageSize {
		return 0, 0, false, fmt.Errorf("page_size must be between 1 and %d", maxPageSize)
	}
	includeTotal, err := parseOptionalBool(r.URL.Query(), "include_total")
	if err != nil {
		return 0, 0, false, err
	}
	return page, pageSize, includeTotal, nil
}

func parseOptionalBool(query url.Values, field string) (bool, error) {
	if !query.Has(field) {
		return false, nil
	}
	switch strings.ToLower(strings.TrimSpace(query.Get(field))) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid %s boolean", field)
	}
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validDirection(value model.Direction) bool {
	return oneOf(string(value), "", string(model.DirectionIn), string(model.DirectionOut))
}

func validProto(value string) bool {
	return oneOf(strings.ToLower(strings.TrimSpace(value)), "", "tcp", "udp")
}

func validAttribution(value model.Attribution) bool {
	return oneOf(string(value), "", string(model.AttributionExact), string(model.AttributionHeuristic), string(model.AttributionGuess), string(model.AttributionUnknown))
}

func validSortOrder(value string) bool {
	return oneOf(strings.ToLower(strings.TrimSpace(value)), "", "asc", "desc")
}

func validateOptionalIP(value string, field string) error {
	if value == "" {
		return nil
	}
	if len(value) > 64 || net.ParseIP(value) == nil {
		return fmt.Errorf("invalid %s", field)
	}
	return nil
}

func summaryRequested(r *http.Request) (bool, error) {
	return parseOptionalBool(r.URL.Query(), "summary")
}

func mergeProcessListItems(primary []model.ProcessListItem, secondary []model.ProcessListItem) []model.ProcessListItem {
	if len(primary) == 0 && len(secondary) == 0 {
		return nil
	}

	merged := make([]model.ProcessListItem, 0, len(primary)+len(secondary))
	seen := make(map[string]struct{}, len(primary)+len(secondary))
	appendItems := func(items []model.ProcessListItem) {
		for _, item := range items {
			if item.PID <= 0 && strings.TrimSpace(item.Comm) == "" && strings.TrimSpace(item.Exe) == "" {
				continue
			}
			key := fmt.Sprintf("%d|%s|%s", item.PID, strings.TrimSpace(item.Comm), strings.TrimSpace(item.Exe))
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, item)
		}
	}

	appendItems(primary)
	appendItems(secondary)
	sort.SliceStable(merged, func(i, j int) bool {
		leftComm := strings.ToLower(strings.TrimSpace(merged[i].Comm))
		rightComm := strings.ToLower(strings.TrimSpace(merged[j].Comm))
		if leftComm != rightComm {
			return leftComm < rightComm
		}
		if merged[i].PID != merged[j].PID {
			return merged[i].PID < merged[j].PID
		}
		return strings.ToLower(strings.TrimSpace(merged[i].Exe)) < strings.ToLower(strings.TrimSpace(merged[j].Exe))
	})
	return merged
}
