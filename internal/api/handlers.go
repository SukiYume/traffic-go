package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type envelope map[string]any

const processSuggestionLimit = 200

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"ok": true})
}

func (s *Server) handleProcesses(w http.ResponseWriter, r *http.Request) {
	processes, err := s.store.QueryKnownProcesses(r.Context(), processSuggestionLimit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	processes = mergeProcessListItems(s.runtime.ActiveProcesses(), processes)
	writeJSON(w, http.StatusOK, envelope{"data": processes})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	start, end, rangeLabel, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	source, err := s.store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	stats, err := s.store.QueryOverview(r.Context(), start, end, source)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	activeStats := s.runtime.ActiveStats()
	stats.ActiveConnections = activeStats.Connections
	stats.ActiveProcesses = activeStats.Processes
	stats.Range = rangeLabel
	writeJSON(w, http.StatusOK, envelope{"data": stats})
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
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_timeseries_query", fmt.Errorf("invalid pid: %w", err))
			return
		}
		pidFilter = &pid
	}
	exeFilter := r.URL.Query().Get("exe")
	source, err := s.store.ResolveUsageSource(start, end, pidFilter != nil, exeFilter != "")
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	bucket := parseBucket(r.URL.Query().Get("bucket"))
	if r.URL.Query().Get("bucket") == "" {
		bucket = defaultBucketForWindow(end.Sub(start), source)
	}
	if source == store.DataSourceHour && bucket < time.Hour {
		bucket = time.Hour
	}
	points, err := s.store.QueryTimeseries(r.Context(), model.TimeseriesQuery{
		Start:     start,
		End:       end,
		Bucket:    bucket,
		GroupBy:   r.URL.Query().Get("group_by"),
		Comm:      r.URL.Query().Get("comm"),
		Exe:       exeFilter,
		RemoteIP:  r.URL.Query().Get("remote_ip"),
		Direction: model.Direction(r.URL.Query().Get("direction")),
		Proto:     r.URL.Query().Get("proto"),
		PID:       pidFilter,
	}, source)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{"data_source": source, "data": points})
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	query, err := parseUsageQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_usage_query", err)
		return
	}
	source, err := s.store.ResolveUsageSource(query.Start, query.End, query.PID != nil, query.Exe != "")
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	records, nextCursor, totalRows, err := s.store.QueryUsage(r.Context(), query, source)
	if err != nil {
		if errors.Is(err, store.ErrDimensionUnavailable) {
			writeDimensionError(w, err)
			return
		}
		if errors.Is(err, store.ErrCursorSortUnsupported) {
			writeError(w, http.StatusBadRequest, "invalid_usage_query", err)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{
		"data_source": source,
		"data":        records,
		"next_cursor": nextCursor,
		"total_rows":  totalRows,
		"page":        query.Page,
		"page_size":   query.PageSize,
	})
}

func (s *Server) handleTopProcesses(w http.ResponseWriter, r *http.Request) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	source, err := s.store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	page, pageSize, err := parsePageParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	entries, totalRows, err := s.store.QueryTopProcesses(
		r.Context(),
		start,
		end,
		source,
		r.URL.Query().Get("group_by"),
		r.URL.Query().Get("sort_by"),
		r.URL.Query().Get("sort_order"),
		pageSize,
		(page-1)*pageSize,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{
		"data_source": source,
		"data":        entries,
		"total_rows":  totalRows,
		"page":        page,
		"page_size":   pageSize,
	})
}

func (s *Server) handleTopRemotes(w http.ResponseWriter, r *http.Request) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	source, err := s.store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	page, pageSize, err := parsePageParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err)
		return
	}
	includeLoopback := parseBoolFlag(r.URL.Query().Get("include_loopback"))
	entries, totalRows, err := s.store.QueryTopRemotes(
		r.Context(),
		start,
		end,
		source,
		model.Direction(r.URL.Query().Get("direction")),
		includeLoopback,
		r.URL.Query().Get("sort_by"),
		r.URL.Query().Get("sort_order"),
		pageSize,
		(page-1)*pageSize,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{
		"data_source": source,
		"data":        entries,
		"total_rows":  totalRows,
		"page":        page,
		"page_size":   pageSize,
	})
}

func (s *Server) handleTopPorts(w http.ResponseWriter, r *http.Request) {
	s.handleTop(w, r, s.store.QueryTopPorts)
}

func (s *Server) handleTop(
	w http.ResponseWriter,
	r *http.Request,
	queryFunc func(context.Context, time.Time, time.Time, string, string) ([]model.TopEntry, error),
) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_range", err)
		return
	}
	source, err := s.store.ResolveUsageSource(start, end, false, false)
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	entries, err := queryFunc(r.Context(), start, end, source, r.URL.Query().Get("by"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{"data_source": source, "data": entries})
}

func (s *Server) handleForwardUsage(w http.ResponseWriter, r *http.Request) {
	query, err := parseForwardQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_forward_query", err)
		return
	}
	source, err := s.store.ResolveUsageSource(query.Start, query.End, false, false)
	if err != nil {
		writeDimensionError(w, err)
		return
	}
	records, nextCursor, totalRows, err := s.store.QueryForwardUsage(r.Context(), query, source)
	if err != nil {
		if errors.Is(err, store.ErrCursorSortUnsupported) {
			writeError(w, http.StatusBadRequest, "invalid_forward_query", err)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}
	writeJSON(w, http.StatusOK, envelope{
		"data_source": store.ForwardDataSource(source),
		"data":        records,
		"next_cursor": nextCursor,
		"total_rows":  totalRows,
		"page":        query.Page,
		"page_size":   query.PageSize,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, envelope{
		"error":   code,
		"message": err.Error(),
	})
}

func writeDimensionError(w http.ResponseWriter, err error) {
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
	query := r.URL.Query()
	if startValue := query.Get("start"); startValue != "" {
		return parseExplicitWindow(query, startValue)
	}

	rangeLabel := strings.TrimSpace(query.Get("range"))
	if rangeLabel == "" {
		rangeLabel = "24h"
	}
	duration, err := parseRange(rangeLabel)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	end := time.Now().UTC()
	start := end.Add(-duration)
	return normalizeWindow(start, end, rangeLabel)
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
		Start:       start,
		End:         end,
		Comm:        r.URL.Query().Get("comm"),
		Exe:         r.URL.Query().Get("exe"),
		RemoteIP:    r.URL.Query().Get("remote_ip"),
		Direction:   model.Direction(r.URL.Query().Get("direction")),
		Proto:       r.URL.Query().Get("proto"),
		Attribution: model.Attribution(r.URL.Query().Get("attribution")),
		Limit:       listParams.Limit,
		Page:        listParams.Page,
		PageSize:    listParams.PageSize,
		SortBy:      listParams.SortBy,
		SortOrder:   listParams.SortOrder,
		CursorTS:    listParams.CursorTS,
		CursorRowID: listParams.CursorRowID,
		UsePage:     listParams.UsePage,
	}
	if pidValue := r.URL.Query().Get("pid"); pidValue != "" {
		pid, err := strconv.Atoi(pidValue)
		if err != nil {
			return query, fmt.Errorf("invalid pid: %w", err)
		}
		query.PID = &pid
	}
	if portValue := r.URL.Query().Get("local_port"); portValue != "" {
		port, err := strconv.Atoi(portValue)
		if err != nil {
			return query, fmt.Errorf("invalid local_port: %w", err)
		}
		query.LocalPort = &port
	}
	if !query.UsePage {
		if err := validateCursorSort(query.SortBy, query.SortOrder); err != nil {
			return query, err
		}
	}
	return query, nil
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
		Start:       start,
		End:         end,
		Proto:       r.URL.Query().Get("proto"),
		OrigSrcIP:   r.URL.Query().Get("orig_src_ip"),
		OrigDstIP:   r.URL.Query().Get("orig_dst_ip"),
		Limit:       listParams.Limit,
		Page:        listParams.Page,
		PageSize:    listParams.PageSize,
		SortBy:      listParams.SortBy,
		SortOrder:   listParams.SortOrder,
		CursorTS:    listParams.CursorTS,
		CursorRowID: listParams.CursorRowID,
		UsePage:     listParams.UsePage,
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
		days, err := strconv.Atoi(strings.TrimSuffix(value, "d"))
		if err != nil {
			return 0, err
		}
		if days <= 0 {
			return 0, fmt.Errorf("range must be positive")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, fmt.Errorf("range must be positive")
	}
	return duration, nil
}

func parseBucket(value string) time.Duration {
	if value == "" {
		return time.Minute
	}
	if strings.HasSuffix(value, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(value, "d"))
		if err == nil {
			return time.Duration(days) * 24 * time.Hour
		}
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return time.Minute
	}
	return duration
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
		if source == store.DataSourceHour {
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
	return start.UTC(), end.UTC(), label, nil
}

type listQueryParams struct {
	Limit       int
	Page        int
	PageSize    int
	SortBy      string
	SortOrder   string
	CursorTS    int64
	CursorRowID int64
	UsePage     bool
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

	params := listQueryParams{
		Limit:     limit,
		Page:      normalizePositivePage(page),
		PageSize:  normalizePageSize(pageSize),
		SortBy:    query.Get("sort_by"),
		SortOrder: query.Get("sort_order"),
		UsePage:   query.Has("page") || query.Has("page_size"),
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

func normalizePositivePage(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func normalizePageSize(value int) int {
	switch {
	case value <= 0:
		return 50
	case value > 200:
		return 200
	default:
		return value
	}
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

func parsePageParams(r *http.Request) (int, int, error) {
	page, err := parseIntWithDefault(r.URL.Query().Get("page"), 1, "page")
	if err != nil {
		return 0, 0, err
	}
	if page <= 0 {
		page = 1
	}
	pageSize, err := parseIntWithDefault(r.URL.Query().Get("page_size"), 25, "page_size")
	if err != nil {
		return 0, 0, err
	}
	if pageSize <= 0 {
		pageSize = 25
	}
	return page, pageSize, nil
}

func parseBoolFlag(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
