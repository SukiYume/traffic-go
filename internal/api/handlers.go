package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type envelope map[string]any

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"ok": true})
}

func (s *Server) handleProcesses(w http.ResponseWriter, r *http.Request) {
	processes := s.runtime.ActiveProcesses()
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
	page, pageSize := parsePageParams(r)
	entries, totalRows, err := s.store.QueryTopProcesses(
		r.Context(),
		start,
		end,
		source,
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
	page, pageSize := parsePageParams(r)
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
		return start.UTC(), end.UTC(), fmt.Sprintf("%s..%s", startValue, endValue), nil
	}

	rangeLabel := query.Get("range")
	if rangeLabel == "" {
		rangeLabel = "24h"
	}
	duration, err := parseRange(rangeLabel)
	if err != nil {
		return time.Time{}, time.Time{}, "", err
	}
	end := time.Now().UTC()
	start := end.Add(-duration)
	return start, end, rangeLabel, nil
}

func parseUsageQuery(r *http.Request) (model.UsageQuery, error) {
	start, end, _, err := parseWindow(r)
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
		Limit:       parseIntWithDefault(r.URL.Query().Get("limit"), 200),
		Page:        parseIntWithDefault(r.URL.Query().Get("page"), 1),
		PageSize:    parseIntWithDefault(r.URL.Query().Get("page_size"), 50),
		SortBy:      r.URL.Query().Get("sort_by"),
		SortOrder:   r.URL.Query().Get("sort_order"),
		UsePage:     r.URL.Query().Has("page") || r.URL.Query().Has("page_size"),
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
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		ts, rowID, err := store.DecodeCursor(cursor)
		if err != nil {
			return query, fmt.Errorf("invalid cursor: %w", err)
		}
		query.CursorTS = ts
		query.CursorRowID = rowID
	}
	return query, nil
}

func parseForwardQuery(r *http.Request) (model.ForwardQuery, error) {
	start, end, _, err := parseWindow(r)
	if err != nil {
		return model.ForwardQuery{}, err
	}
	query := model.ForwardQuery{
		Start:     start,
		End:       end,
		Proto:     r.URL.Query().Get("proto"),
		OrigSrcIP: r.URL.Query().Get("orig_src_ip"),
		OrigDstIP: r.URL.Query().Get("orig_dst_ip"),
		Limit:     parseIntWithDefault(r.URL.Query().Get("limit"), 200),
		Page:      parseIntWithDefault(r.URL.Query().Get("page"), 1),
		PageSize:  parseIntWithDefault(r.URL.Query().Get("page_size"), 50),
		SortBy:    r.URL.Query().Get("sort_by"),
		SortOrder: r.URL.Query().Get("sort_order"),
		UsePage:   r.URL.Query().Has("page") || r.URL.Query().Has("page_size"),
	}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		ts, rowID, err := store.DecodeCursor(cursor)
		if err != nil {
			return query, fmt.Errorf("invalid cursor: %w", err)
		}
		query.CursorTS = ts
		query.CursorRowID = rowID
	}
	return query, nil
}

func parseRange(value string) (time.Duration, error) {
	if strings.HasSuffix(value, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(value, "d"))
		if err != nil {
			return 0, err
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(value)
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

func parseIntWithDefault(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parsePageParams(r *http.Request) (int, int) {
	page := parseIntWithDefault(r.URL.Query().Get("page"), 1)
	if page <= 0 {
		page = 1
	}
	pageSize := parseIntWithDefault(r.URL.Query().Get("page_size"), 25)
	if pageSize <= 0 {
		pageSize = 25
	}
	return page, pageSize
}

func parseBoolFlag(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
