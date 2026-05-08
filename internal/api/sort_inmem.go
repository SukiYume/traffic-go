package api

import (
	"sort"
	"strings"

	"traffic-go/internal/model"
)

func sortProcessSummaries(rows []model.ProcessSummary, sortBy string, sortOrder string) []model.ProcessSummary {
	out := append([]model.ProcessSummary(nil), rows...)
	desc := !strings.EqualFold(sortOrder, "asc")
	cmp := processComparator(sortBy)
	sort.SliceStable(out, func(i, j int) bool {
		primary := cmp(out[i], out[j])
		if primary != 0 {
			if desc {
				return primary > 0
			}
			return primary < 0
		}
		tie := cmpStringNoCase(out[i].Comm, out[j].Comm)
		if tie != 0 {
			return tie < 0
		}
		return cmpInt(derefPID(out[i].PID), derefPID(out[j].PID)) < 0
	})
	return out
}

func processComparator(sortBy string) func(a, b model.ProcessSummary) int {
	switch sortBy {
	case "bytes_up":
		return func(a, b model.ProcessSummary) int { return cmpInt64(a.BytesUp, b.BytesUp) }
	case "bytes_down":
		return func(a, b model.ProcessSummary) int { return cmpInt64(a.BytesDown, b.BytesDown) }
	case "flow_count":
		return func(a, b model.ProcessSummary) int { return cmpInt64(a.FlowCount, b.FlowCount) }
	case "comm":
		return func(a, b model.ProcessSummary) int { return cmpStringNoCase(a.Comm, b.Comm) }
	case "pid":
		return func(a, b model.ProcessSummary) int { return cmpInt(derefPID(a.PID), derefPID(b.PID)) }
	default:
		return func(a, b model.ProcessSummary) int {
			return cmpInt64(a.BytesUp+a.BytesDown, b.BytesUp+b.BytesDown)
		}
	}
}

func sortRemoteSummaries(rows []model.RemoteSummary, sortBy string, sortOrder string) []model.RemoteSummary {
	out := append([]model.RemoteSummary(nil), rows...)
	desc := !strings.EqualFold(sortOrder, "asc")
	cmp := remoteComparator(sortBy)
	sort.SliceStable(out, func(i, j int) bool {
		primary := cmp(out[i], out[j])
		if primary != 0 {
			if desc {
				return primary > 0
			}
			return primary < 0
		}
		tie := strings.Compare(out[i].RemoteIP, out[j].RemoteIP)
		if tie != 0 {
			return tie < 0
		}
		return strings.Compare(string(out[i].Direction), string(out[j].Direction)) < 0
	})
	return out
}

func remoteComparator(sortBy string) func(a, b model.RemoteSummary) int {
	switch sortBy {
	case "bytes_up":
		return func(a, b model.RemoteSummary) int { return cmpInt64(a.BytesUp, b.BytesUp) }
	case "bytes_down":
		return func(a, b model.RemoteSummary) int { return cmpInt64(a.BytesDown, b.BytesDown) }
	case "flow_count":
		return func(a, b model.RemoteSummary) int { return cmpInt64(a.FlowCount, b.FlowCount) }
	case "remote_ip":
		return func(a, b model.RemoteSummary) int { return strings.Compare(a.RemoteIP, b.RemoteIP) }
	case "direction":
		return func(a, b model.RemoteSummary) int {
			return strings.Compare(string(a.Direction), string(b.Direction))
		}
	default:
		return func(a, b model.RemoteSummary) int {
			return cmpInt64(a.BytesUp+a.BytesDown, b.BytesUp+b.BytesDown)
		}
	}
}

func pageSlice[T any](rows []T, page int, pageSize int) []T {
	if pageSize <= 0 || page <= 0 {
		return nil
	}
	offset := (page - 1) * pageSize
	if offset >= len(rows) {
		return nil
	}
	end := offset + pageSize
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end]
}

func cmpInt64(a int64, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpInt(a int, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpStringNoCase(a string, b string) int {
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

func derefPID(pid *int) int {
	if pid == nil {
		return 0
	}
	return *pid
}
