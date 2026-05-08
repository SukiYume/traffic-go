package api

import (
	"testing"

	"traffic-go/internal/model"
)

func intPtr(value int) *int {
	return &value
}

func TestSortProcessSummariesByTotalDesc(t *testing.T) {
	rows := []model.ProcessSummary{
		{Comm: "a", BytesUp: 10, BytesDown: 1},
		{Comm: "b", BytesUp: 1, BytesDown: 50},
		{Comm: "c", BytesUp: 20, BytesDown: 1},
	}
	sorted := sortProcessSummaries(rows, "bytes_total", "desc")
	if sorted[0].Comm != "b" || sorted[1].Comm != "c" || sorted[2].Comm != "a" {
		t.Fatalf("unexpected order: %+v", sorted)
	}
	if rows[0].Comm != "a" {
		t.Fatal("sortProcessSummaries should not mutate input")
	}
}

func TestSortProcessSummariesByPIDAsc(t *testing.T) {
	rows := []model.ProcessSummary{
		{PID: intPtr(30), Comm: "c"},
		{PID: intPtr(10), Comm: "a"},
		{PID: intPtr(20), Comm: "b"},
	}
	sorted := sortProcessSummaries(rows, "pid", "asc")
	if *sorted[0].PID != 10 || *sorted[1].PID != 20 || *sorted[2].PID != 30 {
		t.Fatalf("unexpected pid order: %+v", sorted)
	}
}

func TestPageSlice(t *testing.T) {
	rows := []model.ProcessSummary{{Comm: "a"}, {Comm: "b"}, {Comm: "c"}}
	got := pageSlice(rows, 2, 2)
	if len(got) != 1 || got[0].Comm != "c" {
		t.Fatalf("expected second page [c], got %+v", got)
	}
	got = pageSlice(rows, 5, 10)
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %d rows", len(got))
	}
	got = pageSlice(rows, 1, 1)
	if len(got) != 1 || got[0].Comm != "a" {
		t.Fatalf("expected [a], got %+v", got)
	}
}

func TestSortRemoteSummariesByFlowCountAsc(t *testing.T) {
	rows := []model.RemoteSummary{
		{RemoteIP: "a", FlowCount: 5},
		{RemoteIP: "b", FlowCount: 1},
		{RemoteIP: "c", FlowCount: 10},
	}
	sorted := sortRemoteSummaries(rows, "flow_count", "asc")
	if sorted[0].RemoteIP != "b" || sorted[1].RemoteIP != "a" || sorted[2].RemoteIP != "c" {
		t.Fatalf("unexpected order: %+v", sorted)
	}
}

func TestSortRemoteSummariesUsesAscendingTieBreaker(t *testing.T) {
	rows := []model.RemoteSummary{
		{RemoteIP: "b", BytesUp: 10},
		{RemoteIP: "a", BytesUp: 10},
	}
	sorted := sortRemoteSummaries(rows, "bytes_up", "desc")
	if sorted[0].RemoteIP != "a" || sorted[1].RemoteIP != "b" {
		t.Fatalf("unexpected tie-break order: %+v", sorted)
	}
}
