package collector

import (
	"testing"
	"time"

	"traffic-go/internal/model"
)

func TestParseConntrackLine(t *testing.T) {
	line := "ipv4 2 tcp 6 431999 ESTABLISHED src=10.0.0.2 dst=1.1.1.1 sport=51544 dport=443 packets=10 bytes=1024 src=1.1.1.1 dst=10.0.0.2 sport=443 dport=51544 packets=8 bytes=4096 [ASSURED] mark=0 use=1 id=12345"
	flow, ok, err := parseConntrackLine(line)
	if err != nil {
		t.Fatalf("parse conntrack: %v", err)
	}
	if !ok {
		t.Fatalf("expected flow")
	}
	if flow.CTID != 12345 || flow.OrigSrcIP != "10.0.0.2" || flow.ReplyBytes != 4096 {
		t.Fatalf("unexpected flow: %+v", flow)
	}
	if !flow.HasAccounting {
		t.Fatalf("expected accounting flag")
	}
}

func TestParseConntrackLineWithoutIDOrCounters(t *testing.T) {
	line := "ipv4 2 tcp 6 100 TIME_WAIT src=159.226.171.226 dst=217.69.7.251 sport=52059 dport=12096 src=217.69.7.251 dst=159.226.171.226 sport=12096 dport=52059 mark=0 zone=0 use=2"
	flow, ok, err := parseConntrackLine(line)
	if err != nil {
		t.Fatalf("parse conntrack without id: %v", err)
	}
	if !ok {
		t.Fatalf("expected flow")
	}
	if flow.CTID == 0 {
		t.Fatalf("expected synthetic ctid")
	}
	if flow.HasAccounting {
		t.Fatalf("did not expect accounting flag")
	}
}

func TestDecodeProcEndpoint(t *testing.T) {
	ip, port, err := decodeProcEndpoint("0100007F:1F90")
	if err != nil {
		t.Fatalf("decode endpoint: %v", err)
	}
	if ip != "127.0.0.1" || port != 8080 {
		t.Fatalf("unexpected endpoint %s:%d", ip, port)
	}
}

func TestClassifyFlow(t *testing.T) {
	flow := model.ConntrackFlow{
		Proto:       "tcp",
		OrigSrcIP:   "192.0.2.10",
		OrigDstIP:   "198.51.100.20",
		OrigSrcPort: 50000,
		OrigDstPort: 443,
	}
	localIPs := map[string]struct{}{"192.0.2.10": {}}
	classified := classifyFlow(flow, localIPs)
	if classified.Direction != model.DirectionOut {
		t.Fatalf("unexpected direction: %s", classified.Direction)
	}
}

func TestUpdateSnapshotUsesHeuristicAttributionForUDPFallback(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	snapshot, _, _ := service.updateSnapshot(
		now,
		model.ConntrackFlow{Proto: "udp", OrigBytes: 10, ReplyBytes: 20, OrigPkts: 1, ReplyPkts: 2},
		classifiedFlow{
			Proto:          "udp",
			Direction:      model.DirectionOut,
			LocalIP:        "10.0.0.2",
			LocalPort:      53000,
			RemoteIP:       "8.8.8.8",
			RemotePort:     53,
			Connected:      false,
			MatchedByLocal: true,
		},
		model.ProcessInfo{PID: 1888, Comm: "dnsproxy", Exe: "/usr/bin/dnsproxy"},
		model.FlowSnapshot{},
		false,
	)

	if snapshot.Attribution != model.AttributionHeuristic {
		t.Fatalf("expected heuristic attribution, got %s", snapshot.Attribution)
	}
	if snapshot.PID != 1888 {
		t.Fatalf("expected pid 1888, got %d", snapshot.PID)
	}
}

func TestUpdateSnapshotFallsBackToGuessFromPreviousSnapshot(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	prev := model.FlowSnapshot{
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     41002,
		RemoteIP:      "142.250.72.14",
		RemotePort:    443,
		PID:           2451,
		Comm:          "curl",
		Exe:           "/usr/bin/curl",
		Attribution:   model.AttributionExact,
		StartedAt:     now.Add(-time.Minute),
		BaselineOrig:  100,
		BaselineReply: 220,
		LastOrig:      150,
		LastReply:     300,
		BaselineOPkts: 10,
		BaselineRPkts: 14,
		LastOPkts:     20,
		LastRPkts:     26,
	}

	snapshot, delta, _ := service.updateSnapshot(
		now,
		model.ConntrackFlow{Proto: "tcp", OrigBytes: 190, ReplyBytes: 360, OrigPkts: 24, ReplyPkts: 32},
		classifiedFlow{
			Proto:      "tcp",
			Direction:  model.DirectionOut,
			LocalIP:    "10.0.0.2",
			LocalPort:  41002,
			RemoteIP:   "142.250.72.14",
			RemotePort: 443,
			Connected:  true,
		},
		model.ProcessInfo{},
		prev,
		true,
	)

	if snapshot.Attribution != model.AttributionGuess {
		t.Fatalf("expected guess attribution, got %s", snapshot.Attribution)
	}
	if snapshot.PID != 2451 || snapshot.Comm != "curl" {
		t.Fatalf("expected previous process to be reused, got %+v", snapshot)
	}
	if delta == nil || delta.upBytes <= 0 || delta.downBytes <= 0 {
		t.Fatalf("expected non-zero delta from previous snapshot, got %+v", delta)
	}
}

func TestProcessHintCacheLookupAndExpiry(t *testing.T) {
	service := &Service{processHints: make(map[string]processHint)}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	service.rememberProcessHint(model.FlowSnapshot{
		Proto:     "tcp",
		Direction: model.DirectionOut,
		LocalIP:   "10.0.0.2",
		LocalPort: 8443,
		PID:       3312,
		Comm:      "nginx",
		Exe:       "/usr/sbin/nginx",
	}, now)

	resolved, ok := service.lookupProcessHint(classifiedFlow{
		Proto:     "tcp",
		Direction: model.DirectionOut,
		LocalIP:   "10.0.0.2",
		LocalPort: 8443,
	}, now.Add(30*time.Second))
	if !ok || resolved.PID != 3312 {
		t.Fatalf("expected active process hint, got %+v, ok=%v", resolved, ok)
	}

	_, expired := service.lookupProcessHint(classifiedFlow{
		Proto:     "tcp",
		Direction: model.DirectionOut,
		LocalIP:   "10.0.0.2",
		LocalPort: 8443,
	}, now.Add(2*time.Minute))
	if expired {
		t.Fatalf("expected hint to expire")
	}
}
