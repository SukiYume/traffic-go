package collector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

func TestUnrepresentableKernelCountersAreDroppedWithoutPoisoningTheBaseline(t *testing.T) {
	overflow := uint64(math.MaxInt64) + 1
	if _, ok := representableCounterDelta(overflow, 0); ok {
		t.Fatal("counter delta above SQLite int64 range must be rejected")
	}
	if delta, ok := representableCounterDelta(overflow+9, overflow); !ok || delta != 9 {
		t.Fatalf("collector must resume after an overflow sample, got delta=%d ok=%v", delta, ok)
	}
	if _, ok := representableFlowDelta(model.DirectionOut, overflow, 0, 0, 0); ok {
		t.Fatal("flow delta above SQLite int64 range must be rejected")
	}

	service := &Service{}
	now := time.Unix(1_700_000_000, 0).UTC()
	flow := model.ConntrackFlow{CTID: 77, Proto: "tcp", OrigBytes: overflow, ReplyBytes: 10, OrigPkts: 1, ReplyPkts: 1}
	classified := classifiedFlow{Direction: model.DirectionOut}
	snapshot, delta, forwardDelta, countFlow, resetOwners := service.updateSnapshot(
		now, flow, classified, model.ProcessInfo{}, model.FlowSnapshot{}, false, true, true,
	)
	if delta != nil || forwardDelta != nil || !countFlow || resetOwners {
		t.Fatalf("overflow sample should retain only flow count: delta=%+v forward=%+v count=%v reset=%v", delta, forwardDelta, countFlow, resetOwners)
	}
	if snapshot.LastOrig != overflow {
		t.Fatalf("overflow sample did not advance baseline: got %d want %d", snapshot.LastOrig, overflow)
	}

	flow.OrigBytes += 9
	_, delta, forwardDelta, countFlow, resetOwners = service.updateSnapshot(
		now.Add(time.Second), flow, classified, model.ProcessInfo{}, snapshot, true, true, true,
	)
	if delta == nil || delta.upBytes != 9 || delta.downBytes != 0 || forwardDelta != nil || countFlow || resetOwners {
		t.Fatalf("collector did not recover after overflow baseline: delta=%+v forward=%+v count=%v reset=%v", delta, forwardDelta, countFlow, resetOwners)
	}
}

func TestFirstSyntheticObservationAfterBaselineDoesNotChargeLifetimeCounters(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 7, 11, 5, 50, 0, 0, time.UTC)
	flow := model.ConntrackFlow{
		CTID:          9001,
		SyntheticCTID: true,
		HasAccounting: true,
		Proto:         "udp",
		OrigBytes:     142_553_076_632,
		ReplyBytes:    92_305_874_281,
		OrigPkts:      1_000_000,
		ReplyPkts:     900_000,
	}
	classified := classifiedFlow{
		Proto:      "udp",
		Direction:  model.DirectionOut,
		LocalIP:    "217.69.7.251",
		LocalPort:  55628,
		RemoteIP:   "162.159.192.1",
		RemotePort: 2408,
	}

	snapshot, delta, forwardDelta, countFlow, resetOwners := service.updateSnapshot(
		now, flow, classified, model.ProcessInfo{}, model.FlowSnapshot{}, false, true, false,
	)
	if delta != nil || forwardDelta != nil || !countFlow || resetOwners {
		t.Fatalf("first synthetic observation must seed counters only: delta=%+v forward=%+v count=%v reset=%v", delta, forwardDelta, countFlow, resetOwners)
	}
	if snapshot.LastOrig != flow.OrigBytes || snapshot.LastReply != flow.ReplyBytes || !snapshot.Counted {
		t.Fatalf("unexpected synthetic baseline: %+v", snapshot)
	}

	flow.OrigBytes += 2048
	flow.ReplyBytes += 4096
	flow.OrigPkts++
	flow.ReplyPkts += 2
	_, delta, _, countFlow, resetOwners = service.updateSnapshot(
		now.Add(time.Second), flow, classified, model.ProcessInfo{}, snapshot, true, true, true,
	)
	if delta == nil || delta.upBytes != 2048 || delta.downBytes != 4096 || delta.upPkts != 1 || delta.downPkts != 2 || countFlow || resetOwners {
		t.Fatalf("synthetic flow must resume with incremental counters: delta=%+v count=%v reset=%v", delta, countFlow, resetOwners)
	}
}

func TestNewUsesNoopCollectorOutsideLinuxUnlessMockDataEnabled(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("non-linux safeguard only applies outside linux")
	}

	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")
	trafficStore, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = trafficStore.Close()
	})

	runner := New(cfg, trafficStore, nil)
	if _, ok := runner.(*noopCollector); !ok {
		t.Fatalf("expected noop collector on %s, got %T", runtime.GOOS, runner)
	}

	cfg.MockData = true
	runner = New(cfg, trafficStore, nil)
	if _, ok := runner.(*mockCollector); !ok {
		t.Fatalf("expected opt-in mock collector when mock_data=true, got %T", runner)
	}
}

func TestReadInterfaceCountersParsesProcNetDev(t *testing.T) {
	procFS := t.TempDir()
	netDir := filepath.Join(procFS, "net")
	if err := os.MkdirAll(netDir, 0o755); err != nil {
		t.Fatalf("create proc net dir: %v", err)
	}
	content := `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 100 1 0 0 0 0 0 0 200 2 0 0 0 0 0 0
  eth0: 12345 10 0 0 0 0 0 0 67890 20 0 0 0 0 0 0
`
	if err := os.WriteFile(filepath.Join(netDir, "dev"), []byte(content), 0o644); err != nil {
		t.Fatalf("write proc net dev: %v", err)
	}

	counters, err := ReadInterfaceCounters(procFS)
	if err != nil {
		t.Fatalf("read interface counters: %v", err)
	}
	if len(counters) != 2 {
		t.Fatalf("expected two counters, got %+v", counters)
	}
	if counters[1].Name != "eth0" || counters[1].RxBytes != 12345 || counters[1].TxBytes != 67890 {
		t.Fatalf("unexpected eth0 counter: %+v", counters[1])
	}
}

func TestCollectInterfaceDeltasUsesConfiguredInterfaces(t *testing.T) {
	procFS := t.TempDir()
	writeProcNetDev(t, procFS, map[string][2]uint64{
		"lo":   {100, 100},
		"eth0": {1000, 2000},
		"ens3": {3000, 4000},
	})
	cfg := config.Default()
	cfg.ProcFS = procFS
	cfg.NetworkInterfaces = []string{"eth0"}
	service := &Service{cfg: cfg}

	if err := service.collectInterfaceDeltas(); err != nil {
		t.Fatalf("collect baseline interface deltas: %v", err)
	}
	writeProcNetDev(t, procFS, map[string][2]uint64{
		"lo":   {900, 900},
		"eth0": {1100, 2300},
		"ens3": {9000, 9000},
	})
	if err := service.collectInterfaceDeltas(); err != nil {
		t.Fatalf("collect interface deltas: %v", err)
	}

	if len(service.interfaceBuckets) != 1 {
		t.Fatalf("expected only configured interface bucket, got %+v", service.interfaceBuckets)
	}
	delta := service.interfaceBuckets["eth0"]
	if delta.RxBytes != 100 || delta.TxBytes != 300 {
		t.Fatalf("unexpected eth0 delta: %+v", delta)
	}
	if _, exists := service.interfaceBuckets["ens3"]; exists {
		t.Fatalf("unexpected ens3 bucket: %+v", service.interfaceBuckets)
	}
	if _, exists := service.interfaceBuckets["lo"]; exists {
		t.Fatalf("unexpected loopback bucket: %+v", service.interfaceBuckets)
	}
}

func writeProcNetDev(t *testing.T, procFS string, counters map[string][2]uint64) {
	t.Helper()
	netDir := filepath.Join(procFS, "net")
	if err := os.MkdirAll(netDir, 0o755); err != nil {
		t.Fatalf("create proc net dir: %v", err)
	}
	var builder strings.Builder
	builder.WriteString("Inter-|   Receive                                                |  Transmit\n")
	builder.WriteString(" face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n")
	for name, values := range counters {
		builder.WriteString(fmt.Sprintf(" %s: %d 1 0 0 0 0 0 0 %d 1 0 0 0 0 0 0\n", name, values[0], values[1]))
	}
	if err := os.WriteFile(filepath.Join(netDir, "dev"), []byte(builder.String()), 0o644); err != nil {
		t.Fatalf("write proc net dev: %v", err)
	}
}

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
	if flow.SyntheticCTID {
		t.Fatal("kernel-provided id must not be marked synthetic")
	}
	if !flow.HasAccounting {
		t.Fatalf("expected accounting flag")
	}
}

func TestParseConntrackObservationCarriesZone(t *testing.T) {
	line := "ipv4 2 tcp 6 431999 ESTABLISHED src=10.0.0.2 dst=1.1.1.1 sport=51544 dport=443 packets=10 bytes=1024 src=1.1.1.1 dst=10.0.0.2 sport=443 dport=51544 packets=8 bytes=4096 mark=0 zone=17 use=1 id=12345"
	observation, ok, err := parseConntrackObservation(line)
	if err != nil {
		t.Fatalf("parse conntrack observation: %v", err)
	}
	if !ok || observation.Zone != 17 || observation.Flow.CTID != 12345 {
		t.Fatalf("unexpected zone-aware observation: %+v", observation)
	}
}

func TestParseConntrackLineNormalizesIPv6ForLocalClassification(t *testing.T) {
	line := "ipv6 10 tcp 6 431998 ESTABLISHED src=2400:dd01:102d:1001:8469:ee49:ee8f:a2cf dst=2001:19f0:6801:10f4:5400:01ff:fedc:a255 sport=61650 dport=12096 packets=2607 bytes=1475916 src=2001:19f0:6801:10f4:5400:01ff:fedc:a255 dst=2400:dd01:102d:1001:8469:ee49:ee8f:a2cf sport=12096 dport=61650 packets=2677 bytes=4627395 [ASSURED] mark=0 zone=0 use=2 id=77"
	flow, ok, err := parseConntrackLine(line)
	if err != nil {
		t.Fatalf("parse conntrack ipv6: %v", err)
	}
	if !ok {
		t.Fatalf("expected flow")
	}

	localIP := "2001:19f0:6801:10f4:5400:1ff:fedc:a255"
	if flow.OrigDstIP != localIP || flow.ReplySrcIP != localIP {
		t.Fatalf("expected normalized local IPv6 %s, got orig_dst=%s reply_src=%s", localIP, flow.OrigDstIP, flow.ReplySrcIP)
	}

	classified := classifyFlow(flow, map[string]struct{}{localIP: {}}, socketIndex{})
	if classified.Direction != model.DirectionIn {
		t.Fatalf("expected normalized IPv6 flow to classify as inbound, got %+v", classified)
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
	if !flow.SyntheticCTID {
		t.Fatal("tuple-derived fallback id must be marked synthetic")
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
	classified := classifyFlow(flow, localIPs, socketIndex{})
	if classified.Direction != model.DirectionOut {
		t.Fatalf("unexpected direction: %s", classified.Direction)
	}
}

func TestClassifyFlowUsesTranslatedReplyTuple(t *testing.T) {
	flow := model.ConntrackFlow{
		Proto:        "tcp",
		OrigSrcIP:    "203.0.113.24",
		OrigDstIP:    "198.51.100.10",
		OrigSrcPort:  52144,
		OrigDstPort:  443,
		ReplySrcIP:   "10.88.0.5",
		ReplyDstIP:   "203.0.113.24",
		ReplySrcPort: 8388,
		ReplyDstPort: 52144,
	}
	replyTuple := tuple{
		Proto:      "tcp",
		LocalIP:    "10.88.0.5",
		LocalPort:  8388,
		RemoteIP:   "203.0.113.24",
		RemotePort: 52144,
	}
	classified := classifyFlow(flow, map[string]struct{}{}, socketIndex{
		ByTuple: map[string]socketEntry{
			replyTuple.key(): {Inode: 4321, Connected: true, Present: true},
		},
	})
	if classified.Direction != model.DirectionIn {
		t.Fatalf("expected inbound classification, got %s", classified.Direction)
	}
	if classified.LocalIP != "10.88.0.5" || classified.RemoteIP != "203.0.113.24" {
		t.Fatalf("unexpected translated tuple classification: %+v", classified)
	}
}

func TestClassifyFlowPrefersTranslatedReplyTupleOverLocalDestinationShortcut(t *testing.T) {
	flow := model.ConntrackFlow{
		Proto:        "tcp",
		OrigSrcIP:    "203.0.113.24",
		OrigDstIP:    "198.51.100.10",
		OrigSrcPort:  52144,
		OrigDstPort:  443,
		ReplySrcIP:   "10.88.0.5",
		ReplyDstIP:   "203.0.113.24",
		ReplySrcPort: 8388,
		ReplyDstPort: 52144,
	}
	replyTuple := tuple{
		Proto:      "tcp",
		LocalIP:    "10.88.0.5",
		LocalPort:  8388,
		RemoteIP:   "203.0.113.24",
		RemotePort: 52144,
	}

	classified := classifyFlow(flow, map[string]struct{}{
		"198.51.100.10": {},
	}, socketIndex{
		ByTuple: map[string]socketEntry{
			replyTuple.key(): {Inode: 4321, Connected: true, Present: true},
		},
	})
	if classified.Direction != model.DirectionIn {
		t.Fatalf("expected translated inbound classification, got %s", classified.Direction)
	}
	if classified.LocalIP != "10.88.0.5" || classified.LocalPort != 8388 {
		t.Fatalf("expected translated reply tuple to win, got %+v", classified)
	}
}

func TestUpdateSnapshotUsesHeuristicAttributionForUDPFallback(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	snapshot, _, _, _, _ := service.updateSnapshot(
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
		false,
		false,
	)

	if snapshot.Attribution != model.AttributionHeuristic {
		t.Fatalf("expected heuristic attribution, got %s", snapshot.Attribution)
	}
	if snapshot.PID != 1888 {
		t.Fatalf("expected pid 1888, got %d", snapshot.PID)
	}
}

func TestUpdateSnapshotUsesHeuristicAttributionForTCPLocalFallback(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	snapshot, _, _, _, _ := service.updateSnapshot(
		now,
		model.ConntrackFlow{Proto: "tcp", OrigBytes: 10, ReplyBytes: 20, OrigPkts: 1, ReplyPkts: 2},
		classifiedFlow{
			Proto:          "tcp",
			Direction:      model.DirectionIn,
			LocalIP:        "217.69.7.251",
			LocalPort:      12096,
			RemoteIP:       "159.226.171.226",
			RemotePort:     44598,
			Connected:      false,
			MatchedByLocal: true,
		},
		model.ProcessInfo{PID: 27896, Comm: "ss-server", Exe: "/usr/local/bin/ss-server"},
		model.FlowSnapshot{},
		false,
		false,
		false,
	)

	if snapshot.Attribution != model.AttributionHeuristic {
		t.Fatalf("expected heuristic attribution, got %s", snapshot.Attribution)
	}
	if snapshot.PID != 27896 || snapshot.Comm != "ss-server" {
		t.Fatalf("expected ss-server process mapping, got %+v", snapshot)
	}
}

func TestUpdateSnapshotUsesHeuristicAttributionForAmbiguousProcessOwner(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	snapshot, _, _, _, _ := service.updateSnapshot(
		now,
		model.ConntrackFlow{Proto: "tcp", OrigBytes: 10, ReplyBytes: 20, OrigPkts: 1, ReplyPkts: 2},
		classifiedFlow{
			Proto:      "tcp",
			Direction:  model.DirectionIn,
			LocalIP:    "217.69.7.251",
			LocalPort:  443,
			RemoteIP:   "159.226.171.226",
			RemotePort: 44598,
			Connected:  true,
		},
		model.ProcessInfo{PID: 100, Comm: "nginx", Exe: "/usr/sbin/nginx", Ambiguous: true},
		model.FlowSnapshot{},
		false,
		false,
		false,
	)

	if snapshot.Attribution != model.AttributionHeuristic {
		t.Fatalf("expected ambiguous owner to downgrade attribution to heuristic, got %s", snapshot.Attribution)
	}
}

func TestLookupLocalSocketFallbackMatchesInboundTCPAnyAddrListener(t *testing.T) {
	index := socketIndex{
		ByLocal: map[string]socketEntry{
			localTupleKey("tcp", "0.0.0.0", 12096): {
				Inode:     88001,
				Connected: false,
				Present:   true,
			},
		},
	}

	sock, ok := lookupLocalSocketFallback(index, classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionIn,
		LocalIP:    "217.69.7.251",
		LocalPort:  12096,
		RemoteIP:   "159.226.171.226",
		RemotePort: 44598,
	})
	if !ok {
		t.Fatalf("expected inbound tcp fallback to match wildcard listener")
	}
	if sock.Inode != 88001 {
		t.Fatalf("unexpected inode from fallback: %+v", sock)
	}
}

func TestLookupLocalSocketFallbackDoesNotApplyToOutboundTCP(t *testing.T) {
	index := socketIndex{
		ByLocal: map[string]socketEntry{
			localTupleKey("tcp", "0.0.0.0", 443): {
				Inode:     99001,
				Connected: false,
				Present:   true,
			},
		},
	}

	if _, ok := lookupLocalSocketFallback(index, classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "217.69.7.251",
		LocalPort:  443,
		RemoteIP:   "203.0.113.10",
		RemotePort: 52112,
	}); ok {
		t.Fatalf("expected outbound tcp to skip local listener fallback")
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

	snapshot, delta, _, _, _ := service.updateSnapshot(
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
		true,
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

func TestUpdateSnapshotCountsInitialDeltaAfterBaselineWarmup(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)

	snapshot, delta, _, countFlow, _ := service.updateSnapshot(
		now,
		model.ConntrackFlow{Proto: "tcp", OrigBytes: 512, ReplyBytes: 2048, OrigPkts: 5, ReplyPkts: 9},
		classifiedFlow{
			Proto:      "tcp",
			Direction:  model.DirectionOut,
			LocalIP:    "10.0.0.2",
			LocalPort:  47920,
			RemoteIP:   "142.250.72.14",
			RemotePort: 443,
			Connected:  true,
		},
		model.ProcessInfo{PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server"},
		model.FlowSnapshot{},
		false,
		true,
		false,
	)

	if snapshot.PID != 1088 {
		t.Fatalf("expected resolved pid on first warmed-up observation, got %+v", snapshot)
	}
	if delta == nil {
		t.Fatalf("expected first post-baseline observation to contribute delta")
	}
	if delta.upBytes != 512 || delta.downBytes != 2048 {
		t.Fatalf("unexpected first delta: %+v", delta)
	}
	if delta.upPkts != 5 || delta.downPkts != 9 {
		t.Fatalf("unexpected first packet delta: %+v", delta)
	}
	if !countFlow || !snapshot.Counted {
		t.Fatalf("expected first warmed-up observation to increment flow count once, got counted=%v snapshot=%+v", countFlow, snapshot)
	}
}

func TestUpdateSnapshotReturnsZeroDeltaForSameLineageTupleChange(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)

	prev := model.FlowSnapshot{
		CTID:          7001,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     47920,
		RemoteIP:      "198.51.100.44",
		RemotePort:    443,
		StartedAt:     now.Add(-20 * time.Second),
		BaselineOrig:  100,
		BaselineReply: 300,
		LastOrig:      400,
		LastReply:     1200,
		BaselineOPkts: 1,
		BaselineRPkts: 3,
		LastOPkts:     4,
		LastRPkts:     12,
		Counted:       true,
		LastSeen:      now.Add(-2 * time.Second),
	}

	snapshot, delta, forwardDelta, countFlow, resetOwners := service.updateSnapshot(
		now,
		model.ConntrackFlow{
			CTID:       7001,
			Proto:      "tcp",
			OrigBytes:  400,
			ReplyBytes: 1200,
			OrigPkts:   4,
			ReplyPkts:  12,
		},
		classifiedFlow{
			Proto:      "tcp",
			Direction:  model.DirectionIn,
			LocalIP:    "10.0.0.2",
			LocalPort:  47920,
			RemoteIP:   "198.51.100.44",
			RemotePort: 443,
			Connected:  true,
		},
		model.ProcessInfo{},
		prev,
		true,
		true,
		true,
	)

	if delta == nil || !delta.isZero() {
		t.Fatalf("expected zero delta pointer for rebucketing, got %+v", delta)
	}
	if forwardDelta != nil {
		t.Fatalf("did not expect forward delta for local reclassification, got %+v", forwardDelta)
	}
	if countFlow || resetOwners {
		t.Fatalf("did not expect recount/reset on same lineage reclassification, count=%v reset=%v", countFlow, resetOwners)
	}
	if snapshot.Direction != model.DirectionIn || !snapshot.Counted {
		t.Fatalf("expected snapshot to preserve counted flow with new tuple, got %+v", snapshot)
	}
}

func TestUpdateSnapshotReturnsZeroForwardDeltaForSameLineageForwardReclassification(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)

	prev := model.FlowSnapshot{
		CTID:          8001,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     8388,
		RemoteIP:      "198.51.100.44",
		RemotePort:    443,
		StartedAt:     now.Add(-20 * time.Second),
		BaselineOrig:  100,
		BaselineReply: 300,
		LastOrig:      400,
		LastReply:     1200,
		BaselineOPkts: 1,
		BaselineRPkts: 3,
		LastOPkts:     4,
		LastRPkts:     12,
		Counted:       true,
		LastSeen:      now.Add(-2 * time.Second),
	}

	snapshot, delta, forwardDelta, countFlow, resetOwners := service.updateSnapshot(
		now,
		model.ConntrackFlow{
			CTID:         8001,
			Proto:        "tcp",
			OrigSrcIP:    "203.0.113.24",
			OrigDstIP:    "198.51.100.44",
			OrigSrcPort:  52144,
			OrigDstPort:  443,
			ReplySrcIP:   "198.51.100.44",
			ReplyDstIP:   "203.0.113.24",
			ReplySrcPort: 443,
			ReplyDstPort: 52144,
			OrigBytes:    400,
			ReplyBytes:   1200,
			OrigPkts:     4,
			ReplyPkts:    12,
		},
		classifiedFlow{
			Proto:       "tcp",
			Direction:   model.DirectionForward,
			OrigSrcIP:   "203.0.113.24",
			OrigDstIP:   "198.51.100.44",
			OrigSrcPort: 52144,
			OrigDstPort: 443,
		},
		model.ProcessInfo{},
		prev,
		true,
		true,
		true,
	)

	if delta != nil {
		t.Fatalf("did not expect local delta when reclassifying to forward, got %+v", delta)
	}
	if forwardDelta == nil || !forwardDelta.isZero() {
		t.Fatalf("expected zero forward delta pointer for rebucketing, got %+v", forwardDelta)
	}
	if countFlow || resetOwners {
		t.Fatalf("did not expect recount/reset on same lineage forward reclassification, count=%v reset=%v", countFlow, resetOwners)
	}
	if snapshot.Direction != model.DirectionForward || !snapshot.Counted {
		t.Fatalf("expected forward snapshot to preserve counted state, got %+v", snapshot)
	}
}

func TestNormalizeObservedFlowsDedupesDuplicateTupleByMaxCounters(t *testing.T) {
	flows := []model.ConntrackFlow{
		{
			CTID:          1001,
			Family:        "ipv4",
			Proto:         "udp",
			OrigSrcIP:     "10.0.0.2",
			OrigDstIP:     "162.159.192.1",
			OrigSrcPort:   53337,
			OrigDstPort:   2408,
			ReplySrcIP:    "162.159.192.1",
			ReplyDstIP:    "10.0.0.2",
			ReplySrcPort:  2408,
			ReplyDstPort:  53337,
			OrigBytes:     420,
			ReplyBytes:    2100,
			OrigPkts:      4,
			ReplyPkts:     9,
			HasAccounting: true,
		},
		{
			CTID:          1002,
			Family:        "ipv4",
			Proto:         "udp",
			OrigSrcIP:     "10.0.0.2",
			OrigDstIP:     "162.159.192.1",
			OrigSrcPort:   53337,
			OrigDstPort:   2408,
			ReplySrcIP:    "162.159.192.1",
			ReplyDstIP:    "10.0.0.2",
			ReplySrcPort:  2408,
			ReplyDstPort:  53337,
			OrigBytes:     419,
			ReplyBytes:    2099,
			OrigPkts:      4,
			ReplyPkts:     9,
			HasAccounting: true,
		},
	}

	observed := normalizeObservedFlows(flows, map[string]struct{}{"10.0.0.2": {}}, socketIndex{})
	if len(observed) != 1 {
		t.Fatalf("expected duplicate tuple to collapse into one observed flow, got %d", len(observed))
	}
	if observed[0].Flow.OrigBytes != 420 || observed[0].Flow.ReplyBytes != 2100 {
		t.Fatalf("expected strongest counters to survive normalization, got %+v", observed[0].Flow)
	}
	if len(observed[0].RawIDs) != 2 {
		t.Fatalf("expected both raw conntrack IDs to be retained, got %+v", observed[0].RawIDs)
	}
}

func TestConntrackZonesKeepIdenticalTuplesIndependentlyAccounted(t *testing.T) {
	base := model.ConntrackFlow{
		Proto:         "tcp",
		Family:        "ipv4",
		OrigSrcIP:     "10.0.0.2",
		OrigDstIP:     "1.1.1.1",
		OrigSrcPort:   51544,
		OrigDstPort:   443,
		ReplySrcIP:    "1.1.1.1",
		ReplyDstIP:    "10.0.0.2",
		ReplySrcPort:  443,
		ReplyDstPort:  51544,
		OrigBytes:     100,
		ReplyBytes:    50,
		OrigPkts:      1,
		ReplyPkts:     1,
		HasAccounting: true,
	}
	first := base
	first.CTID = 1001
	second := base
	second.CTID = 2001
	second.OrigBytes = 200
	second.ReplyBytes = 75

	observed := normalizeConntrackObservations([]conntrackObservation{
		{Flow: first, Zone: 1},
		{Flow: second, Zone: 2},
	}, map[string]struct{}{"10.0.0.2": {}}, socketIndex{})
	if len(observed) != 2 {
		t.Fatalf("identical tuples from separate zones must remain independent, got %+v", observed)
	}
	if observed[0].Identity == observed[1].Identity {
		t.Fatalf("zone must participate in flow identity: %+v", observed)
	}

	minute := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	service := &Service{
		currentMinute: minute,
		buckets:       make(map[model.UsageKey]*bucketState),
	}
	for _, flow := range observed {
		snapshot, delta, _, countFlow, _ := service.updateSnapshot(
			minute,
			flow.Flow,
			flow.Classified,
			model.ProcessInfo{},
			model.FlowSnapshot{},
			false,
			true,
			false,
		)
		if delta == nil || !countFlow {
			t.Fatalf("expected independent initial contribution for zone %d", flow.Zone)
		}
		service.addUsage(string(flow.Identity), snapshot, *delta, countFlow)
	}
	if len(service.buckets) != 1 {
		t.Fatalf("zone is an accounting identity, not an exposed usage dimension: %+v", service.buckets)
	}
	for _, bucket := range service.buckets {
		if bucket.bytesUp != 300 || bucket.bytesDown != 125 || bucket.flowCount != 2 || len(bucket.perFlow) != 2 {
			t.Fatalf("expected zone contributions to sum instead of max-merge, got %+v", bucket)
		}
	}
}

func TestSocketIndexSnapshotDefaultIntervalIsIndependentOfCollectorTick(t *testing.T) {
	procFS := t.TempDir()
	writeProcTCP(t, procFS, 11111)

	service := &Service{
		cfg: config.Config{
			ProcFS:       procFS,
			TickInterval: 2 * time.Second,
		},
	}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	key := tuple{
		Proto:      "tcp",
		LocalIP:    "127.0.0.1",
		LocalPort:  12345,
		RemoteIP:   "198.51.100.2",
		RemotePort: 443,
	}.key()

	first, err := service.socketIndexSnapshot(now)
	if err != nil {
		t.Fatalf("read first socket index: %v", err)
	}
	if entry := first.ByTuple[key]; entry.Inode != 11111 {
		t.Fatalf("expected first inode 11111, got %+v", entry)
	}

	writeProcTCP(t, procFS, 22222)
	second, err := service.socketIndexSnapshot(now.Add(1500 * time.Millisecond))
	if err != nil {
		t.Fatalf("read second socket index: %v", err)
	}
	if entry := second.ByTuple[key]; entry.Inode != 11111 {
		t.Fatalf("expected default socket index cache to outlive next collector tick, got %+v", entry)
	}
}

func TestSocketIndexSnapshotUsesConfiguredInterval(t *testing.T) {
	procFS := t.TempDir()
	writeProcTCP(t, procFS, 11111)

	service := &Service{
		cfg: config.Config{
			ProcFS:              procFS,
			TickInterval:        2 * time.Second,
			SocketIndexInterval: 1500 * time.Millisecond,
		},
	}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	key := tuple{
		Proto:      "tcp",
		LocalIP:    "127.0.0.1",
		LocalPort:  12345,
		RemoteIP:   "198.51.100.2",
		RemotePort: 443,
	}.key()

	first, err := service.socketIndexSnapshot(now)
	if err != nil {
		t.Fatalf("read first socket index: %v", err)
	}
	if entry := first.ByTuple[key]; entry.Inode != 11111 {
		t.Fatalf("expected first inode 11111, got %+v", entry)
	}

	writeProcTCP(t, procFS, 22222)
	second, err := service.socketIndexSnapshot(now.Add(1500 * time.Millisecond))
	if err != nil {
		t.Fatalf("read second socket index: %v", err)
	}
	if entry := second.ByTuple[key]; entry.Inode != 22222 {
		t.Fatalf("expected socket index to refresh after configured interval, got %+v", entry)
	}
}

func writeProcTCP(t *testing.T, procFS string, inode uint64) {
	t.Helper()
	netDir := filepath.Join(procFS, "net")
	if err := os.MkdirAll(netDir, 0o755); err != nil {
		t.Fatalf("create proc net dir: %v", err)
	}
	content := fmt.Sprintf(`  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:3039 026433C6:01BB 01 00000000:00000000 00:00000000 00000000 1000 0 %d
`, inode)
	if err := os.WriteFile(filepath.Join(netDir, "tcp"), []byte(content), 0o644); err != nil {
		t.Fatalf("write proc tcp: %v", err)
	}
}

func TestCarryForwardSnapshotsPreventsRecountAfterTransientGap(t *testing.T) {
	service := &Service{}
	start := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	identity := "local|ipv4|udp|out|53337|162.159.192.1|2408|10.0.0.2"

	prev := model.FlowSnapshot{
		Proto:         "udp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     53337,
		RemoteIP:      "162.159.192.1",
		RemotePort:    2408,
		StartedAt:     start,
		BaselineOrig:  420,
		BaselineReply: 2100,
		LastOrig:      420,
		LastReply:     2100,
		BaselineOPkts: 4,
		BaselineRPkts: 9,
		LastOPkts:     4,
		LastRPkts:     9,
		Counted:       true,
		LastSeen:      start,
	}

	carried := service.carryForwardSnapshots(map[string]model.FlowSnapshot{
		identity: prev,
	}, start.Add(4*time.Second))
	snapshot, delta, _, countFlow, _ := service.updateSnapshot(
		start.Add(6*time.Second),
		model.ConntrackFlow{
			Proto:      "udp",
			OrigBytes:  420,
			ReplyBytes: 2100,
			OrigPkts:   4,
			ReplyPkts:  9,
		},
		classifiedFlow{
			Proto:      "udp",
			Direction:  model.DirectionOut,
			LocalIP:    "10.0.0.2",
			LocalPort:  53337,
			RemoteIP:   "162.159.192.1",
			RemotePort: 2408,
		},
		model.ProcessInfo{},
		carried[identity],
		true,
		true,
		true,
	)

	if countFlow {
		t.Fatalf("expected transient gap to preserve existing flow count state")
	}
	if delta == nil || !delta.isZero() {
		t.Fatalf("expected no byte delta after transient gap, got %+v", delta)
	}
	if snapshot.LastOrig != 420 || snapshot.LastReply != 2100 {
		t.Fatalf("unexpected snapshot after transient gap: %+v", snapshot)
	}
}

func TestSnapshotsForTickPreservesConfirmedLineageAfterLongSuccessfulSampleGap(t *testing.T) {
	start := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	identity := flowIdentity("local|ipv4|tcp|out|53337|198.51.100.8|443|10.0.0.2")
	previous := model.FlowSnapshot{
		CTID:          9001,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     53337,
		RemoteIP:      "198.51.100.8",
		RemotePort:    443,
		StartedAt:     start,
		BaselineOrig:  420,
		BaselineReply: 2100,
		LastOrig:      420,
		LastReply:     2100,
		BaselineOPkts: 4,
		BaselineRPkts: 9,
		LastOPkts:     4,
		LastRPkts:     9,
		Counted:       true,
		LastSeen:      start,
	}
	service := &Service{
		snapshots:     map[string]model.FlowSnapshot{string(identity): previous},
		snapshotReady: true,
	}
	observed := []observedFlow{{
		Identity: identity,
		Flow: model.ConntrackFlow{
			CTID:       9001,
			Proto:      "tcp",
			OrigBytes:  500,
			ReplyBytes: 2300,
			OrigPkts:   5,
			ReplyPkts:  11,
		},
		Classified: classifiedFlow{
			Proto:      "tcp",
			Direction:  model.DirectionOut,
			LocalIP:    "10.0.0.2",
			LocalPort:  53337,
			RemoteIP:   "198.51.100.8",
			RemotePort: 443,
		},
		RawIDs: map[uint64]struct{}{9001: {}},
	}}

	now := start.Add(defaultFlowGraceTTL + 20*time.Second)
	next, ready, expiredOwnership := service.snapshotsForTick(now, observed)
	if len(expiredOwnership) != 0 {
		t.Fatalf("real conntrack IDs revived on the same tick must retain ownership: %v", expiredOwnership)
	}
	prev, exists := next[string(identity)]
	if !ready || !exists {
		t.Fatalf("expected stale snapshot for an observed identity to remain available")
	}
	lineageContinues := observedLineageContinues(prev, observed[0].Flow.CTID, observed[0].Flow.SyntheticCTID, observed[0].RawIDs)
	snapshot, delta, _, countFlow, resetOwners := service.updateSnapshot(
		now,
		observed[0].Flow,
		observed[0].Classified,
		model.ProcessInfo{},
		prev,
		exists,
		ready,
		lineageContinues,
	)
	if countFlow || resetOwners {
		t.Fatalf("continued CTID must not be counted or reassigned as a new flow")
	}
	if delta == nil || delta.upBytes != 80 || delta.downBytes != 200 || delta.upPkts != 1 || delta.downPkts != 2 {
		t.Fatalf("expected only counters since the last successful sample, got %+v", delta)
	}
	if snapshot.StartedAt != start || snapshot.LastOrig != 500 || snapshot.LastReply != 2300 {
		t.Fatalf("unexpected continued snapshot: %+v", snapshot)
	}
}

func TestSnapshotsForTickRevivesStaleSyntheticCTIDWithMonotonicAccounting(t *testing.T) {
	start := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	identity := flowIdentity("local|zone=0|ipv4|tcp|out|53337|198.51.100.8|443|10.0.0.2")
	previous := model.FlowSnapshot{
		CTID:          9001,
		SyntheticCTID: true,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     53337,
		RemoteIP:      "198.51.100.8",
		RemotePort:    443,
		LastOrig:      420,
		LastReply:     2100,
		Counted:       true,
		LastSeen:      start,
	}
	service := &Service{
		snapshots:     map[string]model.FlowSnapshot{string(identity): previous},
		snapshotReady: true,
	}
	observed := []observedFlow{{
		Identity: identity,
		Flow: model.ConntrackFlow{
			CTID:          9001,
			SyntheticCTID: true,
			HasAccounting: true,
			Proto:         "tcp",
			OrigBytes:     500,
			ReplyBytes:    2300,
		},
		RawIDs: map[uint64]struct{}{9001: {}},
	}}

	next, ready, expiredOwnership := service.snapshotsForTick(start.Add(defaultFlowGraceTTL+time.Second), observed)
	if !ready {
		t.Fatal("expected existing collector baseline to remain ready")
	}
	if _, exists := next[string(identity)]; !exists {
		t.Fatal("monotonic accounting must preserve a stale tuple-derived baseline across a collector outage")
	}
	if _, exists := expiredOwnership[string(identity)]; exists {
		t.Fatal("continued monotonic accounting must retain existing flow ownership")
	}
}

func TestSnapshotsForTickDoesNotReviveStaleSyntheticCTIDAfterCounterRollback(t *testing.T) {
	start := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	identity := flowIdentity("local|zone=0|ipv4|udp|out|55628|162.159.192.1|2408|217.69.7.251")
	service := &Service{
		snapshots: map[string]model.FlowSnapshot{string(identity): {
			CTID:          9001,
			SyntheticCTID: true,
			LastOrig:      142_000,
			LastReply:     92_000,
			LastOPkts:     1_140,
			LastRPkts:     920,
			LastSeen:      start,
		}},
		snapshotReady: true,
	}
	observed := []observedFlow{{
		Identity: identity,
		Flow: model.ConntrackFlow{
			CTID:          9001,
			SyntheticCTID: true,
			HasAccounting: true,
			OrigBytes:     200,
			ReplyBytes:    100,
			OrigPkts:      2,
			ReplyPkts:     1,
		},
	}}

	next, _, expiredOwnership := service.snapshotsForTick(start.Add(defaultFlowGraceTTL+time.Second), observed)
	if _, exists := next[string(identity)]; exists {
		t.Fatal("counter rollback must remain a new tuple-derived lineage")
	}
	if _, exists := expiredOwnership[string(identity)]; !exists {
		t.Fatal("counter rollback must release the previous flow ownership")
	}
}

func TestSnapshotsForTickExpiresOwnershipWhileTupleIsAbsent(t *testing.T) {
	start := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	flowID := "local|zone=0|ipv4|tcp|out|53337|198.51.100.8|443|10.0.0.2"
	service := &Service{
		snapshots: map[string]model.FlowSnapshot{
			flowID: {
				CTID:      9001,
				LastSeen:  start,
				LastOrig:  420,
				LastReply: 2100,
				Counted:   true,
			},
		},
		snapshotReady: true,
	}

	next, ready, expiredOwnership := service.snapshotsForTick(start.Add(defaultFlowGraceTTL+time.Second), nil)
	if !ready || len(next) != 0 {
		t.Fatalf("expired absent snapshot must be pruned without losing baseline readiness: ready=%v next=%v", ready, next)
	}
	if _, exists := expiredOwnership[flowID]; !exists {
		t.Fatal("an absent expired snapshot must release and freeze its ownership immediately")
	}
}

func TestSyntheticCTIDCounterRollbackStartsNewLineage(t *testing.T) {
	now := time.Date(2026, 4, 20, 10, 0, 2, 0, time.UTC)
	previous := model.FlowSnapshot{
		CTID:          9001,
		SyntheticCTID: true,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     53337,
		RemoteIP:      "198.51.100.8",
		RemotePort:    443,
		LastOrig:      900,
		LastReply:     1200,
		LastOPkts:     9,
		LastRPkts:     12,
		Counted:       true,
		LastSeen:      now.Add(-time.Second),
	}
	flow := model.ConntrackFlow{
		CTID:          9001,
		SyntheticCTID: true,
		Proto:         "tcp",
		OrigBytes:     100,
		ReplyBytes:    200,
		OrigPkts:      1,
		ReplyPkts:     2,
	}
	classified := classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  53337,
		RemoteIP:   "198.51.100.8",
		RemotePort: 443,
	}
	service := &Service{}
	lineage := observedLineageContinues(previous, flow.CTID, flow.SyntheticCTID, map[uint64]struct{}{flow.CTID: {}})
	_, delta, _, countFlow, resetOwners := service.updateSnapshot(now, flow, classified, model.ProcessInfo{}, previous, true, true, lineage)
	if !countFlow || !resetOwners {
		t.Fatalf("counter rollback on a synthetic id must start a new counted lineage, count=%v reset=%v", countFlow, resetOwners)
	}
	if delta == nil || delta.upBytes != 100 || delta.downBytes != 200 || delta.upPkts != 1 || delta.downPkts != 2 {
		t.Fatalf("new synthetic lineage must count its full initial counters, got %+v", delta)
	}
}

func TestTupleReuseKeepsPreviousContributionInOriginalBucket(t *testing.T) {
	service := &Service{
		currentMinute: time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC),
		buckets:       make(map[model.UsageKey]*bucketState),
	}
	now := service.currentMinute
	flowID := "local|ipv4|tcp|out|47920|198.51.100.44|443|10.0.0.2"
	classified := classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  47920,
		RemoteIP:   "198.51.100.44",
		RemotePort: 443,
		Connected:  true,
	}
	oldProcess := model.ProcessInfo{PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server"}
	newProcess := model.ProcessInfo{PID: 2099, Comm: "curl", Exe: "/usr/bin/curl"}

	firstSnapshot, firstDelta, _, firstCount, resetOwners := service.updateSnapshot(
		now,
		model.ConntrackFlow{
			CTID:       1001,
			Proto:      "tcp",
			OrigBytes:  300,
			ReplyBytes: 900,
			OrigPkts:   3,
			ReplyPkts:  7,
		},
		classified,
		oldProcess,
		model.FlowSnapshot{},
		false,
		true,
		false,
	)
	if resetOwners {
		t.Fatalf("did not expect initial observation to reset ownership")
	}
	if firstDelta == nil || !firstCount {
		t.Fatalf("expected first tuple observation to count immediately, got delta=%+v count=%v", firstDelta, firstCount)
	}
	service.addUsage(flowID, firstSnapshot, *firstDelta, firstCount)

	secondSnapshot, secondDelta, _, secondCount, resetOwners := service.updateSnapshot(
		now.Add(4*time.Second),
		model.ConntrackFlow{
			CTID:       2001,
			Proto:      "tcp",
			OrigBytes:  40,
			ReplyBytes: 60,
			OrigPkts:   1,
			ReplyPkts:  1,
		},
		classified,
		newProcess,
		firstSnapshot,
		true,
		true,
		false,
	)
	if !resetOwners {
		t.Fatalf("expected tuple reuse with a new conntrack lineage to reset flow ownership")
	}
	if secondDelta == nil || !secondCount {
		t.Fatalf("expected reused tuple to start a fresh counted flow, got delta=%+v count=%v", secondDelta, secondCount)
	}
	service.releaseFlowOwnership(flowID)
	service.addUsage(flowID, secondSnapshot, *secondDelta, secondCount)

	oldKey := model.UsageKey{
		MinuteTS:    now.Unix(),
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         oldProcess.PID,
		Comm:        oldProcess.Comm,
		Exe:         oldProcess.Exe,
		LocalPort:   classified.LocalPort,
		RemoteIP:    classified.RemoteIP,
		RemotePort:  classified.RemotePort,
		Attribution: model.AttributionExact,
	}
	newKey := model.UsageKey{
		MinuteTS:    now.Unix(),
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         newProcess.PID,
		Comm:        newProcess.Comm,
		Exe:         newProcess.Exe,
		LocalPort:   classified.LocalPort,
		RemoteIP:    classified.RemoteIP,
		RemotePort:  classified.RemotePort,
		Attribution: model.AttributionExact,
	}

	if state := service.buckets[oldKey]; state == nil || state.bytesUp != 300 || state.bytesDown != 900 || state.flowCount != 1 {
		t.Fatalf("expected previous bucket contribution to stay put, got %+v", state)
	}
	if state := service.buckets[newKey]; state == nil || state.bytesUp != 40 || state.bytesDown != 60 || state.flowCount != 1 {
		t.Fatalf("expected new tuple lineage to create its own bucket contribution, got %+v", state)
	}
	if owner, ok := service.usageFlowOwner[flowID]; !ok || owner != newKey {
		t.Fatalf("expected flow owner to move only for the new lineage, got %v %v", ok, owner)
	}
}

func TestExpiredOwnershipFreezesPreviousGeneration(t *testing.T) {
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	service := &Service{
		currentMinute: now,
		buckets:       make(map[model.UsageKey]*bucketState),
		snapshots:     make(map[string]model.FlowSnapshot),
		snapshotReady: true,
	}
	flowID := "local|zone=0|ipv4|tcp|out|47920|198.51.100.44|443|10.0.0.2"
	oldSnapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         1088,
		Comm:        "worker",
		Exe:         "/usr/bin/worker",
		LocalPort:   47920,
		RemoteIP:    "198.51.100.44",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}

	service.addUsage(flowID, oldSnapshot, deltaPair{upBytes: 300, downBytes: 900}, true)
	oldSnapshot.LastSeen = now
	service.snapshots[flowID] = oldSnapshot
	_, _, expiredOwnership := service.snapshotsForTick(now.Add(defaultFlowGraceTTL+time.Second), nil)
	if _, exists := expiredOwnership[flowID]; !exists {
		t.Fatal("absent old generation must expire its ownership")
	}
	for expiredFlowID := range expiredOwnership {
		service.releaseFlowOwnership(expiredFlowID)
	}
	// The next connection initially has the same attribution key and tuple.
	service.addUsage(flowID, oldSnapshot, deltaPair{upBytes: 40, downBytes: 60}, true)

	newSnapshot := oldSnapshot
	newSnapshot.PID = 2099
	newSnapshot.Comm = "curl"
	newSnapshot.Exe = "/usr/bin/curl"
	service.addUsage(flowID, newSnapshot, deltaPair{upBytes: 10, downBytes: 20}, false)

	oldKey := model.UsageKey{
		MinuteTS:    now.Unix(),
		Proto:       oldSnapshot.Proto,
		Direction:   oldSnapshot.Direction,
		PID:         oldSnapshot.PID,
		Comm:        oldSnapshot.Comm,
		Exe:         oldSnapshot.Exe,
		LocalPort:   oldSnapshot.LocalPort,
		RemoteIP:    oldSnapshot.RemoteIP,
		RemotePort:  oldSnapshot.RemotePort,
		Attribution: oldSnapshot.Attribution,
	}
	newKey := oldKey
	newKey.PID = newSnapshot.PID
	newKey.Comm = newSnapshot.Comm
	newKey.Exe = newSnapshot.Exe

	if state := service.buckets[oldKey]; state == nil || state.bytesUp != 300 || state.bytesDown != 900 || state.flowCount != 1 || len(state.perFlow) != 1 {
		t.Fatalf("completed generation must remain frozen in its original bucket, got %+v", state)
	}
	if state := service.buckets[newKey]; state == nil || state.bytesUp != 50 || state.bytesDown != 80 || state.flowCount != 1 || len(state.perFlow) != 1 {
		t.Fatalf("only the active reused generation may be reclassified, got %+v", state)
	}
}

func TestTupleReuseWithHigherCountersStartsNewLineage(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	classified := classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  47920,
		RemoteIP:   "198.51.100.44",
		RemotePort: 443,
		Connected:  true,
	}
	process := model.ProcessInfo{PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server"}

	prev := model.FlowSnapshot{
		CTID:          1001,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     47920,
		RemoteIP:      "198.51.100.44",
		RemotePort:    443,
		PID:           process.PID,
		Comm:          process.Comm,
		Exe:           process.Exe,
		Attribution:   model.AttributionExact,
		StartedAt:     now,
		BaselineOrig:  100,
		BaselineReply: 200,
		LastOrig:      100,
		LastReply:     200,
		BaselineOPkts: 1,
		BaselineRPkts: 2,
		LastOPkts:     1,
		LastRPkts:     2,
		Counted:       true,
		LastSeen:      now,
	}

	snapshot, delta, _, countFlow, resetOwners := service.updateSnapshot(
		now.Add(2*time.Second),
		model.ConntrackFlow{
			CTID:       2001,
			Proto:      "tcp",
			OrigBytes:  300,
			ReplyBytes: 500,
			OrigPkts:   3,
			ReplyPkts:  5,
		},
		classified,
		process,
		prev,
		true,
		true,
		false,
	)
	if !resetOwners {
		t.Fatalf("expected changed conntrack lineage to reset ownership")
	}
	if !countFlow {
		t.Fatalf("expected changed conntrack lineage to count a new flow")
	}
	if delta == nil || delta.upBytes != 300 || delta.downBytes != 500 || delta.upPkts != 3 || delta.downPkts != 5 {
		t.Fatalf("expected new lineage to start from current counters, got delta=%+v", delta)
	}
	if snapshot.CTID != 2001 || !snapshot.Counted {
		t.Fatalf("unexpected new lineage snapshot: %+v", snapshot)
	}
}

func TestTupleReuseWithOverlappingCTIDsStartsNewLineage(t *testing.T) {
	service := &Service{}
	now := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	process := model.ProcessInfo{PID: 1088, Comm: "ss-server", Exe: "/usr/bin/ss-server"}
	prev := model.FlowSnapshot{
		CTID:          1001,
		Proto:         "tcp",
		Direction:     model.DirectionOut,
		LocalIP:       "10.0.0.2",
		LocalPort:     47920,
		RemoteIP:      "198.51.100.44",
		RemotePort:    443,
		PID:           process.PID,
		Comm:          process.Comm,
		Exe:           process.Exe,
		Attribution:   model.AttributionExact,
		StartedAt:     now,
		BaselineOrig:  100,
		BaselineReply: 200,
		LastOrig:      100,
		LastReply:     200,
		BaselineOPkts: 1,
		BaselineRPkts: 2,
		LastOPkts:     1,
		LastRPkts:     2,
		Counted:       true,
		LastSeen:      now,
	}

	observed := normalizeObservedFlows([]model.ConntrackFlow{
		{
			CTID:          1001,
			Family:        "ipv4",
			Proto:         "tcp",
			OrigSrcIP:     "10.0.0.2",
			OrigDstIP:     "198.51.100.44",
			OrigSrcPort:   47920,
			OrigDstPort:   443,
			ReplySrcIP:    "198.51.100.44",
			ReplyDstIP:    "10.0.0.2",
			ReplySrcPort:  443,
			ReplyDstPort:  47920,
			OrigBytes:     100,
			ReplyBytes:    200,
			OrigPkts:      1,
			ReplyPkts:     2,
			HasAccounting: true,
		},
		{
			CTID:          2001,
			Family:        "ipv4",
			Proto:         "tcp",
			OrigSrcIP:     "10.0.0.2",
			OrigDstIP:     "198.51.100.44",
			OrigSrcPort:   47920,
			OrigDstPort:   443,
			ReplySrcIP:    "198.51.100.44",
			ReplyDstIP:    "10.0.0.2",
			ReplySrcPort:  443,
			ReplyDstPort:  47920,
			OrigBytes:     300,
			ReplyBytes:    500,
			OrigPkts:      3,
			ReplyPkts:     5,
			HasAccounting: true,
		},
	}, map[string]struct{}{"10.0.0.2": {}}, socketIndex{})
	if len(observed) != 1 {
		t.Fatalf("expected overlapping tuple to normalize to one candidate, got %d", len(observed))
	}
	if observed[0].Flow.CTID != 2001 {
		t.Fatalf("expected strongest candidate CTID to be selected, got %+v", observed[0].Flow)
	}

	lineageContinues := observedLineageContinues(prev, observed[0].Flow.CTID, observed[0].Flow.SyntheticCTID, observed[0].RawIDs)
	if lineageContinues {
		t.Fatalf("expected selected new CTID to break lineage even when previous CTID is still in RawIDs")
	}
	snapshot, delta, _, countFlow, resetOwners := service.updateSnapshot(
		now.Add(2*time.Second),
		observed[0].Flow,
		observed[0].Classified,
		process,
		prev,
		true,
		true,
		lineageContinues,
	)
	if !resetOwners || !countFlow {
		t.Fatalf("expected overlapping new CTID to reset owners and count a new flow, reset=%v count=%v", resetOwners, countFlow)
	}
	if delta == nil || delta.upBytes != 300 || delta.downBytes != 500 || delta.upPkts != 3 || delta.downPkts != 5 {
		t.Fatalf("expected new lineage to count current counters, got %+v", delta)
	}
	if snapshot.CTID != 2001 {
		t.Fatalf("expected snapshot to move to selected CTID, got %+v", snapshot)
	}
}

func TestProcessResolverThrottlesRepeatedUnknownInodeScans(t *testing.T) {
	base := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	current := base
	scanCalls := 0

	resolver := &processResolver{
		now:           func() time.Time { return current },
		positiveTTL:   30 * time.Second,
		negativeTTL:   10 * time.Second,
		scanCooldown:  5 * time.Second,
		cache:         make(map[uint64]model.ProcessInfo),
		negativeCache: make(map[uint64]time.Time),
		scan: func(context.Context) (map[uint64]model.ProcessInfo, bool) {
			scanCalls++
			return map[uint64]model.ProcessInfo{}, true
		},
	}

	resolver.Resolve(context.Background(), map[uint64]struct{}{1: {}})
	current = base.Add(2 * time.Second)
	resolver.Resolve(context.Background(), map[uint64]struct{}{2: {}})
	current = base.Add(6 * time.Second)
	resolver.Resolve(context.Background(), map[uint64]struct{}{3: {}})

	if scanCalls != 2 {
		t.Fatalf("expected unknown-inode scans to be throttled, got %d full scans", scanCalls)
	}
}

func TestProcessHintCacheLookupAndExpiry(t *testing.T) {
	service := &Service{processHints: make(map[string]processHint)}
	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	service.rememberProcessHint(model.FlowSnapshot{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  8443,
		RemoteIP:   "198.51.100.20",
		RemotePort: 443,
		PID:        3312,
		Comm:       "nginx",
		Exe:        "/usr/sbin/nginx",
	}, now)

	resolved, ok := service.lookupProcessHint(classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  8443,
		RemoteIP:   "198.51.100.20",
		RemotePort: 443,
	}, now.Add(30*time.Second))
	if !ok || resolved.PID != 3312 {
		t.Fatalf("expected active process hint, got %+v, ok=%v", resolved, ok)
	}

	if _, ok := service.lookupProcessHint(classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  8443,
		RemoteIP:   "203.0.113.9",
		RemotePort: 443,
	}, now.Add(30*time.Second)); ok {
		t.Fatalf("expected strict hint key to reject different remote")
	}

	_, expired := service.lookupProcessHint(classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionOut,
		LocalIP:    "10.0.0.2",
		LocalPort:  8443,
		RemoteIP:   "198.51.100.20",
		RemotePort: 443,
	}, now.Add(2*time.Minute))
	if expired {
		t.Fatalf("expected hint to expire")
	}
}

func TestReadSocketIndexReadsPerPIDNetRoots(t *testing.T) {
	procFS := t.TempDir()
	writeSocketFile := func(path string, lines []string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir socket root: %v", err)
		}
		content := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n" + strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write socket file: %v", err)
		}
	}

	writeSocketFile(filepath.Join(procFS, "101", "net", "tcp"), []string{
		"   0: 0100007F:1F90 0200007F:C350 01 00000000:00000000 00:00000000 00000000 0 0 111 1 0000000000000000",
	})
	writeSocketFile(filepath.Join(procFS, "202", "net", "tcp"), []string{
		"   0: 0300007F:208D 0400007F:C351 01 00000000:00000000 00:00000000 00000000 0 0 222 1 0000000000000000",
	})

	index, err := ReadSocketIndex(procFS)
	if err != nil {
		t.Fatalf("read socket index: %v", err)
	}

	firstKey := tuple{Proto: "tcp", LocalIP: "127.0.0.1", LocalPort: 8080, RemoteIP: "127.0.0.2", RemotePort: 50000}.key()
	secondKey := tuple{Proto: "tcp", LocalIP: "127.0.0.3", LocalPort: 8333, RemoteIP: "127.0.0.4", RemotePort: 50001}.key()
	if !index.ByTuple[firstKey].Present || index.ByTuple[firstKey].Inode != 111 {
		t.Fatalf("expected first per-pid tuple, got %+v", index.ByTuple[firstKey])
	}
	if !index.ByTuple[secondKey].Present || index.ByTuple[secondKey].Inode != 222 {
		t.Fatalf("expected second per-pid tuple, got %+v", index.ByTuple[secondKey])
	}
}

func TestAddUsageMovesFlowOwnershipAcrossUsageKeys(t *testing.T) {
	minute := time.Date(2026, 4, 17, 12, 34, 0, 0, time.UTC)
	service := &Service{
		currentMinute:  minute,
		buckets:        make(map[model.UsageKey]*bucketState),
		forwardBuckets: make(map[model.ForwardUsageKey]*bucketState),
	}

	unknownSnapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         0,
		Comm:        "",
		Exe:         "",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionUnknown,
	}
	exactSnapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}

	service.addUsage("flow-9001", unknownSnapshot, deltaPair{upBytes: 100}, true)
	service.addUsage("flow-9001", exactSnapshot, deltaPair{upBytes: 200}, false)

	unknownKey := model.UsageKey{
		MinuteTS:    minute.Unix(),
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         0,
		Comm:        "",
		Exe:         "",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionUnknown,
	}
	exactKey := model.UsageKey{
		MinuteTS:    minute.Unix(),
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}

	if state := service.buckets[unknownKey]; state != nil {
		t.Fatalf("expected unknown bucket to be drained after key change, got %+v", state)
	}
	if state := service.buckets[exactKey]; state == nil || len(state.perFlow) != 1 {
		t.Fatalf("expected exact bucket to own the flow, got %+v", state)
	} else if state.bytesUp != 300 {
		t.Fatalf("expected migrated bytes to move with the flow, got %+v", state)
	} else if state.flowCount != 1 {
		t.Fatalf("expected flow count contribution to migrate with the flow, got %+v", state)
	}
}

func TestAddForwardUsageMovesFlowOwnershipFromUsageBucket(t *testing.T) {
	minute := time.Date(2026, 4, 17, 12, 34, 0, 0, time.UTC)
	service := &Service{
		currentMinute:  minute,
		buckets:        make(map[model.UsageKey]*bucketState),
		forwardBuckets: make(map[model.ForwardUsageKey]*bucketState),
	}

	usageSnapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}
	service.addUsage("flow-7001", usageSnapshot, deltaPair{upBytes: 100}, true)

	forwardClassified := classifiedFlow{
		Proto:       "tcp",
		OrigSrcIP:   "10.0.0.2",
		OrigDstIP:   "203.0.113.8",
		OrigSrcPort: 51000,
		OrigDstPort: 443,
	}
	service.addForwardUsage("flow-7001", forwardClassified, deltaPair{upBytes: 200}, false)

	usageKey := model.UsageKey{
		MinuteTS:    minute.Unix(),
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}
	forwardKey := model.ForwardUsageKey{
		MinuteTS:  minute.Unix(),
		Proto:     "tcp",
		OrigSrcIP: "10.0.0.2",
		OrigDstIP: "203.0.113.8",
		OrigSPort: 51000,
		OrigDPort: 443,
	}

	if state := service.buckets[usageKey]; state != nil {
		t.Fatalf("expected usage bucket contribution to be transferred, got %+v", state)
	}
	if state := service.forwardBuckets[forwardKey]; state == nil || len(state.perFlow) != 1 {
		t.Fatalf("expected forward bucket to own the flow, got %+v", state)
	} else if state.bytesUp != 300 {
		t.Fatalf("expected forward bucket to retain migrated bytes, got %+v", state)
	} else if state.flowCount != 1 {
		t.Fatalf("expected forward bucket to retain migrated flow count, got %+v", state)
	}
}

func TestFlushCurrentBucketsCountsFlowOnlyOnFirstObservedMinute(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	trafficStore, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = trafficStore.Close()
	})

	minute := time.Date(2026, 4, 17, 12, 34, 0, 0, time.UTC)
	service := &Service{
		store:          trafficStore,
		currentMinute:  minute,
		buckets:        make(map[model.UsageKey]*bucketState),
		forwardBuckets: make(map[model.ForwardUsageKey]*bucketState),
		usageFlowOwner: make(map[string]model.UsageKey),
		fwdFlowOwner:   make(map[string]model.ForwardUsageKey),
	}

	snapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}

	service.addUsage("flow-7001", snapshot, deltaPair{upBytes: 100}, true)
	if err := service.flushCurrentBuckets(ctx); err != nil {
		t.Fatalf("flush first minute: %v", err)
	}

	service.currentMinute = minute.Add(time.Minute)
	service.addUsage("flow-7001", snapshot, deltaPair{upBytes: 50}, false)
	if err := service.flushCurrentBuckets(ctx); err != nil {
		t.Fatalf("flush second minute: %v", err)
	}

	rows, _, _, err := trafficStore.QueryUsage(ctx, model.UsageQuery{
		Start:     minute.Add(-time.Minute),
		End:       minute.Add(2 * time.Minute),
		UsePage:   true,
		Page:      1,
		PageSize:  10,
		SortBy:    "minute_ts",
		SortOrder: "asc",
	}, store.DataSourceMinute)
	if err != nil {
		t.Fatalf("query usage rows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 minute rows, got %d", len(rows))
	}
	if rows[0].FlowCount != 1 || rows[1].FlowCount != 0 {
		t.Fatalf("expected flow to count only once across minutes, got %+v", rows)
	}
}

func TestFlushCurrentBucketsSkipsZeroContributionRows(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "traffic.db")

	trafficStore, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = trafficStore.Close()
	})

	minute := time.Date(2026, 4, 17, 12, 34, 0, 0, time.UTC)
	service := &Service{
		store:          trafficStore,
		currentMinute:  minute,
		buckets:        make(map[model.UsageKey]*bucketState),
		forwardBuckets: make(map[model.ForwardUsageKey]*bucketState),
		usageFlowOwner: make(map[string]model.UsageKey),
		fwdFlowOwner:   make(map[string]model.ForwardUsageKey),
	}

	usageSnapshot := model.FlowSnapshot{
		Proto:       "tcp",
		Direction:   model.DirectionOut,
		PID:         2255,
		Comm:        "ss-server",
		Exe:         "/usr/local/bin/ss-server",
		LocalPort:   8388,
		RemoteIP:    "203.0.113.8",
		RemotePort:  443,
		Attribution: model.AttributionExact,
	}
	service.addUsage("flow-7001", usageSnapshot, deltaPair{}, false)

	forwardClassified := classifiedFlow{
		Proto:       "tcp",
		OrigSrcIP:   "10.0.0.2",
		OrigDstIP:   "203.0.113.8",
		OrigSrcPort: 51000,
		OrigDstPort: 443,
	}
	service.addForwardUsage("flow-7002", forwardClassified, deltaPair{}, false)

	if err := service.flushCurrentBuckets(ctx); err != nil {
		t.Fatalf("flush zero contribution buckets: %v", err)
	}

	usageRows, _, usageTotal, err := trafficStore.QueryUsage(ctx, model.UsageQuery{
		Start:    minute.Add(-time.Minute),
		End:      minute.Add(time.Minute),
		UsePage:  true,
		Page:     1,
		PageSize: 10,
	}, store.DataSourceMinute)
	if err != nil {
		t.Fatalf("query usage rows: %v", err)
	}
	if usageTotal != 0 || len(usageRows) != 0 {
		t.Fatalf("expected no usage rows for zero contribution buckets, found total=%d rows=%d", usageTotal, len(usageRows))
	}

	forwardRows, _, forwardTotal, err := trafficStore.QueryForwardUsage(ctx, model.ForwardQuery{
		Start:    minute.Add(-time.Minute),
		End:      minute.Add(time.Minute),
		UsePage:  true,
		Page:     1,
		PageSize: 10,
	}, store.DataSourceMinuteForward)
	if err != nil {
		t.Fatalf("query forward rows: %v", err)
	}
	if forwardTotal != 0 || len(forwardRows) != 0 {
		t.Fatalf("expected no forward rows for zero contribution buckets, found total=%d rows=%d", forwardTotal, len(forwardRows))
	}
}

func TestAttachSocketMetadataFallsBackWhenTupleEntryHasNoInode(t *testing.T) {
	classified := classifiedFlow{
		Proto:      "tcp",
		Direction:  model.DirectionIn,
		LocalIP:    "217.69.7.251",
		LocalPort:  12096,
		RemoteIP:   "159.226.171.226",
		RemotePort: 44598,
		Tuple: tuple{
			Proto:      "tcp",
			LocalIP:    "217.69.7.251",
			LocalPort:  12096,
			RemoteIP:   "159.226.171.226",
			RemotePort: 44598,
		},
	}

	index := socketIndex{
		ByTuple: map[string]socketEntry{
			classified.Tuple.key(): {Inode: 0, Connected: true, Present: true},
		},
		ByLocal: map[string]socketEntry{
			localTupleKey("tcp", "0.0.0.0", 12096): {Inode: 88001, Connected: false, Present: true},
		},
	}

	resolved, ok := attachSocketMetadata(index, classified)
	if !ok {
		t.Fatalf("expected fallback socket resolution")
	}
	if resolved.Inode != 88001 || !resolved.MatchedByLocal {
		t.Fatalf("expected local fallback inode, got %+v", resolved)
	}
}

func TestDiagnosticsRequiresRecentSuccessfulCollection(t *testing.T) {
	service := &Service{cfg: config.Default(), snapshots: make(map[string]model.FlowSnapshot)}
	recent := time.Now().UTC().Add(-5 * time.Second)
	service.snapshotReady = true
	service.recordTickOutcome(recent, nil)
	diagnostics := service.Diagnostics()
	if !diagnostics.CollectionFresh || diagnostics.LastSuccessfulTickTS == nil || diagnostics.LastSuccessfulTickAgeSecs == nil {
		t.Fatalf("expected recent successful collection to be fresh: %+v", diagnostics)
	}

	service.recordTickOutcome(recent.Add(time.Second), errors.New("conntrack unavailable"))
	diagnostics = service.Diagnostics()
	if !diagnostics.CollectionFresh || diagnostics.ConsecutiveTickFailures != 1 {
		t.Fatalf("expected one transient failure to retain a fresh last success: %+v", diagnostics)
	}

	service.recordTickOutcome(time.Now().UTC().Add(-time.Minute), nil)
	diagnostics = service.Diagnostics()
	if diagnostics.CollectionFresh {
		t.Fatalf("expected an old successful collection to be stale: %+v", diagnostics)
	}
}

func TestCollectionFreshnessUsesConfiguredLongTickInterval(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	if !collectionIsFresh(now, now.Add(-3*time.Hour), 4*time.Hour) {
		t.Fatal("a valid long collection interval must not be capped to one hour for readiness")
	}
	if collectionIsFresh(now, now.Add(-13*time.Hour), 4*time.Hour) {
		t.Fatal("collection must become stale after three configured intervals")
	}
}

func BenchmarkParseConntrackObservation(b *testing.B) {
	line := "ipv4 2 tcp 6 431999 ESTABLISHED src=10.0.0.2 dst=1.1.1.1 sport=51544 dport=443 packets=10 bytes=1024 src=1.1.1.1 dst=10.0.0.2 sport=443 dport=51544 packets=8 bytes=4096 mark=0 zone=17 use=1 id=12345"
	b.ReportAllocs()
	for b.Loop() {
		observation, ok, err := parseConntrackObservation(line)
		if err != nil || !ok || observation.Flow.CTID != 12345 {
			b.Fatalf("unexpected parse result: ok=%t err=%v observation=%+v", ok, err, observation)
		}
	}
}
