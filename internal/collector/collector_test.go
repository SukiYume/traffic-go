package collector

import (
	"context"
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

	snapshot, _, _, _ := service.updateSnapshot(
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

	snapshot, _, _, _ := service.updateSnapshot(
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
	)

	if snapshot.Attribution != model.AttributionHeuristic {
		t.Fatalf("expected heuristic attribution, got %s", snapshot.Attribution)
	}
	if snapshot.PID != 27896 || snapshot.Comm != "ss-server" {
		t.Fatalf("expected ss-server process mapping, got %+v", snapshot)
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

	snapshot, delta, _, _ := service.updateSnapshot(
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

	snapshot, delta, _, countFlow := service.updateSnapshot(
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

	service.addUsage(9001, unknownSnapshot, deltaPair{upBytes: 100}, true)
	service.addUsage(9001, exactSnapshot, deltaPair{upBytes: 200}, false)

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
	if state := service.buckets[exactKey]; state == nil || len(state.flows) != 1 {
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
	service.addUsage(7001, usageSnapshot, deltaPair{upBytes: 100}, true)

	forwardClassified := classifiedFlow{
		Proto:       "tcp",
		OrigSrcIP:   "10.0.0.2",
		OrigDstIP:   "203.0.113.8",
		OrigSrcPort: 51000,
		OrigDstPort: 443,
	}
	service.addForwardUsage(7001, forwardClassified, deltaPair{upBytes: 200}, false)

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
	if state := service.forwardBuckets[forwardKey]; state == nil || len(state.flows) != 1 {
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
		usageFlowOwner: make(map[uint64]model.UsageKey),
		fwdFlowOwner:   make(map[uint64]model.ForwardUsageKey),
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

	service.addUsage(7001, snapshot, deltaPair{upBytes: 100}, true)
	if err := service.flushCurrentBuckets(ctx); err != nil {
		t.Fatalf("flush first minute: %v", err)
	}

	service.currentMinute = minute.Add(time.Minute)
	service.addUsage(7001, snapshot, deltaPair{upBytes: 50}, false)
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
		usageFlowOwner: make(map[uint64]model.UsageKey),
		fwdFlowOwner:   make(map[uint64]model.ForwardUsageKey),
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
	service.addUsage(7001, usageSnapshot, deltaPair{}, false)

	forwardClassified := classifiedFlow{
		Proto:       "tcp",
		OrigSrcIP:   "10.0.0.2",
		OrigDstIP:   "203.0.113.8",
		OrigSrcPort: 51000,
		OrigDstPort: 443,
	}
	service.addForwardUsage(7002, forwardClassified, deltaPair{}, false)

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
