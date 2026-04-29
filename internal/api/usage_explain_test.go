package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
)

func TestNormalizeLogMessageForDisplayBinaryEscapes(t *testing.T) {
	input := `\x16\x03\x01\x00\xF3\x01\x00\x00\xEF\x03\x03\xD7+\xA9\xF3\xB8f|(\x01\xFB\x12\xFDmT\xB2\x16\xC5\xE9\x0F\xFC\xFD\x97{Z\x1A\xAB a\xD2f\xA2\x1C`
	got := normalizeLogMessageForDisplay(input)

	if !strings.HasPrefix(got, "[binary ") {
		t.Fatalf("expected binary summary, got: %s", got)
	}
	if !strings.Contains(got, "16 03 01 00 F3") {
		t.Fatalf("expected hex bytes in summary, got: %s", got)
	}
}

func TestNormalizeLogMessageForDisplayTextEscapes(t *testing.T) {
	input := `relay client=74.7.227.153 target=104.26.8.78:443 path=\x2Fcloud\x2F line1\nline2`
	got := normalizeLogMessageForDisplay(input)

	if strings.HasPrefix(got, "[binary ") {
		t.Fatalf("did not expect binary summary for text payload: %s", got)
	}
	if !strings.Contains(got, `path=/cloud/`) {
		t.Fatalf("expected decoded path, got: %s", got)
	}
	if !strings.Contains(got, `line1\nline2`) {
		t.Fatalf("expected escaped newline for display, got: %s", got)
	}
}

func TestParseNginxEvidenceLineUsesForwardedIPWhenRemoteAddrIsLoopback(t *testing.T) {
	line := `127.0.0.1 - - [29/Apr/2026:22:02:00 +0800] "GET /traffic/ HTTP/2.0" 200 1024 "-" "Mozilla/5.0" xff="203.0.113.24, 127.0.0.1" realip="127.0.0.1"`
	evidence, ok := parseNginxEvidenceLine(evidenceSourceNginx, line, time.Time{})
	if !ok {
		t.Fatalf("expected nginx parse success")
	}
	if evidence.ClientIP != "203.0.113.24" {
		t.Fatalf("expected forwarded client IP, got %+v", evidence)
	}
	if evidence.Path != "/traffic/" || evidence.Method != "GET" {
		t.Fatalf("unexpected nginx request evidence: %+v", evidence)
	}
}

func TestParseGenericEvidenceLineFRPSUserConnection(t *testing.T) {
	line := `2026-04-29 22:02:10.161 [I] [proxy/proxy.go:204] [5d8d225ac885e00d] [pc_ssh] get a user connection [213.209.159.235:47700]`
	reference := time.Date(2026, 4, 29, 22, 3, 0, 0, time.Local)
	evidence, ok := parseGenericEvidenceLine(customEvidenceSource("frps"), line, reference)
	if !ok {
		t.Fatalf("expected frps parse success")
	}
	if evidence.ClientIP != "213.209.159.235" {
		t.Fatalf("expected frps user connection source IP, got %+v", evidence)
	}
	if evidence.Host != "pc_ssh" || evidence.Method != "frps-user-connection" {
		t.Fatalf("expected frps proxy name evidence, got %+v", evidence)
	}
	if evidence.TargetIP != "" || evidence.TargetPort != 0 {
		t.Fatalf("did not expect frps user connection line to invent target endpoint, got %+v", evidence)
	}
}

func TestListLogFilesFiltersByDateRange(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "ss-server.log-20260416.gz")
	outside := filepath.Join(dir, "ss-server.log-20260301.gz")
	if err := os.WriteFile(inside, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write inside file: %v", err)
	}
	if err := os.WriteFile(outside, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	start := time.Date(2026, 4, 16, 1, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 4, 16, 1, 20, 0, 0, time.UTC).Unix()
	files, err := evidence.ListLogFiles(dir, isShadowsocksLogFileName, start, end, 10)
	if err != nil {
		t.Fatalf("list log files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(files))
	}
	if !strings.Contains(files[0].Path, "20260416") {
		t.Fatalf("expected in-range file selected, got %s", files[0].Path)
	}
}

func TestSummarizeNginxRequestsIncludesBotAndCount(t *testing.T) {
	status200 := 200
	status404 := 404
	rows := []model.LogEvidence{
		{
			EventTS: 1713232801,
			Method:  "GET",
			Host:    "paris.escape.ac.cn",
			Path:    "/apod/2023/12/AstroPH-2023-12",
			Status:  &status200,
			Message: `127.0.0.1 - - [16/Apr/2026:01:08:12 +0800] "GET /apod/2023/12/AstroPH-2023-12 HTTP/2.0" 200 54614 "https://paris.escape.ac.cn/sitemap.xml" "Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
		},
		{
			EventTS: 1713232803,
			Method:  "GET",
			Host:    "paris.escape.ac.cn",
			Path:    "/apod/2023/12/AstroPH-2023-12",
			Status:  &status200,
			Message: `127.0.0.1 - - [16/Apr/2026:01:08:15 +0800] "GET /apod/2023/12/AstroPH-2023-12 HTTP/2.0" 200 54614 "https://paris.escape.ac.cn/sitemap.xml" "Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
		},
		{
			EventTS: 1713232806,
			Method:  "GET",
			Host:    "paris.escape.ac.cn",
			Path:    "/en/apod/2025/12/AstroPH-2025-12",
			Status:  &status404,
			Message: `127.0.0.1 - - [16/Apr/2026:01:08:17 +0800] "GET /en/apod/2025/12/AstroPH-2025-12 HTTP/2.0" 404 1039 "https://paris.escape.ac.cn/apod/2025/12/AstroPH-2025-12" "Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)" "-"`,
		},
	}

	requests := summarizeNginxRequests(rows, 8)
	if len(requests) != 2 {
		t.Fatalf("expected 2 aggregated requests, got %d", len(requests))
	}
	if requests[0].Count != 2 {
		t.Fatalf("expected top request count 2, got %+v", requests[0])
	}
	if requests[0].Bot != "GPTBot" {
		t.Fatalf("expected GPTBot detection, got %+v", requests[0])
	}

	notes := []string{}
	appendNginxStatusAndAgentNotes(&notes, rows, requests)
	notesText := strings.Join(notes, "\n")
	if !strings.Contains(notesText, "访问端识别") || !strings.Contains(notesText, "GPTBot") {
		t.Fatalf("expected bot summary note, got %s", notesText)
	}
}

func TestSummarizeNginxRequestsNormalizesEscapedBinaryFields(t *testing.T) {
	status400 := 400
	rows := []model.LogEvidence{
		{
			EventTS: 1713233760,
			Method:  `\x16\x03\x01\x01\x07\x01\x00\x01\x03`,
			Path:    `\x03h\xB7\xB0\x00YyA\x84\xC4\xF4\xC3\xFA\x16\x60\xA2\xD2p\x9A`,
			Status:  &status400,
		},
	}

	requests := summarizeNginxRequests(rows, 8)
	if len(requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(requests))
	}
	if strings.Contains(requests[0].Path, `\x16`) || strings.Contains(requests[0].Method, `\x16`) {
		t.Fatalf("expected escaped binary to be normalized, got %+v", requests[0])
	}
	if requests[0].Method != "BINARY" {
		t.Fatalf("expected BINARY method label, got %+v", requests[0])
	}
	if !strings.HasPrefix(requests[0].Path, "[binary ") {
		t.Fatalf("expected binary summary path, got %+v", requests[0])
	}
}

func TestParseSyslogTimestamp(t *testing.T) {
	now := time.Date(2026, 4, 16, 23, 0, 0, 0, time.Local)
	ts, ok := parseSyslogTimestamp("Apr 16 20:34:02 217 /usr/local/bin/ss-server[27896]: connect to chatgpt.com:443", now)
	if !ok {
		t.Fatalf("expected syslog timestamp parse success")
	}
	parsed := time.Unix(ts, 0).In(now.Location())
	if parsed.Year() != 2026 || parsed.Month() != time.April || parsed.Day() != 16 {
		t.Fatalf("unexpected parsed date: %s", parsed)
	}
	if parsed.Hour() != 20 || parsed.Minute() != 34 || parsed.Second() != 2 {
		t.Fatalf("unexpected parsed clock: %s", parsed)
	}
}

func TestParseFlexibleTimestampSupportsShortISOWithoutColonOffset(t *testing.T) {
	line := "2026-04-21T11:20:06+0800 host /usr/local/bin/ss-server[1019]: [12096] connect to chatgpt.com:443"
	reference := time.Date(2026, 4, 21, 11, 22, 0, 0, time.FixedZone("CST", 8*3600))

	ts, ok := parseFlexibleTimestamp(line, reference)
	if !ok {
		t.Fatalf("expected short-iso timestamp parse success")
	}
	parsed := time.Unix(ts, 0).In(reference.Location())
	if parsed.Year() != 2026 || parsed.Month() != time.April || parsed.Day() != 21 {
		t.Fatalf("unexpected parsed date: %s", parsed)
	}
	if parsed.Hour() != 11 || parsed.Minute() != 20 || parsed.Second() != 6 {
		t.Fatalf("unexpected parsed time: %s", parsed)
	}
}

func TestParseSSEvidenceLineConnectHost(t *testing.T) {
	line := "Apr 16 20:34:11 217 /usr/local/bin/ss-server[27896]: [12096] connect to chatgpt.com:443"
	reference := time.Date(2026, 4, 16, 20, 35, 0, 0, time.Local)
	evidence, ok := parseSSEvidenceLine(evidenceSourceSS, line, reference)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if evidence.Method != "connect" {
		t.Fatalf("expected connect method, got %+v", evidence)
	}
	if evidence.Host != "chatgpt.com" {
		t.Fatalf("expected host chatgpt.com, got %+v", evidence)
	}
	if evidence.Path != "443" {
		t.Fatalf("expected target port 443 in path, got %+v", evidence)
	}
	if evidence.EntryPort != 12096 {
		t.Fatalf("expected entry port 12096, got %+v", evidence)
	}
}

func TestParseSSEvidenceLineUDPCacheMiss(t *testing.T) {
	line := "Apr 16 20:34:12 217 /usr/local/bin/ss-server[27896]: [12096] [udp] cache miss: time.apple.com:123 <-> 223.104.41.114:42683"
	reference := time.Date(2026, 4, 16, 20, 35, 0, 0, time.Local)
	evidence, ok := parseSSEvidenceLine(evidenceSourceSS, line, reference)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if evidence.Method != "udp-cache-miss" {
		t.Fatalf("expected udp-cache-miss method, got %+v", evidence)
	}
	if evidence.Host != "time.apple.com" || evidence.Path != "123" {
		t.Fatalf("expected udp target endpoint, got %+v", evidence)
	}
	if evidence.ClientIP != "223.104.41.114" {
		t.Fatalf("expected client ip from udp pair, got %+v", evidence)
	}
	if evidence.EntryPort != 12096 {
		t.Fatalf("expected entry port 12096, got %+v", evidence)
	}
}

func TestParseSSEvidenceLineUDPCacheHitKeepsClientAndTargetDirection(t *testing.T) {
	line := "Apr 29 21:37:22 217 /usr/local/bin/ss-server[993]: [12096] [udp] cache hit: 216.239.36.223:443 <-> 159.226.171.226:51699"
	reference := time.Date(2026, 4, 29, 21, 38, 0, 0, time.Local)
	evidence, ok := parseSSEvidenceLine(evidenceSourceSS, line, reference)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if evidence.Method != "udp-cache-hit" {
		t.Fatalf("expected udp-cache-hit method, got %+v", evidence)
	}
	if evidence.ClientIP != "159.226.171.226" || evidence.TargetIP != "216.239.36.223" {
		t.Fatalf("expected target/client direction to be preserved, got %+v", evidence)
	}
	if evidence.TargetPort != 443 || evidence.Path != "443" {
		t.Fatalf("expected target port 443, got %+v", evidence)
	}
	if evidence.EntryPort != 12096 {
		t.Fatalf("expected entry port 12096, got %+v", evidence)
	}
}

func TestExtractLogEntryPortPrefersLastBracketedPort(t *testing.T) {
	line := "Apr 16 20:34:11 217 /usr/local/bin/ss-server[27896]: [12096] connect to chatgpt.com:443"
	if got := extractLogEntryPort(line); got != 12096 {
		t.Fatalf("expected entry port 12096, got %d", got)
	}
}

func TestIsShadowsocksLogFileNameSupportsSharedDirectoryNames(t *testing.T) {
	cases := []string{"server.log", "manager.log", "obfs.log", "ss-server.log"}
	for _, name := range cases {
		if !isShadowsocksLogFileName(name) {
			t.Fatalf("expected %s to be treated as shadowsocks log", name)
		}
	}
}

func TestLookupConfiguredProcessLogDirFallsBackToShadowsocksFamily(t *testing.T) {
	server := newTestServer(t)
	setProcessLogDir(server, "obfs-server", "/var/log/shadowsocks")

	key, dir, ok := server.lookupConfiguredProcessLogDir("ss-server", "/usr/bin/ss-server")
	if !ok {
		t.Fatalf("expected shadowsocks family fallback to succeed")
	}
	if key != "obfs-server" || dir != "/var/log/shadowsocks" {
		t.Fatalf("unexpected fallback match: key=%q dir=%q", key, dir)
	}
}

func TestScanEvidenceFilesReturnsScannerError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ss-server.log")
	tooLongLine := strings.Repeat("x", 2*1024*1024)
	if err := os.WriteFile(path, []byte(tooLongLine+"\n"), 0o644); err != nil {
		t.Fatalf("write oversized log line: %v", err)
	}

	_, err := evidence.ScanFiles(
		context.Background(),
		evidenceSourceSS,
		[]evidence.LogFileCandidate{{Path: path}},
		func(source string, line string, reference time.Time) (model.LogEvidence, bool) {
			return model.LogEvidence{}, false
		},
		nil,
		0,
		time.Now().Unix(),
		time.Now().Unix(),
		10,
		0,
	)
	if err == nil {
		t.Fatalf("expected scanner error for oversized line")
	}
	if !strings.Contains(err.Error(), "scan evidence file") {
		t.Fatalf("unexpected scanner error: %v", err)
	}
}

func TestListLogFilesSupportsGlobPathSpec(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "frps.log-20260416")
	outside := filepath.Join(dir, "frps.log-20260301")
	if err := os.WriteFile(inside, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write inside file: %v", err)
	}
	if err := os.WriteFile(outside, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	start := time.Date(2026, 4, 16, 1, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 4, 16, 1, 20, 0, 0, time.UTC).Unix()
	pathSpec := filepath.Join(dir, "frps*")
	files, err := evidence.ListLogFiles(pathSpec, isGenericLogFileName, start, end, 10)
	if err != nil {
		t.Fatalf("list log files with glob: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(files))
	}
	if !strings.Contains(files[0].Path, "20260416") {
		t.Fatalf("expected in-range file selected, got %s", files[0].Path)
	}
}

func TestListLogFilesAppliesNameMatcherToDirectFilePath(t *testing.T) {
	dir := t.TempDir()
	notLog := filepath.Join(dir, "frps.txt")
	if err := os.WriteFile(notLog, []byte("test\n"), 0o644); err != nil {
		t.Fatalf("write non-log file: %v", err)
	}

	start := time.Date(2026, 4, 16, 1, 0, 0, 0, time.UTC).Unix()
	end := time.Date(2026, 4, 16, 1, 20, 0, 0, time.UTC).Unix()
	files, err := evidence.ListLogFiles(notLog, isGenericLogFileName, start, end, 10)
	if err != nil {
		t.Fatalf("list direct non-log file: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected direct file path to be filtered by matcher, got %v", files)
	}
}

func TestMessageHasExactIP(t *testing.T) {
	message := `bridge src=11.1.1.10:5000 dst=198.51.100.1:443`
	if messageHasExactIP(message, "1.1.1.1") {
		t.Fatalf("did not expect substring IP match")
	}
	if !messageHasExactIP(message, "11.1.1.10") {
		t.Fatalf("expected exact token IP match")
	}
}

func TestMergeChainUsageMetricsAnchorsAmbiguousHostOnlyOutboundChainsToQueriedRemote(t *testing.T) {
	remotePort := 443
	query := usageExplainQuery{
		Direction:  model.DirectionOut,
		RemoteIP:   "142.250.72.14",
		RemotePort: &remotePort,
	}
	chains := map[string]chainAgg{
		"alpha": {
			SourceIP:   "203.0.113.24",
			TargetHost: "alpha.example",
			TargetPort: 443,
			LocalPort:  12096,
		},
		"beta": {
			SourceIP:   "203.0.113.24",
			TargetHost: "beta.example",
			TargetPort: 443,
			LocalPort:  12096,
		},
	}
	related := []model.UsageRecord{
		{
			Direction:  model.DirectionIn,
			RemoteIP:   "203.0.113.24",
			LocalPort:  12096,
			BytesUp:    1024,
			BytesDown:  4096,
			FlowCount:  3,
			RemotePort: &remotePort,
		},
		{
			Direction:  model.DirectionOut,
			RemoteIP:   "142.250.72.14",
			LocalPort:  47920,
			BytesUp:    2048,
			BytesDown:  8192,
			FlowCount:  4,
			RemotePort: &remotePort,
		},
	}

	mergeChainUsageMetrics(chains, query, related)

	for key, chain := range chains {
		if chain.TargetIP != "142.250.72.14" {
			t.Fatalf("expected ambiguous host-only chain %s to anchor to the queried target ip, got %+v", key, chain)
		}
		if chain.BytesTotal != 10240 || chain.FlowCount != 4 {
			t.Fatalf("expected ambiguous host-only chain %s to reuse the queried outbound totals, got %+v", key, chain)
		}
	}
}

func TestMergeChainUsageMetricsHydratesUniqueHostOnlyOutboundChain(t *testing.T) {
	remotePort := 443
	query := usageExplainQuery{
		Direction:  model.DirectionOut,
		RemoteIP:   "142.250.72.14",
		RemotePort: &remotePort,
	}
	chains := map[string]chainAgg{
		"alpha": {
			TargetHost: "chatgpt.com",
			TargetPort: 443,
			LocalPort:  12096,
		},
	}
	related := []model.UsageRecord{
		{
			Direction:  model.DirectionIn,
			RemoteIP:   "203.0.113.24",
			LocalPort:  12096,
			BytesUp:    1024,
			BytesDown:  4096,
			FlowCount:  3,
			RemotePort: &remotePort,
		},
		{
			Direction:  model.DirectionOut,
			RemoteIP:   "142.250.72.14",
			LocalPort:  47920,
			BytesUp:    2048,
			BytesDown:  8192,
			FlowCount:  4,
			RemotePort: &remotePort,
		},
	}

	mergeChainUsageMetrics(chains, query, related)

	chain := chains["alpha"]
	if chain.TargetIP != "142.250.72.14" {
		t.Fatalf("expected target ip to be hydrated from outbound usage, got %+v", chain)
	}
	if chain.BytesTotal != 10240 || chain.FlowCount != 4 {
		t.Fatalf("expected outbound usage totals to attach to hydrated chain, got %+v", chain)
	}
}

func TestShouldPersistCanonicalChainSkipsWeakZeroMetricCandidates(t *testing.T) {
	if shouldPersistCanonicalChain(usageExplainChain{
		TargetHost: "chatgpt.com",
		TargetPort: nullablePort(443),
		LocalPort:  nullablePort(12096),
		BytesTotal: 0,
		FlowCount:  0,
		Confidence: "medium",
		Evidence:   "ss-log",
		SourceIP:   "",
		TargetIP:   "",
	}) {
		t.Fatalf("expected zero-metric host-only candidate to be skipped")
	}

	if !shouldPersistCanonicalChain(usageExplainChain{
		TargetHost: "chatgpt.com",
		TargetPort: nullablePort(443),
		LocalPort:  nullablePort(12096),
		TargetIP:   "142.250.72.14",
		BytesTotal: 10240,
		FlowCount:  4,
		Confidence: "medium",
		Evidence:   "ss-log",
	}) {
		t.Fatalf("expected hydrated chain with traffic totals to be persisted")
	}
}

func TestShouldPersistCanonicalChainSkipsUnresolvedHostOnlyCandidates(t *testing.T) {
	if shouldPersistCanonicalChain(usageExplainChain{
		SourceIP:   "203.0.113.24",
		TargetHost: "chatgpt.com",
		TargetPort: nullablePort(443),
		LocalPort:  nullablePort(12096),
		BytesTotal: 4096,
		FlowCount:  2,
		Confidence: "high",
		Evidence:   "ss-log",
	}) {
		t.Fatalf("expected host-only chain without target ip to stay ephemeral")
	}
}

func TestMergeExplainChainsPrefersRicherIncomingChain(t *testing.T) {
	existing := []usageExplainChain{
		{
			ChainID:       "usage_chain_1m|a",
			SourceIP:      "203.0.113.24",
			TargetIP:      "142.250.72.14",
			TargetPort:    nullablePort(443),
			LocalPort:     nullablePort(12096),
			BytesTotal:    1024,
			FlowCount:     1,
			EvidenceCount: 1,
			Evidence:      "分钟链路记录",
			Confidence:    "low",
		},
	}
	incoming := []usageExplainChain{
		{
			ChainID:              "usage_chain_1h|b",
			SourceIP:             "203.0.113.24",
			TargetIP:             "142.250.72.14",
			TargetHost:           "chatgpt.com",
			TargetHostNormalized: "chatgpt.com",
			TargetPort:           nullablePort(443),
			LocalPort:            nullablePort(12096),
			BytesTotal:           8192,
			FlowCount:            4,
			EvidenceCount:        3,
			Evidence:             "小时链路记录",
			EvidenceSource:       "ss-log",
			SampleFingerprint:    "fp-1",
			SampleMessage:        "connect to chatgpt.com:443",
			SampleTime:           1713232800,
			Confidence:           "high",
		},
	}

	merged := mergeExplainChains(existing, incoming, 0)
	if len(merged) != 1 {
		t.Fatalf("expected merged chain count 1, got %+v", merged)
	}
	if merged[0].TargetHost != "chatgpt.com" || merged[0].Confidence != "high" {
		t.Fatalf("expected richer incoming chain to win, got %+v", merged[0])
	}
	if merged[0].BytesTotal != 8192 || merged[0].FlowCount != 4 || merged[0].EvidenceCount != 3 {
		t.Fatalf("expected merged metrics to keep richer chain totals, got %+v", merged[0])
	}
}

func TestSummarizePeersSkipsZeroMetricCandidates(t *testing.T) {
	peers := summarizePeers(map[string]peerAgg{
		"zero": {
			Direction:  model.DirectionOut,
			RemoteIP:   "127.0.0.1",
			RemotePort: 18080,
			LocalPort:  59692,
		},
		"real": {
			Direction:  model.DirectionIn,
			RemoteIP:   "203.0.113.24",
			RemotePort: 52144,
			LocalPort:  8388,
			BytesTotal: 4096,
			FlowCount:  2,
		},
	}, 8)
	if len(peers) != 1 {
		t.Fatalf("expected only non-zero peers to remain, got %+v", peers)
	}
	if peers[0].RemoteIP != "203.0.113.24" {
		t.Fatalf("unexpected peer kept after filtering: %+v", peers[0])
	}
}

func TestInferConfidenceDowngradesLoopbackFallbackNginxHits(t *testing.T) {
	confidence := inferConfidence(usageExplainResponse{
		NginxRequests: []usageExplainNginxRequest{
			{Path: "/traffic", Count: 1},
		},
	})
	if confidence != "medium" {
		t.Fatalf("expected fallback-only nginx evidence to be medium confidence, got %s", confidence)
	}

	confidence = inferConfidence(usageExplainResponse{
		NginxRequests: []usageExplainNginxRequest{
			{Path: "/traffic", Count: 1},
		},
		StrongMatch: true,
	})
	if confidence != "high" {
		t.Fatalf("expected direct nginx evidence to remain high confidence, got %s", confidence)
	}
}

func TestInferConfidenceTreatsPeerCooccurrenceAsMedium(t *testing.T) {
	confidence := inferConfidence(usageExplainResponse{
		SourceIPs: []string{"203.0.113.24"},
		TargetIPs: []string{"142.250.72.14"},
	})
	if confidence != "medium" {
		t.Fatalf("expected neighboring peer cooccurrence to stay medium confidence, got %s", confidence)
	}

	confidence = inferConfidence(usageExplainResponse{
		SourceIPs: []string{"203.0.113.24"},
	})
	if confidence != "low" {
		t.Fatalf("expected one-sided peer hints to stay low confidence, got %s", confidence)
	}
}

func TestCollectEntryPortSourceCandidatesFiltersToExplainProcess(t *testing.T) {
	server := newTestServer(t)
	ctx := context.Background()
	minute := time.Date(2026, 4, 21, 11, 20, 0, 0, time.UTC).Unix()
	ssPID := 1088
	nginxPID := 3312

	if err := server.store.FlushMinute(ctx, minute, map[model.UsageKey]model.UsageDelta{
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         ssPID,
			Comm:        "ss-server",
			Exe:         "/usr/bin/ss-server",
			LocalPort:   12096,
			RemoteIP:    "203.0.113.24",
			RemotePort:  44598,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   400,
			BytesDown: 3200,
			PktsUp:    2,
			PktsDown:  9,
			FlowCount: 1,
		},
		{
			MinuteTS:    minute,
			Proto:       "tcp",
			Direction:   model.DirectionIn,
			PID:         nginxPID,
			Comm:        "nginx",
			Exe:         "/usr/sbin/nginx",
			LocalPort:   12096,
			RemoteIP:    "198.51.100.77",
			RemotePort:  41220,
			Attribution: model.AttributionExact,
		}: {
			BytesUp:   600,
			BytesDown: 6400,
			PktsUp:    3,
			PktsDown:  12,
			FlowCount: 1,
		},
	}, nil); err != nil {
		t.Fatalf("seed usage rows: %v", err)
	}

	candidates, err := server.collectEntryPortSourceCandidates(ctx, minute, usageExplainQuery{
		TimeBucket: minute,
		Proto:      "tcp",
		PID:        &ssPID,
		Comm:       "ss-server",
		Exe:        "/usr/bin/ss-server",
	}, []int{12096})
	if err != nil {
		t.Fatalf("collect entry-port source candidates: %v", err)
	}

	portCandidates := candidates[12096]
	if len(portCandidates) != 1 {
		t.Fatalf("expected only same-process candidates, got %+v", portCandidates)
	}
	if portCandidates[0].RemoteIP != "203.0.113.24" {
		t.Fatalf("unexpected candidate selected: %+v", portCandidates[0])
	}
}

func TestLookupOrScanEvidenceAcrossDirsMergesConfiguredDirectoryHits(t *testing.T) {
	server := newTestServer(t)
	dirA := t.TempDir()
	dirB := t.TempDir()
	bucket := time.Date(2026, 4, 21, 11, 20, 0, 0, time.Local)

	lines := map[string]string{
		filepath.Join(dirA, "relay.log"): "2026-04-21 11:20:06 relay client=203.0.113.24 target=142.250.72.14:443\n",
		filepath.Join(dirB, "relay.log"): "2026-04-21 11:20:16 relay client=198.51.100.77 target=1.1.1.1:443\n",
	}
	for path, content := range lines {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write log file %s: %v", path, err)
		}
	}

	rows, notes, err := server.lookupOrScanEvidenceAcrossDirs(
		context.Background(),
		customEvidenceSource("relay"),
		[]string{dirA, dirB},
		bucket.Unix(),
		10,
		true,
		func(model.LogEvidence) bool { return true },
		evidenceQueryHints{},
		isGenericLogFileName,
		parseGenericEvidenceLine,
	)
	if err != nil {
		t.Fatalf("lookup evidence across dirs: %v", err)
	}
	if len(notes) != 0 {
		t.Fatalf("did not expect miss notes when configured directories both contribute rows, got %+v", notes)
	}
	if len(rows) != 2 {
		t.Fatalf("expected rows from both configured directories, got %+v", rows)
	}

	gotClients := []string{rows[0].ClientIP, rows[1].ClientIP}
	if !strings.Contains(strings.Join(gotClients, ","), "203.0.113.24") || !strings.Contains(strings.Join(gotClients, ","), "198.51.100.77") {
		t.Fatalf("expected both directory hits to survive merge, got %+v", rows)
	}
}
