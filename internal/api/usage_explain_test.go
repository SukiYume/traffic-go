package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	files, err := listLogFiles(dir, isShadowsocksLogFileName, start, end, 10)
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
}

func TestScanEvidenceFilesReturnsScannerError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ss-server.log")
	tooLongLine := strings.Repeat("x", 2*1024*1024)
	if err := os.WriteFile(path, []byte(tooLongLine+"\n"), 0o644); err != nil {
		t.Fatalf("write oversized log line: %v", err)
	}

	_, err := scanEvidenceFiles(
		context.Background(),
		evidenceSourceSS,
		[]logFileCandidate{{Path: path}},
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

func TestSSLogDirCandidatesIncludeShadowsocksSubdir(t *testing.T) {
	dirs := ssLogDirCandidates("/var/log")
	found := false
	for _, dir := range dirs {
		if strings.Contains(strings.ToLower(dir), "shadowsocks") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected shadowsocks subdir candidate, got %v", dirs)
	}
}
