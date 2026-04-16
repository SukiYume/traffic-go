package api

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

const (
	usageExplainWindowPadding = int64(60)
	logWindowStrict           = int64(120)
	logWindowFallback         = int64(900)
	maxRelatedPeers           = 8
	maxNginxRequests          = 8
	maxEvidenceRows           = 24
	maxScanFilesStrict        = 4
	maxScanFilesFallback      = 8
	maxScanLinesPerFile       = 240000
)

const (
	evidenceSourceNginx = "nginx"
	evidenceSourceSS    = "ss"
)

var (
	nginxAccessPattern = regexp.MustCompile(`^(.*?)\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3})\s+(\S+)(?:\s+"([^"]*)"\s+"([^"]*)")?`)
	ssClientPattern    = regexp.MustCompile(`(?i)(?:client|src|source|from)\s*[=:]\s*([^\s,;]+)`)
	ssFromPattern      = regexp.MustCompile(`(?i)\bfrom\s+([^\s,;]+)`)
	ssTargetPattern    = regexp.MustCompile(`(?i)(?:target|dst|destination|to)\s*[=:]\s*([^\s,;]+)`)
	ssConnectPattern   = regexp.MustCompile(`(?i)\bconnect to\s+([^\s,;]+)`)
	ssUDPCachePattern  = regexp.MustCompile(`(?i)\[udp\]\s+cache miss:\s*([^\s,;]+)\s*<->\s*([^\s,;]+)`)
	rfc3339Pattern     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})`)
	syslogTimePattern  = regexp.MustCompile(`^[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}`)
	fileDatePattern    = regexp.MustCompile(`(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)`)
)

type usageExplainQuery struct {
	TimeBucket int64
	Proto      string
	Direction  model.Direction
	PID        *int
	Comm       string
	Exe        string
	LocalPort  *int
	RemoteIP   string
	RemotePort *int
}

type usageExplainResponse struct {
	Process       string                     `json:"process"`
	Confidence    string                     `json:"confidence"`
	SourceIPs     []string                   `json:"source_ips"`
	TargetIPs     []string                   `json:"target_ips"`
	RelatedPeers  []usageExplainPeer         `json:"related_peers"`
	NginxRequests []usageExplainNginxRequest `json:"nginx_requests"`
	Notes         []string                   `json:"notes"`
}

type usageExplainPeer struct {
	Direction  model.Direction `json:"direction"`
	RemoteIP   string          `json:"remote_ip"`
	RemotePort *int            `json:"remote_port,omitempty"`
	LocalPort  *int            `json:"local_port,omitempty"`
	BytesTotal int64           `json:"bytes_total"`
	FlowCount  int64           `json:"flow_count"`
}

type usageExplainNginxRequest struct {
	Time      int64  `json:"time"`
	Method    string `json:"method"`
	Host      string `json:"host,omitempty"`
	Path      string `json:"path"`
	Status    int    `json:"status"`
	Count     int    `json:"count"`
	Referer   string `json:"referer,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	Bot       string `json:"bot,omitempty"`
}

type ipWeight struct {
	IP    string
	Bytes int64
}

type peerAgg struct {
	Direction  model.Direction
	RemoteIP   string
	RemotePort int
	LocalPort  int
	BytesTotal int64
	FlowCount  int64
}

type logFileCandidate struct {
	Name    string
	Path    string
	ModTime time.Time
}

type evidenceMatcher func(model.LogEvidence) bool

type evidenceParser func(source string, line string, reference time.Time) (model.LogEvidence, bool)

type evidenceQueryHints struct {
	ClientIP string
	TargetIP string
	AnyIP    string
}

func (s *Server) handleUsageExplain(w http.ResponseWriter, r *http.Request) {
	query, err := parseUsageExplainQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_usage_explain_query", err)
		return
	}

	data, err := s.analyzeUsageExplain(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}

	writeJSON(w, http.StatusOK, envelope{"data": data})
}

func parseUsageExplainQuery(r *http.Request) (usageExplainQuery, error) {
	query := usageExplainQuery{
		TimeBucket: parseInt64WithDefault(r.URL.Query().Get("ts"), 0),
		Proto:      strings.ToLower(strings.TrimSpace(r.URL.Query().Get("proto"))),
		Direction:  model.Direction(strings.TrimSpace(r.URL.Query().Get("direction"))),
		Comm:       strings.TrimSpace(r.URL.Query().Get("comm")),
		Exe:        strings.TrimSpace(r.URL.Query().Get("exe")),
		RemoteIP:   strings.TrimSpace(r.URL.Query().Get("remote_ip")),
	}

	if query.TimeBucket <= 0 {
		return query, fmt.Errorf("ts is required")
	}
	if query.Direction != model.DirectionIn && query.Direction != model.DirectionOut {
		return query, fmt.Errorf("direction must be in or out")
	}
	if query.Proto != "" && query.Proto != "tcp" && query.Proto != "udp" {
		return query, fmt.Errorf("proto must be tcp or udp")
	}

	if value := strings.TrimSpace(r.URL.Query().Get("pid")); value != "" {
		pid, err := strconv.Atoi(value)
		if err != nil {
			return query, fmt.Errorf("invalid pid: %w", err)
		}
		query.PID = &pid
	}
	if value := strings.TrimSpace(r.URL.Query().Get("local_port")); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil {
			return query, fmt.Errorf("invalid local_port: %w", err)
		}
		query.LocalPort = &port
	}
	if value := strings.TrimSpace(r.URL.Query().Get("remote_port")); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil {
			return query, fmt.Errorf("invalid remote_port: %w", err)
		}
		query.RemotePort = &port
	}

	return query, nil
}

func parseInt64WithDefault(value string, fallback int64) int64 {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func (s *Server) analyzeUsageExplain(ctx context.Context, query usageExplainQuery) (usageExplainResponse, error) {
	bucketTS := time.Unix(query.TimeBucket, 0).UTC().Truncate(time.Minute).Unix()

	related, err := s.collectRelatedUsage(ctx, query, bucketTS)
	if err != nil {
		return usageExplainResponse{}, err
	}

	response := usageExplainResponse{
		Process:       processLabel(query.Comm, query.Exe),
		SourceIPs:     make([]string, 0),
		TargetIPs:     make([]string, 0),
		RelatedPeers:  make([]usageExplainPeer, 0),
		NginxRequests: make([]usageExplainNginxRequest, 0),
		Notes:         make([]string, 0),
	}

	sourceIPWeights := make(map[string]int64)
	targetIPWeights := make(map[string]int64)
	peerMap := make(map[string]peerAgg)

	for _, record := range related {
		if record.Direction != model.DirectionIn && record.Direction != model.DirectionOut {
			continue
		}
		if record.RemoteIP == "" {
			continue
		}
		bytesTotal := record.BytesUp + record.BytesDown
		if record.Direction == model.DirectionIn {
			sourceIPWeights[record.RemoteIP] += bytesTotal
		} else {
			targetIPWeights[record.RemoteIP] += bytesTotal
		}

		remotePort := 0
		if record.RemotePort != nil {
			remotePort = *record.RemotePort
		}
		key := fmt.Sprintf("%s|%s|%d|%d", record.Direction, record.RemoteIP, record.LocalPort, remotePort)
		agg := peerMap[key]
		agg.Direction = record.Direction
		agg.RemoteIP = record.RemoteIP
		agg.RemotePort = remotePort
		agg.LocalPort = record.LocalPort
		agg.BytesTotal += bytesTotal
		agg.FlowCount += record.FlowCount
		peerMap[key] = agg
	}

	response.SourceIPs = topIPsByWeight(sourceIPWeights, 6)
	response.TargetIPs = topIPsByWeight(targetIPWeights, 6)
	response.RelatedPeers = summarizePeers(peerMap, maxRelatedPeers)

	if isShadowsocksProcess(query.Comm, query.Exe) {
		s.addCurrentTupleNote(&response, query)
		if err := s.enrichShadowsocksFromLogs(ctx, &response, query, bucketTS); err != nil {
			appendNoteUnique(&response.Notes, fmt.Sprintf("SS 日志检索失败：%v", err))
		}
		appendNoteUnique(&response.Notes, "Shadowsocks 使用加密与复用，日志关联是候选推断，不保证来源与目标一一对应。")
	}

	if isNginxProcess(query.Comm, query.Exe) {
		s.addCurrentTupleNote(&response, query)
		if err := s.enrichNginxFromLogs(ctx, &response, query.RemoteIP, bucketTS); err != nil {
			appendNoteUnique(&response.Notes, fmt.Sprintf("Nginx 日志检索失败：%v", err))
		}
		appendNoteUnique(&response.Notes, "HTTP/HTTPS 网页路径优先来自 access.log 关联；仅靠 conntrack 无法直接提取 URI。")
	}

	if len(response.SourceIPs) == 0 && len(response.TargetIPs) == 0 {
		appendNoteUnique(&response.Notes, "没有找到足够的同进程关联流量，建议放宽筛选条件或扩大时间范围后重试。")
	}

	response.Confidence = inferConfidence(response)
	return response, nil
}

func (s *Server) collectRelatedUsage(ctx context.Context, query usageExplainQuery, bucketTS int64) ([]model.UsageRecord, error) {
	usageQuery := model.UsageQuery{
		Start:     time.Unix(bucketTS-usageExplainWindowPadding, 0).UTC(),
		End:       time.Unix(bucketTS+usageExplainWindowPadding+60, 0).UTC(),
		Proto:     query.Proto,
		UsePage:   true,
		Page:      1,
		PageSize:  200,
		SortBy:    "bytes_total",
		SortOrder: "desc",
	}

	hasIdentity := false
	if query.PID != nil && *query.PID > 0 {
		usageQuery.PID = query.PID
		hasIdentity = true
	}
	if query.Comm != "" {
		usageQuery.Comm = query.Comm
		hasIdentity = true
	}
	if query.Exe != "" {
		usageQuery.Exe = query.Exe
		hasIdentity = true
	}
	if !hasIdentity {
		usageQuery.RemoteIP = query.RemoteIP
		usageQuery.Direction = query.Direction
		usageQuery.LocalPort = query.LocalPort
	}

	records, _, _, err := s.store.QueryUsage(ctx, usageQuery, store.DataSourceMinute)
	if err != nil {
		return nil, fmt.Errorf("query related usage: %w", err)
	}
	return records, nil
}

func (s *Server) enrichNginxFromLogs(ctx context.Context, response *usageExplainResponse, clientIP string, bucketTS int64) error {
	if strings.TrimSpace(clientIP) == "" {
		return nil
	}
	exactMatcher := func(ev model.LogEvidence) bool {
		return sameIP(ev.ClientIP, clientIP)
	}
	rows, note, err := s.lookupOrScanEvidence(
		ctx,
		evidenceSourceNginx,
		s.nginxLogDir,
		bucketTS,
		maxEvidenceRows,
		exactMatcher,
		evidenceQueryHints{ClientIP: clientIP},
		isNginxLogFileName,
		parseNginxEvidenceLine,
	)
	if err != nil {
		return err
	}
	usedLoopbackFallback := false
	if len(rows) == 0 {
		loopbackRows, loopbackNote, loopbackErr := s.lookupOrScanEvidence(
			ctx,
			evidenceSourceNginx,
			s.nginxLogDir,
			bucketTS,
			maxEvidenceRows,
			func(ev model.LogEvidence) bool {
				return isLoopbackIP(ev.ClientIP)
			},
			evidenceQueryHints{},
			isNginxLogFileName,
			parseNginxEvidenceLine,
		)
		if loopbackErr != nil {
			return loopbackErr
		}
		if len(loopbackRows) > 0 {
			rows = loopbackRows
			note = loopbackNote
			usedLoopbackFallback = true
		}
	}
	if note != "" {
		appendNoteUnique(&response.Notes, note)
	}
	if len(rows) == 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("目录 %s 的日志中未匹配到该来源 IP。", resolvedLogDir(s.nginxLogDir, "/var/log/nginx")))
		return nil
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EventTS == rows[j].EventTS {
			return rows[i].Path > rows[j].Path
		}
		return rows[i].EventTS > rows[j].EventTS
	})
	response.NginxRequests = summarizeNginxRequests(rows, maxNginxRequests)
	if usedLoopbackFallback {
		appendNoteUnique(&response.Notes, "未直接命中来源 IP，已回退到本机回环来源日志做候选关联。")
	}
	appendNoteUnique(&response.Notes, fmt.Sprintf("Nginx 日志命中 %d 条，聚合为 %d 组。", len(rows), len(response.NginxRequests)))
	appendNginxStatusAndAgentNotes(&response.Notes, rows, response.NginxRequests)
	return nil
}

func (s *Server) enrichShadowsocksFromLogs(ctx context.Context, response *usageExplainResponse, query usageExplainQuery, bucketTS int64) error {
	remoteIP := strings.TrimSpace(query.RemoteIP)
	remotePort := 0
	if query.RemotePort != nil {
		remotePort = *query.RemotePort
	}
	if remoteIP == "" && !(query.Direction == model.DirectionOut && remotePort > 0) {
		return nil
	}
	matcher := func(ev model.LogEvidence) bool {
		if remoteIP != "" {
			if sameIP(ev.ClientIP, remoteIP) {
				return true
			}
			if sameIP(ev.TargetIP, remoteIP) {
				return true
			}
		}
		// libev ss-server logs often record domain:port without target IP,
		// so fall back to strict same-port matching for outbound explain rows.
		if query.Direction == model.DirectionOut && remotePort > 0 && ev.Host != "" {
			if port, ok := parseEvidencePort(ev.Path); ok && port == remotePort {
				// Keep fallback conservative to reduce false matches on common ports like 443.
				if absInt64(ev.EventTS-bucketTS) > logWindowStrict {
					return false
				}
				if remoteIP != "" && ev.TargetIP != "" && !sameIP(ev.TargetIP, remoteIP) {
					return false
				}
				if ev.Method != "connect" && ev.Method != "udp-cache-miss" && ev.Method != "udp" {
					return false
				}
				return true
			}
		}
		return false
	}

	rows := make([]model.LogEvidence, 0)
	note := ""
	usedLogDir := ""
	for _, logDir := range ssLogDirCandidates(s.ssLogDir) {
		hits, hitNote, err := s.lookupOrScanEvidence(
			ctx,
			evidenceSourceSS,
			logDir,
			bucketTS,
			maxEvidenceRows,
			matcher,
			evidenceQueryHints{AnyIP: remoteIP},
			isShadowsocksLogFileName,
			parseSSEvidenceLine,
		)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if len(hits) == 0 {
			fallbackRows, fallbackNote, fallbackErr := s.lookupOrScanEvidence(
				ctx,
				evidenceSourceSS,
				logDir,
				bucketTS,
				maxEvidenceRows,
				matcher,
				evidenceQueryHints{AnyIP: remoteIP},
				isGenericLogFileName,
				parseSSEvidenceLine,
			)
			if fallbackErr != nil {
				if errors.Is(fallbackErr, os.ErrNotExist) {
					continue
				}
				return fallbackErr
			}
			if len(fallbackRows) > 0 {
				hits = fallbackRows
				if fallbackNote != "" {
					hitNote = fallbackNote
				}
			}
		}
		if len(hits) == 0 {
			continue
		}
		rows = hits
		note = hitNote
		usedLogDir = logDir
		break
	}
	if note != "" {
		appendNoteUnique(&response.Notes, note)
	}
	configuredDir := resolvedLogDir(s.ssLogDir, "/var/log")
	if usedLogDir != "" && filepath.Clean(usedLogDir) != filepath.Clean(configuredDir) {
		appendNoteUnique(&response.Notes, fmt.Sprintf("SS 日志目录自动回退到 %s。", usedLogDir))
	}
	if len(rows) == 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("目录 %s 中未命中 SS 相关日志。", configuredDir))
		return nil
	}

	sourceWeights := make(map[string]int64)
	targetWeights := make(map[string]int64)
	hostCounts := make(map[string]int)
	sourceConfirmed := make(map[string]int)
	directIPHits := 0
	for _, row := range rows {
		if row.ClientIP != "" {
			sourceWeights[row.ClientIP]++
		}
		if row.TargetIP != "" {
			targetWeights[row.TargetIP]++
		}
		if remoteIP != "" && (sameIP(row.ClientIP, remoteIP) || sameIP(row.TargetIP, remoteIP)) {
			directIPHits++
		}
		if query.Direction == model.DirectionOut && remoteIP != "" && row.ClientIP != "" && sameIP(row.TargetIP, remoteIP) {
			sourceConfirmed[row.ClientIP]++
		}
		if row.Method == "udp-cache-miss" && row.ClientIP != "" {
			sourceConfirmed[row.ClientIP] += 2
		}
		if row.Host != "" {
			host := row.Host
			if port, ok := parseEvidencePort(row.Path); ok {
				host = fmt.Sprintf("%s:%d", host, port)
			}
			hostCounts[host]++
		}
	}

	response.SourceIPs = mergeTopIPs(response.SourceIPs, topIPsByCount(sourceWeights, 4), 6)
	response.TargetIPs = mergeTopIPs(response.TargetIPs, topIPsByCount(targetWeights, 4), 6)
	if len(hostCounts) > 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("SS 目标主机候选：%s", formatTopLabeledCounts(hostCounts, 5)))
	}
	if len(sourceConfirmed) > 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("SS 来源IP确认：%s（高置信）", formatTopLabeledCounts(sourceConfirmed, 3)))
	} else if len(sourceWeights) > 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("SS 来源IP候选：%s（中置信）", formatTopLabeledCounts(int64MapToIntMap(sourceWeights), 3)))
	}
	if query.Direction == model.DirectionOut && remotePort > 0 && remoteIP != "" && directIPHits == 0 {
		appendNoteUnique(&response.Notes, "SS 日志未直接命中 remote_ip，已按远端端口筛选目标主机候选。")
	}
	appendNoteUnique(&response.Notes, fmt.Sprintf("SS 日志命中 %d 条。", len(rows)))
	for i := 0; i < len(rows) && i < 2; i++ {
		if rows[i].Message != "" {
			appendNoteUnique(&response.Notes, fmt.Sprintf("SS 样本：%s", rows[i].Message))
		}
	}
	return nil
}

func ssLogDirCandidates(configured string) []string {
	primary := resolvedLogDir(configured, "/var/log")
	candidates := []string{
		primary,
		filepath.Join(primary, "shadowsocks"),
		filepath.Join(primary, "ss"),
		"/var/log/shadowsocks",
		"/var/log/ss",
	}
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{})
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		cleaned := filepath.Clean(trimmed)
		if _, ok := seen[cleaned]; ok {
			continue
		}
		seen[cleaned] = struct{}{}
		result = append(result, cleaned)
	}
	return result
}

func parseEvidencePort(value string) (int, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	port, err := strconv.Atoi(trimmed)
	if err != nil || port <= 0 {
		return 0, false
	}
	return port, true
}

func (s *Server) lookupOrScanEvidence(
	ctx context.Context,
	source string,
	logDir string,
	bucketTS int64,
	limit int,
	matcher evidenceMatcher,
	queryHints evidenceQueryHints,
	fileNameMatcher func(string) bool,
	parser evidenceParser,
) ([]model.LogEvidence, string, error) {
	strictStart := bucketTS - logWindowStrict
	strictEnd := bucketTS + logWindowStrict
	fallbackStart := bucketTS - logWindowFallback
	fallbackEnd := bucketTS + logWindowFallback
	cacheLimit := cacheEvidenceLimit(limit)

	rows, err := s.store.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source:   source,
		StartTS:  strictStart,
		EndTS:    strictEnd,
		ClientIP: queryHints.ClientIP,
		TargetIP: queryHints.TargetIP,
		AnyIP:    queryHints.AnyIP,
		Limit:    cacheLimit,
	})
	if err != nil {
		return nil, "", fmt.Errorf("query cached evidence: %w", err)
	}
	rows = filterEvidence(rows, matcher)
	rows = sortAndTrimEvidence(rows, bucketTS, limit)
	if len(rows) > 0 {
		return rows, "", nil
	}

	rows, err = s.store.QueryLogEvidence(ctx, store.LogEvidenceQuery{
		Source:   source,
		StartTS:  fallbackStart,
		EndTS:    fallbackEnd,
		ClientIP: queryHints.ClientIP,
		TargetIP: queryHints.TargetIP,
		AnyIP:    queryHints.AnyIP,
		Limit:    cacheLimit,
	})
	if err != nil {
		return nil, "", fmt.Errorf("query fallback evidence: %w", err)
	}
	rows = filterEvidence(rows, matcher)
	rows = sortAndTrimEvidence(rows, bucketTS, limit)
	if len(rows) > 0 {
		return rows, "在缓存中命中 ±15 分钟窗口日志。", nil
	}

	strictFiles, err := listLogFiles(logDir, fileNameMatcher, strictStart, strictEnd, maxScanFilesStrict)
	if err != nil {
		return nil, "", err
	}
	if len(strictFiles) > 0 {
		scannedStrict, err := scanEvidenceFiles(ctx, source, strictFiles, parser, matcher, strictStart, strictEnd, bucketTS, limit*3, maxScanLinesPerFile)
		if err != nil {
			return nil, "", err
		}
		if len(scannedStrict) > 0 {
			if err := s.store.UpsertLogEvidenceBatch(ctx, scannedStrict); err != nil {
				return nil, "", fmt.Errorf("persist strict evidence: %w", err)
			}
			return sortAndTrimEvidence(scannedStrict, bucketTS, limit), "", nil
		}
	}

	fallbackFiles, err := listLogFiles(logDir, fileNameMatcher, fallbackStart, fallbackEnd, maxScanFilesFallback)
	if err != nil {
		return nil, "", err
	}
	if len(fallbackFiles) == 0 {
		return nil, "", nil
	}

	scannedFallback, err := scanEvidenceFiles(ctx, source, fallbackFiles, parser, matcher, fallbackStart, fallbackEnd, bucketTS, limit*3, maxScanLinesPerFile)
	if err != nil {
		return nil, "", err
	}
	if len(scannedFallback) > 0 {
		if err := s.store.UpsertLogEvidenceBatch(ctx, scannedFallback); err != nil {
			return nil, "", fmt.Errorf("persist fallback evidence: %w", err)
		}
		return sortAndTrimEvidence(scannedFallback, bucketTS, limit), "在 ±2 分钟窗口未命中，已回退到 ±15 分钟窗口匹配日志。", nil
	}

	return nil, "", nil
}

func cacheEvidenceLimit(limit int) int {
	if limit <= 0 {
		return 200
	}
	computed := limit * 10
	if computed < limit+64 {
		computed = limit + 64
	}
	if computed > 2000 {
		return 2000
	}
	return computed
}

func listLogFiles(logDir string, nameMatcher func(string) bool, startTS int64, endTS int64, maxCandidates int) ([]logFileCandidate, error) {
	dir := resolvedLogDir(logDir, "")
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("读取日志目录失败：%s（%w）", dir, err)
	}
	files := make([]logFileCandidate, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		lower := strings.ToLower(entry.Name())
		if !nameMatcher(lower) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if !isLikelyFileInTimeRange(lower, info.ModTime(), startTS, endTS) {
			continue
		}
		files = append(files, logFileCandidate{
			Name:    entry.Name(),
			Path:    filepath.Join(dir, entry.Name()),
			ModTime: info.ModTime(),
		})
	}

	if len(files) == 0 {
		return nil, nil
	}

	centerTS := (startTS + endTS) / 2
	sort.Slice(files, func(i, j int) bool {
		di := absInt64(files[i].ModTime.Unix() - centerTS)
		dj := absInt64(files[j].ModTime.Unix() - centerTS)
		if di == dj {
			if files[i].ModTime.Equal(files[j].ModTime) {
				return files[i].Path > files[j].Path
			}
			return files[i].ModTime.After(files[j].ModTime)
		}
		return di < dj
	})
	if maxCandidates > 0 && len(files) > maxCandidates {
		files = files[:maxCandidates]
	}
	return files, nil
}

func isLikelyFileInTimeRange(lowerName string, modTime time.Time, startTS int64, endTS int64) bool {
	if startTS <= 0 || endTS <= 0 {
		return true
	}
	if endTS < startTS {
		startTS, endTS = endTS, startTS
	}
	start := time.Unix(startTS, 0).UTC()
	end := time.Unix(endTS, 0).UTC()
	dayStart := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC).Add(-24 * time.Hour)
	dayEnd := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 0, time.UTC).Add(24 * time.Hour)

	dates := extractDateCandidatesFromFileName(lowerName)
	if len(dates) > 0 {
		for _, date := range dates {
			if !date.Before(dayStart) && !date.After(dayEnd) {
				return true
			}
		}
		return false
	}

	if modTime.IsZero() {
		return true
	}
	mod := modTime.UTC()
	if mod.Before(dayStart.Add(-24 * time.Hour)) {
		return false
	}
	if mod.After(dayEnd.Add(72 * time.Hour)) {
		return false
	}
	return true
}

func extractDateCandidatesFromFileName(lowerName string) []time.Time {
	matches := fileDatePattern.FindAllStringSubmatch(lowerName, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[int64]struct{})
	result := make([]time.Time, 0, len(matches))
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		year, errYear := strconv.Atoi(match[1])
		month, errMonth := strconv.Atoi(match[2])
		day, errDay := strconv.Atoi(match[3])
		if errYear != nil || errMonth != nil || errDay != nil {
			continue
		}
		candidate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		if candidate.Year() != year || int(candidate.Month()) != month || candidate.Day() != day {
			continue
		}
		unix := candidate.Unix()
		if _, exists := seen[unix]; exists {
			continue
		}
		seen[unix] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func summarizeNginxRequests(rows []model.LogEvidence, limit int) []usageExplainNginxRequest {
	if len(rows) == 0 {
		return nil
	}
	aggMap := make(map[string]*usageExplainNginxRequest)
	for _, row := range rows {
		status := 0
		if row.Status != nil {
			status = *row.Status
		}
		referer, userAgent := parseNginxHeaderHints(row.Message)
		bot := detectBotFromUserAgent(userAgent)
		method := fallbackText(normalizeNginxRequestPart(row.Method, 32), "GET")
		host := normalizeNginxRequestPart(strings.TrimSpace(row.Host), 128)
		path := fallbackText(normalizeNginxRequestPart(row.Path, 260), "/")
		if strings.HasPrefix(method, "[binary ") {
			path = method
			method = "BINARY"
			host = ""
		}
		if strings.HasPrefix(path, "[binary ") {
			method = "BINARY"
			host = ""
		}

		request := usageExplainNginxRequest{
			Time:      row.EventTS,
			Method:    method,
			Host:      host,
			Path:      path,
			Status:    status,
			Count:     1,
			Referer:   trimDisplayValue(referer, 180),
			UserAgent: trimDisplayValue(userAgent, 180),
			Bot:       bot,
		}

		key := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s", request.Method, request.Host, request.Path, request.Status, request.Bot, request.UserAgent, request.Referer)
		agg, exists := aggMap[key]
		if !exists {
			aggMap[key] = &request
			continue
		}
		agg.Count++
		if request.Time > agg.Time {
			agg.Time = request.Time
		}
	}

	aggs := make([]usageExplainNginxRequest, 0, len(aggMap))
	for _, item := range aggMap {
		aggs = append(aggs, *item)
	}
	sort.Slice(aggs, func(i, j int) bool {
		if aggs[i].Count == aggs[j].Count {
			if aggs[i].Time == aggs[j].Time {
				return aggs[i].Path < aggs[j].Path
			}
			return aggs[i].Time > aggs[j].Time
		}
		return aggs[i].Count > aggs[j].Count
	})
	if limit > 0 && len(aggs) > limit {
		aggs = aggs[:limit]
	}
	return aggs
}

func parseNginxHeaderHints(message string) (string, string) {
	match := nginxAccessPattern.FindStringSubmatch(message)
	if len(match) < 8 {
		return "", ""
	}
	referer := ""
	if len(match) > 6 {
		referer = cleanNginxHeaderValue(match[6])
	}
	userAgent := ""
	if len(match) > 7 {
		userAgent = cleanNginxHeaderValue(match[7])
	}
	return referer, userAgent
}

func cleanNginxHeaderValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "-" {
		return ""
	}
	return trimmed
}

func detectBotFromUserAgent(userAgent string) string {
	lower := strings.ToLower(userAgent)
	switch {
	case strings.Contains(lower, "gptbot"):
		return "GPTBot"
	case strings.Contains(lower, "googlebot"):
		return "Googlebot"
	case strings.Contains(lower, "bingbot"):
		return "Bingbot"
	case strings.Contains(lower, "baiduspider"):
		return "BaiduSpider"
	case strings.Contains(lower, "yandexbot"):
		return "YandexBot"
	case strings.Contains(lower, "duckduckbot"):
		return "DuckDuckBot"
	case strings.Contains(lower, "bytespider"):
		return "ByteSpider"
	case strings.Contains(lower, "bot") || strings.Contains(lower, "crawler") || strings.Contains(lower, "spider"):
		return "Bot"
	default:
		return ""
	}
}

func appendNginxStatusAndAgentNotes(notes *[]string, rows []model.LogEvidence, requests []usageExplainNginxRequest) {
	if len(rows) == 0 {
		return
	}

	statusCounts := make(map[string]int)
	for _, row := range rows {
		if row.Status == nil {
			continue
		}
		class := fmt.Sprintf("%dxx", *row.Status/100)
		statusCounts[class]++
	}
	if len(statusCounts) > 0 {
		appendNoteUnique(notes, fmt.Sprintf("HTTP 状态分布：%s", formatTopLabeledCounts(statusCounts, 4)))
	}

	botCounts := make(map[string]int)
	for _, request := range requests {
		if request.Bot == "" {
			continue
		}
		weight := request.Count
		if weight <= 0 {
			weight = 1
		}
		botCounts[request.Bot] += weight
	}
	if len(botCounts) > 0 {
		appendNoteUnique(notes, fmt.Sprintf("访问端识别：%s", formatTopLabeledCounts(botCounts, 3)))
	}
}

func formatTopLabeledCounts(counts map[string]int, maxItems int) string {
	type countItem struct {
		Label string
		Count int
	}
	items := make([]countItem, 0, len(counts))
	for label, count := range counts {
		if strings.TrimSpace(label) == "" || count <= 0 {
			continue
		}
		items = append(items, countItem{Label: label, Count: count})
	}
	if len(items) == 0 {
		return ""
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Label < items[j].Label
		}
		return items[i].Count > items[j].Count
	})
	if maxItems > 0 && len(items) > maxItems {
		items = items[:maxItems]
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%s(%d)", item.Label, item.Count))
	}
	return strings.Join(parts, "，")
}

func int64MapToIntMap(values map[string]int64) map[string]int {
	result := make(map[string]int, len(values))
	for key, value := range values {
		if value <= 0 {
			continue
		}
		if value > int64(^uint(0)>>1) {
			result[key] = int(^uint(0) >> 1)
			continue
		}
		result[key] = int(value)
	}
	return result
}

func trimDisplayValue(value string, max int) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if max <= 0 || len(trimmed) <= max {
		return trimmed
	}
	if max <= 3 {
		return trimmed[:max]
	}
	return trimmed[:max-3] + "..."
}

func normalizeNginxRequestPart(value string, max int) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	decoded := decodeEscapedLogBytes(trimmed)
	if len(decoded) == 0 {
		return ""
	}
	if isLikelyBinaryRequestPart(decoded, trimmed) {
		return trimDisplayValue(summarizeBinaryPayload(decoded, 32), max)
	}
	normalized := strings.TrimSpace(sanitizeDisplayBytes(decoded))
	if normalized == "" {
		return ""
	}
	return trimDisplayValue(normalized, max)
}

func isLikelyBinaryRequestPart(decoded []byte, original string) bool {
	if isLikelyBinaryPayload(decoded) {
		return true
	}
	if !strings.Contains(original, `\x`) {
		return false
	}
	for _, b := range decoded {
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 0x20 || b == 0x7f {
			return true
		}
	}
	return false
}

func scanEvidenceFiles(
	ctx context.Context,
	source string,
	files []logFileCandidate,
	parser evidenceParser,
	matcher evidenceMatcher,
	startTS int64,
	endTS int64,
	referenceTS int64,
	limit int,
	maxLinesPerFile int,
) ([]model.LogEvidence, error) {
	if len(files) == 0 {
		return nil, nil
	}
	collected := make([]model.LogEvidence, 0)
	seen := make(map[string]struct{})
	reference := time.Unix(referenceTS, 0).In(time.Local)

	for _, file := range files {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		reader, err := openMaybeGzip(file.Path)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(reader)
		buffer := make([]byte, 0, 64*1024)
		scanner.Buffer(buffer, 1024*1024)
		linesRead := 0
		overEndStreak := 0
		for scanner.Scan() {
			if ctx.Err() != nil {
				_ = reader.Close()
				return nil, ctx.Err()
			}
			linesRead++
			if maxLinesPerFile > 0 && linesRead > maxLinesPerFile {
				break
			}
			evidence, ok := parser(source, scanner.Text(), reference)
			if !ok {
				continue
			}
			if evidence.EventTS > endTS {
				overEndStreak++
				if overEndStreak >= 200 {
					break
				}
				continue
			}
			overEndStreak = 0
			if evidence.EventTS < startTS || evidence.EventTS > endTS {
				continue
			}
			if matcher != nil && !matcher(evidence) {
				continue
			}
			if evidence.Fingerprint == "" {
				evidence.Fingerprint = evidenceFingerprint(evidence)
			}
			if _, exists := seen[evidence.Fingerprint]; exists {
				continue
			}
			seen[evidence.Fingerprint] = struct{}{}
			collected = append(collected, evidence)
			if limit > 0 && len(collected) >= limit {
				break
			}
		}
		if err := scanner.Err(); err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("scan evidence file %s: %w", file.Path, err)
		}
		_ = reader.Close()
		if limit > 0 && len(collected) >= limit {
			break
		}
	}
	return collected, nil
}

func openMaybeGzip(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(strings.ToLower(path), ".gz") {
		return file, nil
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &stackedCloser{Reader: reader, closers: []io.Closer{reader, file}}, nil
}

type stackedCloser struct {
	io.Reader
	closers []io.Closer
}

func (s *stackedCloser) Close() error {
	var firstErr error
	for _, closer := range s.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func parseNginxEvidenceLine(source string, line string, _ time.Time) (model.LogEvidence, bool) {
	match := nginxAccessPattern.FindStringSubmatch(line)
	if len(match) < 6 {
		return model.LogEvidence{}, false
	}

	clientIPs := extractIPs(match[1])
	if len(clientIPs) == 0 {
		return model.LogEvidence{}, false
	}
	eventTS, ok := parseNginxTimestamp(match[2])
	if !ok {
		return model.LogEvidence{}, false
	}

	requestParts := strings.Fields(match[3])
	if len(requestParts) < 2 {
		return model.LogEvidence{}, false
	}
	method := requestParts[0]
	host, path := extractHostAndPath(requestParts[1])
	if path == "" {
		path = "/"
	}

	statusValue, err := strconv.Atoi(match[4])
	if err != nil {
		return model.LogEvidence{}, false
	}
	status := statusValue

	evidence := model.LogEvidence{
		Source:   source,
		EventTS:  eventTS,
		ClientIP: clientIPs[0],
		TargetIP: "",
		Host:     host,
		Path:     path,
		Method:   method,
		Status:   &status,
		Message:  truncateMessage(line, 1024),
	}
	evidence.Fingerprint = evidenceFingerprint(evidence)
	return evidence, true
}

func parseSSEvidenceLine(source string, line string, reference time.Time) (model.LogEvidence, bool) {
	eventTS, ok := parseFlexibleTimestamp(line, reference)
	if !ok {
		return model.LogEvidence{}, false
	}

	clientIP := extractEndpointIP(ssClientPattern, line)
	if clientIP == "" {
		clientIP = normalizeIP(extractEndpointToken(ssFromPattern, line))
	}
	targetToken := extractEndpointToken(ssTargetPattern, line)
	method := ""

	if match := ssUDPCachePattern.FindStringSubmatch(line); len(match) >= 3 {
		if targetToken == "" {
			targetToken = match[1]
		}
		if clientIP == "" {
			clientIP = normalizeIP(match[2])
		}
		method = "udp-cache-miss"
	}

	if targetToken == "" {
		if match := ssConnectPattern.FindStringSubmatch(line); len(match) >= 2 {
			targetToken = match[1]
			method = "connect"
		}
	}

	host, targetPort, targetIP := parseSSTargetToken(targetToken)

	ips := extractIPs(line)
	if clientIP == "" && len(ips) > 0 {
		clientIP = ips[0]
	}
	if targetIP == "" && len(ips) > 0 {
		for i := len(ips) - 1; i >= 0; i-- {
			if !sameIP(ips[i], clientIP) {
				targetIP = ips[i]
				break
			}
		}
	}
	if method == "" {
		lower := strings.ToLower(line)
		switch {
		case strings.Contains(lower, "[udp]"):
			method = "udp"
		case strings.Contains(lower, "connect to"):
			method = "connect"
		}
	}
	if clientIP == "" && targetIP == "" && host == "" {
		return model.LogEvidence{}, false
	}

	path := ""
	if targetPort > 0 {
		path = strconv.Itoa(targetPort)
	}

	evidence := model.LogEvidence{
		Source:      source,
		EventTS:     eventTS,
		ClientIP:    clientIP,
		TargetIP:    targetIP,
		Host:        host,
		Path:        path,
		Method:      method,
		Status:      nil,
		Message:     truncateMessage(line, 512),
		Fingerprint: "",
	}
	evidence.Fingerprint = evidenceFingerprint(evidence)
	return evidence, true
}

func parseNginxTimestamp(value string) (int64, bool) {
	ts, err := time.Parse("02/Jan/2006:15:04:05 -0700", value)
	if err != nil {
		return 0, false
	}
	return ts.Unix(), true
}

func parseFlexibleTimestamp(line string, reference time.Time) (int64, bool) {
	if reference.IsZero() {
		reference = time.Now()
	}
	if start := strings.Index(line, "["); start >= 0 {
		if end := strings.Index(line[start:], "]"); end > 1 {
			raw := line[start+1 : start+end]
			if ts, ok := parseNginxTimestamp(raw); ok {
				return ts, true
			}
		}
	}
	if raw := rfc3339Pattern.FindString(line); raw != "" {
		if ts, err := time.Parse(time.RFC3339, raw); err == nil {
			return ts.Unix(), true
		}
		if len(raw) == 24 && strings.HasSuffix(raw, "0000") {
			patched := raw[:22] + ":" + raw[22:]
			if ts, err := time.Parse(time.RFC3339, patched); err == nil {
				return ts.Unix(), true
			}
		}
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
	}
	for _, layout := range layouts {
		if len(line) < len(layout) {
			continue
		}
		raw := line[:len(layout)]
		if ts, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			return ts.Unix(), true
		}
	}
	if ts, ok := parseSyslogTimestamp(line, reference); ok {
		return ts, true
	}
	return 0, false
}

func parseSyslogTimestamp(line string, now time.Time) (int64, bool) {
	raw := syslogTimePattern.FindString(line)
	if raw == "" {
		return 0, false
	}
	parsed, err := time.ParseInLocation("Jan 2 15:04:05", raw, now.Location())
	if err != nil {
		return 0, false
	}
	candidate := time.Date(now.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), 0, now.Location())
	if candidate.After(now.Add(36 * time.Hour)) {
		candidate = candidate.AddDate(-1, 0, 0)
	}
	return candidate.Unix(), true
}

func parseSSTargetToken(value string) (string, int, string) {
	trimmed := strings.Trim(value, "[]()\"' ,;")
	if trimmed == "" {
		return "", 0, ""
	}

	host := trimmed
	port := 0

	if splitHost, splitPort, err := net.SplitHostPort(trimmed); err == nil {
		host = splitHost
		if parsedPort, parseErr := strconv.Atoi(splitPort); parseErr == nil {
			port = parsedPort
		}
	} else {
		lastColon := strings.LastIndex(trimmed, ":")
		if lastColon > 0 && lastColon < len(trimmed)-1 {
			portText := trimmed[lastColon+1:]
			if isDigits(portText) {
				host = trimmed[:lastColon]
				if parsedPort, parseErr := strconv.Atoi(portText); parseErr == nil {
					port = parsedPort
				}
			}
		}
	}

	host = strings.Trim(host, "[]")
	ip := normalizeIP(host)
	if ip != "" {
		return "", port, ip
	}
	return host, port, ""
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func extractEndpointToken(pattern *regexp.Regexp, line string) string {
	match := pattern.FindStringSubmatch(line)
	if len(match) < 2 {
		return ""
	}
	return strings.Trim(match[1], "[]()\"' ,;")
}

func extractEndpointIP(pattern *regexp.Regexp, line string) string {
	return normalizeIP(extractEndpointToken(pattern, line))
}

func normalizeIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	value = strings.Trim(value, "[]")
	ip := net.ParseIP(value)
	if ip == nil {
		return ""
	}
	return ip.String()
}

func extractIPs(text string) []string {
	tokens := strings.Fields(text)
	if len(tokens) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	result := make([]string, 0, 4)
	for _, token := range tokens {
		for _, piece := range strings.Split(token, ",") {
			candidate := strings.Trim(piece, "[]()\"'; ")
			if candidate == "" || candidate == "-" {
				continue
			}
			if host, _, err := net.SplitHostPort(candidate); err == nil {
				candidate = host
			}
			ip := net.ParseIP(strings.Trim(candidate, "[]"))
			if ip == nil {
				continue
			}
			normalized := ip.String()
			if _, exists := seen[normalized]; exists {
				continue
			}
			seen[normalized] = struct{}{}
			result = append(result, normalized)
		}
	}
	return result
}

func filterEvidence(rows []model.LogEvidence, matcher evidenceMatcher) []model.LogEvidence {
	if matcher == nil || len(rows) == 0 {
		return rows
	}
	filtered := make([]model.LogEvidence, 0, len(rows))
	for _, row := range rows {
		if matcher(row) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func sortAndTrimEvidence(rows []model.LogEvidence, bucketTS int64, limit int) []model.LogEvidence {
	if len(rows) == 0 {
		return rows
	}
	sort.Slice(rows, func(i, j int) bool {
		di := absInt64(rows[i].EventTS - bucketTS)
		dj := absInt64(rows[j].EventTS - bucketTS)
		if di == dj {
			return rows[i].EventTS > rows[j].EventTS
		}
		return di < dj
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func isNginxLogFileName(lowerName string) bool {
	if !strings.Contains(lowerName, ".log") {
		return false
	}
	keywords := []string{"nginx", "access", "openresty", "apache", "httpd", "caddy", "web"}
	for _, keyword := range keywords {
		if strings.Contains(lowerName, keyword) {
			return true
		}
	}
	return false
}

func isGenericLogFileName(lowerName string) bool {
	if !strings.Contains(lowerName, ".log") {
		return false
	}
	return true
}

func isShadowsocksLogFileName(lowerName string) bool {
	if !strings.Contains(lowerName, ".log") {
		return false
	}
	keywords := []string{"ss", "shadowsocks", "outline", "v2ray", "xray", "singbox", "hysteria"}
	for _, keyword := range keywords {
		if strings.Contains(lowerName, keyword) {
			return true
		}
	}
	return false
}

func evidenceFingerprint(evidence model.LogEvidence) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf(
		"%s|%d|%s|%s|%s|%s|%s|%v|%s",
		evidence.Source,
		evidence.EventTS,
		evidence.ClientIP,
		evidence.TargetIP,
		evidence.Host,
		evidence.Path,
		evidence.Method,
		statusValue(evidence.Status),
		evidence.Message,
	)))
	return fmt.Sprintf("%x", h.Sum64())
}

func statusValue(status *int) int {
	if status == nil {
		return -1
	}
	return *status
}

func processLabel(comm, exe string) string {
	comm = strings.TrimSpace(comm)
	exe = executableName(exe)
	if comm == "" && exe == "" {
		return "unknown"
	}
	if comm == "" {
		return exe
	}
	if exe == "" {
		return comm
	}
	return fmt.Sprintf("%s (%s)", comm, exe)
}

func executableName(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	token := strings.Fields(trimmed)[0]
	token = strings.Trim(token, `"'`)
	if token == "" {
		return ""
	}
	normalized := strings.ReplaceAll(token, "\\", "/")
	normalized = strings.TrimRight(normalized, "/")
	if normalized == "" {
		return token
	}
	idx := strings.LastIndex(normalized, "/")
	if idx < 0 || idx == len(normalized)-1 {
		return normalized
	}
	return normalized[idx+1:]
}

func topIPsByWeight(weights map[string]int64, maxCount int) []string {
	if maxCount <= 0 || len(weights) == 0 {
		return nil
	}
	items := make([]ipWeight, 0, len(weights))
	for ip, bytes := range weights {
		if ip == "" {
			continue
		}
		items = append(items, ipWeight{IP: ip, Bytes: bytes})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Bytes == items[j].Bytes {
			return items[i].IP < items[j].IP
		}
		return items[i].Bytes > items[j].Bytes
	})
	if len(items) > maxCount {
		items = items[:maxCount]
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, item.IP)
	}
	return result
}

func topIPsByCount(weights map[string]int64, maxCount int) []string {
	return topIPsByWeight(weights, maxCount)
}

func summarizePeers(peers map[string]peerAgg, maxCount int) []usageExplainPeer {
	if len(peers) == 0 {
		return nil
	}
	items := make([]peerAgg, 0, len(peers))
	for _, item := range peers {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].BytesTotal == items[j].BytesTotal {
			if items[i].FlowCount == items[j].FlowCount {
				return items[i].RemoteIP < items[j].RemoteIP
			}
			return items[i].FlowCount > items[j].FlowCount
		}
		return items[i].BytesTotal > items[j].BytesTotal
	})
	if maxCount > 0 && len(items) > maxCount {
		items = items[:maxCount]
	}
	result := make([]usageExplainPeer, 0, len(items))
	for _, item := range items {
		result = append(result, usageExplainPeer{
			Direction:  item.Direction,
			RemoteIP:   item.RemoteIP,
			RemotePort: nullablePort(item.RemotePort),
			LocalPort:  nullablePort(item.LocalPort),
			BytesTotal: item.BytesTotal,
			FlowCount:  item.FlowCount,
		})
	}
	return result
}

func nullablePort(port int) *int {
	if port <= 0 {
		return nil
	}
	result := port
	return &result
}

func inferConfidence(response usageExplainResponse) string {
	if len(response.NginxRequests) > 0 {
		return "high"
	}
	if len(response.SourceIPs) > 0 && len(response.TargetIPs) > 0 {
		return "high"
	}
	if len(response.SourceIPs) > 0 || len(response.TargetIPs) > 0 {
		return "medium"
	}
	return "low"
}

func isShadowsocksProcess(comm, exe string) bool {
	text := strings.ToLower(comm + " " + exe)
	return strings.Contains(text, "ss-") || strings.Contains(text, "shadowsocks")
}

func isNginxProcess(comm, exe string) bool {
	text := strings.ToLower(comm + " " + exe)
	return strings.Contains(text, "nginx") || strings.Contains(text, "openresty") || strings.Contains(text, "apache") || strings.Contains(text, "caddy")
}

func extractHostAndPath(target string) (string, string) {
	if target == "" {
		return "", "/"
	}
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		parsed, err := url.Parse(target)
		if err == nil {
			path := parsed.EscapedPath()
			if path == "" {
				path = "/"
			}
			if parsed.RawQuery != "" {
				path += "?" + parsed.RawQuery
			}
			return parsed.Hostname(), path
		}
	}
	if strings.HasPrefix(target, "/") {
		return "", target
	}
	return "", target
}

func resolvedLogDir(value string, fallback string) string {
	resolved := strings.TrimSpace(value)
	if resolved == "" {
		return fallback
	}
	return resolved
}

func appendNoteUnique(notes *[]string, text string) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return
	}
	for _, existing := range *notes {
		if existing == trimmed {
			return
		}
	}
	*notes = append(*notes, trimmed)
}

func mergeTopIPs(existing []string, incoming []string, maxCount int) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(existing)+len(incoming))
	for _, ip := range existing {
		if ip == "" {
			continue
		}
		if _, exists := seen[ip]; exists {
			continue
		}
		seen[ip] = struct{}{}
		result = append(result, ip)
	}
	for _, ip := range incoming {
		if ip == "" {
			continue
		}
		if _, exists := seen[ip]; exists {
			continue
		}
		seen[ip] = struct{}{}
		result = append(result, ip)
	}
	if maxCount > 0 && len(result) > maxCount {
		return result[:maxCount]
	}
	return result
}

func (s *Server) addCurrentTupleNote(response *usageExplainResponse, query usageExplainQuery) {
	if query.Direction == model.DirectionIn && query.RemoteIP != "" {
		appendNoteUnique(&response.Notes, fmt.Sprintf("当前连接来源 IP：%s", query.RemoteIP))
	}
	if query.Direction == model.DirectionOut && query.RemoteIP != "" {
		appendNoteUnique(&response.Notes, fmt.Sprintf("当前连接目标 IP：%s", query.RemoteIP))
	}
}

func fallbackText(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func truncateMessage(value string, max int) string {
	normalized := normalizeLogMessageForDisplay(value)
	if max <= 0 || len(normalized) <= max {
		return normalized
	}
	if max <= 3 {
		return normalized[:max]
	}
	return normalized[:max-3] + "..."
}

func normalizeLogMessageForDisplay(value string) string {
	decoded := decodeEscapedLogBytes(value)
	if len(decoded) == 0 {
		return ""
	}
	if isLikelyBinaryPayload(decoded) {
		return summarizeBinaryPayload(decoded, 48)
	}
	return sanitizeDisplayBytes(decoded)
}

func decodeEscapedLogBytes(value string) []byte {
	src := []byte(value)
	out := make([]byte, 0, len(src))

	for i := 0; i < len(src); i++ {
		if src[i] != '\\' || i+1 >= len(src) {
			out = append(out, src[i])
			continue
		}

		next := src[i+1]
		switch next {
		case 'x', 'X':
			if i+3 < len(src) {
				hi, okHi := fromHexChar(src[i+2])
				lo, okLo := fromHexChar(src[i+3])
				if okHi && okLo {
					out = append(out, hi<<4|lo)
					i += 3
					continue
				}
			}
		case 'n':
			out = append(out, '\n')
			i++
			continue
		case 'r':
			out = append(out, '\r')
			i++
			continue
		case 't':
			out = append(out, '\t')
			i++
			continue
		case '\\':
			out = append(out, '\\')
			i++
			continue
		case '"':
			out = append(out, '"')
			i++
			continue
		case '\'':
			out = append(out, '\'')
			i++
			continue
		}

		out = append(out, src[i])
	}

	return out
}

func fromHexChar(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func isLikelyBinaryPayload(data []byte) bool {
	if len(data) < 8 {
		return false
	}
	controlCount := 0
	for _, b := range data {
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 0x20 || b == 0x7f {
			controlCount++
		}
	}
	return controlCount*100/len(data) >= 20
}

func summarizeBinaryPayload(data []byte, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = 32
	}
	shown := len(data)
	if shown > maxBytes {
		shown = maxBytes
	}
	parts := make([]string, 0, shown)
	for i := 0; i < shown; i++ {
		parts = append(parts, fmt.Sprintf("%02X", data[i]))
	}
	summary := strings.Join(parts, " ")
	if shown < len(data) {
		summary += " ..."
	}
	return fmt.Sprintf("[binary %dB] %s", len(data), summary)
}

func sanitizeDisplayBytes(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(data))
	for _, b := range data {
		switch b {
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		default:
			if b < 0x20 || b == 0x7f {
				builder.WriteString(fmt.Sprintf("\\x%02X", b))
				continue
			}
			builder.WriteByte(b)
		}
	}
	return builder.String()
}

func sameIP(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return false
	}
	if left == right {
		return true
	}
	leftIP := net.ParseIP(left)
	rightIP := net.ParseIP(right)
	if leftIP == nil || rightIP == nil {
		return false
	}
	return leftIP.Equal(rightIP)
}

func isLoopbackIP(value string) bool {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
