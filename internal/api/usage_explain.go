package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

const (
	usageExplainWindowPadding = int64(60)
	logWindowStrict           = int64(120)
	logWindowFallback         = int64(900)
	explainLogScanBudget      = 1500 * time.Millisecond
	maxRelatedPeers           = 8
	maxNginxRequests          = 0
	maxEvidenceRows           = 128
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
	bracketPortPattern = regexp.MustCompile(`\[(\d{1,5})\]`)
	rfc3339Pattern     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})`)
	syslogTimePattern  = regexp.MustCompile(`^[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}`)
)

type usageExplainQuery struct {
	TimeBucket int64
	DataSource string
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
	Chains        []usageExplainChain        `json:"chains"`
	RelatedPeers  []usageExplainPeer         `json:"related_peers"`
	NginxRequests []usageExplainNginxRequest `json:"nginx_requests"`
	Notes         []string                   `json:"notes"`
	StrongMatch   bool                       `json:"-"`
}

type usageExplainOptions struct {
	allowFileScan bool
}

type usageExplainChain struct {
	ChainID              string `json:"chain_id,omitempty"`
	SourceIP             string `json:"source_ip,omitempty"`
	TargetIP             string `json:"target_ip,omitempty"`
	TargetHost           string `json:"target_host,omitempty"`
	TargetHostNormalized string `json:"target_host_normalized,omitempty"`
	TargetPort           *int   `json:"target_port,omitempty"`
	LocalPort            *int   `json:"local_port,omitempty"`
	BytesTotal           int64  `json:"bytes_total"`
	FlowCount            int64  `json:"flow_count"`
	EvidenceCount        int    `json:"evidence_count"`
	Evidence             string `json:"evidence"`
	EvidenceSource       string `json:"evidence_source,omitempty"`
	SampleFingerprint    string `json:"sample_fingerprint,omitempty"`
	SampleMessage        string `json:"sample_message,omitempty"`
	SampleTime           int64  `json:"sample_time,omitempty"`
	Confidence           string `json:"confidence"`
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
	Time              int64  `json:"time"`
	Method            string `json:"method"`
	Host              string `json:"host,omitempty"`
	HostNormalized    string `json:"host_normalized,omitempty"`
	Path              string `json:"path"`
	Status            int    `json:"status"`
	Count             int    `json:"count"`
	ClientIP          string `json:"client_ip,omitempty"`
	Referer           string `json:"referer,omitempty"`
	UserAgent         string `json:"user_agent,omitempty"`
	Bot               string `json:"bot,omitempty"`
	SampleFingerprint string `json:"sample_fingerprint,omitempty"`
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

type chainAgg struct {
	ChainID              string
	SourceIP             string
	TargetIP             string
	TargetHost           string
	TargetHostNormalized string
	TargetPort           int
	LocalPort            int
	BytesTotal           int64
	FlowCount            int64
	EvidenceCount        int
	Evidence             string
	EvidenceSource       string
	SampleFingerprint    string
	SampleMessage        string
	SampleTime           int64
	Confidence           string
}

type entrySourceCandidate struct {
	RemoteIP   string
	BytesTotal int64
	FlowCount  int64
}

type evidenceMatcher = evidence.Matcher

type evidenceParser = evidence.Parser

type evidenceQueryHints = evidence.QueryHints

func (s *Server) handleUsageExplain(w http.ResponseWriter, r *http.Request) {
	query, err := parseUsageExplainQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_usage_explain_query", err)
		return
	}

	allowFileScan := r.URL.Query().Has("scan") && parseBoolFlag(r.URL.Query().Get("scan"))
	data, err := s.analyzeUsageExplain(r.Context(), query, allowFileScan)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err)
		return
	}

	writeJSON(w, http.StatusOK, envelope{"data": data})
}

func parseUsageExplainQuery(r *http.Request) (usageExplainQuery, error) {
	query := usageExplainQuery{
		TimeBucket: parseInt64WithDefault(r.URL.Query().Get("ts"), 0),
		DataSource: strings.TrimSpace(r.URL.Query().Get("data_source")),
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

func (s *Server) analyzeUsageExplain(ctx context.Context, query usageExplainQuery, allowFileScan bool) (usageExplainResponse, error) {
	return s.analyzeUsageExplainWithOptions(ctx, query, usageExplainOptions{allowFileScan: allowFileScan})
}

func (s *Server) analyzeUsageExplainWithOptions(ctx context.Context, query usageExplainQuery, options usageExplainOptions) (usageExplainResponse, error) {
	bucketTS := time.Unix(query.TimeBucket, 0).UTC().Truncate(time.Minute).Unix()

	response := usageExplainResponse{
		Process:       processLabel(query.Comm, query.Exe),
		SourceIPs:     make([]string, 0),
		TargetIPs:     make([]string, 0),
		Chains:        make([]usageExplainChain, 0),
		RelatedPeers:  make([]usageExplainPeer, 0),
		NginxRequests: make([]usageExplainNginxRequest, 0),
		Notes:         make([]string, 0),
	}
	allowFileScan := options.allowFileScan
	if query.DataSource == store.DataSourceHour {
		allowFileScan = false
		appendNoteUnique(&response.Notes, "当前展示的是小时聚合数据，仅回放已持久化链路和已缓存日志线索。")
	}
	allowCachedEvidence := allowFileScan || strings.TrimSpace(query.DataSource) != ""

	related := make([]model.UsageRecord, 0)
	if query.DataSource != store.DataSourceHour {
		var err error
		related, err = s.collectRelatedUsage(ctx, query, bucketTS)
		if err != nil {
			return usageExplainResponse{}, err
		}
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
		processName := processIdentityKey(query.Comm, query.Exe)
		if processName == "" {
			processName = "ss-server"
		}
		lookupName, logDir, ok := s.lookupConfiguredProcessLogDir(query.Comm, query.Exe)
		if ok {
			if lookupName != "" {
				processName = lookupName
			}
			logDirs := s.lookupConfiguredProcessLogDirs(query.Comm, query.Exe)
			if len(logDirs) == 0 && strings.TrimSpace(logDir) != "" {
				logDirs = []string{logDir}
			}
			if allowCachedEvidence {
				s.addCurrentTupleNote(&response, query)
				if err := s.enrichShadowsocksFromLogs(ctx, &response, query, bucketTS, logDirs, related, allowFileScan); err != nil {
					appendNoteUnique(&response.Notes, fmt.Sprintf("SS 日志检索失败：%v", err))
				}
			} else {
				appendNoteUnique(&response.Notes, "未提供 data_source，已跳过日志缓存回放；如需即时文件扫描请显式传入 scan=1。")
			}
		} else {
			appendNoteUnique(&response.Notes, fmt.Sprintf("进程 %s 未配置日志路径，已跳过日志检索。", processName))
		}
		appendNoteUnique(&response.Notes, "Shadowsocks 使用加密与复用，日志关联是候选推断，不保证来源与目标一一对应。")
	} else if isNginxProcess(query.Comm, query.Exe) {
		processName := processIdentityKey(query.Comm, query.Exe)
		if processName == "" {
			processName = "nginx"
		}
		lookupName, logDir, ok := s.lookupConfiguredProcessLogDir(query.Comm, query.Exe)
		if ok {
			if lookupName != "" {
				processName = lookupName
			}
			if allowCachedEvidence {
				s.addCurrentTupleNote(&response, query)
				if err := s.enrichNginxFromLogs(ctx, &response, query.RemoteIP, bucketTS, logDir, allowFileScan); err != nil {
					appendNoteUnique(&response.Notes, fmt.Sprintf("Nginx 日志检索失败：%v", err))
				}
			} else {
				appendNoteUnique(&response.Notes, "未提供 data_source，已跳过日志缓存回放；如需即时文件扫描请显式传入 scan=1。")
			}
		} else {
			appendNoteUnique(&response.Notes, fmt.Sprintf("进程 %s 未配置日志路径，已跳过日志检索。", processName))
		}
		appendNoteUnique(&response.Notes, "HTTP/HTTPS 网页路径优先来自 access.log 关联；仅靠 conntrack 无法直接提取 URI。")
	} else {
		processName := processIdentityKey(query.Comm, query.Exe)
		if processName != "" {
			lookupName, logDir, ok := s.lookupConfiguredProcessLogDir(query.Comm, query.Exe)
			if ok {
				if lookupName != "" {
					processName = lookupName
				}
				if allowCachedEvidence {
					s.addCurrentTupleNote(&response, query)
					if err := s.enrichGenericProcessFromLogs(ctx, &response, query, bucketTS, related, processName, logDir, allowFileScan); err != nil {
						appendNoteUnique(&response.Notes, fmt.Sprintf("%s 日志检索失败：%v", processName, err))
					}
				} else {
					appendNoteUnique(&response.Notes, "未提供 data_source，已跳过日志缓存回放；如需即时文件扫描请显式传入 scan=1。")
				}
			} else {
				appendNoteUnique(&response.Notes, fmt.Sprintf("进程 %s 未配置日志路径，已跳过日志检索。", processName))
			}
		}
	}

	response.Chains = assignCanonicalChainIDs(bucketTS, query, response.Chains)
	storedChains, err := s.loadPersistedChains(ctx, bucketTS, query)
	if err != nil {
		return usageExplainResponse{}, fmt.Errorf("load persisted chains: %w", err)
	}
	if len(storedChains) > 0 {
		hadChains := len(response.Chains)
		response.Chains = mergeExplainChains(response.Chains, storedChains, 0)
		response.SourceIPs = mergeTopIPs(response.SourceIPs, chainIPs(storedChains, true), 6)
		response.TargetIPs = mergeTopIPs(response.TargetIPs, chainIPs(storedChains, false), 6)
		if hadChains == 0 && len(response.Chains) > 0 {
			appendNoteUnique(&response.Notes, fmt.Sprintf("已回放 %d 条已记录链路。", len(storedChains)))
		}
	}

	if len(response.SourceIPs) == 0 && len(response.TargetIPs) == 0 {
		appendNoteUnique(&response.Notes, "没有找到足够的同进程关联流量，建议放宽筛选条件或扩大时间范围后重试。")
	}

	response.Chains = assignCanonicalChainIDs(bucketTS, query, response.Chains)
	response.SourceIPs = mergeTopIPs(response.SourceIPs, chainIPs(response.Chains, true), 6)
	response.TargetIPs = mergeTopIPs(response.TargetIPs, chainIPs(response.Chains, false), 6)
	if err := s.persistCanonicalChains(ctx, bucketTS, query, response.Chains); err != nil {
		return usageExplainResponse{}, fmt.Errorf("persist canonical chains: %w", err)
	}

	response.Confidence = inferConfidence(response)
	return response, nil
}

func (s *Server) collectRelatedUsage(ctx context.Context, query usageExplainQuery, bucketTS int64) ([]model.UsageRecord, error) {
	baseQuery := model.UsageQuery{
		Start:     time.Unix(bucketTS-usageExplainWindowPadding, 0).UTC(),
		End:       time.Unix(bucketTS+usageExplainWindowPadding+60, 0).UTC(),
		Proto:     query.Proto,
		UsePage:   true,
		Page:      1,
		PageSize:  120,
		SortBy:    "bytes_total",
		SortOrder: "desc",
	}

	hasIdentity := false
	if query.PID != nil && *query.PID > 0 {
		baseQuery.PID = query.PID
		hasIdentity = true
	}
	if query.Comm != "" {
		baseQuery.Comm = query.Comm
		hasIdentity = true
	}
	if query.Exe != "" {
		baseQuery.Exe = query.Exe
		hasIdentity = true
	}
	if !hasIdentity {
		currentQuery := baseQuery
		currentQuery.RemoteIP = query.RemoteIP
		currentQuery.Direction = query.Direction
		currentQuery.LocalPort = query.LocalPort
		currentQuery.PageSize = 200

		currentRecords, _, _, err := s.store.QueryUsage(ctx, currentQuery, store.DataSourceMinute)
		if err != nil {
			return nil, fmt.Errorf("query related usage: %w", err)
		}

		oppositeQuery := baseQuery
		oppositeQuery.Direction = oppositeDirection(query.Direction)
		oppositeQuery.PageSize = 200

		counterpartRows, _, _, err := s.store.QueryUsage(ctx, oppositeQuery, store.DataSourceMinute)
		if err != nil {
			return nil, fmt.Errorf("query counterpart usage without identity: %w", err)
		}

		anchorBytes := int64(0)
		for _, row := range currentRecords {
			anchorBytes += usageBytesTotal(row)
		}

		result := make([]model.UsageRecord, 0, len(currentRecords)+6)
		result = append(result, currentRecords...)
		result = append(result, selectCounterpartUsage(query, bucketTS, anchorBytes, counterpartRows, 6)...)
		return dedupeUsageRecords(result), nil
	}

	currentQuery := baseQuery
	currentQuery.Direction = query.Direction
	currentQuery.RemoteIP = query.RemoteIP
	currentQuery.LocalPort = query.LocalPort
	currentQuery.PageSize = 200

	currentRecords, _, _, err := s.store.QueryUsage(ctx, currentQuery, store.DataSourceMinute)
	if err != nil {
		return nil, fmt.Errorf("query anchored usage: %w", err)
	}

	oppositeQuery := baseQuery
	oppositeQuery.Direction = oppositeDirection(query.Direction)
	oppositeQuery.PageSize = 200

	counterpartRows, _, _, err := s.store.QueryUsage(ctx, oppositeQuery, store.DataSourceMinute)
	if err != nil {
		return nil, fmt.Errorf("query counterpart usage: %w", err)
	}

	anchorBytes := int64(0)
	for _, row := range currentRecords {
		anchorBytes += usageBytesTotal(row)
	}

	result := make([]model.UsageRecord, 0, len(currentRecords)+6)
	result = append(result, currentRecords...)
	result = append(result, selectCounterpartUsage(query, bucketTS, anchorBytes, counterpartRows, 6)...)
	return dedupeUsageRecords(result), nil
}

func (s *Server) enrichNginxFromLogs(ctx context.Context, response *usageExplainResponse, clientIP string, bucketTS int64, logDir string, allowFileScan bool) error {
	if strings.TrimSpace(clientIP) == "" {
		return nil
	}
	exactMatcher := func(ev model.LogEvidence) bool {
		return sameIP(ev.ClientIP, clientIP)
	}
	rows, note, err := s.lookupOrScanEvidence(
		ctx,
		evidenceSourceNginx,
		logDir,
		bucketTS,
		maxEvidenceRows,
		allowFileScan,
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
			logDir,
			bucketTS,
			maxEvidenceRows,
			allowFileScan,
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
		if !allowFileScan {
			appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 的日志缓存未命中，已跳过同步文件扫描，等待后台预热后重试。", resolvedLogDir(logDir, "")))
			return nil
		}
		appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 的日志中未匹配到该来源 IP。", resolvedLogDir(logDir, "")))
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
	} else if len(response.NginxRequests) > 0 {
		response.StrongMatch = true
	}
	appendNoteUnique(&response.Notes, fmt.Sprintf("Nginx 日志命中 %d 条，聚合为 %d 组。", len(rows), len(response.NginxRequests)))
	appendNginxStatusAndAgentNotes(&response.Notes, rows, response.NginxRequests)
	return nil
}

func (s *Server) enrichShadowsocksFromLogs(ctx context.Context, response *usageExplainResponse, query usageExplainQuery, bucketTS int64, logDirs []string, related []model.UsageRecord, allowFileScan bool) error {
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
				if evidence.AbsInt64(ev.EventTS-bucketTS) > logWindowStrict {
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

	rows, notes, err := s.lookupOrScanEvidenceAcrossSourcesAndDirs(
		ctx,
		shadowsocksEvidenceSources(),
		logDirs,
		bucketTS,
		maxEvidenceRows,
		allowFileScan,
		matcher,
		evidenceQueryHints{AnyIP: remoteIP, TargetPort: remotePort},
		isShadowsocksLogFileName,
		parseSSEvidenceLine,
	)
	if err != nil {
		return err
	}
	for _, note := range notes {
		appendNoteUnique(&response.Notes, note)
	}
	configuredDir := formatResolvedLogDirs(logDirs)
	if len(rows) == 0 {
		if !allowFileScan {
			appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 的日志缓存未命中，已跳过同步文件扫描，等待后台预热后重试。", configuredDir))
			return nil
		}
		appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 中未命中 SS 相关日志。", configuredDir))
		return nil
	}

	sourceWeights := make(map[string]int64)
	targetWeights := make(map[string]int64)
	hostCounts := make(map[string]int)
	sourceConfirmed := make(map[string]int)
	chainMap := make(map[string]chainAgg)
	entryPorts := make([]int, 0, len(rows))
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
		if row.EntryPort > 0 {
			entryPorts = append(entryPorts, row.EntryPort)
		}
		addExplainChain(chainMap, row, query, "ss-log")
	}
	sourceRows, sourceNotes, err := s.lookupShadowsocksSourceEvidenceByEntryPort(ctx, bucketTS, logDirs, entryPorts, allowFileScan)
	if err != nil {
		return err
	}
	for _, note := range sourceNotes {
		appendNoteUnique(&response.Notes, note)
	}
	if len(sourceRows) > 0 {
		for _, row := range sourceRows {
			if row.ClientIP == "" || isLoopbackIP(row.ClientIP) {
				continue
			}
			sourceWeights[row.ClientIP]++
		}
		hydrateChainsFromEntryPortCandidates(chainMap, buildEntryPortSourceCandidatesFromEvidence(sourceRows))
		promoteConfirmedSourcesFromEvidence(sourceConfirmed, sourceRows)
	}
	entrySources, err := s.collectEntryPortSourceCandidates(ctx, bucketTS, query.Proto, entryPorts)
	if err != nil {
		return err
	}
	hydrateChainsFromEntryPortCandidates(chainMap, entrySources)
	mergeChainUsageMetrics(chainMap, query, related)

	response.SourceIPs = mergeTopIPs(response.SourceIPs, topIPsByCount(sourceWeights, 4), 6)
	response.TargetIPs = mergeTopIPs(response.TargetIPs, topIPsByCount(targetWeights, 4), 6)
	response.Chains = mergeExplainChains(response.Chains, summarizeChains(chainMap, 0), 0)
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
	if len(sourceRows) > 0 {
		appendNoteUnique(&response.Notes, fmt.Sprintf("SS/obfs 入口日志命中 %d 条来源候选。", len(sourceRows)))
	}
	for i := 0; i < len(rows) && i < 2; i++ {
		if rows[i].Message != "" {
			appendNoteUnique(&response.Notes, fmt.Sprintf("SS 样本：%s", rows[i].Message))
		}
	}
	return nil
}

func (s *Server) enrichGenericProcessFromLogs(
	ctx context.Context,
	response *usageExplainResponse,
	query usageExplainQuery,
	bucketTS int64,
	related []model.UsageRecord,
	processName string,
	logDir string,
	allowFileScan bool,
) error {
	remoteIP := strings.TrimSpace(query.RemoteIP)
	remotePort := 0
	if query.RemotePort != nil {
		remotePort = *query.RemotePort
	}
	if remoteIP == "" && remotePort <= 0 {
		return nil
	}

	matcher := func(ev model.LogEvidence) bool {
		if remoteIP != "" {
			if sameIP(ev.ClientIP, remoteIP) || sameIP(ev.TargetIP, remoteIP) {
				return true
			}
			if messageHasExactIP(ev.Message, remoteIP) {
				return true
			}
		}
		if remotePort > 0 {
			if ev.TargetPort == remotePort {
				return true
			}
			if port, ok := parseEvidencePort(ev.Path); ok && port == remotePort {
				return true
			}
		}
		return false
	}

	source := customEvidenceSource(processName)
	rows, note, err := s.lookupOrScanEvidence(
		ctx,
		source,
		logDir,
		bucketTS,
		maxEvidenceRows,
		allowFileScan,
		matcher,
		evidenceQueryHints{AnyIP: remoteIP, TargetPort: remotePort},
		isGenericLogFileName,
		parseGenericEvidenceLine,
	)
	if err != nil {
		return err
	}
	if note != "" {
		appendNoteUnique(&response.Notes, note)
	}
	if len(rows) == 0 {
		if !allowFileScan {
			appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 的日志缓存未命中，已跳过同步文件扫描，等待后台预热后重试。", resolvedLogDir(logDir, "")))
			return nil
		}
		appendNoteUnique(&response.Notes, fmt.Sprintf("路径 %s 中未命中 %s 相关日志。", resolvedLogDir(logDir, ""), processName))
		return nil
	}

	sourceWeights := make(map[string]int64)
	targetWeights := make(map[string]int64)
	chainMap := make(map[string]chainAgg)
	for _, row := range rows {
		if row.ClientIP != "" {
			sourceWeights[row.ClientIP]++
		}
		if row.TargetIP != "" {
			targetWeights[row.TargetIP]++
		}
		addExplainChain(chainMap, row, query, processEvidenceLabel(processName))
	}
	mergeChainUsageMetrics(chainMap, query, related)

	response.SourceIPs = mergeTopIPs(response.SourceIPs, topIPsByCount(sourceWeights, 4), 6)
	response.TargetIPs = mergeTopIPs(response.TargetIPs, topIPsByCount(targetWeights, 4), 6)
	response.Chains = mergeExplainChains(response.Chains, summarizeChains(chainMap, 0), 0)

	appendNoteUnique(&response.Notes, fmt.Sprintf("%s 日志命中 %d 条。", processName, len(rows)))
	for i := 0; i < len(rows) && i < 2; i++ {
		if rows[i].Message != "" {
			appendNoteUnique(&response.Notes, fmt.Sprintf("%s 样本：%s", processName, rows[i].Message))
		}
	}
	return nil
}

func customEvidenceSource(processName string) string {
	name := strings.TrimSpace(processName)
	if name == "" {
		return "proc"
	}
	return fmt.Sprintf("proc:%s", name)
}

func parseEvidencePort(value string) (int, bool) {
	return evidence.ParsePort(value)
}

func (s *Server) lookupOrScanEvidence(
	ctx context.Context,
	source string,
	logDir string,
	bucketTS int64,
	limit int,
	allowFileScan bool,
	matcher evidenceMatcher,
	queryHints evidenceQueryHints,
	fileNameMatcher func(string) bool,
	parser evidenceParser,
) ([]model.LogEvidence, string, error) {
	return evidence.LookupOrScan(ctx, s.store, evidence.SearchOptions{
		Source:               source,
		LogDir:               logDir,
		BucketTS:             bucketTS,
		Limit:                limit,
		QueryHints:           evidence.QueryHints(queryHints),
		FileNameMatcher:      fileNameMatcher,
		Parser:               evidence.Parser(parser),
		Matcher:              evidence.Matcher(matcher),
		StrictWindow:         logWindowStrict,
		FallbackWindow:       logWindowFallback,
		ScanBudget:           explainLogScanBudget,
		MaxScanFilesStrict:   maxScanFilesStrict,
		MaxScanFilesFallback: maxScanFilesFallback,
		MaxScanLinesPerFile:  maxScanLinesPerFile,
		CacheOnly:            !allowFileScan,
	})
}

func (s *Server) lookupOrScanEvidenceAcrossDirs(
	ctx context.Context,
	source string,
	logDirs []string,
	bucketTS int64,
	limit int,
	allowFileScan bool,
	matcher evidenceMatcher,
	queryHints evidenceQueryHints,
	fileNameMatcher func(string) bool,
	parser evidenceParser,
) ([]model.LogEvidence, []string, error) {
	uniqueDirs := uniqueNonEmptyStrings(logDirs)
	if len(uniqueDirs) == 0 {
		return nil, nil, nil
	}

	rows := make([]model.LogEvidence, 0, limit)
	notes := make([]string, 0, len(uniqueDirs))
	for _, logDir := range uniqueDirs {
		scannedRows, note, err := s.lookupOrScanEvidence(
			ctx,
			source,
			logDir,
			bucketTS,
			limit,
			allowFileScan,
			matcher,
			queryHints,
			fileNameMatcher,
			parser,
		)
		if err != nil {
			return nil, nil, err
		}
		if note != "" {
			appendUniqueString(&notes, note)
		}
		rows = appendDedupEvidenceRows(rows, scannedRows)
		if len(rows) > 0 {
			break
		}
	}
	return evidence.SortAndTrim(rows, bucketTS, limit), notes, nil
}

func shadowsocksEvidenceSources() []string {
	sources := make([]string, 0, 1+len(shadowsocksFamilyLookupKeys()))
	sources = append(sources, evidenceSourceSS)
	for _, key := range shadowsocksFamilyLookupKeys() {
		sources = append(sources, customEvidenceSource(key))
	}
	return uniqueNonEmptyStrings(sources)
}

func (s *Server) lookupOrScanEvidenceAcrossSourcesAndDirs(
	ctx context.Context,
	sources []string,
	logDirs []string,
	bucketTS int64,
	limit int,
	allowFileScan bool,
	matcher evidenceMatcher,
	queryHints evidenceQueryHints,
	fileNameMatcher func(string) bool,
	parser evidenceParser,
) ([]model.LogEvidence, []string, error) {
	orderedSources := uniqueNonEmptyStrings(sources)
	if len(orderedSources) == 0 {
		return nil, nil, nil
	}

	rows := make([]model.LogEvidence, 0, limit)
	notes := make([]string, 0, len(orderedSources))
	for index, source := range orderedSources {
		// Only the canonical source should perform live file scans. Older source
		// keys are read back from cache for compatibility with pre-upgrade DBs.
		sourceRows, sourceNotes, err := s.lookupOrScanEvidenceAcrossDirs(
			ctx,
			source,
			logDirs,
			bucketTS,
			limit,
			allowFileScan && index == 0,
			matcher,
			queryHints,
			fileNameMatcher,
			parser,
		)
		if err != nil {
			return nil, nil, err
		}
		for _, note := range sourceNotes {
			appendUniqueString(&notes, note)
		}
		rows = appendDedupEvidenceRows(rows, sourceRows)
	}
	return evidence.SortAndTrim(rows, bucketTS, limit), notes, nil
}

func (s *Server) lookupShadowsocksSourceEvidenceByEntryPort(
	ctx context.Context,
	bucketTS int64,
	logDirs []string,
	entryPorts []int,
	allowFileScan bool,
) ([]model.LogEvidence, []string, error) {
	ports := uniquePositiveInts(entryPorts)
	if len(ports) == 0 {
		return nil, nil, nil
	}

	rows := make([]model.LogEvidence, 0, len(ports)*4)
	notes := make([]string, 0, len(ports))
	for _, entryPort := range ports {
		portValue := entryPort
		entryRows, entryNotes, err := s.lookupOrScanEvidenceAcrossSourcesAndDirs(
			ctx,
			shadowsocksEvidenceSources(),
			logDirs,
			bucketTS,
			maxRelatedPeers*4,
			allowFileScan,
			func(ev model.LogEvidence) bool {
				return ev.EntryPort == portValue && ev.ClientIP != "" && !isLoopbackIP(ev.ClientIP)
			},
			evidenceQueryHints{EntryPort: portValue},
			isShadowsocksLogFileName,
			parseSSEvidenceLine,
		)
		if err != nil {
			return nil, nil, err
		}
		for _, note := range entryNotes {
			appendUniqueString(&notes, note)
		}
		rows = appendDedupEvidenceRows(rows, entryRows)
	}
	return evidence.SortAndTrim(rows, bucketTS, maxEvidenceRows), notes, nil
}

func (s *Server) collectEntryPortSourceCandidates(ctx context.Context, bucketTS int64, proto string, entryPorts []int) (map[int][]entrySourceCandidate, error) {
	result := make(map[int][]entrySourceCandidate)
	seen := make(map[int]struct{}, len(entryPorts))
	for _, entryPort := range entryPorts {
		if entryPort <= 0 {
			continue
		}
		if _, ok := seen[entryPort]; ok {
			continue
		}
		seen[entryPort] = struct{}{}

		portValue := entryPort
		rows, _, _, err := s.store.QueryUsage(ctx, model.UsageQuery{
			Start:     time.Unix(bucketTS-usageExplainWindowPadding, 0).UTC(),
			End:       time.Unix(bucketTS+usageExplainWindowPadding+60, 0).UTC(),
			Direction: model.DirectionIn,
			Proto:     proto,
			LocalPort: &portValue,
			UsePage:   true,
			Page:      1,
			PageSize:  200,
			SortBy:    "bytes_total",
			SortOrder: "desc",
		}, store.DataSourceMinute)
		if err != nil {
			return nil, fmt.Errorf("query entry-port source candidates: %w", err)
		}

		aggregated := make(map[string]entrySourceCandidate)
		for _, row := range rows {
			if row.RemoteIP == "" || isLoopbackIP(row.RemoteIP) {
				continue
			}
			candidate := aggregated[row.RemoteIP]
			candidate.RemoteIP = row.RemoteIP
			candidate.BytesTotal += usageBytesTotal(row)
			candidate.FlowCount += row.FlowCount
			aggregated[row.RemoteIP] = candidate
		}

		candidates := make([]entrySourceCandidate, 0, len(aggregated))
		for _, candidate := range aggregated {
			candidates = append(candidates, candidate)
		}
		result[entryPort] = candidates
	}
	return result, nil
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
			Time:              row.EventTS,
			Method:            method,
			Host:              host,
			HostNormalized:    evidence.NormalizeHost(host),
			Path:              path,
			Status:            status,
			Count:             1,
			ClientIP:          row.ClientIP,
			Referer:           trimDisplayValue(referer, 180),
			UserAgent:         trimDisplayValue(userAgent, 180),
			Bot:               bot,
			SampleFingerprint: row.Fingerprint,
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
			agg.ClientIP = request.ClientIP
			agg.SampleFingerprint = request.SampleFingerprint
			agg.Referer = request.Referer
			agg.UserAgent = request.UserAgent
			agg.Bot = request.Bot
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

	ev := model.LogEvidence{
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
	ev = evidence.Normalize(ev)
	ev.Fingerprint = evidenceFingerprint(ev)
	return ev, true
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

	ev := model.LogEvidence{
		Source:      source,
		EventTS:     eventTS,
		ClientIP:    clientIP,
		TargetIP:    targetIP,
		Host:        host,
		Path:        path,
		Method:      method,
		EntryPort:   extractLogEntryPort(line),
		TargetPort:  targetPort,
		Status:      nil,
		Message:     truncateMessage(line, 512),
		Fingerprint: "",
	}
	ev = evidence.Normalize(ev)
	ev.Fingerprint = evidenceFingerprint(ev)
	return ev, true
}

func parseGenericEvidenceLine(source string, line string, reference time.Time) (model.LogEvidence, bool) {
	eventTS, ok := parseFlexibleTimestamp(line, reference)
	if !ok {
		return model.LogEvidence{}, false
	}

	clientIP := extractEndpointIP(ssClientPattern, line)
	if clientIP == "" {
		clientIP = normalizeIP(extractEndpointToken(ssFromPattern, line))
	}

	targetToken := extractEndpointToken(ssTargetPattern, line)
	if targetToken == "" {
		if match := ssConnectPattern.FindStringSubmatch(line); len(match) >= 2 {
			targetToken = match[1]
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

	if clientIP == "" && targetIP == "" && host == "" {
		return model.LogEvidence{}, false
	}

	method := "log"
	lower := strings.ToLower(line)
	if strings.Contains(lower, "connect") {
		method = "connect"
	} else if strings.Contains(lower, "accept") {
		method = "accept"
	}

	path := ""
	if targetPort > 0 {
		path = strconv.Itoa(targetPort)
	}

	ev := model.LogEvidence{
		Source:    source,
		EventTS:   eventTS,
		ClientIP:  clientIP,
		TargetIP:  targetIP,
		Host:      host,
		Path:      path,
		Method:    method,
		EntryPort: extractLogEntryPort(line),
		Message:   truncateMessage(line, 512),
	}
	ev = evidence.Normalize(ev)
	ev.Fingerprint = evidenceFingerprint(ev)
	return ev, true
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

func extractLogEntryPort(line string) int {
	matches := bracketPortPattern.FindAllStringSubmatch(line, -1)
	if len(matches) == 0 {
		return 0
	}
	for i := len(matches) - 1; i >= 0; i-- {
		port, err := strconv.Atoi(matches[i][1])
		if err != nil || port <= 0 || port > 65535 {
			continue
		}
		return port
	}
	return 0
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
	keywords := []string{"ss", "shadowsocks", "outline", "obfs", "server", "manager", "v2ray", "xray", "singbox", "hysteria"}
	for _, keyword := range keywords {
		if strings.Contains(lowerName, keyword) {
			return true
		}
	}
	return false
}

func evidenceFingerprint(row model.LogEvidence) string {
	return evidence.Fingerprint(row)
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

func oppositeDirection(direction model.Direction) model.Direction {
	if direction == model.DirectionIn {
		return model.DirectionOut
	}
	return model.DirectionIn
}

type scoredUsageRecord struct {
	record model.UsageRecord
	score  int
	delta  int64
	bytes  int64
}

func selectCounterpartUsage(query usageExplainQuery, bucketTS int64, anchorBytes int64, rows []model.UsageRecord, maxCount int) []model.UsageRecord {
	if len(rows) == 0 || maxCount <= 0 {
		return nil
	}
	candidates := make([]scoredUsageRecord, 0, len(rows))
	for _, row := range rows {
		if row.Direction != oppositeDirection(query.Direction) || row.RemoteIP == "" {
			continue
		}
		delta := evidence.AbsInt64(row.TimeBucket - bucketTS)
		if delta > usageExplainWindowPadding+60 {
			continue
		}
		bytesTotal := usageBytesTotal(row)
		score := 0
		switch {
		case delta == 0:
			score += 6
		case delta <= 60:
			score += 4
		default:
			score += 2
		}
		if anchorBytes > 0 && bytesTotal > 0 {
			ratio := float64(maxInt64(anchorBytes, bytesTotal)) / float64(minInt64(anchorBytes, bytesTotal))
			switch {
			case ratio <= 2:
				score += 4
			case ratio <= 5:
				score += 2
			}
		}
		if query.LocalPort != nil && row.LocalPort == *query.LocalPort {
			score++
		}
		if query.RemotePort != nil && row.RemotePort != nil && *row.RemotePort == *query.RemotePort {
			score++
		}
		if query.RemoteIP != "" && sameIP(row.RemoteIP, query.RemoteIP) {
			score -= 3
		}
		if row.Attribution != nil && (*row.Attribution == model.AttributionExact || *row.Attribution == model.AttributionHeuristic) {
			score++
		}
		if score < 4 {
			continue
		}
		candidates = append(candidates, scoredUsageRecord{
			record: row,
			score:  score,
			delta:  delta,
			bytes:  bytesTotal,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			if candidates[i].delta == candidates[j].delta {
				if candidates[i].bytes == candidates[j].bytes {
					return candidates[i].record.RemoteIP < candidates[j].record.RemoteIP
				}
				return candidates[i].bytes > candidates[j].bytes
			}
			return candidates[i].delta < candidates[j].delta
		}
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) > maxCount {
		candidates = candidates[:maxCount]
	}
	result := make([]model.UsageRecord, 0, len(candidates))
	for _, candidate := range candidates {
		result = append(result, candidate.record)
	}
	return result
}

func usageBytesTotal(row model.UsageRecord) int64 {
	return row.BytesUp + row.BytesDown
}

func dedupeUsageRecords(rows []model.UsageRecord) []model.UsageRecord {
	if len(rows) <= 1 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	result := make([]model.UsageRecord, 0, len(rows))
	for _, row := range rows {
		key := fmt.Sprintf("%d|%d|%s|%s|%v|%s|%d|%s|%v", row.RowID, row.TimeBucket, row.Proto, row.Direction, row.PID, row.RemoteIP, row.LocalPort, nullableStringValue(row.Exe), nullablePortValue(row.RemotePort))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, row)
	}
	return result
}

func nullableStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullablePortValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func summarizePeers(peers map[string]peerAgg, maxCount int) []usageExplainPeer {
	if len(peers) == 0 {
		return nil
	}
	items := make([]peerAgg, 0, len(peers))
	for _, item := range peers {
		if item.BytesTotal <= 0 && item.FlowCount <= 0 {
			continue
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil
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

func addExplainChain(chains map[string]chainAgg, row model.LogEvidence, query usageExplainQuery, evidenceLabel string) {
	if chains == nil {
		return
	}
	if row.TargetIP == "" && row.Host == "" {
		return
	}

	targetPort := row.TargetPort
	if targetPort <= 0 {
		targetPort, _ = parseEvidencePort(row.Path)
	}
	entryPort := row.EntryPort
	if entryPort <= 0 {
		entryPort = queryLocalPort(query)
	}
	targetIP := strings.TrimSpace(row.TargetIP)
	key := fmt.Sprintf("%s|%d|%s|%s|%d", row.ClientIP, entryPort, targetIP, evidence.NormalizeHost(row.Host), targetPort)
	chain := chains[key]
	if sourceIP := strings.TrimSpace(row.ClientIP); sourceIP != "" {
		chain.SourceIP = sourceIP
	}
	if targetIP != "" {
		chain.TargetIP = targetIP
	}
	if row.Host != "" {
		chain.TargetHost = row.Host
		chain.TargetHostNormalized = evidence.NormalizeHost(row.Host)
	}
	if targetPort > 0 {
		chain.TargetPort = targetPort
	}
	if entryPort > 0 {
		chain.LocalPort = entryPort
	}
	chain.EvidenceCount++
	chain.Evidence = fallbackText(strings.TrimSpace(evidenceLabel), "log")
	chain.EvidenceSource = fallbackText(strings.TrimSpace(evidenceLabel), "log")
	if row.Fingerprint != "" && (chain.SampleFingerprint == "" || row.EventTS >= chain.SampleTime) {
		chain.SampleFingerprint = row.Fingerprint
		chain.SampleMessage = row.Message
		chain.SampleTime = row.EventTS
	}
	chain.Confidence = explainChainConfidence(chain.SourceIP, chain.TargetIP)
	chains[key] = chain
}

func explainChainConfidence(sourceIP string, targetIP string) string {
	sourceIP = strings.TrimSpace(sourceIP)
	targetIP = strings.TrimSpace(targetIP)
	if sourceIP != "" && targetIP != "" {
		return "high"
	}
	if sourceIP != "" || targetIP != "" {
		return "medium"
	}
	return "low"
}

func processEvidenceLabel(processName string) string {
	normalized := strings.ToLower(strings.TrimSpace(processName))
	if normalized == "" {
		return "proc-log"
	}
	return normalized + "-log"
}

func mergeChainUsageMetrics(chains map[string]chainAgg, query usageExplainQuery, related []model.UsageRecord) {
	if len(chains) == 0 || len(related) == 0 {
		return
	}

	type usageTotals struct {
		bytes int64
		flows int64
	}

	byTargetPort := make(map[string]usageTotals)
	bySourceEntry := make(map[string]usageTotals)
	hostOnlyCandidates := make(map[string]int)
	sourceEntryCandidates := make(map[string]int)
	for _, row := range related {
		if row.Direction == model.DirectionIn && row.RemoteIP != "" && row.LocalPort > 0 {
			key := fmt.Sprintf("%s|%d", row.RemoteIP, row.LocalPort)
			totals := bySourceEntry[key]
			totals.bytes += usageBytesTotal(row)
			totals.flows += row.FlowCount
			bySourceEntry[key] = totals
		}
		if row.Direction != model.DirectionOut {
			continue
		}
		if row.RemoteIP == "" {
			continue
		}
		port := 0
		if row.RemotePort != nil {
			port = *row.RemotePort
		}
		key := fmt.Sprintf("%s|%d", row.RemoteIP, port)
		totals := byTargetPort[key]
		totals.bytes += usageBytesTotal(row)
		totals.flows += row.FlowCount
		byTargetPort[key] = totals
	}
	for _, chain := range chains {
		if chain.SourceIP != "" && chain.LocalPort > 0 {
			sourceEntryCandidates[fmt.Sprintf("%s|%d", chain.SourceIP, chain.LocalPort)]++
		}
		if chain.TargetIP == "" && chain.TargetPort > 0 {
			hostOnlyCandidates[fmt.Sprintf("%d|%d", chain.LocalPort, chain.TargetPort)]++
		}
	}

	for key, chain := range chains {
		if chain.TargetIP == "" && query.Direction == model.DirectionOut && strings.TrimSpace(query.RemoteIP) != "" {
			candidateKey := fmt.Sprintf("%d|%d", chain.LocalPort, chain.TargetPort)
			if hostOnlyCandidates[candidateKey] == 1 && (query.RemotePort == nil || chain.TargetPort <= 0 || *query.RemotePort == chain.TargetPort) {
				chain.TargetIP = strings.TrimSpace(query.RemoteIP)
			}
		}
		if chain.TargetPort <= 0 && query.RemotePort != nil {
			chain.TargetPort = *query.RemotePort
		}
		port := chain.TargetPort
		lookupKey := fmt.Sprintf("%s|%d", chain.TargetIP, port)
		if totals, ok := byTargetPort[lookupKey]; ok {
			chain.BytesTotal = totals.bytes
			chain.FlowCount = totals.flows
			chain.Confidence = explainChainConfidence(chain.SourceIP, chain.TargetIP)
			chains[key] = chain
			continue
		}
		if query.Direction == model.DirectionOut && strings.TrimSpace(query.RemoteIP) != "" {
			candidateKey := fmt.Sprintf("%d|%d", chain.LocalPort, chain.TargetPort)
			if hostOnlyCandidates[candidateKey] != 1 {
				continue
			}
			anchorKey := fmt.Sprintf("%s|%d", strings.TrimSpace(query.RemoteIP), port)
			if totals, ok := byTargetPort[anchorKey]; ok {
				chain.TargetIP = strings.TrimSpace(query.RemoteIP)
				chain.BytesTotal = totals.bytes
				chain.FlowCount = totals.flows
				chain.Confidence = explainChainConfidence(chain.SourceIP, chain.TargetIP)
				chains[key] = chain
				continue
			}
		}
		if chain.SourceIP != "" && chain.LocalPort > 0 {
			sourceKey := fmt.Sprintf("%s|%d", chain.SourceIP, chain.LocalPort)
			if sourceEntryCandidates[sourceKey] == 1 {
				if totals, ok := bySourceEntry[sourceKey]; ok {
					chain.BytesTotal = totals.bytes
					chain.FlowCount = totals.flows
					chain.Confidence = explainChainConfidence(chain.SourceIP, chain.TargetIP)
					chains[key] = chain
				}
			}
		}
	}
}

func hydrateChainsFromEntryPortCandidates(chains map[string]chainAgg, candidates map[int][]entrySourceCandidate) {
	for key, chain := range chains {
		if chain.SourceIP != "" || chain.LocalPort <= 0 {
			continue
		}
		ip, ok := chooseEntrySourceCandidate(candidates[chain.LocalPort])
		if !ok {
			continue
		}
		chain.SourceIP = ip
		chain.Confidence = explainChainConfidence(chain.SourceIP, chain.TargetIP)
		chains[key] = chain
	}
}

func buildEntryPortSourceCandidatesFromEvidence(rows []model.LogEvidence) map[int][]entrySourceCandidate {
	if len(rows) == 0 {
		return nil
	}
	aggregated := make(map[int]map[string]entrySourceCandidate)
	for _, row := range rows {
		if row.EntryPort <= 0 || row.ClientIP == "" || isLoopbackIP(row.ClientIP) {
			continue
		}
		perPort := aggregated[row.EntryPort]
		if perPort == nil {
			perPort = make(map[string]entrySourceCandidate)
			aggregated[row.EntryPort] = perPort
		}
		candidate := perPort[row.ClientIP]
		candidate.RemoteIP = row.ClientIP
		candidate.FlowCount++
		candidate.BytesTotal++
		if row.Method == "udp-cache-miss" || row.TargetIP != "" || row.Host != "" {
			candidate.BytesTotal += 2
		}
		perPort[row.ClientIP] = candidate
	}

	result := make(map[int][]entrySourceCandidate, len(aggregated))
	for entryPort, perPort := range aggregated {
		items := make([]entrySourceCandidate, 0, len(perPort))
		for _, candidate := range perPort {
			items = append(items, candidate)
		}
		result[entryPort] = items
	}
	return result
}

func promoteConfirmedSourcesFromEvidence(target map[string]int, rows []model.LogEvidence) {
	candidates := buildEntryPortSourceCandidatesFromEvidence(rows)
	for _, perPort := range candidates {
		sourceIP, ok := chooseEntrySourceCandidate(perPort)
		if !ok || sourceIP == "" {
			continue
		}
		target[sourceIP] += 2
	}
}

func chooseEntrySourceCandidate(candidates []entrySourceCandidate) (string, bool) {
	if len(candidates) == 0 {
		return "", false
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].BytesTotal == candidates[j].BytesTotal {
			if candidates[i].FlowCount == candidates[j].FlowCount {
				return candidates[i].RemoteIP < candidates[j].RemoteIP
			}
			return candidates[i].FlowCount > candidates[j].FlowCount
		}
		return candidates[i].BytesTotal > candidates[j].BytesTotal
	})
	if len(candidates) == 1 {
		return candidates[0].RemoteIP, true
	}
	if candidates[0].BytesTotal > 0 && candidates[0].BytesTotal >= candidates[1].BytesTotal*3 {
		return candidates[0].RemoteIP, true
	}
	return "", false
}

func summarizeChains(chains map[string]chainAgg, maxCount int) []usageExplainChain {
	if len(chains) == 0 {
		return nil
	}
	items := make([]chainAgg, 0, len(chains))
	for _, item := range chains {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].FlowCount == items[j].FlowCount {
			if items[i].EvidenceCount == items[j].EvidenceCount {
				return items[i].SourceIP < items[j].SourceIP
			}
			return items[i].EvidenceCount > items[j].EvidenceCount
		}
		return items[i].FlowCount > items[j].FlowCount
	})
	if maxCount > 0 && len(items) > maxCount {
		items = items[:maxCount]
	}
	result := make([]usageExplainChain, 0, len(items))
	for _, item := range items {
		result = append(result, usageExplainChain{
			SourceIP:             item.SourceIP,
			TargetIP:             item.TargetIP,
			TargetHost:           item.TargetHost,
			TargetHostNormalized: item.TargetHostNormalized,
			TargetPort:           nullablePort(item.TargetPort),
			LocalPort:            nullablePort(item.LocalPort),
			BytesTotal:           item.BytesTotal,
			FlowCount:            item.FlowCount,
			EvidenceCount:        item.EvidenceCount,
			Evidence:             item.Evidence,
			EvidenceSource:       item.EvidenceSource,
			SampleFingerprint:    item.SampleFingerprint,
			SampleMessage:        item.SampleMessage,
			SampleTime:           item.SampleTime,
			Confidence:           item.Confidence,
		})
	}
	return result
}

func mergeExplainChains(existing []usageExplainChain, incoming []usageExplainChain, maxCount int) []usageExplainChain {
	if len(existing) == 0 && len(incoming) == 0 {
		return nil
	}
	order := make([]string, 0, len(existing)+len(incoming))
	merged := make(map[string]usageExplainChain, len(existing)+len(incoming))
	appendChain := func(chain usageExplainChain) {
		key := explainChainIdentity(chain)
		if _, ok := merged[key]; !ok {
			order = append(order, key)
			merged[key] = chain
			return
		}
		merged[key] = mergeExplainChainPair(merged[key], chain)
	}
	for _, chain := range existing {
		appendChain(chain)
	}
	for _, chain := range incoming {
		appendChain(chain)
	}
	result := make([]usageExplainChain, 0, len(merged))
	for _, key := range order {
		result = append(result, merged[key])
	}
	sortExplainChains(result)
	if maxCount > 0 && len(result) > maxCount {
		return result[:maxCount]
	}
	return result
}

func (s *Server) loadPersistedChains(ctx context.Context, bucketTS int64, query usageExplainQuery) ([]usageExplainChain, error) {
	pidFilter, hasIdentity := explainChainPIDFilter(query.PID)
	if strings.TrimSpace(query.Comm) != "" || strings.TrimSpace(query.Exe) != "" {
		hasIdentity = true
	}
	if !hasIdentity {
		return nil, nil
	}

	type chainLookup struct {
		bucket int64
		source string
		label  string
	}
	hourBucket := time.Unix(bucketTS, 0).UTC().Truncate(time.Hour).Unix()
	lookups := []chainLookup{
		{bucket: bucketTS, source: store.DataSourceMinuteChain, label: "分钟链路记录"},
		{bucket: hourBucket, source: store.DataSourceHourChain, label: "小时链路记录"},
	}
	if query.DataSource == store.DataSourceHour {
		lookups[0], lookups[1] = lookups[1], lookups[0]
	}

	var (
		rows        []model.UsageChainRecord
		sourceLabel string
		err         error
	)
	for _, lookup := range lookups {
		rows, err = s.store.QueryUsageChainsForProcess(ctx, lookup.bucket, pidFilter, query.Comm, query.Exe, lookup.source)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			sourceLabel = lookup.label
			break
		}
	}
	if len(rows) == 0 {
		return nil, nil
	}

	relevant := make([]model.UsageChainRecord, 0, len(rows))
	for _, row := range rows {
		if chainRecordMatchesExplain(row, query) {
			relevant = append(relevant, row)
		}
	}
	if len(relevant) == 0 {
		return nil, nil
	}

	chains := make([]usageExplainChain, 0, len(relevant))
	for _, row := range relevant {
		chain := usageExplainChainFromRecord(row, sourceLabel)
		if !shouldPersistCanonicalChain(chain) {
			continue
		}
		chains = append(chains, chain)
	}
	if len(chains) == 0 {
		return nil, nil
	}
	return chains, nil
}

func explainChainIdentity(chain usageExplainChain) string {
	targetHost := ""
	if strings.TrimSpace(chain.TargetIP) == "" {
		targetHost = strings.TrimSpace(chain.TargetHostNormalized)
		if targetHost == "" {
			targetHost = evidence.NormalizeHost(strings.TrimSpace(chain.TargetHost))
		}
	}
	return fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		strings.TrimSpace(chain.SourceIP),
		strings.TrimSpace(chain.TargetIP),
		targetHost,
		nullablePortValue(chain.TargetPort),
		nullablePortValue(chain.LocalPort),
	)
}

func explainConfidenceRank(value string) int {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return 3
	case "medium":
		return 2
	default:
		return 1
	}
}

func explainChainCompleteness(chain usageExplainChain) int {
	score := 0
	if strings.TrimSpace(chain.SourceIP) != "" {
		score++
	}
	if strings.TrimSpace(chain.TargetIP) != "" {
		score++
	}
	if strings.TrimSpace(chain.TargetHost) != "" || strings.TrimSpace(chain.TargetHostNormalized) != "" {
		score++
	}
	if nullablePortValue(chain.TargetPort) > 0 {
		score++
	}
	if nullablePortValue(chain.LocalPort) > 0 {
		score++
	}
	if strings.TrimSpace(chain.SampleFingerprint) != "" {
		score++
	}
	if strings.TrimSpace(chain.SampleMessage) != "" {
		score++
	}
	return score
}

func preferExplainChain(left usageExplainChain, right usageExplainChain) bool {
	leftRank := explainConfidenceRank(left.Confidence)
	rightRank := explainConfidenceRank(right.Confidence)
	if leftRank != rightRank {
		return leftRank > rightRank
	}
	leftComplete := explainChainCompleteness(left)
	rightComplete := explainChainCompleteness(right)
	if leftComplete != rightComplete {
		return leftComplete > rightComplete
	}
	if left.EvidenceCount != right.EvidenceCount {
		return left.EvidenceCount > right.EvidenceCount
	}
	if left.FlowCount != right.FlowCount {
		return left.FlowCount > right.FlowCount
	}
	if left.BytesTotal != right.BytesTotal {
		return left.BytesTotal > right.BytesTotal
	}
	if left.SampleTime != right.SampleTime {
		return left.SampleTime > right.SampleTime
	}
	return strings.TrimSpace(left.ChainID) <= strings.TrimSpace(right.ChainID)
}

func mergeExplainChainPair(current usageExplainChain, incoming usageExplainChain) usageExplainChain {
	primary := current
	secondary := incoming
	if !preferExplainChain(primary, secondary) {
		primary, secondary = secondary, primary
	}

	if strings.TrimSpace(primary.ChainID) == "" {
		primary.ChainID = strings.TrimSpace(secondary.ChainID)
	}
	if strings.TrimSpace(primary.SourceIP) == "" {
		primary.SourceIP = strings.TrimSpace(secondary.SourceIP)
	}
	if strings.TrimSpace(primary.TargetIP) == "" {
		primary.TargetIP = strings.TrimSpace(secondary.TargetIP)
	}
	if strings.TrimSpace(primary.TargetHost) == "" {
		primary.TargetHost = strings.TrimSpace(secondary.TargetHost)
	}
	if strings.TrimSpace(primary.TargetHostNormalized) == "" {
		primary.TargetHostNormalized = fallbackText(strings.TrimSpace(secondary.TargetHostNormalized), evidence.NormalizeHost(primary.TargetHost))
	}
	if primary.TargetPort == nil && secondary.TargetPort != nil {
		primary.TargetPort = secondary.TargetPort
	}
	if primary.LocalPort == nil && secondary.LocalPort != nil {
		primary.LocalPort = secondary.LocalPort
	}
	if primary.BytesTotal < secondary.BytesTotal {
		primary.BytesTotal = secondary.BytesTotal
	}
	if primary.FlowCount < secondary.FlowCount {
		primary.FlowCount = secondary.FlowCount
	}
	if primary.EvidenceCount < secondary.EvidenceCount {
		primary.EvidenceCount = secondary.EvidenceCount
	}
	if strings.TrimSpace(primary.Evidence) == "" {
		primary.Evidence = strings.TrimSpace(secondary.Evidence)
	}
	if strings.TrimSpace(primary.EvidenceSource) == "" {
		primary.EvidenceSource = strings.TrimSpace(secondary.EvidenceSource)
	}
	if explainConfidenceRank(secondary.Confidence) > explainConfidenceRank(primary.Confidence) {
		primary.Confidence = secondary.Confidence
	}
	if secondary.SampleTime > primary.SampleTime {
		primary.SampleTime = secondary.SampleTime
		if strings.TrimSpace(secondary.SampleFingerprint) != "" {
			primary.SampleFingerprint = strings.TrimSpace(secondary.SampleFingerprint)
		}
		if strings.TrimSpace(secondary.SampleMessage) != "" {
			primary.SampleMessage = strings.TrimSpace(secondary.SampleMessage)
		}
	} else {
		if strings.TrimSpace(primary.SampleFingerprint) == "" {
			primary.SampleFingerprint = strings.TrimSpace(secondary.SampleFingerprint)
		}
		if strings.TrimSpace(primary.SampleMessage) == "" {
			primary.SampleMessage = strings.TrimSpace(secondary.SampleMessage)
		}
	}
	return primary
}

func sortExplainChains(chains []usageExplainChain) {
	sort.Slice(chains, func(i, j int) bool {
		if chains[i].FlowCount == chains[j].FlowCount {
			if chains[i].EvidenceCount == chains[j].EvidenceCount {
				if chains[i].BytesTotal == chains[j].BytesTotal {
					return explainChainIdentity(chains[i]) < explainChainIdentity(chains[j])
				}
				return chains[i].BytesTotal > chains[j].BytesTotal
			}
			return chains[i].EvidenceCount > chains[j].EvidenceCount
		}
		return chains[i].FlowCount > chains[j].FlowCount
	})
}

func explainChainPIDFilter(pid *int) (*int, bool) {
	if pid == nil || *pid <= 0 {
		return nil, false
	}
	result := *pid
	return &result, true
}

func chainRecordMatchesExplain(record model.UsageChainRecord, query usageExplainQuery) bool {
	switch query.Direction {
	case model.DirectionIn:
		if query.RemoteIP != "" {
			if !sameIP(record.SourceIP, query.RemoteIP) {
				return false
			}
			if query.LocalPort != nil && record.EntryPort != nil && *record.EntryPort != *query.LocalPort {
				return false
			}
			return true
		}
		return record.EntryPort != nil && query.LocalPort != nil && *record.EntryPort == *query.LocalPort
	case model.DirectionOut:
		if query.RemoteIP == "" {
			return false
		}
		if !sameIP(record.TargetIP, query.RemoteIP) {
			return false
		}
		if query.RemotePort != nil && record.TargetPort != nil && *record.TargetPort != *query.RemotePort {
			return false
		}
		return true
	default:
		return false
	}
}

func usageExplainChainFromRecord(record model.UsageChainRecord, evidenceLabel string) usageExplainChain {
	evidenceSource := strings.TrimSpace(record.EvidenceSource)
	return usageExplainChain{
		ChainID:              record.ChainID,
		SourceIP:             strings.TrimSpace(record.SourceIP),
		TargetIP:             strings.TrimSpace(record.TargetIP),
		TargetHost:           strings.TrimSpace(record.TargetHost),
		TargetHostNormalized: strings.TrimSpace(record.TargetHostNormalized),
		TargetPort:           record.TargetPort,
		LocalPort:            record.EntryPort,
		BytesTotal:           record.BytesTotal,
		FlowCount:            record.FlowCount,
		EvidenceCount:        record.EvidenceCount,
		Evidence:             fallbackText(strings.TrimSpace(evidenceLabel), evidenceSource),
		EvidenceSource:       evidenceSource,
		SampleFingerprint:    strings.TrimSpace(record.SampleFingerprint),
		SampleMessage:        strings.TrimSpace(record.SampleMessage),
		SampleTime:           record.SampleTime,
		Confidence:           record.Confidence,
	}
}

func assignCanonicalChainIDs(bucketTS int64, query usageExplainQuery, chains []usageExplainChain) []usageExplainChain {
	if len(chains) == 0 {
		return nil
	}
	exe := strings.TrimSpace(query.Exe)
	var exePtr *string
	if exe != "" {
		exePtr = &exe
	}

	result := make([]usageExplainChain, 0, len(chains))
	for _, chain := range chains {
		if strings.TrimSpace(chain.ChainID) == "" {
			record := model.UsageChainRecord{
				TimeBucket: bucketTS,
				PID:        query.PID,
				Comm:       strings.TrimSpace(query.Comm),
				Exe:        exePtr,
				SourceIP:   strings.TrimSpace(chain.SourceIP),
				EntryPort:  chain.LocalPort,
				TargetIP:   strings.TrimSpace(chain.TargetIP),
				TargetHost: strings.TrimSpace(chain.TargetHost),
				TargetPort: chain.TargetPort,
				Confidence: chain.Confidence,
			}
			chain.ChainID = store.BuildUsageChainID(bucketTS, store.DataSourceMinuteChain, record)
		}
		result = append(result, chain)
	}
	return result
}

func (s *Server) persistCanonicalChains(ctx context.Context, bucketTS int64, query usageExplainQuery, chains []usageExplainChain) error {
	if len(chains) == 0 {
		return nil
	}

	exe := strings.TrimSpace(query.Exe)
	var exePtr *string
	if exe != "" {
		exePtr = &exe
	}

	records := make([]model.UsageChainRecord, 0, len(chains))
	for _, chain := range chains {
		if strings.HasPrefix(strings.TrimSpace(chain.ChainID), store.DataSourceHourChain+"|") {
			continue
		}
		if !shouldPersistCanonicalChain(chain) {
			continue
		}
		records = append(records, model.UsageChainRecord{
			ChainID:           chain.ChainID,
			TimeBucket:        bucketTS,
			PID:               query.PID,
			Comm:              strings.TrimSpace(query.Comm),
			Exe:               exePtr,
			SourceIP:          strings.TrimSpace(chain.SourceIP),
			EntryPort:         chain.LocalPort,
			TargetIP:          strings.TrimSpace(chain.TargetIP),
			TargetHost:        strings.TrimSpace(chain.TargetHost),
			TargetPort:        chain.TargetPort,
			BytesTotal:        chain.BytesTotal,
			FlowCount:         chain.FlowCount,
			EvidenceCount:     chain.EvidenceCount,
			EvidenceSource:    fallbackText(strings.TrimSpace(chain.EvidenceSource), strings.TrimSpace(chain.Evidence)),
			Confidence:        chain.Confidence,
			SampleFingerprint: chain.SampleFingerprint,
			SampleMessage:     chain.SampleMessage,
			SampleTime:        chain.SampleTime,
		})
	}
	if len(records) == 0 {
		return nil
	}
	return s.store.UpsertUsageChains(ctx, records)
}

func shouldPersistCanonicalChain(chain usageExplainChain) bool {
	if explainConfidenceRank(chain.Confidence) < 2 {
		return false
	}
	if nullablePortValue(chain.LocalPort) <= 0 {
		return false
	}
	if chain.BytesTotal <= 0 && chain.FlowCount <= 0 {
		return false
	}
	hasTargetDescriptor := strings.TrimSpace(chain.TargetIP) != "" ||
		strings.TrimSpace(chain.TargetHostNormalized) != "" ||
		strings.TrimSpace(chain.TargetHost) != ""
	if !hasTargetDescriptor {
		return false
	}
	// Keep persisted chains replayable. Host-only candidates without a resolved
	// target IP can still be shown during live analysis, but once detached from
	// the original minute window they become too ambiguous to reuse safely.
	return strings.TrimSpace(chain.TargetIP) != ""
}

func queryLocalPort(query usageExplainQuery) int {
	if query.LocalPort == nil {
		return 0
	}
	return *query.LocalPort
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
		if !response.StrongMatch {
			return "medium"
		}
		return "high"
	}
	if len(response.Chains) > 0 {
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

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func isShadowsocksProcess(comm, exe string) bool {
	text := strings.ToLower(comm + " " + exe)
	return strings.Contains(text, "ss-") || strings.Contains(text, "shadowsocks") || strings.Contains(text, "obfs")
}

func processIdentityKey(comm, exe string) string {
	if normalizedComm := strings.ToLower(strings.TrimSpace(comm)); normalizedComm != "" {
		return normalizedComm
	}
	if normalizedExe := strings.ToLower(strings.TrimSpace(executableName(exe))); normalizedExe != "" {
		return normalizedExe
	}
	return ""
}

func processLookupKeys(comm, exe string) []string {
	keys := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	appendKey := func(value string) {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			return
		}
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		keys = append(keys, normalized)
	}

	appendKey(comm)
	exeValue := strings.TrimSpace(exe)
	appendKey(exeValue)
	if exeValue != "" {
		normalizedPath := strings.ReplaceAll(exeValue, "\\", "/")
		appendKey(normalizedPath)
		appendKey(filepath.Base(normalizedPath))
	}
	return keys
}

func shadowsocksFamilyLookupKeys() []string {
	return []string{
		"ss-server",
		"ss-manager",
		"obfs-server",
		"obfs-local",
		"simple-obfs",
	}
}

func (s *Server) lookupConfiguredProcessLogDir(comm, exe string) (string, string, bool) {
	if len(s.processLogDirs) == 0 {
		return "", "", false
	}
	for _, key := range processLookupKeys(comm, exe) {
		if dir, ok := s.processLogDirs[key]; ok && strings.TrimSpace(dir) != "" {
			return key, dir, true
		}
	}
	if isShadowsocksProcess(comm, exe) {
		for _, key := range shadowsocksFamilyLookupKeys() {
			if dir, ok := s.processLogDirs[key]; ok && strings.TrimSpace(dir) != "" {
				return key, dir, true
			}
		}
	}
	return "", "", false
}

func (s *Server) lookupConfiguredProcessLogDirs(comm, exe string) []string {
	if len(s.processLogDirs) == 0 {
		return nil
	}
	keys := processLookupKeys(comm, exe)
	if isShadowsocksProcess(comm, exe) {
		keys = append(keys, shadowsocksFamilyLookupKeys()...)
	}
	result := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		dir, ok := s.processLogDirs[key]
		if !ok {
			continue
		}
		normalizedDir := strings.TrimSpace(dir)
		if normalizedDir == "" {
			continue
		}
		if _, ok := seen[normalizedDir]; ok {
			continue
		}
		seen[normalizedDir] = struct{}{}
		result = append(result, normalizedDir)
	}
	return result
}

func isNginxProcess(comm, exe string) bool {
	text := strings.ToLower(comm + " " + exe)
	return strings.Contains(text, "nginx")
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
	return evidence.ResolvedLogDir(value, fallback)
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

func appendUniqueString(items *[]string, value string) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return
	}
	for _, existing := range *items {
		if existing == trimmed {
			return
		}
	}
	*items = append(*items, trimmed)
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func uniquePositiveInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	result := make([]int, 0, len(values))
	seen := make(map[int]struct{}, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func appendDedupEvidenceRows(existing []model.LogEvidence, incoming []model.LogEvidence) []model.LogEvidence {
	if len(incoming) == 0 {
		return existing
	}
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	for _, row := range existing {
		if row.Fingerprint == "" {
			continue
		}
		seen[row.Fingerprint] = struct{}{}
	}
	for _, row := range incoming {
		if row.Fingerprint != "" {
			if _, ok := seen[row.Fingerprint]; ok {
				continue
			}
			seen[row.Fingerprint] = struct{}{}
		}
		existing = append(existing, row)
	}
	return existing
}

func formatResolvedLogDirs(logDirs []string) string {
	uniqueDirs := uniqueNonEmptyStrings(logDirs)
	if len(uniqueDirs) == 0 {
		return ""
	}
	resolved := make([]string, 0, len(uniqueDirs))
	for _, logDir := range uniqueDirs {
		resolved = append(resolved, resolvedLogDir(logDir, ""))
	}
	return strings.Join(resolved, "，")
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

func chainIPs(chains []usageExplainChain, source bool) []string {
	if len(chains) == 0 {
		return nil
	}
	result := make([]string, 0, len(chains))
	seen := make(map[string]struct{}, len(chains))
	for _, chain := range chains {
		ip := chain.TargetIP
		if source {
			ip = chain.SourceIP
		}
		if ip == "" {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		result = append(result, ip)
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

func messageHasExactIP(message string, targetIP string) bool {
	if strings.TrimSpace(message) == "" || strings.TrimSpace(targetIP) == "" {
		return false
	}
	for _, candidate := range extractIPs(message) {
		if sameIP(candidate, targetIP) {
			return true
		}
	}

	for _, token := range strings.Fields(message) {
		candidate := token
		if idx := strings.Index(candidate, "="); idx >= 0 && idx < len(candidate)-1 {
			candidate = candidate[idx+1:]
		}
		if normalized := normalizeIP(strings.Trim(candidate, "[]()\"';,")); normalized != "" && sameIP(normalized, targetIP) {
			return true
		}
	}
	return false
}

func isLoopbackIP(value string) bool {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}
