package api

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"traffic-go/internal/evidence"
	"traffic-go/internal/model"
)

const (
	backgroundPrefetchMaxEvidenceRows = 512
)

func defaultShadowsocksJournalReader(ctx context.Context, start time.Time, end time.Time) ([]string, error) {
	if end.Before(start) {
		start, end = end, start
	}

	lines := make([]string, 0, 256)
	seen := make(map[string]struct{}, 256)
	for _, comm := range shadowsocksFamilyLookupKeys() {
		args := []string{
			"--since", fmt.Sprintf("@%d", start.Unix()),
			"--until", fmt.Sprintf("@%d", end.Unix()),
			"-o", "short-iso",
			"--no-pager",
			"-q",
			"_COMM=" + comm,
		}
		cmd := exec.CommandContext(ctx, "journalctl", args...)
		output, err := cmd.Output()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, exec.ErrNotFound) {
				return nil, nil
			}
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && strings.TrimSpace(string(exitErr.Stderr)) == "" {
				continue
			}
			return nil, fmt.Errorf("read shadowsocks journal for %s: %w", comm, err)
		}
		for _, line := range strings.Split(strings.ReplaceAll(string(output), "\r\n", "\n"), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			lines = append(lines, trimmed)
		}
	}
	return lines, nil
}

func (s *Server) loadShadowsocksJournalEvidence(
	ctx context.Context,
	startTS int64,
	endTS int64,
	referenceTS int64,
	limit int,
	matcher evidenceMatcher,
	scanBudget time.Duration,
) ([]model.LogEvidence, string, error) {
	if s == nil || s.store == nil || s.readShadowsocksJournal == nil || !s.enableSSJournalFallback {
		return nil, "", nil
	}
	if endTS < startTS {
		startTS, endTS = endTS, startTS
	}
	if referenceTS <= 0 {
		referenceTS = endTS
	}

	scanCtx, cancel := context.WithCancel(ctx)
	if scanBudget > 0 {
		scanCtx, cancel = context.WithTimeout(ctx, scanBudget)
	}
	defer cancel()

	lines, err := s.readShadowsocksJournal(scanCtx, time.Unix(startTS, 0).UTC(), time.Unix(endTS, 0).UTC())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, formatShadowsocksJournalTimeoutNote(scanBudget), nil
		}
		return nil, "", err
	}
	if len(lines) == 0 {
		return nil, "", nil
	}

	rows, err := evidence.ScanLines(
		scanCtx,
		evidenceSourceSS,
		lines,
		parseSSEvidenceLine,
		matcher,
		startTS,
		endTS,
		referenceTS,
		limit,
	)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return evidence.SortAndTrim(rows, referenceTS, limit), formatShadowsocksJournalTimeoutNote(scanBudget), nil
	}
	if err != nil {
		return nil, "", err
	}
	if len(rows) == 0 {
		return nil, "", nil
	}
	if err := s.store.UpsertLogEvidenceBatch(ctx, rows); err != nil {
		return nil, "", fmt.Errorf("persist shadowsocks journal evidence: %w", err)
	}
	return evidence.SortAndTrim(rows, referenceTS, limit), "", nil
}

func (s *Server) lookupShadowsocksJournalEvidence(ctx context.Context, bucketTS int64, limit int, matcher evidenceMatcher) ([]model.LogEvidence, string, error) {
	strictRows, strictNote, err := s.loadShadowsocksJournalEvidence(
		ctx,
		bucketTS-logWindowStrict,
		bucketTS+logWindowStrict,
		bucketTS,
		limit,
		matcher,
		explainLogScanBudget,
	)
	if err != nil {
		return nil, "", err
	}
	if len(strictRows) > 0 {
		return strictRows, "已回退到 systemd journal 匹配实时 SS 日志。", nil
	}

	fallbackRows, fallbackNote, err := s.loadShadowsocksJournalEvidence(
		ctx,
		bucketTS-logWindowFallback,
		bucketTS+logWindowFallback,
		bucketTS,
		limit,
		matcher,
		explainLogScanBudget,
	)
	if err != nil {
		return nil, "", err
	}
	if len(fallbackRows) > 0 {
		return fallbackRows, "已回退到 systemd journal 的 ±15 分钟窗口匹配 SS 日志。", nil
	}
	if fallbackNote != "" {
		return nil, fallbackNote, nil
	}
	return nil, strictNote, nil
}

func (s *Server) lookupShadowsocksJournalEvidenceByEntryPorts(
	ctx context.Context,
	bucketTS int64,
	entryPorts []int,
	limitPerPort int,
) (map[int][]model.LogEvidence, []string, error) {
	ports := uniquePositiveInts(entryPorts)
	if len(ports) == 0 || !s.enableSSJournalFallback {
		return nil, nil, nil
	}

	portSet := make(map[int]struct{}, len(ports))
	for _, port := range ports {
		portSet[port] = struct{}{}
	}

	rowsByPort := make(map[int][]model.LogEvidence, len(ports))
	notes := make([]string, 0, 3)
	remaining := ports

	strictRows, strictNote, err := s.loadShadowsocksJournalEvidence(
		ctx,
		bucketTS-logWindowStrict,
		bucketTS+logWindowStrict,
		bucketTS,
		len(ports)*limitPerPort,
		func(ev model.LogEvidence) bool {
			_, ok := portSet[ev.EntryPort]
			return ok && ev.ClientIP != "" && !isLoopbackIP(ev.ClientIP)
		},
		explainLogScanBudget,
	)
	if err != nil {
		return nil, nil, err
	}
	strictByPort := groupLogEvidenceByEntryPort(strictRows, bucketTS, limitPerPort)
	for port, portRows := range strictByPort {
		rowsByPort[port] = portRows
	}
	if len(strictRows) > 0 {
		appendUniqueString(&notes, "SS 入口来源日志已回退到 systemd journal。")
		appendUniqueString(&notes, "已回退到 systemd journal 匹配实时 SS 日志。")
	}
	if strictNote != "" {
		appendUniqueString(&notes, strictNote)
	}
	remaining = collectMissingEntryPorts(ports, rowsByPort)
	if len(remaining) == 0 {
		return rowsByPort, notes, nil
	}

	remainingSet := make(map[int]struct{}, len(remaining))
	for _, port := range remaining {
		remainingSet[port] = struct{}{}
	}
	fallbackRows, fallbackNote, err := s.loadShadowsocksJournalEvidence(
		ctx,
		bucketTS-logWindowFallback,
		bucketTS+logWindowFallback,
		bucketTS,
		len(remaining)*limitPerPort,
		func(ev model.LogEvidence) bool {
			_, ok := remainingSet[ev.EntryPort]
			return ok && ev.ClientIP != "" && !isLoopbackIP(ev.ClientIP)
		},
		explainLogScanBudget,
	)
	if err != nil {
		return nil, nil, err
	}
	for port, portRows := range groupLogEvidenceByEntryPort(fallbackRows, bucketTS, limitPerPort) {
		if len(portRows) == 0 {
			continue
		}
		rowsByPort[port] = appendDedupEvidenceRows(rowsByPort[port], portRows)
		rowsByPort[port] = evidence.SortAndTrim(rowsByPort[port], bucketTS, limitPerPort)
	}
	if len(fallbackRows) > 0 {
		appendUniqueString(&notes, "SS 入口来源日志已回退到 systemd journal。")
		appendUniqueString(&notes, "已回退到 systemd journal 的 ±15 分钟窗口匹配 SS 日志。")
	}
	if fallbackNote != "" {
		appendUniqueString(&notes, fallbackNote)
	}
	return rowsByPort, notes, nil
}

func groupLogEvidenceByEntryPort(rows []model.LogEvidence, bucketTS int64, limit int) map[int][]model.LogEvidence {
	if len(rows) == 0 {
		return nil
	}
	grouped := make(map[int][]model.LogEvidence)
	for _, row := range rows {
		if row.EntryPort <= 0 {
			continue
		}
		grouped[row.EntryPort] = append(grouped[row.EntryPort], row)
	}
	for port, portRows := range grouped {
		grouped[port] = evidence.SortAndTrim(portRows, bucketTS, limit)
	}
	return grouped
}

func collectMissingEntryPorts(entryPorts []int, rowsByPort map[int][]model.LogEvidence) []int {
	if len(entryPorts) == 0 {
		return nil
	}
	missing := make([]int, 0, len(entryPorts))
	for _, port := range entryPorts {
		if len(rowsByPort[port]) > 0 {
			continue
		}
		missing = append(missing, port)
	}
	return missing
}

func formatShadowsocksJournalTimeoutNote(scanBudget time.Duration) string {
	if scanBudget <= 0 {
		return "systemd journal 扫描超时，已跳过实时 SS 日志。"
	}
	return fmt.Sprintf("systemd journal 扫描超过 %s，已跳过实时 SS 日志。", scanBudget.Truncate(100*time.Millisecond))
}
