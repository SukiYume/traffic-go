package collector

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"

	"traffic-go/internal/model"
)

func ReadConntrackSnapshot(path string) ([]model.ConntrackFlow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open conntrack snapshot: %w", err)
	}
	defer file.Close()

	flows := make([]model.ConntrackFlow, 0, 1024)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		flow, ok, err := parseConntrackLine(line)
		if err != nil {
			return nil, err
		}
		if ok {
			flows = append(flows, flow)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan conntrack snapshot: %w", err)
	}
	return flows, nil
}

func parseConntrackLine(line string) (model.ConntrackFlow, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 10 {
		return model.ConntrackFlow{}, false, nil
	}

	flow := model.ConntrackFlow{
		Family: fields[0],
		Proto:  fields[2],
	}
	if flow.Proto != "tcp" && flow.Proto != "udp" {
		return model.ConntrackFlow{}, false, nil
	}

	tokenValues := make(map[string][]string)
	for _, field := range fields {
		if !strings.Contains(field, "=") {
			if flow.State == "" && (field == "ESTABLISHED" || field == "UNREPLIED" || field == "ASSURED") {
				flow.State = field
			}
			continue
		}
		parts := strings.SplitN(field, "=", 2)
		tokenValues[parts[0]] = append(tokenValues[parts[0]], parts[1])
	}

	flow.OrigSrcIP = firstValue(tokenValues["src"], 0)
	flow.OrigDstIP = firstValue(tokenValues["dst"], 0)
	flow.ReplySrcIP = firstValue(tokenValues["src"], 1)
	flow.ReplyDstIP = firstValue(tokenValues["dst"], 1)
	flow.OrigSrcPort = mustAtoi(firstValue(tokenValues["sport"], 0))
	flow.OrigDstPort = mustAtoi(firstValue(tokenValues["dport"], 0))
	flow.ReplySrcPort = mustAtoi(firstValue(tokenValues["sport"], 1))
	flow.ReplyDstPort = mustAtoi(firstValue(tokenValues["dport"], 1))
	flow.OrigBytes = mustAtoi64(firstValue(tokenValues["bytes"], 0))
	flow.ReplyBytes = mustAtoi64(firstValue(tokenValues["bytes"], 1))
	flow.OrigPkts = mustAtoi64(firstValue(tokenValues["packets"], 0))
	flow.ReplyPkts = mustAtoi64(firstValue(tokenValues["packets"], 1))
	flow.HasAccounting = len(tokenValues["bytes"]) >= 2 || len(tokenValues["packets"]) >= 2
	flow.CTID = mustAtoi64(firstValue(tokenValues["id"], 0))
	if flow.CTID == 0 {
		flow.CTID = syntheticCTID(flow)
	}

	if flow.OrigSrcIP == "" || flow.OrigDstIP == "" {
		return model.ConntrackFlow{}, false, nil
	}
	return flow, true, nil
}

func syntheticCTID(flow model.ConntrackFlow) uint64 {
	// Fallback when older kernels omit conntrack IDs. Use a 64-bit hash to
	// reduce collision risk for long-running hosts with many active tuples.
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(fmt.Sprintf(
		"%s|%s|%s|%d|%s|%d|%s|%d|%s|%d",
		flow.Family,
		flow.Proto,
		flow.OrigSrcIP,
		flow.OrigSrcPort,
		flow.OrigDstIP,
		flow.OrigDstPort,
		flow.ReplySrcIP,
		flow.ReplySrcPort,
		flow.ReplyDstIP,
		flow.ReplyDstPort,
	)))
	if sum := hash.Sum64(); sum != 0 {
		return sum
	}
	return 1
}

func firstValue(values []string, idx int) string {
	if len(values) <= idx {
		return ""
	}
	return values[idx]
}

func mustAtoi(value string) int {
	if value == "" {
		return 0
	}
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func mustAtoi64(value string) uint64 {
	if value == "" {
		return 0
	}
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}
