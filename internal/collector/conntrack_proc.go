package collector

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"

	"traffic-go/internal/model"
)

type conntrackObservation struct {
	Flow model.ConntrackFlow
	Zone uint16
}

func readConntrackObservations(path string) ([]conntrackObservation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open conntrack snapshot: %w", err)
	}
	defer file.Close()

	flows := make([]conntrackObservation, 0, 1024)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		flow, ok, err := parseConntrackObservation(line)
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
	observation, ok, err := parseConntrackObservation(line)
	return observation.Flow, ok, err
}

func parseConntrackObservation(line string) (conntrackObservation, bool, error) {
	flow := model.ConntrackFlow{}

	var (
		srcValues    [2]string
		dstValues    [2]string
		sportValues  [2]string
		dportValues  [2]string
		byteValues   [2]string
		packetValues [2]string
		srcCount     int
		dstCount     int
		sportCount   int
		dportCount   int
		byteCount    int
		packetCount  int
		zoneToken    string
		idToken      string
		fieldCount   int
	)
	appendToken := func(values *[2]string, count *int, value string) {
		if *count < len(values) {
			values[*count] = value
		}
		*count++
	}
	for field := range strings.FieldsSeq(line) {
		switch fieldCount {
		case 0:
			flow.Family = field
		case 2:
			flow.Proto = field
		}
		fieldCount++
		key, value, found := strings.Cut(field, "=")
		if !found {
			state := strings.Trim(field, "[]")
			if flow.State == "" && (state == "ESTABLISHED" || state == "UNREPLIED" || state == "ASSURED") {
				flow.State = state
			}
			continue
		}
		switch key {
		case "src":
			appendToken(&srcValues, &srcCount, value)
		case "dst":
			appendToken(&dstValues, &dstCount, value)
		case "sport":
			appendToken(&sportValues, &sportCount, value)
		case "dport":
			appendToken(&dportValues, &dportCount, value)
		case "bytes":
			appendToken(&byteValues, &byteCount, value)
		case "packets":
			appendToken(&packetValues, &packetCount, value)
		case "zone":
			if zoneToken == "" {
				zoneToken = value
			}
		case "id":
			if idToken == "" {
				idToken = value
			}
		}
	}
	if fieldCount < 10 || (flow.Proto != "tcp" && flow.Proto != "udp") {
		return conntrackObservation{}, false, nil
	}

	flow.OrigSrcIP = normalizeConntrackIP(srcValues[0])
	flow.OrigDstIP = normalizeConntrackIP(dstValues[0])
	flow.ReplySrcIP = normalizeConntrackIP(srcValues[1])
	flow.ReplyDstIP = normalizeConntrackIP(dstValues[1])
	flow.OrigSrcPort = mustAtoi(sportValues[0])
	flow.OrigDstPort = mustAtoi(dportValues[0])
	flow.ReplySrcPort = mustAtoi(sportValues[1])
	flow.ReplyDstPort = mustAtoi(dportValues[1])
	flow.OrigBytes = mustAtoi64(byteValues[0])
	flow.ReplyBytes = mustAtoi64(byteValues[1])
	flow.OrigPkts = mustAtoi64(packetValues[0])
	flow.ReplyPkts = mustAtoi64(packetValues[1])
	flow.HasAccounting = byteCount >= 2 || packetCount >= 2
	zoneValue := mustAtoi64(zoneToken)
	if zoneValue > uint64(^uint16(0)) {
		return conntrackObservation{}, false, fmt.Errorf("conntrack zone out of range: %d", zoneValue)
	}
	zone := uint16(zoneValue)
	flow.CTID = mustAtoi64(idToken)
	if flow.CTID == 0 {
		flow.CTID = syntheticCTID(flow, zone)
		flow.SyntheticCTID = true
	}

	if flow.OrigSrcIP == "" || flow.OrigDstIP == "" {
		return conntrackObservation{}, false, nil
	}
	return conntrackObservation{Flow: flow, Zone: zone}, true, nil
}

func syntheticCTID(flow model.ConntrackFlow, zone uint16) uint64 {
	// Fallback when older kernels omit conntrack IDs. Use a 64-bit hash to
	// reduce collision risk for long-running hosts with many active tuples.
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(fmt.Sprintf(
		"%d|%s|%s|%s|%d|%s|%d|%s|%d|%s|%d",
		zone,
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

func normalizeConntrackIP(value string) string {
	ip := net.ParseIP(value)
	if ip == nil {
		return value
	}
	return ip.String()
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
