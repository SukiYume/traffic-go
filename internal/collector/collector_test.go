package collector

import (
	"testing"

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
