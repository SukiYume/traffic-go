package model

import "time"

type Direction string

const (
	DirectionIn      Direction = "in"
	DirectionOut     Direction = "out"
	DirectionForward Direction = "forward"
)

type Attribution string

const (
	AttributionExact     Attribution = "exact"
	AttributionHeuristic Attribution = "heuristic"
	AttributionGuess     Attribution = "guess"
	AttributionUnknown   Attribution = "unknown"
)

type ConntrackFlow struct {
	CTID          uint64
	Proto         string
	Family        string
	OrigSrcIP     string
	OrigDstIP     string
	OrigSrcPort   int
	OrigDstPort   int
	ReplySrcIP    string
	ReplyDstIP    string
	ReplySrcPort  int
	ReplyDstPort  int
	OrigBytes     uint64
	ReplyBytes    uint64
	OrigPkts      uint64
	ReplyPkts     uint64
	State         string
	HasAccounting bool
}

type ProcessInfo struct {
	PID  int
	Comm string
	Exe  string
}

type FlowSnapshot struct {
	CTID          uint64
	Proto         string
	LocalIP       string
	LocalPort     int
	RemoteIP      string
	RemotePort    int
	Direction     Direction
	PID           int
	Comm          string
	Exe           string
	Attribution   Attribution
	StartedAt     time.Time
	BaselineOrig  uint64
	BaselineReply uint64
	LastOrig      uint64
	LastReply     uint64
	BaselineOPkts uint64
	BaselineRPkts uint64
	LastOPkts     uint64
	LastRPkts     uint64
	// Counted records whether this conntrack flow has already contributed its
	// one-time flow_count increment. Byte/packet deltas can keep accruing after
	// that, but the flow itself should only be counted once.
	Counted  bool
	LastSeen time.Time
}

type UsageKey struct {
	MinuteTS    int64
	Proto       string
	Direction   Direction
	PID         int
	Comm        string
	Exe         string
	LocalPort   int
	RemoteIP    string
	RemotePort  int
	Attribution Attribution
}

type UsageDelta struct {
	BytesUp   int64
	BytesDown int64
	PktsUp    int64
	PktsDown  int64
	FlowCount int64
}

type UsageRecord struct {
	RowID       int64        `json:"row_id"`
	TimeBucket  int64        `json:"time_bucket"`
	Proto       string       `json:"proto"`
	Direction   Direction    `json:"direction"`
	PID         *int         `json:"pid"`
	Comm        string       `json:"comm"`
	Exe         *string      `json:"exe"`
	LocalPort   int          `json:"local_port"`
	RemoteIP    string       `json:"remote_ip"`
	RemotePort  *int         `json:"remote_port"`
	Attribution *Attribution `json:"attribution"`
	BytesUp     int64        `json:"bytes_up"`
	BytesDown   int64        `json:"bytes_down"`
	PktsUp      int64        `json:"pkts_up"`
	PktsDown    int64        `json:"pkts_down"`
	FlowCount   int64        `json:"flow_count"`
	DataSource  string       `json:"data_source"`
}

type ForwardUsageKey struct {
	MinuteTS  int64
	Proto     string
	OrigSrcIP string
	OrigDstIP string
	OrigSPort int
	OrigDPort int
}

type ForwardUsageRecord struct {
	RowID      int64  `json:"row_id"`
	TimeBucket int64  `json:"time_bucket"`
	Proto      string `json:"proto"`
	OrigSrcIP  string `json:"orig_src_ip"`
	OrigDstIP  string `json:"orig_dst_ip"`
	OrigSPort  int    `json:"orig_sport"`
	OrigDPort  int    `json:"orig_dport"`
	BytesOrig  int64  `json:"bytes_orig"`
	BytesReply int64  `json:"bytes_reply"`
	PktsOrig   int64  `json:"pkts_orig"`
	PktsReply  int64  `json:"pkts_reply"`
	FlowCount  int64  `json:"flow_count"`
	DataSource string `json:"data_source"`
}

type OverviewStats struct {
	Range             string `json:"range"`
	DataSource        string `json:"data_source"`
	BytesUp           int64  `json:"bytes_up"`
	BytesDown         int64  `json:"bytes_down"`
	FlowCount         int64  `json:"flow_count"`
	ActiveConnections int64  `json:"active_connections"`
	ActiveProcesses   int64  `json:"active_processes"`
}

type TimeseriesPoint struct {
	BucketTS   int64  `json:"bucket_ts"`
	Group      string `json:"group"`
	BytesUp    int64  `json:"bytes_up"`
	BytesDown  int64  `json:"bytes_down"`
	FlowCount  int64  `json:"flow_count"`
	DataSource string `json:"data_source"`
}

type TopEntry struct {
	Key        string `json:"key"`
	BytesUp    int64  `json:"bytes_up"`
	BytesDown  int64  `json:"bytes_down"`
	FlowCount  int64  `json:"flow_count"`
	DataSource string `json:"data_source"`
}

type ProcessSummary struct {
	PID        *int    `json:"pid"`
	Comm       string  `json:"comm"`
	Exe        *string `json:"exe"`
	BytesUp    int64   `json:"bytes_up"`
	BytesDown  int64   `json:"bytes_down"`
	FlowCount  int64   `json:"flow_count"`
	DataSource string  `json:"data_source"`
}

type RemoteSummary struct {
	Direction  Direction `json:"direction"`
	RemoteIP   string    `json:"remote_ip"`
	BytesUp    int64     `json:"bytes_up"`
	BytesDown  int64     `json:"bytes_down"`
	FlowCount  int64     `json:"flow_count"`
	DataSource string    `json:"data_source"`
}

type ProcessListItem struct {
	PID  int    `json:"pid"`
	Comm string `json:"comm"`
	Exe  string `json:"exe"`
}

type ActiveStats struct {
	Connections int64
	Processes   int64
}

type LogEvidence struct {
	Source         string
	EventTS        int64
	ClientIP       string
	TargetIP       string
	Host           string
	HostNormalized string
	Path           string
	Method         string
	EntryPort      int
	TargetPort     int
	Status         *int
	Message        string
	Fingerprint    string
}

type UsageChainRecord struct {
	ChainID              string  `json:"chain_id"`
	TimeBucket           int64   `json:"time_bucket"`
	PID                  *int    `json:"pid"`
	Comm                 string  `json:"comm"`
	Exe                  *string `json:"exe"`
	SourceIP             string  `json:"source_ip"`
	EntryPort            *int    `json:"entry_port"`
	TargetIP             string  `json:"target_ip"`
	TargetHost           string  `json:"target_host"`
	TargetHostNormalized string  `json:"target_host_normalized"`
	TargetPort           *int    `json:"target_port"`
	BytesTotal           int64   `json:"bytes_total"`
	FlowCount            int64   `json:"flow_count"`
	EvidenceCount        int     `json:"evidence_count"`
	EvidenceSource       string  `json:"evidence_source"`
	Confidence           string  `json:"confidence"`
	SampleFingerprint    string  `json:"sample_fingerprint"`
	SampleMessage        string  `json:"sample_message"`
	SampleTime           int64   `json:"sample_time"`
	DataSource           string  `json:"data_source"`
}

type UsageQuery struct {
	Start       time.Time
	End         time.Time
	Comm        string
	Exe         string
	RemoteIP    string
	RemotePort  *int
	Direction   Direction
	Proto       string
	Attribution Attribution
	PID         *int
	LocalPort   *int
	Limit       int
	Page        int
	PageSize    int
	SortBy      string
	SortOrder   string
	UsePage     bool
	CursorTS    int64
	CursorRowID int64
}

type ForwardQuery struct {
	Start       time.Time
	End         time.Time
	Proto       string
	OrigSrcIP   string
	OrigDstIP   string
	Limit       int
	Page        int
	PageSize    int
	SortBy      string
	SortOrder   string
	UsePage     bool
	CursorTS    int64
	CursorRowID int64
}

type TimeseriesQuery struct {
	Start     time.Time
	End       time.Time
	Bucket    time.Duration
	GroupBy   string
	Comm      string
	Exe       string
	RemoteIP  string
	Direction Direction
	Proto     string
	PID       *int
}
