package store

const schemaSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS usage_1m (
    minute_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    direction TEXT NOT NULL,
    pid INTEGER NOT NULL,
    comm TEXT NOT NULL,
    exe TEXT NOT NULL,
    local_port INTEGER NOT NULL,
    remote_ip TEXT NOT NULL,
    remote_port INTEGER NOT NULL,
    attribution TEXT NOT NULL,
    bytes_up INTEGER NOT NULL,
    bytes_down INTEGER NOT NULL,
    pkts_up INTEGER NOT NULL,
    pkts_down INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (minute_ts, proto, direction, pid, comm, exe, local_port, remote_ip, remote_port, attribution)
);

CREATE INDEX IF NOT EXISTS idx_usage_1m_minute_comm ON usage_1m (minute_ts, comm);
CREATE INDEX IF NOT EXISTS idx_usage_1m_minute_remote ON usage_1m (minute_ts, remote_ip);
CREATE INDEX IF NOT EXISTS idx_usage_1m_minute_port ON usage_1m (minute_ts, local_port);
CREATE INDEX IF NOT EXISTS idx_usage_1m_minute_pid ON usage_1m (minute_ts, pid);
CREATE INDEX IF NOT EXISTS idx_usage_1m_comm_minute ON usage_1m (comm, minute_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1m_remote_minute ON usage_1m (remote_ip, minute_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1m_direction_minute ON usage_1m (direction, minute_ts);

CREATE TABLE IF NOT EXISTS usage_1h (
    hour_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    direction TEXT NOT NULL,
    comm TEXT NOT NULL,
    local_port INTEGER NOT NULL,
    remote_ip TEXT NOT NULL,
    bytes_up INTEGER NOT NULL,
    bytes_down INTEGER NOT NULL,
    pkts_up INTEGER NOT NULL,
    pkts_down INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (hour_ts, proto, direction, comm, local_port, remote_ip)
);

CREATE INDEX IF NOT EXISTS idx_usage_1h_hour_comm ON usage_1h (hour_ts, comm);
CREATE INDEX IF NOT EXISTS idx_usage_1h_hour_remote ON usage_1h (hour_ts, remote_ip);
CREATE INDEX IF NOT EXISTS idx_usage_1h_comm_hour ON usage_1h (comm, hour_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1h_remote_hour ON usage_1h (remote_ip, hour_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1h_direction_hour ON usage_1h (direction, hour_ts);

CREATE TABLE IF NOT EXISTS usage_1d (
    day_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    direction TEXT NOT NULL,
    comm TEXT NOT NULL,
    local_port INTEGER NOT NULL,
    remote_ip TEXT NOT NULL,
    bytes_up INTEGER NOT NULL,
    bytes_down INTEGER NOT NULL,
    pkts_up INTEGER NOT NULL,
    pkts_down INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (day_ts, proto, direction, comm, local_port, remote_ip)
);

CREATE INDEX IF NOT EXISTS idx_usage_1d_day_comm ON usage_1d (day_ts, comm);
CREATE INDEX IF NOT EXISTS idx_usage_1d_day_remote ON usage_1d (day_ts, remote_ip);
CREATE INDEX IF NOT EXISTS idx_usage_1d_comm_day ON usage_1d (comm, day_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1d_remote_day ON usage_1d (remote_ip, day_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1d_direction_day ON usage_1d (direction, day_ts);

CREATE TABLE IF NOT EXISTS usage_1m_forward (
    minute_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    orig_src_ip TEXT NOT NULL,
    orig_dst_ip TEXT NOT NULL,
    orig_sport INTEGER NOT NULL,
    orig_dport INTEGER NOT NULL,
    bytes_orig INTEGER NOT NULL,
    bytes_reply INTEGER NOT NULL,
    pkts_orig INTEGER NOT NULL,
    pkts_reply INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (minute_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport)
);

CREATE INDEX IF NOT EXISTS idx_usage_1m_forward_src_minute ON usage_1m_forward (orig_src_ip, minute_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1m_forward_dst_minute ON usage_1m_forward (orig_dst_ip, minute_ts);

CREATE TABLE IF NOT EXISTS usage_1h_forward (
    hour_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    orig_src_ip TEXT NOT NULL,
    orig_dst_ip TEXT NOT NULL,
    orig_sport INTEGER NOT NULL,
    orig_dport INTEGER NOT NULL,
    bytes_orig INTEGER NOT NULL,
    bytes_reply INTEGER NOT NULL,
    pkts_orig INTEGER NOT NULL,
    pkts_reply INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (hour_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport)
);

CREATE INDEX IF NOT EXISTS idx_usage_1h_forward_src_hour ON usage_1h_forward (orig_src_ip, hour_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1h_forward_dst_hour ON usage_1h_forward (orig_dst_ip, hour_ts);

CREATE TABLE IF NOT EXISTS usage_1d_forward (
    day_ts INTEGER NOT NULL,
    proto TEXT NOT NULL,
    orig_src_ip TEXT NOT NULL,
    orig_dst_ip TEXT NOT NULL,
    orig_sport INTEGER NOT NULL,
    orig_dport INTEGER NOT NULL,
    bytes_orig INTEGER NOT NULL,
    bytes_reply INTEGER NOT NULL,
    pkts_orig INTEGER NOT NULL,
    pkts_reply INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    PRIMARY KEY (day_ts, proto, orig_src_ip, orig_dst_ip, orig_sport, orig_dport)
);

CREATE INDEX IF NOT EXISTS idx_usage_1d_forward_src_day ON usage_1d_forward (orig_src_ip, day_ts);
CREATE INDEX IF NOT EXISTS idx_usage_1d_forward_dst_day ON usage_1d_forward (orig_dst_ip, day_ts);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dirty_chain_hours (
    hour_ts INTEGER NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS usage_monthly (
    month_ts INTEGER NOT NULL PRIMARY KEY,
    bytes_up INTEGER NOT NULL,
    bytes_down INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    forward_bytes_orig INTEGER NOT NULL,
    forward_bytes_reply INTEGER NOT NULL,
    forward_flow_count INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL,
    chain_count INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS log_evidence (
    source TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    client_ip TEXT NOT NULL,
    target_ip TEXT NOT NULL,
    host TEXT NOT NULL,
    host_normalized TEXT NOT NULL DEFAULT '',
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    entry_port INTEGER NOT NULL DEFAULT 0,
    target_port INTEGER NOT NULL DEFAULT 0,
    status INTEGER,
    message TEXT NOT NULL,
    fingerprint TEXT NOT NULL PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_log_evidence_lookup ON log_evidence (source, event_ts, client_ip, target_ip);
CREATE INDEX IF NOT EXISTS idx_log_evidence_client_lookup ON log_evidence (source, event_ts, client_ip);
CREATE INDEX IF NOT EXISTS idx_log_evidence_target_lookup ON log_evidence (source, event_ts, target_ip);
CREATE INDEX IF NOT EXISTS idx_log_evidence_created_at ON log_evidence (created_at);
CREATE INDEX IF NOT EXISTS idx_log_evidence_entry_port ON log_evidence (source, event_ts, entry_port);
CREATE INDEX IF NOT EXISTS idx_log_evidence_host_port ON log_evidence (source, event_ts, host_normalized, target_port);

CREATE TABLE IF NOT EXISTS usage_chain_1m (
    minute_ts INTEGER NOT NULL,
    chain_id TEXT NOT NULL PRIMARY KEY,
    pid INTEGER NOT NULL,
    comm TEXT NOT NULL,
    exe TEXT NOT NULL,
    source_ip TEXT NOT NULL,
    entry_port INTEGER NOT NULL,
    target_ip TEXT NOT NULL,
    target_host TEXT NOT NULL,
    target_host_normalized TEXT NOT NULL,
    target_port INTEGER NOT NULL,
    bytes_total INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL,
    evidence_source TEXT NOT NULL,
    confidence TEXT NOT NULL,
    confidence_rank INTEGER NOT NULL,
    sample_fingerprint TEXT NOT NULL,
    sample_message TEXT NOT NULL,
    sample_time INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usage_chain_1m_lookup ON usage_chain_1m (minute_ts, pid, comm, entry_port, target_ip, target_host_normalized, target_port);
CREATE INDEX IF NOT EXISTS idx_usage_chain_1m_source ON usage_chain_1m (minute_ts, source_ip, entry_port);

CREATE TABLE IF NOT EXISTS usage_chain_1h (
    hour_ts INTEGER NOT NULL,
    chain_id TEXT NOT NULL PRIMARY KEY,
    pid INTEGER NOT NULL,
    comm TEXT NOT NULL,
    exe TEXT NOT NULL,
    source_ip TEXT NOT NULL,
    entry_port INTEGER NOT NULL,
    target_ip TEXT NOT NULL,
    target_host TEXT NOT NULL,
    target_host_normalized TEXT NOT NULL,
    target_port INTEGER NOT NULL,
    bytes_total INTEGER NOT NULL,
    flow_count INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL,
    evidence_source TEXT NOT NULL,
    confidence TEXT NOT NULL,
    confidence_rank INTEGER NOT NULL,
    sample_fingerprint TEXT NOT NULL,
    sample_message TEXT NOT NULL,
    sample_time INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usage_chain_1h_lookup ON usage_chain_1h (hour_ts, pid, comm, entry_port, target_ip, target_host_normalized, target_port);
CREATE INDEX IF NOT EXISTS idx_usage_chain_1h_source ON usage_chain_1h (hour_ts, source_ip, entry_port);
`
