package store

const schemaSQL = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
-- Compatibility cleanup for databases created before runtime snapshots moved fully in-memory.
DROP TABLE IF EXISTS flows_snapshot;

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

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS log_evidence (
    source TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    client_ip TEXT NOT NULL,
    target_ip TEXT NOT NULL,
    host TEXT NOT NULL,
    path TEXT NOT NULL,
    method TEXT NOT NULL,
    status INTEGER,
    message TEXT NOT NULL,
    fingerprint TEXT NOT NULL PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_log_evidence_lookup ON log_evidence (source, event_ts, client_ip, target_ip);
CREATE INDEX IF NOT EXISTS idx_log_evidence_created_at ON log_evidence (created_at);
`
