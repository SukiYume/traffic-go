import type {
  ForwardUsageResponse,
  ProcessSummaryResponse,
  ProcessesResponse,
  RangeKey,
  RemoteSummaryResponse,
  TimeSeriesFilters,
  TimeSeriesPoint,
  TopResponse,
  TrafficApiClient,
  UsageQuery,
  UsageResponse,
  OverviewStats,
  UsageRow,
  ForwardUsageRow,
  ProcessOption,
  UsageExplain,
} from '../types';
import { RANGE_TO_BUCKET } from '../ranges';

const BASE_TS = 1_735_689_600;
const MINUTE = 60;

const PROCESSES: ProcessOption[] = [
  { pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', totalBytes: 842_731_520 },
  { pid: 2041, comm: 'v2ray', exe: '/opt/v2ray/v2ray', totalBytes: 631_320_128 },
  { pid: 3312, comm: 'nginx', exe: '/usr/sbin/nginx', totalBytes: 188_220_416 },
  { pid: 4920, comm: 'dockerd', exe: '/usr/bin/dockerd', totalBytes: 93_114_880 },
  { pid: 0, comm: 'unknown', exe: '', totalBytes: 41_562_112 },
];

const USAGE_ROWS: UsageRow[] = [
  { minuteTs: BASE_TS - 720, proto: 'tcp', direction: 'in', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 8388, remoteIp: '203.0.113.24', remotePort: 52144, attribution: 'exact', bytesUp: 182_000, bytesDown: 1_240_000, pktsUp: 320, pktsDown: 960, flowCount: 3 },
  { minuteTs: BASE_TS - 660, proto: 'tcp', direction: 'out', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 47920, remoteIp: '142.250.72.14', remotePort: 443, attribution: 'exact', bytesUp: 1_918_000, bytesDown: 6_144_000, pktsUp: 980, pktsDown: 1_620, flowCount: 4 },
  { minuteTs: BASE_TS - 600, proto: 'udp', direction: 'out', pid: null, comm: null, exe: null, localPort: 53011, remoteIp: '8.8.8.8', remotePort: 53, attribution: 'unknown', bytesUp: 18_432, bytesDown: 32_768, pktsUp: 20, pktsDown: 24, flowCount: 1 },
  { minuteTs: BASE_TS - 540, proto: 'tcp', direction: 'out', pid: 2041, comm: 'v2ray', exe: '/opt/v2ray/v2ray', localPort: 55712, remoteIp: '104.16.132.229', remotePort: 443, attribution: 'exact', bytesUp: 4_230_144, bytesDown: 14_742_528, pktsUp: 1_024, pktsDown: 2_448, flowCount: 2 },
  { minuteTs: BASE_TS - 480, proto: 'tcp', direction: 'in', pid: 3312, comm: 'nginx', exe: '/usr/sbin/nginx', localPort: 80, remoteIp: '198.51.100.17', remotePort: 41220, attribution: 'exact', bytesUp: 122_880, bytesDown: 896_000, pktsUp: 250, pktsDown: 530, flowCount: 5 },
  { minuteTs: BASE_TS - 420, proto: 'tcp', direction: 'out', pid: 4920, comm: 'dockerd', exe: '/usr/bin/dockerd', localPort: 41688, remoteIp: '93.184.216.34', remotePort: 443, attribution: 'exact', bytesUp: 512_000, bytesDown: 1_024_000, pktsUp: 400, pktsDown: 420, flowCount: 1 },
  { minuteTs: BASE_TS - 360, proto: 'udp', direction: 'out', pid: null, comm: null, exe: null, localPort: 5353, remoteIp: '1.1.1.1', remotePort: 443, attribution: 'unknown', bytesUp: 91_136, bytesDown: 142_336, pktsUp: 88, pktsDown: 96, flowCount: 2 },
  { minuteTs: BASE_TS - 300, proto: 'tcp', direction: 'out', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 51234, remoteIp: '172.217.160.110', remotePort: 443, attribution: 'exact', bytesUp: 2_987_520, bytesDown: 9_437_184, pktsUp: 1_420, pktsDown: 2_928, flowCount: 5 },
];

const FORWARD_ROWS: ForwardUsageRow[] = [
  { minuteTs: BASE_TS - 600, proto: 'tcp', origSrc: '10.0.0.2', origDst: '1.1.1.1', origSport: 51122, origDport: 443, bytesOrig: 4_096_000, bytesReply: 11_468_800, pktsOrig: 1_120, pktsReply: 1_980, flowCount: 2 },
  { minuteTs: BASE_TS - 420, proto: 'tcp', origSrc: '10.0.0.3', origDst: '8.8.8.8', origSport: 53201, origDport: 443, bytesOrig: 2_457_600, bytesReply: 7_782_400, pktsOrig: 640, pktsReply: 1_220, flowCount: 1 },
];

function rangeToMinutes(range: RangeKey) {
  return { '1h': 60, '24h': 24 * 60, '7d': 7 * 24 * 60, '30d': 30 * 24 * 60, '90d': 90 * 24 * 60 }[range];
}

function bucketMinutes(bucket: string) {
  return { '1m': 1, '5m': 5, '1h': 60, '6h': 360, '1d': 1440 }[bucket] ?? 1;
}

function aggregateSeries(range: RangeKey, bucket: string): TimeSeriesPoint[] {
  const totalMinutes = rangeToMinutes(range);
  const stepMinutes = bucketMinutes(bucket);
  const totalBuckets = Math.max(6, Math.floor(totalMinutes / stepMinutes));
  return Array.from({ length: totalBuckets }, (_, index) => {
    const ts = BASE_TS - (totalBuckets - index) * stepMinutes * MINUTE;
    const wave = index % 6;
    return {
      ts,
      up: (wave + 1) * 2_500_000 + index * 180_000,
      down: (wave + 2) * 4_000_000 + index * 210_000,
      flowCount: wave + 2,
      label: new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }).format(ts * 1000),
    };
  });
}

function overviewFor(range: RangeKey): OverviewStats {
  const factor = { '1h': 1, '24h': 6, '7d': 18, '30d': 48, '90d': 90 }[range];
  return {
    bytesUp: 28_742_314 * factor,
    bytesDown: 91_331_120 * factor,
    flowCount: 128 * factor,
    activeConnections: Math.max(12, Math.round((48 * factor) / 6)),
    activeProcesses: PROCESSES.filter((p) => p.pid > 0).length,
    dataSource: range === '90d' ? 'usage_1h' : 'usage_1m',
    range,
  };
}

function filterUsage(query: UsageQuery, rows: UsageRow[]) {
  return rows.filter((row) => {
    if (query.comm && row.comm !== query.comm) return false;
    if (query.pid && row.pid !== Number(query.pid)) return false;
    if (query.exe && row.exe !== query.exe) return false;
    if (query.remoteIp && row.remoteIp !== query.remoteIp) return false;
    if (query.localPort && row.localPort !== Number(query.localPort)) return false;
    if (query.direction && row.direction !== query.direction) return false;
    if (query.proto && row.proto !== query.proto) return false;
    if (query.attribution && row.attribution !== query.attribution) return false;
    return true;
  });
}

function filterForward(query: UsageQuery, rows: ForwardUsageRow[]) {
  return rows.filter((row) => !query.proto || row.proto === query.proto);
}

function paginate<T>(rows: T[], page = 1, pageSize = 50) {
  const safePage = Math.max(page, 1);
  const safePageSize = Math.max(1, pageSize);
  const start = (safePage - 1) * safePageSize;
  return {
    rows: rows.slice(start, start + safePageSize),
    page: safePage,
    pageSize: safePageSize,
    totalRows: rows.length,
  };
}

function createFilteredUsage(query: UsageQuery) {
  return filterUsage(query, USAGE_ROWS).sort((a, b) => b.minuteTs - a.minuteTs);
}

function normalizeUsageRows(range: RangeKey, rows: UsageRow[]) {
  if (range !== '90d') {
    return rows;
  }
  return rows.map((row) => ({
    ...row,
    pid: null,
    exe: null,
    remotePort: null,
    attribution: null,
  }));
}

function createFilteredForward(query: UsageQuery) {
  return filterForward(query, FORWARD_ROWS).sort((a, b) => b.minuteTs - a.minuteTs);
}

function createRelatedRowsForExplain(row: UsageRow) {
  const byIdentity = USAGE_ROWS.filter((candidate) => {
    if (candidate.proto !== row.proto) return false;
    if (Math.abs(candidate.minuteTs - row.minuteTs) > 120) return false;
    if (row.pid != null && row.pid > 0) return candidate.pid === row.pid;
    if (row.comm) return candidate.comm === row.comm;
    if (row.exe) return candidate.exe === row.exe;
    return candidate.direction === row.direction && candidate.remoteIp === row.remoteIp;
  });
  if (byIdentity.length) {
    return byIdentity;
  }
  return USAGE_ROWS.filter((candidate) => candidate.proto === row.proto && candidate.direction === row.direction && candidate.remoteIp === row.remoteIp);
}

function sortIpsByWeight(weights: Map<string, number>) {
  return [...weights.entries()]
    .sort((left, right) => {
      if (left[1] === right[1]) return left[0].localeCompare(right[0]);
      return right[1] - left[1];
    })
    .map(([ip]) => ip)
    .slice(0, 6);
}

function inferExplainConfidence(payload: Omit<UsageExplain, 'confidence' | 'process'>): UsageExplain['confidence'] {
  if (payload.nginxRequests.length) return 'high';
  if (payload.sourceIps.length && payload.targetIps.length) return 'high';
  if (payload.sourceIps.length || payload.targetIps.length) return 'medium';
  return 'low';
}

function isShadowsocksRow(row: UsageRow) {
  const text = `${row.comm ?? ''} ${row.exe ?? ''}`.toLowerCase();
  return text.includes('ss-') || text.includes('shadowsocks');
}

function isNginxRow(row: UsageRow) {
  const text = `${row.comm ?? ''} ${row.exe ?? ''}`.toLowerCase();
  return text.includes('nginx') || text.includes('openresty') || text.includes('apache') || text.includes('caddy');
}

function buildMockUsageExplain(row: UsageRow): UsageExplain {
  const relatedRows = createRelatedRowsForExplain(row);
  const sourceWeights = new Map<string, number>();
  const targetWeights = new Map<string, number>();
  const relatedPeersMap = new Map<string, { direction: UsageRow['direction']; remoteIp: string; remotePort: number | null; localPort: number | null; bytesTotal: number; flowCount: number }>();

  for (const related of relatedRows) {
    if (!related.remoteIp) continue;
    const bytesTotal = related.bytesUp + related.bytesDown;
    if (related.direction === 'in') {
      sourceWeights.set(related.remoteIp, (sourceWeights.get(related.remoteIp) ?? 0) + bytesTotal);
    } else {
      targetWeights.set(related.remoteIp, (targetWeights.get(related.remoteIp) ?? 0) + bytesTotal);
    }
    const key = `${related.direction}:${related.remoteIp}:${related.localPort ?? 0}:${related.remotePort ?? 0}`;
    const current = relatedPeersMap.get(key) ?? {
      direction: related.direction,
      remoteIp: related.remoteIp,
      remotePort: related.remotePort,
      localPort: related.localPort,
      bytesTotal: 0,
      flowCount: 0,
    };
    current.bytesTotal += bytesTotal;
    current.flowCount += related.flowCount;
    relatedPeersMap.set(key, current);
  }

  const nginxRequests =
    isNginxRow(row) && row.direction === 'in' && row.remoteIp
      ? [
          {
            time: row.minuteTs + 12,
            method: 'GET',
            host: 'example.com',
            path: '/traffic/usage',
            status: 200,
            count: 3,
            referer: 'https://example.com/sitemap.xml',
            userAgent: 'Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)',
            bot: 'GPTBot',
          },
        ]
      : [];

  const notes: string[] = [];
  if (isShadowsocksRow(row)) {
    notes.push('Shadowsocks 只能做同进程同时间窗关联，无法保证来源 IP 与目标 IP 一一对应。');
  }
  if (isNginxRow(row)) {
    notes.push('网页路径依赖 access.log 关联，conntrack 本身无法直接给出 URI。');
  }
  if (!sourceWeights.size && !targetWeights.size) {
    notes.push('未找到可用的关联流量样本。');
  }

  const basePayload = {
    sourceIps: sortIpsByWeight(sourceWeights),
    targetIps: sortIpsByWeight(targetWeights),
    relatedPeers: [...relatedPeersMap.values()]
      .sort((left, right) => {
        if (left.bytesTotal === right.bytesTotal) return right.flowCount - left.flowCount;
        return right.bytesTotal - left.bytesTotal;
      })
      .slice(0, 8),
    nginxRequests,
    notes,
  };

  return {
    process: row.comm ? (row.exe ? `${row.comm} (${row.exe})` : row.comm) : 'unknown',
    confidence: inferExplainConfidence(basePayload),
    ...basePayload,
  };
}

function processSummaries(range: RangeKey, query?: { page?: number; pageSize?: number }): ProcessSummaryResponse {
  const rows = PROCESSES.map((process) => ({
    pid: range === '90d' ? null : process.pid,
    comm: process.comm,
    exe: range === '90d' ? null : process.exe,
    bytesUp: Math.round(process.totalBytes * 0.34),
    bytesDown: Math.round(process.totalBytes * 0.66),
    flowCount: Math.max(1, Math.round(process.totalBytes / (32 * 1024 * 1024))),
    totalBytes: process.totalBytes,
  })).sort((left, right) => right.totalBytes - left.totalBytes);
  const page = paginate(rows, query?.page ?? 1, query?.pageSize ?? 25);
  return {
    dataSource: range === '90d' ? 'usage_1h' : 'usage_1m',
    rows: page.rows,
    page: page.page,
    pageSize: page.pageSize,
    totalRows: page.totalRows,
  };
}

function remoteSummaries(
  range: RangeKey,
  options?: { page?: number; pageSize?: number; direction?: 'in' | 'out'; includeLoopback?: boolean },
): RemoteSummaryResponse {
  const grouped = new Map<string, { direction: 'in' | 'out'; remoteIp: string; bytesUp: number; bytesDown: number; flowCount: number }>();
  for (const row of createFilteredUsage({ range })) {
    if (options?.direction && row.direction !== options.direction) continue;
    if (!options?.includeLoopback && (row.remoteIp === '127.0.0.1' || row.remoteIp === '::1')) continue;
    const key = `${row.direction}:${row.remoteIp}`;
    const current = grouped.get(key) ?? {
      direction: row.direction,
      remoteIp: row.remoteIp ?? 'unknown',
      bytesUp: 0,
      bytesDown: 0,
      flowCount: 0,
    };
    current.bytesUp += row.bytesUp;
    current.bytesDown += row.bytesDown;
    current.flowCount += row.flowCount;
    grouped.set(key, current);
  }
  const rows = [...grouped.values()]
    .map((row) => ({ ...row, totalBytes: row.bytesUp + row.bytesDown }))
    .sort((left, right) => right.totalBytes - left.totalBytes);
  const page = paginate(rows, options?.page ?? 1, options?.pageSize ?? 25);
  return {
    dataSource: range === '90d' ? 'usage_1h' : 'usage_1m',
    rows: page.rows,
    page: page.page,
    pageSize: page.pageSize,
    totalRows: page.totalRows,
  };
}

function topRowsFromUsage(rows: UsageRow[], accessor: (row: UsageRow) => string | number | null) {
  const grouped = new Map<string, { label: string; bytesUp: number; bytesDown: number }>();
  for (const row of rows) {
    const key = String(accessor(row) ?? 'unknown');
    const current = grouped.get(key) ?? { label: key, bytesUp: 0, bytesDown: 0 };
    current.bytesUp += row.bytesUp;
    current.bytesDown += row.bytesDown;
    grouped.set(key, current);
  }
  return [...grouped.values()]
    .map((item) => ({
      label: item.label,
      value: item.bytesUp + item.bytesDown,
      bytesUp: item.bytesUp,
      bytesDown: item.bytesDown,
    }))
    .sort((a, b) => b.value - a.value);
}

export function createMockApiClient(): TrafficApiClient {
  return {
    async getOverview(range) {
      return overviewFor(range);
    },
    async getTimeSeries(range, groupBy = 'direction', filters?: TimeSeriesFilters) {
      return {
        dataSource: range === '90d' ? 'usage_1h' : 'usage_1m',
        bucket: RANGE_TO_BUCKET[range],
        points: aggregateSeries(range, RANGE_TO_BUCKET[range]).map((point, index) => {
          const filterModifier = filters?.comm ? 1.18 : filters?.remoteIp ? 1.08 : 1;
          const modifier = (groupBy === 'comm' ? 1.4 : groupBy === 'remote_ip' ? 1.1 : 1) * filterModifier;
          return {
            ...point,
            up: Math.round(point.up * modifier + (index % 3) * 110_000),
            down: Math.round(point.down * modifier + (index % 4) * 160_000),
            flowCount: Math.max(1, Math.round(point.flowCount * modifier)),
          };
        }),
      };
    },
    async getUsage(query) {
      const rows = normalizeUsageRows(query.range, createFilteredUsage(query));
      const page = paginate(rows, query.page ?? 1, query.pageSize ?? 25);
      return {
        dataSource: query.range === '90d' ? 'usage_1h' : 'usage_1m',
        rows: page.rows,
        nextCursor: null,
        page: page.page,
        pageSize: page.pageSize,
        totalRows: page.totalRows,
      };
    },
    async getUsageExplain(row) {
      return buildMockUsageExplain(row);
    },
    async getTopProcesses(range, options) {
      return processSummaries(range, options);
    },
    async getTopRemotes(range, options) {
      return remoteSummaries(range, options);
    },
    async getTopPorts(range) {
      return {
        dataSource: range === '90d' ? 'usage_1h' : 'usage_1m',
        rows: topRowsFromUsage(createFilteredUsage({ range }), (row) => row.localPort ?? 'unknown').slice(0, 5),
      };
    },
    async getProcesses() {
      return { processes: PROCESSES };
    },
    async getForwardUsage(query) {
      const rows = createFilteredForward(query);
      const page = paginate(rows, query.page ?? 1, query.pageSize ?? 25);
      return {
        dataSource: query.range === '90d' ? 'usage_1h_forward' : 'usage_1m_forward',
        rows: page.rows,
        nextCursor: null,
        page: page.page,
        pageSize: page.pageSize,
        totalRows: page.totalRows,
      };
    },
  };
}
