import type {
  ForwardSortKey,
  ForwardUsageQuery,
  ForwardUsageResponse,
  ProcessGroupBy,
  ProcessSortKey,
  ProcessSummaryResponse,
  ProcessesResponse,
  RangeKey,
  RemoteSortKey,
  RemoteSummaryResponse,
  SortOrder,
  TimeSeriesFilters,
  TimeSeriesPoint,
  TopResponse,
  TrafficApiClient,
  UsageQuery,
  UsageResponse,
  UsageSortKey,
  OverviewStats,
  UsageRow,
  ForwardUsageRow,
  MonthlyUsageResponse,
  ProcessOption,
  UsageExplain,
} from '../types';
import { RANGE_TO_BUCKET } from '../ranges';
import { executableName } from '../utils';

const BASE_TS = 1_735_689_600;
const MINUTE = 60;

type MockProcessSeed = ProcessOption & { totalBytes: number };

const PROCESSES: MockProcessSeed[] = [
  { pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', totalBytes: 842_731_520 },
  { pid: 2041, comm: 'v2ray', exe: '/opt/v2ray/v2ray', totalBytes: 631_320_128 },
  { pid: 3312, comm: 'nginx', exe: '/usr/sbin/nginx', totalBytes: 188_220_416 },
  { pid: 4920, comm: 'dockerd', exe: '/usr/bin/dockerd', totalBytes: 93_114_880 },
  { pid: 0, comm: 'unknown', exe: '', totalBytes: 41_562_112 },
];

const monthTs = (year: number, month: number) => Date.UTC(year, month - 1, 1) / 1000;

const MONTHLY_ROWS: MonthlyUsageResponse['rows'] = [
  {
    monthTs: monthTs(2026, 4),
    bytesUp: 1_208_742_000,
    bytesDown: 3_835_906_000,
    flowCount: 6_144,
    forwardBytesOrig: 310_421_000,
    forwardBytesReply: 812_330_000,
    forwardFlowCount: 420,
    evidenceCount: 1_318,
    chainCount: 274,
    updatedAt: BASE_TS,
    archived: false,
    detailAvailable: true,
    detailRange: 'this_month',
    totalBytes: 5_044_648_000,
    forwardTotalBytes: 1_122_751_000,
  },
  {
    monthTs: monthTs(2026, 3),
    bytesUp: 4_402_130_000,
    bytesDown: 12_840_520_000,
    flowCount: 18_924,
    forwardBytesOrig: 1_930_088_000,
    forwardBytesReply: 4_406_220_000,
    forwardFlowCount: 1_208,
    evidenceCount: 5_841,
    chainCount: 1_092,
    updatedAt: BASE_TS - 1_800,
    archived: false,
    detailAvailable: true,
    detailRange: 'last_month',
    totalBytes: 17_242_650_000,
    forwardTotalBytes: 6_336_308_000,
  },
  {
    monthTs: monthTs(2026, 2),
    bytesUp: 3_980_044_000,
    bytesDown: 10_112_774_000,
    flowCount: 16_310,
    forwardBytesOrig: 1_504_992_000,
    forwardBytesReply: 3_808_331_000,
    forwardFlowCount: 976,
    evidenceCount: 4_228,
    chainCount: 834,
    updatedAt: BASE_TS - 3_600,
    archived: false,
    detailAvailable: true,
    detailRange: 'two_months_ago',
    totalBytes: 14_092_818_000,
    forwardTotalBytes: 5_313_323_000,
  },
  {
    monthTs: monthTs(2026, 1),
    bytesUp: 3_214_500_000,
    bytesDown: 9_748_120_000,
    flowCount: 14_009,
    forwardBytesOrig: 1_140_300_000,
    forwardBytesReply: 3_554_880_000,
    forwardFlowCount: 802,
    evidenceCount: 3_902,
    chainCount: 646,
    updatedAt: BASE_TS - 7_200,
    archived: true,
    detailAvailable: false,
    detailRange: null,
    totalBytes: 12_962_620_000,
    forwardTotalBytes: 4_695_180_000,
  },
];

const USAGE_ROWS: UsageRow[] = [
  { minuteTs: BASE_TS - 720, proto: 'tcp', direction: 'in', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 8388, remoteIp: '203.0.113.24', remotePort: 52144, attribution: 'exact', bytesUp: 182_000, bytesDown: 1_240_000, pktsUp: 320, pktsDown: 960, flowCount: 3 },
  { minuteTs: BASE_TS - 660, proto: 'tcp', direction: 'out', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 47920, remoteIp: '198.51.100.44', remotePort: 443, attribution: 'exact', bytesUp: 1_918_000, bytesDown: 6_144_000, pktsUp: 980, pktsDown: 1_620, flowCount: 4 },
  { minuteTs: BASE_TS - 600, proto: 'udp', direction: 'out', pid: null, comm: null, exe: null, localPort: 53011, remoteIp: '203.0.113.53', remotePort: 53, attribution: 'unknown', bytesUp: 18_432, bytesDown: 32_768, pktsUp: 20, pktsDown: 24, flowCount: 1 },
  { minuteTs: BASE_TS - 540, proto: 'tcp', direction: 'out', pid: 2041, comm: 'v2ray', exe: '/opt/v2ray/v2ray', localPort: 55712, remoteIp: '192.0.2.25', remotePort: 443, attribution: 'exact', bytesUp: 4_230_144, bytesDown: 14_742_528, pktsUp: 1_024, pktsDown: 2_448, flowCount: 2 },
  { minuteTs: BASE_TS - 480, proto: 'tcp', direction: 'in', pid: 3312, comm: 'nginx', exe: '/usr/sbin/nginx', localPort: 80, remoteIp: '198.51.100.17', remotePort: 41220, attribution: 'exact', bytesUp: 122_880, bytesDown: 896_000, pktsUp: 250, pktsDown: 530, flowCount: 5 },
  { minuteTs: BASE_TS - 420, proto: 'tcp', direction: 'out', pid: 4920, comm: 'dockerd', exe: '/usr/bin/dockerd', localPort: 41688, remoteIp: '203.0.113.88', remotePort: 443, attribution: 'exact', bytesUp: 512_000, bytesDown: 1_024_000, pktsUp: 400, pktsDown: 420, flowCount: 1 },
  { minuteTs: BASE_TS - 360, proto: 'udp', direction: 'out', pid: null, comm: null, exe: null, localPort: 5353, remoteIp: '192.0.2.53', remotePort: 443, attribution: 'unknown', bytesUp: 91_136, bytesDown: 142_336, pktsUp: 88, pktsDown: 96, flowCount: 2 },
  { minuteTs: BASE_TS - 300, proto: 'tcp', direction: 'out', pid: 1088, comm: 'ss-server', exe: '/usr/bin/ss-server', localPort: 51234, remoteIp: '198.51.100.118', remotePort: 443, attribution: 'exact', bytesUp: 2_987_520, bytesDown: 9_437_184, pktsUp: 1_420, pktsDown: 2_928, flowCount: 5 },
];

const FORWARD_ROWS: ForwardUsageRow[] = [
  { minuteTs: BASE_TS - 600, proto: 'tcp', origSrc: '10.0.0.2', origDst: '198.51.100.53', origSport: 51122, origDport: 443, bytesOrig: 4_096_000, bytesReply: 11_468_800, pktsOrig: 1_120, pktsReply: 1_980, flowCount: 2 },
  { minuteTs: BASE_TS - 420, proto: 'tcp', origSrc: '10.0.0.3', origDst: '203.0.113.53', origSport: 53201, origDport: 443, bytesOrig: 2_457_600, bytesReply: 7_782_400, pktsOrig: 640, pktsReply: 1_220, flowCount: 1 },
];

function rangeToMinutes(range: RangeKey) {
  return {
    '1h': 60,
    '24h': 24 * 60,
    '7d': 7 * 24 * 60,
    this_month: 31 * 24 * 60,
    last_month: 30 * 24 * 60,
    two_months_ago: 28 * 24 * 60,
  }[range];
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

function buildGroupedSeries(points: TimeSeriesPoint[], groupBy: 'direction' | 'comm' | 'remote_ip') {
  if (groupBy === 'direction') {
    return [
      {
        key: 'in',
        points: points.map((point, index) => {
          const inboundShare = 0.34 + (index % 3) * 0.07;
          const bytesTotal = point.up + point.down;
          return {
            ...point,
            up: Math.round(bytesTotal * inboundShare * 0.42),
            down: Math.round(bytesTotal * inboundShare * 0.58),
            flowCount: Math.max(1, Math.round(point.flowCount * inboundShare)),
          };
        }),
      },
      {
        key: 'out',
        points: points.map((point, index) => {
          const inboundShare = 0.34 + (index % 3) * 0.07;
          const outboundShare = 1 - inboundShare;
          const bytesTotal = point.up + point.down;
          return {
            ...point,
            up: Math.round(bytesTotal * outboundShare * 0.36),
            down: Math.round(bytesTotal * outboundShare * 0.64),
            flowCount: Math.max(1, point.flowCount - Math.round(point.flowCount * inboundShare)),
          };
        }),
      },
    ];
  }

  const key = groupBy === 'comm' ? 'process' : 'remote';
  return [{ key, points }];
}

function overviewFor(range: RangeKey): OverviewStats {
  const factor = { '1h': 1, '24h': 6, '7d': 18, this_month: 42, last_month: 40, two_months_ago: 37 }[range];
  return {
    bytesUp: 28_742_314 * factor,
    bytesDown: 91_331_120 * factor,
    flowCount: 128 * factor,
    activeConnections: Math.max(12, Math.round((48 * factor) / 6)),
    activeProcesses: PROCESSES.filter((p) => p.pid > 0).length,
    dataSource: 'usage_1m',
    range,
  };
}

function filterUsage(query: UsageQuery, rows: UsageRow[]) {
  return rows.filter((row) => {
    if (query.comm && row.comm !== query.comm) return false;
    if (query.pid && row.pid !== Number(query.pid)) return false;
    if (query.exe) {
      const filter = query.exe.trim();
      const rowExe = row.exe?.trim() ?? '';
      if (rowExe !== filter && executableName(rowExe) !== executableName(filter)) {
        return false;
      }
    }
    if (query.remoteIp && row.remoteIp !== query.remoteIp) return false;
    if (query.localPort && row.localPort !== Number(query.localPort)) return false;
    if (query.direction && row.direction !== query.direction) return false;
    if (query.proto && row.proto !== query.proto) return false;
    if (query.attribution && row.attribution !== query.attribution) return false;
    return true;
  });
}

function filterForward(query: ForwardUsageQuery, rows: ForwardUsageRow[]) {
  return rows.filter((row) => {
    if (query.proto && row.proto !== query.proto) return false;
    if (query.origSrcIp && row.origSrc !== query.origSrcIp) return false;
    if (query.origDstIp && row.origDst !== query.origDstIp) return false;
    return true;
  });
}

function compareText(left: string | null | undefined, right: string | null | undefined) {
  return (left ?? '').localeCompare(right ?? '', 'zh-CN');
}

function compareNumber(left: number | null | undefined, right: number | null | undefined) {
  return (left ?? 0) - (right ?? 0);
}

function normalizeSortOrder(sortOrder?: SortOrder): SortOrder {
  return sortOrder === 'asc' ? 'asc' : 'desc';
}

function applyOrder(value: number, sortOrder: SortOrder) {
  return sortOrder === 'asc' ? value : -value;
}

function sortUsageRows(rows: UsageRow[], sortBy?: UsageSortKey, sortOrder?: SortOrder) {
  const key = sortBy ?? 'minuteTs';
  const order = normalizeSortOrder(sortOrder);
  return [...rows].sort((left, right) => {
    let result = 0;
    switch (key) {
      case 'bytesUp':
        result = compareNumber(left.bytesUp, right.bytesUp);
        break;
      case 'bytesDown':
        result = compareNumber(left.bytesDown, right.bytesDown);
        break;
      case 'bytesTotal':
        result = compareNumber(left.bytesUp + left.bytesDown, right.bytesUp + right.bytesDown);
        break;
      case 'flowCount':
        result = compareNumber(left.flowCount, right.flowCount);
        break;
      case 'remoteIp':
        result = compareText(left.remoteIp, right.remoteIp);
        break;
      case 'direction':
        result = compareText(left.direction, right.direction);
        break;
      case 'localPort':
        result = compareNumber(left.localPort, right.localPort);
        break;
      case 'comm':
        result = compareText(left.comm, right.comm);
        break;
      case 'pid':
        result = compareNumber(left.pid, right.pid);
        break;
      case 'minuteTs':
      default:
        result = compareNumber(left.minuteTs, right.minuteTs);
        break;
    }

    if (result !== 0) {
      return applyOrder(result, order);
    }
    return compareNumber(right.minuteTs, left.minuteTs);
  });
}

function sortForwardRows(rows: ForwardUsageRow[], sortBy?: ForwardSortKey, sortOrder?: SortOrder) {
  const key = sortBy ?? 'minuteTs';
  const order = normalizeSortOrder(sortOrder);
  return [...rows].sort((left, right) => {
    let result = 0;
    switch (key) {
      case 'bytesOrig':
        result = compareNumber(left.bytesOrig, right.bytesOrig);
        break;
      case 'bytesReply':
        result = compareNumber(left.bytesReply, right.bytesReply);
        break;
      case 'bytesTotal':
        result = compareNumber(left.bytesOrig + left.bytesReply, right.bytesOrig + right.bytesReply);
        break;
      case 'flowCount':
        result = compareNumber(left.flowCount, right.flowCount);
        break;
      case 'origSrc':
        result = compareText(left.origSrc, right.origSrc);
        break;
      case 'origDst':
        result = compareText(left.origDst, right.origDst);
        break;
      case 'minuteTs':
      default:
        result = compareNumber(left.minuteTs, right.minuteTs);
        break;
    }

    if (result !== 0) {
      return applyOrder(result, order);
    }
    return compareNumber(right.minuteTs, left.minuteTs);
  });
}

function sortProcessRows(
  rows: Array<{
    pid: number | null;
    comm: string | null;
    exe: string | null;
    bytesUp: number;
    bytesDown: number;
    flowCount: number;
    totalBytes: number;
  }>,
  sortBy?: ProcessSortKey,
  sortOrder?: SortOrder,
  allowPID = true,
) {
  const key = sortBy ?? 'bytesTotal';
  const order = normalizeSortOrder(sortOrder);

  return [...rows].sort((left, right) => {
    let result = 0;
    switch (key) {
      case 'comm':
        result = compareText(left.comm, right.comm);
        break;
      case 'pid':
        if (allowPID) {
          result = compareNumber(left.pid, right.pid);
          break;
        }
        result = compareNumber(left.totalBytes, right.totalBytes);
        break;
      case 'bytesUp':
        result = compareNumber(left.bytesUp, right.bytesUp);
        break;
      case 'bytesDown':
        result = compareNumber(left.bytesDown, right.bytesDown);
        break;
      case 'flowCount':
        result = compareNumber(left.flowCount, right.flowCount);
        break;
      case 'bytesTotal':
      default:
        result = compareNumber(left.totalBytes, right.totalBytes);
        break;
    }

    if (result !== 0) {
      return applyOrder(result, order);
    }
    return compareText(left.comm, right.comm);
  });
}

function sortRemoteRows(
  rows: Array<{
    direction: 'in' | 'out';
    remoteIp: string;
    bytesUp: number;
    bytesDown: number;
    flowCount: number;
    totalBytes: number;
  }>,
  sortBy?: RemoteSortKey,
  sortOrder?: SortOrder,
) {
  const key = sortBy ?? 'bytesTotal';
  const order = normalizeSortOrder(sortOrder);

  return [...rows].sort((left, right) => {
    let result = 0;
    switch (key) {
      case 'remoteIp':
        result = compareText(left.remoteIp, right.remoteIp);
        break;
      case 'direction':
        result = compareText(left.direction, right.direction);
        break;
      case 'bytesUp':
        result = compareNumber(left.bytesUp, right.bytesUp);
        break;
      case 'bytesDown':
        result = compareNumber(left.bytesDown, right.bytesDown);
        break;
      case 'flowCount':
        result = compareNumber(left.flowCount, right.flowCount);
        break;
      case 'bytesTotal':
      default:
        result = compareNumber(left.totalBytes, right.totalBytes);
        break;
    }

    if (result !== 0) {
      return applyOrder(result, order);
    }
    return compareText(left.remoteIp, right.remoteIp);
  });
}

function paginate<T>(rows: T[], page = 1, pageSize = 50) {
  const safePage = Math.max(page, 1);
  const safePageSize = Math.min(Math.max(1, pageSize), 200);
  const start = (safePage - 1) * safePageSize;
  return {
    rows: rows.slice(start, start + safePageSize),
    page: safePage,
    pageSize: safePageSize,
    totalRows: rows.length,
  };
}

function createFilteredUsage(query: UsageQuery) {
  return sortUsageRows(filterUsage(query, USAGE_ROWS), query.sortBy, query.sortOrder);
}

function normalizeUsageRows(range: RangeKey, rows: UsageRow[]) {
  void range;
  return rows;
}

function createFilteredForward(query: ForwardUsageQuery) {
  return sortForwardRows(filterForward(query, FORWARD_ROWS), query.sortBy, query.sortOrder);
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
  if (payload.chains.length) return 'high';
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

  const sourceIps = sortIpsByWeight(sourceWeights);
  const targetIps = sortIpsByWeight(targetWeights);
  const chains =
    isShadowsocksRow(row) && (sourceIps.length || targetIps.length)
      ? [
          {
            sourceIp: sourceIps[0] ?? null,
            targetIp: targetIps[0] ?? null,
            targetHost: row.remotePort === 443 ? 'api.example.test' : null,
            targetPort: row.remotePort ?? null,
            localPort: row.localPort ?? null,
            bytesTotal: row.bytesUp + row.bytesDown,
            flowCount: row.flowCount,
            evidenceCount: Math.max(1, row.flowCount),
            evidence: 'ss-log',
            confidence: (targetIps.length && sourceIps.length ? 'high' : 'medium') as UsageExplain['confidence'],
          },
        ]
      : [];

  const basePayload = {
    sourceIps,
    targetIps,
    chains,
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

function processSummaries(
  range: RangeKey,
  query?: { page?: number; pageSize?: number; sortBy?: ProcessSortKey; sortOrder?: SortOrder; groupBy?: ProcessGroupBy },
): ProcessSummaryResponse {
  void range;
  const effectiveGroupBy: ProcessGroupBy = query?.groupBy ?? 'pid';

  const baseRows: ProcessSummaryResponse['rows'] = PROCESSES.map((process) => ({
    pid: process.pid,
    comm: process.comm,
    exe: process.exe,
    bytesUp: Math.round(process.totalBytes * 0.34),
    bytesDown: Math.round(process.totalBytes * 0.66),
    flowCount: Math.max(1, Math.round(process.totalBytes / (32 * 1024 * 1024))),
    totalBytes: process.totalBytes,
  }));

  const groupedRows = effectiveGroupBy === 'comm'
    ? (() => {
        const map = new Map<string, (typeof baseRows)[number]>();
        for (const row of baseRows) {
          const key = row.comm ?? '未知';
          const existing = map.get(key);
          if (!existing) {
            map.set(key, { ...row, pid: null, exe: null });
            continue;
          }
          existing.bytesUp += row.bytesUp;
          existing.bytesDown += row.bytesDown;
          existing.flowCount += row.flowCount;
          existing.totalBytes += row.totalBytes;
        }
        return [...map.values()];
      })()
    : baseRows;

  const sortedRows = sortProcessRows(groupedRows, query?.sortBy, query?.sortOrder, effectiveGroupBy === 'pid');
  const page = paginate(sortedRows, query?.page ?? 1, query?.pageSize ?? 25);
  return {
    dataSource: 'usage_1m',
    rows: page.rows,
    page: page.page,
    pageSize: page.pageSize,
    totalRows: page.totalRows,
  };
}

function remoteSummaries(
  range: RangeKey,
  options?: { page?: number; pageSize?: number; direction?: 'in' | 'out'; includeLoopback?: boolean; sortBy?: RemoteSortKey; sortOrder?: SortOrder },
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
  const rows = [...grouped.values()].map((row) => ({ ...row, totalBytes: row.bytesUp + row.bytesDown }));
  const sortedRows = sortRemoteRows(rows, options?.sortBy, options?.sortOrder);
  const page = paginate(sortedRows, options?.page ?? 1, options?.pageSize ?? 25);
  return {
    dataSource: 'usage_1m',
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
    async getMonthlyUsage() {
      return { rows: MONTHLY_ROWS };
    },
    async getTimeSeries(range, groupBy = 'direction', filters?: TimeSeriesFilters) {
      const basePoints = aggregateSeries(range, RANGE_TO_BUCKET[range]).map((point, index) => {
        const filterModifier = filters?.comm ? 1.18 : filters?.remoteIp ? 1.08 : 1;
        const modifier = (groupBy === 'comm' ? 1.4 : groupBy === 'remote_ip' ? 1.1 : 1) * filterModifier;
        return {
          ...point,
          up: Math.round(point.up * modifier + (index % 3) * 110_000),
          down: Math.round(point.down * modifier + (index % 4) * 160_000),
          flowCount: Math.max(1, Math.round(point.flowCount * modifier)),
        };
      });
      const groups = buildGroupedSeries(basePoints, groupBy);
      return {
        dataSource: 'usage_1m',
        bucket: RANGE_TO_BUCKET[range],
        groupBy,
        points: basePoints,
        groups,
      };
    },
    async getUsage(query) {
      const rows = normalizeUsageRows(query.range, createFilteredUsage(query));
      const page = paginate(rows, query.page ?? 1, query.pageSize ?? 25);
      return {
        dataSource: 'usage_1m',
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
        dataSource: 'usage_1m',
        rows: topRowsFromUsage(createFilteredUsage({ range }), (row) => row.localPort ?? 'unknown').slice(0, 5),
      };
    },
    async getProcesses() {
      return {
        processes: PROCESSES.map(({ totalBytes: _totalBytes, ...process }) => process),
      };
    },
    async getForwardUsage(query) {
      const rows = createFilteredForward(query);
      const page = paginate(rows, query.page ?? 1, query.pageSize ?? 25);
      return {
        dataSource: 'usage_1m_forward',
        rows: page.rows,
        nextCursor: null,
        page: page.page,
        pageSize: page.pageSize,
        totalRows: page.totalRows,
      };
    },
  };
}
