import type {
  BucketKey,
  ForwardUsageResponse,
  ForwardUsageRow,
  ForwardSortKey,
  GroupBy,
  OverviewStats,
  ProcessSortKey,
  ProcessesResponse,
  ProcessSummaryResponse,
  RangeKey,
  RemoteSortKey,
  RemoteSummaryResponse,
  SortOrder,
  TimeSeriesFilters,
  TimeSeriesResponse,
  TopResponse,
  TrafficApiClient,
  UsageQuery,
  UsageResponse,
  UsageRow,
  UsageSortKey,
} from './types';
import { withAppBase } from './base-path';
import { createMockApiClient } from './data/mock';
import { RANGE_TO_BUCKET } from './ranges';

type QueryValue = string | number | boolean | undefined | null;

type RawOverviewResponse = {
  data: {
    range: RangeKey;
    data_source: OverviewStats['dataSource'];
    bytes_up: number;
    bytes_down: number;
    flow_count: number;
    active_connections: number;
    active_processes: number;
  };
};

type RawTimeseriesResponse = {
  data_source: TimeSeriesResponse['dataSource'];
  data: Array<{
    bucket_ts: number;
    group: string;
    bytes_up: number;
    bytes_down: number;
    flow_count: number;
  }>;
};

type RawUsageResponse = {
  data_source: UsageResponse['dataSource'];
  next_cursor: string | null;
  page?: number;
  page_size?: number;
  total_rows?: number;
  data: Array<{
    time_bucket: number;
    proto: UsageRow['proto'];
    direction: UsageRow['direction'];
    pid: number | null;
    comm: string;
    exe: string | null;
    local_port: number;
    remote_ip: string;
    remote_port: number | null;
    attribution: UsageRow['attribution'] | '';
    bytes_up: number;
    bytes_down: number;
    pkts_up: number;
    pkts_down: number;
    flow_count: number;
  }>;
};

type RawTopResponse = {
  data_source: TopResponse['dataSource'];
  data: Array<{
    key: string;
    bytes_up: number;
    bytes_down: number;
  }>;
};

type RawProcessesResponse = {
  data: Array<{
    pid: number;
    comm: string;
    exe: string;
  }>;
};

type RawProcessSummaryResponse = {
  data_source: ProcessSummaryResponse['dataSource'];
  page?: number;
  page_size?: number;
  total_rows?: number;
  data: Array<{
    pid: number | null;
    comm: string;
    exe: string | null;
    bytes_up: number;
    bytes_down: number;
    flow_count: number;
  }>;
};

type RawRemoteSummaryResponse = {
  data_source: RemoteSummaryResponse['dataSource'];
  page?: number;
  page_size?: number;
  total_rows?: number;
  data: Array<{
    direction: 'in' | 'out';
    remote_ip: string;
    bytes_up: number;
    bytes_down: number;
    flow_count: number;
  }>;
};

type RawForwardResponse = {
  data_source: ForwardUsageResponse['dataSource'];
  next_cursor: string | null;
  page?: number;
  page_size?: number;
  total_rows?: number;
  data: Array<{
    time_bucket: number;
    proto: ForwardUsageRow['proto'];
    orig_src_ip: string;
    orig_dst_ip: string;
    orig_sport: number;
    orig_dport: number;
    bytes_orig: number;
    bytes_reply: number;
    pkts_orig: number;
    pkts_reply: number;
    flow_count: number;
  }>;
};

function buildQuery(entries: Array<[string, QueryValue]>) {
  const search = new URLSearchParams();
  for (const [key, value] of entries) {
    if (value === undefined || value === null || value === '') continue;
    search.set(key, String(value));
  }
  const query = search.toString();
  return query ? `?${query}` : '';
}

function usageSortKey(value?: UsageSortKey) {
  return {
    minuteTs: undefined,
    bytesUp: 'bytes_up',
    bytesDown: 'bytes_down',
    bytesTotal: 'bytes_total',
    flowCount: 'flow_count',
    remoteIp: 'remote_ip',
    direction: 'direction',
    localPort: 'local_port',
    comm: 'comm',
    pid: 'pid',
  }[value ?? 'minuteTs'];
}

function forwardSortKey(value?: ForwardSortKey) {
  return {
    minuteTs: undefined,
    bytesOrig: 'bytes_orig',
    bytesReply: 'bytes_reply',
    bytesTotal: 'bytes_total',
    flowCount: 'flow_count',
    origSrc: 'orig_src_ip',
    origDst: 'orig_dst_ip',
  }[value ?? 'minuteTs'];
}

function processSortKey(value?: ProcessSortKey) {
  return {
    comm: 'comm',
    pid: 'pid',
    bytesUp: 'bytes_up',
    bytesDown: 'bytes_down',
    bytesTotal: 'total',
    flowCount: 'flow_count',
  }[value ?? 'bytesTotal'];
}

function remoteSortKey(value?: RemoteSortKey) {
  return {
    remoteIp: 'remote_ip',
    direction: 'direction',
    bytesUp: 'bytes_up',
    bytesDown: 'bytes_down',
    bytesTotal: 'total',
    flowCount: 'flow_count',
  }[value ?? 'bytesTotal'];
}

function buildUsageQuery(query: UsageQuery) {
  return buildQuery([
    ['range', query.range],
    ['comm', query.comm],
    ['pid', query.pid],
    ['exe', query.exe],
    ['remote_ip', query.remoteIp],
    ['local_port', query.localPort],
    ['direction', query.direction],
    ['proto', query.proto],
    ['attribution', query.attribution],
    ['cursor', query.cursor],
    ['limit', query.limit ?? undefined],
    ['page', query.page ?? undefined],
    ['page_size', query.pageSize ?? undefined],
    ['sort_by', usageSortKey(query.sortBy)],
    ['sort_order', query.sortOrder ?? undefined],
  ]);
}

function buildForwardQuery(query: UsageQuery) {
  return buildQuery([
    ['range', query.range],
    ['proto', query.proto],
    ['cursor', query.cursor],
    ['limit', query.limit ?? undefined],
    ['page', query.page ?? undefined],
    ['page_size', query.pageSize ?? undefined],
    ['sort_by', forwardSortKey(query.sortBy as ForwardSortKey | undefined)],
    ['sort_order', query.sortOrder ?? undefined],
  ]);
}

function buildTopProcessesQuery(
  range: RangeKey,
  options?: { page?: number; pageSize?: number; sortBy?: ProcessSortKey; sortOrder?: SortOrder },
) {
  return buildQuery([
    ['range', range],
    ['page', options?.page ?? undefined],
    ['page_size', options?.pageSize ?? undefined],
    ['sort_by', processSortKey(options?.sortBy)],
    ['sort_order', options?.sortOrder ?? undefined],
  ]);
}

function buildTopRemotesQuery(
  range: RangeKey,
  options?: { page?: number; pageSize?: number; sortBy?: RemoteSortKey; sortOrder?: SortOrder; direction?: 'in' | 'out'; includeLoopback?: boolean },
) {
  return buildQuery([
    ['range', range],
    ['page', options?.page ?? undefined],
    ['page_size', options?.pageSize ?? undefined],
    ['sort_by', remoteSortKey(options?.sortBy)],
    ['sort_order', options?.sortOrder ?? undefined],
    ['direction', options?.direction],
    ['include_loopback', options?.includeLoopback ? 1 : undefined],
  ]);
}

function buildTimeSeriesQuery(range: RangeKey, bucket: BucketKey, groupBy: GroupBy, filters?: TimeSeriesFilters) {
  return buildQuery([
    ['range', range],
    ['bucket', bucket],
    ['group_by', groupBy],
    ['comm', filters?.comm],
    ['pid', filters?.pid],
    ['exe', filters?.exe],
    ['remote_ip', filters?.remoteIp],
    ['direction', filters?.direction],
    ['proto', filters?.proto],
  ]);
}

async function requestJson<T>(path: string, decode: (raw: unknown) => T): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return decode(await response.json());
}

function formatPointLabel(ts: number, bucket: BucketKey) {
  const options: Intl.DateTimeFormatOptions =
    bucket === '1d'
      ? { month: '2-digit', day: '2-digit' }
      : bucket === '1h' || bucket === '6h'
        ? { month: '2-digit', day: '2-digit', hour: '2-digit' }
        : { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' };
  return new Intl.DateTimeFormat('zh-CN', options).format(ts * 1000);
}

function decodeOverview(raw: unknown): OverviewStats {
  const payload = raw as RawOverviewResponse;
  return {
    bytesUp: payload.data.bytes_up,
    bytesDown: payload.data.bytes_down,
    flowCount: payload.data.flow_count,
    activeConnections: payload.data.active_connections,
    activeProcesses: payload.data.active_processes,
    dataSource: payload.data.data_source,
    range: payload.data.range,
  };
}

function decodeTimeSeries(raw: unknown, bucket: BucketKey): TimeSeriesResponse {
  const payload = raw as RawTimeseriesResponse;
  const byBucket = new Map<number, { up: number; down: number; flowCount: number }>();
  for (const point of payload.data ?? []) {
    const current = byBucket.get(point.bucket_ts) ?? { up: 0, down: 0, flowCount: 0 };
    current.up += point.bytes_up;
    current.down += point.bytes_down;
    current.flowCount += point.flow_count;
    byBucket.set(point.bucket_ts, current);
  }

  return {
    dataSource: payload.data_source,
    bucket,
    points: [...byBucket.entries()]
      .sort((left, right) => left[0] - right[0])
      .map(([ts, value]) => ({
        ts,
        up: value.up,
        down: value.down,
        flowCount: value.flowCount,
        label: formatPointLabel(ts, bucket),
      })),
  };
}

function decodeUsage(raw: unknown): UsageResponse {
  const payload = raw as RawUsageResponse;
  return {
    dataSource: payload.data_source,
    nextCursor: payload.next_cursor,
    page: payload.page ?? 1,
    pageSize: payload.page_size ?? (payload.data?.length ?? 0),
    totalRows: payload.total_rows ?? payload.data?.length ?? 0,
    rows: (payload.data ?? []).map((row) => ({
      minuteTs: row.time_bucket,
      proto: row.proto,
      direction: row.direction,
      pid: row.pid ?? null,
      comm: row.comm || null,
      exe: row.exe ?? null,
      localPort: row.local_port || null,
      remoteIp: row.remote_ip || null,
      remotePort: row.remote_port ?? null,
      attribution: row.attribution ? row.attribution : null,
      bytesUp: row.bytes_up,
      bytesDown: row.bytes_down,
      pktsUp: row.pkts_up,
      pktsDown: row.pkts_down,
      flowCount: row.flow_count,
    })),
  };
}

function decodeTop(raw: unknown): TopResponse {
  const payload = raw as RawTopResponse;
  return {
    dataSource: payload.data_source,
    rows: (payload.data ?? []).map((row) => ({
      label: row.key,
      value: row.bytes_up + row.bytes_down,
      bytesUp: row.bytes_up,
      bytesDown: row.bytes_down,
    })),
  };
}

function decodeProcesses(raw: unknown): ProcessesResponse {
  const payload = raw as RawProcessesResponse;
  return {
    processes: (payload.data ?? []).map((process) => ({
      pid: process.pid,
      comm: process.comm,
      exe: process.exe,
      totalBytes: 0,
    })),
  };
}

function decodeProcessSummary(raw: unknown): ProcessSummaryResponse {
  const payload = raw as RawProcessSummaryResponse;
  return {
    dataSource: payload.data_source,
    page: payload.page ?? 1,
    pageSize: payload.page_size ?? (payload.data?.length ?? 0),
    totalRows: payload.total_rows ?? payload.data?.length ?? 0,
    rows: (payload.data ?? []).map((row) => ({
      pid: row.pid ?? null,
      comm: row.comm || null,
      exe: row.exe ?? null,
      bytesUp: row.bytes_up,
      bytesDown: row.bytes_down,
      flowCount: row.flow_count,
      totalBytes: row.bytes_up + row.bytes_down,
    })),
  };
}

function decodeRemoteSummary(raw: unknown): RemoteSummaryResponse {
  const payload = raw as RawRemoteSummaryResponse;
  return {
    dataSource: payload.data_source,
    page: payload.page ?? 1,
    pageSize: payload.page_size ?? (payload.data?.length ?? 0),
    totalRows: payload.total_rows ?? payload.data?.length ?? 0,
    rows: (payload.data ?? []).map((row) => ({
      direction: row.direction,
      remoteIp: row.remote_ip || null,
      bytesUp: row.bytes_up,
      bytesDown: row.bytes_down,
      flowCount: row.flow_count,
      totalBytes: row.bytes_up + row.bytes_down,
    })),
  };
}

function decodeForwardUsage(raw: unknown): ForwardUsageResponse {
  const payload = raw as RawForwardResponse;
  return {
    dataSource: payload.data_source,
    nextCursor: payload.next_cursor,
    page: payload.page ?? 1,
    pageSize: payload.page_size ?? (payload.data?.length ?? 0),
    totalRows: payload.total_rows ?? payload.data?.length ?? 0,
    rows: (payload.data ?? []).map((row) => ({
      minuteTs: row.time_bucket,
      proto: row.proto,
      origSrc: row.orig_src_ip,
      origDst: row.orig_dst_ip,
      origSport: row.orig_sport,
      origDport: row.orig_dport,
      bytesOrig: row.bytes_orig,
      bytesReply: row.bytes_reply,
      pktsOrig: row.pkts_orig,
      pktsReply: row.pkts_reply,
      flowCount: row.flow_count,
    })),
  };
}

export function createHttpClient(): TrafficApiClient {
  return {
    getOverview(range) {
      return requestJson(withAppBase(`/api/v1/stats/overview${buildQuery([['range', range]])}`), decodeOverview);
    },
    getTimeSeries(range, groupBy: GroupBy = 'direction', filters) {
      const bucket = RANGE_TO_BUCKET[range];
      return requestJson(
        withAppBase(`/api/v1/stats/timeseries${buildTimeSeriesQuery(range, bucket, groupBy, filters)}`),
        (raw) => decodeTimeSeries(raw, bucket),
      );
    },
    getUsage(query: UsageQuery) {
      return requestJson(withAppBase(`/api/v1/usage${buildUsageQuery(query)}`), decodeUsage);
    },
    getTopProcesses(range, options) {
      return requestJson(withAppBase(`/api/v1/top/processes${buildTopProcessesQuery(range, options)}`), decodeProcessSummary);
    },
    getTopRemotes(range, options) {
      return requestJson(withAppBase(`/api/v1/top/remotes${buildTopRemotesQuery(range, options)}`), decodeRemoteSummary);
    },
    getTopPorts(range) {
      return requestJson(withAppBase(`/api/v1/top/ports${buildQuery([['range', range], ['by', 'total']])}`), decodeTop);
    },
    getProcesses() {
      return requestJson(withAppBase('/api/v1/processes'), decodeProcesses);
    },
    getForwardUsage(query: UsageQuery) {
      return requestJson(withAppBase(`/api/v1/forward/usage${buildForwardQuery(query)}`), decodeForwardUsage);
    },
  };
}

export function shouldUseMockApi(envValue?: string) {
  return envValue === '1';
}

export function createAppApiClient(): TrafficApiClient {
  if (shouldUseMockApi(import.meta.env.VITE_TRAFFICGO_USE_MOCK)) {
    return createMockApiClient();
  }
  return createHttpClient();
}

export { RANGE_TO_BUCKET };
