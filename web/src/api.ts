import type {
  BucketKey,
  ForwardUsageQuery,
  ForwardUsageResponse,
  ForwardUsageRow,
  ForwardSortKey,
  GroupBy,
  OverviewStats,
  ProcessGroupBy,
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
  ApiRequestOptions,
  UsageExplain,
  UsageQuery,
  UsageResponse,
  UsageRow,
  UsageSortKey,
} from './types';
import { withAppBase } from './base-path';
import { createMockApiClient } from './data/mock';
import { RANGE_TO_BUCKET } from './ranges';
import {
  forwardSortParam,
  normalizeUsageSortKey,
  processSortParam,
  remoteSortParam,
  usageSortParam,
} from './sort-keys';

type QueryValue = string | number | boolean | undefined | null;
type ListQueryOptions<SortKey extends string> = {
  cursor?: string;
  limit?: number;
  page?: number;
  pageSize?: number;
  sortBy?: SortKey;
  sortOrder?: SortOrder;
};

type RawErrorResponse = {
  error?: string;
  message?: string;
};

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

type RawUsageExplainResponse = {
  data: {
    process: string;
    confidence: UsageExplain['confidence'];
    source_ips: string[];
    target_ips: string[];
    chains?: Array<{
      chain_id?: string;
      source_ip?: string;
      target_ip?: string;
      target_host?: string;
      target_host_normalized?: string;
      target_port?: number | null;
      local_port?: number | null;
      bytes_total: number;
      flow_count: number;
      evidence_count?: number;
      evidence: string;
      evidence_source?: string;
      sample_fingerprint?: string;
      sample_message?: string;
      sample_time?: number;
      confidence: UsageExplain['confidence'];
    }>;
    related_peers: Array<{
      direction: UsageRow['direction'];
      remote_ip: string;
      remote_port?: number | null;
      local_port?: number | null;
      bytes_total: number;
      flow_count: number;
    }>;
    nginx_requests: Array<{
      time: number;
      method: string;
      host?: string;
      host_normalized?: string;
      path: string;
      status: number;
      count?: number;
      client_ip?: string;
      referer?: string;
      user_agent?: string;
      bot?: string;
      sample_fingerprint?: string;
    }>;
    notes: string[];
  };
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

function appendListQuery<SortKey extends string>(
  entries: Array<[string, QueryValue]>,
  options: ListQueryOptions<SortKey>,
  sortParam: QueryValue,
) {
  return [
    ...entries,
    ['cursor', options.cursor],
    ['limit', options.limit ?? undefined],
    ['page', options.page ?? undefined],
    ['page_size', options.pageSize ?? undefined],
    ['sort_by', sortParam],
    ['sort_order', options.sortOrder ?? undefined],
  ] as Array<[string, QueryValue]>;
}

function buildUsageQuery(query: UsageQuery) {
  return buildQuery(
    appendListQuery(
      [
        ['range', query.range],
        ['comm', query.comm],
        ['pid', query.pid],
        ['exe', query.exe],
        ['remote_ip', query.remoteIp],
        ['local_port', query.localPort],
        ['direction', query.direction],
        ['proto', query.proto],
        ['attribution', query.attribution],
      ],
      query,
      usageSortParam(query.sortBy),
    ),
  );
}

function buildForwardQuery(query: ForwardUsageQuery) {
  return buildQuery(
    appendListQuery(
      [
        ['range', query.range],
        ['proto', query.proto],
        ['orig_src_ip', query.origSrcIp],
        ['orig_dst_ip', query.origDstIp],
      ],
      query,
      forwardSortParam(query.sortBy),
    ),
  );
}

function buildUsageExplainQuery(row: UsageRow, options?: { dataSource?: UsageResponse['dataSource']; allowScan?: boolean }) {
  return buildQuery([
    ['ts', row.minuteTs],
    ['data_source', options?.dataSource],
    ['proto', row.proto],
    ['direction', row.direction],
    ['pid', row.pid ?? undefined],
    ['comm', row.comm],
    ['exe', row.exe],
    ['local_port', row.localPort],
    ['remote_ip', row.remoteIp],
    ['remote_port', row.remotePort],
    ['scan', options?.allowScan ? 1 : undefined],
  ]);
}

function buildTopProcessesQuery(
  range: RangeKey,
  options?: { page?: number; pageSize?: number; sortBy?: ProcessSortKey; sortOrder?: SortOrder; groupBy?: ProcessGroupBy },
) {
  return buildQuery([
    ['range', range],
    ['page', options?.page ?? undefined],
    ['page_size', options?.pageSize ?? undefined],
    ['sort_by', processSortParam(options?.sortBy)],
    ['sort_order', options?.sortOrder ?? undefined],
    ['group_by', options?.groupBy],
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
    ['sort_by', remoteSortParam(options?.sortBy)],
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

export class ApiError extends Error {
  status: number;
  code?: string;

  constructor(status: number, code: string | undefined, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

export function isDimensionUnavailableError(error: unknown) {
  return error instanceof ApiError && error.code === 'dimension_unavailable';
}

async function requestJson<T>(path: string, decode: (raw: unknown) => T, options?: ApiRequestOptions): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: 'application/json' },
    signal: options?.signal,
  });
  if (!response.ok) {
    let payload: RawErrorResponse | null = null;
    try {
      payload = (await response.json()) as RawErrorResponse;
    } catch {
      payload = null;
    }
    throw new ApiError(response.status, payload?.error, payload?.message ?? `Request failed: ${response.status}`);
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

function decodeUsageExplain(raw: unknown): UsageExplain {
  const payload = raw as RawUsageExplainResponse;
  return {
    process: payload.data.process,
    confidence: payload.data.confidence,
    sourceIps: payload.data.source_ips ?? [],
    targetIps: payload.data.target_ips ?? [],
    chains: (payload.data.chains ?? []).map((chain) => ({
      chainId: chain.chain_id ?? null,
      sourceIp: chain.source_ip ?? null,
      targetIp: chain.target_ip ?? null,
      targetHost: chain.target_host ?? null,
      targetHostNormalized: chain.target_host_normalized ?? null,
      targetPort: chain.target_port ?? null,
      localPort: chain.local_port ?? null,
      bytesTotal: chain.bytes_total,
      flowCount: chain.flow_count,
      evidenceCount: chain.evidence_count ?? 0,
      evidence: chain.evidence,
      evidenceSource: chain.evidence_source ?? null,
      sampleFingerprint: chain.sample_fingerprint ?? null,
      sampleMessage: chain.sample_message ?? null,
      sampleTime: chain.sample_time ?? null,
      confidence: chain.confidence,
    })),
    relatedPeers: (payload.data.related_peers ?? []).map((peer) => ({
      direction: peer.direction,
      remoteIp: peer.remote_ip,
      remotePort: peer.remote_port ?? null,
      localPort: peer.local_port ?? null,
      bytesTotal: peer.bytes_total,
      flowCount: peer.flow_count,
    })),
    nginxRequests: (payload.data.nginx_requests ?? []).map((request) => ({
      time: request.time,
      method: request.method,
      host: request.host ?? null,
      hostNormalized: request.host_normalized ?? null,
      path: request.path,
      status: request.status,
      count: request.count ?? 1,
      clientIp: request.client_ip ?? null,
      referer: request.referer ?? null,
      userAgent: request.user_agent ?? null,
      bot: request.bot ?? null,
      sampleFingerprint: request.sample_fingerprint ?? null,
    })),
    notes: payload.data.notes ?? [],
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
      comm: row.comm ?? null,
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
    getOverview(range, requestOptions) {
      return requestJson(withAppBase(`/api/v1/stats/overview${buildQuery([['range', range]])}`), decodeOverview, requestOptions);
    },
    getTimeSeries(range, groupBy: GroupBy = 'direction', filters, requestOptions) {
      const bucket = RANGE_TO_BUCKET[range];
      return requestJson(
        withAppBase(`/api/v1/stats/timeseries${buildTimeSeriesQuery(range, bucket, groupBy, filters)}`),
        (raw) => decodeTimeSeries(raw, bucket),
        requestOptions,
      );
    },
    getUsage(query: UsageQuery, requestOptions) {
      return requestJson(withAppBase(`/api/v1/usage${buildUsageQuery(query)}`), decodeUsage, requestOptions);
    },
    getUsageExplain(row: UsageRow, options, requestOptions) {
      return requestJson(withAppBase(`/api/v1/usage/explain${buildUsageExplainQuery(row, options)}`), decodeUsageExplain, requestOptions);
    },
    getTopProcesses(range, options, requestOptions) {
      return requestJson(withAppBase(`/api/v1/top/processes${buildTopProcessesQuery(range, options)}`), decodeProcessSummary, requestOptions);
    },
    getTopRemotes(range, options, requestOptions) {
      return requestJson(withAppBase(`/api/v1/top/remotes${buildTopRemotesQuery(range, options)}`), decodeRemoteSummary, requestOptions);
    },
    getTopPorts(range, requestOptions) {
      return requestJson(withAppBase(`/api/v1/top/ports${buildQuery([['range', range], ['by', 'total']])}`), decodeTop, requestOptions);
    },
    getProcesses(requestOptions) {
      return requestJson(withAppBase('/api/v1/processes'), decodeProcesses, requestOptions);
    },
    getForwardUsage(query: ForwardUsageQuery, requestOptions) {
      return requestJson(withAppBase(`/api/v1/forward/usage${buildForwardQuery(query)}`), decodeForwardUsage, requestOptions);
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
export { normalizeUsageSortKey };
