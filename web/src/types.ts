export type RangeKey = '1h' | '24h' | '7d' | '30d' | '90d';
export type BucketKey = '1m' | '5m' | '1h' | '6h' | '1d';
export type GroupBy = 'direction' | 'comm' | 'remote_ip';
export type Direction = 'in' | 'out' | 'forward';
export type Attribution = 'exact' | 'unknown';
export type DataSource = 'usage_1m' | 'usage_1h' | 'usage_1m_forward' | 'usage_1h_forward';
export type SortOrder = 'asc' | 'desc';
export type UsageSortKey = 'minuteTs' | 'bytesUp' | 'bytesDown' | 'bytesTotal' | 'flowCount' | 'remoteIp' | 'direction' | 'localPort' | 'comm' | 'pid';
export type ForwardSortKey = 'minuteTs' | 'bytesOrig' | 'bytesReply' | 'bytesTotal' | 'flowCount' | 'origSrc' | 'origDst';
export type RemoteSortKey = 'remoteIp' | 'direction' | 'bytesUp' | 'bytesDown' | 'bytesTotal' | 'flowCount';
export type ProcessSortKey = 'comm' | 'pid' | 'bytesUp' | 'bytesDown' | 'bytesTotal' | 'flowCount';

export interface OverviewStats {
  bytesUp: number;
  bytesDown: number;
  activeConnections: number;
  activeProcesses: number;
  flowCount?: number;
  dataSource: DataSource;
  range: RangeKey;
}

export interface TimeSeriesPoint {
  ts: number;
  up: number;
  down: number;
  flowCount: number;
  label: string;
}

export interface TimeSeriesResponse {
  dataSource: DataSource;
  bucket: BucketKey;
  points: TimeSeriesPoint[];
}

export interface TimeSeriesFilters {
  comm?: string;
  pid?: number;
  exe?: string;
  remoteIp?: string;
  direction?: string;
  proto?: string;
}

export interface UsageRow {
  minuteTs: number;
  proto: 'tcp' | 'udp';
  direction: Exclude<Direction, 'forward'>;
  pid: number | null;
  comm: string | null;
  exe: string | null;
  localPort: number | null;
  remoteIp: string | null;
  remotePort: number | null;
  attribution: Attribution | null;
  bytesUp: number;
  bytesDown: number;
  pktsUp: number;
  pktsDown: number;
  flowCount: number;
}

export interface ForwardUsageRow {
  minuteTs: number;
  proto: 'tcp' | 'udp';
  origSrc: string;
  origDst: string;
  origSport: number;
  origDport: number;
  bytesOrig: number;
  bytesReply: number;
  pktsOrig: number;
  pktsReply: number;
  flowCount: number;
}

export interface TopRow {
  label: string;
  value: number;
  bytesUp: number;
  bytesDown: number;
  meta?: string;
}

export interface ProcessOption {
  pid: number;
  comm: string;
  exe: string;
  totalBytes: number;
}

export interface UsageQuery {
  range: RangeKey;
  comm?: string;
  pid?: string;
  exe?: string;
  remoteIp?: string;
  localPort?: string;
  direction?: string;
  proto?: string;
  attribution?: string;
  cursor?: string;
  limit?: number;
  page?: number;
  pageSize?: number;
  sortBy?: UsageSortKey;
  sortOrder?: SortOrder;
}

export interface PaginationMeta {
  page: number;
  pageSize: number;
  totalRows: number;
}

export interface UsageResponse {
  dataSource: DataSource;
  rows: UsageRow[];
  nextCursor: string | null;
  page: number;
  pageSize: number;
  totalRows: number;
}

export interface ForwardUsageResponse {
  dataSource: DataSource;
  rows: ForwardUsageRow[];
  nextCursor: string | null;
  page: number;
  pageSize: number;
  totalRows: number;
}

export interface ProcessSummaryRow {
  pid: number | null;
  comm: string | null;
  exe: string | null;
  bytesUp: number;
  bytesDown: number;
  flowCount: number;
  totalBytes: number;
}

export interface ProcessSummaryResponse extends PaginationMeta {
  dataSource: DataSource;
  rows: ProcessSummaryRow[];
}

export interface RemoteSummaryRow {
  direction: Exclude<Direction, 'forward'>;
  remoteIp: string | null;
  bytesUp: number;
  bytesDown: number;
  flowCount: number;
  totalBytes: number;
}

export interface RemoteSummaryResponse extends PaginationMeta {
  dataSource: DataSource;
  rows: RemoteSummaryRow[];
}

export interface TopResponse {
  dataSource: DataSource;
  rows: TopRow[];
}

export interface ProcessesResponse {
  processes: ProcessOption[];
}

export interface TrafficApiClient {
  getOverview(range: RangeKey): Promise<OverviewStats>;
  getTimeSeries(range: RangeKey, groupBy?: GroupBy, filters?: TimeSeriesFilters): Promise<TimeSeriesResponse>;
  getUsage(query: UsageQuery): Promise<UsageResponse>;
  getTopProcesses(range: RangeKey, options?: { page?: number; pageSize?: number; sortBy?: ProcessSortKey; sortOrder?: SortOrder }): Promise<ProcessSummaryResponse>;
  getTopRemotes(range: RangeKey, options?: { page?: number; pageSize?: number; sortBy?: RemoteSortKey; sortOrder?: SortOrder; direction?: Exclude<Direction, 'forward'>; includeLoopback?: boolean }): Promise<RemoteSummaryResponse>;
  getTopPorts(range: RangeKey): Promise<TopResponse>;
  getProcesses(): Promise<ProcessesResponse>;
  getForwardUsage(query: UsageQuery): Promise<ForwardUsageResponse>;
}
