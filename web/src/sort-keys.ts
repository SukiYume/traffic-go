import type {
  ForwardSortKey,
  ProcessSortKey,
  RemoteSortKey,
  UsageSortKey,
} from './types';

// Keep table sort ids and API sort params in one place so page components
// don't drift away from the backend contract during UI-only refactors.
function normalizeSortKey<T extends string>(
  value: string | undefined,
  allowed: readonly T[],
  fallback: T,
) {
  return allowed.includes(value as T) ? (value as T) : fallback;
}

const usageSortKeys = [
  'minuteTs',
  'bytesUp',
  'bytesDown',
  'bytesTotal',
  'flowCount',
  'remoteIp',
  'direction',
  'localPort',
  'comm',
  'pid',
] as const satisfies readonly UsageSortKey[];

const forwardSortKeys = [
  'minuteTs',
  'bytesOrig',
  'bytesReply',
  'bytesTotal',
  'flowCount',
  'origSrc',
  'origDst',
] as const satisfies readonly ForwardSortKey[];

const processSortKeys = [
  'comm',
  'pid',
  'bytesUp',
  'bytesDown',
  'bytesTotal',
  'flowCount',
] as const satisfies readonly ProcessSortKey[];

const remoteSortKeys = [
  'remoteIp',
  'direction',
  'bytesUp',
  'bytesDown',
  'bytesTotal',
  'flowCount',
] as const satisfies readonly RemoteSortKey[];

export function normalizeUsageSortKey(value?: string): UsageSortKey {
  return normalizeSortKey(value, usageSortKeys, 'minuteTs');
}

export function normalizeForwardSortKey(value?: string): ForwardSortKey {
  return normalizeSortKey(value, forwardSortKeys, 'minuteTs');
}

export function normalizeProcessSortKey(value?: string): ProcessSortKey {
  return normalizeSortKey(value, processSortKeys, 'bytesTotal');
}

export function normalizeRemoteSortKey(value?: string): RemoteSortKey {
  return normalizeSortKey(value, remoteSortKeys, 'bytesTotal');
}

export function usageSortParam(value?: UsageSortKey) {
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
  }[normalizeUsageSortKey(value)];
}

export function forwardSortParam(value?: ForwardSortKey) {
  return {
    minuteTs: undefined,
    bytesOrig: 'bytes_orig',
    bytesReply: 'bytes_reply',
    bytesTotal: 'bytes_total',
    flowCount: 'flow_count',
    origSrc: 'orig_src_ip',
    origDst: 'orig_dst_ip',
  }[normalizeForwardSortKey(value)];
}

export function processSortParam(value?: ProcessSortKey) {
  return {
    comm: 'comm',
    pid: 'pid',
    bytesUp: 'bytes_up',
    bytesDown: 'bytes_down',
    bytesTotal: 'bytes_total',
    flowCount: 'flow_count',
  }[normalizeProcessSortKey(value)];
}

export function remoteSortParam(value?: RemoteSortKey) {
  return {
    remoteIp: 'remote_ip',
    direction: 'direction',
    bytesUp: 'bytes_up',
    bytesDown: 'bytes_down',
    bytesTotal: 'bytes_total',
    flowCount: 'flow_count',
  }[normalizeRemoteSortKey(value)];
}
