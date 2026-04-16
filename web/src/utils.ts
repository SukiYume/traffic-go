import type { BucketKey, DataSource, Direction, RangeKey } from './types';

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'];

export function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0 B';
  let current = value;
  let unit = 0;
  while (current >= 1024 && unit < BYTE_UNITS.length - 1) {
    current /= 1024;
    unit += 1;
  }
  return `${current >= 100 ? current.toFixed(0) : current >= 10 ? current.toFixed(1) : current.toFixed(2)} ${BYTE_UNITS[unit]}`;
}

export function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN').format(value);
}

export function formatDateTime(ts: number) {
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(ts * 1000);
}

export function formatLongDateTime(ts: number) {
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(ts * 1000);
}

export function rangeLabel(range: RangeKey) {
  return {
    '1h': '1 小时',
    '24h': '24 小时',
    '7d': '7 天',
    '30d': '30 天',
    '90d': '90 天',
  }[range];
}

export function isLongRange(range: RangeKey) {
  return range === '90d';
}

export function bucketLabel(bucket: BucketKey) {
  return {
    '1m': '1 分钟',
    '5m': '5 分钟',
    '1h': '1 小时',
    '6h': '6 小时',
    '1d': '1 天',
  }[bucket];
}

export function dataSourceLabel(dataSource: DataSource) {
  return {
    usage_1m: '分钟明细',
    usage_1h: '小时聚合',
    usage_1m_forward: '分钟转发明细',
    usage_1h_forward: '小时转发聚合',
  }[dataSource];
}

export function dataSourceDescription(dataSource: DataSource) {
  return {
    usage_1m: '逐分钟累计的明细数据，支持 PID / EXE 等细粒度维度。',
    usage_1h: '按小时聚合后的历史数据，长时间范围会自动切换到这里。',
    usage_1m_forward: '逐分钟累计的 forward / NAT 明细。',
    usage_1h_forward: '按小时聚合后的 forward / NAT 历史数据。',
  }[dataSource];
}

export function peerRoleLabel(direction: Exclude<Direction, 'forward'>) {
  return direction === 'in' ? '访问 VPS 的来源' : 'VPS 访问的目标';
}

export function directionLabel(direction: Direction) {
  return {
    in: '入站',
    out: '出站',
    forward: '转发',
  }[direction];
}

export function chartTickLabel(ts: number, range: RangeKey) {
  const options: Intl.DateTimeFormatOptions =
    range === '1h'
      ? { hour: '2-digit', minute: '2-digit' }
      : range === '24h'
        ? { month: '2-digit', day: '2-digit', hour: '2-digit' }
        : range === '7d'
          ? { month: '2-digit', day: '2-digit', hour: '2-digit' }
          : { month: '2-digit', day: '2-digit' };
  return new Intl.DateTimeFormat('zh-CN', options).format(ts * 1000);
}

export function safeText(value?: string | null) {
  return value && value.trim() ? value : '未知';
}

export function clampText(value: string, max = 32) {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 1)}…`;
}
