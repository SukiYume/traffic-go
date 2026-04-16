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

export function dataSourceAutoNote(dataSource: DataSource) {
  return dataSource.endsWith('_1h')
    ? '当前页面正在读取小时聚合历史。它不是一个手动筛选项，时间范围或维度变化后会自动切换。'
    : '当前页面正在读取分钟明细。它不是一个手动筛选项，时间范围或维度变化后会自动切换。';
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

export function executableName(value?: string | null): string | null {
  if (!value || !value.trim()) return null;
  const trimmed = value.trim().replace(/^['"]+|['"]+$/g, '');
  const token = trimmed.split(/\s+/)[0] ?? '';
  const normalized = token.replace(/\\/g, '/').replace(/\/+$/, '');
  if (!normalized) return null;
  const last = normalized.split('/').pop();
  if (!last || !last.trim()) return normalized;
  return last.trim();
}

export function displayExecutableName(value?: string | null) {
  return executableName(value) ?? '未知';
}

export function clampText(value: string, max = 32) {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 1)}…`;
}

export function attributionLabel(value: string | null | undefined) {
  if (value === 'exact') return 'exact';
  if (value === 'heuristic') return 'heuristic';
  if (value === 'guess') return 'guess';
  if (value === 'unknown') return 'unknown';
  return '未标记';
}

export function attributionDescription(value: string | null | undefined) {
  if (value === 'exact') {
    return '通过 conntrack 与 socket / 进程映射成功命中，当前流量可以明确归到这个进程。';
  }
  if (value === 'heuristic') {
    return '通过本地端口、协议等规则推断到进程，准确度通常较高，但不如 exact 严格。';
  }
  if (value === 'guess') {
    return '基于短时间窗口内的历史命中做延续猜测，适合排查参考，不建议单独作为审计结论。';
  }
  if (value === 'unknown') {
    return '只看到了流量，但无法稳定映射到具体进程，常见于短连接、UDP 无连接场景或 /proc 信息缺失。';
  }
  return '当前数据源没有这列，或者该条记录没有归因信息。';
}

type PortEntry = string | Record<string, string>;

const PORT_NAMES: Record<number, PortEntry> = {
  21: 'FTP',
  22: 'SSH',
  25: 'SMTP',
  53: 'DNS',
  80: 'HTTP',
  110: 'POP3',
  143: 'IMAP',
  443: { tcp: 'HTTPS', udp: 'QUIC' },
  587: 'SMTP',
  993: 'IMAPS',
  995: 'POP3S',
  1194: 'OpenVPN',
  3306: 'MySQL',
  3478: 'STUN',
  5432: 'PostgreSQL',
  6379: 'Redis',
  8080: 'HTTP-alt',
  8443: 'HTTPS-alt',
  19302: 'STUN',
  51820: 'WireGuard',
};

export function serviceNameForPort(port: number | null | undefined, proto?: string): string | null {
  if (port == null) return null;
  const entry = PORT_NAMES[port];
  if (!entry) return null;
  if (typeof entry === 'string') return entry;
  return entry[proto ?? ''] ?? null;
}
