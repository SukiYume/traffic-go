import { describe, expect, it } from 'vitest';
import {
  dataSourceAutoNote,
  dataSourceDescription,
  dataSourceLabel,
  displayExecutableName,
  executableName,
  chartTickLabel,
  formatBucketTimeLabel,
  formatDateTime,
  formatUrlPath,
  isLoopbackIp,
  minuteDimensionsUnavailable,
  serviceNameForPort,
} from '../utils';

describe('serviceNameForPort', () => {
  it('returns null for null port', () => {
    expect(serviceNameForPort(null)).toBeNull();
  });

  it('returns null for undefined port', () => {
    expect(serviceNameForPort(undefined)).toBeNull();
  });

  it('returns DNS for port 53 (no proto)', () => {
    expect(serviceNameForPort(53)).toBe('DNS');
  });

  it('returns HTTPS for port 443 tcp', () => {
    expect(serviceNameForPort(443, 'tcp')).toBe('HTTPS');
  });

  it('returns QUIC for port 443 udp', () => {
    expect(serviceNameForPort(443, 'udp')).toBe('QUIC');
  });

  it('returns SSH for port 22', () => {
    expect(serviceNameForPort(22)).toBe('SSH');
  });

  it('returns WireGuard for port 51820', () => {
    expect(serviceNameForPort(51820)).toBe('WireGuard');
  });

  it('returns null for unknown port', () => {
    expect(serviceNameForPort(12345)).toBeNull();
  });

  it('returns null for port 443 with unknown proto', () => {
    expect(serviceNameForPort(443, 'sctp')).toBeNull();
  });
});

describe('executableName', () => {
  it('extracts basename from unix path', () => {
    expect(executableName('/usr/local/bin/obfs-server')).toBe('obfs-server');
  });

  it('extracts basename from windows-style path', () => {
    expect(executableName('C:\\tools\\bin\\ss-server.exe')).toBe('ss-server.exe');
  });

  it('returns token basename when command includes args', () => {
    expect(executableName('/usr/bin/python3 -m http.server')).toBe('python3');
  });

  it('returns null for empty input', () => {
    expect(executableName('')).toBeNull();
  });
});

describe('displayExecutableName', () => {
  it('returns fallback for empty input', () => {
    expect(displayExecutableName(undefined)).toBe('未知');
  });
});

describe('formatUrlPath', () => {
  it('decodes percent-encoded unicode path segments for display', () => {
    expect(
      formatUrlPath('/note/%E6%97%A5%E8%AF%AD%E7%AC%94%E8%AE%B0/%E6%97%A5%E8%AF%AD%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0-%E4%B8%AD%E7%BA%A7'),
    ).toBe('/note/日语笔记/日语学习笔记-中级');
  });

  it('keeps reserved encoded delimiters and falls back on malformed input', () => {
    expect(formatUrlPath('/api?exe=%2Fusr%2Fbin%2Fnginx')).toBe('/api?exe=%2Fusr%2Fbin%2Fnginx');
    expect(formatUrlPath('/bad/%E6%A')).toBe('/bad/%E6%A');
  });
});

describe('UTC time formatting', () => {
  it('keeps traffic timestamps in UTC for detailed labels', () => {
    const mayFirstUTC = 1777593600;

    expect(formatDateTime(mayFirstUTC)).toBe('05/01 00:00');
    expect(formatBucketTimeLabel(mayFirstUTC, '5m')).toBe('05/01 00:00');
  });

  it('keeps chart ticks in UTC for hourly ranges', () => {
    expect(chartTickLabel(1777593600, '24h')).toBe('05/01 00时');
  });
});

describe('isLoopbackIp', () => {
  it('matches IPv4 loopback range and IPv6 loopback', () => {
    expect(isLoopbackIp('127.0.0.1')).toBe(true);
    expect(isLoopbackIp('127.10.0.2')).toBe(true);
    expect(isLoopbackIp('::1')).toBe(true);
    expect(isLoopbackIp('203.0.113.24')).toBe(false);
  });
});

describe('data source metadata', () => {
  it('keeps hourly and minute source semantics aligned', () => {
    expect(dataSourceLabel('usage_1h')).toBe('小时聚合');
    expect(dataSourceDescription('usage_1h')).toContain('按小时聚合');
    expect(dataSourceAutoNote('usage_1h')).toContain('小时聚合历史');
    expect(minuteDimensionsUnavailable('usage_1h')).toBe(true);
  });

  it('keeps forward minute sources distinct from usage minute sources', () => {
    expect(dataSourceLabel('usage_1m_forward')).toBe('分钟转发明细');
    expect(dataSourceAutoNote('usage_1m_forward')).toContain('分钟转发明细');
    expect(minuteDimensionsUnavailable('usage_1m_forward')).toBe(false);
  });
});
