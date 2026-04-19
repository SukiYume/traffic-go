import { describe, expect, it } from 'vitest';
import {
  dataSourceAutoNote,
  dataSourceDescription,
  dataSourceLabel,
  displayExecutableName,
  executableName,
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
