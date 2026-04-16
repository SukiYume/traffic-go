import { describe, expect, it } from 'vitest';
import { serviceNameForPort } from '../utils';

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
