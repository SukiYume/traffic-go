import { describe, expect, it } from 'vitest';
import { isRangeKey, normalizeRangeKey } from '../ranges';

describe('range helpers', () => {
  it('recognizes valid range keys', () => {
    expect(isRangeKey('1h')).toBe(true);
    expect(isRangeKey('24h')).toBe(true);
    expect(isRangeKey('7d')).toBe(true);
    expect(isRangeKey('30d')).toBe(true);
    expect(isRangeKey('90d')).toBe(true);
  });

  it('rejects invalid range keys', () => {
    expect(isRangeKey('')).toBe(false);
    expect(isRangeKey('6h')).toBe(false);
    expect(isRangeKey('365d')).toBe(false);
    expect(isRangeKey(undefined)).toBe(false);
    expect(isRangeKey(null)).toBe(false);
  });

  it('normalizes invalid values to fallback', () => {
    expect(normalizeRangeKey('bad', '24h')).toBe('24h');
    expect(normalizeRangeKey(undefined, '7d')).toBe('7d');
  });

  it('keeps valid range values unchanged', () => {
    expect(normalizeRangeKey('1h', '24h')).toBe('1h');
    expect(normalizeRangeKey('90d', '24h')).toBe('90d');
  });
});
