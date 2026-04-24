import type { BucketKey, RangeKey } from './types';

export const RANGE_TO_BUCKET: Record<RangeKey, BucketKey> = {
  '1h': '1m',
  '24h': '5m',
  '7d': '1h',
  this_month: '1d',
  last_month: '1d',
  two_months_ago: '1d',
};

const RANGE_KEYS = ['1h', '24h', '7d', 'this_month', 'last_month', 'two_months_ago'] as const;

export function isRangeKey(value: string | null | undefined): value is RangeKey {
  return value != null && (RANGE_KEYS as readonly string[]).includes(value);
}

export function normalizeRangeKey(value: string | null | undefined, fallback: RangeKey): RangeKey {
  return isRangeKey(value) ? value : fallback;
}
