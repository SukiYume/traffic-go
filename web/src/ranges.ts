import type { BucketKey, RangeKey } from './types';

export const RANGE_TO_BUCKET: Record<RangeKey, BucketKey> = {
  '1h': '1m',
  '24h': '5m',
  '7d': '1h',
  '30d': '6h',
  '90d': '1d',
};

const RANGE_KEYS = ['1h', '24h', '7d', '30d', '90d'] as const;

export function isRangeKey(value: string | null | undefined): value is RangeKey {
  return value != null && (RANGE_KEYS as readonly string[]).includes(value);
}

export function normalizeRangeKey(value: string | null | undefined, fallback: RangeKey): RangeKey {
  return isRangeKey(value) ? value : fallback;
}
