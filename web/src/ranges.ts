import type { BucketKey, RangeKey } from './types';

export const RANGE_TO_BUCKET: Record<RangeKey, BucketKey> = {
  '1h': '1m',
  '24h': '5m',
  '7d': '1h',
  '30d': '6h',
  '90d': '1d',
};
