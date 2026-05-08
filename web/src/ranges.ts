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

// rangeWindow returns the [start, end] timestamp window (in unix seconds, UTC) that
// a given range key covers. Used by chart axes so they render the requested window
// even when no data has been loaded yet — switching ranges paints the new axis
// immediately instead of staying anchored to whatever data was previously fetched.
export function rangeWindow(range: RangeKey, now: Date = new Date()): { start: number; end: number } {
  const nowSec = Math.floor(now.getTime() / 1000);
  switch (range) {
    case '1h':
      return { start: nowSec - 3600, end: nowSec };
    case '24h':
      return { start: nowSec - 86_400, end: nowSec };
    case '7d':
      return { start: nowSec - 7 * 86_400, end: nowSec };
    case 'this_month':
      return monthBoundsSec(now, 0);
    case 'last_month':
      return monthBoundsSec(now, -1);
    case 'two_months_ago':
      return monthBoundsSec(now, -2);
  }
}

function monthBoundsSec(now: Date, monthOffset: number): { start: number; end: number } {
  const start = Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + monthOffset, 1);
  const end = Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + monthOffset + 1, 1);
  return { start: Math.floor(start / 1000), end: Math.floor(end / 1000) };
}
