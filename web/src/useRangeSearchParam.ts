import { useSearchParams } from 'react-router-dom';
import { normalizeRangeKey } from './ranges';
import type { RangeKey } from './types';

const defaultRange = '24h' satisfies RangeKey;

type SearchParamValue = string | number | boolean | null | undefined;

export function buildRangedPath(path: string, range: RangeKey, extra?: Record<string, SearchParamValue>) {
  const params = new URLSearchParams();
  params.set('range', range);
  for (const [key, value] of Object.entries(extra ?? {})) {
    if (value !== undefined && value !== null && value !== '') {
      params.set(key, String(value));
    }
  }
  return `${path}?${params.toString()}`;
}

export function useRangeSearchParam(fallback: RangeKey = defaultRange) {
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get('range'), fallback);
  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };
  return { params, setParams, range, setRange } as const;
}
