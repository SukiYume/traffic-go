export const LIVE_REFETCH_INTERVAL_MS = 45_000;

export const liveQueryOptions = {
  refetchInterval: LIVE_REFETCH_INTERVAL_MS,
  refetchIntervalInBackground: false,
  refetchOnWindowFocus: true,
} as const;

export function latestSuccessfulUpdate(...timestamps: Array<number | undefined>) {
  const completed = timestamps.filter((value): value is number => Boolean(value && value > 0));
  return completed.length ? Math.min(...completed) : 0;
}
