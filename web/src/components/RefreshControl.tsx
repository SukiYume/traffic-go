import { LIVE_REFETCH_INTERVAL_MS } from '../query-refresh';

function formatUpdatedAt(value: number) {
  if (!value) return '尚未更新';
  return `上次更新 ${new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(value)}`;
}

export function RefreshControl({
  updatedAt,
  isFetching,
  onRefresh,
}: {
  updatedAt: number;
  isFetching: boolean;
  onRefresh: () => void | Promise<unknown>;
}) {
  return (
    <div className="refresh-control" aria-live="polite">
      <span>{isFetching ? '正在刷新…' : formatUpdatedAt(updatedAt)}</span>
      <span className="refresh-control-interval">每 {LIVE_REFETCH_INTERVAL_MS / 1000} 秒自动刷新</span>
      <button type="button" className="chip ghost" onClick={() => void onRefresh()} disabled={isFetching}>
        {isFetching ? '刷新中' : '立即刷新'}
      </button>
    </div>
  );
}
