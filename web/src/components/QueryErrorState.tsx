import { EmptyState } from "./EmptyState";

function describeQueryError(error: unknown) {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return "请求失败，请稍后重试。";
}

export function QueryErrorState({
  error,
  title,
  compact = false,
  onRetry,
  retrying = false,
}: {
  error: unknown;
  title: string;
  compact?: boolean;
  onRetry?: () => void | Promise<unknown>;
  retrying?: boolean;
}) {
  const retryButton = onRetry ? (
    <button type="button" className="chip ghost" onClick={() => void onRetry()} disabled={retrying}>
      {retrying ? '重试中' : '重试'}
    </button>
  ) : null;
  if (compact) {
    return (
      <div className="query-error-banner" role="alert">
        <strong>{title}</strong>
        <span>{describeQueryError(error)}</span>
        {retryButton}
      </div>
    );
  }
  return <EmptyState title={title} description={describeQueryError(error)} action={retryButton} role="alert" />;
}
