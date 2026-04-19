import { EmptyState } from "./EmptyState";

export function describeQueryError(error: unknown) {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return "请求失败，请稍后重试。";
}

export function QueryErrorState({
  error,
  title,
  compact = false,
}: {
  error: unknown;
  title: string;
  compact?: boolean;
}) {
  if (compact) {
    return (
      <div className="query-error-banner" role="alert">
        <strong>{title}</strong>
        <span>{describeQueryError(error)}</span>
      </div>
    );
  }
  return <EmptyState title={title} description={describeQueryError(error)} />;
}
