import { dataSourceLabel, formatDateTime } from '../utils';
import type { DataSource, SummaryCoverage } from '../types';

export function DataSourceBadge({ dataSource, coverage }: { dataSource: DataSource; coverage?: SummaryCoverage }) {
  return (
    <>
      <div className="status-pill data-source-pill" title={dataSourceLabel(dataSource)}>
        <strong>{dataSourceLabel(dataSource)}</strong>
        <span>{dataSource}</span>
      </div>
      {coverage?.approximate ? (
        <div
          className="status-pill coverage-pill"
          role="status"
          title={`实际聚合覆盖 ${formatDateTime(coverage.effectiveStart)} 至 ${formatDateTime(coverage.effectiveEnd)} UTC（结束时间不含）`}
        >
          <strong>近似窗口</strong>
          <span>边界按完整聚合桶扩展</span>
        </div>
      ) : null}
    </>
  );
}
