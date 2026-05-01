import { dataSourceLabel, minuteDimensionsUnavailable } from '../utils';
import type { DataSource } from '../types';

export function DataSourceBadge({ dataSource }: { dataSource: DataSource }) {
  const summaryLabel =
    dataSource === 'interface_1m' ? '网卡分钟' : minuteDimensionsUnavailable(dataSource) ? '小时聚合' : '分钟明细';

  return (
    <div className="status-pill data-source-pill" title={dataSourceLabel(dataSource)}>
      <strong>{summaryLabel}</strong>
      <span>{dataSource}</span>
    </div>
  );
}
