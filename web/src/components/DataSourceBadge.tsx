import { dataSourceLabel } from '../utils';
import type { DataSource } from '../types';

export function DataSourceBadge({ dataSource }: { dataSource: DataSource }) {
  return (
    <div className="status-pill data-source-pill" title={dataSourceLabel(dataSource)}>
      <strong>{dataSourceLabel(dataSource)}</strong>
      <span>{dataSource}</span>
    </div>
  );
}
