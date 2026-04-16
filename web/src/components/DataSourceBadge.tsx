import type { DataSource } from '../types';
import { dataSourceDescription, dataSourceLabel } from '../utils';

export function DataSourceBadge({ dataSource }: { dataSource: DataSource }) {
  return (
    <div className="status-pill data-source-pill" title={dataSourceDescription(dataSource)}>
      <strong>{dataSourceLabel(dataSource)}</strong>
      <span>{dataSource}</span>
    </div>
  );
}
