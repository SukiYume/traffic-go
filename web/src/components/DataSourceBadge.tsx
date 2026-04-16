import type { DataSource } from '../types';
import { dataSourceDescription, dataSourceAutoNote } from '../utils';

export function DataSourceBadge({ dataSource }: { dataSource: DataSource }) {
  return (
    <div className="status-pill data-source-pill">
      <strong>{dataSource.endsWith('_1h') ? '小时聚合' : '分钟明细'}</strong>
      <span>{dataSource}</span>
    </div>
  );
}
