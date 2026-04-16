import type { DataSource } from '../types';
import { dataSourceDescription, dataSourceAutoNote } from '../utils';

export function DataSourceBadge({ dataSource }: { dataSource: DataSource }) {
  const tooltip = `${dataSourceDescription(dataSource)}\n\n${dataSourceAutoNote(dataSource)}`;
  return (
    <div className="status-pill data-source-pill" title={tooltip}>
      <strong>{dataSource.endsWith('_1h') ? '小时聚合' : '分钟明细'} ⓘ</strong>
      <span>{dataSource}</span>
    </div>
  );
}
