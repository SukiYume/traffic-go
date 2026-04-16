import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useSearchParams } from 'react-router-dom';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import type { ForwardUsageRow, RangeKey } from '../types';
import { formatBytes, formatDateTime, rangeLabel } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<ForwardUsageRow>();

export function ForwardPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'minuteTs', desc: true }]);

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  useEffect(() => {
    setPage(1);
  }, [range, sorting]);

  const currentSort = sorting[0];
  const query = useQuery({
    queryKey: ['forward', range, page, currentSort?.id, currentSort?.desc],
    queryFn: () =>
      api.getForwardUsage({
        range,
        page,
        pageSize,
        sortBy: (currentSort?.id as any) ?? 'minuteTs',
        sortOrder: currentSort?.desc ? 'desc' : 'asc',
      }),
  });

  const columns = useMemo(
    () => [
      columnHelper.accessor('minuteTs', {
        id: 'minuteTs',
        header: '时间',
        cell: (info) => formatDateTime(info.getValue()),
      }),
      columnHelper.accessor('origSrc', { id: 'origSrc', header: '来源 IP' }),
      columnHelper.accessor('origDst', { id: 'origDst', header: '目标 IP' }),
      columnHelper.accessor('origSport', { id: 'origSport', header: '来源端口', enableSorting: false }),
      columnHelper.accessor('origDport', { id: 'origDport', header: '目标端口', enableSorting: false }),
      columnHelper.accessor('bytesOrig', { id: 'bytesOrig', header: '原向流量', cell: (info) => formatBytes(info.getValue()) }),
      columnHelper.accessor('bytesReply', { id: 'bytesReply', header: '回包流量', cell: (info) => formatBytes(info.getValue()) }),
      columnHelper.accessor('flowCount', { id: 'flowCount', header: '流数' }),
      columnHelper.display({
        id: 'bytesTotal',
        header: '总量',
        cell: (info) => formatBytes(info.row.original.bytesOrig + info.row.original.bytesReply),
      }),
    ],
    [],
  );

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Forward</p>
          <h2>转发流量</h2>
          <p>独立查看 NAT / 转发流量的来源 IP、目标 IP 与双向字节数，不与进程明细混算。</p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {query.data ? <DataSourceBadge dataSource={query.data.dataSource} /> : null}
          </section>
        </div>
        <RangeSelect value={range} onChange={setRange} />
      </header>

      {query.data?.rows.length ? (
        <DataTable
          columns={columns}
          data={query.data.rows}
          sorting={sorting}
          onSortingChange={setSorting}
          manualSorting
          pagination={{
            page: query.data.page,
            pageSize: query.data.pageSize,
            totalRows: query.data.totalRows,
            onPageChange: setPage,
          }}
        />
      ) : (
        <EmptyState title="暂无转发流量" description="这台机器当前没有 forward 方向的聚合结果。" />
      )}
    </div>
  );
}
