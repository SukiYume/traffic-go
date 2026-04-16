import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useSearchParams } from 'react-router-dom';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import type { RangeKey, RemoteSummaryRow } from '../types';
import { directionLabel, formatBytes, peerRoleLabel, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<RemoteSummaryRow>();

export function RemotesPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;
  const direction = (params.get('direction') as 'in' | 'out' | null) ?? '';
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'bytesTotal', desc: true }]);

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const setDirection = (next: '' | 'in' | 'out') => {
    const nextParams = new URLSearchParams(params);
    if (next) {
      nextParams.set('direction', next);
    } else {
      nextParams.delete('direction');
    }
    setParams(nextParams, { replace: true });
  };

  useEffect(() => {
    setPage(1);
  }, [range, direction, sorting]);

  const currentSort = sorting[0];
  const query = useQuery({
    queryKey: ['top-remotes', range, direction, page, currentSort?.id, currentSort?.desc],
    queryFn: () =>
      api.getTopRemotes(range, {
        page,
        pageSize,
        direction: direction || undefined,
        sortBy: (currentSort?.id as any) ?? 'bytesTotal',
        sortOrder: currentSort?.desc ? 'desc' : 'asc',
      }),
  });

  const columns = useMemo(
    () => [
      columnHelper.accessor('direction', {
        id: 'direction',
        header: '方向',
        cell: (info) => directionLabel(info.getValue()),
      }),
      columnHelper.display({
        id: 'peerRole',
        header: '对端角色',
        enableSorting: false,
        cell: (info) => peerRoleLabel(info.row.original.direction),
      }),
      columnHelper.accessor('remoteIp', {
        id: 'remoteIp',
        header: '对端 IP',
        cell: (info) => safeText(info.getValue()),
      }),
      columnHelper.accessor('bytesUp', {
        id: 'bytesUp',
        header: '上行',
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('bytesDown', {
        id: 'bytesDown',
        header: '下行',
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('flowCount', {
        id: 'flowCount',
        header: '流数',
      }),
      columnHelper.accessor('totalBytes', {
        id: 'bytesTotal',
        header: '总量',
        cell: (info) => formatBytes(info.getValue()),
      }),
    ],
    [],
  );

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Remotes</p>
          <h2>对端 IP 聚合</h2>
          <p>把“谁在访问这台 VPS”和“这台 VPS 在访问谁”拆开看，默认隐藏 127.0.0.1 / ::1 这类回环地址。</p>
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

      <section className="segmented-control" aria-label="远端方向">
        <button type="button" className={direction === '' ? 'chip active' : 'chip'} onClick={() => setDirection('')}>
          全部对端
        </button>
        <button type="button" className={direction === 'in' ? 'chip active' : 'chip'} onClick={() => setDirection('in')}>
          入站来源
        </button>
        <button type="button" className={direction === 'out' ? 'chip active' : 'chip'} onClick={() => setDirection('out')}>
          出站目标
        </button>
      </section>

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
        <EmptyState title="暂无对端聚合" description="当前时间范围没有命中的对端 IP 数据。" />
      )}
    </div>
  );
}
