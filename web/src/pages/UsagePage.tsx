import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { FiltersBar } from '../components/FiltersBar';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import type { RangeKey, UsageRow } from '../types';
import { clampText, directionLabel, formatBytes, formatDateTime, isLongRange, peerRoleLabel, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<UsageRow>();

function useUsageFilters() {
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;

  const filters = {
    comm: params.get('comm') ?? '',
    pid: params.get('pid') ?? '',
    exe: params.get('exe') ?? '',
    remoteIp: params.get('remoteIp') ?? '',
    localPort: params.get('localPort') ?? '',
    direction: params.get('direction') ?? '',
    proto: params.get('proto') ?? '',
    attribution: params.get('attribution') ?? '',
  };

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    if (next === '90d') {
      nextParams.delete('pid');
      nextParams.delete('exe');
    }
    setParams(nextParams, { replace: true });
  };

  const setFilters = (next: typeof filters) => {
    const nextParams = new URLSearchParams({ range });
    for (const [key, value] of Object.entries(next)) {
      if (value) nextParams.set(key, value);
    }
    setParams(nextParams, { replace: true });
  };

  return { range, filters, setRange, setFilters };
}

export function UsagePage() {
  const api = useApiClient();
  const { range, filters, setRange, setFilters } = useUsageFilters();
  const longRange = isLongRange(range);
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'minuteTs', desc: true }]);

  useEffect(() => {
    setPage(1);
  }, [range, filters, sorting]);

  useEffect(() => {
    if (!longRange) return;
    if (!filters.pid && !filters.exe) return;
    setFilters({ ...filters, pid: '', exe: '' });
  }, [filters, longRange, setFilters]);

  const currentSort = sorting[0];

  const query = useQuery({
    queryKey: ['usage', range, filters, page, currentSort?.id, currentSort?.desc],
    queryFn: () =>
      api.getUsage({
        range,
        ...filters,
        page,
        pageSize,
        sortBy: (currentSort?.id as any) ?? 'minuteTs',
        sortOrder: currentSort?.desc ? 'desc' : 'asc',
      }),
    placeholderData: keepPreviousData,
  });
  const processes = useQuery({
    queryKey: ['processes'],
    queryFn: () => api.getProcesses(),
  });

  const columns = useMemo(() => {
    const baseColumns = [
      columnHelper.accessor('minuteTs', {
        id: 'minuteTs',
        header: '时间',
        cell: (info) => formatDateTime(info.getValue()),
      }),
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
      columnHelper.accessor('localPort', {
        id: 'localPort',
        header: '本地端口',
        cell: (info) => info.getValue() ?? '未知',
      }),
      columnHelper.accessor('proto', {
        id: 'proto',
        header: '协议',
      }),
      columnHelper.accessor('comm', {
        id: 'comm',
        header: '进程',
        cell: (info) => safeText(info.getValue()),
      }),
    ];

    const detailedColumns =
      query.data?.dataSource === 'usage_1h'
        ? []
        : [
            columnHelper.accessor('pid', {
              id: 'pid',
              header: 'PID',
              cell: (info) => info.getValue() ?? '未知',
            }),
            columnHelper.accessor('exe', {
              id: 'exe',
              header: 'EXE',
              enableSorting: false,
              cell: (info) => <span title={info.getValue() ?? undefined}>{clampText(safeText(info.getValue()), 28)}</span>,
            }),
          ];

    const tailColumns = [
      ...(query.data?.dataSource === 'usage_1h'
        ? []
        : [
            columnHelper.accessor('attribution', {
              id: 'attribution',
              header: '归因',
              enableSorting: false,
              cell: (info) => safeText(info.getValue()),
            }),
          ]),
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
      columnHelper.display({
        id: 'bytesTotal',
        header: '总流量',
        cell: (info) => formatBytes(info.row.original.bytesUp + info.row.original.bytesDown),
      }),
    ];

    return [...baseColumns, ...detailedColumns, ...tailColumns];
  }, [query.data?.dataSource]);

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Usage</p>
          <h2>流量明细</h2>
          <p>逐条检查入站来源、出站目标、进程归因和端口明细。排序、分页和时间窗口会一起同步到后端查询。</p>
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

      <FiltersBar range={range} processes={processes.data?.processes ?? []} filters={filters} onChange={setFilters} />

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
        <EmptyState title="暂无明细" description="当前筛选条件没有命中数据。" />
      )}
    </div>
  );
}
