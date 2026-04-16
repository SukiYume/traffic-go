import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useCallback, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { FiltersBar } from '../components/FiltersBar';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import type { RangeKey, UsageRow } from '../types';
import {
  attributionDescription,
  clampText,
  directionLabel,
  formatBytes,
  formatDateTime,
  formatNumber,
  isLongRange,
  peerRoleLabel,
  rangeLabel,
  safeText,
  serviceNameForPort,
} from '../utils';

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

function UsageExpandPanel({
  row,
  onFilterByIp,
}: {
  row: UsageRow;
  onFilterByIp: (ip: string) => void;
}): ReactNode {
  const serviceName = serviceNameForPort(row.remotePort, row.proto);
  const portLabel =
    row.remotePort != null
      ? serviceName
        ? `${row.remotePort} / ${serviceName}`
        : String(row.remotePort)
      : '未知';
  const rateUp = Math.round(row.bytesUp / 60);
  const rateDown = Math.round(row.bytesDown / 60);

  return (
    <div className="row-expand">
      <div className="row-expand-grid">
        <div>
          <span>对端端口</span>
          <strong>{portLabel}</strong>
        </div>
        <div>
          <span>数据包数</span>
          <strong>
            ↑ {formatNumber(row.pktsUp)} · ↓ {formatNumber(row.pktsDown)}
          </strong>
        </div>
        <div>
          <span>连接数</span>
          <strong>{row.flowCount} flows</strong>
        </div>
        <div>
          <span>归因详情</span>
          <strong>{attributionDescription(row.attribution)}</strong>
        </div>
        <div>
          <span>平均速率</span>
          <strong>
            ↑ {formatBytes(rateUp)}/s · ↓ {formatBytes(rateDown)}/s
          </strong>
        </div>
      </div>
      {row.remoteIp && (
        <div className="row-expand-actions">
          <button
            type="button"
            className="chip"
            onClick={() => onFilterByIp(row.remoteIp!)}
          >
            过滤此 IP：{row.remoteIp}
          </button>
        </div>
      )}
    </div>
  );
}

export function UsagePage() {
  const api = useApiClient();
  const { range, filters, setRange, setFilters } = useUsageFilters();
  const longRange = isLongRange(range);
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'minuteTs', desc: true }]);
  const [expandedRowIndex, setExpandedRowIndex] = useState<number | null>(null);

  // Serialize filters to a stable string so this effect only fires when filter
  // values actually change, not on every render (filters object is recreated each render).
  const filtersKey = JSON.stringify(filters);

  useEffect(() => {
    setPage(1);
    setExpandedRowIndex(null);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, filtersKey, sorting]);

  useEffect(() => {
    if (!longRange) return;
    if (!filters.pid && !filters.exe) return;
    setFilters({ ...filters, pid: '', exe: '' });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filtersKey, longRange]);

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

  const onFilterByIp = useCallback(
    (ip: string) => {
      setFilters({ ...filters, remoteIp: ip });
      setExpandedRowIndex(null);
    },
    [filters, setFilters],
  );

  const columns = useMemo(() => {
    const isHourly = query.data?.dataSource === 'usage_1h';

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
        cell: (info) => {
          const ip = info.getValue();
          if (!ip) return '未知';
          return (
            <button
              type="button"
              className="ip-link"
              onClick={(e) => {
                e.stopPropagation();
                onFilterByIp(ip);
              }}
            >
              {ip}
            </button>
          );
        },
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

    const detailedColumns = isHourly
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
            cell: (info) => (
              <span title={info.getValue() ?? undefined}>{clampText(safeText(info.getValue()), 28)}</span>
            ),
          }),
          columnHelper.accessor('remotePort', {
            id: 'remotePort',
            header: '对端端口',
            enableSorting: false,
            cell: (info) => {
              const port = info.getValue();
              if (port == null) return '—';
              const svc = serviceNameForPort(port, info.row.original.proto);
              return svc ? `${port} / ${svc}` : String(port);
            },
          }),
          columnHelper.accessor('attribution', {
            id: 'attribution',
            header: () => (
              <span title="exact: 精确匹配(端口+IP+协议全匹配)&#10;unknown: 只看到流量，无法稳定映射到进程">
                归因 ⓘ
              </span>
            ),
            enableSorting: false,
            cell: (info) => safeText(info.getValue()),
          }),
        ];

    const tailColumns = [
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
  }, [query.data?.dataSource, onFilterByIp]);

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Usage</p>
          <h2>流量明细</h2>
          <p>
            逐条流量明细，深挖具体连接行为。点击任意行展开详情，点击对端 IP 快速过滤。排序、分页和时间窗口实时同步到后端。
          </p>
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
          expandedRowIndex={expandedRowIndex}
          onExpandRow={setExpandedRowIndex}
          renderExpandedRow={(row) => (
            <UsageExpandPanel row={row} onFilterByIp={onFilterByIp} />
          )}
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
