import { useEffect, useMemo, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useSearchParams } from 'react-router-dom';
import { ChartPanel } from '../components/ChartPanel';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { QueryErrorState } from '../components/QueryErrorState';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import { normalizeRangeKey } from '../ranges';
import { normalizeProcessSortKey } from '../sort-keys';
import type { ProcessGroupBy, ProcessSummaryRow, RangeKey } from '../types';
import { clampText, displayExecutableName, executableName, formatBytes, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<ProcessSummaryRow>();

function processRowKey(row: Pick<ProcessSummaryRow, 'pid' | 'comm' | 'exe'>) {
  return `${row.pid ?? 'none'}-${row.comm ?? ''}-${row.exe ?? ''}`;
}

function buildProcessSeriesFilters(row: ProcessSummaryRow | null) {
  if (!row) {
    return undefined;
  }
  if (row.pid !== null) {
    return {
      comm: row.comm ?? undefined,
      pid: row.pid,
      exe: row.exe ?? undefined,
    };
  }
  const filters: { comm?: string; exe?: string } = {};
  if (row.comm?.trim()) {
    filters.comm = row.comm;
  }
  if (row.exe?.trim()) {
    filters.exe = row.exe;
  }
  return Object.keys(filters).length ? filters : undefined;
}

export function ProcessesPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get('range'), defaultRange);
  const [page, setPage] = useState(1);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'totalBytes', desc: true }]);
  const [groupBy, setGroupBy] = useState<ProcessGroupBy>(() => (range === '90d' ? 'comm' : 'pid'));
  const effectiveGroupBy: ProcessGroupBy = range === '90d' ? 'comm' : groupBy;

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const currentSort = sorting[0];
  const query = useQuery({
    queryKey: ['process-summaries', range, effectiveGroupBy, page, currentSort?.id, currentSort?.desc],
    queryFn: ({ signal }) =>
      api.getTopProcesses(range, {
        page,
        pageSize,
        groupBy: effectiveGroupBy,
        sortBy: normalizeProcessSortKey(currentSort?.id),
        sortOrder: currentSort?.desc ? 'desc' : 'asc',
      }, { signal }),
    placeholderData: keepPreviousData,
  });

  const rows = query.data?.rows ?? [];
  const showPIDColumn = effectiveGroupBy === 'pid' && query.data?.dataSource !== 'usage_1h';
  const showExeColumn = effectiveGroupBy === 'pid' && query.data?.dataSource !== 'usage_1h';

  useEffect(() => {
    setPage(1);
  }, [range, effectiveGroupBy, sorting]);

  useEffect(() => {
    if (query.data?.dataSource !== 'usage_1h') return;
    if (groupBy === 'comm') return;
    setGroupBy('comm');
  }, [query.data?.dataSource, groupBy]);

  useEffect(() => {
    if (!showPIDColumn && sorting[0]?.id === 'pid') {
      setSorting([{ id: 'totalBytes', desc: true }]);
    }
  }, [showPIDColumn, sorting]);

  const selectedProcess = useMemo(
    () => rows.find((row) => processRowKey(row) === selectedKey) ?? null,
    [rows, selectedKey],
  );

  useEffect(() => {
    if (!rows.length) {
      setSelectedKey(null);
      return;
    }
    // Keep the table highlight and the chart target on the same source of truth.
    if (!selectedKey || !rows.some((row) => processRowKey(row) === selectedKey)) {
      setSelectedKey(processRowKey(rows[0]));
    }
  }, [rows, selectedKey]);

  const selectedSeriesFilters = useMemo(() => buildProcessSeriesFilters(selectedProcess), [selectedProcess]);
  const canQuerySeries = Boolean(selectedSeriesFilters);

  const series = useQuery({
    queryKey: ['process-series', range, selectedProcess ? processRowKey(selectedProcess) : null],
    queryFn: ({ signal }) => {
      if (!selectedSeriesFilters) {
        throw new Error('missing process series filters');
      }
      return api.getTimeSeries(range, 'direction', selectedSeriesFilters, { signal });
    },
    enabled: canQuerySeries,
  });

  const columns = useMemo(
    () => [
      columnHelper.accessor('comm', {
        id: 'comm',
        header: '进程',
        meta: { className: 'col-process', nowrap: true },
        cell: (info) => safeText(info.getValue()),
      }),
      ...(showPIDColumn
        ? [
            columnHelper.accessor('pid', {
              id: 'pid',
              header: 'PID',
              enableSorting: true,
              meta: { className: 'col-pid', nowrap: true },
              cell: (info) => info.getValue() ?? '未知',
            }),
          ]
        : []),
      ...(showExeColumn
        ? [
            columnHelper.accessor('exe', {
              id: 'exe',
              header: 'EXE',
              enableSorting: false,
              meta: { className: 'col-exe', nowrap: true },
              cell: (info) => {
                const raw = info.getValue();
                const cmd = executableName(raw);
                return <span title={cmd ?? undefined}>{clampText(displayExecutableName(raw), 36)}</span>;
              },
            }),
          ]
        : []),
      columnHelper.accessor('bytesUp', {
        id: 'bytesUp',
        header: '上行',
        meta: { className: 'col-bytes', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('bytesDown', {
        id: 'bytesDown',
        header: '下行',
        meta: { className: 'col-bytes', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('totalBytes', {
        id: 'totalBytes',
        header: '总流量',
        meta: { className: 'col-bytes col-bytes-total', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
    ],
    [showExeColumn, showPIDColumn],
  );

  const processesTableClassName = showPIDColumn
    ? 'processes-table processes-table-pid table-dense'
    : 'processes-table processes-table-comm table-dense';

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Processes</p>
          <h2>进程聚合</h2>
          <p>
            该页面用于回答“是谁在消耗带宽”：可按 PID 或按进程名聚合比较流量占比，点击任意行会在下方展示该进程的时间趋势，
            便于区分持续高占用与短时突发。
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

      {query.data?.dataSource !== 'usage_1h' && range !== '90d' && (
        <section className="segmented-control" aria-label="聚合方式">
          <button type="button" className={groupBy === 'pid' ? 'chip active' : 'chip'} onClick={() => setGroupBy('pid')}>
            按 PID 聚合
          </button>
          <button type="button" className={groupBy === 'comm' ? 'chip active' : 'chip'} onClick={() => setGroupBy('comm')}>
            按进程名聚合
          </button>
        </section>
      )}

      {query.isError && rows.length ? (
        <QueryErrorState error={query.error} title="进程聚合刷新失败，当前展示旧结果" compact />
      ) : null}

      {query.isError && !rows.length ? (
        <QueryErrorState error={query.error} title="进程聚合加载失败" />
      ) : query.isPending && !query.data ? (
        <EmptyState title="进程聚合加载中" description="正在获取当前时间范围内的进程聚合结果。" />
      ) : rows.length ? (
        <DataTable
          columns={columns}
          data={rows}
          cardClassName="table-card-auto"
          tableClassName={processesTableClassName}
          sorting={sorting}
          onSortingChange={setSorting}
          manualSorting
          onRowClick={(row) => setSelectedKey(processRowKey(row))}
          isRowSelected={(row) => processRowKey(row) === selectedKey}
          pagination={{
            page: query.data?.page ?? 1,
            pageSize: query.data?.pageSize ?? pageSize,
            totalRows: query.data?.totalRows ?? 0,
            onPageChange: setPage,
          }}
          emptyText="当前时间范围没有进程聚合结果。"
        />
      ) : (
        <EmptyState title="暂无进程聚合" description="当前时间范围没有可以展示的进程流量。" />
      )}

      {selectedProcess && !canQuerySeries ? (
        <EmptyState title="无法绘制趋势" description="这条小时聚合记录缺少稳定进程名，无法定位到单个进程趋势。" />
      ) : selectedProcess && series.isError ? (
        <QueryErrorState error={series.error} title="进程趋势加载失败" />
      ) : selectedProcess && series.data ? (
        <ChartPanel
          points={series.data.points}
          range={range}
          title={`流量趋势 · ${safeText(selectedProcess.comm)}`}
          subtitle={selectedProcess.pid !== null ? `PID ${selectedProcess.pid} · ${displayExecutableName(selectedProcess.exe)}` : '当前窗口已降级为按进程名聚合'}
        />
      ) : selectedProcess && canQuerySeries ? (
        <EmptyState title="趋势加载中" description="正在获取该进程的时间趋势。" />
      ) : null}
    </div>
  );
}
