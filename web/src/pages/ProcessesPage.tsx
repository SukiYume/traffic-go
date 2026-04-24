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
import type { ProcessSummaryRow, RangeKey } from '../types';
import { useResettingPage } from '../useResettingPage';
import { formatBytes, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<ProcessSummaryRow>();

type ProcessTableSection = 'pid' | 'comm';
type SelectedProcessState = {
  section: ProcessTableSection;
  key: string;
};

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

function findSelectedRow(rows: ProcessSummaryRow[], selected: SelectedProcessState | null, section: ProcessTableSection) {
  if (!selected || selected.section !== section) {
    return null;
  }
  return rows.find((row) => processRowKey(row) === selected.key) ?? null;
}

export function ProcessesPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get('range'), defaultRange);
  const [pidSorting, setPidSorting] = useState<SortingState>([{ id: 'totalBytes', desc: true }]);
  const [commSorting, setCommSorting] = useState<SortingState>([{ id: 'totalBytes', desc: true }]);
  const [selected, setSelected] = useState<SelectedProcessState | null>(null);

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const currentPIDSort = pidSorting[0];
  const currentCommSort = commSorting[0];
  const pidResetKey = JSON.stringify([range, currentPIDSort?.id ?? null, currentPIDSort?.desc ?? null]);
  const commResetKey = JSON.stringify([range, currentCommSort?.id ?? null, currentCommSort?.desc ?? null]);
  const [pidPage, setPidPage] = useResettingPage(pidResetKey);
  const [commPage, setCommPage] = useResettingPage(commResetKey);

  const pidQuery = useQuery({
    queryKey: ['process-summaries', range, 'pid', pidPage, currentPIDSort?.id, currentPIDSort?.desc],
    queryFn: ({ signal }) =>
      api.getTopProcesses(range, {
        page: pidPage,
        pageSize,
        groupBy: 'pid',
        sortBy: normalizeProcessSortKey(currentPIDSort?.id),
        sortOrder: currentPIDSort?.desc ? 'desc' : 'asc',
      }, { signal }),
    placeholderData: keepPreviousData,
  });

  const commQuery = useQuery({
    queryKey: ['process-summaries', range, 'comm', commPage, currentCommSort?.id, currentCommSort?.desc],
    queryFn: ({ signal }) =>
      api.getTopProcesses(range, {
        page: commPage,
        pageSize,
        groupBy: 'comm',
        sortBy: normalizeProcessSortKey(currentCommSort?.id),
        sortOrder: currentCommSort?.desc ? 'desc' : 'asc',
      }, { signal }),
    placeholderData: keepPreviousData,
  });

  const activeDataSource = pidQuery.data?.dataSource ?? commQuery.data?.dataSource;
  const pidDimensionUnavailable =
    pidQuery.data?.dataSource === 'usage_1h' ||
    commQuery.data?.dataSource === 'usage_1h';

  const pidRows = pidDimensionUnavailable ? [] : (pidQuery.data?.rows ?? []);
  const commRows = commQuery.data?.rows ?? [];

  const selectedPIDRow = useMemo(() => findSelectedRow(pidRows, selected, 'pid'), [pidRows, selected]);
  const selectedCommRow = useMemo(() => findSelectedRow(commRows, selected, 'comm'), [commRows, selected]);
  const selectedProcess = selectedPIDRow ?? selectedCommRow;

  useEffect(() => {
    if (selectedPIDRow || selectedCommRow) {
      return;
    }
    if (selected?.section === 'comm' && commRows.length) {
      setSelected({ section: 'comm', key: processRowKey(commRows[0]) });
      return;
    }
    if (selected?.section === 'pid' && pidRows.length) {
      setSelected({ section: 'pid', key: processRowKey(pidRows[0]) });
      return;
    }
    if (!selected && pidRows.length) {
      setSelected({ section: 'pid', key: processRowKey(pidRows[0]) });
      return;
    }
    if (!selected && commRows.length) {
      setSelected({ section: 'comm', key: processRowKey(commRows[0]) });
      return;
    }
    if (commRows.length) {
      setSelected({ section: 'comm', key: processRowKey(commRows[0]) });
      return;
    }
    if (pidRows.length) {
      setSelected({ section: 'pid', key: processRowKey(pidRows[0]) });
      return;
    }
    setSelected(null);
  }, [commRows, pidRows, selected, selectedCommRow, selectedPIDRow]);

  const selectedSeriesFilters = useMemo(() => buildProcessSeriesFilters(selectedProcess), [selectedProcess]);
  const canQuerySeries = Boolean(selectedSeriesFilters);

  const series = useQuery({
    queryKey: ['process-series', range, selected?.section ?? null, selectedProcess ? processRowKey(selectedProcess) : null],
    queryFn: ({ signal }) => {
      if (!selectedSeriesFilters) {
        throw new Error('missing process series filters');
      }
      return api.getTimeSeries(range, 'direction', selectedSeriesFilters, { signal });
    },
    enabled: canQuerySeries,
  });

  const pidColumns = useMemo(
    () => [
      columnHelper.accessor('comm', {
        id: 'comm',
        header: '进程',
        meta: { className: 'col-process', nowrap: true },
        cell: (info) => safeText(info.getValue()),
      }),
      columnHelper.accessor('pid', {
        id: 'pid',
        header: 'PID',
        enableSorting: true,
        meta: { className: 'col-pid', nowrap: true },
        cell: (info) => info.getValue() ?? '未知',
      }),
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
    [],
  );

  const commColumns = useMemo(
    () => [
      columnHelper.accessor('comm', {
        id: 'comm',
        header: '进程',
        meta: { className: 'col-process', nowrap: true },
        cell: (info) => safeText(info.getValue()),
      }),
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
    [],
  );

  const initialLoading =
    !commQuery.data &&
    commQuery.isPending &&
    (!pidQuery.data || pidQuery.isPending);

  const initialError =
    !commRows.length &&
    commQuery.isError &&
    (pidDimensionUnavailable || !pidRows.length);

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Processes</p>
          <h2>进程聚合</h2>
          <p>
            该页面用于回答“是谁在消耗带宽”：上方先看按 PID 聚合，定位到具体进程实例；下方再看按进程名聚合，
            用于观察同名进程合并后的总体占比。点击任意行会在下方展示对应趋势。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {activeDataSource ? <DataSourceBadge dataSource={activeDataSource} /> : null}
          </section>
        </div>
        <RangeSelect value={range} onChange={setRange} />
      </header>

      {initialError ? (
        <QueryErrorState error={commQuery.error ?? pidQuery.error} title="进程聚合加载失败" />
      ) : initialLoading ? (
        <EmptyState title="进程聚合加载中" description="正在获取当前时间范围内的进程聚合结果。" />
      ) : (
        <section className="processes-sections">
          <section className="processes-section">
            <div className="processes-section-head">
              <div>
                <h3>按 PID 聚合</h3>
                <p>区分同名进程的不同实例，更适合排查到底是哪个 PID 在消耗带宽。</p>
              </div>
            </div>
            {pidQuery.isError && pidRows.length ? (
              <QueryErrorState error={pidQuery.error} title="按 PID 聚合刷新失败，当前展示旧结果" compact />
            ) : null}
            {pidDimensionUnavailable ? (
              <EmptyState
                title="当前窗口不提供 PID 维度"
                description="当前时间范围已回退到小时聚合历史；这里不再展示 PID 粒度，只保留下方的按进程名聚合结果。"
              />
            ) : pidQuery.isError && !pidRows.length ? (
              <QueryErrorState error={pidQuery.error} title="按 PID 聚合加载失败" />
            ) : pidQuery.isPending && !pidQuery.data ? (
              <EmptyState title="按 PID 聚合加载中" description="正在获取进程实例级别的聚合结果。" />
            ) : pidRows.length ? (
              <DataTable
                columns={pidColumns}
                data={pidRows}
                cardClassName="table-card-auto"
                tableClassName="processes-table processes-table-pid table-dense"
                sorting={pidSorting}
                onSortingChange={setPidSorting}
                manualSorting
                onRowClick={(row) => setSelected({ section: 'pid', key: processRowKey(row) })}
                isRowSelected={(row) => selected?.section === 'pid' && selected.key === processRowKey(row)}
                pagination={{
                  page: pidQuery.data?.page ?? 1,
                  pageSize: pidQuery.data?.pageSize ?? pageSize,
                  totalRows: pidQuery.data?.totalRows ?? 0,
                  onPageChange: setPidPage,
                }}
                emptyText="当前时间范围没有按 PID 聚合结果。"
              />
            ) : (
              <EmptyState title="暂无按 PID 聚合结果" description="当前时间范围没有可以展示的进程实例流量。" />
            )}
          </section>

          <section className="processes-section">
            <div className="processes-section-head">
              <div>
                <h3>按进程名聚合</h3>
                <p>把同名 PID 合并后看整体流量占比，适合快速判断某类服务的总消耗。</p>
              </div>
            </div>
            {commQuery.isError && commRows.length ? (
              <QueryErrorState error={commQuery.error} title="按进程名聚合刷新失败，当前展示旧结果" compact />
            ) : null}
            {commQuery.isError && !commRows.length ? (
              <QueryErrorState error={commQuery.error} title="按进程名聚合加载失败" />
            ) : commQuery.isPending && !commQuery.data ? (
              <EmptyState title="按进程名聚合加载中" description="正在获取进程名级别的聚合结果。" />
            ) : commRows.length ? (
              <DataTable
                columns={commColumns}
                data={commRows}
                cardClassName="table-card-auto"
                tableClassName="processes-table processes-table-comm table-dense"
                sorting={commSorting}
                onSortingChange={setCommSorting}
                manualSorting
                onRowClick={(row) => setSelected({ section: 'comm', key: processRowKey(row) })}
                isRowSelected={(row) => selected?.section === 'comm' && selected.key === processRowKey(row)}
                pagination={{
                  page: commQuery.data?.page ?? 1,
                  pageSize: commQuery.data?.pageSize ?? pageSize,
                  totalRows: commQuery.data?.totalRows ?? 0,
                  onPageChange: setCommPage,
                }}
                emptyText="当前时间范围没有按进程名聚合结果。"
              />
            ) : (
              <EmptyState title="暂无按进程名聚合结果" description="当前时间范围没有可以展示的进程名流量。" />
            )}
          </section>
        </section>
      )}

      {selectedProcess && !canQuerySeries ? (
        <EmptyState title="无法绘制趋势" description="这条记录缺少稳定进程标识，无法定位到单个进程趋势。" />
      ) : selectedProcess && series.isError ? (
        <QueryErrorState error={series.error} title="进程趋势加载失败" />
      ) : selectedProcess && series.data ? (
        <ChartPanel
          points={series.data.points}
          groups={series.data.groups}
          groupBy={series.data.groupBy}
          range={range}
          title={`流量趋势 · ${safeText(selectedProcess.comm)}`}
          subtitle={
            selected?.section === 'pid'
              ? `按 PID 聚合 · PID ${selectedProcess.pid ?? '未知'} · 入站 / 出站总量`
              : activeDataSource === 'usage_1h'
                ? '当前窗口已降级为按进程名聚合 · 入站 / 出站总量'
                : '按进程名聚合 · 入站 / 出站总量'
          }
        />
      ) : selectedProcess && canQuerySeries ? (
        <EmptyState title="趋势加载中" description="正在获取该进程的时间趋势。" />
      ) : null}
    </div>
  );
}
