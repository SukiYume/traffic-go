import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useSearchParams } from 'react-router-dom';
import { ChartPanel } from '../components/ChartPanel';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import type { ProcessSummaryRow, RangeKey } from '../types';
import { clampText, formatBytes, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const columnHelper = createColumnHelper<ProcessSummaryRow>();

export function ProcessesPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'totalBytes', desc: true }]);

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const query = useQuery({
    queryKey: ['process-summaries', range],
    queryFn: () => api.getTopProcesses(range, { page: 1, pageSize: 100, sortBy: 'bytesTotal', sortOrder: 'desc' }),
  });

  const rows = query.data?.rows ?? [];
  const selectedProcess = useMemo(() => {
    if (!rows.length) return null;
    const fallback = rows[0];
    if (!selectedKey) return fallback;
    return rows.find((row) => `${row.pid ?? 'none'}-${row.comm ?? ''}-${row.exe ?? ''}` === selectedKey) ?? fallback;
  }, [rows, selectedKey]);

  useEffect(() => {
    if (!rows.length) {
      setSelectedKey(null);
      return;
    }
    if (!selectedProcess) {
      const fallback = rows[0];
      setSelectedKey(`${fallback.pid ?? 'none'}-${fallback.comm ?? ''}-${fallback.exe ?? ''}`);
    }
  }, [rows, selectedProcess]);

  const series = useQuery({
    queryKey: ['process-series', range, selectedProcess?.pid, selectedProcess?.comm],
    queryFn: () =>
      api.getTimeSeries(
        range,
        'direction',
        selectedProcess
          ? selectedProcess.pid !== null
            ? {
                comm: selectedProcess.comm ?? undefined,
                pid: selectedProcess.pid,
                exe: selectedProcess.exe ?? undefined,
              }
            : { comm: selectedProcess.comm ?? undefined }
          : undefined,
      ),
    enabled: Boolean(selectedProcess),
  });

  const columns = useMemo(
    () => [
      columnHelper.accessor('comm', {
        id: 'comm',
        header: '进程',
        cell: (info) => safeText(info.getValue()),
      }),
      columnHelper.accessor('pid', {
        id: 'pid',
        header: 'PID',
        enableSorting: query.data?.dataSource !== 'usage_1h',
        cell: (info) => info.getValue() ?? '按进程名聚合',
      }),
      columnHelper.accessor('exe', {
        id: 'exe',
        header: 'EXE',
        enableSorting: false,
        cell: (info) => <span title={info.getValue() ?? undefined}>{clampText(safeText(info.getValue()), 36)}</span>,
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
      columnHelper.accessor('totalBytes', {
        id: 'totalBytes',
        header: '总流量',
        cell: (info) => formatBytes(info.getValue()),
      }),
    ],
    [query.data?.dataSource],
  );

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Processes</p>
          <h2>进程聚合</h2>
          <p>按时间窗口查看进程消耗，并点选任意进程检查它在当前窗口内的趋势。</p>
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

      {rows.length ? (
        <DataTable
          columns={columns}
          data={rows}
          sorting={sorting}
          onSortingChange={setSorting}
          onRowClick={(row) => setSelectedKey(`${row.pid ?? 'none'}-${row.comm ?? ''}-${row.exe ?? ''}`)}
          isRowSelected={(row) => `${row.pid ?? 'none'}-${row.comm ?? ''}-${row.exe ?? ''}` === selectedKey}
          emptyText="当前时间范围没有进程聚合结果。"
        />
      ) : (
        <EmptyState title="暂无进程聚合" description="当前时间范围没有可以展示的进程流量。" />
      )}

      {selectedProcess && series.data ? (
        <ChartPanel
          points={series.data.points}
          range={range}
          title={`流量趋势 · ${safeText(selectedProcess.comm)}`}
          subtitle={selectedProcess.pid !== null ? `PID ${selectedProcess.pid} · ${selectedProcess.exe ?? 'EXE 未知'}` : '当前窗口已降级为按进程名聚合'}
        />
      ) : null}
    </div>
  );
}
