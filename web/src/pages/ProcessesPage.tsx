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
import { clampText, displayExecutableName, executableName, formatBytes, rangeLabel, safeText } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const columnHelper = createColumnHelper<ProcessSummaryRow>();

export function ProcessesPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [sorting, setSorting] = useState<SortingState>([{ id: 'totalBytes', desc: true }]);
  const [groupBy, setGroupBy] = useState<'pid' | 'comm'>('pid');

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const query = useQuery({
    queryKey: ['process-summaries', range],
    queryFn: () => api.getTopProcesses(range, { page: 1, pageSize: 100, sortBy: 'bytesTotal', sortOrder: 'desc' }),
  });

  const rawRows = query.data?.rows ?? [];
  const showPIDColumn = groupBy === 'pid' && query.data?.dataSource !== 'usage_1h';

  useEffect(() => {
    if (!showPIDColumn && sorting[0]?.id === 'pid') {
      setSorting([{ id: 'totalBytes', desc: true }]);
    }
  }, [showPIDColumn, sorting]);

  const rows = useMemo(() => {
    if (groupBy === 'pid' || query.data?.dataSource === 'usage_1h') return rawRows;
    const map = new Map<string, ProcessSummaryRow>();
    for (const row of rawRows) {
      const key = row.comm ?? '未知';
      const existing = map.get(key);
      if (!existing) {
        map.set(key, { ...row, pid: null, exe: null });
      } else {
        existing.bytesUp += row.bytesUp;
        existing.bytesDown += row.bytesDown;
        existing.totalBytes += row.totalBytes;
        existing.flowCount += row.flowCount;
      }
    }
    return Array.from(map.values()).sort((a, b) => {
      const sortColumn = sorting[0];
      if (!sortColumn) return b.totalBytes - a.totalBytes;
      const vA = a[sortColumn.id as keyof ProcessSummaryRow] as number;
      const vB = b[sortColumn.id as keyof ProcessSummaryRow] as number;
      return sortColumn.desc ? vB - vA : vA - vB;
    });
  }, [rawRows, groupBy, sorting, query.data?.dataSource]);
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
    [showPIDColumn],
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

      {query.data?.dataSource !== 'usage_1h' && (
        <section className="segmented-control" aria-label="聚合方式">
          <button type="button" className={groupBy === 'pid' ? 'chip active' : 'chip'} onClick={() => setGroupBy('pid')}>
            按 PID 聚合
          </button>
          <button type="button" className={groupBy === 'comm' ? 'chip active' : 'chip'} onClick={() => setGroupBy('comm')}>
            按进程名聚合
          </button>
        </section>
      )}

      {rows.length ? (
        <DataTable
          columns={columns}
          data={rows}
          cardClassName="table-card-auto"
          tableClassName={processesTableClassName}
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
          subtitle={selectedProcess.pid !== null ? `PID ${selectedProcess.pid} · ${displayExecutableName(selectedProcess.exe)}` : '当前窗口已降级为按进程名聚合'}
        />
      ) : null}
    </div>
  );
}
