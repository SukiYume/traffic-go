import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createColumnHelper } from '@tanstack/react-table';
import { Link } from 'react-router-dom';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { QueryErrorState } from '../components/QueryErrorState';
import { StatCard } from '../components/StatCard';
import { useApiClient } from '../api-context';
import type { MonthlyUsageSummary } from '../types';
import { buildRangedPath } from '../useRangeSearchParam';
import { formatBytes, formatMonth, formatNumber, rangeLabel } from '../utils';

const columnHelper = createColumnHelper<MonthlyUsageSummary>();

function detailPath(row: MonthlyUsageSummary) {
  if (!row.detailRange) {
    return null;
  }
  return buildRangedPath('/usage', row.detailRange);
}

function MonthStatus({ row }: { row: MonthlyUsageSummary }) {
  if (row.archived) {
    return <span className="archive-state archived">已归档</span>;
  }
  if (row.detailRange) {
    return <span className="archive-state live">明细可查 · {rangeLabel(row.detailRange)}</span>;
  }
  if (row.detailAvailable) {
    return <span className="archive-state live">明细保留</span>;
  }
  return <span className="archive-state archived">仅汇总</span>;
}

export function HistoryPage() {
  const api = useApiClient();
  const query = useQuery({
    queryKey: ['monthly-usage'],
    queryFn: ({ signal }) => api.getMonthlyUsage({ signal }),
  });

  const rows = query.data?.rows ?? [];
  const summary = useMemo(
    () =>
      rows.reduce(
        (acc, row) => {
          acc.bytesTotal += row.totalBytes;
          acc.flowCount += row.flowCount;
          acc.archivedMonths += row.archived ? 1 : 0;
          acc.liveMonths += row.archived ? 0 : 1;
          return acc;
        },
        { bytesTotal: 0, flowCount: 0, archivedMonths: 0, liveMonths: 0 },
      ),
    [rows],
  );

  const columns = useMemo(
    () => [
      columnHelper.accessor('monthTs', {
        id: 'monthTs',
        header: '月份',
        meta: { className: 'col-time', nowrap: true },
        cell: (info) => formatMonth(info.getValue()),
      }),
      columnHelper.display({
        id: 'status',
        header: '状态',
        meta: { className: 'col-status', nowrap: true },
        cell: (info) => <MonthStatus row={info.row.original} />,
      }),
      columnHelper.accessor('totalBytes', {
        id: 'totalBytes',
        header: '总流量',
        meta: { className: 'col-bytes col-bytes-total', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
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
      columnHelper.accessor('forwardTotalBytes', {
        id: 'forwardTotalBytes',
        header: '转发总量',
        meta: { className: 'col-bytes', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('flowCount', {
        id: 'flowCount',
        header: '连接数',
        meta: { className: 'col-count', align: 'right', nowrap: true },
        cell: (info) => formatNumber(info.getValue()),
      }),
      columnHelper.display({
        id: 'evidence',
        header: '证据 / 链路',
        meta: { className: 'col-count', align: 'right', nowrap: true },
        cell: (info) => `${formatNumber(info.row.original.evidenceCount)} / ${formatNumber(info.row.original.chainCount)}`,
      }),
      columnHelper.display({
        id: 'actions',
        header: '操作',
        meta: { className: 'col-action', align: 'right', nowrap: true },
        cell: (info) => {
          const path = detailPath(info.row.original);
          if (!path) {
            return <span className="archive-note">仅月度汇总</span>;
          }
          return <Link to={path}>查看明细</Link>;
        },
      }),
    ],
    [],
  );

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">History</p>
          <h2>月度归档</h2>
          <p>
            按 UTC 自然月查看所有已保存历史。保留期内的月份可以继续进入明细页；已归档月份只保留月度总量、
            转发总量、证据数和链路数。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>完整明细</strong>
              <span>本月 / 上月 / 上上月</span>
            </div>
            <div className="status-pill">
              <strong>历史归档</strong>
              <span>自然月汇总</span>
            </div>
          </section>
        </div>
      </header>

      {query.isError ? (
        <QueryErrorState error={query.error} title="月度归档加载失败" />
      ) : rows.length ? (
        <>
          <section className="stat-grid">
            <StatCard label="历史总流量" value={summary.bytesTotal} suffix="bytes" />
            <StatCard label="历史连接数" value={summary.flowCount} suffix="count" />
            <StatCard label="明细月份" value={summary.liveMonths} suffix="count" />
            <StatCard label="归档月份" value={summary.archivedMonths} suffix="count" />
          </section>

          <DataTable columns={columns} data={rows} emptyText="暂无月度历史" />
        </>
      ) : query.isPending ? (
        <EmptyState title="月度归档加载中" description="正在读取自然月历史汇总。" />
      ) : (
        <EmptyState title="暂无月度历史" description="还没有可展示的自然月汇总或保留期内明细。" />
      )}
    </div>
  );
}
