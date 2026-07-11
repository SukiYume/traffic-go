import { useMemo, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { CustomSelect } from '../components/CustomSelect';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { QueryErrorState } from '../components/QueryErrorState';
import { RangeSelect } from '../components/RangeSelect';
import { RefreshControl } from '../components/RefreshControl';
import { useApiClient } from '../api-context';
import { liveQueryOptions } from '../query-refresh';
import { normalizeForwardSortKey } from '../sort-keys';
import type { ForwardUsageRow } from '../types';
import { useRangeSearchParam } from '../useRangeSearchParam';
import { useResettingPage } from '../useResettingPage';
import { useExactTotal } from '../useExactTotal';
import { formatBytes, formatDateTime, rangeLabel } from '../utils';

const pageSize = 25;
const columnHelper = createColumnHelper<ForwardUsageRow>();
const protoOptions = [
  { value: '', label: '全部协议' },
  { value: 'tcp', label: 'TCP' },
  { value: 'udp', label: 'UDP' },
];

export function ForwardPage() {
  const api = useApiClient();
  const { params, range, setRange, setRangedParams } = useRangeSearchParam();
  const filters = {
    origSrcIp: params.get('origSrcIp') ?? '',
    origDstIp: params.get('origDstIp') ?? '',
    proto: params.get('proto') ?? '',
  };
  const [sorting, setSorting] = useState<SortingState>([{ id: 'minuteTs', desc: true }]);

  const setFilter = (key: keyof typeof filters, value: string) => {
    setRangedParams({ ...filters, [key]: value });
  };

  const currentSort = sorting[0];
  const pageResetKey = JSON.stringify([
    range,
    filters.origSrcIp,
    filters.origDstIp,
    filters.proto,
    currentSort?.id ?? null,
    currentSort?.desc ?? null,
  ]);
  const [page, setPage] = useResettingPage(pageResetKey);
  const { includeTotal, requestExactTotal } = useExactTotal(pageResetKey);
  const query = useQuery({
    queryKey: ['forward', range, filters.origSrcIp, filters.origDstIp, filters.proto, page, currentSort?.id, currentSort?.desc, includeTotal],
    queryFn: ({ signal }) =>
      api.getForwardUsage({
        range,
        proto: filters.proto,
        origSrcIp: filters.origSrcIp,
        origDstIp: filters.origDstIp,
        page,
        pageSize,
        sortBy: normalizeForwardSortKey(currentSort?.id),
        sortOrder: currentSort?.desc ? 'desc' : 'asc',
        includeTotal,
      }, { signal }),
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const currentData = query.isPlaceholderData ? undefined : query.data;

  const columns = useMemo(
    () => [
      columnHelper.accessor('minuteTs', {
        id: 'minuteTs',
        header: '时间',
        meta: { className: 'col-time', nowrap: true },
        cell: (info) => formatDateTime(info.getValue()),
      }),
      columnHelper.accessor('origSrc', { id: 'origSrc', header: '来源 IP', meta: { className: 'col-fwd-ip', nowrap: false } }),
      columnHelper.accessor('origDst', { id: 'origDst', header: '目标 IP', meta: { className: 'col-fwd-ip', nowrap: false } }),
      columnHelper.accessor('origSport', { id: 'origSport', header: '来源端口', enableSorting: false, meta: { className: 'col-fwd-port', align: 'right', nowrap: true } }),
      columnHelper.accessor('origDport', { id: 'origDport', header: '目标端口', enableSorting: false, meta: { className: 'col-fwd-port', align: 'right', nowrap: true } }),
      columnHelper.accessor('bytesOrig', {
        id: 'bytesOrig',
        header: '原向流量',
        meta: { className: 'col-bytes', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('bytesReply', {
        id: 'bytesReply',
        header: '回包流量',
        meta: { className: 'col-bytes', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor('flowCount', { id: 'flowCount', header: '流数', meta: { className: 'col-fwd-count', align: 'right', nowrap: true } }),
      columnHelper.accessor((row) => row.bytesOrig + row.bytesReply, {
        id: 'bytesTotal',
        header: '总量',
        meta: { className: 'col-bytes col-bytes-total', align: 'right', nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
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
          <p>
            用于单独观察 NAT/转发路径上的通信关系，重点关注来源 IP、目标 IP 与原向/回包字节差异。
            该视图不与本机进程流量混算，适合排查网关、旁路或代理场景中的异常转发。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {currentData ? <DataSourceBadge dataSource={currentData.dataSource} coverage={currentData.coverage} /> : null}
          </section>
        </div>
        <div className="page-head-actions">
          <RangeSelect value={range} onChange={setRange} />
          <RefreshControl updatedAt={query.dataUpdatedAt} isFetching={query.isFetching} onRefresh={query.refetch} />
        </div>
      </header>

      <section className="filters" style={{ marginBottom: 16 }}>
        <label>
          <span>来源 IP</span>
          <input
            value={filters.origSrcIp}
            onChange={(event) => setFilter('origSrcIp', event.target.value)}
            placeholder="10.0.0.2"
          />
        </label>
        <label>
          <span>目标 IP</span>
          <input
            value={filters.origDstIp}
            onChange={(event) => setFilter('origDstIp', event.target.value)}
            placeholder="1.1.1.1"
          />
        </label>
        <label>
          <span>协议</span>
          <CustomSelect
            value={filters.proto}
            options={protoOptions}
            onChange={(value) => setFilter('proto', value)}
          />
        </label>
      </section>

      {query.isError && (currentData?.rows.length ?? 0) > 0 ? (
        <QueryErrorState error={query.error} title="转发流量刷新失败，当前展示上次成功结果" compact onRetry={query.refetch} retrying={query.isFetching} />
      ) : null}

      {query.isError && !currentData?.rows.length ? (
        <QueryErrorState error={query.error} title="转发流量加载失败" onRetry={query.refetch} retrying={query.isFetching} />
      ) : query.isPlaceholderData || (query.isPending && !currentData) ? (
        <EmptyState title="转发流量加载中" description="正在汇总当前时间范围内的 forward / NAT 流量。" />
      ) : currentData?.rows.length ? (
        <DataTable
          columns={columns}
          data={currentData.rows}
          tableClassName="forward-table table-dense"
          sorting={sorting}
          onSortingChange={setSorting}
          manualSorting
          busy={query.isFetching}
          pagination={{
            page: currentData.page,
            pageSize: currentData.pageSize,
            totalRows: currentData.totalRows,
            hasMore: currentData.hasMore,
            totalRowsExact: currentData.totalRowsExact,
            onPageChange: setPage,
            onRequestExactTotal: requestExactTotal,
            exactTotalBusy: query.isFetching && includeTotal,
          }}
        />
      ) : (
        <EmptyState title="暂无转发流量" description="这台机器当前没有 forward 方向的聚合结果。" />
      )}
    </div>
  );
}
