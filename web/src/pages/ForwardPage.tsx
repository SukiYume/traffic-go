import { useMemo, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { createColumnHelper, type SortingState } from '@tanstack/react-table';
import { useSearchParams } from 'react-router-dom';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { CustomSelect } from '../components/CustomSelect';
import { DataTable } from '../components/DataTable';
import { EmptyState } from '../components/EmptyState';
import { QueryErrorState } from '../components/QueryErrorState';
import { RangeSelect } from '../components/RangeSelect';
import { useApiClient } from '../api-context';
import { normalizeRangeKey } from '../ranges';
import { normalizeForwardSortKey } from '../sort-keys';
import type { ForwardUsageRow, RangeKey } from '../types';
import { useResettingPage } from '../useResettingPage';
import { formatBytes, formatDateTime, rangeLabel } from '../utils';

const defaultRange = '24h' satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<ForwardUsageRow>();
const protoOptions = [
  { value: '', label: '全部协议' },
  { value: 'tcp', label: 'TCP' },
  { value: 'udp', label: 'UDP' },
];

export function ForwardPage() {
  const api = useApiClient();
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get('range'), defaultRange);
  const filters = {
    origSrcIp: params.get('origSrcIp') ?? '',
    origDstIp: params.get('origDstIp') ?? '',
    proto: params.get('proto') ?? '',
  };
  const [sorting, setSorting] = useState<SortingState>([{ id: 'minuteTs', desc: true }]);

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };

  const setFilter = (key: keyof typeof filters, value: string) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', range);
    if (value) {
      nextParams.set(key, value);
    } else {
      nextParams.delete(key);
    }
    setParams(nextParams, { replace: true });
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
  const query = useQuery({
    queryKey: ['forward', range, filters.origSrcIp, filters.origDstIp, filters.proto, page, currentSort?.id, currentSort?.desc],
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
      }, { signal }),
    placeholderData: keepPreviousData,
  });

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
      columnHelper.display({
        id: 'bytesTotal',
        header: '总量',
        meta: { className: 'col-bytes col-bytes-total', align: 'right', nowrap: true },
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
          <p>
            用于单独观察 NAT/转发路径上的通信关系，重点关注来源 IP、目标 IP 与原向/回包字节差异。
            该视图不与本机进程流量混算，适合排查网关、旁路或代理场景中的异常转发。
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

      {query.isError && (query.data?.rows.length ?? 0) > 0 ? (
        <QueryErrorState error={query.error} title="转发流量刷新失败，当前展示旧结果" compact />
      ) : null}

      {query.isError && !query.data?.rows.length ? (
        <QueryErrorState error={query.error} title="转发流量加载失败" />
      ) : query.isPending && !query.data ? (
        <EmptyState title="转发流量加载中" description="正在汇总当前时间范围内的 forward / NAT 流量。" />
      ) : query.data?.rows.length ? (
        <DataTable
          columns={columns}
          data={query.data.rows}
          tableClassName="forward-table table-dense"
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
