import { useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { createColumnHelper, type SortingState } from "@tanstack/react-table";
import { useNavigate } from "react-router-dom";
import { DataSourceBadge } from "../components/DataSourceBadge";
import { DataTable } from "../components/DataTable";
import { EmptyState } from "../components/EmptyState";
import { QueryErrorState } from "../components/QueryErrorState";
import { RangeSelect } from "../components/RangeSelect";
import { RefreshControl } from "../components/RefreshControl";
import { useApiClient } from "../api-context";
import { liveQueryOptions } from "../query-refresh";
import { normalizeRemoteSortKey } from "../sort-keys";
import type { RemoteSummaryRow } from "../types";
import { buildRangedPath, useRangeSearchParam } from "../useRangeSearchParam";
import { useResettingPage } from "../useResettingPage";
import { useExactTotal } from "../useExactTotal";
import {
  directionLabel,
  formatBytes,
  isLoopbackIp,
  rangeLabel,
  safeText,
} from "../utils";

const pageSize = 25;
const columnHelper = createColumnHelper<RemoteSummaryRow>();

export function RemotesPage() {
  const api = useApiClient();
  const navigate = useNavigate();
  const { params, range, setRange, setRangedParams } = useRangeSearchParam();
  const direction = (params.get("direction") as "in" | "out" | null) ?? "";
  const includeLoopback =
    params.get("exclude_loopback") !== "1" &&
    params.get("include_loopback") !== "0";
  const [sorting, setSorting] = useState<SortingState>([
    { id: "bytesTotal", desc: true },
  ]);

  const setDirection = (next: "" | "in" | "out") => {
    setRangedParams({
      direction: next,
      exclude_loopback: includeLoopback ? undefined : "1",
    });
  };

  const setIncludeLoopback = (next: boolean) => {
    setRangedParams({
      direction,
      exclude_loopback: next ? undefined : "1",
    });
  };

  const currentSort = sorting[0];
  const pageResetKey = JSON.stringify([
    range,
    direction,
    includeLoopback,
    currentSort?.id ?? null,
    currentSort?.desc ?? null,
  ]);
  const [page, setPage] = useResettingPage(pageResetKey);
  const { includeTotal, requestExactTotal } = useExactTotal(pageResetKey);
  const query = useQuery({
    queryKey: [
      "top-remotes",
      range,
      direction,
      includeLoopback,
      page,
      currentSort?.id,
      currentSort?.desc,
      includeTotal,
    ],
    queryFn: ({ signal }) =>
      api.getTopRemotes(
        range,
        {
          page,
          pageSize,
          direction: direction || undefined,
          includeLoopback,
          sortBy: normalizeRemoteSortKey(currentSort?.id),
          sortOrder: currentSort?.desc ? "desc" : "asc",
          includeTotal,
        },
        { signal },
      ),
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const currentData = query.isPlaceholderData ? undefined : query.data;

  const columns = useMemo(
    () => [
      columnHelper.accessor("direction", {
        id: "direction",
        header: "方向",
        meta: { className: "col-direction", align: "center", nowrap: true },
        cell: (info) => directionLabel(info.getValue()),
      }),
      columnHelper.accessor("remoteIp", {
        id: "remoteIp",
        header: "对端 IP",
        meta: { className: "col-remote-ip col-remote-ip-plain", nowrap: false },
        cell: (info) => {
          const ip = info.getValue();
          return (
            <span className="ip-cell">
              <span>{safeText(ip)}</span>
              {isLoopbackIp(ip) ? (
                <span className="inline-tag">本机回环</span>
              ) : null}
            </span>
          );
        },
      }),
      columnHelper.accessor("bytesUp", {
        id: "bytesUp",
        header: "上行",
        meta: { className: "col-bytes", align: "right", nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor("bytesDown", {
        id: "bytesDown",
        header: "下行",
        meta: { className: "col-bytes", align: "right", nowrap: true },
        cell: (info) => formatBytes(info.getValue()),
      }),
      columnHelper.accessor("flowCount", {
        id: "flowCount",
        header: "流数",
        meta: { className: "col-fwd-count", align: "right", nowrap: true },
      }),
      columnHelper.accessor("totalBytes", {
        id: "bytesTotal",
        header: "总量",
        meta: {
          className: "col-bytes col-bytes-total",
          align: "right",
          nowrap: true,
        },
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
          <p>
            按对端 IP 聚合展示流量总量，适合先识别“哪个方向上的哪个 IP
            跑掉了流量”。这里是 IP 级排行，不直接展示进程与端口细节；
            点击任意对端行会跳转到「流量明细」并自动带入筛选条件，继续查看逐条连接和进程归因。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {currentData ? (
              <DataSourceBadge
                dataSource={currentData.dataSource}
                coverage={currentData.coverage}
              />
            ) : null}
          </section>
        </div>
        <div className="page-head-actions">
          <RangeSelect value={range} onChange={setRange} />
          <RefreshControl
            updatedAt={query.dataUpdatedAt}
            isFetching={query.isFetching}
            onRefresh={query.refetch}
          />
        </div>
      </header>

      <section className="segmented-control" role="group" aria-label="远端方向">
        <button
          type="button"
          aria-pressed={direction === ""}
          className={direction === "" ? "chip active" : "chip"}
          onClick={() => setDirection("")}
        >
          全部对端
        </button>
        <button
          type="button"
          aria-pressed={direction === "in"}
          className={direction === "in" ? "chip active" : "chip"}
          onClick={() => setDirection("in")}
        >
          入站来源
        </button>
        <button
          type="button"
          aria-pressed={direction === "out"}
          className={direction === "out" ? "chip active" : "chip"}
          onClick={() => setDirection("out")}
        >
          出站目标
        </button>
        <button
          type="button"
          aria-pressed={includeLoopback}
          className={includeLoopback ? "chip active" : "chip"}
          onClick={() => setIncludeLoopback(!includeLoopback)}
        >
          {includeLoopback ? "排除本机回环" : "显示本机回环"}
        </button>
      </section>

      {query.isError && (currentData?.rows.length ?? 0) > 0 ? (
        <QueryErrorState
          error={query.error}
          title="对端聚合刷新失败，当前展示上次成功结果"
          compact
          onRetry={query.refetch}
          retrying={query.isFetching}
        />
      ) : null}

      {query.isError && !currentData?.rows.length ? (
        <QueryErrorState
          error={query.error}
          title="对端聚合加载失败"
          onRetry={query.refetch}
          retrying={query.isFetching}
        />
      ) : query.isPlaceholderData || (query.isPending && !currentData) ? (
        <EmptyState
          title="对端聚合加载中"
          description="正在聚合当前时间范围内的对端 IP 数据。"
        />
      ) : currentData?.rows.length ? (
        <DataTable
          columns={columns}
          data={currentData.rows}
          tableClassName="remotes-table table-dense"
          sorting={sorting}
          onSortingChange={setSorting}
          manualSorting
          busy={query.isFetching}
          rowActionLabel={() => "查看明细"}
          onRowClick={(row) => {
            navigate(
              buildRangedPath("/usage", range, {
                remoteIp: row.remoteIp,
                direction: row.direction,
              }),
            );
          }}
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
        <EmptyState
          title="暂无对端聚合"
          description="当前时间范围没有命中的对端 IP 数据。"
        />
      )}
    </div>
  );
}
