import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useRef } from "react";
import type { KeyboardEvent } from "react";
import { Link } from "react-router-dom";
import { ChartPanel } from "../components/ChartPanel";
import { DataSourceBadge } from "../components/DataSourceBadge";
import { EmptyState } from "../components/EmptyState";
import { QueryErrorState } from "../components/QueryErrorState";
import { RangeSelect } from "../components/RangeSelect";
import { RefreshControl } from "../components/RefreshControl";
import { StatCard } from "../components/StatCard";
import { useApiClient } from "../api-context";
import { latestSuccessfulUpdate, liveQueryOptions } from "../query-refresh";
import { buildRangedPath, useRangeSearchParam } from "../useRangeSearchParam";
import {
  displayExecutableName,
  formatBytes,
  peerRoleLabel,
  rangeLabel,
  safeText,
} from "../utils";
import type { TimeSeriesPoint } from "../types";

type DashboardTrafficView = "interface" | "direction";

function normalizeTrafficView(value: string | null): DashboardTrafficView {
  return value === "direction" ? "direction" : "interface";
}

function StatGridSkeleton() {
  return (
    <section
      className="stat-grid stat-grid-loading"
      aria-label="总览统计加载中"
    >
      {["总上行", "总下行", "活跃连接", "活跃进程"].map((label) => (
        <div key={label} className="stat-card stat-card-skeleton">
          <span>{label}</span>
          <strong aria-hidden="true" />
        </div>
      ))}
    </section>
  );
}

export function DashboardPage() {
  const api = useApiClient();
  const { params, setParams, range, setRange } = useRangeSearchParam();
  const trafficView = normalizeTrafficView(params.get("traffic_view"));
  const setTrafficView = (next: DashboardTrafficView) => {
    const nextParams = new URLSearchParams(params);
    if (next === "interface") {
      nextParams.delete("traffic_view");
    } else {
      nextParams.set("traffic_view", next);
    }
    setParams(nextParams, { replace: true });
  };

  const useCombinedDashboard = typeof api.getDashboard === "function";
  const dashboard = useQuery({
    queryKey: ["dashboard", range],
    queryFn: ({ signal }) => api.getDashboard!(range, { signal }),
    enabled: useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const networkSeries = useQuery({
    queryKey: ["network-series", range],
    queryFn: ({ signal }) => api.getNetworkTimeSeries(range, { signal }),
    enabled: trafficView === "interface",
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacyOverview = useQuery({
    queryKey: ["overview", range],
    queryFn: ({ signal }) => api.getOverview(range, { signal }),
    enabled: !useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacySeries = useQuery({
    queryKey: ["series", range, "direction"],
    queryFn: ({ signal }) =>
      api.getTimeSeries(range, "direction", undefined, { signal }),
    enabled: !useCombinedDashboard && trafficView === "direction",
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacyTopProcesses = useQuery({
    queryKey: ["top-processes", range, "dashboard"],
    queryFn: ({ signal }) =>
      api.getTopProcesses(
        range,
        { page: 1, pageSize: 5, groupBy: "pid", summary: false },
        { signal },
      ),
    enabled: !useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacyTopInboundRemotes = useQuery({
    queryKey: ["top-remotes", range, "in"],
    queryFn: ({ signal }) =>
      api.getTopRemotes(
        range,
        {
          page: 1,
          pageSize: 5,
          direction: "in",
          includeLoopback: true,
          summary: true,
        },
        { signal },
      ),
    enabled: !useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacyTopOutboundRemotes = useQuery({
    queryKey: ["top-remotes", range, "out"],
    queryFn: ({ signal }) =>
      api.getTopRemotes(
        range,
        {
          page: 1,
          pageSize: 5,
          direction: "out",
          includeLoopback: true,
          summary: true,
        },
        { signal },
      ),
    enabled: !useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });
  const legacyTopPorts = useQuery({
    queryKey: ["top-ports", range],
    queryFn: ({ signal }) => api.getTopPorts(range, { signal }),
    enabled: !useCombinedDashboard,
    placeholderData: keepPreviousData,
    ...liveQueryOptions,
  });

  const overview = useCombinedDashboard ? dashboard : legacyOverview;
  const series = useCombinedDashboard ? dashboard : legacySeries;
  const topProcesses = useCombinedDashboard ? dashboard : legacyTopProcesses;
  const topInboundRemotes = useCombinedDashboard
    ? dashboard
    : legacyTopInboundRemotes;
  const topOutboundRemotes = useCombinedDashboard
    ? dashboard
    : legacyTopOutboundRemotes;
  const topPorts = useCombinedDashboard ? dashboard : legacyTopPorts;
  const dashboardData =
    useCombinedDashboard && !dashboard.isPlaceholderData
      ? dashboard.data
      : undefined;
  const overviewData = useCombinedDashboard
    ? dashboardData?.overview
    : legacyOverview.isPlaceholderData
      ? undefined
      : legacyOverview.data;
  const networkData = networkSeries.isPlaceholderData
    ? undefined
    : networkSeries.data;
  const seriesData = useCombinedDashboard
    ? dashboardData?.timeseries
    : legacySeries.isPlaceholderData
      ? undefined
      : legacySeries.data;
  const topProcessesData = useCombinedDashboard
    ? dashboardData?.topProcesses
    : legacyTopProcesses.isPlaceholderData
      ? undefined
      : legacyTopProcesses.data;
  const topInboundData = useCombinedDashboard
    ? dashboardData?.topInboundRemotes
    : legacyTopInboundRemotes.isPlaceholderData
      ? undefined
      : legacyTopInboundRemotes.data;
  const topOutboundData = useCombinedDashboard
    ? dashboardData?.topOutboundRemotes
    : legacyTopOutboundRemotes.isPlaceholderData
      ? undefined
      : legacyTopOutboundRemotes.data;
  const topPortsData = useCombinedDashboard
    ? dashboardData?.topPorts
    : legacyTopPorts.isPlaceholderData
      ? undefined
      : legacyTopPorts.data;

  const selectedTrafficTotals =
    trafficView === "interface" && networkData
      ? summarizeTrafficPoints(networkData.points)
      : trafficView === "direction" && overviewData
        ? { bytesUp: overviewData.bytesUp, bytesDown: overviewData.bytesDown }
        : null;
  const selectedTrafficDataSource =
    trafficView === "interface"
      ? networkData?.dataSource
      : overviewData?.dataSource;
  const selectedTrafficCoverage =
    trafficView === "interface"
      ? networkData?.coverage
      : overviewData?.coverage;
  const cards =
    overviewData && selectedTrafficTotals
      ? [
          {
            label: "总上行",
            value: selectedTrafficTotals.bytesUp,
            suffix: "bytes" as const,
          },
          {
            label: "总下行",
            value: selectedTrafficTotals.bytesDown,
            suffix: "bytes" as const,
          },
          {
            label: "活跃连接",
            value: overviewData.activeConnections,
            suffix: "count" as const,
          },
          {
            label: "活跃进程",
            value: overviewData.activeProcesses,
            suffix: "count" as const,
          },
        ]
      : [];

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <h2>流量总览</h2>
          <p>
            这是默认的全局视图：先看总上/下行、活跃连接与活跃进程，再结合趋势图判断是否存在突发峰值。若发现异常，
            可直接从下方 Top 列表进入进程、对端或明细页面继续排查。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {selectedTrafficDataSource ? (
              <DataSourceBadge
                dataSource={selectedTrafficDataSource}
                coverage={selectedTrafficCoverage}
              />
            ) : null}
          </section>
        </div>
        <div className="page-head-actions">
          <RangeSelect value={range} onChange={setRange} />
          <RefreshControl
            updatedAt={
              useCombinedDashboard
                ? latestSuccessfulUpdate(
                    dashboard.dataUpdatedAt,
                    networkSeries.dataUpdatedAt,
                  )
                : latestSuccessfulUpdate(
                    legacyOverview.dataUpdatedAt,
                    trafficView === "interface"
                      ? networkSeries.dataUpdatedAt
                      : legacySeries.dataUpdatedAt,
                    legacyTopProcesses.dataUpdatedAt,
                    legacyTopInboundRemotes.dataUpdatedAt,
                    legacyTopOutboundRemotes.dataUpdatedAt,
                    legacyTopPorts.dataUpdatedAt,
                  )
            }
            isFetching={
              useCombinedDashboard
                ? dashboard.isFetching || networkSeries.isFetching
                : legacyOverview.isFetching ||
                  networkSeries.isFetching ||
                  legacySeries.isFetching ||
                  legacyTopProcesses.isFetching ||
                  legacyTopInboundRemotes.isFetching ||
                  legacyTopOutboundRemotes.isFetching ||
                  legacyTopPorts.isFetching
            }
            onRefresh={() => {
              if (useCombinedDashboard) {
                return Promise.all([
                  dashboard.refetch(),
                  trafficView === "interface"
                    ? networkSeries.refetch()
                    : Promise.resolve(),
                ]);
              }
              return Promise.all([
                legacyOverview.refetch(),
                trafficView === "interface"
                  ? networkSeries.refetch()
                  : legacySeries.refetch(),
                legacyTopProcesses.refetch(),
                legacyTopInboundRemotes.refetch(),
                legacyTopOutboundRemotes.refetch(),
                legacyTopPorts.refetch(),
              ]);
            }}
          />
        </div>
      </header>

      {overview.isError ? (
        <QueryErrorState
          error={overview.error}
          title="总览加载失败"
          onRetry={overview.refetch}
          retrying={overview.isFetching}
        />
      ) : trafficView === "interface" && networkSeries.isError ? (
        <QueryErrorState
          error={networkSeries.error}
          title="网卡口径加载失败"
          onRetry={networkSeries.refetch}
          retrying={networkSeries.isFetching}
        />
      ) : cards.length ? (
        <section className="stat-grid">
          {cards.map((card) => (
            <StatCard key={card.label} {...card} />
          ))}
        </section>
      ) : (
        <StatGridSkeleton />
      )}

      {trafficView === "interface" &&
      networkSeries.isError ? null : trafficView === "direction" &&
        series.isError ? (
        <QueryErrorState
          error={series.error}
          title="连接方向趋势加载失败"
          onRetry={series.refetch}
          retrying={series.isFetching}
        />
      ) : trafficView === "interface" && networkData ? (
        <ChartPanel
          points={networkData.points}
          groupBy={undefined}
          range={range}
          title="网卡流量趋势"
          subtitle="按公网网卡聚合：接收 RX / 发送 TX"
          upLabel="发送 TX"
          downLabel="接收 RX"
          busy={networkSeries.isFetching}
          actions={
            <TrafficViewSelect value={trafficView} onChange={setTrafficView} />
          }
        />
      ) : trafficView === "direction" && seriesData ? (
        <ChartPanel
          points={seriesData.points}
          groups={seriesData.groups}
          groupBy={seriesData.groupBy}
          range={range}
          title="连接方向趋势"
          subtitle="按方向聚合：入站 / 出站总量"
          busy={series.isFetching}
          actions={
            <TrafficViewSelect value={trafficView} onChange={setTrafficView} />
          }
        />
      ) : (
        <EmptyState title="趋势加载中" description="正在获取时间序列。" />
      )}

      <section className="panel-grid dashboard-grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Top 进程</h2>
            <Link to={buildRangedPath("/processes", range)}>查看全部</Link>
          </div>
          {topProcesses.isError ? (
            <QueryErrorState
              error={topProcesses.error}
              title="Top 进程加载失败"
              compact
              onRetry={topProcesses.refetch}
              retrying={topProcesses.isFetching}
            />
          ) : topProcesses.isPlaceholderData ||
            (topProcesses.isPending && !topProcessesData) ? (
            <EmptyState
              title="Top 进程加载中"
              description="正在获取当前时间范围的进程排行。"
            />
          ) : topProcessesData?.rows.length ? (
            <ol className="top-list detailed-list">
              {topProcessesData.rows.map((row, index) => (
                <li
                  key={`${row.pid ?? "none"}-${row.comm ?? "unknown"}-${index}`}
                >
                  <div>
                    <strong>{safeText(row.comm)}</strong>
                    <span>
                      {row.pid !== null
                        ? `PID ${row.pid}`
                        : "当前窗口已降级为按进程名聚合"}
                    </span>
                  </div>
                  <div className="top-value">
                    <strong>{formatBytes(row.totalBytes)}</strong>
                    <span>
                      {row.pid !== null
                        ? displayExecutableName(row.exe)
                        : "小时聚合 / EXE 在此视图不展示"}
                    </span>
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState
              title="暂无进程数据"
              description="当前时间范围没有命中的进程排行。"
            />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 入站来源 IP</h2>
            <Link
              to={buildRangedPath("/remotes", range, {
                direction: "in",
                include_loopback: "1",
              })}
            >
              查看全部
            </Link>
          </div>
          {topInboundRemotes.isError ? (
            <QueryErrorState
              error={topInboundRemotes.error}
              title="入站排行加载失败"
              compact
              onRetry={topInboundRemotes.refetch}
              retrying={topInboundRemotes.isFetching}
            />
          ) : topInboundRemotes.isPlaceholderData ||
            (topInboundRemotes.isPending && !topInboundData) ? (
            <EmptyState
              title="入站排行加载中"
              description="正在获取当前时间范围的入站来源。"
            />
          ) : topInboundData?.rows.length ? (
            <ol className="top-list detailed-list">
              {topInboundData.rows.map((row) => (
                <li key={`in-${row.remoteIp}`}>
                  <div>
                    <strong>{safeText(row.remoteIp)}</strong>
                    <span>{peerRoleLabel("in")}</span>
                  </div>
                  <div className="top-value">
                    <strong>{formatBytes(row.totalBytes)}</strong>
                    <span>
                      上行 {formatBytes(row.bytesUp)} / 下行{" "}
                      {formatBytes(row.bytesDown)}
                    </span>
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState
              title="暂无入站来源"
              description="当前时间范围没有命中的入站来源 IP。"
            />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 出站目标 IP</h2>
            <Link
              to={buildRangedPath("/remotes", range, {
                direction: "out",
                include_loopback: "1",
              })}
            >
              查看全部
            </Link>
          </div>
          {topOutboundRemotes.isError ? (
            <QueryErrorState
              error={topOutboundRemotes.error}
              title="出站排行加载失败"
              compact
              onRetry={topOutboundRemotes.refetch}
              retrying={topOutboundRemotes.isFetching}
            />
          ) : topOutboundRemotes.isPlaceholderData ||
            (topOutboundRemotes.isPending && !topOutboundData) ? (
            <EmptyState
              title="出站排行加载中"
              description="正在获取当前时间范围的出站目标。"
            />
          ) : topOutboundData?.rows.length ? (
            <ol className="top-list detailed-list">
              {topOutboundData.rows.map((row) => (
                <li key={`out-${row.remoteIp}`}>
                  <div>
                    <strong>{safeText(row.remoteIp)}</strong>
                    <span>{peerRoleLabel("out")}</span>
                  </div>
                  <div className="top-value">
                    <strong>{formatBytes(row.totalBytes)}</strong>
                    <span>
                      上行 {formatBytes(row.bytesUp)} / 下行{" "}
                      {formatBytes(row.bytesDown)}
                    </span>
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState
              title="暂无出站目标"
              description="当前时间范围没有命中的出站目标 IP。"
            />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 端口</h2>
            <Link to={buildRangedPath("/usage", range)}>查看全部</Link>
          </div>
          {topPorts.isError ? (
            <QueryErrorState
              error={topPorts.error}
              title="端口排行加载失败"
              compact
              onRetry={topPorts.refetch}
              retrying={topPorts.isFetching}
            />
          ) : topPorts.isPlaceholderData ||
            (topPorts.isPending && !topPortsData) ? (
            <EmptyState
              title="端口排行加载中"
              description="正在获取当前时间范围的端口排行。"
            />
          ) : topPortsData?.rows.length ? (
            <ol className="top-list">
              {topPortsData.rows.slice(0, 5).map((row) => (
                <li key={row.label}>
                  <span>{row.label}</span>
                  <strong>{formatBytes(row.value)}</strong>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState
              title="暂无端口数据"
              description="当前时间范围没有命中的端口排行。"
            />
          )}
        </section>
      </section>
    </div>
  );
}

function summarizeTrafficPoints(points: TimeSeriesPoint[]) {
  return points.reduce(
    (totals, point) => ({
      bytesUp: totals.bytesUp + point.up,
      bytesDown: totals.bytesDown + point.down,
    }),
    { bytesUp: 0, bytesDown: 0 },
  );
}

function TrafficViewSelect({
  value,
  onChange,
}: {
  value: DashboardTrafficView;
  onChange: (value: DashboardTrafficView) => void;
}) {
  const options: Array<{ value: DashboardTrafficView; label: string }> = [
    { value: "interface", label: "网卡口径" },
    { value: "direction", label: "连接方向" },
  ];
  const buttons = useRef<Array<HTMLButtonElement | null>>([]);
  const selectAt = (index: number) => {
    const normalized = (index + options.length) % options.length;
    onChange(options[normalized].value);
    buttons.current[normalized]?.focus();
  };
  const onKeyDown = (
    event: KeyboardEvent<HTMLButtonElement>,
    index: number,
  ) => {
    let nextIndex: number | null = null;
    if (event.key === "ArrowRight") nextIndex = index + 1;
    if (event.key === "ArrowLeft") nextIndex = index - 1;
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = options.length - 1;
    if (nextIndex == null) return;
    event.preventDefault();
    selectAt(nextIndex);
  };
  return (
    <div className="segmented-control" role="tablist" aria-label="流量趋势口径">
      {options.map((option, index) => (
        <button
          key={option.value}
          ref={(element) => {
            buttons.current[index] = element;
          }}
          type="button"
          role="tab"
          aria-selected={value === option.value}
          tabIndex={value === option.value ? 0 : -1}
          className={value === option.value ? "chip active" : "chip"}
          onClick={() => onChange(option.value)}
          onKeyDown={(event) => onKeyDown(event, index)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}
