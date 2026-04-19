import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { createColumnHelper, type SortingState } from "@tanstack/react-table";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { DataSourceBadge } from "../components/DataSourceBadge";
import { DataTable } from "../components/DataTable";
import { EmptyState } from "../components/EmptyState";
import { FiltersBar } from "../components/FiltersBar";
import { QueryErrorState } from "../components/QueryErrorState";
import { RangeSelect } from "../components/RangeSelect";
import { isDimensionUnavailableError, normalizeUsageSortKey } from "../api";
import { useApiClient } from "../api-context";
import { normalizeRangeKey } from "../ranges";
import type { DataSource, RangeKey, UsageRow } from "../types";
import {
  attributionDescription,
  clampText,
  directionLabel,
  displayExecutableName,
  executableName,
  formatBytes,
  formatDateTime,
  formatNumber,
  minuteDimensionsUnavailable,
  rangeLabel,
  safeText,
  serviceNameForPort,
} from "../utils";

const defaultRange = "24h" satisfies RangeKey;
const pageSize = 25;
const columnHelper = createColumnHelper<UsageRow>();

function usageRowKey(row: UsageRow) {
  return JSON.stringify([
    row.minuteTs,
    row.proto,
    row.direction,
    row.pid ?? "",
    row.comm ?? "",
    row.exe ?? "",
    row.localPort ?? "",
    row.remoteIp ?? "",
    row.remotePort ?? "",
    row.attribution ?? "",
  ]);
}

function useUsageFilters() {
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get("range"), defaultRange);

  const filters = {
    comm: params.get("comm") ?? "",
    pid: params.get("pid") ?? "",
    exe: params.get("exe") ?? "",
    remoteIp: params.get("remoteIp") ?? "",
    localPort: params.get("localPort") ?? "",
    direction: params.get("direction") ?? "",
    proto: params.get("proto") ?? "",
    attribution: params.get("attribution") ?? "",
  };

  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set("range", next);
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

function clearMinuteOnlyFilters(
  filters: ReturnType<typeof useUsageFilters>["filters"],
) {
  return {
    ...filters,
    pid: "",
    exe: "",
    attribution: "",
  };
}

function processAnalysis(row: UsageRow) {
  if (
    row.comm === "nginx" ||
    row.comm === "caddy" ||
    row.comm === "apache2" ||
    row.comm === "httpd"
  ) {
    const isOut = row.direction === "out";
    const desc = isOut
      ? `(出站) 这表示 ${row.comm} 正在作为反向代理，向对端目标 (${row.remoteIp}) 发起请求。`
      : `(入站) 客户端 (${row.remoteIp}) 正在访问服务器端。`;
    return `💡 Nginx/Web 服务：${desc} 
因 HTTPS 或 L7 封装，底层探针只记录双向字节流，无法直接抓取 HTTP 层域名或 URI。可结合对端 IP 检索网站存取日志 (Access Log) 进一步溯源。`;
  }
  if (row.comm && row.comm.includes("ss-")) {
    const isOut = row.direction === "out";
    const desc = isOut
      ? `作为出站代理隧道，向对端 (${row.remoteIp}) 发送并请求数据。`
      : `作为服务端接收了来自对端 (${row.remoteIp}) 的加密代理请求。`;
    return `💡 Shadowsocks 代理隧道：${desc}
由于载荷完全加密，且内核级探针无法进行应用层穿透，若需审计翻墙/代理的具体目标网站，需要在 SS 宿主层面注入日志或者开启详细记录日志。`;
  }
  if (row.comm === "sshd") {
    return `💡 SSH 服务：对端 (${row.remoteIp}) 正在与主机通过 22 端口 (或改写端口) 通信。如流量巨大请关注是否在进行 SFTP 大文件传输或构建 SSH 隐藏隧道。`;
  }
  if (!row.comm && row.remotePort === 443) {
    return `💡 HTTPS 流：通用加密网页流量。在 Linux 内核层只能获取加密握手后的四层载荷数据。对端IP可通过 ping 或 curl 测试以获取对应的主机/公司信息。`;
  }
  if (row.comm === "docker-proxy") {
    return `💡 Docker 容器映射：此流量由 docker-proxy 桥接。由于主机端口被容器占用，真实流量往往转发到了容器内部进程。`;
  }
  return null;
}

function explainConfidenceLabel(confidence: "low" | "medium" | "high") {
  return {
    low: "低",
    medium: "中",
    high: "高",
  }[confidence];
}

function UsageExpandPanel({
  row,
  onFilterByIp,
  dataSource,
}: {
  row: UsageRow;
  onFilterByIp: (ip: string) => void;
  dataSource?: DataSource;
}): ReactNode {
  const api = useApiClient();
  const serviceName = serviceNameForPort(row.remotePort, row.proto);
  const portLabel =
    row.remotePort != null
      ? serviceName
        ? `${row.remotePort} / ${serviceName}`
        : String(row.remotePort)
      : "未知";
  const rateUp = Math.round(row.bytesUp / 60);
  const rateDown = Math.round(row.bytesDown / 60);
  const analysisHint = processAnalysis(row);
  const explainQuery = useQuery({
    queryKey: [
      "usage-explain",
      dataSource,
      row.minuteTs,
      row.proto,
      row.direction,
      row.pid,
      row.comm,
      row.exe,
      row.localPort,
      row.remoteIp,
      row.remotePort,
      row.attribution,
    ],
    queryFn: () => api.getUsageExplain(row, { dataSource }),
    enabled: dataSource != null,
    staleTime: 30_000,
  });
  const explain = explainQuery.data;

  return (
    <div className="row-expand">
      {analysisHint && (
        <div className="row-expand-alert">
          {analysisHint.split("\n").map((line, i) => (
            <div key={i}>{line}</div>
          ))}
        </div>
      )}
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
      <section className="row-expand-analysis">
        <div className="row-expand-analysis-head">
          <span>关联分析</span>
          <strong>
            {explain
              ? `置信度 ${explainConfidenceLabel(explain.confidence)}`
              : explainQuery.isPending
                ? "分析中…"
                : "未命中"}
          </strong>
        </div>
        {explainQuery.isError ? (
          <div className="row-expand-analysis-note">分析失败，请稍后重试。</div>
        ) : null}
        {explain ? (
          <>
            <div className="row-expand-grid row-expand-grid-analysis">
              <div>
                <span>来源 IP 候选</span>
                <strong>
                  {explain.sourceIps.length
                    ? explain.sourceIps.join(" · ")
                    : "暂无"}
                </strong>
              </div>
              <div>
                <span>目标 IP 候选</span>
                <strong>
                  {explain.targetIps.length
                    ? explain.targetIps.join(" · ")
                    : "暂无"}
                </strong>
              </div>
              <div>
                <span>关联样本</span>
                <strong>{explain.relatedPeers.length} 条</strong>
              </div>
            </div>

            {explain.chains.length > 0 ? (
              <div className="row-expand-analysis-list">
                <span>访问链路候选</span>
                {explain.chains.map((chain, index) => {
                  const left = chain.sourceIp ?? "来源待定";
                  const middle =
                    chain.localPort != null
                      ? `${row.comm ?? "process"}:${chain.localPort}`
                      : (row.comm ?? "process");
                  const rightHost = chain.targetHost
                    ? ` (${chain.targetHost})`
                    : "";
                  const rightPort =
                    chain.targetPort != null ? `:${chain.targetPort}` : "";
                  const right =
                    chain.targetIp ?? chain.targetHost ?? "目标待定";
                  return (
                    <div
                      key={
                        chain.chainId ??
                        `${chain.sourceIp ?? "unknown"}-${chain.targetIp ?? chain.targetHost ?? index}`
                      }
                      className="usage-chain"
                    >
                      <strong>
                        {left} → {middle} → {right}
                        {right === chain.targetIp
                          ? `${rightHost}${rightPort}`
                          : chain.targetPort != null
                            ? `:${chain.targetPort}`
                            : ""}
                      </strong>
                      <span>
                        {chain.evidenceSource ?? chain.evidence} ·{" "}
                        {chain.confidence} · {chain.evidenceCount} 条样本
                        {chain.flowCount > 0
                          ? ` · ${chain.flowCount} flows`
                          : ""}
                        {chain.bytesTotal > 0
                          ? ` · ${formatBytes(chain.bytesTotal)}`
                          : ""}
                        {chain.sampleTime
                          ? ` · ${formatDateTime(chain.sampleTime)}`
                          : ""}
                      </span>
                    </div>
                  );
                })}
              </div>
            ) : null}

            {explain.sourceIps.length > 0 || explain.targetIps.length > 0 ? (
              <div className="row-expand-actions">
                {explain.sourceIps.slice(0, 3).map((ip) => (
                  <button
                    key={`src-${ip}`}
                    type="button"
                    className="chip"
                    onClick={() => onFilterByIp(ip)}
                  >
                    来源 IP：{ip}
                  </button>
                ))}
                {explain.targetIps.slice(0, 3).map((ip) => (
                  <button
                    key={`dst-${ip}`}
                    type="button"
                    className="chip"
                    onClick={() => onFilterByIp(ip)}
                  >
                    目标 IP：{ip}
                  </button>
                ))}
              </div>
            ) : null}

            {explain.nginxRequests.length > 0 ? (
              <div className="row-expand-analysis-list">
                <span>网页访问线索（聚合）</span>
                {explain.nginxRequests.slice(0, 5).map((request, index) => (
                  <div
                    key={`${request.time}-${request.path}-${index}`}
                    className="row-expand-analysis-note"
                  >
                    {formatDateTime(request.time)} · {request.method}{" "}
                    {request.host ? `${request.host}` : ""}
                    {request.path} · {request.status} · {request.count} 次
                    {request.bot
                      ? ` · ${request.bot}`
                      : request.userAgent
                        ? ` · ${clampText(request.userAgent, 64)}`
                        : ""}
                    {request.referer
                      ? ` · 来路 ${clampText(request.referer, 64)}`
                      : ""}
                  </div>
                ))}
              </div>
            ) : null}

            {explain.notes.slice(0, 3).map((note) => (
              <div key={note} className="row-expand-analysis-note">
                {note}
              </div>
            ))}
          </>
        ) : null}
      </section>
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
  const [page, setPage] = useState(1);
  const [sorting, setSorting] = useState<SortingState>([
    { id: "minuteTs", desc: true },
  ]);
  const [expandedRowKey, setExpandedRowKey] = useState<string | null>(null);

  // Serialize filters to a stable string so this effect only fires when filter
  // values actually change, not on every render (filters object is recreated each render).
  const filtersKey = JSON.stringify(filters);

  useEffect(() => {
    setPage(1);
    setExpandedRowKey(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range, filtersKey, sorting]);

  useEffect(() => {
    setExpandedRowKey(null);
  }, [page]);

  const currentSort = sorting[0];

  const query = useQuery({
    queryKey: [
      "usage",
      range,
      filters,
      page,
      currentSort?.id,
      currentSort?.desc,
    ],
    queryFn: () =>
      api.getUsage({
        range,
        ...filters,
        page,
        pageSize,
        sortBy: normalizeUsageSortKey(currentSort?.id),
        sortOrder: currentSort?.desc ? "desc" : "asc",
      }),
    placeholderData: keepPreviousData,
  });
  const processes = useQuery({
    queryKey: ["processes"],
    queryFn: () => api.getProcesses(),
  });

  useEffect(() => {
    if (!minuteDimensionsUnavailable(query.data?.dataSource ?? null)) return;
    if (!filters.pid && !filters.exe && !filters.attribution) return;
    setFilters(clearMinuteOnlyFilters(filters));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filtersKey, query.data?.dataSource]);

  useEffect(() => {
    if (!isDimensionUnavailableError(query.error)) return;
    if (!filters.pid && !filters.exe && !filters.attribution) return;
    setFilters(clearMinuteOnlyFilters(filters));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filtersKey, query.error]);

  const onFilterByIp = useCallback(
    (ip: string) => {
      setFilters({ ...filters, remoteIp: ip });
      setExpandedRowKey(null);
    },
    [filters, setFilters],
  );

  const columns = useMemo(() => {
    const isHourly = query.data?.dataSource === "usage_1h";

    const baseColumns = [
      columnHelper.accessor("minuteTs", {
        id: "minuteTs",
        header: "时间",
        meta: { className: "col-time", nowrap: true },
        cell: (info) => formatDateTime(info.getValue()),
      }),
      columnHelper.accessor("direction", {
        id: "direction",
        header: "方向",
        meta: { className: "col-direction", align: "center", nowrap: true },
        cell: (info) => directionLabel(info.getValue()),
      }),
      columnHelper.accessor("remoteIp", {
        id: "remoteIp",
        header: "对端 IP",
        meta: { className: "col-remote-ip", nowrap: true },
        cell: (info) => {
          const ip = info.getValue();
          if (!ip) return "未知";
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
      columnHelper.accessor("localPort", {
        id: "localPort",
        header: "本地端口",
        meta: { className: "col-local-port", align: "center", nowrap: true },
        cell: (info) => info.getValue() ?? "未知",
      }),
      columnHelper.accessor("proto", {
        id: "proto",
        header: "协议",
        meta: { className: "col-proto", align: "center", nowrap: true },
      }),
      columnHelper.accessor("comm", {
        id: "comm",
        header: "进程",
        meta: { className: "col-comm", nowrap: true },
        cell: (info) => safeText(info.getValue()),
      }),
    ];

    const detailedColumns = isHourly
      ? []
      : [
          columnHelper.accessor("pid", {
            id: "pid",
            header: "PID",
            meta: { className: "col-pid", align: "right", nowrap: true },
            cell: (info) => info.getValue() ?? "未知",
          }),
          columnHelper.accessor("exe", {
            id: "exe",
            header: "EXE",
            enableSorting: false,
            meta: { className: "col-exe", nowrap: true },
            cell: (info) => {
              const raw = info.getValue();
              const cmd = executableName(raw);
              return (
                <span title={cmd ?? undefined}>
                  {clampText(displayExecutableName(raw), 28)}
                </span>
              );
            },
          }),
          columnHelper.accessor("remotePort", {
            id: "remotePort",
            header: "对端端口",
            enableSorting: false,
            meta: {
              className: "col-remote-port",
              align: "center",
              nowrap: true,
            },
            cell: (info) => {
              const port = info.getValue();
              if (port == null) return "—";
              const svc = serviceNameForPort(port, info.row.original.proto);
              return svc ? `${port} / ${svc}` : String(port);
            },
          }),
          columnHelper.accessor("attribution", {
            id: "attribution",
            header: "归因",
            enableSorting: false,
            meta: { className: "col-attribution", nowrap: true },
            cell: (info) => safeText(info.getValue()),
          }),
        ];

    const tailColumns = [
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
      columnHelper.display({
        id: "bytesTotal",
        header: "总流量",
        meta: {
          className: "col-bytes col-bytes-total",
          align: "right",
          nowrap: true,
        },
        cell: (info) =>
          formatBytes(info.row.original.bytesUp + info.row.original.bytesDown),
      }),
    ];

    return [...baseColumns, ...detailedColumns, ...tailColumns];
  }, [query.data?.dataSource, onFilterByIp]);

  const usageTableClassName =
    query.data?.dataSource === "usage_1h"
      ? "usage-table usage-table-hourly table-dense"
      : "usage-table table-dense";

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <p className="eyebrow">Usage</p>
          <h2>流量明细</h2>
          <p>
            这里展示逐条连接记录，适合做精细排查：建议先按时间和方向收敛范围，再按进程、PID、EXE
            或对端 IP 过滤， 点击任意行可展开查看速率、归因与日志关联线索。
            <br />
            <strong style={{ color: "#94a3b8" }}>归因状态说明：</strong>{" "}
            exact(精确命中
            PID)、heuristic(短时端口复用推测)、guess(系统尝试猜测)、unknown(未能获取关联)。
          </p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {query.data ? (
              <DataSourceBadge dataSource={query.data.dataSource} />
            ) : null}
          </section>
        </div>
        <RangeSelect value={range} onChange={setRange} />
      </header>

      <FiltersBar
        dataSource={query.data?.dataSource}
        processes={processes.data?.processes ?? []}
        filters={filters}
        onChange={setFilters}
      />

      {query.isError && query.data?.rows.length ? (
        <QueryErrorState
          error={query.error}
          title="明细刷新失败，当前展示旧结果"
          compact
        />
      ) : null}

      {query.isError && !query.data?.rows.length ? (
        <QueryErrorState error={query.error} title="明细加载失败" />
      ) : query.data?.rows.length ? (
        <DataTable
          columns={columns}
          data={query.data.rows}
          tableClassName={usageTableClassName}
          sorting={sorting}
          onSortingChange={setSorting}
          manualSorting
          expandedRowKey={expandedRowKey}
          onExpandRowKeyChange={setExpandedRowKey}
          getExpandedRowKey={usageRowKey}
          renderExpandedRow={(row) => (
            <UsageExpandPanel
              row={row}
              onFilterByIp={onFilterByIp}
              dataSource={query.data?.dataSource}
            />
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
