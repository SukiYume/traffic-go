import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { App, RouteErrorBoundary } from "../App";
import { ApiError } from "../api";
import { ApiProvider } from "../api-context";
import { createMockApiClient } from "../data/mock";
import { ForwardPage } from "../pages/ForwardPage";
import { ProcessesPage } from "../pages/ProcessesPage";
import { RemotesPage } from "../pages/RemotesPage";
import { UsagePage } from "../pages/UsagePage";
import { DashboardPage } from "../pages/DashboardPage";
import { HistoryPage } from "../pages/HistoryPage";
import type { DataSource, ProcessGroupBy, TrafficApiClient } from "../types";

function renderWithProviders(
  path: string,
  element: ReactNode,
  client: TrafficApiClient = createMockApiClient(),
) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <ApiProvider client={client}>
        <MemoryRouter
          initialEntries={[path]}
          future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
        >
          {element}
        </MemoryRouter>
      </ApiProvider>
    </QueryClientProvider>,
  );
}

function pendingPromise<T>() {
  return new Promise<T>(() => {});
}

it("shows a refresh fallback when a lazy route fails", () => {
  const consoleError = vi
    .spyOn(console, "error")
    .mockImplementation(() => undefined);
  const suppressWindowError = (event: ErrorEvent) => event.preventDefault();
  window.addEventListener("error", suppressWindowError);
  function BrokenRoute(): ReactNode {
    throw new Error("missing route chunk");
  }

  render(
    <RouteErrorBoundary>
      <BrokenRoute />
    </RouteErrorBoundary>,
  );

  expect(screen.getByRole("alert")).toHaveTextContent("页面资源加载失败");
  expect(screen.getByRole("button", { name: "刷新页面" })).toBeInTheDocument();
  window.removeEventListener("error", suppressWindowError);
  consoleError.mockRestore();
});

function expectStatValue(label: string, value: string) {
  const labelElement = screen.getByText(label);
  const card = labelElement.closest("section");
  if (!card) {
    throw new Error(`expected stat card for ${label}`);
  }
  expect(within(card).getByText(value)).toBeInTheDocument();
}

function createAggregatedClient(
  dataSource: Extract<DataSource, "usage_1h" | "usage_1d">,
): TrafficApiClient {
  const base = createMockApiClient();
  return {
    ...base,
    async getUsage(query, requestOptions) {
      const response = await base.getUsage(query, requestOptions);
      return {
        ...response,
        dataSource,
        rows: response.rows.map((row) => ({
          ...row,
          pid: null,
          exe: null,
          remotePort: null,
          attribution: null,
        })),
      };
    },
    async getTopProcesses(range, options, requestOptions) {
      const response = await base.getTopProcesses(
        range,
        options,
        requestOptions,
      );
      return {
        ...response,
        dataSource,
        rows: response.rows.map((row) => ({ ...row, pid: null, exe: null })),
      };
    },
  };
}

describe("traffic-go web ui", () => {
  it("renders dashboard overview", async () => {
    renderWithProviders("/", <DashboardPage />);
    expect(await screen.findByText("流量总览")).toBeInTheDocument();
    expect(await screen.findByText("网卡流量趋势")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "网卡口径" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(await screen.findByText("总上行")).toBeInTheDocument();
    expect(await screen.findByText("Top 进程")).toBeInTheDocument();
  });

  it("keeps dashboard chart loading separate from overview stat loading", async () => {
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      getOverview() {
        return pendingPromise();
      },
    };

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("网卡流量趋势")).toBeInTheDocument();
    expect(screen.getByLabelText("总览统计加载中")).toBeInTheDocument();
    expect(screen.queryByText("总览加载中")).not.toBeInTheDocument();
  });

  it("defaults dashboard trend to network mode and switches to direction mode", async () => {
    const base = createMockApiClient();
    let networkCalls = 0;
    let directionCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getNetworkTimeSeries(range, requestOptions) {
        networkCalls += 1;
        return base.getNetworkTimeSeries(range, requestOptions);
      },
      async getTimeSeries(range, groupBy, filters, requestOptions) {
        directionCalls += 1;
        return base.getTimeSeries(range, groupBy, filters, requestOptions);
      },
    };
    const user = userEvent.setup();

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("网卡流量趋势")).toBeInTheDocument();
    await waitFor(() => expect(networkCalls).toBeGreaterThan(0));
    expect(directionCalls).toBe(0);

    screen.getByRole("tab", { name: "网卡口径" }).focus();
    await user.keyboard("{ArrowRight}");

    expect(await screen.findByText("连接方向趋势")).toBeInTheDocument();
    await waitFor(() => expect(directionCalls).toBeGreaterThan(0));
  });

  it("switches dashboard summary traffic totals with the selected traffic view", async () => {
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getOverview(range, requestOptions) {
        const response = await base.getOverview(range, requestOptions);
        return {
          ...response,
          bytesUp: 4096,
          bytesDown: 6144,
          activeConnections: 7,
          activeProcesses: 3,
        };
      },
      async getNetworkTimeSeries() {
        return {
          dataSource: "interface_1m",
          bucket: "1m",
          points: [
            {
              ts: 1710000000,
              up: 8192,
              down: 16384,
              flowCount: 0,
              label: "12:00",
            },
          ],
        };
      },
    };
    const user = userEvent.setup();

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("网卡流量趋势")).toBeInTheDocument();
    await waitFor(() => expectStatValue("总上行", "8.00 KB"));
    expectStatValue("总下行", "16.0 KB");
    expect(screen.getByText("interface_1m")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "连接方向" }));

    expect(await screen.findByText("连接方向趋势")).toBeInTheDocument();
    await waitFor(() => expectStatValue("总上行", "4.00 KB"));
    expectStatValue("总下行", "6.00 KB");
    expect(screen.getByText("usage_1m")).toBeInTheDocument();
  });

  it("shows a single dashboard error when network interface totals fail", async () => {
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getNetworkTimeSeries() {
        throw new Error("interface stats unavailable");
      },
    };

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("网卡口径加载失败")).toBeInTheDocument();
    expect(screen.getByText("interface stats unavailable")).toBeInTheDocument();
    expect(screen.queryByText("网卡趋势加载失败")).not.toBeInTheDocument();
  });

  it("queries dashboard top processes by pid and shows pid/exe in minute windows", async () => {
    const base = createMockApiClient();
    const topProcessCalls: Array<{
      groupBy: ProcessGroupBy;
      summary: boolean;
    }> = [];
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options, requestOptions) {
        topProcessCalls.push({
          groupBy: options?.groupBy ?? "pid",
          summary: !!options?.summary,
        });
        return base.getTopProcesses(range, options, requestOptions);
      },
    };

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("PID 1088")).toBeInTheDocument();
    expect(await screen.findAllByText("ss-server")).not.toHaveLength(0);
    expect(screen.queryByText("未归因 / EXE 不可用")).not.toBeInTheDocument();
    expect(topProcessCalls).toContainEqual({ groupBy: "pid", summary: false });
  });

  it("keeps dashboard top processes in comm fallback when the window is hourly", async () => {
    renderWithProviders(
      "/?range=last_month",
      <DashboardPage />,
      createAggregatedClient("usage_1h"),
    );

    expect(
      (await screen.findAllByText("当前窗口已降级为按进程名聚合")).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText("小时聚合 / EXE 在此视图不展示").length,
    ).toBeGreaterThan(0);
  });

  it("keeps dashboard monthly ranges on the aggregated interface trend", async () => {
    const base = createMockApiClient();
    let networkCalls = 0;
    let directionCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getNetworkTimeSeries(range, requestOptions) {
        networkCalls += 1;
        return base.getNetworkTimeSeries(range, requestOptions);
      },
      async getTimeSeries(range, groupBy, filters, requestOptions) {
        directionCalls += 1;
        return base.getTimeSeries(range, groupBy, filters, requestOptions);
      },
    };

    renderWithProviders("/?range=last_month", <DashboardPage />, client);

    expect(await screen.findByText("网卡流量趋势")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "网卡口径" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByText("interface_1d")).toBeInTheDocument();
    await waitFor(() => expect(networkCalls).toBeGreaterThan(0));
    expect(directionCalls).toBe(0);
  });

  it("keeps dashboard drill-down links on the current range and preserves the loopback flag for remotes", async () => {
    renderWithProviders("/?range=last_month", <DashboardPage />);
    expect(await screen.findByText("流量总览")).toBeInTheDocument();

    const hrefs = screen
      .getAllByRole("link", { name: "查看全部" })
      .map((link) => link.getAttribute("href"));

    expect(hrefs).toEqual(
      expect.arrayContaining([
        "/processes?range=last_month",
        "/remotes?range=last_month&direction=in&include_loopback=1",
        "/remotes?range=last_month&direction=out&include_loopback=1",
        "/usage?range=last_month",
      ]),
    );
  });

  it("disables pid and exe when the backend returns hourly data", async () => {
    const base = createAggregatedClient("usage_1h");
    let usageCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls += 1;
        if (query.pid || query.exe || query.attribution) {
          throw new ApiError(
            400,
            "dimension_unavailable",
            "minute-only dimensions are unavailable",
          );
        }
        return base.getUsage(query, requestOptions);
      },
    };
    renderWithProviders(
      "/usage?range=last_month&pid=1088&exe=ss-server&attribution=exact",
      <UsagePage />,
      client,
    );
    const pid = await screen.findByLabelText("PID");
    const exe = await screen.findByLabelText("EXE");
    const attributionLabel = await screen.findByText("全部归因");
    const attribution = attributionLabel.closest("button");
    await waitFor(() => expect(pid).toBeDisabled());
    expect(exe).toBeDisabled();
    expect(attribution).not.toBeNull();
    expect(attribution).toBeDisabled();
    expect(
      screen.getByText(
        "超过分钟明细保留窗口的数据会切换到小时表，PID / EXE 维度不可用。",
      ),
    ).toBeInTheDocument();
    expect(usageCalls).toBeGreaterThanOrEqual(2);
  });

  it("hides minute-only columns when usage data comes from the hourly source", async () => {
    renderWithProviders(
      "/usage?range=last_month",
      <UsagePage />,
      createAggregatedClient("usage_1h"),
    );
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "PID" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "EXE" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "归因" }),
    ).not.toBeInTheDocument();
  });

  it("hides minute-only columns when usage data comes from the daily source", async () => {
    renderWithProviders(
      "/usage?range=this_month",
      <UsagePage />,
      createAggregatedClient("usage_1d"),
    );
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "PID" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "EXE" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "归因" }),
    ).not.toBeInTheDocument();
  });

  it("replays persisted chain analysis for hourly usage rows", async () => {
    const user = userEvent.setup();
    const base = createAggregatedClient("usage_1h");
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        return base.getUsage(
          { ...query, sortBy: "minuteTs", sortOrder: "desc" },
          requestOptions,
        );
      },
    };

    renderWithProviders("/usage?range=last_month", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );

    expect(await screen.findByText("访问链路候选")).toBeInTheDocument();
    expect(
      screen.queryByText("当前是小时聚合数据，不支持细粒度关联分析。"),
    ).not.toBeInTheDocument();
  });

  it("uses the real hourly bucket span when rendering average rate", async () => {
    const user = userEvent.setup();
    renderWithProviders(
      "/usage?range=last_month",
      <UsagePage />,
      createAggregatedClient("usage_1h"),
    );
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    const remoteIp = await screen.findByRole("button", {
      name: "203.0.113.24",
    });
    const row = remoteIp.closest("tr");
    if (!row) {
      throw new Error("expected hourly usage row for 203.0.113.24");
    }
    await user.click(row);

    expect(
      await screen.findByText(
        (_, element) => element?.textContent === "↑ 51.0 B/s · ↓ 344 B/s",
      ),
    ).toBeInTheDocument();
  });

  it("uses the real daily bucket span when rendering average rate", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        const response = await base.getUsage(query, requestOptions);
        return {
          ...response,
          dataSource: "usage_1d",
          rows: response.rows.slice(0, 1).map((row) => ({
            ...row,
            pid: null,
            exe: null,
            remotePort: null,
            attribution: null,
            bytesUp: 86400,
            bytesDown: 172800,
          })),
        };
      },
    };

    renderWithProviders("/usage?range=this_month", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );

    expect(
      await screen.findByText(
        (_, element) => element?.textContent === "↑ 1.00 B/s · ↓ 2.00 B/s",
      ),
    ).toBeInTheDocument();
  });

  it("offers an on-demand deep scan path from the usage explain panel", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const explainCalls: Array<{ dataSource?: string; allowScan?: boolean }> =
      [];
    const client: TrafficApiClient = {
      ...base,
      async getUsageExplain(row, options) {
        explainCalls.push(options ?? {});
        return base.getUsageExplain(row, options);
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );
    expect(
      await screen.findByRole("button", { name: "尝试深度扫描日志" }),
    ).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "尝试深度扫描日志" }));

    expect(explainCalls.some((call) => call.allowScan === true)).toBe(true);
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "尝试深度扫描日志" }),
      ).not.toBeInTheDocument(),
    );
  });

  it("automatically retries a busy deep scan after Retry-After", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    let scanCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getUsageExplain(row, options, requestOptions) {
        if (!options?.allowScan) {
          return base.getUsageExplain(row, options, requestOptions);
        }
        scanCalls += 1;
        if (scanCalls === 1) {
          throw new ApiError(
            429,
            "scan_rate_limited",
            "scan busy",
            undefined,
            0.001,
          );
        }
        return base.getUsageExplain(row, options, requestOptions);
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );
    await user.click(
      await screen.findByRole("button", { name: "尝试深度扫描日志" }),
    );

    await waitFor(() => expect(scanCalls).toBe(2));
    expect(screen.queryByText(/深度扫描失败/)).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "尝试深度扫描日志" }),
    ).not.toBeInTheDocument();
  });

  it("clears minute-only filters when the backend downgrades usage to hourly data", async () => {
    const base = createAggregatedClient("usage_1h");
    let usageCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query) {
        usageCalls += 1;
        if (
          query.range === "last_month" &&
          (query.pid || query.exe || query.attribution)
        ) {
          throw new ApiError(
            400,
            "dimension_unavailable",
            "minute-only dimensions are unavailable",
          );
        }
        return base.getUsage(query);
      },
    };

    renderWithProviders(
      "/usage?range=last_month&pid=1088&exe=ss-server&attribution=exact",
      <UsagePage />,
      client,
    );

    expect(
      await screen.findByText(
        "超过分钟明细保留窗口的数据会切换到小时表，PID / EXE 维度不可用。",
      ),
    ).toBeInTheDocument();
    expect(await screen.findByLabelText("PID")).toBeDisabled();
    expect(await screen.findByLabelText("EXE")).toBeDisabled();
    const attributionLabel = await screen.findByText("全部归因");
    expect(attributionLabel.closest("button")).toBeDisabled();
    expect(usageCalls).toBeGreaterThanOrEqual(2);
  });

  it("shows usage request errors instead of an empty state", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    let usageCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getUsage() {
        usageCalls += 1;
        throw new ApiError(
          500,
          "boom",
          "usage request failed",
          "req-visible-42",
        );
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);

    expect(await screen.findByText("明细加载失败")).toBeInTheDocument();
    expect(screen.getByText(/usage request failed/)).toHaveTextContent(
      "请求 ID：req-visible-42",
    );
    expect(screen.queryByText("暂无明细")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重试" }));
    await waitFor(() => expect(usageCalls).toBeGreaterThanOrEqual(2));
  });

  it("resets usage pagination before issuing filtered queries", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage({ ...query, pageSize: 1 }, requestOptions);
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    expect(usageCalls.every((call) => call.includeTotal === false)).toBe(true);

    await user.click(await screen.findByRole("button", { name: "下一页" }));
    await waitFor(() => {
      expect(usageCalls.some((call) => call.page === 2)).toBe(true);
    });

    usageCalls.length = 0;
    await user.type(screen.getByLabelText("对端 IP"), "198.51.100.44");

    await waitFor(() => {
      expect(usageCalls.length).toBeGreaterThan(0);
    });
    expect(usageCalls.every((call) => call.page === 1)).toBe(true);
  });

  it("requests an exact usage total only after explicit opt-in", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        const response = await base.getUsage(query, requestOptions);
        return query.includeTotal
          ? response
          : {
              ...response,
              hasMore: true,
              totalRowsExact: false,
              totalRows: response.rows.length + 1,
            };
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    const exactTotal = await screen.findByRole("button", {
      name: "计算精确总数",
    });
    expect(usageCalls.some((call) => call.includeTotal === true)).toBe(false);

    await user.click(exactTotal);
    await waitFor(() =>
      expect(usageCalls.some((call) => call.includeTotal === true)).toBe(true),
    );
  });

  it("does not expose placeholder rows after changing the usage scope", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        if (query.range === "7d") {
          return pendingPromise();
        }
        const response = await base.getUsage(query, requestOptions);
        return {
          ...response,
          rows: response.rows
            .slice(0, 1)
            .map((row) => ({ ...row, remoteIp: "198.51.100.200" })),
        };
      },
    };

    renderWithProviders("/usage?range=24h", <UsagePage />, client);
    expect(
      await screen.findByRole("button", { name: "198.51.100.200" }),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "7 天" }));
    expect(await screen.findByText("明细加载中")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "198.51.100.200" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("row", { name: "展开详情" }),
    ).not.toBeInTheDocument();
  });

  it("does not present a previous dashboard ranking as the new range", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options, requestOptions) {
        if (range === "7d") return pendingPromise();
        const response = await base.getTopProcesses(
          range,
          options,
          requestOptions,
        );
        return {
          ...response,
          rows: response.rows
            .slice(0, 1)
            .map((row) => ({ ...row, comm: "old-dashboard-process" })),
        };
      },
    };

    renderWithProviders("/?range=24h", <DashboardPage />, client);
    expect(
      await screen.findByText("old-dashboard-process"),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "7 天" }));
    expect(await screen.findByText("Top 进程加载中")).toBeInTheDocument();
    expect(screen.queryByText("old-dashboard-process")).not.toBeInTheDocument();
  });

  it("does not leave previous process rows actionable during a range change", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options, requestOptions) {
        if (range === "7d") return pendingPromise();
        const response = await base.getTopProcesses(
          range,
          options,
          requestOptions,
        );
        return {
          ...response,
          rows: response.rows
            .slice(0, 1)
            .map((row) => ({ ...row, comm: "old-process-row" })),
        };
      },
    };

    renderWithProviders("/processes?range=24h", <ProcessesPage />, client);
    expect(
      (await screen.findAllByText("old-process-row")).length,
    ).toBeGreaterThan(0);

    await user.click(screen.getByRole("tab", { name: "7 天" }));
    expect(await screen.findByText("按 PID 聚合加载中")).toBeInTheDocument();
    expect(screen.queryByText("old-process-row")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("row", { name: "查看趋势" }),
    ).not.toBeInTheDocument();
  });

  it("does not leave previous remote rows actionable during a range change", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getTopRemotes(range, options, requestOptions) {
        if (range === "7d") return pendingPromise();
        const response = await base.getTopRemotes(
          range,
          options,
          requestOptions,
        );
        return {
          ...response,
          rows: response.rows
            .slice(0, 1)
            .map((row) => ({ ...row, remoteIp: "198.51.100.201" })),
        };
      },
    };

    renderWithProviders("/remotes?range=24h", <RemotesPage />, client);
    expect(await screen.findByText("198.51.100.201")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "7 天" }));
    expect(await screen.findByText("对端聚合加载中")).toBeInTheDocument();
    expect(screen.queryByText("198.51.100.201")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("row", { name: "查看明细" }),
    ).not.toBeInTheDocument();
  });

  it("does not present previous forward rows under new filters", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getForwardUsage(query, requestOptions) {
        if (query.range === "7d") return pendingPromise();
        const response = await base.getForwardUsage(query, requestOptions);
        return {
          ...response,
          rows: response.rows
            .slice(0, 1)
            .map((row) => ({ ...row, origSrc: "198.51.100.202" })),
        };
      },
    };

    renderWithProviders("/forward?range=24h", <ForwardPage />, client);
    expect(await screen.findByText("198.51.100.202")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "7 天" }));
    expect(await screen.findByText("转发流量加载中")).toBeInTheDocument();
    expect(screen.queryByText("198.51.100.202")).not.toBeInTheDocument();
  });

  it("defaults monthly usage windows to total traffic sorting", async () => {
    const base = createAggregatedClient("usage_1h");
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage(query, requestOptions);
      },
    };

    renderWithProviders("/usage?range=last_month", <UsagePage />, client);

    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    await waitFor(() => {
      expect(
        usageCalls.some(
          (call) => call.sortBy === "bytesTotal" && call.sortOrder === "desc",
        ),
      ).toBe(true);
    });
  });

  it("keeps recent usage windows sorted by time", async () => {
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage(query, requestOptions);
      },
    };

    renderWithProviders("/usage?range=7d", <UsagePage />, client);

    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    await waitFor(() => {
      expect(
        usageCalls.some(
          (call) => call.sortBy === "minuteTs" && call.sortOrder === "desc",
        ),
      ).toBe(true);
    });
  });

  it("keeps the current month usage window sorted by time by default", async () => {
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage(query, requestOptions);
      },
    };

    renderWithProviders("/usage?range=this_month", <UsagePage />, client);

    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    await waitFor(() => {
      expect(
        usageCalls.some(
          (call) => call.sortBy === "minuteTs" && call.sortOrder === "desc",
        ),
      ).toBe(true);
    });
  });

  it("sends protocol sorting from the usage table header", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage(query, requestOptions);
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    const protoHeader = await screen.findByRole("columnheader", {
      name: /协议/,
    });
    await user.click(within(protoHeader).getByRole("button", { name: /协议/ }));

    await waitFor(() => {
      expect(usageCalls.some((call) => call.sortBy === "proto")).toBe(true);
    });
  });

  it("mounts the app shell with navigation", async () => {
    renderWithProviders("/", <App />);
    expect(screen.getByRole("link", { name: "Dashboard" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Usage" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Forward" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "History" })).toBeInTheDocument();
    expect(await screen.findByText("流量总览")).toBeInTheDocument();
  });

  it("keeps explicit usage windows in app metadata", async () => {
    renderWithProviders("/usage?start=1767225600&end=1769904000", <App />);

    await waitFor(() => {
      expect(
        document.head.querySelector('link[rel="canonical"]'),
      ).toHaveAttribute(
        "href",
        `${window.location.origin}/usage?start=1767225600&end=1769904000`,
      );
    });
    expect(
      document.head.querySelector('meta[property="og:url"]'),
    ).toHaveAttribute(
      "content",
      `${window.location.origin}/usage?start=1767225600&end=1769904000`,
    );
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
  });

  it("shows monthly history and links retained months to detail", async () => {
    renderWithProviders("/history", <HistoryPage />);

    expect(await screen.findByText("月度归档")).toBeInTheDocument();
    expect(await screen.findByText("历史总流量")).toBeInTheDocument();
    expect(screen.getByText("已归档 · 仅汇总")).toBeInTheDocument();
    expect(
      screen.getAllByRole("link", { name: "查看明细" })[0],
    ).toHaveAttribute("href", "/usage?range=this_month");
    expect(screen.getByText("仅月度汇总")).toBeInTheDocument();
  });

  it("links older retained months with an explicit usage window", async () => {
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getMonthlyUsage() {
        return {
          rows: [
            {
              monthTs: 1767225600,
              bytesUp: 100,
              bytesDown: 200,
              flowCount: 3,
              forwardBytesOrig: 10,
              forwardBytesReply: 20,
              forwardFlowCount: 1,
              evidenceCount: 2,
              chainCount: 1,
              updatedAt: 1769904000,
              archived: true,
              detailAvailable: true,
              detailRange: null,
              totalBytes: 300,
              forwardTotalBytes: 30,
            },
          ],
        };
      },
    };

    renderWithProviders("/history", <HistoryPage />, client);

    expect(await screen.findByText("已归档 · 明细保留")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "查看明细" })).toHaveAttribute(
      "href",
      "/usage?start=1767225600&end=1769904000",
    );
  });

  it("preserves explicit usage windows when filters change", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const usageCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query, requestOptions) {
        usageCalls.push({ ...query });
        return base.getUsage(query, requestOptions);
      },
    };

    renderWithProviders(
      "/usage?start=1767225600&end=1769904000",
      <UsagePage />,
      client,
    );
    expect(await screen.findByText("2026年1月 明细")).toBeInTheDocument();

    await user.type(screen.getByLabelText("对端 IP"), "198.51.100.44");

    await waitFor(() => {
      expect(usageCalls.some((call) => call.remoteIp === "198.51.100.44")).toBe(
        true,
      );
    });
    expect(
      usageCalls
        .filter((call) => call.remoteIp === "198.51.100.44")
        .every(
          (call) =>
            call.start === 1767225600 &&
            call.end === 1769904000 &&
            call.range === undefined,
        ),
    ).toBe(true);
  });

  it("expands a usage row to show detail panel on click", async () => {
    const user = userEvent.setup();
    renderWithProviders("/usage", <UsagePage />);
    // Wait for table to load
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    // Use the first row's explicit operation instead of making the whole row interactive.
    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );
    // Expanded panel should show these labels
    // '对端端口' also appears as a column header, so use getAllByText
    expect(screen.getAllByText("对端端口").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("数据包数")).toBeInTheDocument();
    expect(screen.getByText("连接数")).toBeInTheDocument();
    expect(screen.getByText("归因详情")).toBeInTheDocument();
    expect(screen.getByText("平均速率")).toBeInTheDocument();
  });

  it("shows loopback linkage candidates in usage details", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getUsage() {
        return {
          dataSource: "usage_1m",
          nextCursor: null,
          page: 1,
          pageSize: 25,
          totalRows: 1,
          rows: [
            {
              minuteTs: 1710000000,
              proto: "tcp",
              direction: "out",
              pid: null,
              comm: null,
              exe: null,
              localPort: 52000,
              remoteIp: "127.0.0.1",
              remotePort: 18080,
              attribution: "unknown",
              bytesUp: 1024,
              bytesDown: 4096,
              pktsUp: 10,
              pktsDown: 12,
              flowCount: 2,
            },
          ],
        };
      },
      async getUsageExplain() {
        return {
          process: "unknown",
          confidence: "medium",
          sourceIps: ["112.0.38.89"],
          targetIps: [],
          chains: [],
          relatedPeers: [],
          nginxRequests: [],
          notes: [],
        };
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    expect(await screen.findByText("本机回环")).toBeInTheDocument();

    await user.click(
      (await screen.findAllByRole("row", { name: "展开详情" }))[0],
    );

    expect(await screen.findByText("回环链路候选")).toBeInTheDocument();
    expect(
      screen.getByText("112.0.38.89 → 本机回环:18080"),
    ).toBeInTheDocument();
  });

  it("shows loopback remotes by default and can exclude them", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const summaryFlags: boolean[] = [];
    const client: TrafficApiClient = {
      ...base,
      async getTopRemotes(range, options) {
        summaryFlags.push(!!options?.summary);
        return {
          dataSource: "usage_1m",
          page: 1,
          pageSize: 25,
          totalRows: 1,
          rows: [
            {
              direction: "out",
              remoteIp: options?.includeLoopback ? "127.0.0.1" : "203.0.113.24",
              bytesUp: 1024,
              bytesDown: 4096,
              flowCount: 2,
              totalBytes: 5120,
            },
          ],
        };
      },
    };

    renderWithProviders("/remotes", <RemotesPage />, client);
    expect(await screen.findByText("127.0.0.1")).toBeInTheDocument();
    expect(summaryFlags).toContain(false);

    await user.click(screen.getByRole("button", { name: "排除本机回环" }));
    expect(await screen.findByText("203.0.113.24")).toBeInTheDocument();
  });

  it("renders the processes investigation page", async () => {
    renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("按 PID 聚合")).toBeInTheDocument();
    expect(await screen.findByText("按进程名聚合")).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: "EXE" }),
    ).not.toBeInTheDocument();
  });

  it("requests both process groupings and lets the server select retained granularity", async () => {
    const base = createMockApiClient();
    const processCalls: Array<{ groupBy: ProcessGroupBy; summary: boolean }> =
      [];
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options) {
        processCalls.push({
          groupBy: options?.groupBy ?? "pid",
          summary: !!options?.summary,
        });
        return base.getTopProcesses(range, options);
      },
    };

    renderWithProviders(
      "/processes?range=last_month",
      <ProcessesPage />,
      client,
    );
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect((await screen.findAllByText("ss-server")).length).toBeGreaterThan(0);
    await waitFor(() => {
      expect(processCalls).toEqual(
        expect.arrayContaining([
          { groupBy: "pid", summary: false },
          { groupBy: "comm", summary: false },
        ]),
      );
    });
  });

  it("hides PID process rows when natural month windows use daily aggregates", async () => {
    renderWithProviders(
      "/processes?range=this_month",
      <ProcessesPage />,
      createAggregatedClient("usage_1d"),
    );
    expect(
      await screen.findByText("当前窗口不提供 PID 维度"),
    ).toBeInTheDocument();
    expect(screen.getByText("usage_1d")).toBeInTheDocument();
  });

  it("selects the first process row by default so the chart matches the highlighted row", async () => {
    const { container } = renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("流量趋势 · ss-server")).toBeInTheDocument();
    expect(container.querySelectorAll("tr.selected")).toHaveLength(1);
    expect(container.querySelector("tr.selected")).toHaveTextContent(
      "ss-server",
    );
  });

  it("keeps comm-table selection inside the comm table when the selected row disappears after pagination", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(_range, options) {
        if (options?.groupBy === "comm") {
          const rows =
            options.page === 2
              ? [
                  {
                    pid: null,
                    comm: "comm-b",
                    exe: null,
                    bytesUp: 30,
                    bytesDown: 70,
                    flowCount: 2,
                    totalBytes: 100,
                  },
                ]
              : [
                  {
                    pid: null,
                    comm: "comm-a",
                    exe: null,
                    bytesUp: 50,
                    bytesDown: 150,
                    flowCount: 3,
                    totalBytes: 200,
                  },
                ];
          return {
            dataSource: "usage_1m",
            page: options?.page ?? 1,
            pageSize: 1,
            totalRows: 2,
            rows,
          };
        }

        return {
          dataSource: "usage_1m",
          page: 1,
          pageSize: 1,
          totalRows: 1,
          rows: [
            {
              pid: 11,
              comm: "pid-one",
              exe: "/usr/bin/pid-one",
              bytesUp: 120,
              bytesDown: 240,
              flowCount: 4,
              totalBytes: 360,
            },
          ],
        };
      },
      async getTimeSeries(_range, groupBy = "direction") {
        return {
          dataSource: "usage_1m",
          bucket: "5m",
          groupBy,
          points: [
            {
              ts: 1710000000,
              up: 40,
              down: 60,
              flowCount: 3,
              label: "03/09 16:00",
            },
          ],
          groups: [
            {
              key: "in",
              points: [
                {
                  ts: 1710000000,
                  up: 10,
                  down: 20,
                  flowCount: 1,
                  label: "03/09 16:00",
                },
              ],
            },
            {
              key: "out",
              points: [
                {
                  ts: 1710000000,
                  up: 30,
                  down: 40,
                  flowCount: 2,
                  label: "03/09 16:00",
                },
              ],
            },
          ],
        };
      },
    };

    const { container } = renderWithProviders(
      "/processes",
      <ProcessesPage />,
      client,
    );
    expect(await screen.findByText("流量趋势 · pid-one")).toBeInTheDocument();

    const commSection = screen.getByText("按进程名聚合").closest("section");
    if (!commSection) {
      throw new Error("expected comm section");
    }

    const commARow = within(commSection).getByText("comm-a").closest("tr");
    if (!commARow) {
      throw new Error("expected comm-a row");
    }
    await user.click(commARow);
    expect(await screen.findByText("流量趋势 · comm-a")).toBeInTheDocument();

    await user.click(
      within(commSection).getByRole("button", { name: "下一页" }),
    );

    expect(await screen.findByText("流量趋势 · comm-b")).toBeInTheDocument();
    expect(container.querySelectorAll("tr.selected")).toHaveLength(1);
    expect(container.querySelector("tr.selected")).toHaveTextContent("comm-b");
  });

  it("shows a loading state instead of an empty state on the processes page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getTopProcesses: () =>
        pendingPromise<
          Awaited<ReturnType<TrafficApiClient["getTopProcesses"]>>
        >(),
    };

    renderWithProviders("/processes", <ProcessesPage />, pendingClient);
    expect(screen.getByText("进程聚合加载中")).toBeInTheDocument();
  });

  it("shows a loading state instead of an empty state on the remotes page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getTopRemotes: () =>
        pendingPromise<
          Awaited<ReturnType<TrafficApiClient["getTopRemotes"]>>
        >(),
    };

    renderWithProviders("/remotes", <RemotesPage />, pendingClient);
    expect(screen.getByText("对端聚合加载中")).toBeInTheDocument();
  });

  it("shows a loading state instead of an empty state on the forward page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getForwardUsage: () =>
        pendingPromise<
          Awaited<ReturnType<TrafficApiClient["getForwardUsage"]>>
        >(),
    };

    renderWithProviders("/forward", <ForwardPage />, pendingClient);
    expect(screen.getByText("转发流量加载中")).toBeInTheDocument();
  });

  it("renders the forward investigation page", async () => {
    renderWithProviders("/forward", <ForwardPage />);
    expect(await screen.findByText("转发流量")).toBeInTheDocument();
  });

  it("sorts the forward total column through the API", async () => {
    const base = createMockApiClient();
    const forwardCalls: Array<Record<string, unknown>> = [];
    const client: TrafficApiClient = {
      ...base,
      async getForwardUsage(query, requestOptions) {
        forwardCalls.push({ ...query });
        return base.getForwardUsage(query, requestOptions);
      },
    };
    const user = userEvent.setup();

    renderWithProviders("/forward", <ForwardPage />, client);

    const totalSort = await screen.findByRole("button", { name: /总量/ });
    await user.click(totalSort);
    await waitFor(() => {
      expect(forwardCalls.some((call) => call.sortBy === "bytesTotal")).toBe(
        true,
      );
    });
  });
});
