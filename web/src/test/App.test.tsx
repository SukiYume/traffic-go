import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { App } from "../App";
import { ApiError } from "../api";
import { ApiProvider } from "../api-context";
import { createMockApiClient } from "../data/mock";
import { ForwardPage } from "../pages/ForwardPage";
import { ProcessesPage } from "../pages/ProcessesPage";
import { RemotesPage } from "../pages/RemotesPage";
import { UsagePage } from "../pages/UsagePage";
import { DashboardPage } from "../pages/DashboardPage";
import { HistoryPage } from "../pages/HistoryPage";
import type { ProcessGroupBy, TrafficApiClient } from "../types";

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
        <MemoryRouter initialEntries={[path]}>{element}</MemoryRouter>
      </ApiProvider>
    </QueryClientProvider>,
  );
}

function pendingPromise<T>() {
  return new Promise<T>(() => {});
}

function expectStatValue(label: string, value: string) {
  const labelElement = screen.getByText(label);
  const card = labelElement.closest("section");
  if (!card) {
    throw new Error(`expected stat card for ${label}`);
  }
  expect(within(card).getByText(value)).toBeInTheDocument();
}

function createHourlyClient(): TrafficApiClient {
  const base = createMockApiClient();
  return {
    ...base,
    async getUsage(query, requestOptions) {
      const response = await base.getUsage(query, requestOptions);
      return {
        ...response,
        dataSource: "usage_1h",
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
      const response = await base.getTopProcesses(range, options, requestOptions);
      return {
        ...response,
        dataSource: "usage_1h",
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
    expect(screen.getByRole("tab", { name: "网卡口径" })).toHaveAttribute("aria-selected", "true");
    expect(await screen.findByText("总上行")).toBeInTheDocument();
    expect(await screen.findByText("Top 进程")).toBeInTheDocument();
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

    await user.click(screen.getByRole("tab", { name: "连接方向" }));

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
            { ts: 1710000000, up: 8192, down: 16384, flowCount: 0, label: "12:00" },
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
    const topProcessCalls: ProcessGroupBy[] = [];
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options, requestOptions) {
        topProcessCalls.push(options?.groupBy ?? 'pid');
        return base.getTopProcesses(range, options, requestOptions);
      },
    };

    renderWithProviders("/", <DashboardPage />, client);

    expect(await screen.findByText("PID 1088")).toBeInTheDocument();
    expect(await screen.findAllByText("ss-server")).not.toHaveLength(0);
    expect(screen.queryByText("未归因 / EXE 不可用")).not.toBeInTheDocument();
    expect(topProcessCalls).toContain("pid");
  });

  it("keeps dashboard top processes in comm fallback when the window is hourly", async () => {
    renderWithProviders("/?range=last_month", <DashboardPage />, createHourlyClient());

    expect((await screen.findAllByText("当前窗口已降级为按进程名聚合")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("小时聚合 / EXE 在此视图不展示").length).toBeGreaterThan(0);
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
    const base = createHourlyClient();
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
    renderWithProviders("/usage?range=last_month", <UsagePage />, createHourlyClient());
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
    renderWithProviders("/usage?range=last_month", <UsagePage />, createHourlyClient());
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    const rows = await screen.findAllByRole("row");
    await user.click(rows[1]);

    expect(await screen.findByText("访问链路候选")).toBeInTheDocument();
    expect(
      screen.queryByText("当前是小时聚合数据，不支持细粒度关联分析。"),
    ).not.toBeInTheDocument();
  });

  it("uses the real hourly bucket span when rendering average rate", async () => {
    const user = userEvent.setup();
    renderWithProviders("/usage?range=last_month", <UsagePage />, createHourlyClient());
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    const remoteIp = await screen.findByRole("button", { name: "203.0.113.24" });
    const row = remoteIp.closest("tr");
    if (!row) {
      throw new Error("expected hourly usage row for 203.0.113.24");
    }
    await user.click(row);

    expect(
      await screen.findByText((_, element) =>
        element?.textContent === "↑ 51.0 B/s · ↓ 344 B/s",
      ),
    ).toBeInTheDocument();
  });

  it("offers an on-demand deep scan path from the usage explain panel", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const explainCalls: Array<{ dataSource?: string; allowScan?: boolean }> = [];
    const client: TrafficApiClient = {
      ...base,
      async getUsageExplain(row, options) {
        explainCalls.push(options ?? {});
        return base.getUsageExplain(row, options);
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);
    expect(await screen.findByText("流量明细")).toBeInTheDocument();

    const rows = await screen.findAllByRole("row");
    await user.click(rows[1]);
    expect(await screen.findByRole("button", { name: "尝试深度扫描日志" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "尝试深度扫描日志" }));

    expect(explainCalls.some((call) => call.allowScan === true)).toBe(true);
  });

  it("clears minute-only filters when the backend downgrades usage to hourly data", async () => {
    const base = createHourlyClient();
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
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getUsage() {
        throw new ApiError(500, "boom", "usage request failed");
      },
    };

    renderWithProviders("/usage", <UsagePage />, client);

    expect(await screen.findByText("明细加载失败")).toBeInTheDocument();
    expect(screen.getByText("usage request failed")).toBeInTheDocument();
    expect(screen.queryByText("暂无明细")).not.toBeInTheDocument();
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

  it("mounts the app shell with navigation", () => {
    renderWithProviders("/", <App />);
    expect(screen.getByRole("link", { name: "Dashboard" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Usage" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Forward" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "History" })).toBeInTheDocument();
  });

  it("shows monthly history and links retained months to detail", async () => {
    renderWithProviders("/history", <HistoryPage />);

    expect(await screen.findByText("月度归档")).toBeInTheDocument();
    expect(await screen.findByText("历史总流量")).toBeInTheDocument();
    expect(screen.getByText("已归档")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "查看明细" })[0]).toHaveAttribute(
      "href",
      "/usage?range=this_month",
    );
    expect(screen.getByText("仅月度汇总")).toBeInTheDocument();
  });

  it("expands a usage row to show detail panel on click", async () => {
    const user = userEvent.setup();
    renderWithProviders("/usage", <UsagePage />);
    // Wait for table to load
    expect(await screen.findByText("流量明细")).toBeInTheDocument();
    // Find the first data row (not header) and click it
    const rows = await screen.findAllByRole("row");
    // rows[0] = thead row, rows[1] = first data row
    await user.click(rows[1]);
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

    const rows = await screen.findAllByRole("row");
    await user.click(rows[1]);

    expect(await screen.findByText("回环链路候选")).toBeInTheDocument();
    expect(screen.getByText("112.0.38.89 → 本机回环:18080")).toBeInTheDocument();
  });

  it("shows loopback remotes by default and can exclude them", async () => {
    const user = userEvent.setup();
    const base = createMockApiClient();
    const client: TrafficApiClient = {
      ...base,
      async getTopRemotes(range, options) {
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

    await user.click(screen.getByRole("button", { name: "排除本机回环" }));
    expect(await screen.findByText("203.0.113.24")).toBeInTheDocument();
  });

  it("renders the processes investigation page", async () => {
    renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("按 PID 聚合")).toBeInTheDocument();
    expect(await screen.findByText("按进程名聚合")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "EXE" })).not.toBeInTheDocument();
  });

  it("requests both PID and process-name summaries for natural month process windows", async () => {
    const base = createMockApiClient();
    const processCalls: ProcessGroupBy[] = [];
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options) {
        processCalls.push(options?.groupBy ?? 'pid');
        return base.getTopProcesses(range, options);
      },
    };

    renderWithProviders("/processes?range=last_month", <ProcessesPage />, client);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect((await screen.findAllByText("ss-server")).length).toBeGreaterThan(0);
    await waitFor(() => {
      expect(processCalls).toEqual(expect.arrayContaining(["pid", "comm"]));
    });
  });

  it("selects the first process row by default so the chart matches the highlighted row", async () => {
    const { container } = renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("流量趋势 · ss-server")).toBeInTheDocument();
    expect(container.querySelectorAll("tr.selected")).toHaveLength(1);
    expect(container.querySelector("tr.selected")).toHaveTextContent("ss-server");
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
              ? [{ pid: null, comm: "comm-b", exe: null, bytesUp: 30, bytesDown: 70, flowCount: 2, totalBytes: 100 }]
              : [{ pid: null, comm: "comm-a", exe: null, bytesUp: 50, bytesDown: 150, flowCount: 3, totalBytes: 200 }];
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
          rows: [{ pid: 11, comm: "pid-one", exe: "/usr/bin/pid-one", bytesUp: 120, bytesDown: 240, flowCount: 4, totalBytes: 360 }],
        };
      },
      async getTimeSeries(_range, groupBy = "direction") {
        return {
          dataSource: "usage_1m",
          bucket: "5m",
          groupBy,
          points: [{ ts: 1710000000, up: 40, down: 60, flowCount: 3, label: "03/09 16:00" }],
          groups: [
            { key: "in", points: [{ ts: 1710000000, up: 10, down: 20, flowCount: 1, label: "03/09 16:00" }] },
            { key: "out", points: [{ ts: 1710000000, up: 30, down: 40, flowCount: 2, label: "03/09 16:00" }] },
          ],
        };
      },
    };

    const { container } = renderWithProviders("/processes", <ProcessesPage />, client);
    expect(await screen.findByText("流量趋势 · pid-one")).toBeInTheDocument();

    const commSection = screen.getByText("按进程名聚合").closest("section");
    if (!commSection) {
      throw new Error("expected comm section");
    }

    await user.click(within(commSection).getByText("comm-a"));
    expect(await screen.findByText("流量趋势 · comm-a")).toBeInTheDocument();

    await user.click(within(commSection).getByRole("button", { name: "下一页" }));

    expect(await screen.findByText("流量趋势 · comm-b")).toBeInTheDocument();
    expect(container.querySelectorAll("tr.selected")).toHaveLength(1);
    expect(container.querySelector("tr.selected")).toHaveTextContent("comm-b");
  });

  it("shows a loading state instead of an empty state on the processes page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getTopProcesses: () => pendingPromise<Awaited<ReturnType<TrafficApiClient["getTopProcesses"]>>>(),
    };

    renderWithProviders("/processes", <ProcessesPage />, pendingClient);
    expect(screen.getByText("进程聚合加载中")).toBeInTheDocument();
  });

  it("shows a loading state instead of an empty state on the remotes page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getTopRemotes: () => pendingPromise<Awaited<ReturnType<TrafficApiClient["getTopRemotes"]>>>(),
    };

    renderWithProviders("/remotes", <RemotesPage />, pendingClient);
    expect(screen.getByText("对端聚合加载中")).toBeInTheDocument();
  });

  it("shows a loading state instead of an empty state on the forward page first load", () => {
    const base = createMockApiClient();
    const pendingClient: TrafficApiClient = {
      ...base,
      getForwardUsage: () => pendingPromise<Awaited<ReturnType<TrafficApiClient["getForwardUsage"]>>>(),
    };

    renderWithProviders("/forward", <ForwardPage />, pendingClient);
    expect(screen.getByText("转发流量加载中")).toBeInTheDocument();
  });

  it("renders the forward investigation page", async () => {
    renderWithProviders("/forward", <ForwardPage />);
    expect(await screen.findByText("转发流量")).toBeInTheDocument();
  });
});
