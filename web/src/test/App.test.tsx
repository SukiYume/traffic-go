import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
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

describe("traffic-go web ui", () => {
  it("renders dashboard overview", async () => {
    renderWithProviders("/", <DashboardPage />);
    expect(await screen.findByText("流量总览")).toBeInTheDocument();
    expect(await screen.findByText("总上行")).toBeInTheDocument();
    expect(await screen.findByText("Top 进程")).toBeInTheDocument();
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
    renderWithProviders("/?range=90d", <DashboardPage />);

    expect((await screen.findAllByText("当前窗口已降级为按进程名聚合")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("小时聚合 / EXE 在此视图不展示").length).toBeGreaterThan(0);
  });

  it("keeps dashboard drill-down links on the current range", async () => {
    renderWithProviders("/?range=90d", <DashboardPage />);
    expect(await screen.findByText("流量总览")).toBeInTheDocument();

    const hrefs = screen
      .getAllByRole("link", { name: "查看全部" })
      .map((link) => link.getAttribute("href"));

    expect(hrefs).toEqual(
      expect.arrayContaining([
        "/processes?range=90d",
        "/remotes?range=90d&direction=in",
        "/remotes?range=90d&direction=out",
        "/usage?range=90d",
      ]),
    );
  });

  it("disables pid and exe when range is longer than 30 days", async () => {
    renderWithProviders(
      "/usage?range=90d&pid=1088&exe=ss-server",
      <UsagePage />,
    );
    const pid = await screen.findByLabelText("PID");
    const exe = await screen.findByLabelText("EXE");
    const attributionLabel = await screen.findByText("全部归因");
    const attribution = attributionLabel.closest("button");
    expect(pid).toBeDisabled();
    expect(exe).toBeDisabled();
    expect(attribution).not.toBeNull();
    expect(attribution).toBeDisabled();
    expect(
      screen.getByText(
        "超过分钟明细保留窗口的数据会切换到小时表，PID / EXE 维度不可用。",
      ),
    ).toBeInTheDocument();
  });

  it("hides minute-only columns when usage data comes from the hourly source", async () => {
    renderWithProviders("/usage?range=90d", <UsagePage />);
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
    renderWithProviders("/usage?range=90d", <UsagePage />);
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
    renderWithProviders("/usage?range=90d", <UsagePage />);
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
    const base = createMockApiClient();
    let usageCalls = 0;
    const client: TrafficApiClient = {
      ...base,
      async getUsage(query) {
        usageCalls += 1;
        if (
          query.range === "30d" &&
          (query.pid || query.exe || query.attribution)
        ) {
          throw new ApiError(
            400,
            "dimension_unavailable",
            "minute-only dimensions are unavailable",
          );
        }
        const hourly = await base.getUsage({ ...query, range: "90d" });
        return { ...hourly, dataSource: "usage_1h" };
      },
    };

    renderWithProviders(
      "/usage?range=30d&pid=1088&exe=ss-server&attribution=exact",
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

  it("mounts the app shell with navigation", () => {
    renderWithProviders("/", <App />);
    expect(screen.getByRole("link", { name: "Dashboard" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Usage" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Forward" })).toBeInTheDocument();
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

  it("shows loopback remotes when the toggle is enabled", async () => {
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
    expect(await screen.findByText("203.0.113.24")).toBeInTheDocument();
    expect(screen.queryByText("127.0.0.1")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "显示本机回环" }));
    expect(await screen.findByText("127.0.0.1")).toBeInTheDocument();
  });

  it("renders the processes investigation page", async () => {
    renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("按 PID 聚合")).toBeInTheDocument();
    expect(await screen.findByText("按进程名聚合")).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "EXE" })).not.toBeInTheDocument();
  });

  it("does not issue a redundant pid-group request on the initial 90d processes load", async () => {
    const base = createMockApiClient();
    const processCalls: ProcessGroupBy[] = [];
    const client: TrafficApiClient = {
      ...base,
      async getTopProcesses(range, options) {
        processCalls.push(options?.groupBy ?? 'pid');
        return base.getTopProcesses(range, options);
      },
    };

    renderWithProviders("/processes?range=90d", <ProcessesPage />, client);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("ss-server")).toBeInTheDocument();
    expect(screen.getByText("当前窗口不提供 PID 维度")).toBeInTheDocument();
    await waitFor(() => {
      expect(processCalls).toEqual(["comm"]);
    });
  });

  it("selects the first process row by default so the chart matches the highlighted row", async () => {
    const { container } = renderWithProviders("/processes", <ProcessesPage />);
    expect(await screen.findByText("进程聚合")).toBeInTheDocument();
    expect(await screen.findByText("流量趋势 · ss-server")).toBeInTheDocument();
    expect(container.querySelectorAll("tr.selected")).toHaveLength(1);
    expect(container.querySelector("tr.selected")).toHaveTextContent("ss-server");
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
