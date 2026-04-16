import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { App } from '../App';
import { ApiProvider } from '../api-context';
import { createMockApiClient } from '../data/mock';
import { UsagePage } from '../pages/UsagePage';
import { DashboardPage } from '../pages/DashboardPage';

function renderWithProviders(path: string, element: ReactNode) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <ApiProvider client={createMockApiClient()}>
        <MemoryRouter initialEntries={[path]}>{element}</MemoryRouter>
      </ApiProvider>
    </QueryClientProvider>,
  );
}

describe('traffic-go web ui', () => {
  it('renders dashboard overview', async () => {
    renderWithProviders('/', <DashboardPage />);
    expect(await screen.findByText('流量总览')).toBeInTheDocument();
    expect(await screen.findByText('总上行')).toBeInTheDocument();
    expect(await screen.findByText('Top 进程')).toBeInTheDocument();
  });

  it('disables pid and exe when range is longer than 30 days', async () => {
    renderWithProviders('/usage?range=90d&pid=1088&exe=ss-server', <UsagePage />);
    const pid = await screen.findByLabelText('PID');
    const exe = await screen.findByLabelText('EXE');
    expect(pid).toBeDisabled();
    expect(exe).toBeDisabled();
    expect(screen.getByText('超过分钟明细保留窗口的数据会切换到小时表，PID / EXE 维度不可用。')).toBeInTheDocument();
  });

  it('hides minute-only columns when usage data comes from the hourly source', async () => {
    renderWithProviders('/usage?range=90d', <UsagePage />);
    expect(await screen.findByText('流量明细')).toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'PID' })).not.toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: 'EXE' })).not.toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: '归因' })).not.toBeInTheDocument();
  });

  it('mounts the app shell with navigation', () => {
    renderWithProviders('/', <App />);
    expect(screen.getByRole('link', { name: 'Dashboard' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Usage' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Forward' })).toBeInTheDocument();
  });

  it('expands a usage row to show detail panel on click', async () => {
    const user = userEvent.setup();
    renderWithProviders('/usage', <UsagePage />);
    // Wait for table to load
    expect(await screen.findByText('流量明细')).toBeInTheDocument();
    // Find the first data row (not header) and click it
    const rows = await screen.findAllByRole('row');
    // rows[0] = thead row, rows[1] = first data row
    await user.click(rows[1]);
    // Expanded panel should show these labels
    // '对端端口' also appears as a column header, so use getAllByText
    expect(screen.getAllByText('对端端口').length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText('数据包数')).toBeInTheDocument();
    expect(screen.getByText('连接数')).toBeInTheDocument();
    expect(screen.getByText('归因详情')).toBeInTheDocument();
    expect(screen.getByText('平均速率')).toBeInTheDocument();
  });
});
