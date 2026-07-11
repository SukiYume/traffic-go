import { useState } from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { RangeSelect } from '../components/RangeSelect';
import { RefreshControl } from '../components/RefreshControl';
import { LIVE_REFETCH_INTERVAL_MS, liveQueryOptions } from '../query-refresh';
import type { RangeKey } from '../types';

function ControlledRangeSelect() {
  const [value, setValue] = useState<RangeKey>('24h');
  return <RangeSelect value={value} onChange={setValue} />;
}

describe('live refresh controls', () => {
  it('uses a visible-page 45 second refresh interval and focus refresh', () => {
    expect(LIVE_REFETCH_INTERVAL_MS).toBe(45_000);
    expect(liveQueryOptions).toMatchObject({
      refetchInterval: 45_000,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: true,
    });
  });

  it('shows last update state and supports manual refresh', async () => {
    const user = userEvent.setup();
    const onRefresh = vi.fn();
    render(<RefreshControl updatedAt={0} isFetching={false} onRefresh={onRefresh} />);

    expect(screen.getByText('尚未更新')).toBeInTheDocument();
    expect(screen.getByText('每 45 秒自动刷新')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: '立即刷新' }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
});

describe('RangeSelect keyboard navigation', () => {
  it('moves selection and focus with arrow, Home, and End keys', async () => {
    const user = userEvent.setup();
    render(<ControlledRangeSelect />);

    const current = screen.getByRole('tab', { name: '24 小时' });
    current.focus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: '7 天' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tab', { name: '7 天' })).toHaveFocus();

    await user.keyboard('{End}');
    expect(screen.getByRole('tab', { name: '上上月' })).toHaveFocus();
    await user.keyboard('{Home}');
    expect(screen.getByRole('tab', { name: '1 小时' })).toHaveFocus();
  });
});
