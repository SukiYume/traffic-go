import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { DataSourceBadge } from '../components/DataSourceBadge';

describe('DataSourceBadge', () => {
  it('uses the shared data source label', () => {
    render(<DataSourceBadge dataSource="usage_1h_forward" />);

    expect(screen.getByText('小时转发聚合')).toBeInTheDocument();
    expect(screen.getByText('usage_1h_forward')).toBeInTheDocument();
  });

  it('labels interface daily aggregate data as network interface data', () => {
    render(<DataSourceBadge dataSource="interface_1d" />);

    expect(screen.getByText('网卡日聚合')).toBeInTheDocument();
    expect(screen.getByText('interface_1d')).toBeInTheDocument();
  });

  it('shows the effective bucket warning only for approximate summary coverage', () => {
    const { rerender } = render(
      <DataSourceBadge
        dataSource="usage_1d"
        coverage={{ approximate: true, effectiveStart: 1710000000, effectiveEnd: 1710086400 }}
      />,
    );

    expect(screen.getByText('近似窗口')).toBeInTheDocument();
    expect(screen.getByText('边界按完整聚合桶扩展')).toBeInTheDocument();

    rerender(
      <DataSourceBadge
        dataSource="usage_1d"
        coverage={{ approximate: false, effectiveStart: 1710000000, effectiveEnd: 1710086400 }}
      />,
    );
    expect(screen.queryByText('近似窗口')).not.toBeInTheDocument();
  });
});
