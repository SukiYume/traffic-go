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
});
