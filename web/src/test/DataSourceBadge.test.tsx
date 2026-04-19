import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { DataSourceBadge } from '../components/DataSourceBadge';

describe('DataSourceBadge', () => {
  it('treats hourly forward sources as hourly aggregates', () => {
    render(<DataSourceBadge dataSource="usage_1h_forward" />);

    expect(screen.getByText('小时聚合')).toBeInTheDocument();
    expect(screen.getByText('usage_1h_forward')).toBeInTheDocument();
  });
});

