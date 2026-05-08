import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ChartPanel } from '../components/ChartPanel';

describe('ChartPanel', () => {
  it('marks the panel busy and shows a refreshing badge', () => {
    const { container } = render(
      <ChartPanel points={[]} range="7d" busy title="趋势" subtitle="子标题" />,
    );
    expect(screen.getByText('刷新中')).toBeInTheDocument();
    const panel = container.querySelector('.chart-panel');
    expect(panel?.getAttribute('aria-busy')).toBe('true');
  });

  it('omits the refreshing badge and aria-busy when not busy', () => {
    const { container } = render(
      <ChartPanel points={[]} range="7d" title="趋势" subtitle="子标题" />,
    );
    expect(screen.queryByText('刷新中')).not.toBeInTheDocument();
    const panel = container.querySelector('.chart-panel');
    expect(panel?.getAttribute('aria-busy')).toBeNull();
  });

  it('renders no line series when points are empty', () => {
    const { container } = render(
      <ChartPanel points={[]} range="7d" title="趋势" subtitle="子标题" />,
    );
    // recharts emits one <path class="recharts-curve recharts-line-curve"> per Line that has data.
    const lineCurves = container.querySelectorAll('.recharts-line-curve');
    for (const curve of lineCurves) {
      const d = curve.getAttribute('d') ?? '';
      // empty data still renders the <path> element but with no `d` segment.
      expect(d).toBe('');
    }
  });
});
