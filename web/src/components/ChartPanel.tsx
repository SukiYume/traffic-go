import { type ReactNode, useMemo } from 'react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { GroupBy, RangeKey, TimeSeriesGroup, TimeSeriesPoint } from '../types';
import { chartTickLabel, formatBytes, formatLongDateTime } from '../utils';

const directionPalette = {
  in: '#f59e0b',
  out: '#38bdf8',
} as const;

function directionGroupLabel(key: string) {
  if (key === 'in') {
    return '入站';
  }
  if (key === 'out') {
    return '出站';
  }
  return key;
}

export function ChartPanel({
  points,
  groups = [],
  groupBy,
  range,
  title = '流量趋势',
  subtitle = '上行 / 下行',
  upLabel = '上行',
  downLabel = '下行',
  actions,
}: {
  points: TimeSeriesPoint[];
  groups?: TimeSeriesGroup[];
  groupBy?: GroupBy;
  range: RangeKey;
  title?: string;
  subtitle?: string;
  upLabel?: string;
  downLabel?: string;
  actions?: ReactNode;
}) {
  const showDirectionGroups = groupBy === 'direction' && groups.length > 0;
  const groupedData = useMemo(() => {
    if (!showDirectionGroups) {
      return [];
    }
    const merged = new Map<number, Record<string, number | string>>();
    for (const group of groups) {
      for (const point of group.points) {
        const current = merged.get(point.ts) ?? { ts: point.ts, label: point.label };
        current[group.key] = point.up + point.down;
        merged.set(point.ts, current);
      }
    }
    return [...merged.values()].sort((left, right) => Number(left.ts) - Number(right.ts));
  }, [groups, showDirectionGroups]);

  return (
    <section className="panel chart-panel">
      <div className="panel-head">
        <div>
          <h2>{title}</h2>
          <span>{subtitle}</span>
        </div>
        {actions}
      </div>
      <div className="chart-frame">
        <ResponsiveContainer width="100%" height={360}>
          <LineChart data={showDirectionGroups ? groupedData : points}>
            <CartesianGrid stroke="rgba(128,145,172,0.18)" vertical={false} />
            <XAxis
              dataKey="ts"
              type="number"
              scale="time"
              domain={['dataMin', 'dataMax']}
              tickFormatter={(value) => chartTickLabel(Number(value), range)}
              tick={{ fill: '#91a0b8', fontSize: 12 }}
              minTickGap={range === '1h' ? 28 : range === '24h' ? 36 : 48}
            />
            <YAxis tickFormatter={(value) => formatBytes(Number(value))} tick={{ fill: '#91a0b8', fontSize: 12 }} width={78} />
            <Tooltip
              labelFormatter={(value) => formatLongDateTime(Number(value))}
              contentStyle={{
                background: '#0d1422',
                border: '1px solid rgba(134, 154, 181, 0.24)',
                borderRadius: 14,
                color: '#e9edf5',
              }}
              formatter={(value: number, name: string) => [formatBytes(value), name]}
            />
            <Legend />
            {showDirectionGroups ? (
              groups.map((group) => (
                <Line
                  key={group.key}
                  type="monotone"
                  dataKey={group.key}
                  stroke={directionPalette[group.key as keyof typeof directionPalette] ?? '#6ee7b7'}
                  strokeWidth={2.5}
                  dot={false}
                  name={directionGroupLabel(group.key)}
                />
              ))
            ) : (
              <>
                <Line type="monotone" dataKey="down" stroke="#60a5fa" strokeWidth={2.5} dot={false} name={downLabel} />
                <Line type="monotone" dataKey="up" stroke="#6ee7b7" strokeWidth={2.5} dot={false} name={upLabel} />
              </>
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
