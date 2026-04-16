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
import type { RangeKey, TimeSeriesPoint } from '../types';
import { chartTickLabel, formatBytes, formatLongDateTime } from '../utils';

export function ChartPanel({
  points,
  range,
  title = '流量趋势',
  subtitle = '上行 / 下行',
}: {
  points: TimeSeriesPoint[];
  range: RangeKey;
  title?: string;
  subtitle?: string;
}) {
  return (
    <section className="panel chart-panel">
      <div className="panel-head">
        <div>
          <h2>{title}</h2>
          <span>{subtitle}</span>
        </div>
      </div>
      <div className="chart-frame">
        <ResponsiveContainer width="100%" height={360}>
          <LineChart data={points}>
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
              formatter={(value: number) => formatBytes(value)}
            />
            <Legend />
            <Line type="monotone" dataKey="up" stroke="#6ee7b7" strokeWidth={2.5} dot={false} name="上行" />
            <Line type="monotone" dataKey="down" stroke="#60a5fa" strokeWidth={2.5} dot={false} name="下行" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
