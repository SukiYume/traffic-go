import { useEffect, useMemo, useState } from 'react';
import {
  Brush,
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
import { chartTickLabel, formatBytes, formatLongDateTime, formatNumber } from '../utils';

function clampSelection(max: number, start: number, end: number) {
  if (max <= 0) {
    return { start: 0, end: 0 };
  }
  return {
    start: Math.max(0, Math.min(start, max - 1)),
    end: Math.max(0, Math.min(end, max - 1)),
  };
}

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
  const [selection, setSelection] = useState(() =>
    clampSelection(points.length, Math.max(0, points.length - Math.min(points.length, 24)), points.length - 1),
  );

  useEffect(() => {
    setSelection(clampSelection(points.length, Math.max(0, points.length - Math.min(points.length, 24)), points.length - 1));
  }, [points]);

  const selectedPoints = useMemo(
    () => points.slice(selection.start, selection.end + 1),
    [points, selection.end, selection.start],
  );
  const summary = useMemo(() => {
    const rows = selectedPoints.length ? selectedPoints : points;
    return rows.reduce(
      (acc, point) => {
        acc.up += point.up;
        acc.down += point.down;
        acc.flowCount += point.flowCount;
        return acc;
      },
      { up: 0, down: 0, flowCount: 0 },
    );
  }, [points, selectedPoints]);

  const selectionLabel =
    selectedPoints.length > 1
      ? `${formatLongDateTime(selectedPoints[0].ts)} - ${formatLongDateTime(selectedPoints[selectedPoints.length - 1].ts)}`
      : selectedPoints[0]
        ? formatLongDateTime(selectedPoints[0].ts)
        : '暂无数据';

  return (
    <section className="panel chart-panel">
      <div className="panel-head">
        <div>
          <h2>{title}</h2>
          <span>{subtitle}</span>
        </div>
        <span className="panel-muted">拖动底部选区可聚焦任意时间段</span>
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
            {points.length > 8 ? (
              <Brush
                dataKey="ts"
                height={28}
                stroke="#7dd3fc"
                startIndex={selection.start}
                endIndex={selection.end}
                tickFormatter={(value) => chartTickLabel(Number(value), range)}
                onChange={(next) => {
                  if (typeof next?.startIndex !== 'number' || typeof next?.endIndex !== 'number') return;
                  setSelection(clampSelection(points.length, next.startIndex, next.endIndex));
                }}
              />
            ) : null}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="chart-summary">
        <div className="status-pill">
          <strong>当前选区</strong>
          <span>{selectionLabel}</span>
        </div>
        <div className="status-pill">
          <strong>选区上行</strong>
          <span>{formatBytes(summary.up)}</span>
        </div>
        <div className="status-pill">
          <strong>选区下行</strong>
          <span>{formatBytes(summary.down)}</span>
        </div>
        <div className="status-pill">
          <strong>选区流数</strong>
          <span>{formatNumber(summary.flowCount)}</span>
        </div>
      </div>
    </section>
  );
}
