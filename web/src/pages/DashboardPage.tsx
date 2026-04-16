import { useQuery } from '@tanstack/react-query';
import { Link, useSearchParams } from 'react-router-dom';
import { ChartPanel } from '../components/ChartPanel';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { EmptyState } from '../components/EmptyState';
import { RangeSelect } from '../components/RangeSelect';
import { StatCard } from '../components/StatCard';
import { useApiClient } from '../api-context';
import { formatBytes, peerRoleLabel, rangeLabel, safeText } from '../utils';
import type { RangeKey } from '../types';

const defaultRange = '24h' satisfies RangeKey;

function useRangeParam() {
  const [params, setParams] = useSearchParams();
  const range = (params.get('range') as RangeKey | null) ?? defaultRange;
  const setRange = (next: RangeKey) => {
    const nextParams = new URLSearchParams(params);
    nextParams.set('range', next);
    setParams(nextParams, { replace: true });
  };
  return [range, setRange] as const;
}

export function DashboardPage() {
  const api = useApiClient();
  const [range, setRange] = useRangeParam();

  const overview = useQuery({
    queryKey: ['overview', range],
    queryFn: () => api.getOverview(range),
  });
  const series = useQuery({
    queryKey: ['series', range, 'direction'],
    queryFn: () => api.getTimeSeries(range, 'direction'),
  });
  const topProcesses = useQuery({
    queryKey: ['top-processes', range, 'dashboard'],
    queryFn: () => api.getTopProcesses(range, { page: 1, pageSize: 5 }),
  });
  const topInboundRemotes = useQuery({
    queryKey: ['top-remotes', range, 'in'],
    queryFn: () => api.getTopRemotes(range, { page: 1, pageSize: 5, direction: 'in' }),
  });
  const topOutboundRemotes = useQuery({
    queryKey: ['top-remotes', range, 'out'],
    queryFn: () => api.getTopRemotes(range, { page: 1, pageSize: 5, direction: 'out' }),
  });
  const topPorts = useQuery({
    queryKey: ['top-ports', range],
    queryFn: () => api.getTopPorts(range),
  });

  const cards = overview.data
    ? [
        { label: '总上行', value: overview.data.bytesUp, suffix: 'bytes' as const },
        { label: '总下行', value: overview.data.bytesDown, suffix: 'bytes' as const },
        { label: '活跃连接', value: overview.data.activeConnections, suffix: 'count' as const },
        { label: '活跃进程', value: overview.data.activeProcesses, suffix: 'count' as const },
      ]
    : [];

  return (
    <div className="page">
      <header className="page-head hero-head">
        <div className="hero-copy">
          <h2>流量总览</h2>
          <p>按时间窗口检查真实采集数据，快速定位进程、入口来源 IP、出口目标 IP 与端口热点。</p>
          <section className="status-row">
            <div className="status-pill">
              <strong>时间范围</strong>
              <span>{rangeLabel(range)}</span>
            </div>
            {overview.data ? <DataSourceBadge dataSource={overview.data.dataSource} /> : null}
          </section>
        </div>
        <RangeSelect value={range} onChange={setRange} />
      </header>

      <section className="stat-grid">
        {cards.map((card) => (
          <StatCard key={card.label} {...card} />
        ))}
      </section>

      {series.data ? (
        <ChartPanel points={series.data.points} range={range} />
      ) : (
        <EmptyState title="趋势加载中" description="正在获取时间序列。" />
      )}

      <section className="panel-grid dashboard-grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Top 进程</h2>
            <Link to="/processes">查看全部</Link>
          </div>
          <ol className="top-list detailed-list">
            {topProcesses.data?.rows.map((row, index) => (
              <li key={`${row.pid ?? 'none'}-${row.comm ?? 'unknown'}-${index}`}>
                <div>
                  <strong>{safeText(row.comm)}</strong>
                  <span>{row.pid !== null ? `PID ${row.pid}` : '按进程名聚合'}</span>
                </div>
                <div className="top-value">
                  <strong>{formatBytes(row.totalBytes)}</strong>
                  <span>{row.exe ? row.exe : '未归因 / EXE 不可用'}</span>
                </div>
              </li>
            ))}
          </ol>
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 入站来源 IP</h2>
            <Link to="/remotes?direction=in">查看全部</Link>
          </div>
          <ol className="top-list detailed-list">
            {topInboundRemotes.data?.rows.map((row) => (
              <li key={`in-${row.remoteIp}`}>
                <div>
                  <strong>{safeText(row.remoteIp)}</strong>
                  <span>{peerRoleLabel('in')}</span>
                </div>
                <div className="top-value">
                  <strong>{formatBytes(row.totalBytes)}</strong>
                  <span>上行 {formatBytes(row.bytesUp)} / 下行 {formatBytes(row.bytesDown)}</span>
                </div>
              </li>
            ))}
          </ol>
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 出站目标 IP</h2>
            <Link to="/remotes?direction=out">查看全部</Link>
          </div>
          <ol className="top-list detailed-list">
            {topOutboundRemotes.data?.rows.map((row) => (
              <li key={`out-${row.remoteIp}`}>
                <div>
                  <strong>{safeText(row.remoteIp)}</strong>
                  <span>{peerRoleLabel('out')}</span>
                </div>
                <div className="top-value">
                  <strong>{formatBytes(row.totalBytes)}</strong>
                  <span>上行 {formatBytes(row.bytesUp)} / 下行 {formatBytes(row.bytesDown)}</span>
                </div>
              </li>
            ))}
          </ol>
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 端口</h2>
            <Link to="/usage">查看全部</Link>
          </div>
          <ol className="top-list">
            {topPorts.data?.rows.slice(0, 5).map((row) => (
              <li key={row.label}>
                <span>{row.label}</span>
                <strong>{formatBytes(row.value)}</strong>
              </li>
            ))}
          </ol>
        </section>
      </section>
    </div>
  );
}
