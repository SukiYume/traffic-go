import { useQuery } from '@tanstack/react-query';
import { Link, useSearchParams } from 'react-router-dom';
import { ChartPanel } from '../components/ChartPanel';
import { DataSourceBadge } from '../components/DataSourceBadge';
import { EmptyState } from '../components/EmptyState';
import { QueryErrorState } from '../components/QueryErrorState';
import { RangeSelect } from '../components/RangeSelect';
import { StatCard } from '../components/StatCard';
import { useApiClient } from '../api-context';
import { normalizeRangeKey } from '../ranges';
import { displayExecutableName, formatBytes, peerRoleLabel, rangeLabel, safeText } from '../utils';
import type { RangeKey } from '../types';

const defaultRange = '24h' satisfies RangeKey;

function buildDrilldownPath(path: string, range: RangeKey, extra?: Record<string, string>) {
  const params = new URLSearchParams();
  params.set('range', range);
  for (const [key, value] of Object.entries(extra ?? {})) {
    if (value) {
      params.set(key, value);
    }
  }
  return `${path}?${params.toString()}`;
}

function useRangeParam() {
  const [params, setParams] = useSearchParams();
  const range = normalizeRangeKey(params.get('range'), defaultRange);
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
    queryFn: ({ signal }) => api.getOverview(range, { signal }),
  });
  const series = useQuery({
    queryKey: ['series', range, 'direction'],
    queryFn: ({ signal }) => api.getTimeSeries(range, 'direction', undefined, { signal }),
  });
  const topProcesses = useQuery({
    queryKey: ['top-processes', range, 'dashboard'],
    queryFn: ({ signal }) => api.getTopProcesses(range, { page: 1, pageSize: 5, groupBy: 'pid' }, { signal }),
  });
  const topInboundRemotes = useQuery({
    queryKey: ['top-remotes', range, 'in'],
    queryFn: ({ signal }) => api.getTopRemotes(range, { page: 1, pageSize: 5, direction: 'in', includeLoopback: true }, { signal }),
  });
  const topOutboundRemotes = useQuery({
    queryKey: ['top-remotes', range, 'out'],
    queryFn: ({ signal }) => api.getTopRemotes(range, { page: 1, pageSize: 5, direction: 'out', includeLoopback: true }, { signal }),
  });
  const topPorts = useQuery({
    queryKey: ['top-ports', range],
    queryFn: ({ signal }) => api.getTopPorts(range, { signal }),
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
          <p>
            这是默认的全局视图：先看总上/下行、活跃连接与活跃进程，再结合趋势图判断是否存在突发峰值。若发现异常，
            可直接从下方 Top 列表进入进程、对端或明细页面继续排查。
          </p>
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

      {overview.isError ? (
        <QueryErrorState error={overview.error} title="总览加载失败" />
      ) : cards.length ? (
        <section className="stat-grid">
          {cards.map((card) => (
            <StatCard key={card.label} {...card} />
          ))}
        </section>
      ) : (
        <EmptyState title="总览加载中" description="正在获取总览统计。" />
      )}

      {series.isError ? (
        <QueryErrorState error={series.error} title="趋势加载失败" />
      ) : series.data ? (
        <ChartPanel
          points={series.data.points}
          groups={series.data.groups}
          groupBy={series.data.groupBy}
          range={range}
          subtitle="按方向聚合：入站 / 出站总量"
        />
      ) : (
        <EmptyState title="趋势加载中" description="正在获取时间序列。" />
      )}

      <section className="panel-grid dashboard-grid">
        <section className="panel">
          <div className="panel-head">
            <h2>Top 进程</h2>
            <Link to={buildDrilldownPath('/processes', range)}>查看全部</Link>
          </div>
          {topProcesses.isError ? (
            <QueryErrorState error={topProcesses.error} title="Top 进程加载失败" compact />
          ) : topProcesses.data?.rows.length ? (
            <ol className="top-list detailed-list">
              {topProcesses.data.rows.map((row, index) => (
                <li key={`${row.pid ?? 'none'}-${row.comm ?? 'unknown'}-${index}`}>
                  <div>
                    <strong>{safeText(row.comm)}</strong>
                    <span>{row.pid !== null ? `PID ${row.pid}` : '当前窗口已降级为按进程名聚合'}</span>
                  </div>
                  <div className="top-value">
                    <strong>{formatBytes(row.totalBytes)}</strong>
                    <span>{row.pid !== null ? displayExecutableName(row.exe) : '小时聚合 / EXE 在此视图不展示'}</span>
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState title="暂无进程数据" description="当前时间范围没有命中的进程排行。" />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 入站来源 IP</h2>
            <Link to={buildDrilldownPath('/remotes', range, { direction: 'in', include_loopback: '1' })}>查看全部</Link>
          </div>
          {topInboundRemotes.isError ? (
            <QueryErrorState error={topInboundRemotes.error} title="入站排行加载失败" compact />
          ) : topInboundRemotes.data?.rows.length ? (
            <ol className="top-list detailed-list">
              {topInboundRemotes.data.rows.map((row) => (
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
          ) : (
            <EmptyState title="暂无入站来源" description="当前时间范围没有命中的入站来源 IP。" />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 出站目标 IP</h2>
            <Link to={buildDrilldownPath('/remotes', range, { direction: 'out', include_loopback: '1' })}>查看全部</Link>
          </div>
          {topOutboundRemotes.isError ? (
            <QueryErrorState error={topOutboundRemotes.error} title="出站排行加载失败" compact />
          ) : topOutboundRemotes.data?.rows.length ? (
            <ol className="top-list detailed-list">
              {topOutboundRemotes.data.rows.map((row) => (
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
          ) : (
            <EmptyState title="暂无出站目标" description="当前时间范围没有命中的出站目标 IP。" />
          )}
        </section>
        <section className="panel">
          <div className="panel-head">
            <h2>Top 端口</h2>
            <Link to={buildDrilldownPath('/usage', range)}>查看全部</Link>
          </div>
          {topPorts.isError ? (
            <QueryErrorState error={topPorts.error} title="端口排行加载失败" compact />
          ) : topPorts.data?.rows.length ? (
            <ol className="top-list">
              {topPorts.data.rows.slice(0, 5).map((row) => (
                <li key={row.label}>
                  <span>{row.label}</span>
                  <strong>{formatBytes(row.value)}</strong>
                </li>
              ))}
            </ol>
          ) : (
            <EmptyState title="暂无端口数据" description="当前时间范围没有命中的端口排行。" />
          )}
        </section>
      </section>
    </div>
  );
}
