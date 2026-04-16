import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHttpClient, shouldUseMockApi } from '../api';

const fetchMock = vi.fn<typeof fetch>();

describe('http api client', () => {
  afterEach(() => {
    fetchMock.mockReset();
    vi.unstubAllGlobals();
  });

  it('maps usage filters to backend query params and decodes snake_case rows', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1m',
          next_cursor: 'next-1',
          page: 2,
          page_size: 10,
          total_rows: 21,
          data: [
            {
              time_bucket: 1710000000,
              proto: 'tcp',
              direction: 'out',
              pid: 42,
              comm: 'ss-server',
              exe: '/usr/bin/ss-server',
              local_port: 8388,
              remote_ip: '1.1.1.1',
              remote_port: 443,
              attribution: 'exact',
              bytes_up: 120,
              bytes_down: 360,
              pkts_up: 4,
              pkts_down: 8,
              flow_count: 1,
            },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const response = await client.getUsage({
      range: '24h',
      remoteIp: '1.1.1.1',
      localPort: '8388',
      page: 2,
      pageSize: 10,
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage?range=24h&remote_ip=1.1.1.1&local_port=8388&page=2&page_size=10&sort_by=bytes_total&sort_order=desc',
      expect.any(Object),
    );
    expect(response.rows[0]).toMatchObject({
      minuteTs: 1710000000,
      localPort: 8388,
      remoteIp: '1.1.1.1',
      bytesUp: 120,
      bytesDown: 360,
    });
    expect(response.nextCursor).toBe('next-1');
    expect(response.page).toBe(2);
    expect(response.totalRows).toBe(21);
  });

  it('decodes overview and aggregates timeseries buckets from backend payloads', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data: {
              range: '24h',
              data_source: 'usage_1m',
              bytes_up: 10,
              bytes_down: 20,
              flow_count: 1,
              active_connections: 3,
              active_processes: 2,
            },
          }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data_source: 'usage_1m',
            data: [
              { bucket_ts: 1710000000, group: 'in', bytes_up: 10, bytes_down: 20, flow_count: 1 },
              { bucket_ts: 1710000000, group: 'out', bytes_up: 30, bytes_down: 40, flow_count: 2 },
            ],
          }),
        ),
      );

    const client = createHttpClient();
    const overview = await client.getOverview('24h');
    const series = await client.getTimeSeries('24h', 'direction', { comm: 'ss-server' });

    expect(overview).toMatchObject({
      bytesUp: 10,
      bytesDown: 20,
      activeConnections: 3,
      activeProcesses: 2,
      dataSource: 'usage_1m',
    });
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/v1/stats/timeseries?range=24h&bucket=5m&group_by=direction&comm=ss-server',
      expect.any(Object),
    );
    expect(series.points).toHaveLength(1);
    expect(series.points[0]).toMatchObject({ ts: 1710000000, up: 40, down: 60, flowCount: 3 });
  });

  it('preserves null-only hourly dimensions in usage responses', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1h',
          next_cursor: null,
          page: 1,
          page_size: 25,
          total_rows: 1,
          data: [
            {
              time_bucket: 1710000000,
              proto: 'tcp',
              direction: 'out',
              pid: null,
              comm: 'ss-server',
              exe: null,
              local_port: 8388,
              remote_ip: '1.1.1.1',
              remote_port: null,
              attribution: null,
              bytes_up: 120,
              bytes_down: 360,
              pkts_up: 4,
              pkts_down: 8,
              flow_count: 1,
            },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const response = await client.getUsage({ range: '90d' });

    expect(response.dataSource).toBe('usage_1h');
    expect(response.rows[0]).toMatchObject({
      pid: null,
      exe: null,
      remotePort: null,
      attribution: null,
    });
  });

  it('builds paged process and remote summary queries', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data_source: 'usage_1m',
            page: 1,
            page_size: 5,
            total_rows: 9,
            data: [
              { pid: 1045, comm: 'nginx', exe: '/usr/sbin/nginx', bytes_up: 120, bytes_down: 360, flow_count: 3 },
            ],
          }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data_source: 'usage_1m',
            page: 2,
            page_size: 10,
            total_rows: 30,
            data: [
              { direction: 'in', remote_ip: '203.0.113.24', bytes_up: 120, bytes_down: 360, flow_count: 3 },
            ],
          }),
        ),
      );

    const client = createHttpClient();
    const processes = await client.getTopProcesses('24h', { page: 1, pageSize: 5, sortBy: 'bytesTotal', sortOrder: 'desc' });
    const remotes = await client.getTopRemotes('24h', {
      page: 2,
      pageSize: 10,
      direction: 'in',
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      '/api/v1/top/processes?range=24h&page=1&page_size=5&sort_by=total&sort_order=desc',
      expect.any(Object),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/v1/top/remotes?range=24h&page=2&page_size=10&sort_by=total&sort_order=desc&direction=in',
      expect.any(Object),
    );
    expect(processes.rows[0]).toMatchObject({ pid: 1045, comm: 'nginx', totalBytes: 480 });
    expect(remotes.rows[0]).toMatchObject({ direction: 'in', remoteIp: '203.0.113.24', totalBytes: 480 });
  });

  it('requests usage explain endpoint and decodes nested fields', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            process: 'ss-server (/usr/bin/ss-server)',
            confidence: 'medium',
            source_ips: ['203.0.113.24'],
            target_ips: ['142.250.72.14'],
            related_peers: [
              {
                direction: 'out',
                remote_ip: '142.250.72.14',
                remote_port: 443,
                local_port: 47920,
                bytes_total: 8062000,
                flow_count: 4,
              },
            ],
            nginx_requests: [],
            notes: ['Shadowsocks 只能做同进程同时间窗关联。'],
          },
        }),
      ),
    );

    const client = createHttpClient();
    const result = await client.getUsageExplain({
      minuteTs: 1710000000,
      proto: 'tcp',
      direction: 'out',
      pid: 1088,
      comm: 'ss-server',
      exe: '/usr/bin/ss-server',
      localPort: 47920,
      remoteIp: '142.250.72.14',
      remotePort: 443,
      attribution: 'exact',
      bytesUp: 100,
      bytesDown: 200,
      pktsUp: 1,
      pktsDown: 2,
      flowCount: 1,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage/explain?ts=1710000000&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=%2Fusr%2Fbin%2Fss-server&local_port=47920&remote_ip=142.250.72.14&remote_port=443',
      expect.any(Object),
    );
    expect(result).toMatchObject({
      process: 'ss-server (/usr/bin/ss-server)',
      confidence: 'medium',
      sourceIps: ['203.0.113.24'],
      targetIps: ['142.250.72.14'],
      notes: ['Shadowsocks 只能做同进程同时间窗关联。'],
    });
    expect(result.relatedPeers[0]).toMatchObject({
      direction: 'out',
      remoteIp: '142.250.72.14',
      remotePort: 443,
      localPort: 47920,
      bytesTotal: 8062000,
      flowCount: 4,
    });

    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          data: {
            process: 'nginx (/usr/sbin/nginx)',
            confidence: 'high',
            source_ips: ['74.7.227.153'],
            target_ips: ['127.0.0.1'],
            related_peers: [],
            nginx_requests: [
              {
                time: 1710000000,
                method: 'GET',
                host: 'paris.escape.ac.cn',
                path: '/apod/2023/12/AstroPH-2023-12',
                status: 200,
                count: 3,
                referer: 'https://paris.escape.ac.cn/sitemap.xml',
                user_agent: 'Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)',
                bot: 'GPTBot',
              },
            ],
            notes: ['访问端识别：GPTBot(3)'],
          },
        }),
      ),
    );

    const nginxExplain = await client.getUsageExplain({
      minuteTs: 1710000000,
      proto: 'tcp',
      direction: 'in',
      pid: 32161,
      comm: 'nginx',
      exe: '/usr/sbin/nginx',
      localPort: 443,
      remoteIp: '74.7.227.153',
      remotePort: 36892,
      attribution: 'exact',
      bytesUp: 10,
      bytesDown: 20,
      pktsUp: 1,
      pktsDown: 1,
      flowCount: 1,
    });

    expect(nginxExplain.nginxRequests[0]).toMatchObject({
      path: '/apod/2023/12/AstroPH-2023-12',
      count: 3,
      bot: 'GPTBot',
      referer: 'https://paris.escape.ac.cn/sitemap.xml',
    });
  });

  it('uses mock api only when explicitly enabled', () => {
    expect(shouldUseMockApi(undefined)).toBe(false);
    expect(shouldUseMockApi('0')).toBe(false);
    expect(shouldUseMockApi('1')).toBe(true);
  });
});
