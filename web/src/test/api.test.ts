import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHttpClient, normalizeUsageSortKey, shouldUseMockApi } from '../api';
import { createMockApiClient } from '../data/mock';

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
              remote_ip: '198.51.100.53',
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
      remoteIp: '198.51.100.53',
      localPort: '8388',
      page: 2,
      pageSize: 10,
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage?range=24h&remote_ip=198.51.100.53&local_port=8388&page=2&page_size=10&sort_by=bytes_total&sort_order=desc',
      expect.any(Object),
    );
    expect(response.rows[0]).toMatchObject({
      minuteTs: 1710000000,
      localPort: 8388,
      remoteIp: '198.51.100.53',
      bytesUp: 120,
      bytesDown: 360,
    });
    expect(response.nextCursor).toBe('next-1');
    expect(response.page).toBe(2);
    expect(response.totalRows).toBe(21);
  });

  it('passes AbortSignal through to fetch so superseded queries can be cancelled', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1m',
          next_cursor: null,
          page: 1,
          page_size: 1,
          total_rows: 1,
          data: [],
        }),
      ),
    );

    const client = createHttpClient();
    const controller = new AbortController();
    await client.getUsage({ range: '24h' }, { signal: controller.signal });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage?range=24h',
      expect.objectContaining({
        signal: controller.signal,
      }),
    );
  });

  it('decodes monthly archive rows', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data: [
            {
              month_ts: 1769904000,
              bytes_up: 100,
              bytes_down: 250,
              flow_count: 3,
              forward_bytes_orig: 40,
              forward_bytes_reply: 60,
              forward_flow_count: 2,
              evidence_count: 7,
              chain_count: 5,
              updated_at: 1772323200,
              archived: false,
              detail_available: true,
              detail_range: 'last_month',
            },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const response = await client.getMonthlyUsage();

    expect(fetchMock).toHaveBeenCalledWith('/api/v1/stats/monthly', expect.any(Object));
    expect(response.rows[0]).toMatchObject({
      monthTs: 1769904000,
      totalBytes: 350,
      forwardTotalBytes: 100,
      detailRange: 'last_month',
      archived: false,
    });
  });

  it('preserves grouped timeseries semantics while aggregating total buckets', async () => {
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
    expect(series.groupBy).toBe('direction');
    expect(series.points).toHaveLength(1);
    expect(series.points[0]).toMatchObject({ ts: 1710000000, up: 40, down: 60, flowCount: 3 });
    expect(series.groups).toHaveLength(2);
    expect(series.groups).toEqual([
      {
        key: 'in',
        points: [expect.objectContaining({ ts: 1710000000, up: 10, down: 20, flowCount: 1 })],
      },
      {
        key: 'out',
        points: [expect.objectContaining({ ts: 1710000000, up: 30, down: 40, flowCount: 2 })],
      },
    ]);
  });

  it('fetches network interface timeseries as RX and TX points', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          data_source: 'interface_1m',
          data: [
            { bucket_ts: 1710000000, interface: 'eth0', rx_bytes: 100, tx_bytes: 50 },
            { bucket_ts: 1710000000, interface: 'ens3', rx_bytes: 25, tx_bytes: 10 },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const series = await client.getNetworkTimeSeries('24h');

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/stats/interfaces/timeseries?range=24h&bucket=5m',
      expect.any(Object),
    );
    expect(series).toMatchObject({
      dataSource: 'interface_1m',
      bucket: '5m',
      points: [expect.objectContaining({ ts: 1710000000, down: 125, up: 60, flowCount: 0 })],
    });
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
              remote_ip: '198.51.100.53',
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
    const response = await client.getUsage({ range: 'last_month' });

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
    const processes = await client.getTopProcesses('24h', {
      page: 1,
      pageSize: 5,
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
      groupBy: 'comm',
    });
    const remotes = await client.getTopRemotes('24h', {
      page: 2,
      pageSize: 10,
      direction: 'in',
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      '/api/v1/top/processes?range=24h&page=1&page_size=5&sort_by=bytes_total&sort_order=desc&group_by=comm',
      expect.any(Object),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/v1/top/remotes?range=24h&page=2&page_size=10&sort_by=bytes_total&sort_order=desc&direction=in',
      expect.any(Object),
    );
    expect(processes.rows[0]).toMatchObject({ pid: 1045, comm: 'nginx', totalBytes: 480 });
    expect(remotes.rows[0]).toMatchObject({ direction: 'in', remoteIp: '203.0.113.24', totalBytes: 480 });
  });

  it('preserves empty process names in hourly process summaries', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1h',
          page: 1,
          page_size: 25,
          total_rows: 1,
          data: [
            {
              pid: null,
              comm: '',
              exe: null,
              bytes_up: 120,
              bytes_down: 360,
              flow_count: 3,
            },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const response = await client.getTopProcesses('last_month', { groupBy: 'comm' });

    expect(response.dataSource).toBe('usage_1h');
    expect(response.rows[0]).toMatchObject({
      pid: null,
      comm: '',
      exe: null,
      totalBytes: 480,
    });
  });

  it('includes the loopback flag in remote summary queries when requested', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1m',
          page: 1,
          page_size: 10,
          total_rows: 1,
          data: [
            { direction: 'out', remote_ip: '127.0.0.1', bytes_up: 120, bytes_down: 360, flow_count: 3 },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    await client.getTopRemotes('24h', {
      page: 1,
      pageSize: 10,
      includeLoopback: true,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/top/remotes?range=24h&page=1&page_size=10&sort_by=bytes_total&include_loopback=1',
      expect.any(Object),
    );
  });

  it('sends an explicit exclude flag when remote summaries hide loopback', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1m',
          page: 1,
          page_size: 10,
          total_rows: 0,
          data: [],
        }),
      ),
    );

    const client = createHttpClient();
    await client.getTopRemotes('24h', {
      page: 1,
      pageSize: 10,
      includeLoopback: false,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/top/remotes?range=24h&page=1&page_size=10&sort_by=bytes_total&exclude_loopback=1',
      expect.any(Object),
    );
  });

  it('maps forward filters to backend query params', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data_source: 'usage_1m_forward',
          next_cursor: null,
          page: 1,
          page_size: 10,
          total_rows: 1,
          data: [
            {
              time_bucket: 1710000000,
              proto: 'tcp',
              orig_src_ip: '10.0.0.2',
              orig_dst_ip: '203.0.113.53',
              orig_sport: 51122,
              orig_dport: 443,
              bytes_orig: 120,
              bytes_reply: 360,
              pkts_orig: 4,
              pkts_reply: 8,
              flow_count: 1,
            },
          ],
        }),
      ),
    );

    const client = createHttpClient();
    const response = await client.getForwardUsage({
      range: '24h',
      proto: 'tcp',
      origSrcIp: '10.0.0.2',
      origDstIp: '203.0.113.53',
      page: 1,
      pageSize: 10,
      sortBy: 'bytesTotal',
      sortOrder: 'desc',
    });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/forward/usage?range=24h&proto=tcp&orig_src_ip=10.0.0.2&orig_dst_ip=203.0.113.53&page=1&page_size=10&sort_by=bytes_total&sort_order=desc',
      expect.any(Object),
    );
    expect(response.rows[0]).toMatchObject({
      origSrc: '10.0.0.2',
      origDst: '203.0.113.53',
      bytesOrig: 120,
      bytesReply: 360,
    });
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
            target_ips: ['198.51.100.44'],
            chains: [
              {
                chain_id: 'usage_chain_1m|1710000000|1088|ss-server|/usr/bin/ss-server|203.0.113.24|47920|198.51.100.44|api.example.test|443',
                source_ip: '203.0.113.24',
                target_ip: '198.51.100.44',
                target_host: 'api.example.test',
                target_host_normalized: 'api.example.test',
                target_port: 443,
                local_port: 47920,
                bytes_total: 8062000,
                flow_count: 4,
                evidence_count: 3,
                evidence: 'ss-log',
                evidence_source: 'ss',
                sample_fingerprint: 'fp-1',
                sample_message: 'relay connect',
                sample_time: 1710000012,
                confidence: 'high',
              },
            ],
            related_peers: [
              {
                direction: 'out',
                remote_ip: '198.51.100.44',
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
      remoteIp: '198.51.100.44',
      remotePort: 443,
      attribution: 'exact',
      bytesUp: 100,
      bytesDown: 200,
      pktsUp: 1,
      pktsDown: 2,
      flowCount: 1,
    }, { dataSource: 'usage_1h' });

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage/explain?ts=1710000000&data_source=usage_1h&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=%2Fusr%2Fbin%2Fss-server&local_port=47920&remote_ip=198.51.100.44&remote_port=443',
      expect.any(Object),
    );
    expect(result).toMatchObject({
      process: 'ss-server (/usr/bin/ss-server)',
      confidence: 'medium',
      sourceIps: ['203.0.113.24'],
      targetIps: ['198.51.100.44'],
      notes: ['Shadowsocks 只能做同进程同时间窗关联。'],
    });
    expect(result.chains[0]).toMatchObject({
      chainId: 'usage_chain_1m|1710000000|1088|ss-server|/usr/bin/ss-server|203.0.113.24|47920|198.51.100.44|api.example.test|443',
      sourceIp: '203.0.113.24',
      targetIp: '198.51.100.44',
      targetHost: 'api.example.test',
      targetHostNormalized: 'api.example.test',
      targetPort: 443,
      localPort: 47920,
      evidenceCount: 3,
      evidence: 'ss-log',
      evidenceSource: 'ss',
      sampleFingerprint: 'fp-1',
      sampleMessage: 'relay connect',
      sampleTime: 1710000012,
      confidence: 'high',
    });
    expect(result.relatedPeers[0]).toMatchObject({
      direction: 'out',
      remoteIp: '198.51.100.44',
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
            source_ips: ['198.51.100.42'],
            target_ips: ['127.0.0.1'],
            related_peers: [],
            nginx_requests: [
              {
                time: 1710000000,
                method: 'GET',
                host: 'crawler.example.test',
                host_normalized: 'crawler.example.test',
                path: '/apod/2023/12/AstroPH-2023-12',
                status: 200,
                count: 3,
                client_ip: '127.0.0.1',
                referer: 'https://crawler.example.test/sitemap.xml',
                user_agent: 'Mozilla/5.0 (compatible; GPTBot/1.3; +https://openai.com/gptbot)',
                bot: 'GPTBot',
                sample_fingerprint: 'nginx-fp-1',
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
      remoteIp: '198.51.100.42',
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
      clientIp: '127.0.0.1',
      hostNormalized: 'crawler.example.test',
      referer: 'https://crawler.example.test/sitemap.xml',
      sampleFingerprint: 'nginx-fp-1',
    });
  });

  it('includes the scan flag when on-demand explain scanning is requested', async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            process: 'ss-server (/usr/bin/ss-server)',
            confidence: 'low',
            source_ips: [],
            target_ips: [],
            chains: [],
            related_peers: [],
            nginx_requests: [],
            notes: [],
          },
        }),
      ),
    );

    const client = createHttpClient();
    await client.getUsageExplain(
      {
        minuteTs: 1710000000,
        proto: 'tcp',
        direction: 'out',
        pid: 1088,
        comm: 'ss-server',
        exe: '/usr/bin/ss-server',
        localPort: 47920,
        remoteIp: '198.51.100.44',
        remotePort: 443,
        attribution: 'exact',
        bytesUp: 100,
        bytesDown: 200,
        pktsUp: 1,
        pktsDown: 2,
        flowCount: 1,
      },
      { dataSource: 'usage_1m', allowScan: true },
    );

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/usage/explain?ts=1710000000&data_source=usage_1m&proto=tcp&direction=out&pid=1088&comm=ss-server&exe=%2Fusr%2Fbin%2Fss-server&local_port=47920&remote_ip=198.51.100.44&remote_port=443&scan=1',
      expect.any(Object),
    );
  });

  it('uses mock api only when explicitly enabled', () => {
    expect(shouldUseMockApi(undefined)).toBe(false);
    expect(shouldUseMockApi('0')).toBe(false);
    expect(shouldUseMockApi('1')).toBe(true);
  });
});

describe('mock api client', () => {
  it('matches basename exe filters and clamps page sizes like the backend', async () => {
    const client = createMockApiClient();
    const usage = await client.getUsage({
      range: '24h',
      exe: 'ss-server',
      page: 1,
      pageSize: 999,
    });

    expect(usage.rows.length).toBeGreaterThan(0);
    expect(usage.pageSize).toBe(200);
    expect(usage.rows.every((row) => row.exe?.includes('ss-server') ?? false)).toBe(true);
  });
});

describe('normalizeUsageSortKey', () => {
  it('falls back to minuteTs for unknown values', () => {
    expect(normalizeUsageSortKey('bogus')).toBe('minuteTs');
  });

  it('keeps supported sort keys unchanged', () => {
    expect(normalizeUsageSortKey('bytesTotal')).toBe('bytesTotal');
  });
});
