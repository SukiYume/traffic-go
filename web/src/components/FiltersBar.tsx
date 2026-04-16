import type { ProcessOption, RangeKey } from '../types';
import { DimensionHint } from './DimensionHint';

type Filters = {
  comm: string;
  pid: string;
  exe: string;
  remoteIp: string;
  localPort: string;
  direction: string;
  proto: string;
  attribution: string;
};

const directionOptions = [
  { value: '', label: '全部方向' },
  { value: 'in', label: '入站' },
  { value: 'out', label: '出站' },
];

const protoOptions = [
  { value: '', label: '全部协议' },
  { value: 'tcp', label: 'TCP' },
  { value: 'udp', label: 'UDP' },
];

const attributionOptions = [
  { value: '', label: '全部归因' },
  { value: 'exact', label: '精确' },
  { value: 'unknown', label: '未知' },
];

export function FiltersBar({
  range,
  processes,
  filters,
  onChange,
}: {
  range: RangeKey;
  processes: ProcessOption[];
  filters: Filters;
  onChange: (next: Filters) => void;
}) {
  const longRange = range === '90d';
  const update = (key: keyof Filters, value: string) => onChange({ ...filters, [key]: value });

  return (
    <div className="filters">
      <label>
        <span>进程</span>
        <input
          list="traffic-processes"
          value={filters.comm}
          onChange={(event) => update('comm', event.target.value)}
          placeholder="ss-server"
        />
      </label>
      <label>
        <span>PID</span>
        <input
          value={filters.pid}
          onChange={(event) => update('pid', event.target.value)}
          placeholder="1088"
          disabled={longRange}
          title={longRange ? '超过分钟明细保留窗口的数据按进程名聚合，无法按具体 PID 筛选' : undefined}
        />
      </label>
      <label>
        <span>EXE</span>
        <input
          value={filters.exe}
          onChange={(event) => update('exe', event.target.value)}
          placeholder="/usr/bin/ss-server"
          disabled={longRange}
          title={longRange ? '超过分钟明细保留窗口的数据按进程名聚合，无法按具体 EXE 筛选' : undefined}
        />
      </label>
      <label>
        <span>对端 IP</span>
        <input value={filters.remoteIp} onChange={(event) => update('remoteIp', event.target.value)} placeholder="8.8.8.8" />
      </label>
      <label>
        <span>本地端口</span>
        <input value={filters.localPort} onChange={(event) => update('localPort', event.target.value)} placeholder="443" />
      </label>
      <label>
        <span>方向</span>
        <select value={filters.direction} onChange={(event) => update('direction', event.target.value)}>
          {directionOptions.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </label>
      <label>
        <span>协议</span>
        <select value={filters.proto} onChange={(event) => update('proto', event.target.value)}>
          {protoOptions.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </label>
      <label>
        <span>归因</span>
        <select value={filters.attribution} onChange={(event) => update('attribution', event.target.value)}>
          {attributionOptions.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </label>
      <datalist id="traffic-processes">
        {processes.map((process) => (
          <option key={`${process.pid}-${process.comm}`} value={process.comm} />
        ))}
      </datalist>
      <DimensionHint visible={longRange} />
    </div>
  );
}
