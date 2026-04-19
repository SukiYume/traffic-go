import { CustomSelect } from './CustomSelect';
import { DimensionHint } from './DimensionHint';
import { attributionDescription, minuteDimensionsUnavailable } from '../utils';
import type { DataSource, ProcessOption } from '../types';

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
  { value: 'exact', label: 'exact — 精确匹配' },
  { value: 'heuristic', label: 'heuristic — 规则推断' },
  { value: 'guess', label: 'guess — 短时猜测' },
  { value: 'unknown', label: 'unknown — 无法归因' },
];

function distinctProcessNames(processes: ProcessOption[]) {
  const suggestions: string[] = [];
  const seen = new Set<string>();
  for (const process of processes) {
    const comm = process.comm.trim();
    if (!comm || seen.has(comm)) {
      continue;
    }
    seen.add(comm);
    suggestions.push(comm);
  }
  return suggestions;
}

export function FiltersBar({
  dataSource,
  processes,
  filters,
  onChange,
}: {
  dataSource?: DataSource;
  processes: ProcessOption[];
  filters: Filters;
  onChange: (next: Filters) => void;
}) {
  const minuteOnlyUnavailable = minuteDimensionsUnavailable(dataSource ?? null);
  const minuteOnlyTitle = minuteOnlyUnavailable ? '当前页面已切换到小时聚合，无法使用分钟级归因维度筛选' : undefined;
  const update = (key: keyof Filters, value: string) => onChange({ ...filters, [key]: value });
  const attributionHint = attributionDescription(filters.attribution || null);
  const processSuggestions = distinctProcessNames(processes);

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
          disabled={minuteOnlyUnavailable}
          title={minuteOnlyUnavailable ? '当前页面已切换到小时聚合，无法按具体 PID 筛选' : undefined}
        />
      </label>
      <label>
        <span>EXE</span>
        <input
          value={filters.exe}
          onChange={(event) => update('exe', event.target.value)}
          placeholder="ss-server"
          disabled={minuteOnlyUnavailable}
          title={minuteOnlyUnavailable ? '当前页面已切换到小时聚合，无法按具体 EXE 筛选' : undefined}
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
        <CustomSelect
          value={filters.direction}
          options={directionOptions}
          onChange={(v) => update('direction', v)}
        />
      </label>
      <label>
        <span>协议</span>
        <CustomSelect
          value={filters.proto}
          options={protoOptions}
          onChange={(v) => update('proto', v)}
        />
      </label>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <label style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <span>归因</span>
          <CustomSelect
            value={filters.attribution}
            options={attributionOptions}
            onChange={(v) => update('attribution', v)}
            disabled={minuteOnlyUnavailable}
            title={minuteOnlyTitle}
          />
        </label>
        {filters.attribution && (
          <span className="filter-hint">{attributionHint}</span>
        )}
      </div>
      <datalist id="traffic-processes">
        {processSuggestions.map((comm) => (
          <option key={comm} value={comm} />
        ))}
      </datalist>
      <DimensionHint visible={minuteOnlyUnavailable} />
    </div>
  );
}
