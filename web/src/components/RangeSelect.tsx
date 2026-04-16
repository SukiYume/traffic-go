import type { RangeKey } from '../types';
import { rangeLabel } from '../utils';

const ranges: RangeKey[] = ['1h', '24h', '7d', '30d', '90d'];

export function RangeSelect({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (value: RangeKey) => void;
}) {
  return (
    <div className="range-select" role="tablist" aria-label="时间范围">
      {ranges.map((range) => (
        <button
          key={range}
          type="button"
          role="tab"
          aria-selected={value === range}
          className={value === range ? 'chip active' : 'chip'}
          onClick={() => onChange(range)}
        >
          {rangeLabel(range)}
        </button>
      ))}
    </div>
  );
}
