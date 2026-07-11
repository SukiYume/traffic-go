import { useRef } from 'react';
import type { KeyboardEvent } from 'react';
import type { RangeKey } from '../types';
import { rangeLabel } from '../utils';

const ranges: RangeKey[] = ['1h', '24h', '7d', 'this_month', 'last_month', 'two_months_ago'];

export function RangeSelect({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (value: RangeKey) => void;
}) {
  const buttons = useRef<Array<HTMLButtonElement | null>>([]);
  const selectAt = (index: number) => {
    const normalized = (index + ranges.length) % ranges.length;
    onChange(ranges[normalized]);
    buttons.current[normalized]?.focus();
  };
  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    let nextIndex: number | null = null;
    if (event.key === 'ArrowRight') nextIndex = index + 1;
    if (event.key === 'ArrowLeft') nextIndex = index - 1;
    if (event.key === 'Home') nextIndex = 0;
    if (event.key === 'End') nextIndex = ranges.length - 1;
    if (nextIndex == null) return;
    event.preventDefault();
    selectAt(nextIndex);
  };

  return (
    <div className="range-select" role="tablist" aria-label="时间范围">
      {ranges.map((range, index) => (
        <button
          key={range}
          ref={(element) => { buttons.current[index] = element; }}
          type="button"
          role="tab"
          aria-selected={value === range}
          tabIndex={value === range ? 0 : -1}
          className={value === range ? 'chip active' : 'chip'}
          onClick={() => onChange(range)}
          onKeyDown={(event) => onKeyDown(event, index)}
        >
          {rangeLabel(range)}
        </button>
      ))}
    </div>
  );
}
