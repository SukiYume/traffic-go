import { formatBytes, formatNumber } from '../utils';

export function StatCard({
  label,
  value,
  suffix,
}: {
  label: string;
  value: number;
  suffix?: 'bytes' | 'count';
}) {
  return (
    <section className="stat-card">
      <span>{label}</span>
      <strong>{suffix === 'bytes' ? formatBytes(value) : formatNumber(value)}</strong>
    </section>
  );
}
