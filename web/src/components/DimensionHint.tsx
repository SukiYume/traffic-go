export function DimensionHint({ visible }: { visible: boolean }) {
  if (!visible) return null;
  return (
    <div className="dimension-hint" role="note">
      超过分钟明细保留窗口的数据会切换到小时表，PID / EXE 维度不可用。
    </div>
  );
}
