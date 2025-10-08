export type FlowKind = 'deposit' | 'withdrawal' | 'dividend' | 'interest';

/** flowDays = dates with external cash flows (deposits, withdrawals). */
export function buildTWRIndex(
  days: string[],
  invested: Map<string, number>,
  flowDays: Set<string>,
  start = 100,
): Map<string, number> {
  const out = new Map<string, number>();
  if (!days.length) {
    return out;
  }
  let base = days[0];
  out.set(base, start);
  let idx = start;
  for (let i = 1; i < days.length; i++) {
    const day = days[i];
    const baseValue = invested.get(base) ?? 0;
    const currentValue = invested.get(day) ?? 0;
    const r = baseValue === 0 ? 0 : (currentValue - baseValue) / baseValue;
    idx = idx * (1 + r);
    out.set(day, idx);
    if (flowDays.has(day)) {
      base = day;
    }
  }
  return out;
}
