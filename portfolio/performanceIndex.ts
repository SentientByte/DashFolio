export type FlowKind = 'deposit' | 'withdrawal' | 'dividend' | 'interest' | 'fee';

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
  const hasHoldings = (value: number): boolean => Number.isFinite(value) && value > 0;
  const getValue = (day: string): number => invested.get(day) ?? 0;

  let base: string | null = null;
  let idx = start;

  for (const day of days) {
    const currentValue = getValue(day);
    if (!hasHoldings(currentValue)) {
      if (flowDays.has(day)) {
        base = null;
      }
      continue;
    }

    if (base === null) {
      base = day;
      out.set(day, idx);
      continue;
    }

    const baseValue = getValue(base);
    if (!hasHoldings(baseValue)) {
      base = day;
      out.set(day, idx);
      continue;
    }

    const r = (currentValue - baseValue) / baseValue;
    idx = idx * (1 + r);
    out.set(day, idx);
    if (flowDays.has(day)) {
      base = day;
    }
  }
  return out;
}
