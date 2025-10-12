export type QtyBySymbol = Record<string, number>;
export type CloseBySymbol = Record<string, number>;

export function investedValue(qtys: QtyBySymbol, closes: CloseBySymbol): number {
  let value = 0;
  for (const symbol in qtys) {
    if (Object.prototype.hasOwnProperty.call(qtys, symbol)) {
      const quantity = qtys[symbol];
      const close = closes[symbol];
      if (!Number.isFinite(quantity) || !Number.isFinite(close)) {
        continue;
      }
      value += quantity * close;
    }
  }
  return value;
}

/** returns Map<ISODate, investedValue> */
export function buildInvestedSeries(
  days: string[],
  qtyByDay: Map<string, QtyBySymbol>,
  closeByDay: Map<string, CloseBySymbol>,
): Map<string, number> {
  const out = new Map<string, number>();
  for (const day of days) {
    const qtys = qtyByDay.get(day) ?? {};
    const closes = closeByDay.get(day) ?? {};
    out.set(day, investedValue(qtys, closes));
  }
  return out;
}
