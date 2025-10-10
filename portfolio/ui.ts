export interface HoldingSnapshot {
  quantity?: number | null;
  current_price?: number | null;
  previous_close?: number | null;
  todays_gain?: number | null;
  todays_gain_pct?: number | null;
}

export interface TodayChangeResult {
  value: number | null;
  pct: number | null;
  reason: string | null;
}

function isValidNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

export function computeTodayChange(holding: HoldingSnapshot | null | undefined): TodayChangeResult {
  if (!holding) {
    return { value: 0, pct: 0, reason: null };
  }

  const quantity = Number(holding.quantity ?? 0);
  const currentPrice = Number(holding.current_price ?? NaN);
  const previousClose = Number(holding.previous_close ?? NaN);

  if (quantity === 0) {
    return { value: 0, pct: 0, reason: null };
  }

  if (Number.isFinite(currentPrice) && Number.isFinite(previousClose) && previousClose > 0) {
    const priceDiff = currentPrice - previousClose;
    const value = priceDiff * quantity;
    const pct = previousClose !== 0 ? (priceDiff / previousClose) * 100 : null;
    if (pct === null || !Number.isFinite(pct)) {
      return { value: null, pct: null, reason: 'No prior close available' };
    }
    return { value, pct, reason: null };
  }

  if (isValidNumber(holding.todays_gain) && isValidNumber(holding.todays_gain_pct)) {
    return {
      value: holding.todays_gain,
      pct: holding.todays_gain_pct,
      reason: null,
    };
  }

  return { value: null, pct: null, reason: 'No prior close available' };
}

export function buildGainCell(
  value: number | null | undefined,
  percent: number | null | undefined,
  options: { placeholder?: string; tooltip?: string } = {},
): string {
  const placeholder = options.placeholder ?? '—';
  const tooltip = options.tooltip ?? '';
  const numericValue = Number(value);
  const numericPercent = Number(percent);
  const hasValue = Number.isFinite(numericValue);
  const hasPercent = Number.isFinite(numericPercent);

  if (!hasValue || !hasPercent) {
    const tooltipAttr = tooltip ? ` title="${tooltip}"` : '';
    return `<span class="d-block text-muted"${tooltipAttr}>${placeholder}</span>`;
  }

  const positive = numericValue >= 0;
  const cls = positive ? 'gain-positive' : 'gain-negative';
  const sign = positive ? '+' : '-';
  const absoluteValue = Math.abs(numericValue).toFixed(2);
  const percentText = `${numericPercent >= 0 ? '+' : '-'}${Math.abs(numericPercent).toFixed(2)}%`;
  return `<span class="d-block fw-semibold ${cls}">${sign}$${absoluteValue}</span>` +
    `<small class="text-muted">${percentText}</small>`;
}
