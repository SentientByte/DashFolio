import { describe, expect, it } from 'vitest';

import { buildInvestedSeries, investedValue } from '../investedSeries';

describe('investedValue', () => {
  it('sums position values for matching tickers', () => {
    const qtys = { AAPL: 10, MSFT: 5 };
    const closes = { AAPL: 150, MSFT: 320 };
    expect(investedValue(qtys, closes)).toBe(10 * 150 + 5 * 320);
  });

  it('skips symbols with missing or non-finite values', () => {
    const qtys = { AAPL: 10, MSFT: Number.NaN, TSLA: 4 };
    const closes = { AAPL: 150, TSLA: undefined as unknown as number, GOOG: 100 }; // undefined should be skipped
    expect(investedValue(qtys, closes)).toBe(1500);
  });
});

describe('buildInvestedSeries', () => {
  it('builds a map keyed by iso date preserving order', () => {
    const days = ['2024-01-01', '2024-01-02'];
    const qtyByDay = new Map([
      ['2024-01-01', { AAPL: 5 }],
      ['2024-01-02', { AAPL: 5, MSFT: 3 }],
    ]);
    const closeByDay = new Map([
      ['2024-01-01', { AAPL: 100 }],
      ['2024-01-02', { AAPL: 110, MSFT: 300 }],
    ]);

    const series = buildInvestedSeries(days, qtyByDay, closeByDay);
    expect(Array.from(series.keys())).toEqual(days);
    expect(series.get('2024-01-01')).toBeCloseTo(500);
    expect(series.get('2024-01-02')).toBeCloseTo(5 * 110 + 3 * 300);
  });

  it('handles missing maps by defaulting to zero values', () => {
    const days = ['2024-01-03'];
    const series = buildInvestedSeries(days, new Map(), new Map());
    expect(series.get('2024-01-03')).toBe(0);
  });
});
