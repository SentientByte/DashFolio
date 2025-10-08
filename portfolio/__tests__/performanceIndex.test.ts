import { describe, expect, it } from 'vitest';
import { buildInvestedSeries, QtyBySymbol, CloseBySymbol } from '../investedSeries';
import { buildTWRIndex } from '../performanceIndex';

describe('performance index', () => {
  it('ignores deposits when computing TWR', () => {
    const days = ['2024-01-01', '2024-01-02', '2024-01-03'];
    const invested = new Map<string, number>([
      ['2024-01-01', 10_000],
      ['2024-01-02', 10_000],
      ['2024-01-03', 10_500],
    ]);
    const flowDays = new Set<string>(['2024-01-02']);
    const index = buildTWRIndex(days, invested, flowDays, 100);
    expect(index.get('2024-01-01')).toBeCloseTo(100, 6);
    expect(index.get('2024-01-02')).toBeCloseTo(100, 6);
    expect(index.get('2024-01-03')).toBeCloseTo(105, 6);
  });

  it('excludes cash from invested percentages', () => {
    const days = ['2024-01-01', '2024-01-02'];
    const qtyByDay = new Map<string, QtyBySymbol>([
      ['2024-01-01', { AAA: 10 }],
      ['2024-01-02', { AAA: 10, CASH: 5000 }],
    ]);
    const closeByDay = new Map<string, CloseBySymbol>([
      ['2024-01-01', { AAA: 100 }],
      ['2024-01-02', { AAA: 100 }],
    ]);
    const investedSeries = buildInvestedSeries(days, qtyByDay, closeByDay);
    expect(investedSeries.get('2024-01-01')).toBe(1000);
    expect(investedSeries.get('2024-01-02')).toBe(1000);

    const index = buildTWRIndex(days, investedSeries, new Set<string>(), 100);
    expect(index.get('2024-01-01')).toBeCloseTo(100, 6);
    expect(index.get('2024-01-02')).toBeCloseTo(100, 6);
  });
});
