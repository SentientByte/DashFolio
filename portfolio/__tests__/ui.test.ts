import { describe, expect, it } from 'vitest';
import { buildGainCell, computeTodayChange } from '../ui';

describe('UI helpers', () => {
  it('returns null change and placeholder when previous close missing', () => {
    const change = computeTodayChange({
      quantity: 5,
      current_price: 42,
      previous_close: null,
    });

    expect(change.value).toBeNull();
    expect(change.pct).toBeNull();
    expect(change.reason).toBe('No prior close available');

    const cell = buildGainCell(change.value, change.pct, {
      tooltip: change.reason ?? undefined,
    });
    expect(cell).toContain('—');
    expect(cell).toContain('No prior close available');
  });
});
