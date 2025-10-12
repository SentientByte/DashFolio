# DashFolio agent instructions — `Calculations/`

## Purpose of this package
The `Calculations` package houses the analytics engine (portfolio valuation, market data hydration, risk analysis, and snapshot caching). Modules here must stay importable from both CLI scripts (`main.py`) and the Flask app without side effects.

## Coding guidelines
- Keep modules focused. Shared helpers belong in `utils.py`; persistence helpers belong in `storage.py`. Resist circular imports by moving cross-cutting concerns into `Calculations/__init__.py` or dedicated helpers.
- Public functions must have precise docstrings describing required DataFrame columns and the units for returned values. Mention when a function mutates inputs versus returning new copies.
- Always normalise date indexes using `utils.normalize_index` before comparing time series. Persisted timestamps should be timezone-naive UTC.
- When reading or writing SQLite data, go through `storage.connect` and the provided ensure/insert helpers. Do not open bare `sqlite3` connections in these modules.
- Random simulations (e.g., Monte Carlo) should accept a `seed` keyword argument when you introduce new functionality. Default to `None` so existing workflows stay stochastic while enabling reproducible tests.

## Pandas & numpy usage
- Convert user-facing percentages to floats in the range `0–100` and document whether values are absolute or fractional.
- Prefer vectorised operations over Python loops. If you must iterate row-by-row, justify it in a short comment explaining the business rule.
- Avoid modifying global numpy or pandas options. If a function needs a temporary setting, use a context manager.

## Error handling & logging
- Raise `ValueError` for invalid inputs instead of printing where possible. Existing `print` statements used for CLI progress should be funnelled through small helper functions if you add new logging.
- Return empty DataFrames instead of `None` when callers expect tabular data. Callers should never have to guard against `None`.

## Testing expectations
- Add or update doctests/Vitest coverage when you change deterministic helpers. For probabilistic functions, include unit tests that assert bounds or invariants (e.g., probability is between 0 and 1).

Following these conventions keeps the analytics engine predictable and reusable across DashFolio entry points.
