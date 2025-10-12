# DashFolio agent instructions

## Repository-wide expectations
- Preserve the single-user workflow described in `structure.md`. When you adjust onboarding, portfolio, or risk calculations, update the document so it continues to match the behaviour.
- Never commit credentials, API keys, or personally identifiable data. Configuration defaults belong in `config.json`; secrets must stay outside the repo.
- Reuse the filesystem helpers in `app_paths.py` instead of hard-coding absolute paths. This keeps Windows batch scripts and Linux shells in sync.
- Database schema changes must be reflected in `Calculations/storage.py` and documented under the "SQLite schema" section of `structure.md`.
- Prefer small, well-named modules. If a change touches both the calculation layer and presentation, split the work into separate commits whenever possible.

## Python code (applies to `*.py` files everywhere)
- New modules should begin with `from __future__ import annotations` and include a module-level docstring summarising their purpose.
- Use type hints on public functions and return `TypedDict`/`dataclass` objects when returning structured records. Avoid untyped `dict` unless the shape is obvious.
- Keep functions side-effect free unless they explicitly persist data. Wrap database writes in helpers from `Calculations.storage` instead of opening SQLite connections ad-hoc.
- Follow PEP 8 (4-space indents, snake_case function names) and include doctring sections that explain important parameters and return values.
- Guard expensive imports (e.g., `pandas`, `numpy`) at module scope only when they are truly required. Do not introduce new heavy dependencies without updating `README.md` and installation docs.

## TypeScript and front-end utilities
- This project uses ESM TypeScript. Keep exports named and avoid default exports so the test suite can tree-shake effectively.
- Maintain pure, deterministic helpers in `portfolio/`—they are consumed inside Jinja templates and by Vitest. If you add new helpers, mirror them with unit tests.
- Run `npm test` (Vitest) whenever you modify TypeScript logic, and keep snapshots or inline expectations readable.

## Templates, styles, and assets
- HTML templates live under `templates/`. Follow the indentation style you see in the existing files (4 spaces) and prefer `{% set %}`/macros over duplicating markup.
- Custom styling goes in `static/css/app.css`. Avoid editing `assets/` directly unless you are updating the vendor theme—see `assets/AGENTS.md` for details.
- When you add a new visual element that has behaviour tied to TypeScript helpers, document the expected context variables in the surrounding template.

## Testing & tooling
- Python code does not have a global test suite; at minimum run `python -m compileall Calculations services` to catch syntax errors when you touch Python files.
- Run targeted scripts (e.g., `python main.py`) only when necessary; avoid long-running market-data refreshes in CI notes.
- For TypeScript changes run `npm test`. Front-end visual regressions should be captured with screenshots when the interface changes.

Following these instructions keeps DashFolio's backtesting, risk analytics, and dashboard layers aligned.
