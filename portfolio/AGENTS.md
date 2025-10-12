# DashFolio agent instructions — `portfolio/`

## Module expectations
The TypeScript utilities in this directory power template formatting, chart data assembly, and unit tests executed with Vitest. Keep helpers pure and free of browser-only APIs so they remain testable in Node.

## Style & structure
- Use named exports for all functions and interfaces. Add new types next to the helpers that consume them.
- Keep indentation at two spaces and prefer `const` over `let` unless mutation is necessary.
- Validate nullable numeric fields with dedicated type guards (`Number.isFinite`, etc.). When you add new helpers that parse backend payloads, include exhaustive handling for `null`, `undefined`, and string values.
- Document return shapes with JSDoc blocks when the TypeScript type alone is not self-explanatory.

## Testing discipline
- Every new helper must have a Vitest test under `portfolio/__tests__`. Tests should exercise both happy and edge cases (missing fields, zero quantities, negative values).
- Use deterministic inputs; do not rely on current time or random data. When formatting numbers, assert on exact strings.
- Run `npm test` after updating helpers or tests.

## Interop with Flask templates
- Keep generated HTML snippets minimal and rely on Bootstrap utility classes already loaded by the templates. If you add CSS hooks, document the expected classes in `static/css/app.css` and update the template instructions.
- When you change the names of context fields consumed here, update the relevant Jinja templates and note the change in commit messages.

These practices keep the TypeScript utilities predictable for both the dashboard and the test suite.
