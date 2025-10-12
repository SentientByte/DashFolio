# DashFolio agent instructions — `static/`

## Scope
Files in this directory hold first-party static assets served via Flask (`/static`). Custom styles belong in `css/app.css`; images/icons go alongside.

## Styling guidelines (`css/app.css`)
- Use four spaces for indentation and group related declarations with blank lines. Place custom properties (`--foo`) at the top-level `:root` when they are reused.
- Prefer semantic class names (`portfolio-metric-card`) rather than purely visual ones (`.blue-box`). Document new hooks in comments near their first declaration.
- Keep colour palette aligned with the CSS variables defined at the top of the file. Introduce new colours via variables instead of hard-coded hex values inside rules.
- Use responsive-friendly units (`rem`, `clamp`, flexbox) to match the existing layout patterns. Avoid fixed pixel widths unless necessary.

## Asset management
- Optimise new images before committing. For SVG icons place them under `templates/icons/` instead of here so they can be inlined.
- Do not modify files under `assets/` from this directory; that folder contains vendor theme resources managed separately.

Following these rules keeps the DashFolio look-and-feel cohesive without diverging from the Sneat theme baseline.
