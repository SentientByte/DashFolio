# DashFolio agent instructions — `templates/`

## Purpose
Templates render Flask context data into the Sneat dashboard theme. They must remain server-rendered, accessible, and compatible with Bootstrap 5 classes already included by the layout.

## Authoring guidelines
- Use four-space indentation for HTML and Jinja blocks. Align `{% %}` and `{{ }}` with surrounding markup for readability.
- Favour `{% extends 'layout.html' %}` with `{% block %}` overrides instead of duplicating layout chrome. Shared widgets should be factored into `{% include %}` or macro files.
- Reference context variables defensively: default to fallbacks with `|default` filters or `if` guards when data may be missing.
- Avoid inline JavaScript in templates. If behaviour is needed, wire it through TypeScript utilities or static assets and document the dependency in comments.
- Keep text ready for localisation by avoiding concatenated strings; wrap dynamic values with filters that handle pluralisation or formatting where needed.

## Styling hooks
- Prefer existing utility classes from Sneat/Bootstrap. Only add new classes when necessary and document them in `static/css/app.css`.
- SVG icons belong in `templates/icons/` and should be included with `{% include %}` to keep markup DRY.

## Data integrity
- When you change the structure of context dictionaries consumed by templates, update related docstrings in services and the instructions under `portfolio/AGENTS.md`.
- Respect authentication guards: any link or form that mutates data must check `is_authenticated` or rely on Flask routes that already enforce it.

See `templates/portfolio/AGENTS.md` for extra rules specific to the dashboard partials.
