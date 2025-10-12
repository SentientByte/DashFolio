# DashFolio agent instructions — `assets/`

## Vendor theme assets
The `assets/` directory is a copy of the Sneat dashboard theme bundled for offline use. Treat these files as third-party artefacts.

## Modification policy
- Do **not** edit files here unless you are upgrading the vendor theme. Custom tweaks belong in `static/css/app.css` or front-end utilities under `portfolio/`.
- If an upgrade is required, document the upstream version, source URL, and changes you made in the commit message. Keep diffs limited to the files you actually touched.
- When adding new assets, prefer placing them under `static/` unless they truly belong alongside the vendor files.

Keeping this folder untouched ensures future theme updates remain manageable.
