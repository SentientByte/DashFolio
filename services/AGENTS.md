# DashFolio agent instructions — `services/`

## Role of the services layer
Service modules provide thin orchestration helpers for the Flask app: configuration management, authentication, formatting, and portfolio state I/O. They should remain lightweight and framework-agnostic so they can be re-used in CLI contexts.

## Coding conventions
- Keep modules free of Flask imports. Accept primitive arguments (strings, dicts, datetimes) and return serialisable structures that templates can consume directly.
- Apply `from __future__ import annotations` to new files and annotate return types. When returning dictionaries, define `TypedDict` classes if the shape is stable.
- When interacting with SQLite, call into `Calculations.storage` for connections and schema management. Avoid duplicating SQL that already exists there.
- Configuration helpers must respect the defaults in `config.json`. Document any new keys in both `README.md` and `structure.md`.
- Formatting helpers should never mutate their inputs. Include doctest-style examples in the docstring when you add new public formatters.

## Error handling
- Raise descriptive exceptions for invalid state (e.g., missing user record) so callers can map them to Flask responses. Do not exit the process from this layer.
- Log noteworthy events through `services.activity_log.append_log` instead of `print` statements.

## Testing
- When adding logic that transforms data (e.g., currency formatting), mirror it with unit tests under `portfolio/__tests__` or create a Python test alongside the service if TypeScript coverage is insufficient.

Keeping the services layer declarative ensures `app.py` stays readable and the single-user workflow remains predictable.
