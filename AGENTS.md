# AGENTS.md

Guidance for coding agents working in `fastdataframe`.

## Scope

- `fastdataframe` is a Python library that bridges Pydantic models with dataframe backends.
- Active backends in this repo: Polars, PyArrow, Iceberg.
- Prefer minimal, backend-scoped changes; avoid cross-backend refactors unless required.

## Repository map

- Source: `src/fastdataframe/`
- Tests: `tests/`
- Build/config: `pyproject.toml`
- Lockfile: `uv.lock`
- Hooks: `.pre-commit-config.yaml`
- CI: `.github/workflows/ci.yml`
- Cursor rules: `.cursor/rules/*.mdc`
- OpenSpec process docs: `openspec/AGENTS.md`

## Environment and tooling

- Python target: `>=3.12` (`.python-version` is `3.12`).
- Dependency and virtualenv workflow: `uv`.
- Tests: `pytest`.
- Lint/format: `ruff` (`ruff-check`, `ruff-format`).
- Type checks: `ty`.

## Setup commands

- Install all dependencies + optional backends (recommended):
  - `uv sync --all-extras`
- Install default dependencies only:
  - `uv sync`
- Install dev group explicitly:
  - `uv sync --group dev`

Notes:
- CI tests run with `uv sync --all-extras`.
- If backend tests fail due to missing dependencies, re-sync with `--all-extras`.

## Build, lint, and test commands

### Build

- `uv build`

### Lint and format

- `uv run ruff check .`
- `uv run ruff check . --fix`
- `uv run ruff format .`
- `uv run ty check`
- `uv run pre-commit run --all-files`

### Tests

- Full suite:
  - `uv run pytest tests/`
- CI-like coverage run:
  - `uv run pytest tests/ --cov=src/fastdataframe --cov-report=xml`
- Backend/test-area slices:
  - `uv run pytest tests/core/`
  - `uv run pytest tests/polars/`
  - `uv run pytest tests/pyarrow/`
  - `uv run pytest tests/iceberg/`
  - `uv run pytest tests/e2e/`

### Single-test execution (important)

- One file:
  - `uv run pytest tests/polars/test_model.py`
- One class:
  - `uv run pytest tests/polars/test_model.py::TestCast`
- One test function:
  - `uv run pytest tests/polars/test_model.py::TestCast::test_cast_to_model_schema_with_dataframe`
- By keyword:
  - `uv run pytest -k "optional and schema" tests/`
- Stop on first failure:
  - `uv run pytest -x tests/polars/test_model.py::TestCast::test_cast_to_model_schema_with_dataframe`

## Code style guidelines

### Formatting

- Let `ruff format` define final formatting.
- Keep line length around `88`.
- Preserve existing docstring and module structure style.

### Imports

- Group imports: standard library, third-party, local (`fastdataframe...`).
- Prefer absolute imports for project modules.
- Keep imports minimal; remove unused imports.

### Types and annotations

- Add type hints for public APIs and non-trivial internal helpers.
- Prefer modern generics (`list[str]`, `dict[str, Any]`) in new code.
- Keep dataframe unions explicit (example: `pl.DataFrame | pl.LazyFrame`).
- Use `Literal`/`TypeVar`/aliases consistently with nearby code.
- Use one optional style consistently per file (`T | None` or `Optional[T]`).

### Pydantic conventions

- Target Pydantic v2 APIs (`model_fields`, `model_json_schema`, etc.).
- Preserve field alias behavior (`serialization` vs `validation`).
- Keep `ColumnInfo` metadata behavior compatible with current helpers.

### Naming

- Classes: `PascalCase`.
- Functions/variables/methods: `snake_case`.
- Constants: `UPPER_SNAKE_CASE`.
- Tests: `test_*.py`, classes `Test...`, functions `test_...`.

### Error handling

- Raise specific exceptions for invalid inputs and unsupported type combinations.
- Prefer `ValueError` for invalid schema/type input unless a better exception exists.
- Keep error messages explicit and actionable.
- For schema validation flows, return `ValidationError` models where APIs already do so.
- Do not swallow exceptions silently.

## Testing guidelines

- Add/adjust tests with every behavior change.
- Mirror source structure in tests (`tests/polars`, `tests/pyarrow`, etc.).
- Prefer focused unit tests; add e2e coverage for cross-backend flows.
- Use `pytest.mark.parametrize` for type/format matrices.
- Keep fixtures isolated; use `tmp_path` for filesystem state.

## Cursor and Copilot rules

Detected Cursor rule files:

- `.cursor/rules/fastdataframe.mdc`
- `.cursor/rules/python.mdc`
- `.cursor/rules/pytest.mdc`
- `.cursor/rules/uv.mdc`
- `.cursor/rules/python-polylith.mdc`
- `.cursor/rules/just.mdc`
- `.cursor/rules/devcontainer.mdc`

How to apply them in this repo:

- Follow `uv` + `pytest` as primary tooling.
- Apply Python and pytest best practices pragmatically, matching existing patterns.
- `just.mdc` exists, but there is no `justfile`; use direct `uv` commands.
- Use devcontainer guidance only when editing `.devcontainer/*`.

Copilot rule check:

- No `.github/copilot-instructions.md` file is present.

## Practical workflow for agents

- Start with targeted tests (single file/node id), then expand scope.
- Run backend-specific suites for touched areas.
- For non-trivial changes, run `ruff`, `ty`, and relevant pytest slices before finishing.
- Keep diffs small and architecture-consistent.
- If working on OpenSpec/spec-process changes, also read `openspec/AGENTS.md`.
