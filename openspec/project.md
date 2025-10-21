# Project Context

## Purpose
FastDataFrame is a Python library that implements a **bridge pattern** between Pydantic models and dataframe libraries. It enables seamless conversion of Pydantic models to dataframe-specific models (Polars, Apache Iceberg) with:
- Automatic schema validation and type conversion
- Rich column metadata (uniqueness constraints, date formats, boolean mappings)
- Support for complex types (Optional, Union, nested structures)
- Backend-agnostic model definitions with backend-specific implementations

## Tech Stack
- **Language**: Python ≥3.12
- **Package Manager**: UV (uv sync, uv add, uv run)
- **Core Dependencies**:
  - pydantic ≥2.0 (model definitions and validation)
  - annotated-types (rich type annotations)
- **Backend Libraries**:
  - polars ≥1.26.0 (DataFrame/LazyFrame support)
  - pyiceberg ≥0.9 (Apache Iceberg table schemas)
- **Development Tools**:
  - pytest (testing framework with parametrization)
  - mypy (static type checking)
  - ruff (linting and formatting)
  - pytest-cov (coverage reporting)
  - gh (GitHub CLI for issue/PR management)

## Project Conventions

### Code Style
- **Type Hints**: All functions must have complete type annotations
- **Formatting**: Use `uv run ruff format src/ tests/` (enforced)
- **Linting**: Use `uv run ruff check src/ tests/` (must pass)
- **Import Organization**: Backend-specific modules should not be imported by core
- **Naming**:
  - Models: `{Backend}FastDataframeModel` (e.g., `PolarsFastDataframeModel`)
  - Type utilities: `_types.py` within each backend module
  - Test files: `test_{module}.py` mirroring source structure

### Architecture Patterns
- **Bridge Pattern**: Core abstraction (`FastDataframeModel`) with backend-specific implementations
- **Factory Method**: `FastDataframeModel.from_base_model()` creates backend-specific models
- **Schema Translation**: Each backend handles its own type mapping and validation
- **Module Structure**:
  - `src/fastdataframe/core/`: Base classes and shared utilities
  - `src/fastdataframe/{backend}/`: Backend-specific implementations
  - Each backend is self-contained with its own types, validation, and utilities
- **Avoid Circular Imports**: Core modules are backend-agnostic
- **src Layout**: All source code under `src/fastdataframe/`

### Testing Strategy
- **Mirror Source Structure**: `tests/` directory mirrors `src/` structure
- **Parametrized Tests**: Use `@pytest.mark.parametrize` extensively for type variations
- **Test Independence**: Each test must be independent and isolated
- **Coverage Focus**:
  - Type system correctness (nullable, union types, edge cases)
  - Schema validation accuracy
  - Backend-specific functionality
  - Error handling and validation
- **Run Commands**:
  - All tests: `uv run pytest`
  - With coverage: `uv run pytest --cov=src/fastdataframe`
  - Specific module: `uv run pytest tests/core/` or `tests/polars/` or `tests/iceberg/`
- **Philosophy**: Write tests for every new feature before implementation

### Git Workflow
- **Issue-Based Development**:
  - Always create/reference GitHub issue before changes: `gh issue create` or `gh issue list`
  - Every work item must have a corresponding issue number
- **Worktree Workflow**:
  - Create dedicated worktree for each issue: `git worktree add ../<issue-number>-<title-with-hyphens>`
  - Perform all work within the worktree
  - Example: Issue #42 → `git worktree add ../42-add-datetime-validation`
- **Branching**: Work in isolated worktrees, merge to main via pull requests
- **Commits**: Reference issue numbers in commit messages
- **Main Branch**: `main` (use this for PRs)
- **Code Quality**: All checks must pass (mypy, ruff check, ruff format, pytest) before PR

## Domain Context
- **Dataframe Bridge**: The library translates between Pydantic's type system and dataframe libraries' type systems
- **Column Metadata**: `ColumnInfo` annotations provide rich metadata beyond basic types:
  - `unique=True`: Column contains unique values
  - `date_formats`: List of acceptable date format strings
  - `bool_strings`: Mapping of string values to boolean (e.g., `{"yes": True, "no": False}`)
- **Alias Support**: Both validation and serialization aliases are supported via `Model.model_columns(alias_type="serialization" | "validation")`
- **Type Mapping**: Each backend implements its own Python → backend type mapping
- **Schema Validation**: `validate_schema()` returns structured validation errors for mismatches

## Important Constraints
- **Python Version**: Must support Python ≥3.12 (specified in pyproject.toml)
- **Backend Isolation**: Backend implementations must be self-contained and not cross-import
- **Type Safety**: Heavy use of TypeVar and generic types - maintain type safety throughout
- **Pydantic Integration**: Must maintain compatibility with Pydantic ≥2.0 API
- **No Circular Imports**: Core modules cannot depend on backend-specific modules
- **Backward Compatibility**: Changes to core interfaces affect all backends

## External Dependencies
- **Polars**: DataFrame library for high-performance data processing
- **PyIceberg**: Apache Iceberg table format for data lakes
- **Pydantic**: Core dependency for model definitions and validation
- **GitHub**: Issue tracking and pull request workflow (via gh CLI)
- **pytest ecosystem**: Testing framework with plugins (pytest-cov)
- **UV package manager**: Modern Python package and project manager
