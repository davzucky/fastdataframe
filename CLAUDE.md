# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Package Management (UV)
- **Install dependencies**: `uv sync` (installs from uv.lock)
- **Add dependency**: `uv add <package>` 
- **Add dev dependency**: `uv add --group dev <package>`
- **Update dependencies**: `uv update` then `uv lock`
- **Install specific groups**: `uv sync --no-group dev --no-group lint`

### Testing
- **Run all tests**: `uv run pytest`
- **Run specific test**: `uv run pytest tests/path/to/test_file.py::test_function`
- **Run with coverage**: `uv run pytest --cov=src/fastdataframe`
- **Test specific module**: `uv run pytest tests/core/` or `uv run pytest tests/polars/` or `uv run pytest tests/iceberg/`

### Code Quality
- **Type checking**: `uv run mypy src/`
- **Linting**: `uv run ruff check src/ tests/`
- **Format code**: `uv run ruff format src/ tests/`
- **Run all quality checks**: Run mypy, ruff check, and ruff format in sequence

### Python Version
- Project requires Python ≥3.12 (specified in pyproject.toml)
- Use `uv python install 3.12` to install if needed

## Architecture Overview

### Core Design Pattern
FastDataFrame implements a **bridge pattern** between Pydantic models and dataframe libraries. The architecture follows these principles:

1. **Base Abstraction**: `FastDataframeModel` provides the core interface
2. **Dataframe-Specific Implementations**: `PolarsFastDataframeModel`, `IcebergFastDataframeModel` extend the base
3. **Schema Translation**: Each implementation handles dataframe-specific schema validation and type conversion

### Key Components

#### Core Module (`src/fastdataframe/core/`)
- **`model.py`**: Base `FastDataframeModel` class with `from_base_model()` factory method
- **`annotation.py`**: `ColumnInfo` class for column metadata (uniqueness, date formats, boolean mappings)
- **`types.py`**: Type system mappings and utilities
- **`validation.py`**: Core validation logic and error handling
- **`json_schema.py`**: JSON schema validation utilities

#### Backend Implementations
- **`polars/`**: Polars DataFrame/LazyFrame integration with casting and schema validation
- **`iceberg/`**: Apache Iceberg table schema generation and validation

#### Type System
- Uses `ColumnInfo` annotations for rich metadata (uniqueness constraints, date formats, boolean string mappings)
- Handles Optional types, Union types, and complex nested structures
- Maps Python types to backend-specific types (Polars dtypes, Iceberg schema types)

### Key Patterns

#### Model Creation Pattern
```python
# Convert any Pydantic model to dataframe-specific model
BaseModel -> FastDataframeModel.from_base_model() -> BackendSpecificModel
```

#### Schema Validation Pattern
```python
# Each backend implements validate_schema()
errors = BackendModel.validate_schema(dataframe)
if errors:
    handle_validation_errors(errors)
```

#### Column Information Pattern
```python
# Extract column metadata with alias support
columns = Model.model_columns(alias_type="serialization")  # or "validation"
```

## Development Guidelines

### Adding New Backend Support
1. Create new module under `src/fastdataframe/{backend}/`
2. Implement `{Backend}FastDataframeModel` extending `FastDataframeModel`
3. Implement required methods: `validate_schema()`, type mapping functions
4. Add type conversion utilities in `_types.py`
5. Create comprehensive tests in `tests/{backend}/`

### Writing Tests
- **Structure**: Mirror source structure in `tests/` directory
- **Parametrized tests**: Use `@pytest.mark.parametrize` for type variations (see test_model.py)
- **Test isolation**: Each test should be independent
- **Test data**: Use representative data that covers edge cases (nullable types, union types, etc.)

### Code Organization
- Follow **src layout** with code in `src/fastdataframe/`
- Use **typed interfaces** - all functions should have type hints
- **Avoid circular imports** - core modules should not import from backend-specific modules
- **Backend isolation** - each backend implementation should be self-contained

### Key Implementation Notes
- **Type safety**: Heavy use of TypeVar and generic types
- **Pydantic integration**: Leverages Pydantic's `create_model()` for dynamic model creation
- **Alias support**: Both validation and serialization aliases are supported
- **Error handling**: Uses custom ValidationError class for consistent error reporting
- **Schema generation**: Each backend generates its own schema format from Pydantic models

### Dependencies
- **Core**: pydantic ≥2.0, annotated-types
- **Polars backend**: polars ≥1.26.0
- **Iceberg backend**: pyiceberg ≥0.9
- **Development**: pytest, mypy, ruff, pytest-cov

### Testing Philosophy  
Write tests for every new feature. The codebase uses parametrized testing extensively to cover type variations and edge cases. Focus on:
- Type system correctness (nullable, union types)
- Schema validation accuracy
- Backend-specific functionality
- Error handling and validation