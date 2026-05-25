# FastDataFrame

**FastDataFrame** bridges [Pydantic](https://docs.pydantic.dev/) models and dataframe/table backends. A FastDataFrame model owns backend-neutral column definitions, and backend modules expose stateless functions for Polars, PyArrow, and Apache Iceberg.

Supported backends:

- [Polars](https://www.pola.rs/) `DataFrame` and `LazyFrame`
- [PyArrow](https://arrow.apache.org/docs/python/) schemas
- [Apache Iceberg](https://iceberg.apache.org/) schemas/tables through PyIceberg

## Core idea

Define the schema once:

```python
from typing import Annotated

from pydantic import BaseModel, Field

from fastdataframe import ColumnInfo, FastDataFrameModel, Int32


class User(BaseModel):
    user_id: Annotated[
        int,
        Field(validation_alias="userId", serialization_alias="user_id"),
        ColumnInfo(dtype=Int32()),
    ]
    name: str
    score: float = 0.0
    nickname: str | None = None


FastUser = FastDataFrameModel.from_base_model(User)
```

Then generate backend-native schemas with stateless backend functions:

```python
import fastdataframe.polars as fpl
import fastdataframe.pyarrow as farrow
import fastdataframe.iceberg as fice

polars_schema = fpl.schema(FastUser)
arrow_schema = farrow.schema(FastUser)
iceberg_schema = fice.schema(FastUser)
```

## Column definitions

`FastDataFrameModel` owns immutable, backend-neutral column definitions:

```python
FastUser.column_definitions
FastUser.column_map
```

`ColumnInfo` is optional user-authored metadata. Fields without `ColumnInfo` receive default metadata.

```python
class Trade(FastDataFrameModel):
    trade_id: str
    quantity: Annotated[int, ColumnInfo(dtype=Int32(), is_unique=False)]
```

## Name accessors

Resolved names are available as immutable accessors keyed by Python field name:

```python
FastUser.serialization_names.user_id  # "user_id"
FastUser.validation_names.user_id     # "userId"
FastUser.storage_names.user_id        # "user_id"
FastUser.serialization_names["user_id"]
```

The storage name is the canonical dataframe/table column name and defaults to the Pydantic serialization name.

## Dtype refinements

`ColumnInfo(dtype=...)` can refine backend schema generation while the Python annotation remains the semantic type.

Initial backend-neutral scalar dtypes include:

- `Boolean`, `String`, `Binary`
- `Int8`, `Int16`, `Int32`, `Int64`
- `Float32`, `Float64`
- `Date`, `Time`, `Timestamp`
- `Decimal`

Unsigned integer dtypes are intentionally not included initially. Small signed integers are widened when mapped to Iceberg where necessary.

## Polars

```python
import polars as pl
import fastdataframe.polars as fpl

raw = pl.DataFrame({"user_id": ["1"], "name": ["Alice"], "score": ["1.5"], "nickname": [None]})

cast_df = fpl.cast(FastUser, raw)
errors = fpl.validate_schema(FastUser, cast_df)
```

`fpl.string_schema(FastUser)` returns a schema with all columns as strings for ingest flows.

## PyArrow

```python
import fastdataframe.pyarrow as farrow

schema = farrow.schema(FastUser)
string_schema = farrow.string_schema(FastUser)
```

PyArrow schemas encode nullability from `Optional` / `None` unions. Pydantic defaults do not imply nullable storage.

## Iceberg

```python
import fastdataframe.iceberg as fice

schema = fice.schema(FastUser)
```

Iceberg migration support is additive-only by default:

```python
fice.apply_additive_migration(FastUser, table)
```

Destructive deletes are intentionally not automatic.

For Polars-to-Iceberg persistence, data is written through the FastDataFrame-generated PyArrow schema boundary:

```python
fice.append_polars(FastUser, table, cast_df)
```

This is important because Polars schemas do not encode column nullability in the same way as PyArrow and Iceberg.

## Column lifecycle

Deprecated fields remain model fields, remain in schemas, and must be nullable:

```python
class UserV2(FastDataFrameModel):
    old_score: Annotated[float | None, ColumnInfo(deprecated=True)]
    score: float
```

Deprecated and removed column names can be reserved through model config to prevent unsafe reuse:

```python
class UserV3(FastDataFrameModel):
    model_config = {
        "fastdataframe_deprecated_column_names": {"old_score"},
        "fastdataframe_removed_column_names": {"very_old_score"},
    }

    score: float
```

Removed names remain reserved even if a backend later physically deletes the column.

## Installation

```bash
pip install fastdataframe
# or with optional backends
pip install 'fastdataframe[polars,pyarrow,iceberg]'
```

## Development

```bash
uv sync --all-extras
uv run pytest tests/
uv run ruff check .
uv run ruff format .
uv run ty check
```

## License

MIT
