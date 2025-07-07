# FastDataFrame

**FastDataFrame** is a modern Python library that bridges the world of [Pydantic](https://docs.pydantic.dev/) models and dataframe libraries, providing a standard interface for validating, transforming, and creating dataframes from Pydantic models. It currently supports:

- [Polars](https://www.pola.rs/) DataFrame and LazyFrame
- [Apache Iceberg](https://iceberg.apache.org/) tables

The goal is to make it easy to:
- Validate a dataframe against a Pydantic model schema
- Convert a dataframe to an iterable of Pydantic models
- Create a dataframe from a list of Pydantic models, with correct types

---

## Features

- **Schema Validation:** Ensure your dataframe matches your Pydantic model's schema.
- **Model Conversion:** Easily convert between dataframes and Pydantic models.
- **Type-Safe DataFrames:** Automatically infer and enforce correct column types.
- **Multi-Backend:** Works with Polars and Iceberg (with plans for Pandas, PyArrow, and more).

---

## Polars Integration

### 1. Define Your Pydantic Model

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    age: int
```

### 2. Create a Polars DataFrame from Models

```python
import polars as pl
from fastdataframe.polars.model import PolarsFastDataframeModel

# Create a Polars-compatible model
PolarsUser = PolarsFastDataframeModel.from_base_model(User)

# List of Pydantic models
users = [
    User(id=1, name="Alice", age=30),
    User(id=2, name="Bob", age=25),
]

# Create a DataFrame
df = pl.DataFrame([user.model_dump() for user in users])

# Cast DataFrame to match the model schema (enforces types)
df = PolarsUser.cast_to_model_schema(df)
print(df)
```

### 3. Validate a DataFrame Against the Model

```python
errors = PolarsUser.validate_schema(df)
if errors:
    print("Validation errors:", errors)
else:
    print("DataFrame is valid!")
```

### 4. Convert DataFrame Rows to Pydantic Models

```python
models = [PolarsUser(**row) for row in df.to_dicts()]
print(models)
```

---

## Iceberg Integration

### 1. Define Your Pydantic Model

```python
from pydantic import BaseModel

class Transaction(BaseModel):
    transaction_id: str
    amount: float
    timestamp: str
```

### 2. Generate Iceberg Schema from Model

```python
from fastdataframe.iceberg.model import IcebergFastDataframeModel

IcebergTransaction = IcebergFastDataframeModel.from_base_model(Transaction)
iceberg_schema = IcebergTransaction.get_iceberg_schema()
print(iceberg_schema)
```

### 3. Validate Iceberg Table Schema

```python
# Suppose you have an Iceberg table schema as a dict
table_schema = {
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "timestamp", "type": "string"},
    ]
}

errors = IcebergTransaction.validate_schema(table_schema)
if errors:
    print("Schema validation errors:", errors)
else:
    print("Iceberg schema is valid!")
```

---

## Why FastDataFrame?

- **Type Safety:** Enforce your data contracts at the dataframe level.
- **Consistency:** Use the same Pydantic models for both API and data processing.
- **Extensible:** Designed to support multiple dataframe backends.

---

## Installation

```bash
uv pip install fastdataframe
# or
pip install fastdataframe
```

---

## Roadmap

- [x] Polars support
- [x] Iceberg support
- [ ] Pandas support
- [ ] PyArrow support
- [ ] PyIceberg support

---

## License

MIT
