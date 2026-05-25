# PRD: FastDataFrame Core Rewrite

## Problem Statement

FastDataFrame currently bridges Pydantic models to dataframe and lakehouse backends, but the core schema concept is not explicit enough for a full multi-backend design. Backend implementations read raw Pydantic field information directly, duplicate alias/nullability/type logic, and sometimes leak backend-native type concepts into model declarations. This makes it hard to provide a stable API across Polars, PyArrow, and Iceberg, especially for schema validation, Iceberg schema evolution, column lifecycle management, and Polars-to-Iceberg persistence.

Users need a rewrite that makes the Pydantic model declaration the source of truth while introducing a backend-neutral normalized column layer. That layer must support optional column metadata, portable dtype refinements, name accessors, schema validation, and backend-specific functional APIs without coupling the core package to every backend implementation detail.

## Solution

Rewrite FastDataFrame around a backend-neutral `FastDataFrameModel` that owns immutable `ColumnDefinition` objects, analogous to how Pydantic `BaseModel` owns `model_fields`.

Users will define schemas either by subclassing `FastDataFrameModel` directly or by generating one from a plain Pydantic `BaseModel` using `FastDataFrameModel.from_base_model()`. During this model layer, FastDataFrame derives `ColumnDefinition`s from Pydantic fields and optional `ColumnInfo` metadata.

Backend modules will expose stateless functional APIs. For example, Polars support should be exposed through functions such as `fastdataframe.polars.schema(model)`, rather than backend-specific model subclasses. Backend functions consume `ColumnDefinition`s, not raw Pydantic fields.

The rewrite introduces:

- a clear separation between `ColumnInfo` and `ColumnDefinition`
- read-only class-owned column metadata on `FastDataFrameModel`
- read-only name accessors such as `User.serialization_names.user_id`
- a backend-neutral scalar dtype system
- canonical full-schema validation first
- additive-only Iceberg schema migration on day one
- explicit lifecycle support for deprecated and removed columns
- a PyArrow persistence schema boundary for Polars-to-Iceberg writes

## User Stories

1. As a data engineer, I want to define a dataframe schema once using a Pydantic model, so that I can generate backend schemas consistently.
2. As a data engineer, I want to convert a plain Pydantic `BaseModel` into a `FastDataFrameModel`, so that existing domain models can be reused for dataframe schemas.
3. As a library user, I want `from_base_model()` to create a new FastDataFrame model class, so that the original Pydantic model is not mutated.
4. As a library user, I want `from_base_model()` to preserve schema-relevant field information, so that aliases, defaults, annotations, constraints, and column metadata remain available.
5. As a library user, I want `from_base_model()` to be schema-only initially, so that the conversion contract is predictable and not tied to preserving every Pydantic runtime behavior.
6. As a future maintainer, I want Pydantic validator extraction to be treated as a later feature, so that dataframe value validation can be designed separately from schema generation.
7. As a backend implementer, I want every backend to consume `ColumnDefinition`s instead of raw Pydantic `FieldInfo`, so that backend code is simpler and consistent.
8. As a backend implementer, I want `ColumnDefinition` to be backend-neutral, so that it does not contain Polars, PyArrow, or Iceberg native types.
9. As a schema author, I want `ColumnInfo` to be optional, so that ordinary Pydantic fields work without boilerplate.
10. As a schema author, I want `ColumnInfo` to store only user-authored dataframe/lakehouse intent, so that it is not confused with resolved column definitions.
11. As a schema author, I want to declare a backend-neutral `dtype` in `ColumnInfo`, so that I can refine physical/logical representation without importing Polars or PyArrow types into core declarations.
12. As a schema author, I want `dtype` compatibility checked against the Python annotation, so that the Pydantic type remains the semantic type.
13. As a schema author, I want string raw input casting to be handled by backend functions, so that `str` annotations are not misused to represent timestamps or dates.
14. As a schema author, I want signed small integer dtypes supported, so that Polars and PyArrow schemas can use narrower integer widths.
15. As an Iceberg user, I accept small signed integers widening to Iceberg `int`, so that the core dtype set remains useful across backends.
16. As a schema author, I do not need unsigned integers on day one, so that the first dtype system avoids lossy unsigned semantics in Iceberg.
17. As a schema author, I want scalar dtype refinements first, so that containers can continue to be expressed by Python annotations.
18. As a schema author, I want list and struct shapes derived from Python/Pydantic annotations, so that I do not need to duplicate nested schema structure in a dtype DSL.
19. As a developer, I want read-only `column_definitions`, so that schema metadata cannot be accidentally mutated.
20. As a developer, I want read-only `column_map`, so that I can look up column definitions by canonical name.
21. As a developer, I want column definitions built lazily by default, so that class creation remains robust with Pydantic model lifecycle behavior.
22. As a developer, I want an eager validation option, so that applications can fail early at import/startup if ColumnDefinitions are invalid.
23. As a developer, I want eager mode to validate only backend-neutral ColumnDefinitions, so that optional backend dependencies are not required for core validation.
24. As a developer, I want `User.serialization_names.user_id` to return the resolved serialization name, so that column references are centralized and typo-resistant.
25. As a developer, I want `User.validation_names.user_id` to return the resolved validation name, so that ingest and validation flows can reference aliases consistently.
26. As a developer, I want name accessors keyed by Python field name, so that aliases remain values rather than attribute names.
27. As a developer, I want name accessors to support item access as well as attribute access, so that dynamic or unusual model fields can still be addressed.
28. As a developer, I want name accessors to be immutable at runtime, so that generated names remain stable.
29. As a typing-focused user, I want the name accessor design to allow future generated stubs or `Literal` typing, so that stronger static typing can be added later.
30. As a Polars user, I want a stateless function to generate a Polars schema, so that I do not need a backend-specific model subclass.
31. As a Polars user, I want a stateless function to cast a DataFrame or LazyFrame according to a FastDataFrame model, so that raw data can be normalized before validation or persistence.
32. As a Polars user, I want schema validation to compare column names and dtypes, so that incompatible frames are caught early.
33. As a PyArrow user, I want a stateless function to generate a PyArrow schema, so that Arrow tables and Parquet workflows can share the same model contract.
34. As a PyArrow user, I want nullability encoded in the PyArrow schema, so that persistence formats can enforce required and nullable columns.
35. As an Iceberg user, I want a stateless function to generate an Iceberg schema, so that table creation can use the same model contract.
36. As an Iceberg user, I want optional `iceberg_id` metadata, so that field identity can be introduced without requiring IDs for every day-one model.
37. As an Iceberg user, I want additive-only migration on day one, so that schema evolution does not break existing consumers.
38. As an Iceberg user, I want destructive column deletion to be explicit, so that removed columns are not deleted accidentally.
39. As an Iceberg user, I want deprecated column names to remain reserved, so that old column names cannot be reused with new meanings.
40. As an Iceberg user, I want removed column names to remain reserved even after physical deletion, so that schema history remains safe.
41. As a schema author, I want deprecated fields to be nullable, so that producers are not forced to keep supplying meaningful values for discouraged fields.
42. As a schema author, I want deprecated fields to remain in name accessors while they are still fields, so that existing code can still reference them during migration.
43. As a schema author, I want hard-removed columns declared in model config, so that fields can be removed from the model while preserving lifecycle intent.
44. As a schema author, I want Pydantic defaults and dataframe column presence treated separately, so that every model field remains a canonical dataframe column.
45. As a schema author, I want nullability derived from `Optional` / `None` unions, so that default values do not incorrectly imply nullable storage.
46. As a schema validator user, I want initial schema validation to validate the canonical full schema, so that missing model columns are treated as schema errors.
47. As an ingestion pipeline author, I want permissive ingest validation deferred, so that canonical schema behavior is implemented clearly first.
48. As a lakehouse pipeline author, I want Polars-to-Iceberg writes to pass through a FastDataFrame PyArrow schema, so that nullability and storage schema constraints are enforced before Iceberg persistence.
49. As a lakehouse pipeline author, I want Polars schemas not to be treated as the final persistence contract, so that Polars’ lack of schema-level nullability does not hide Iceberg write errors.
50. As a maintainer, I want backend modules to be stateless function namespaces, so that APIs are easy to test and reason about.
51. As a maintainer, I want optional backend dependencies isolated to backend modules, so that core schema extraction does not require Polars, PyArrow, or PyIceberg.
52. As a maintainer, I want a clear out-of-scope list, so that the rewrite does not become a full dataframe validation framework in one step.

## Implementation Decisions

### Core model and column metadata

- `FastDataFrameModel` is the canonical model base for FastDataFrame schemas.
- `FastDataFrameModel.from_base_model()` creates a new backend-neutral FastDataFrame model class from a Pydantic `BaseModel`.
- The generated class should be named using the original model name plus a FastDataFrame suffix.
- `from_base_model()` is schema-only for the initial rewrite. It preserves schema-relevant field declarations but does not promise to preserve all validators, methods, computed fields, serializers, or private attributes.
- Pydantic validator extraction for dataframe value validation is a future design track.
- `ColumnInfo` is optional metadata attached to Pydantic fields.
- `ColumnDefinition` is created and owned by `FastDataFrameModel`, not by backend modules.
- `ColumnDefinition` is immutable and backend-neutral.
- Backend functions consume `ColumnDefinition`s instead of raw Pydantic field information.
- `ColumnDefinition` contains resolved names, Python/Pydantic type information, nullability, requiredness, defaults, constraints, and `ColumnInfo`.
- `ColumnDefinition` must not contain backend-native Polars, PyArrow, or Iceberg types.

### ColumnDefinition lifecycle

- `column_definitions` is exposed as a read-only class-owned property.
- `column_map` is exposed as a read-only class-owned property.
- ColumnDefinitions are built lazily by default.
- Eager mode is opt-in.
- Eager mode validates only backend-neutral ColumnDefinitions and does not require backend packages.
- Invalid ColumnDefinitions should fail clearly when built lazily or eagerly.

### Name accessors

- FastDataFrame models expose name accessors derived from ColumnDefinitions.
- Required accessors include serialization names and validation names.
- Storage names should also be represented, with storage name defaulting to serialization name.
- Accessors are keyed by Python field name.
- Accessors support attribute access for normal Python field names.
- Accessors support item access for all fields.
- Accessors are immutable at runtime.
- Initial typing may return `str`; the design should not block future generated stubs or `Literal` return types.
- Name accessors exist only on `FastDataFrameModel` classes, including those generated by `from_base_model()`.

### Column naming

- The canonical dataframe/table column name is the storage name.
- The storage name defaults to Pydantic’s serialization name.
- Validation names remain available for ingest/validation workflows.
- Python field names remain the stable developer-facing handles.

### Dtype system

- `ColumnInfo` includes an optional `dtype` field.
- `dtype` is a backend-neutral scalar logical type refinement.
- `dtype` must be compatible with the Python annotation.
- Python annotations remain the semantic validation type.
- Backend functions map `dtype` first and fall back to Python annotations when no dtype is declared.
- Initial dtypes are scalar refinements only.
- Container shapes are derived from Python/Pydantic annotations.
- Initial dtype coverage includes booleans, strings, binary data, signed integers, floats, dates, times, timestamps, and decimals.
- Signed small integers are supported in the core dtype set.
- Iceberg may widen small signed integers to its available integer type.
- Unsigned integers are out of scope initially.
- Backend-native dtypes are not part of core `ColumnInfo`.

### Nullability, requiredness, and column presence

- Column presence and Pydantic required input are separate concepts.
- Every model field is a canonical dataframe/table column by default, regardless of Pydantic defaults.
- Nullability is derived from `Optional` / `None` union types.
- Defaults do not make a column nullable.
- Initial schema validation validates the canonical full schema.
- Permissive ingest validation for missing defaultable/nullable fields is deferred.

### Backend API shape

- Backend modules expose stateless functions.
- Backend-specific model subclasses are not the primary API.
- Core remains backend-neutral.
- Optional backend dependencies remain isolated to backend modules.
- Polars support should expose schema generation, string schema generation, schema validation, and casting functions.
- PyArrow support should expose schema generation and string schema generation.
- Iceberg support should expose schema generation and additive-only migration functions.

### Schema validation

- Initial validation scope is schema validation only.
- Schema validation does not apply Pydantic field/model validators.
- Schema validation validates the canonical full dataframe/table schema.
- Polars schema validation can validate names and dtypes but cannot prove schema-level nullability.
- PyArrow and Iceberg schema validation can include nullability.

### Column lifecycle

- Active fields are normal model fields included in generated schemas.
- Deprecated fields remain model fields and remain in generated schemas.
- Deprecated fields must be nullable.
- Deprecated fields remain available through name accessors.
- Deprecated column names are removed from the model but reserved in model config.
- Deprecated column names are not included in generated canonical schemas.
- Deprecated column names are not physically deleted by default.
- Removed column names are eligible for explicit destructive deletion from backends.
- Removed column names remain reserved and must not be reused.

### Iceberg schema evolution

- Day-one Iceberg migration is additive-only.
- Additive migration can create or add missing columns but should not rename or delete columns automatically.
- Destructive deletion must be an explicit operation.
- `ColumnInfo` includes optional Iceberg field identity metadata.
- Iceberg field identity is optional day one.
- If explicit field identity is present, Iceberg schema generation should use it.
- If explicit field identity is absent, schema generation may assign deterministic IDs for new schema objects.
- Strict field-ID modes can be added later.
- The design is informed by the additive-only schema evolution pattern used in the Hawk codebase.

### Polars-to-Iceberg persistence

- Polars schemas are not the final persistence contract for Iceberg writes because they do not encode nullability like PyArrow/Iceberg.
- Any Polars-to-Iceberg write helper should pass through the FastDataFrame-generated PyArrow schema.
- The persistence flow should be: Polars DataFrame -> FastDataFrame Polars normalization -> PyArrow Table cast to FastDataFrame PyArrow schema -> Iceberg write.
- Data-level null checks for non-nullable columns must happen before or during the PyArrow/Iceberg persistence boundary.

## Testing Decisions

- Tests should focus on public behavior rather than implementation details.
- Core tests should verify that `FastDataFrameModel` owns immutable ColumnDefinitions.
- Core tests should verify `from_base_model()` schema-only conversion behavior.
- Core tests should verify optional `ColumnInfo` defaulting.
- Core tests should verify dtype compatibility with Python annotations.
- Core tests should verify nullable/default/required distinctions.
- Core tests should verify deprecated field and reserved/removed column rules.
- Core tests should verify name accessors using both attribute and item access.
- Polars tests should verify schema generation from ColumnDefinitions.
- Polars tests should verify string schema generation.
- Polars tests should verify casting behavior for supported scalar types.
- Polars tests should verify schema validation against the canonical full schema.
- PyArrow tests should verify schema generation including nullability.
- PyArrow tests should verify scalar dtype mappings.
- PyArrow tests should verify schemas used as the persistence boundary.
- Iceberg tests should verify schema generation from ColumnDefinitions.
- Iceberg tests should verify signed integer widening behavior.
- Iceberg tests should verify optional field identity handling.
- Iceberg tests should verify additive-only migration planning/apply behavior.
- Iceberg tests should verify deprecated/removed column lifecycle behavior.
- End-to-end tests should verify Pydantic/FastDataFrame -> Polars -> PyArrow -> Iceberg flows.
- Existing tests in `tests/core`, `tests/polars`, `tests/pyarrow`, `tests/iceberg`, and `tests/e2e` provide prior art and should be reorganized or rewritten around the new public APIs.
- Compatibility tests should ensure backend modules do not read raw Pydantic field metadata directly where ColumnDefinitions are available.

## Out of Scope

- Applying Pydantic field/model validators to dataframe values.
- Full row-level or cell-level dataframe validation using Pydantic validators.
- Generated `.pyi` stubs or static `Literal` typing for name accessors.
- Type-checker plugins.
- Unsigned integer dtypes.
- Backend-specific dtype extensions.
- A full dtype DSL for containers.
- Permissive ingest schema validation mode.
- Automatic destructive Iceberg migrations.
- Automatic column renames in Iceberg.
- Guaranteeing exact round-trip preservation of small integer dtype through Iceberg schema introspection.
- Preserving all behavioral aspects of source Pydantic models in `from_base_model()`.

## Further Notes

- The rewrite should keep core concepts precise:
  - `ColumnInfo` is user-authored metadata.
  - `ColumnDefinition` is resolved backend-neutral schema state owned by `FastDataFrameModel`.
  - backend schemas are native schema objects produced from ColumnDefinitions.
- The glossary in `CONTEXT.md` should remain synchronized with implementation decisions as the rewrite proceeds.
- If Iceberg schema evolution behavior becomes more sophisticated than additive-only, an ADR should likely be created because it is hard to reverse, surprising without context, and involves real trade-offs.
- If Pydantic validator extraction becomes part of a later milestone, it should receive its own PRD or design document because it introduces performance, semantics, and error-reporting questions beyond schema translation.
