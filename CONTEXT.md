# FastDataFrame

FastDataFrame defines dataframe schemas from Pydantic models and translates them into backend-specific schemas for dataframe processing and lakehouse storage.

## Language

**Pydantic Model**:
The user-authored data contract that declares fields, Python types, aliases, and validation behavior. It is the public declaration surface for a dataframe schema.
_Avoid_: Base schema, source class

**ColumnInfo**:
User-authored column metadata attached to a Pydantic field for dataframe and lakehouse concerns that are not part of the Python type itself. It contains user intent, including optional backend-neutral dtype refinements, not resolved backend-native types.
_Avoid_: Column definition, field definition

**Dtype**:
A backend-neutral logical column type declared in ColumnInfo to refine the Python annotation for dataframe and table schema generation. Initial Dtypes are scalar refinements; container shapes are derived from Python annotations. A Dtype must be compatible with the field's Python annotation and mappable to each supported backend, though some backends may widen representation such as small signed integers in Iceberg.
_Avoid_: Strict type, Polars type, Arrow type, Iceberg type when referring to the backend-neutral concept

**ColumnDefinition**:
The backend-neutral, immutable, normalized representation of one dataframe column owned by a FastDataFrameModel class, analogous to how Pydantic BaseModel owns model fields. It is derived from a Pydantic field and optional ColumnInfo metadata; backend implementations consume ColumnDefinitions rather than raw Pydantic field objects. ColumnDefinitions include resolved names, Python/Pydantic type information, nullability, requiredness, defaults, constraints, and ColumnInfo; they do not contain backend-native types. ColumnDefinitions are built lazily by default and may be built eagerly only for backend-neutral validation.
_Avoid_: ColumnInfo, FieldInfo, backend column

**Storage Name**:
The canonical dataframe and table column name for a ColumnDefinition. It defaults to the Pydantic serialization name.
_Avoid_: Validation name, Python name when referring to persisted dataframe/table columns

**Name Accessor**:
A read-only class-owned helper that exposes resolved column names as attributes, such as `User.serialization_names.user_id` or `User.validation_names.user_id`.
_Avoid_: Enum, constants class when the values are derived from ColumnDefinitions

**Deprecated Field**:
A field that remains in the FastDataFrameModel and backend schema but is marked as discouraged for new use. Deprecated Fields must be nullable and remain available through name accessors while they are still model fields.
_Avoid_: Removed column when the field is still present in the model

**Deprecated Column Name**:
A column name that has been removed from the FastDataFrameModel but remains reserved to prevent reuse and protect existing consumers. It is not part of generated canonical schemas and is not physically deleted by default.
_Avoid_: Deprecated Field when the field is no longer present in the model

**Removed Column Name**:
A previously used column name that is eligible for explicit destructive deletion from a backend schema. Removed Column Names remain reserved and must not be reused.
_Avoid_: Deprecated Column Name when physical deletion is intended

**Backend Schema**:
A schema object native to a dataframe or table backend, such as a Polars, PyArrow, or Iceberg schema.
_Avoid_: Model schema when referring to backend-native schemas

**Persistence Schema Boundary**:
The PyArrow schema boundary used before writing dataframe data into Iceberg. Polars data destined for Iceberg is converted through the FastDataFrame-generated PyArrow schema because Polars schemas do not encode column nullability in the same way as PyArrow and Iceberg.
_Avoid_: Polars schema as the final persistence contract for Iceberg writes

**Backend Function**:
A stateless function in a backend namespace that consumes a FastDataFrame model or ColumnDefinitions and returns a backend-specific result.
_Avoid_: Backend adapter, backend service, backend model when no state is involved

## Example dialogue

Developer: “Should the Polars backend inspect the Pydantic field directly?”
Domain expert: “No. The Pydantic Model is the declaration surface, but each backend should consume the normalized ColumnDefinition.”

Developer: “Where do we put lakehouse metadata like deprecation or Iceberg field identity?”
Domain expert: “That belongs in ColumnInfo, then becomes part of the derived ColumnDefinition.”
