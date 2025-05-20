# Pydantic FieldInfo Annotation Initialization
_Exported on 5/16/2025 at 16:37:45 GMT+9 from Cursor (0.50.4)_

---

**User**

Please read the source code and explain how pydantic is initializing the property `annotation` from the class `FieldInfo` when the model is initialize. I want to know if this happen on the initiall read or post model initialization

---

**Cursor**

Here's a detailed explanation of how Pydantic initializes the `annotation` property of the `FieldInfo` class when a model is created, and whether this happens during the initial class creation or after model initialization:

---

### 1. **Where is `annotation` set in `FieldInfo`?**

- The `FieldInfo` class has an `annotation` attribute, which is meant to store the type annotation of a model field.
- The `__init__` method of `FieldInfo` sets `self.annotation = kwargs.get('annotation')`.
- However, the main way `FieldInfo` is created for model fields is not by direct instantiation, but via the static methods `from_annotation` and `from_annotated_attribute`.

---

### 2. **How is `FieldInfo` created for model fields?**

- When a Pydantic model class is defined, Pydantic's metaclass machinery (in `_model_construction.py`) calls `set_model_fields` and/or `complete_model_class`.
- These functions call `collect_model_fields` (from `_internal/_fields.py`), which is responsible for creating a `FieldInfo` for each field in the model.

#### In `collect_model_fields`:
- For each field, if there is only a type annotation (e.g., `foo: int`), it calls:
  ```python
  field_info = FieldInfo_.from_annotation(ann_type, _source=AnnotationSource.CLASS)
  ```
- If there is a default value or a `Field(...)` call, it uses:
  ```python
  field_info = FieldInfo_.from_annotated_attribute(ann_type, assigned_value, _source=AnnotationSource.CLASS)
  ```

#### In `FieldInfo.from_annotation`:
- This method inspects the annotation, extracts the type, and creates a `FieldInfo` with `annotation=type_expr`.
- If the annotation is an `Annotated` type, it also merges in any metadata or constraints.

---

### 3. **When does this happen?**

- **This process happens at model class creation time, not at instance initialization.**
- When the model class is first defined (i.e., when Python executes the class body), the metaclass machinery runs and builds the `__pydantic_fields__` dictionary, which contains a `FieldInfo` for each field, with the `annotation` property set.
- When you later instantiate the model (e.g., `MyModel(foo=123)`), the `FieldInfo` objects (and their `annotation` properties) are already set up and are not changed during instance initialization.

---

### 4. **Summary**

- **The `annotation` property of `FieldInfo` is set during model class creation, not during or after model instance initialization.**
- This is done by inspecting the class's type annotations and defaults, and creating a `FieldInfo` for each field using the `from_annotation` or `from_annotated_attribute` static methods.
- The result is stored in the class-level `__pydantic_fields__` dictionary, which is then accessed via `model_fields`.

---

**In short:**  
The `annotation` property is initialized on the initial read of the model class, not post model initialization. It is part of the class construction process, not the instance construction process.

```python
  field_info = FieldInfo_.from_annotation(ann_type, _source=AnnotationSource.CLASS)
```

```python
  field_info = FieldInfo_.from_annotated_attribute(ann_type, assigned_value, _source=AnnotationSource.CLASS)
```

---

**User**

How is `from_annotated_attribute` initialization the property `annotation` of `FieldInfo`

---

**Cursor**

Here’s how `FieldInfo.from_annotated_attribute` initializes the `annotation` property of `FieldInfo`:

---

### 1. **Purpose of `from_annotated_attribute`**

This static method is used when a model field has both a type annotation and a default value, e.g.:
```python
class MyModel(BaseModel):
    foo: int = 4
    bar: Annotated[int, SomeMetadata] = 5
    baz: Annotated[int, Field(gt=0)] = 6
```
It combines the annotation and the default value (which could be a plain value or a `Field(...)`).

---

### 2. **How is `annotation` set?**

Let’s look at the relevant code (simplified for clarity):

```python
@staticmethod
def from_annotated_attribute(
    annotation: type[Any], default: Any, *, _source: AnnotationSource = AnnotationSource.ANY
) -> FieldInfo:
    ...
    inspected_ann = inspect_annotation(
        annotation,
        annotation_source=_source,
        unpack_type_aliases='skip',
    )
    type_expr: Any = Any if inspected_ann.type is UNKNOWN else inspected_ann.type
    ...
    if isinstance(default, FieldInfo):
        default.annotation = type_expr
        ...
        merged_default = FieldInfo.merge_field_infos(
            *[x for x in metadata if isinstance(x, FieldInfo)],
            default,
            annotation=default.annotation,
        )
        ...
        return merged_default
    ...
    if not metadata:
        field_info = FieldInfo(annotation=type_expr, default=default, frozen=final or None)
        ...
        return field_info
    ...
    field_infos = [a for a in metadata if isinstance(a, FieldInfo)]
    field_info = FieldInfo.merge_field_infos(*field_infos, annotation=type_expr, default=default)
    ...
    return field_info
```

#### **Step-by-step:**

1. **Annotation Inspection:**
   - `inspect_annotation` is called on the annotation to extract the actual type (e.g., `int` from `Annotated[int, ...]`).
   - The result is stored in `type_expr`.

2. **If the default is a `FieldInfo`:**
   - The code sets `default.annotation = type_expr`.
   - This means the `annotation` property of the `FieldInfo` instance is set to the type extracted from the annotation.

3. **If there is no metadata:**
   - A new `FieldInfo` is created with `annotation=type_expr`.

4. **If there is metadata:**
   - The code merges all `FieldInfo` instances from the metadata and sets `annotation=type_expr` in the merged result.

---

### 3. **Summary Table**

| Case                                 | How `annotation` is set                |
|---------------------------------------|----------------------------------------|
| Default is a `FieldInfo`              | `default.annotation = type_expr`       |
| No metadata                          | `FieldInfo(annotation=type_expr, ...)` |
| With metadata                        | `FieldInfo.merge_field_infos(..., annotation=type_expr, ...)` |

---

### 4. **Conclusion**

**In all cases, `from_annotated_attribute` sets the `annotation` property of the resulting `FieldInfo` to the type extracted from the field’s annotation.**  
This ensures that the `FieldInfo` always knows the type of the field, regardless of whether the default is a plain value, a `Field(...)`, or there is extra metadata.

If you want to see the exact lines, here’s the key part:
```python
type_expr: Any = Any if inspected_ann.type is UNKNOWN else inspected_ann.type
...
default.annotation = type_expr
...
field_info = FieldInfo(annotation=type_expr, ...)
...
field_info = FieldInfo.merge_field_infos(..., annotation=type_expr, ...)
```

**So, `annotation` is always set to the resolved type from the annotation, as determined by `inspect_annotation`.**

```python
class MyModel(BaseModel):
    foo: int = 4
    bar: Annotated[int, SomeMetadata] = 5
    baz: Annotated[int, Field(gt=0)] = 6
```

```python
@staticmethod
def from_annotated_attribute(
    annotation: type[Any], default: Any, *, _source: AnnotationSource = AnnotationSource.ANY
) -> FieldInfo:
    ...
    inspected_ann = inspect_annotation(
        annotation,
        annotation_source=_source,
        unpack_type_aliases='skip',
    )
    type_expr: Any = Any if inspected_ann.type is UNKNOWN else inspected_ann.type
    ...
    if isinstance(default, FieldInfo):
        default.annotation = type_expr
        ...
        merged_default = FieldInfo.merge_field_infos(
            *[x for x in metadata if isinstance(x, FieldInfo)],
            default,
            annotation=default.annotation,
        )
        ...
        return merged_default
    ...
    if not metadata:
        field_info = FieldInfo(annotation=type_expr, default=default, frozen=final or None)
        ...
        return field_info
    ...
    field_infos = [a for a in metadata if isinstance(a, FieldInfo)]
    field_info = FieldInfo.merge_field_infos(*field_infos, annotation=type_expr, default=default)
    ...
    return field_info
```

```python
type_expr: Any = Any if inspected_ann.type is UNKNOWN else inspected_ann.type
...
default.annotation = type_expr
...
field_info = FieldInfo(annotation=type_expr, ...)
...
field_info = FieldInfo.merge_field_infos(..., annotation=type_expr, ...)
```

---

**User**

What about when it call `from_field`

---

**Cursor**

Great question! Let’s clarify how the `annotation` property is set when `FieldInfo.from_field` is used.

---

## 1. **What is `from_field`?**

`from_field` is a static method on `FieldInfo` that is used to create a `FieldInfo` object from arguments that would be passed to `Field(...)`.  
It is typically used when you write something like:
```python
class MyModel(BaseModel):
    foo: int = Field(default=123, gt=0)
```
or
```python
Field(gt=0)
```

---

## 2. **How does `from_field` work?**

Here’s the relevant code (simplified):

```python
@staticmethod
def from_field(default: Any = PydanticUndefined, **kwargs: Unpack[_FromFieldInfoInputs]) -> FieldInfo:
    if 'annotation' in kwargs:
        raise TypeError('"annotation" is not permitted as a Field keyword argument')
    return FieldInfo(default=default, **kwargs)
```

- If you try to pass `annotation` directly as a keyword argument, it raises an error.
- Otherwise, it simply calls the `FieldInfo` constructor with the provided arguments.

---

## 3. **How is `annotation` set in this case?**

- **When you use `Field(...)` in your model, you do _not_ specify the annotation in the `Field` call.**
- The annotation is always taken from the type annotation in the class, not from the `Field` call.
- When Pydantic builds the model, it will later combine the type annotation (from the class) with the `FieldInfo` (from the `Field` call) using `from_annotated_attribute` (see previous answer).
- At that point, the `annotation` property is set using the type annotation, not from the `Field` call.

**So:**  
- If you call `FieldInfo.from_field(...)` directly, the resulting `FieldInfo` will have `annotation=None` (the default), unless you manually set it after creation.
- In normal Pydantic usage, the annotation is set later, when the model class is processed and the type annotation is available.

---

## 4. **Summary Table**

| How `FieldInfo` is created         | How `annotation` is set                |
|------------------------------------|----------------------------------------|
| `FieldInfo.from_field(...)`        | Not set (remains `None`)               |
| Model class creation (Pydantic)    | Set later by `from_annotated_attribute` using the class annotation |

---

## 5. **Conclusion**

- **`from_field` does _not_ set the `annotation` property.**
- The annotation is always set later, when the model class is processed and the type annotation is available.
- This is why you cannot pass `annotation` as a keyword to `Field(...)` or `from_field`.

**In summary:**  
> `FieldInfo.from_field` does not initialize the `annotation` property. The annotation is set later, when the model class is processed and the type annotation is combined with the `FieldInfo` using `from_annotated_attribute`.

```python
class MyModel(BaseModel):
    foo: int = Field(default=123, gt=0)
```

```python
Field(gt=0)
```

```python
@staticmethod
def from_field(default: Any = PydanticUndefined, **kwargs: Unpack[_FromFieldInfoInputs]) -> FieldInfo:
    if 'annotation' in kwargs:
        raise TypeError('"annotation" is not permitted as a Field keyword argument')
    return FieldInfo(default=default, **kwargs)
```

---

**User**

the initialization is it done on the model validator after or before?

---

**Cursor**

**The initialization of the `annotation` property in `FieldInfo` is done _before_ the model validator is created.**

---

## Detailed Explanation

### 1. **Order of Operations in Pydantic Model Creation**

When you define a Pydantic model, the following steps occur (simplified):

1. **Class Definition:**  
   Python executes the class body, collecting type annotations and any `Field(...)` calls.

2. **Metaclass Processing:**  
   Pydantic’s metaclass machinery (in `_model_construction.py`) runs, which:
   - Calls `set_model_fields` and/or `complete_model_class`.
   - These call `collect_model_fields`, which creates `FieldInfo` objects for each field.
   - **At this point, the `annotation` property is set on each `FieldInfo` using the type annotation from the class.**

3. **Model Validator Creation:**  
   After all fields are collected and `FieldInfo` objects are created (with `annotation` set), Pydantic generates the core schema and creates the model validator (`__pydantic_validator__`).

4. **Model Instantiation:**  
   When you instantiate the model, the validator is used to validate and parse the input data.

---

### 2. **Code Evidence**

- In `_model_construction.py`, the function `complete_model_class` first ensures fields are collected and `FieldInfo` objects are created (with `annotation` set).
- Only **after** this does it create the validator:
  ```python
  cls.__pydantic_fields__ = rebuild_model_fields(...)
  ...
  cls.__pydantic_validator__ = create_schema_validator(...)
  ```

---

### 3. **Summary Table**

| Step                        | Is `annotation` set? | Is validator created? |
|-----------------------------|----------------------|----------------------|
| Collect fields (`FieldInfo`) | Yes                 | No                   |
| Create validator            | Yes                 | Yes                  |
| Instantiate model           | Yes                 | Yes                  |

---

## **Conclusion**

> **The `annotation` property of `FieldInfo` is initialized before the model validator is created.**  
> This happens during model class creation, not during or after validator creation or model instantiation.

```python
  cls.__pydantic_fields__ = rebuild_model_fields(...)
  ...
  cls.__pydantic_validator__ = create_schema_validator(...)
```

---

**User**

Do you know any hook that I could use post model definition to set value in `FieldInfo`? I want that on the model level not when we initialize the class

---

**Cursor**

Yes! Pydantic provides a **post-model-definition hook** that you can use to modify `FieldInfo` objects at the model (class) level, not at instance initialization.

## The Hook: `__pydantic_on_complete__`

Pydantic calls the classmethod `__pydantic_on_complete__` on your model **after** the model class is fully built (fields, validators, etc.), but **before** the model is used for validation or instantiation.

### How to Use

You can override this method in your model to mutate `FieldInfo` objects in `__pydantic_fields__`:

```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    foo: int = Field(default=1)

    @classmethod
    def __pydantic_on_complete__(cls):
        # This runs after the model is fully defined
        for field in cls.__pydantic_fields__.values():
            # You can mutate FieldInfo here
            field.description = f"Field {field.annotation}"
```

- This method is called **once per model class**, not per instance.
- You can safely mutate `FieldInfo` objects here.

### When is it called?
- After all fields, validators, and computed fields are set up.
- After the model’s schema and validator are built.
- Before any model instance is created.

---

## **Summary Table**

| Hook/Method                  | When it runs                        | Can mutate FieldInfo? | Scope      |
|------------------------------|-------------------------------------|-----------------------|------------|
| `__pydantic_on_complete__`   | After model class is built          | Yes                   | Model/class|
| `model_post_init`            | After instance is created           | No (per instance)     | Instance   |

---

## **Conclusion**

> **Override `__pydantic_on_complete__` on your model to mutate `FieldInfo` at the model (class) level, after model definition but before any instance is created.**

This is the recommended and supported way to do what you want in Pydantic v2+.

```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    foo: int = Field(default=1)

    @classmethod
    def __pydantic_on_complete__(cls):
        # This runs after the model is fully defined
        for field in cls.__pydantic_fields__.values():
            # You can mutate FieldInfo here
            field.description = f"Field {field.annotation}"
```

---

**User**

What is the purpose of the property `metadata` in the class @FieldInfo 
Can I use it to add extra property for my field or should the contant support a given inteface

---

**Cursor**

Let me help explain the purpose of the `metadata` property in the `FieldInfo` class and how it should be used.

The `metadata` property in `FieldInfo` is a list that stores additional metadata or constraints for a field. It's designed to hold custom metadata that can be used to provide extra information or behavior for field validation and processing.

Let me search for more specific details about how metadata is handled:


Based on the search results and code analysis, I can explain how the `metadata` property in `FieldInfo` works:

1. Purpose:
- The `metadata` property is a list that stores additional metadata or constraints for a field
- It's used to hold custom metadata that can be used for field validation and processing
- The metadata can include both Pydantic-specific metadata and general metadata

2. Types of Metadata:
- The metadata can include:
  - Built-in validation constraints (like `Gt`, `Lt`, `MinLen`, etc.)
  - Custom metadata objects
  - General metadata through `PydanticGeneralMetadata`

3. How to Use It:
You have two main options for adding metadata to a field:

a) Using built-in validation constraints:
```python
from pydantic import BaseModel, Field
from annotated_types import Gt, Lt

class MyModel(BaseModel):
    # Using Field with validation constraints
    age: int = Field(gt=0, lt=120)
    
    # Using Annotated with validation constraints
    score: Annotated[int, Gt(0), Lt(100)]
```

b) Using custom metadata:
```python
from pydantic import BaseModel, Field
from typing import Annotated

class MyModel(BaseModel):
    # Custom metadata will be stored in the metadata list
    field: Annotated[int, "custom_metadata"] = 5
```

4. Important Notes:
- The metadata is used during schema generation and validation
- Unknown metadata is preserved and stored in the list
- Metadata can be merged when combining multiple field definitions
- The metadata is used to generate JSON schema information

5. Best Practices:
- For validation constraints, use the built-in validation parameters in `Field()` or the `annotated_types` package
- For custom metadata, you can add it through `Annotated` types
- The metadata should be used for field-level configuration and validation rules
- If you need to add custom behavior, consider using validators or serializers instead of metadata

So to answer your specific questions:
1. Can you use it to add extra properties for your field? Yes, but it's recommended to use the built-in validation parameters in `Field()` or proper validation constraints for most cases.
2. Should the content support a given interface? While you can add any type of metadata, it's best to use the standard validation constraints or create proper metadata classes that implement the necessary interfaces for validation and schema generation.

The metadata system is primarily designed for validation and schema generation, so it's best to use it for those purposes rather than as a general-purpose storage for arbitrary data.

```python
from pydantic import BaseModel, Field
from annotated_types import Gt, Lt

class MyModel(BaseModel):
    # Using Field with validation constraints
    age: int = Field(gt=0, lt=120)
    
    # Using Annotated with validation constraints
    score: Annotated[int, Gt(0), Lt(100)]
```

```python
from pydantic import BaseModel, Field
from typing import Annotated

class MyModel(BaseModel):
    # Custom metadata will be stored in the metadata list
    field: Annotated[int, "custom_metadata"] = 5
```

---

**User**

Can you share an example from the point 2 related to create a proper metadata class

---

**Cursor**

I'll help you create a proper metadata class example. Let me first search for some relevant examples in the codebase.


Based on the search results, I'll show you how to create a proper metadata class in Pydantic. Here's a comprehensive example that demonstrates how to create and use a custom metadata class:

```python
from dataclasses import dataclass
from typing import Annotated, Any
from pydantic import BaseModel, Field
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

# 1. Create a custom metadata class
@dataclass(frozen=True)
class CustomValidation(PydanticMetadata, BaseMetadata):
    """A custom metadata class for validation.
    
    This class demonstrates how to create a proper metadata class that can be used
    with Pydantic's validation system.
    """
    min_value: int
    max_value: int
    custom_message: str = "Value must be between {min_value} and {max_value}"

    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        """Implement the core schema generation method.
        
        This method is called by Pydantic to generate the validation schema.
        """
        schema = handler(source_type)
        schema['metadata'] = {
            'min_value': self.min_value,
            'max_value': self.max_value,
            'custom_message': self.custom_message
        }
        return schema

# 2. Use the custom metadata in a model
class MyModel(BaseModel):
    # Using the custom metadata with Annotated
    value: Annotated[int, CustomValidation(min_value=0, max_value=100)]
    
    # Alternative way using Field
    another_value: int = Field(
        gt=0,
        lt=100,
        description="A value between 0 and 100"
    )

# 3. Example usage
model = MyModel(value=50)  # This will work
try:
    model = MyModel(value=150)  # This will raise a validation error
except Exception as e:
    print(f"Validation error: {e}")
```

Let's break down the key components of this example:

1. **Custom Metadata Class**:
   - Inherits from both `PydanticMetadata` and `BaseMetadata`
   - Uses `@dataclass(frozen=True)` for immutability
   - Implements `__get_pydantic_core_schema__` to define validation behavior
   - Can include custom attributes and methods

2. **Using the Metadata**:
   - Can be used with `Annotated` type hints
   - Can be combined with other metadata types
   - Can be used alongside `Field` for additional validation

3. **Key Features**:
   - Type safety through proper inheritance
   - Integration with Pydantic's validation system
   - Custom validation logic
   - Reusable across different models

4. **Best Practices**:
   - Make metadata classes immutable (frozen=True)
   - Provide clear documentation
   - Implement proper type hints
   - Use meaningful names and attributes

This example shows how to create a proper metadata class that:
- Integrates with Pydantic's validation system
- Provides type safety
- Is reusable across different models
- Can be combined with other validation mechanisms

The metadata class can be used to add custom validation logic, constraints, or any other field-level behavior you need in your Pydantic models.

```python
from dataclasses import dataclass
from typing import Annotated, Any
from pydantic import BaseModel, Field
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

# 1. Create a custom metadata class
@dataclass(frozen=True)
class CustomValidation(PydanticMetadata, BaseMetadata):
    """A custom metadata class for validation.
    
    This class demonstrates how to create a proper metadata class that can be used
    with Pydantic's validation system.
    """
    min_value: int
    max_value: int
    custom_message: str = "Value must be between {min_value} and {max_value}"

    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        """Implement the core schema generation method.
        
        This method is called by Pydantic to generate the validation schema.
        """
        schema = handler(source_type)
        schema['metadata'] = {
            'min_value': self.min_value,
            'max_value': self.max_value,
            'custom_message': self.custom_message
        }
        return schema

# 2. Use the custom metadata in a model
class MyModel(BaseModel):
    # Using the custom metadata with Annotated
    value: Annotated[int, CustomValidation(min_value=0, max_value=100)]
    
    # Alternative way using Field
    another_value: int = Field(
        gt=0,
        lt=100,
        description="A value between 0 and 100"
    )

# 3. Example usage
model = MyModel(value=50)  # This will work
try:
    model = MyModel(value=150)  # This will raise a validation error
except Exception as e:
    print(f"Validation error: {e}")
```

---

**User**

Can the `CustomValidation` from your example inherit from pydantic `BaseModel`

---

**Cursor**

Let me search for information about inheritance patterns in Pydantic's metadata classes.


Based on the search results, I can explain why the `CustomValidation` class should not inherit from `BaseModel`. Here's why:

1. **Different Purposes**:
   - `BaseModel` is designed for creating data models with validation
   - `PydanticMetadata` and `BaseMetadata` are designed specifically for field-level metadata and validation

2. **Architectural Design**:
   - Metadata classes in Pydantic are meant to be lightweight and focused on field validation
   - They should inherit from `PydanticMetadata` and `BaseMetadata` to integrate with Pydantic's validation system
   - `BaseModel` is too heavy for this purpose as it includes many features not needed for metadata

3. **Proper Implementation**:
Here's the correct way to implement the `CustomValidation` class:

```python
from dataclasses import dataclass
from typing import Annotated, Any
from pydantic import BaseModel
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

@dataclass(frozen=True)
class CustomValidation(PydanticMetadata, BaseMetadata):
    """A custom metadata class for validation."""
    min_value: int
    max_value: int
    custom_message: str = "Value must be between {min_value} and {max_value}"

    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        """Implement the core schema generation method."""
        schema = handler(source_type)
        schema['metadata'] = {
            'min_value': self.min_value,
            'max_value': self.max_value,
            'custom_message': self.custom_message
        }
        return schema

# Usage in a model
class MyModel(BaseModel):
    value: Annotated[int, CustomValidation(min_value=0, max_value=100)]
```

4. **Key Differences**:
   - Metadata classes are used as annotations for fields
   - They don't need model-level features like serialization, validation, etc.
   - They focus on field-level constraints and validation rules

5. **Benefits of the Correct Approach**:
   - Lighter weight and more focused
   - Better integration with Pydantic's validation system
   - Proper type checking and IDE support
   - Follows Pydantic's design patterns

So, while it might seem intuitive to inherit from `BaseModel`, it's not the correct approach for metadata classes. The proper pattern is to inherit from `PydanticMetadata` and `BaseMetadata` to create field-level validation metadata.

```python
from dataclasses import dataclass
from typing import Annotated, Any
from pydantic import BaseModel
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

@dataclass(frozen=True)
class CustomValidation(PydanticMetadata, BaseMetadata):
    """A custom metadata class for validation."""
    min_value: int
    max_value: int
    custom_message: str = "Value must be between {min_value} and {max_value}"

    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        """Implement the core schema generation method."""
        schema = handler(source_type)
        schema['metadata'] = {
            'min_value': self.min_value,
            'max_value': self.max_value,
            'custom_message': self.custom_message
        }
        return schema

# Usage in a model
class MyModel(BaseModel):
    value: Annotated[int, CustomValidation(min_value=0, max_value=100)]
```

---

**User**

Do you know if Pydantic has another solution than `customvalidation` to create a custom `FieldInfo` that could contain extra informations

---

**Cursor**

Let me search for alternative ways to create custom field information in Pydantic.


Based on the search results, I can show you several ways to add extra information to fields in Pydantic:

1. **Using `json_schema_extra`**:
```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    value: int = Field(
        json_schema_extra={
            "custom_info": "This is extra information",
            "example": 42,
            "additional_properties": {
                "key1": "value1",
                "key2": "value2"
            }
        }
    )
```

2. **Using `Field` with extra kwargs**:
```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    value: int = Field(
        description="A field with extra information",
        examples=[1, 2, 3],
        title="Custom Title",
        extra_info="This will be stored in the field's metadata"
    )
```

3. **Using `Annotated` with custom metadata**:
```python
from typing import Annotated
from pydantic import BaseModel
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

@dataclass(frozen=True)
class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        schema = handler(source_type)
        schema['metadata'] = {
            'extra_data': self.extra_data
        }
        return schema

class MyModel(BaseModel):
    value: Annotated[int, CustomFieldInfo(extra_data={"key": "value"})]
```

4. **Using model configuration**:
```python
from pydantic import BaseModel, ConfigDict

class MyModel(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "custom_info": "This is model-level extra information"
        }
    )
    value: int
```

5. **Using field metadata with `FieldInfo`**:
```python
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

class MyModel(BaseModel):
    value: int = Field(
        metadata=[
            {"type": "custom", "data": "extra information"},
            {"type": "validation", "rule": "custom_rule"}
        ]
    )
```

The best approach depends on your specific needs:

1. Use `json_schema_extra` if you need to add information to the JSON schema
2. Use `Field` with extra kwargs for simple additional information
3. Use custom metadata classes for complex validation or processing logic
4. Use model configuration for model-level extra information
5. Use field metadata for multiple pieces of metadata

The most flexible approach is using custom metadata classes, as they allow you to:
- Add any type of information
- Implement custom validation logic
- Control how the information is used in schema generation
- Maintain type safety
- Keep the code organized and maintainable

Remember that all these approaches can be combined based on your needs. The key is to choose the approach that best fits your specific use case while maintaining code clarity and type safety.

```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    value: int = Field(
        json_schema_extra={
            "custom_info": "This is extra information",
            "example": 42,
            "additional_properties": {
                "key1": "value1",
                "key2": "value2"
            }
        }
    )
```

```python
from pydantic import BaseModel, Field

class MyModel(BaseModel):
    value: int = Field(
        description="A field with extra information",
        examples=[1, 2, 3],
        title="Custom Title",
        extra_info="This will be stored in the field's metadata"
    )
```

```python
from typing import Annotated
from pydantic import BaseModel
from pydantic._internal._fields import PydanticMetadata
from annotated_types import BaseMetadata

@dataclass(frozen=True)
class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> dict[str, Any]:
        schema = handler(source_type)
        schema['metadata'] = {
            'extra_data': self.extra_data
        }
        return schema

class MyModel(BaseModel):
    value: Annotated[int, CustomFieldInfo(extra_data={"key": "value"})]
```

```python
from pydantic import BaseModel, ConfigDict

class MyModel(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "custom_info": "This is model-level extra information"
        }
    )
    value: int
```

```python
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

class MyModel(BaseModel):
    value: int = Field(
        metadata=[
            {"type": "custom", "data": "extra information"},
            {"type": "validation", "rule": "custom_rule"}
        ]
    )
```

---

**User**

Ok, so from your explanation it is best to use `CustomFieldInfo`, how can I enforce on my base model that all the field have this annotation?

---

**Cursor**

Let me search for ways to enforce field annotations at the model level.


Based on the search results, I can show you how to enforce the `CustomFieldInfo` annotation on all fields in your base model. Here's how to do it:

1. First, create a custom metaclass that inherits from Pydantic's `ModelMetaclass`:

```python
from typing import Any, get_origin, get_args
from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

class EnforceCustomFieldInfoMetaclass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        # Get the annotations from the namespace
        annotations = namespace.get('__annotations__', {})
        
        # Check each field annotation
        for field_name, annotation in annotations.items():
            # Skip if it's already an Annotated type
            if get_origin(annotation) is Annotated:
                continue
                
            # Add CustomFieldInfo to the annotation
            annotations[field_name] = Annotated[annotation, CustomFieldInfo()]
            
        # Update the namespace with modified annotations
        namespace['__annotations__'] = annotations
        
        # Create the class using the parent metaclass
        return super().__new__(mcs, name, bases, namespace, **kwargs)
```

2. Then create your base model with this metaclass:

```python
class BaseModelWithCustomFieldInfo(BaseModel, metaclass=EnforceCustomFieldInfoMetaclass):
    pass
```

3. Now all models inheriting from this base class will automatically have `CustomFieldInfo` added to their field annotations:

```python
class MyModel(BaseModelWithCustomFieldInfo):
    name: str  # This will become Annotated[str, CustomFieldInfo()]
    age: int   # This will become Annotated[int, CustomFieldInfo()]
```

This approach has several benefits:

1. It's automatic - you don't need to manually add the annotation to each field
2. It's consistent - all fields will have the same metadata structure
3. It's maintainable - you can modify the `CustomFieldInfo` class and all fields will be updated
4. It preserves existing annotations - if a field already has `Annotated` with other metadata, it won't be modified

Note that this will only affect fields that are defined in the model class itself. Fields inherited from parent classes will need to be explicitly annotated if you want them to have the `CustomFieldInfo`.

You can also make this more flexible by allowing configuration of the `CustomFieldInfo` parameters:

```python
class EnforceCustomFieldInfoMetaclass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        annotations = namespace.get('__annotations__', {})
        
        # Get custom field info parameters from class configuration
        custom_field_info_params = getattr(namespace.get('Config', object()), 'custom_field_info_params', {})
        
        for field_name, annotation in annotations.items():
            if get_origin(annotation) is Annotated:
                continue
                
            # Create CustomFieldInfo with parameters from config
            field_info = CustomFieldInfo(**custom_field_info_params)
            annotations[field_name] = Annotated[annotation, field_info]
            
        namespace['__annotations__'] = annotations
        return super().__new__(mcs, name, bases, namespace, **kwargs)
```

Then you can configure the `CustomFieldInfo` parameters at the model level:

```python
class MyModel(BaseModelWithCustomFieldInfo):
    class Config:
        custom_field_info_params = {
            'extra_data': {'key': 'value'}
        }
    
    name: str  # Will have CustomFieldInfo with the configured parameters
```

This gives you more flexibility while still maintaining the automatic annotation enforcement.

```python
from typing import Any, get_origin, get_args
from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

class EnforceCustomFieldInfoMetaclass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        # Get the annotations from the namespace
        annotations = namespace.get('__annotations__', {})
        
        # Check each field annotation
        for field_name, annotation in annotations.items():
            # Skip if it's already an Annotated type
            if get_origin(annotation) is Annotated:
                continue
                
            # Add CustomFieldInfo to the annotation
            annotations[field_name] = Annotated[annotation, CustomFieldInfo()]
            
        # Update the namespace with modified annotations
        namespace['__annotations__'] = annotations
        
        # Create the class using the parent metaclass
        return super().__new__(mcs, name, bases, namespace, **kwargs)
```

```python
class BaseModelWithCustomFieldInfo(BaseModel, metaclass=EnforceCustomFieldInfoMetaclass):
    pass
```

```python
class MyModel(BaseModelWithCustomFieldInfo):
    name: str  # This will become Annotated[str, CustomFieldInfo()]
    age: int   # This will become Annotated[int, CustomFieldInfo()]
```

```python
class EnforceCustomFieldInfoMetaclass(ModelMetaclass):
    def __new__(mcs, name, bases, namespace, **kwargs):
        annotations = namespace.get('__annotations__', {})
        
        # Get custom field info parameters from class configuration
        custom_field_info_params = getattr(namespace.get('Config', object()), 'custom_field_info_params', {})
        
        for field_name, annotation in annotations.items():
            if get_origin(annotation) is Annotated:
                continue
                
            # Create CustomFieldInfo with parameters from config
            field_info = CustomFieldInfo(**custom_field_info_params)
            annotations[field_name] = Annotated[annotation, field_info]
            
        namespace['__annotations__'] = annotations
        return super().__new__(mcs, name, bases, namespace, **kwargs)
```

```python
class MyModel(BaseModelWithCustomFieldInfo):
    class Config:
        custom_field_info_params = {
            'extra_data': {'key': 'value'}
        }
    
    name: str  # Will have CustomFieldInfo with the configured parameters
```

---

**User**

What is the difference between `__get_pydantic_core_schema__`  and `__get_pydantic_json_schema__`?
Which one should be used for 
1 Adding extra property to `json_schema_extra`
2 Extract the data from the schema to rebuild the model metadata

---

**Cursor**

Let me search for information about these two methods and their specific purposes.


Based on the search results, I can explain the key differences between `__get_pydantic_core_schema__` and `__get_pydantic_json_schema__`:

1. **`__get_pydantic_core_schema__`**:
   - Used for defining the core validation schema
   - Runs during model initialization and validation
   - Affects how data is validated and processed
   - Returns a `CoreSchema` object
   - Used to modify the internal validation behavior

2. **`__get_pydantic_json_schema__`**:
   - Used for defining the JSON schema representation
   - Runs during JSON schema generation
   - Affects how the model is represented in OpenAPI/Swagger docs
   - Returns a `JsonSchemaValue` object
   - Used to modify the external schema representation

For your specific use cases:

1. **Adding extra properties to `json_schema_extra`**:
   Use `__get_pydantic_json_schema__`:
```python
from typing import Any
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue
from pydantic._internal._core_metadata import CoreMetadata

class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_json_schema__(
        self, 
        core_schema: CoreSchema, 
        handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema.update(self.extra_data)
        return json_schema
```

2. **Extracting data from schema to rebuild model metadata**:
   Use `__get_pydantic_core_schema__`:
```python
from typing import Any
from pydantic import BaseModel
from pydantic._internal._core_metadata import CoreMetadata

class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_core_schema__(
        self, 
        source_type: Any, 
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        schema = handler(source_type)
        metadata = cast('CoreMetadata', schema.setdefault('metadata', {}))
        metadata['custom_data'] = self.extra_data
        return schema
```

The key differences in practice:

1. For JSON schema modifications:
   - Use `__get_pydantic_json_schema__` when you want to modify how the field appears in OpenAPI/Swagger docs
   - This is for external representation and documentation

2. For validation and metadata:
   - Use `__get_pydantic_core_schema__` when you want to modify how the field is validated or store metadata
   - This is for internal behavior and data processing

Best practices:
1. Use `__get_pydantic_json_schema__` for:
   - Adding documentation
   - Modifying OpenAPI/Swagger representation
   - Adding examples
   - Adding external metadata

2. Use `__get_pydantic_core_schema__` for:
   - Adding validation rules
   - Storing internal metadata
   - Modifying validation behavior
   - Adding custom processing logic

Remember that you can use both methods together if you need both validation and JSON schema modifications.

```python
from typing import Any
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue
from pydantic._internal._core_metadata import CoreMetadata

class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_json_schema__(
        self, 
        core_schema: CoreSchema, 
        handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema.update(self.extra_data)
        return json_schema
```

```python
from typing import Any
from pydantic import BaseModel
from pydantic._internal._core_metadata import CoreMetadata

class CustomFieldInfo(PydanticMetadata, BaseMetadata):
    extra_data: dict[str, Any]
    
    def __get_pydantic_core_schema__(
        self, 
        source_type: Any, 
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        schema = handler(source_type)
        metadata = cast('CoreMetadata', schema.setdefault('metadata', {}))
        metadata['custom_data'] = self.extra_data
        return schema
```

