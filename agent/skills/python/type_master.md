# Skill: type_master

The agent ensures all Python functions and methods have comprehensive type hints for improved code quality and maintainability.

## Responsibility
Enforce static type checking across the codebase using type hints and validation tools.

## Rules
- All function signatures must include type hints for parameters and return values
- Use `typing` module for complex types (Union, Optional, Any, Protocol, TypeVar, Generic)
- Configure static type checkers (Mypy or Pyright) in the project
- Use Pydantic for runtime data validation when needed
- Avoid using `Any` unless absolutely necessary; prefer specific types
- Document type aliases for complex or reused type definitions

## Behavior

### Step 1: Add Type Hints to Functions
- Analyze all function and method signatures
- Add type hints for all parameters
- Add return type annotations
- Use `-> None` for functions that don't return a value

### Step 2: Import Required Types
- Import necessary types from `typing` module
- Use `from __future__ import annotations` for forward references (Python 3.7+)
- Define custom type aliases for complex types

### Step 3: Configure Type Checker
- Add `mypy.ini` or `pyproject.toml` configuration
- Set strict mode options: `strict = true` or equivalent
- Configure ignored modules or error codes if needed
- Add type checker to CI/CD pipeline

### Step 4: Implement Runtime Validation
- Use Pydantic models for data validation at runtime
- Define BaseModel subclasses for structured data
- Validate API inputs, configuration files, and external data

## Example Usage

**Basic Type Hints:**
```python
from typing import List, Optional, Dict, Union

def process_items(items: List[str], max_count: Optional[int] = None) -> Dict[str, int]:
    """Process items and return statistics."""
    result: Dict[str, int] = {}
    for item in items[:max_count]:
        result[item] = len(item)
    return result
```

**Advanced Types with Protocols:**
```python
from typing import Protocol, TypeVar, Generic

class Drawable(Protocol):
    def draw(self) -> None:
        ...

T = TypeVar('T', bound=Drawable)

def render_items(items: List[T]) -> None:
    for item in items:
        item.draw()
```

**Pydantic Runtime Validation:**
```python
from pydantic import BaseModel, Field, validator

class UserConfig(BaseModel):
    username: str = Field(..., min_length=3, max_length=20)
    age: int = Field(..., ge=0, le=150)
    email: Optional[str] = None
    
    @validator('email')
    def validate_email(cls, v):
        if v and '@' not in v:
            raise ValueError('Invalid email format')
        return v
```

**Mypy Configuration (pyproject.toml):**
```toml
[tool.mypy]
python_version = "3.10"
strict = true
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
```

## Notes
- Type hints improve IDE autocomplete and catch errors early
- Use `reveal_type()` in mypy for debugging type inference
- Consider using `typing.cast()` when type narrowing is needed
- Keep types as specific as possible for better safety
