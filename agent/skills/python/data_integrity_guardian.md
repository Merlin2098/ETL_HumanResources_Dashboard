# Skill: data_integrity_guardian

The agent validates all inputs and outputs to ensure data integrity and prevent errors.

## Responsibility
Implement comprehensive validation using schemas, type checking, and custom validators.

## Rules
- Validate all external inputs (user input, API requests, file data)
- Validate outputs before sending to external systems
- Use Pydantic for schema-based validation
- Define custom validators for business rules
- Provide clear error messages for validation failures
- Fail fast: validate early in the processing pipeline

## Behavior

### Step 1: Define Data Schemas
- Create Pydantic models with types and constraints
- Add field validators for custom rules

### Step 2: Validate Input Data
- Parse and validate incoming data against schemas
- Catch validation errors and return meaningful messages

### Step 3: Validate Business Rules
- Implement custom validators for domain-specific rules
- Check relationships between fields

### Step 4: Validate Outputs
- Verify data before sending to external systems

## Example Usage

```python
from pydantic import BaseModel, Field, EmailStr, validator

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=20)
    email: EmailStr
    age: int = Field(..., ge=0, le=150)
    password: str = Field(..., min_length=8)
    
    @validator('password')
    def validate_password(cls, v):
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain uppercase letter')
        return v
```

## Notes
- Validation should be defensive but not paranoid
- Provide helpful error messages for users
- Use type hints combined with Pydantic for maximum safety
