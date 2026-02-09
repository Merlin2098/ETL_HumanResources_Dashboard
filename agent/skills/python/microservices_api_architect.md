# Skill: microservices_api_architect

The agent designs and implements scalable microservices and API architectures using modern Python frameworks.

## Responsibility
Structure projects using FastAPI or Flask with clean architecture principles, dependency injection, and proper error handling.

## Rules
- Use FastAPI for async-first, high-performance APIs
- Use Flask for simpler, traditional WSGI applications
- Apply Domain-Driven Design (DDD) folder structure
- Implement dependency injection for testability
- Use middleware for cross-cutting concerns (logging, auth, CORS)
- Implement global exception handlers
- Version APIs properly (v1, v2 in routes)
- Document APIs using OpenAPI/Swagger

## Behavior

### Step 1: Project Structure (DDD)
- Organize code by domain boundaries:
  ```
  /app
    /domain          # Business logic, entities
    /application     # Use cases, services
    /infrastructure  # External dependencies (DB, APIs)
    /presentation    # Controllers, routes, schemas
  ```

### Step 2: Implement Dependency Injection
- Create dependency providers
- Use FastAPI's `Depends()` or implement custom DI container
- Inject repositories, services, and configurations
- Avoid global state; pass dependencies explicitly

### Step 3: Configure Middleware
- Add CORS middleware for cross-origin requests
- Implement logging middleware for request/response tracking
- Add authentication/authorization middleware
- Handle request timing and rate limiting

### Step 4: Global Exception Handling
- Create custom exception classes
- Register global exception handlers
- Return consistent error responses (RFC 7807 Problem Details)
- Log exceptions with context

### Step 5: API Documentation
- Use automatic OpenAPI generation
- Add descriptions, examples, and tags to endpoints
- Document request/response models
- Provide usage examples in docs

## Example Usage

**FastAPI Project Structure:**
```python
# app/main.py
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.presentation.routes import users, products
from app.infrastructure.database import get_db

app = FastAPI(title="Microservice API", version="1.0.0")

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(users.router, prefix="/api/v1/users", tags=["users"])
app.include_router(products.router, prefix="/api/v1/products", tags=["products"])
```

**Dependency Injection:**
```python
# app/infrastructure/database.py
from sqlalchemy.orm import Session

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# app/presentation/routes/users.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.application.services import UserService
from app.infrastructure.database import get_db

router = APIRouter()

@router.get("/{user_id}")
async def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    service: UserService = Depends()
):
    return await service.get_user(user_id, db)
```

**Global Exception Handler:**
```python
# app/presentation/exceptions.py
from fastapi import Request, status
from fastapi.responses import JSONResponse

class BusinessException(Exception):
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code

@app.exception_handler(BusinessException)
async def business_exception_handler(request: Request, exc: BusinessException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "type": "about:blank",
            "title": "Business Error",
            "status": exc.status_code,
            "detail": exc.message,
        }
    )
```

**Domain-Driven Structure:**
```python
# app/domain/entities/user.py
from dataclasses import dataclass

@dataclass
class User:
    id: int
    username: str
    email: str
    
    def can_perform_action(self) -> bool:
        # Business logic here
        return True

# app/application/services/user_service.py
from app.domain.entities.user import User
from app.infrastructure.repositories.user_repository import UserRepository

class UserService:
    def __init__(self, repository: UserRepository):
        self.repository = repository
    
    async def get_user(self, user_id: int) -> User:
        user = await self.repository.find_by_id(user_id)
        if not user:
            raise NotFoundException(f"User {user_id} not found")
        return user
```

## Notes
- Keep business logic in domain layer, not in routes
- Use DTOs/schemas for API contracts
- Implement health check endpoints
- Consider API gateway patterns for multiple services
- Use async/await for I/O-bound operations in FastAPI
