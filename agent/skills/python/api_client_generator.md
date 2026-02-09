# Skill: api_client_generator

The agent generates API client code automatically from OpenAPI/Swagger or GraphQL schemas.

## Responsibility
Create type-safe API clients from schema definitions, reducing manual coding and errors.

## Rules
- Generate clients from OpenAPI 3.0+ specifications
- Generate clients from GraphQL schemas
- Include type hints for all methods
- Handle authentication and headers
- Implement error handling
- Generate async clients when appropriate
- Document generated methods
- Keep generated code separate from custom code

## Behavior

### Step 1: Obtain API Schema
- Download OpenAPI/Swagger JSON or YAML
- Fetch GraphQL schema via introspection
- Validate schema is well-formed

### Step 2: Choose Generation Tool
- **OpenAPI**: Use openapi-python-client, datamodel-code-generator
- **GraphQL**: Use ariadne-codegen, sgqlc
- Configure generation options (async, auth, etc.)

### Step 3: Generate Client Code
- Run code generator with schema
- Review generated code structure
- Customize templates if needed

### Step 4: Integrate Generated Client
- Import generated client in application
- Configure authentication and base URL
- Use type-safe methods
- Handle errors appropriately

## Example Usage

**OpenAPI Client Generation:**
```bash
# Install openapi-python-client
pip install openapi-python-client

# Generate client from URL
openapi-python-client generate --url https://api.example.com/openapi.json

# Generate from local file
openapi-python-client generate --path ./openapi.yaml

# Generate with custom options
openapi-python-client generate \
    --url https://api.example.com/openapi.json \
    --output-path ./generated_client \
    --custom-template-path ./templates
```

**Using Generated OpenAPI Client:**
```python
from generated_client import Client
from generated_client.api.users import get_user, create_user
from generated_client.models import UserCreate

# Initialize client
client = Client(base_url="https://api.example.com", token="your-api-key")

# Use generated methods with type safety
user = get_user.sync(client=client, user_id=123)
print(f"User: {user.username}")

# Create new user
new_user_data = UserCreate(
    username="john_doe",
    email="john@example.com"
)
created_user = create_user.sync(client=client, json_body=new_user_data)
```

**GraphQL Client Generation:**
```bash
# Install ariadne-codegen
pip install ariadne-codegen

# Generate from GraphQL endpoint
ariadne-codegen client \
    --schema-url https://api.example.com/graphql \
    --output ./generated_graphql

# Generate from local schema
ariadne-codegen client \
    --schema-path ./schema.graphql \
    --queries-path ./queries \
    --output ./generated_graphql
```

**GraphQL Query Definition:**
```graphql
# queries/get_user.graphql
query GetUser($id: ID!) {
  user(id: $id) {
    id
    username
    email
    profile {
      firstName
      lastName
    }
  }
}

mutation CreateUser($input: UserInput!) {
  createUser(input: $input) {
    id
    username
  }
}
```

**Using Generated GraphQL Client:**
```python
from generated_graphql import Client
from generated_graphql.get_user import GetUser
from generated_graphql.create_user import CreateUser, UserInput

# Initialize client
client = Client(url="https://api.example.com/graphql")

# Execute query with type safety
result = client.execute(GetUser(id="123"))
if result.user:
    print(f"User: {result.user.username}")
    print(f"Name: {result.user.profile.first_name}")

# Execute mutation
user_input = UserInput(username="john_doe", email="john@example.com")
create_result = client.execute(CreateUser(input=user_input))
```

**Custom Client Wrapper:**
```python
from typing import Optional
from generated_client import Client as GeneratedClient
from generated_client.api.users import get_user, list_users
from generated_client.models import User

class APIClient:
    """Wrapper around generated client with custom logic."""
    
    def __init__(self, base_url: str, api_key: str):
        self._client = GeneratedClient(
            base_url=base_url,
            token=api_key,
            timeout=30.0
        )
    
    def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID with error handling."""
        try:
            return get_user.sync(client=self._client, user_id=user_id)
        except Exception as e:
            print(f"Error fetching user {user_id}: {e}")
            return None
    
    def get_all_users(self) -> list[User]:
        """Get all users with pagination handling."""
        all_users = []
        page = 1
        
        while True:
            response = list_users.sync(
                client=self._client,
                page=page,
                per_page=100
            )
            
            if not response.users:
                break
            
            all_users.extend(response.users)
            
            if not response.has_next_page:
                break
            
            page += 1
        
        return all_users
```

**Async Client Usage:**
```python
import asyncio
from generated_client import AsyncClient
from generated_client.api.users import get_user

async def fetch_multiple_users(user_ids: list[int]):
    """Fetch multiple users concurrently."""
    client = AsyncClient(base_url="https://api.example.com")
    
    tasks = [
        get_user.asyncio(client=client, user_id=uid)
        for uid in user_ids
    ]
    
    users = await asyncio.gather(*tasks, return_exceptions=True)
    return [u for u in users if not isinstance(u, Exception)]

# Usage
user_ids = [1, 2, 3, 4, 5]
users = asyncio.run(fetch_multiple_users(user_ids))
```

**Configuration File (pyproject.toml for ariadne-codegen):**
```toml
[tool.ariadne-codegen]
schema_path = "schema.graphql"
queries_path = "queries"
target_package_path = "generated_graphql"
client_name = "Client"
include_comments = true
convert_to_snake_case = true
```

## Notes
- Regenerate client when API schema changes
- Keep generated code in version control or generate in CI/CD
- Don't modify generated code directly; use wrappers
- Validate schema before generation
- Consider using datamodel-code-generator for Pydantic models from JSON Schema
