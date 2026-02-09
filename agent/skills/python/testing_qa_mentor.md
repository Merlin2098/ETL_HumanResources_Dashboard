# Skill: testing_qa_mentor

The agent generates comprehensive unit and integration tests, ensuring high code quality and coverage.

## Responsibility
Create robust test suites using pytest, implement mocking strategies, and maintain high code coverage.

## Rules
- Write tests for all business logic functions
- Use pytest as the primary testing framework
- Achieve minimum 80% code coverage
- Use fixtures for test data and setup
- Mock external dependencies (databases, APIs, file I/O)
- Use parametrization for testing multiple scenarios
- Implement property-based testing for complex logic
- Separate unit tests from integration tests
- Name tests clearly: `test_function_name_scenario_expected_result`

## Behavior

### Step 1: Set Up Testing Framework
- Install pytest and plugins: `pytest-cov`, `pytest-mock`, `pytest-asyncio`
- Create `pytest.ini` or `pyproject.toml` configuration
- Set up test directory structure mirroring source code
- Configure coverage reporting

### Step 2: Write Unit Tests
- Test individual functions in isolation
- Use `unittest.mock` or `pytest-mock` for dependencies
- Test edge cases, boundary conditions, and error paths
- Assert expected outputs and side effects

### Step 3: Create Fixtures
- Define reusable test data as pytest fixtures
- Use fixture scopes appropriately (function, class, module, session)
- Create factory fixtures for complex objects
- Use `conftest.py` for shared fixtures

### Step 4: Implement Parametrized Tests
- Use `@pytest.mark.parametrize` for testing multiple inputs
- Cover various scenarios without duplicating test code
- Test both valid and invalid inputs

### Step 5: Property-Based Testing
- Use Hypothesis for generating test cases
- Define properties that should always hold true
- Let Hypothesis find edge cases automatically

### Step 6: Integration Tests
- Test interactions between components
- Use test databases or in-memory alternatives
- Test API endpoints end-to-end
- Verify database transactions and rollbacks

## Example Usage

**Basic Unit Test with Mocking:**
```python
# tests/test_user_service.py
import pytest
from unittest.mock import Mock
from app.services.user_service import UserService

@pytest.fixture
def mock_repository():
    return Mock()

@pytest.fixture
def user_service(mock_repository):
    return UserService(repository=mock_repository)

def test_get_user_returns_user_when_exists(user_service, mock_repository):
    # Arrange
    expected_user = {"id": 1, "name": "John"}
    mock_repository.find_by_id.return_value = expected_user
    
    # Act
    result = user_service.get_user(1)
    
    # Assert
    assert result == expected_user
    mock_repository.find_by_id.assert_called_once_with(1)

def test_get_user_raises_exception_when_not_found(user_service, mock_repository):
    # Arrange
    mock_repository.find_by_id.return_value = None
    
    # Act & Assert
    with pytest.raises(UserNotFoundException):
        user_service.get_user(999)
```

**Parametrized Tests:**
```python
@pytest.mark.parametrize("input_value,expected", [
    (0, 0),
    (1, 1),
    (5, 120),
    (-1, None),
])
def test_factorial_various_inputs(input_value, expected):
    result = factorial(input_value)
    assert result == expected
```

**Property-Based Testing with Hypothesis:**
```python
from hypothesis import given, strategies as st

@given(st.lists(st.integers()))
def test_reverse_twice_equals_original(lst):
    """Reversing a list twice should return the original list."""
    assert reverse(reverse(lst)) == lst

@given(st.text(), st.text())
def test_string_concatenation_length(s1, s2):
    """Length of concatenated strings equals sum of individual lengths."""
    result = s1 + s2
    assert len(result) == len(s1) + len(s2)
```

**Integration Test with Test Database:**
```python
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import Base, User

@pytest.fixture(scope="function")
def test_db():
    # Create in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    session.close()

def test_user_creation_persists_to_database(test_db):
    # Arrange
    user = User(username="testuser", email="test@example.com")
    
    # Act
    test_db.add(user)
    test_db.commit()
    
    # Assert
    retrieved = test_db.query(User).filter_by(username="testuser").first()
    assert retrieved is not None
    assert retrieved.email == "test@example.com"
```

**pytest Configuration (pyproject.toml):**
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--cov=app",
    "--cov-report=html",
    "--cov-report=term-missing",
    "--strict-markers",
    "-v"
]
markers = [
    "unit: Unit tests",
    "integration: Integration tests",
    "slow: Slow running tests"
]
```

## Notes
- Run tests frequently during development
- Use markers to separate fast unit tests from slow integration tests
- Mock external services to avoid flaky tests
- Aim for meaningful coverage, not just high percentages
- Test behavior, not implementation details
