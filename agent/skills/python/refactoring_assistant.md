# Skill: refactoring_assistant

The agent identifies code smells and suggests refactoring to improve maintainability.

## Responsibility
Detect code duplication, long functions, god classes, and suggest splitting or extracting logic.

## Rules
- Keep functions under 20-30 lines when possible
- Keep classes focused on single responsibility
- Extract duplicated code into reusable functions
- Split large modules into smaller, cohesive ones
- Reduce cyclomatic complexity
- Improve naming and readability
- Maintain backward compatibility when refactoring APIs

## Behavior

### Step 1: Identify Code Smells
- Long functions (>50 lines)
- Duplicated code blocks
- God classes (too many responsibilities)
- Long parameter lists (>5 parameters)
- Deep nesting (>3 levels)
- High cyclomatic complexity

### Step 2: Suggest Refactoring Strategies
- Extract method: Pull logic into separate function
- Extract class: Group related functionality
- Replace conditionals with polymorphism
- Introduce parameter object for long parameter lists
- Use design patterns where appropriate

### Step 3: Implement Refactoring
- Make small, incremental changes
- Run tests after each change
- Commit frequently
- Document breaking changes

### Step 4: Verify Improvements
- Ensure tests still pass
- Check code coverage maintained or improved
- Verify performance not degraded

## Example Usage

**Extract Method Refactoring:**
```python
# ❌ Before: Long function with multiple responsibilities
def process_order(order_data):
    # Validate order
    if not order_data.get('items'):
        raise ValueError("No items in order")
    if not order_data.get('customer_id'):
        raise ValueError("No customer ID")
    
    # Calculate total
    total = 0
    for item in order_data['items']:
        price = item['price']
        quantity = item['quantity']
        total += price * quantity
    
    # Apply discount
    if order_data.get('discount_code'):
        if order_data['discount_code'] == 'SAVE10':
            total *= 0.9
        elif order_data['discount_code'] == 'SAVE20':
            total *= 0.8
    
    # Save to database
    db.execute(
        "INSERT INTO orders (customer_id, total) VALUES (?, ?)",
        (order_data['customer_id'], total)
    )
    
    return total

# ✅ After: Extracted into focused functions
def validate_order(order_data: dict) -> None:
    """Validate order has required fields."""
    if not order_data.get('items'):
        raise ValueError("No items in order")
    if not order_data.get('customer_id'):
        raise ValueError("No customer ID")

def calculate_order_total(items: list[dict]) -> float:
    """Calculate total from order items."""
    return sum(item['price'] * item['quantity'] for item in items)

def apply_discount(total: float, discount_code: str) -> float:
    """Apply discount code to total."""
    discounts = {
        'SAVE10': 0.9,
        'SAVE20': 0.8,
    }
    multiplier = discounts.get(discount_code, 1.0)
    return total * multiplier

def save_order(customer_id: int, total: float) -> None:
    """Save order to database."""
    db.execute(
        "INSERT INTO orders (customer_id, total) VALUES (?, ?)",
        (customer_id, total)
    )

def process_order(order_data: dict) -> float:
    """Process order with validation, calculation, and persistence."""
    validate_order(order_data)
    
    total = calculate_order_total(order_data['items'])
    
    if discount_code := order_data.get('discount_code'):
        total = apply_discount(total, discount_code)
    
    save_order(order_data['customer_id'], total)
    
    return total
```

**Extract Class Refactoring:**
```python
# ❌ Before: God class with too many responsibilities
class UserManager:
    def create_user(self, data): ...
    def update_user(self, user_id, data): ...
    def delete_user(self, user_id): ...
    def send_welcome_email(self, user): ...
    def send_password_reset(self, user): ...
    def validate_email(self, email): ...
    def hash_password(self, password): ...
    def generate_token(self, user): ...
    def verify_token(self, token): ...

# ✅ After: Split into focused classes
class UserRepository:
    """Handle user data persistence."""
    def create(self, data): ...
    def update(self, user_id, data): ...
    def delete(self, user_id): ...
    def find_by_id(self, user_id): ...

class EmailService:
    """Handle email notifications."""
    def send_welcome_email(self, user): ...
    def send_password_reset(self, user): ...

class AuthService:
    """Handle authentication logic."""
    def hash_password(self, password): ...
    def verify_password(self, password, hash): ...
    def generate_token(self, user): ...
    def verify_token(self, token): ...

class UserValidator:
    """Validate user data."""
    def validate_email(self, email): ...
    def validate_password(self, password): ...
```

**Introduce Parameter Object:**
```python
# ❌ Before: Long parameter list
def create_user(username, email, first_name, last_name, age, country, city, zip_code):
    ...

# ✅ After: Parameter object
from dataclasses import dataclass

@dataclass
class UserData:
    username: str
    email: str
    first_name: str
    last_name: str
    age: int
    country: str
    city: str
    zip_code: str

def create_user(user_data: UserData):
    ...
```

**Replace Conditionals with Polymorphism:**
```python
# ❌ Before: Type checking
def calculate_shipping(order, shipping_type):
    if shipping_type == 'standard':
        return order.total * 0.05
    elif shipping_type == 'express':
        return order.total * 0.15
    elif shipping_type == 'overnight':
        return order.total * 0.25
    else:
        return 0

# ✅ After: Polymorphism
from abc import ABC, abstractmethod

class ShippingStrategy(ABC):
    @abstractmethod
    def calculate(self, order) -> float:
        pass

class StandardShipping(ShippingStrategy):
    def calculate(self, order) -> float:
        return order.total * 0.05

class ExpressShipping(ShippingStrategy):
    def calculate(self, order) -> float:
        return order.total * 0.15

class OvernightShipping(ShippingStrategy):
    def calculate(self, order) -> float:
        return order.total * 0.25

def calculate_shipping(order, strategy: ShippingStrategy) -> float:
    return strategy.calculate(order)
```

**Reduce Nesting:**
```python
# ❌ Before: Deep nesting
def process_data(data):
    if data:
        if data.is_valid():
            if data.has_permission():
                if data.is_active():
                    return data.process()
                else:
                    return "Inactive"
            else:
                return "No permission"
        else:
            return "Invalid"
    else:
        return "No data"

# ✅ After: Guard clauses
def process_data(data):
    if not data:
        return "No data"
    if not data.is_valid():
        return "Invalid"
    if not data.has_permission():
        return "No permission"
    if not data.is_active():
        return "Inactive"
    
    return data.process()
```

## Notes
- Refactor continuously, not in big batches
- Always have tests before refactoring
- Use IDE refactoring tools when available
- Document why, not just what, in refactoring commits
- Consider using tools like radon for complexity metrics
