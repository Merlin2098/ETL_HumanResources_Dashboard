# Skill: naming_control_flow

The agent enforces clear, descriptive naming conventions and simplifies control flow for maximum readability.

## Responsibility
Ensure code uses declarative, self-documenting names and minimizes complex control structures.

## Rules
- Use descriptive, intention-revealing names
- Avoid ambiguous names like `x`, `tmp`, `data`, `info`
- Use verbs for functions: `calculate_total()`, `fetch_user()`, `validate_input()`
- Use nouns for classes: `UserManager`, `PaymentProcessor`, `DataValidator`
- Use lowercase_with_underscores for variables and functions (snake_case)
- Use PascalCase for class names
- Use UPPERCASE for constants
- Minimize nested if/else statements
- Prefer comprehensions over complex loops
- Use guard clauses to reduce nesting
- Leverage polymorphism over conditionals

## Behavior

### Step 1: Apply Descriptive Naming
- Choose names that reveal intent
- Avoid abbreviations unless widely understood
- Make names searchable and pronounceable
- Use consistent terminology throughout codebase

### Step 2: Simplify Control Flow
- Replace nested conditions with guard clauses
- Extract complex conditions into named functions
- Use early returns to reduce nesting
- Replace conditional chains with polymorphism or dictionaries

### Step 3: Refactor Complex Logic
- Break down large if/else blocks
- Use lookup tables (dictionaries) instead of multiple conditions
- Extract complex expressions into variables
- Use pattern matching (Python 3.10+) when appropriate

### Step 4: Optimize Loops and Iterations
- Use list/dict/set comprehensions for simple transformations
- Use `any()` and `all()` for boolean checks
- Use `enumerate()` when needing indices
- Use `zip()` for parallel iteration

## Example Usage

**Bad Naming vs Good Naming:**
```python
# ❌ Bad: Ambiguous names
def f(x, y):
    tmp = x + y
    data = tmp * 2
    return data

# ✅ Good: Descriptive names
def calculate_discounted_price(original_price: float, discount_rate: float) -> float:
    """Calculate final price after applying discount."""
    subtotal = original_price + discount_rate
    final_price = subtotal * 2
    return final_price
```

**Guard Clauses to Reduce Nesting:**
```python
# ❌ Bad: Deep nesting
def process_user(user):
    if user is not None:
        if user.is_active:
            if user.has_permission('write'):
                return perform_action(user)
            else:
                return "No permission"
        else:
            return "User inactive"
    else:
        return "User not found"

# ✅ Good: Guard clauses
def process_user(user):
    """Process user if valid and authorized."""
    if user is None:
        return "User not found"
    
    if not user.is_active:
        return "User inactive"
    
    if not user.has_permission('write'):
        return "No permission"
    
    return perform_action(user)
```

**Replace Conditionals with Dictionaries:**
```python
# ❌ Bad: Long if/elif chain
def get_day_name(day_number):
    if day_number == 1:
        return "Monday"
    elif day_number == 2:
        return "Tuesday"
    elif day_number == 3:
        return "Wednesday"
    # ... more conditions

# ✅ Good: Lookup dictionary
def get_day_name(day_number: int) -> str:
    """Get day name from day number."""
    days = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
        7: "Sunday"
    }
    return days.get(day_number, "Invalid day")
```

**Use Polymorphism Instead of Type Checking:**
```python
# ❌ Bad: Type checking
def process_payment(payment):
    if isinstance(payment, CreditCardPayment):
        return payment.charge_card()
    elif isinstance(payment, PayPalPayment):
        return payment.send_to_paypal()
    elif isinstance(payment, BankTransferPayment):
        return payment.initiate_transfer()

# ✅ Good: Polymorphism
class Payment(ABC):
    @abstractmethod
    def process(self) -> bool:
        pass

class CreditCardPayment(Payment):
    def process(self) -> bool:
        return self.charge_card()

class PayPalPayment(Payment):
    def process(self) -> bool:
        return self.send_to_paypal()

def process_payment(payment: Payment) -> bool:
    """Process any payment type."""
    return payment.process()
```

**Comprehensions and Built-in Functions:**
```python
# ❌ Bad: Complex loop
result = []
for i in range(len(items)):
    if items[i] > 10:
        result.append(items[i] * 2)

# ✅ Good: List comprehension
result = [item * 2 for item in items if item > 10]

# ❌ Bad: Loop for boolean check
def has_any_active(users):
    for user in users:
        if user.is_active:
            return True
    return False

# ✅ Good: Use any()
def has_any_active(users):
    """Check if any user is active."""
    return any(user.is_active for user in users)
```

**Pattern Matching (Python 3.10+):**
```python
# ✅ Good: Use match/case for complex conditions
def handle_response(response):
    """Handle API response based on status."""
    match response.status:
        case 200:
            return process_success(response.data)
        case 404:
            return handle_not_found()
        case 500 | 502 | 503:
            return handle_server_error()
        case _:
            return handle_unknown_error()
```

**Extract Complex Conditions:**
```python
# ❌ Bad: Complex inline condition
if (user.age >= 18 and user.country == "USA" and 
    user.has_license and not user.is_suspended):
    allow_driving()

# ✅ Good: Named function for clarity
def can_drive_in_usa(user: User) -> bool:
    """Check if user meets requirements to drive in USA."""
    return (
        user.age >= 18
        and user.country == "USA"
        and user.has_license
        and not user.is_suspended
    )

if can_drive_in_usa(user):
    allow_driving()
```

## Notes
- Names should explain "why", not just "what"
- Avoid mental mapping (e.g., `i` for iterator in complex logic)
- Consistency is key: stick to naming patterns across the codebase
- Readability trumps brevity
- Code is read far more often than written
