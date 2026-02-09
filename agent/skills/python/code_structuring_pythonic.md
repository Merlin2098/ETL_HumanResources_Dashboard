# Skill: code_structuring_pythonic

The agent structures code following Pythonic principles and SOLID design patterns.

## Responsibility
Organize code with proper separation of concerns, using functions for atomic operations and classes for stateful behavior.

## Rules
- Use functions for small, single-purpose operations
- Use classes when encapsulating state and related behaviors
- Keep functions focused on one responsibility (SRP)
- Apply SOLID principles for classes and modules
- Prefer composition over inheritance
- Use dataclasses or Pydantic for data structures
- Keep modules cohesive and loosely coupled
- Avoid deep nesting; extract logic into functions
- Write self-documenting code with clear names

## Behavior

### Step 1: Choose Between Functions and Classes
- Use **functions** for:
  - Stateless transformations
  - Utility operations
  - Simple data processing
  - Pure logic without side effects
  
- Use **classes** for:
  - Encapsulating related state and behavior
  - Implementing interfaces or protocols
  - Managing resources (files, connections)
  - Complex objects with multiple methods

### Step 2: Apply Single Responsibility Principle (SRP)
- Each function should do one thing well
- Each class should have one reason to change
- Extract helper functions for complex logic
- Split large classes into smaller, focused ones

### Step 3: Follow Open/Closed Principle (OCP)
- Design for extension without modification
- Use abstract base classes or protocols
- Implement strategy pattern for varying behaviors

### Step 4: Liskov Substitution Principle (LSP)
- Ensure subclasses can replace parent classes
- Maintain contract of base class
- Avoid breaking preconditions or postconditions

### Step 5: Interface Segregation Principle (ISP)
- Create small, focused interfaces
- Don't force classes to implement unused methods
- Use protocols for defining interfaces

### Step 6: Dependency Inversion Principle (DIP)
- Depend on abstractions, not concrete implementations
- Inject dependencies rather than creating them
- Use dependency injection for testability

## Example Usage

**Functions for Atomic Operations:**
```python
from typing import List, Dict

def calculate_total(prices: List[float]) -> float:
    """Calculate sum of prices."""
    return sum(prices)

def apply_discount(price: float, discount_percent: float) -> float:
    """Apply discount to a price."""
    return price * (1 - discount_percent / 100)

def format_currency(amount: float) -> str:
    """Format amount as currency string."""
    return f"${amount:.2f}"
```

**Classes for State Encapsulation:**
```python
from dataclasses import dataclass
from typing import List
from decimal import Decimal

@dataclass
class Product:
    name: str
    price: Decimal
    quantity: int

class ShoppingCart:
    """Manages items in a shopping cart."""
    
    def __init__(self):
        self._items: List[Product] = []
    
    def add_item(self, product: Product) -> None:
        """Add product to cart."""
        self._items.append(product)
    
    def remove_item(self, product_name: str) -> bool:
        """Remove product from cart by name."""
        for i, item in enumerate(self._items):
            if item.name == product_name:
                self._items.pop(i)
                return True
        return False
    
    def calculate_total(self) -> Decimal:
        """Calculate total cost of items in cart."""
        return sum(item.price * item.quantity for item in self._items)
    
    @property
    def item_count(self) -> int:
        """Get total number of items."""
        return len(self._items)
```

**SOLID Principles Example:**
```python
from abc import ABC, abstractmethod
from typing import Protocol

# Interface Segregation & Dependency Inversion
class PaymentProcessor(Protocol):
    """Payment processing interface."""
    def process_payment(self, amount: float) -> bool:
        ...

class StripePaymentProcessor:
    """Concrete implementation for Stripe."""
    def process_payment(self, amount: float) -> bool:
        # Stripe-specific logic
        return True

class PayPalPaymentProcessor:
    """Concrete implementation for PayPal."""
    def process_payment(self, amount: float) -> bool:
        # PayPal-specific logic
        return True

# Single Responsibility & Dependency Inversion
class OrderService:
    """Handles order processing."""
    
    def __init__(self, payment_processor: PaymentProcessor):
        self._payment_processor = payment_processor
    
    def process_order(self, cart: ShoppingCart) -> bool:
        """Process an order from a shopping cart."""
        total = cart.calculate_total()
        return self._payment_processor.process_payment(float(total))

# Usage with dependency injection
stripe_processor = StripePaymentProcessor()
order_service = OrderService(stripe_processor)
```

**Composition Over Inheritance:**
```python
# Instead of complex inheritance hierarchies, use composition

class Logger:
    def log(self, message: str) -> None:
        print(f"LOG: {message}")

class Validator:
    def validate(self, data: dict) -> bool:
        return all(data.values())

class DataProcessor:
    """Uses composition rather than inheriting from Logger and Validator."""
    
    def __init__(self, logger: Logger, validator: Validator):
        self._logger = logger
        self._validator = validator
    
    def process(self, data: dict) -> bool:
        self._logger.log("Starting processing")
        
        if not self._validator.validate(data):
            self._logger.log("Validation failed")
            return False
        
        self._logger.log("Processing completed")
        return True
```

**Dataclasses for Data Structures:**
```python
from dataclasses import dataclass, field
from typing import List
from datetime import datetime

@dataclass
class Address:
    street: str
    city: str
    zip_code: str
    country: str = "USA"

@dataclass
class Customer:
    id: int
    name: str
    email: str
    addresses: List[Address] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    
    def add_address(self, address: Address) -> None:
        """Add an address to customer."""
        self.addresses.append(address)
```

## Notes
- Pythonic code is readable and follows Python idioms
- Use list comprehensions instead of loops when appropriate
- Leverage built-in functions: `map()`, `filter()`, `zip()`, `enumerate()`
- Use context managers (`with` statement) for resource management
- Follow PEP 8 style guide
- Keep functions short (ideally under 20 lines)
