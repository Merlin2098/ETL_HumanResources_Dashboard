# Skill: externalized_logic_handler

The agent extracts business logic from code into external definitions (JSON, YAML, SQL) to improve flexibility and maintainability.

## Responsibility
Load, parse, validate, and execute logic defined in external files, allowing rule updates without code deployment.

## Rules
- Defne volatile business rules in external files (JSON, YAML)
- Use SQL files for complex data retrieval logic
- Validate external definitions against schemas at runtime
- Cache loaded logic to avoid IO penalties
- Provide a consistent interface for executing external rules
- Sandbox execution environments if evaluating dynamic expressions (use `ast.literal_eval` or specialized libraries, avoid `eval()`)
- Version external logic files

## Behavior

### Step 1: Define External Formats
- Choose appropriate format:
  - **YAML/JSON**: Configuration, decision trees, simple rules
  - **SQL**: Data selection, filtering, aggregation
  - **Python Scripts (Restricted)**: Plugin-based logic (use with extreme caution)

### Step 2: Implement Logic Loader
- Create a loader to read files from disk or remote storage
- implement caching strategy (TTL or file watcher)
- Parse content into internal data structures
- Handle missing or malformed files gracefully

### Step 3: Validate Logic Definitions
- Use Pydantic or Schema libraries to validate structure
- Check logical consistency (e.g., no circular references)
- Fail fast if rules are invalid

### Step 4: Execute Logic
- **For Rules**: Evaluate conditions against context data
- **For SQL**: Parametrize queries and execute safely
- **For Workflows**: interpret steps and execute actions

## Example Usage

**External Rule Definition (YAML):**
```yaml
# rules/pricing.yaml
rules:
  - name: "Bulk Discount"
    condition: "quantity > 100"
    action: "apply_discount(0.15)"
    priority: 1
  - name: "VIP Customer"
    condition: "customer_tier == 'VIP'"
    action: "apply_discount(0.10)"
    priority: 2
```

**Rule Engine Implementation:**
```python
import yaml
from typing import List, Dict, Any
from pydantic import BaseModel
import simpleeval  # Safe expression evaluation

class Rule(BaseModel):
    name: str
    condition: str
    action: str
    priority: int

class RuleEngine:
    def __init__(self, rules_path: str):
        self.rules = self._load_rules(rules_path)
    
    def _load_rules(self, path: str) -> List[Rule]:
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        rules = [Rule(**r) for r in data['rules']]
        return sorted(rules, key=lambda x: x.priority)

    def evaluate(self, context: Dict[str, Any]) -> List[str]:
        """Evaluate rules against context and return actions to take."""
        actions = []
        for rule in self.rules:
            # Safe evaluation of condition string
            if simpleeval.simple_eval(rule.condition, names=context):
                actions.append(rule.action)
        return actions

# Usage
engine = RuleEngine('rules/pricing.yaml')
context = {'quantity': 150, 'customer_tier': 'REGULAR'}
actions = engine.evaluate(context)
print(f"Triggered actions: {actions}")
# Output: ['apply_discount(0.15)']
```

**External SQL Management:**
```python
# queries/get_high_value_users.sql
# -- description: Get users who spent more than :amount in last :days
SELECT 
    u.id, u.email, sum(o.total) as total_spend
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.created_at > date('now', '-' || :days || ' days')
GROUP BY u.id
HAVING total_spend > :amount
```

**SQL Loader and Executor:**
```python
import sqlite3
from pathlib import Path

class SQLLoader:
    def __init__(self, query_dir: str, db_path: str):
        self.query_dir = Path(query_dir)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def get_query(self, query_name: str) -> str:
        """Load SQL from file."""
        file_path = self.query_dir / f"{query_name}.sql"
        if not file_path.exists():
            raise FileNotFoundError(f"Query {query_name} not found")
        
        with open(file_path, 'r') as f:
            # Strip comments and empty lines
            return "\n".join(
                line for line in f 
                if not line.strip().startswith("--")
            )

    def execute(self, query_name: str, **params):
        """Execute named query with parameters."""
        sql = self.get_query(query_name)
        cursor = self.conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

# Usage
loader = SQLLoader('queries', 'app.db')
users = loader.execute('get_high_value_users', amount=1000, days=30)
```

**JSON Workflow Definition:**
```json
// workflow.json
{
  "steps": [
    {
      "step_id": "validate",
      "action": "validate_input",
      "next_step": "process"
    },
    {
      "step_id": "process",
      "action": "process_data",
      "next_step": "notify",
      "error_step": "log_error"
    },
    {
      "step_id": "notify",
      "action": "send_email",
      "next_step": null
    }
  ]
}
```

## Notes
- **Security**: Never use `eval()` on external content. Use `simpleeval` or `ast.literal_eval`.
- **Validation**: Schema validation is critical. Invalid external logic crashing the app is a major risk.
- **Testing**: Test the engine with various rule files, not just the hardcoded ones.
- **Hot-swapping**: Implementing a file watcher allows rules to update without restarting the service.
