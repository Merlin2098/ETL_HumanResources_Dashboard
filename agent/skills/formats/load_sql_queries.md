# Skill: SQL Query Loader

## Overview
Load and manage SQL query files from a dedicated queries directory for reuse across ETL workflows. Provides centralized query management and version control.

## When to Use This Skill
- Loading parameterized SQL queries for execution
- Managing reusable query templates
- Separating SQL logic from Python code
- Version-controlling SQL queries
- Building query libraries for data pipelines

## When NOT to Use This Skill
- Inline SQL queries (1-2 lines)
- Dynamic query construction
- When SQL is generated programmatically

## Implementation

```python
from pathlib import Path
from typing import Optional

def sql_query_loader(
    queries_dir: str,
    query_name: str,
    file_extension: str = ".sql",
    encoding: str = "utf-8"
) -> str:
    """
    Load SQL query from dedicated queries directory.
    
    Args:
        queries_dir: Path to queries folder (e.g., "./queries")
        query_name: Name of query file without extension
        file_extension: Query file extension (default: ".sql")
        encoding: File encoding (default: "utf-8")
    
    Returns:
        str containing SQL query text
    
    Raises:
        FileNotFoundError: If query file does not exist
        UnicodeDecodeError: If encoding is incorrect
    """
    # Construct file path
    query_path = Path(queries_dir) / f"{query_name}{file_extension}"
    
    # Validate file exists
    if not query_path.exists():
        raise FileNotFoundError(
            f"Query file not found: {query_path}\n"
            f"Queries directory: {Path(queries_dir).absolute()}"
        )
    
    # Read query file
    try:
        with open(query_path, 'r', encoding=encoding) as f:
            sql_query = f.read()
        
        return sql_query
    
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(
            e.encoding, e.object, e.start, e.end,
            f"Failed to decode {query_path} with encoding {encoding}"
        )

```

## Directory Structure

```
project/
├── queries/
│   ├── get_customers.sql
│   ├── calculate_revenue.sql
│   ├── transform_sales.sql
│   ├── aggregations/
│   │   ├── daily_summary.sql
│   │   └── monthly_rollup.sql
│   └── reports/
│       ├── executive_dashboard.sql
│       └── weekly_metrics.sql
├── main.py
└── etl_pipeline.py
```

## Usage Examples

```python
# Example 1: Load simple query
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="get_customers"
)
print(sql)
# Output: SELECT * FROM customers WHERE active = true

# Example 2: Load query from subdirectory
sql = sql_query_loader(
    queries_dir="./queries/aggregations",
    query_name="daily_summary"
)

# Example 3: Load query with custom extension
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="complex_report",
    file_extension=".txt"
)

# Example 4: Load query with different encoding
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="legacy_query",
    encoding="latin-1"
)

# Example 5: Use in ETL pipeline
import duckdb

# Load query
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="transform_sales"
)

# Execute query
conn = duckdb.connect()
result = conn.execute(sql).fetchdf()
```

## Query File Best Practices

### 1. Parameterized Queries

```sql
-- queries/get_sales_by_date.sql
SELECT 
    order_id,
    customer_id,
    total_amount,
    order_date
FROM sales
WHERE order_date >= '{start_date}'
  AND order_date < '{end_date}'
```

```python
# Load and parameterize
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="get_sales_by_date"
)

# Substitute parameters
sql_final = sql.format(
    start_date="2024-01-01",
    end_date="2024-02-01"
)
```

### 2. Documented Queries

```sql
-- queries/complex_transform.sql
/*
Query: Complex Sales Transformation
Author: Data Team
Created: 2024-01-15
Description: Transforms raw sales data with deduplication and aggregation
Dependencies: bronze.sales, bronze.customers
Output: silver.sales_transformed
*/

WITH deduplicated AS (
    SELECT DISTINCT
        order_id,
        customer_id,
        amount
    FROM bronze.sales
),
aggregated AS (
    SELECT 
        customer_id,
        SUM(amount) as total_amount,
        COUNT(*) as order_count
    FROM deduplicated
    GROUP BY customer_id
)
SELECT * FROM aggregated
```

### 3. Modular Queries

```sql
-- queries/base_filters.sql
-- Reusable WHERE clause
WHERE 
    is_active = true
    AND deleted_at IS NULL
    AND created_at >= CURRENT_DATE - INTERVAL '90 days'
```

## Advanced Usage Patterns

### Pattern 1: Query Library

```python
class QueryLibrary:
    """Centralized query management"""
    
    def __init__(self, queries_dir: str):
        self.queries_dir = queries_dir
    
    def get(self, query_name: str) -> str:
        """Load query by name"""
        return sql_query_loader(
            queries_dir=self.queries_dir,
            query_name=query_name
        )
    
    def list_queries(self) -> list:
        """List all available queries"""
        queries_path = Path(self.queries_dir)
        return [f.stem for f in queries_path.glob("*.sql")]

# Usage
library = QueryLibrary("./queries")
sql = library.get("transform_sales")
available = library.list_queries()
print(f"Available queries: {available}")
```

### Pattern 2: Query Validator

```python
def validate_query_syntax(sql: str) -> dict:
    """Validate SQL syntax before execution"""
    import duckdb
    
    try:
        conn = duckdb.connect(":memory:")
        conn.execute(f"EXPLAIN {sql}")
        return {"valid": True, "error": None}
    except Exception as e:
        return {"valid": False, "error": str(e)}

# Load and validate
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="complex_report"
)

validation = validate_query_syntax(sql)
if not validation["valid"]:
    print(f"Query validation failed: {validation['error']}")
```

### Pattern 3: Query Caching

```python
from functools import lru_cache

@lru_cache(maxsize=32)
def cached_query_loader(queries_dir: str, query_name: str) -> str:
    """Load query with caching for performance"""
    return sql_query_loader(
        queries_dir=queries_dir,
        query_name=query_name
    )

# First call: reads from file
sql = cached_query_loader("./queries", "get_customers")

# Second call: returns from cache
sql = cached_query_loader("./queries", "get_customers")  # Instant
```

## Error Handling

```python
def safe_sql_loader(queries_dir: str, query_name: str) -> dict:
    """Load SQL query with error handling"""
    
    try:
        sql = sql_query_loader(
            queries_dir=queries_dir,
            query_name=query_name
        )
        
        return {
            "success": True,
            "query": sql,
            "lines": len(sql.split('\n')),
            "size_bytes": len(sql.encode('utf-8'))
        }
    
    except FileNotFoundError as e:
        return {
            "success": False,
            "error": "FileNotFoundError",
            "message": str(e),
            "query": None
        }
    
    except UnicodeDecodeError as e:
        return {
            "success": False,
            "error": "UnicodeDecodeError",
            "message": f"Encoding error: {e}",
            "query": None
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": "UnexpectedError",
            "message": str(e),
            "query": None
        }
```

## Multi-Statement Queries

```sql
-- queries/multi_step_transform.sql
-- Create temporary table
CREATE TEMP TABLE temp_sales AS
SELECT * FROM bronze.sales
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days';

-- Transform data
INSERT INTO silver.sales_summary
SELECT 
    customer_id,
    SUM(amount) as total_amount
FROM temp_sales
GROUP BY customer_id;

-- Clean up
DROP TABLE temp_sales;
```

```python
# Load multi-statement query
sql = sql_query_loader(
    queries_dir="./queries",
    query_name="multi_step_transform"
)

# Split and execute each statement
statements = [s.strip() for s in sql.split(';') if s.strip()]
for stmt in statements:
    conn.execute(stmt)
```

## Best Practices

1. **Organize queries by purpose**
   ```
   queries/
   ├── extract/       # Data extraction queries
   ├── transform/     # Transformation queries
   ├── load/          # Data loading queries
   └── reports/       # Reporting queries
   ```

2. **Use consistent naming**
   ```
   get_customers.sql          # Read operations
   transform_sales.sql        # Transformations
   load_warehouse.sql         # Write operations
   ```

3. **Add comments and documentation**
   ```sql
   -- Purpose: Daily sales aggregation
   -- Input: bronze.sales
   -- Output: silver.daily_sales
   -- Schedule: Daily at 2 AM
   SELECT ...
   ```

4. **Version control queries**
   ```bash
   git add queries/
   git commit -m "Add sales transformation query"
   ```

## Performance Characteristics

- **Load Time**: <1ms for typical queries (<100 KB)
- **Memory Usage**: Negligible (query text only)
- **No Caching**: Reads from disk every call (use wrapper for caching)

## Dependencies

```bash
# No additional dependencies required
# Uses Python standard library only
```

## Common Pitfalls

1. **Hardcoded paths**
   ```python
   # Bad: Absolute path
   sql = sql_query_loader("/home/user/queries", "query")
   
   # Good: Relative path
   sql = sql_query_loader("./queries", "query")
   ```

2. **Missing file extension**
   ```python
   # Skill adds .sql automatically
   sql = sql_query_loader("./queries", "get_data")  # Looks for get_data.sql
   ```

3. **Encoding issues with special characters**
   ```python
   # If query has special characters
   sql = sql_query_loader(
       queries_dir="./queries",
       query_name="special_chars",
       encoding="utf-8"  # Ensure UTF-8
   )
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- User wants to execute a named query
- Building reusable ETL pipelines
- Loading SQL templates for parameterization
- Query logic is stored separately from code

Agents should NOT use this skill when:
- Query is simple and inline is clearer
- SQL is generated dynamically
- Query is one-time use
