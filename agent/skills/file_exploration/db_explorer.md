# Database Explorer

## Responsibility
Connect to and explore relational (SQL) and NoSQL databases to discover schemas and extract data.

## Detailed Behavior
1.  **Connection Management**:
    -   Establish connections using connection strings or provided credentials.
    -   Support common dialects: PostgreSQL, MySQL, SQLite, SQL Server.
    -   Handle connection timeouts and authentication errors.
2.  **Schema Discovery**:
    -   List available tables and views.
    -   Describe table schemas: column names, types, primary keys, and foreign keys.
3.  **Query Execution**:
    -   Execute `SELECT` queries safely (read-only mode enforced where possible).
    -   Support parameterized queries to prevent injection.
    -   Handle query errors (syntax, runtime).
4.  **Data Extraction**:
    -   Fetch query results as list of dictionaries or DataFrames.
    -   Handle data type conversion (decimals, datetimes) to Python native types.
5.  **NoSQL Support (Optional/Extensible)**:
    -   Adapt behaviors for document stores (MongoDB) using collection listings and `find` operations.

## Example Usage
```python
from agent.skills.file_exploration import DBExplorer

db_tool = DBExplorer()
db_tool.connect("postgresql://user:pass@localhost:5432/mydb")

# Discovery
tables = db_tool.list_tables()

# Query
data = db_tool.query("SELECT * FROM users WHERE active = %s", params=(True,))
```
