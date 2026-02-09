# File Explorer

## Responsibility
Provide a unified interface for exploring multiple file formats and databases. This skill acts as a facade, delegating parsing and reading tasks to format-specific skills while handling common file operations like detection, validation, and error management.

## Detailed Behavior
1.  **File/Database Detection**:
    -   Identify the file type based on extension (e.g., .pdf, .xml, .html) or MIME type.
    -   Detect database connection strings or configuration files.
2.  **Validation**:
    -   Verify file existence and read permissions.
    -   Ensure the file is not empty or corrupted (basic checks).
3.  **Delegation**:
    -   Based on the detected type, delegate the reading/parsing task to the appropriate format-specific skill (e.g., `pdf_explorer`, `csv_explorer`, `db_explorer`).
    -   If no specific explorer exists, attempt a generic text read or binary read if applicable, or return a "format not supported" error.
4.  **Error Handling**:
    -   Catch common errors: `FileNotFoundError`, `PermissionError`, `UnicodeDecodeError`.
    -   Handle format-specific parsing errors returned by delegates.
5.  **Logging**:
    -   Log the start of exploration (file path/target).
    -   Log the detected type and the delegate being used.
    -   Log successful extraction or detailed error messages.

## Example Usage
```python
from agent.skills.file_exploration import FileExplorer

explorer = FileExplorer()

# Explore a PDF
result = explorer.explore("documents/report.pdf")
print(result) # Output from PDF Explorer

# Explore a CSV
data = explorer.explore("data/sales.csv")
print(data) # Output from CSV Explorer

# Handle unsupported
try:
    explorer.explore("unknown.xyz")
except ValueError as e:
    print(e)
```
