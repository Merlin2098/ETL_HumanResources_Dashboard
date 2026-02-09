# JSON Explorer

## Responsibility
Parse and validate JSON data, supporting large files via streaming and schema validation.

## Detailed Behavior
1.  **Parsing**:
    -   Load JSON content from file.
    -   For small files: Load fully into memory.
    -   For large files: Use a streaming parser (e.g., `ijson`) to iterate over records without loading the entire file.
2.  **Validation**:
    -   Check for syntax errors (bracket mismatch, trailing commas).
    -   (Optional) Validate against a provided JSON Schema.
3.  **Structure Analysis**:
    -   Identify the root structure (object vs. array).
    -   Extract keys or infer schema from the data if explicit schema is missing.
4.  **Access/Query**:
    -   Support JSONPath queries to extract specific subsets of data.

## Example Usage
```python
from agent.skills.file_exploration import JSONExplorer

json_tool = JSONExplorer()

# Standard load
data = json_tool.load("users.json")

# Streaming large file
for record in json_tool.stream("large_log_dump.json", item_path="logs.item"):
    process(record)

# Validation
is_valid = json_tool.validate("payload.json", schema="schema.json")
```
