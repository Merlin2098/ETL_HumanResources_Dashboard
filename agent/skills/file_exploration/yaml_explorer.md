# YAML Explorer

## Responsibility
Parse YAML configuration files safely, handling complex hierarchies, anchors, and aliases.

## Detailed Behavior
1.  **Safe Parsing**:
    -   Use `safe_load` to prevent code execution vulnerabilities.
    -   Handle YAML syntax errors (indentation issues) with clear feedback.
2.  **Structure Handling**:
    -   Resolve anchors and aliases correctly to fully expand the data structure.
    -   Maintain hierarchy and data types (booleans, timestamps, nulls).
3.  **Validation**:
    -   Ensure the root element matches expected types (e.g., dict vs list).
    -   Check for duplicate keys (which standard YAML parsers might overwrite silently).
4.  **Navigation**:
    -   Allow dot-notation access to nested configuration properties.

## Example Usage
```python
from agent.skills.file_exploration import YAMLExplorer

yaml_tool = YAMLExplorer()
config = yaml_tool.load("deploy.yaml")

# Auto-resolves aliases
db_host = config.get("database.primary.host")
```
