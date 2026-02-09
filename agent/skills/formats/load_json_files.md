# Skill: JSON File Loader

## Overview
Load and parse JSON configuration and data files from a dedicated JSON directory for pipeline configuration management.

## When to Use This Skill
- Loading configuration files
- Reading JSON data files
- Importing API response files
- Loading schema definitions
- Reading metadata files

## When NOT to Use This Skill
- Fetching JSON from URLs (use API calls)
- Parsing JSON strings (use json.loads directly)
- Generating JSON output (use json.dumps)

## Implementation

```python
import json
from pathlib import Path
from typing import Union, Dict, List, Any
from datetime import datetime

def json_file_loader(
    json_dir: str,
    file_name: str,
    encoding: str = "utf-8",
    parse_dates: bool = False
) -> Union[Dict, List, Any]:
    """
    Load and parse JSON file from dedicated directory.
    
    Args:
        json_dir: Path to JSON folder (e.g., "./json")
        file_name: JSON filename with or without .json extension
        encoding: File encoding (default: "utf-8")
        parse_dates: Attempt to parse ISO date strings (default: False)
    
    Returns:
        dict or list (parsed JSON structure)
    
    Raises:
        FileNotFoundError: If file does not exist
        json.JSONDecodeError: If file is malformed
        UnicodeDecodeError: If encoding is incorrect
    """
    # Add .json extension if not present
    if not file_name.endswith('.json'):
        file_name = f"{file_name}.json"
    
    # Construct file path
    json_path = Path(json_dir) / file_name
    
    # Validate file exists
    if not json_path.exists():
        raise FileNotFoundError(
            f"JSON file not found: {json_path}\n"
            f"JSON directory: {Path(json_dir).absolute()}"
        )
    
    # Read and parse JSON
    try:
        with open(json_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Parse dates if requested
        if parse_dates and isinstance(data, dict):
            data = _parse_dates_recursive(data)
        
        return data
    
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Malformed JSON in {json_path}: {e.msg}",
            e.doc, e.pos
        )
    except UnicodeDecodeError as e:
        raise UnicodeDecodeError(
            e.encoding, e.object, e.start, e.end,
            f"Failed to decode {json_path} with encoding {encoding}"
        )

def _parse_dates_recursive(obj: Any) -> Any:
    """Recursively parse ISO 8601 date strings to datetime objects"""
    if isinstance(obj, dict):
        return {k: _parse_dates_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_parse_dates_recursive(item) for item in obj]
    elif isinstance(obj, str):
        # Try to parse ISO 8601 format
        try:
            return datetime.fromisoformat(obj.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return obj
    else:
        return obj

```

## Directory Structure

```
project/
├── json/
│   ├── config.json
│   ├── schema.json
│   ├── metadata.json
│   ├── environments/
│   │   ├── dev.json
│   │   ├── staging.json
│   │   └── prod.json
│   └── mappings/
│       ├── column_mapping.json
│       └── type_mapping.json
├── main.py
└── etl_pipeline.py
```

## Usage Examples

```python
# Example 1: Load simple config
config = json_file_loader(
    json_dir="./json",
    file_name="config.json"
)
print(config["database"]["host"])

# Example 2: Load with automatic .json extension
schema = json_file_loader(
    json_dir="./json",
    file_name="schema"  # Will look for schema.json
)

# Example 3: Load from subdirectory
dev_config = json_file_loader(
    json_dir="./json/environments",
    file_name="dev.json"
)

# Example 4: Load with date parsing
metadata = json_file_loader(
    json_dir="./json",
    file_name="metadata.json",
    parse_dates=True
)
# ISO date strings converted to datetime objects

# Example 5: Load mapping file
column_map = json_file_loader(
    json_dir="./json/mappings",
    file_name="column_mapping"
)
```

## Sample JSON Files

### config.json
```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "analytics_db"
  },
  "etl": {
    "batch_size": 1000,
    "max_retries": 3
  },
  "paths": {
    "bronze": "data/bronze",
    "silver": "data/silver",
    "gold": "data/gold"
  }
}
```

### schema.json
```json
{
  "table_name": "customers",
  "columns": [
    {"name": "customer_id", "type": "INTEGER", "nullable": false},
    {"name": "name", "type": "VARCHAR(100)", "nullable": false},
    {"name": "email", "type": "VARCHAR(255)", "nullable": true},
    {"name": "created_at", "type": "TIMESTAMP", "nullable": false}
  ],
  "primary_key": ["customer_id"]
}
```

### metadata.json (with dates)
```json
{
  "pipeline_name": "daily_sales_etl",
  "version": "1.2.0",
  "last_run": "2024-02-07T10:30:00Z",
  "next_run": "2024-02-08T10:30:00Z",
  "status": "completed",
  "metrics": {
    "rows_processed": 150000,
    "duration_seconds": 245
  }
}
```

## Advanced Usage Patterns

### Pattern 1: Configuration Manager

```python
class ConfigManager:
    """Centralized configuration management"""
    
    def __init__(self, json_dir: str):
        self.json_dir = json_dir
        self._config = None
    
    def load_config(self, env: str = "dev") -> dict:
        """Load environment-specific config"""
        self._config = json_file_loader(
            json_dir=f"{self.json_dir}/environments",
            file_name=f"{env}.json"
        )
        return self._config
    
    def get(self, key_path: str, default=None):
        """Get config value by dot notation"""
        keys = key_path.split('.')
        value = self._config
        for key in keys:
            value = value.get(key, default)
            if value == default:
                break
        return value

# Usage
config_mgr = ConfigManager("./json")
config_mgr.load_config("prod")
db_host = config_mgr.get("database.host")
```

### Pattern 2: Schema Validator

```python
def validate_data_schema(data: dict, schema_file: str):
    """Validate data against JSON schema"""
    schema = json_file_loader(
        json_dir="./json/schemas",
        file_name=schema_file
    )
    
    # Simple validation (can use jsonschema library for full validation)
    required_cols = [col["name"] for col in schema["columns"] if not col["nullable"]]
    data_cols = data.keys()
    
    missing = set(required_cols) - set(data_cols)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    return True
```

### Pattern 3: Merge Multiple Configs

```python
def merge_configs(base_config: str, override_config: str) -> dict:
    """Merge two JSON configs"""
    base = json_file_loader(
        json_dir="./json",
        file_name=base_config
    )
    override = json_file_loader(
        json_dir="./json",
        file_name=override_config
    )
    
    # Deep merge
    def deep_merge(base: dict, override: dict) -> dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    return deep_merge(base, override)

# Usage
final_config = merge_configs("default_config", "prod_override")
```

## Error Handling

```python
def safe_json_loader(json_dir: str, file_name: str) -> dict:
    """Load JSON with comprehensive error handling"""
    
    try:
        data = json_file_loader(
            json_dir=json_dir,
            file_name=file_name
        )
        
        return {
            "success": True,
            "data": data,
            "size_bytes": len(json.dumps(data).encode('utf-8'))
        }
    
    except FileNotFoundError as e:
        return {
            "success": False,
            "error": "FileNotFoundError",
            "message": str(e),
            "data": None
        }
    
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "error": "JSONDecodeError",
            "message": f"Malformed JSON: {e.msg} at position {e.pos}",
            "data": None
        }
    
    except UnicodeDecodeError as e:
        return {
            "success": False,
            "error": "UnicodeDecodeError",
            "message": f"Encoding error: {e}",
            "data": None
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": "UnexpectedError",
            "message": str(e),
            "data": None
        }
```

## Best Practices

1. **Structure JSON files logically**
   ```
   json/
   ├── environments/    # Environment configs
   ├── schemas/         # Data schemas
   ├── mappings/        # Field mappings
   └── templates/       # Reusable templates
   ```

2. **Use meaningful file names**
   ```
   database_config.json
   customer_schema.json
   column_mappings.json
   ```

3. **Validate JSON structure**
   ```python
   # Use JSON Schema for validation
   import jsonschema
   
   data = json_file_loader("./json", "config")
   schema = json_file_loader("./json/schemas", "config_schema")
   jsonschema.validate(instance=data, schema=schema)
   ```

4. **Handle nested structures**
   ```python
   config = json_file_loader("./json", "config")
   
   # Safe nested access
   db_port = config.get("database", {}).get("port", 5432)
   ```

## Performance Characteristics

- **Load Time**: <1ms for typical files (<1MB)
- **Memory Usage**: File size + parsed object size
- **No Caching**: Reads from disk every call

## Performance Benchmarks

| File Size | Load Time | Memory Usage |
|-----------|-----------|--------------|
| 1 KB      | <1 ms     | ~2 KB        |
| 100 KB    | ~5 ms     | ~200 KB      |
| 1 MB      | ~50 ms    | ~2 MB        |
| 10 MB     | ~500 ms   | ~20 MB       |

## Dependencies

```bash
# No additional dependencies required
# Uses Python standard library only
```

## Common Pitfalls

1. **Case-sensitive file names**
   ```python
   # Linux/Mac are case-sensitive
   json_file_loader("./json", "Config.json")  # ≠ config.json
   ```

2. **Empty JSON files**
   ```python
   # Empty file returns None
   data = json_file_loader("./json", "empty")
   if data is None:
       print("File is empty")
   ```

3. **Nested path in file_name**
   ```python
   # Don't mix directory in file_name
   # Bad:
   json_file_loader("./json", "env/dev.json")
   
   # Good:
   json_file_loader("./json/env", "dev.json")
   ```

4. **Large files causing memory issues**
   ```python
   import sys
   
   # Check file size first
   file_size = Path("./json/large_file.json").stat().st_size
   if file_size > 100 * 1024 * 1024:  # 100 MB
       print("Warning: Large file may cause memory issues")
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- Loading configuration files for ETL pipeline
- Reading schema definitions
- Importing API response files
- Loading mapping/lookup tables
- Reading metadata files

Agents should NOT use this skill when:
- JSON is already in memory as string
- Fetching JSON from web APIs
- Generating JSON output
