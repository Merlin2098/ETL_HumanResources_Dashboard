# Skill: Excel Read (Pandas - Exploratory)

## Overview
Load Excel files into memory for exploratory analysis, schema inspection, and interactive data exploration where performance is secondary to convenience.

## When to Use This Skill
- Quick data exploration and ad-hoc analysis
- Small to medium Excel files (<50MB)
- Interactive data science workflows
- When you need pandas-specific functionality
- Schema inspection and data profiling

## When NOT to Use This Skill
- Large files (>100MB) - use `excel_read_openpyxl_polars_performance` instead
- Production ETL pipelines requiring high performance
- Memory-constrained environments
- When you need maximum read speed

## Implementation

```python
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional

def excel_read_pandas_exploratory(
    file_path: str,
    sheet_name: Union[str, int, None] = None,
    header_row: Optional[int] = 0,
    usecols: Union[str, List, None] = None,
    nrows: Optional[int] = None
) -> pd.DataFrame:
    """
    Load Excel file using pandas for exploratory analysis.
    
    Args:
        file_path: Absolute or relative path to Excel file (.xlsx, .xls)
        sheet_name: Sheet name, index, or None for first sheet
        header_row: Row index to use as column names (0-indexed)
        usecols: Columns to parse (Excel format like "A:E" or list of names)
        nrows: Number of rows to read (None = all rows)
    
    Returns:
        pandas.DataFrame object in memory
    
    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If sheet_name is invalid
    """
    # Validate file exists
    if not Path(file_path).exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")
    
    # Read Excel file
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=header_row,
            usecols=usecols,
            nrows=nrows
        )
        return df
    
    except ValueError as e:
        raise ValueError(f"Invalid sheet name or parameters: {e}")
    except Exception as e:
        raise Exception(f"Error reading Excel file: {e}")

```

## Usage Examples

```python
# Example 1: Read first sheet, all data
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx"
)

# Example 2: Read specific sheet by name
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx",
    sheet_name="Q4_2024"
)

# Example 3: Read specific columns only
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx",
    usecols=["Product", "Revenue", "Date"]
)

# Example 4: Read first 100 rows for preview
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx",
    nrows=100
)

# Example 5: Read sheet by index (0-based)
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx",
    sheet_name=2  # Third sheet
)

# Example 6: Read columns by Excel range
df = excel_read_pandas_exploratory(
    file_path="data/sales.xlsx",
    usecols="A:E"  # Columns A through E
)
```

## Best Practices

1. **File Size Awareness**: Check file size before loading
   ```python
   import os
   file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
   if file_size_mb > 50:
       print(f"Warning: Large file ({file_size_mb:.1f}MB), consider using openpyxl+polars skill")
   ```

2. **Memory Management**: For large files, read in chunks
   ```python
   # Read first N rows to inspect
   preview = excel_read_pandas_exploratory(file_path="large.xlsx", nrows=1000)
   ```

3. **Data Type Inference**: Pandas infers types automatically, but verify
   ```python
   df = excel_read_pandas_exploratory(file_path="data.xlsx")
   print(df.dtypes)  # Check inferred types
   print(df.info())  # Memory usage and types
   ```

4. **Handle Missing Values**: Pandas converts empty cells to NaN
   ```python
   df = excel_read_pandas_exploratory(file_path="data.xlsx")
   print(df.isna().sum())  # Count missing values per column
   ```

## Error Handling

```python
from pathlib import Path

def safe_excel_read(file_path: str, **kwargs):
    """Wrapper with comprehensive error handling"""
    try:
        # Check file exists
        if not Path(file_path).exists():
            return {"error": "FileNotFoundError", "message": f"File not found: {file_path}"}
        
        # Check file extension
        if not file_path.endswith(('.xlsx', '.xls')):
            return {"error": "ValueError", "message": "File must be .xlsx or .xls"}
        
        # Read file
        df = excel_read_pandas_exploratory(file_path, **kwargs)
        
        return {
            "success": True,
            "dataframe": df,
            "rows": len(df),
            "columns": len(df.columns)
        }
    
    except FileNotFoundError as e:
        return {"error": "FileNotFoundError", "message": str(e)}
    except ValueError as e:
        return {"error": "ValueError", "message": str(e)}
    except Exception as e:
        return {"error": "UnexpectedError", "message": str(e)}
```

## Performance Characteristics

- **Speed**: Medium (slower than openpyxl+polars for large files)
- **Memory**: High (entire dataset loaded into RAM)
- **File Size Limits**:
  - Comfortable: <10MB
  - Acceptable: 10-50MB
  - Avoid: >100MB

## Dependencies

```bash
pip install pandas openpyxl xlrd
```

- `pandas`: Core library
- `openpyxl`: For .xlsx files
- `xlrd`: For .xls files (legacy format)

## Common Pitfalls

1. **Wrong sheet name**: Causes ValueError
   ```python
   # List available sheets first
   xl_file = pd.ExcelFile("data.xlsx")
   print(xl_file.sheet_names)
   ```

2. **Memory exhaustion**: Large files crash
   - Solution: Use `nrows` parameter or switch to openpyxl+polars skill

3. **Date parsing issues**: Dates may be read as strings
   ```python
   # Manually parse dates if needed
   df['date_column'] = pd.to_datetime(df['date_column'])
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- User requests exploratory data analysis
- File size is small/medium (<50MB)
- Pandas-specific features are needed
- Interactive analysis is the goal

Agents should NOT use this skill for:
- Production ETL jobs
- Large file processing
- Performance-critical workflows
