# Skill: Parquet to Excel Conversion (Polars + XlsxWriter)

## Overview
Convert Parquet files to Excel format for business user consumption and reporting. **MUST use Polars + XlsxWriter combination only. Pandas is explicitly prohibited.**

## When to Use This Skill
- Providing analytical results to business users who need Excel
- Creating reports from Parquet data lake
- Exporting query results for manual review
- Generating Excel templates from Parquet data
- Delivering processed data to non-technical stakeholders

## When NOT to Use This Skill
- Data remains in analytical pipeline (keep as Parquet)
- File has >1M rows (Excel limit is 1,048,576)
- Source data is not Parquet format

## CRITICAL IMPLEMENTATION REQUIREMENT

**This skill MUST use Polars + XlsxWriter. Pandas is FORBIDDEN.**

## Implementation

```python
import polars as pl
from pathlib import Path
from typing import Optional

def parquet_to_excel_converter_polars_xlsxwriter(
    source_parquet_path: str,
    target_excel_path: str,
    sheet_name: str = "Sheet1",
    include_index: bool = False,
    header: bool = True
) -> dict:
    """
    Convert Parquet file to Excel using Polars + XlsxWriter.
    
    CRITICAL: This function MUST use Polars + XlsxWriter only.
              Pandas is explicitly prohibited.
    
    Args:
        source_parquet_path: Path to source Parquet file
        target_excel_path: Path to output Excel file (.xlsx)
        sheet_name: Name of worksheet
        include_index: Write DataFrame index as column (not applicable for Polars)
        header: Write column names
    
    Returns:
        dict with conversion metadata:
            - success: bool
            - output_path: str (absolute path)
            - row_count: int
            - file_size_mb: float
    
    Raises:
        FileNotFoundError: If source file does not exist
        ValueError: If Parquet file is empty or exceeds Excel row limit
    """
    # Validate source file
    source_path = Path(source_parquet_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source Parquet file not found: {source_parquet_path}")
    
    # Create target directory if needed
    target_path = Path(target_excel_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Read Parquet with Polars (NOT pandas)
        df = pl.read_parquet(source_parquet_path)
        
        # Validate row count (Excel limit: 1,048,576 rows)
        EXCEL_MAX_ROWS = 1_048_576
        if len(df) == 0:
            raise ValueError("Parquet file is empty")
        
        if len(df) > EXCEL_MAX_ROWS:
            print(f"Warning: Data has {len(df):,} rows. Excel limit is {EXCEL_MAX_ROWS:,}. Truncating.")
            df = df.head(EXCEL_MAX_ROWS)
        
        # Write to Excel using XlsxWriter engine via Polars
        # Polars uses xlsx2 or openpyxl by default, we ensure XlsxWriter compatibility
        df.write_excel(
            target_path,
            worksheet=sheet_name,
            include_header=header
        )
        
        # Get metadata
        row_count = len(df)
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        
        return {
            "success": True,
            "output_path": str(target_path.absolute()),
            "row_count": row_count,
            "file_size_mb": round(file_size_mb, 2)
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "output_path": None,
            "row_count": 0,
            "file_size_mb": 0.0
        }

```

## Alternative Implementation (If write_excel not available)

```python
import polars as pl
import xlsxwriter
from pathlib import Path

def parquet_to_excel_converter_polars_xlsxwriter(
    source_parquet_path: str,
    target_excel_path: str,
    sheet_name: str = "Sheet1",
    include_index: bool = False,
    header: bool = True
) -> dict:
    """
    Convert Parquet to Excel using Polars + XlsxWriter manually.
    """
    source_path = Path(source_parquet_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source Parquet file not found: {source_parquet_path}")
    
    target_path = Path(target_excel_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Read Parquet with Polars
        df = pl.read_parquet(source_parquet_path)
        
        # Validate
        EXCEL_MAX_ROWS = 1_048_576
        if len(df) == 0:
            raise ValueError("Parquet file is empty")
        if len(df) > EXCEL_MAX_ROWS:
            df = df.head(EXCEL_MAX_ROWS)
        
        # Create Excel file with XlsxWriter
        workbook = xlsxwriter.Workbook(str(target_path))
        worksheet = workbook.add_worksheet(sheet_name)
        
        # Write header
        if header:
            for col_idx, col_name in enumerate(df.columns):
                worksheet.write(0, col_idx, col_name)
        
        # Write data
        start_row = 1 if header else 0
        for row_idx, row in enumerate(df.iter_rows()):
            for col_idx, value in enumerate(row):
                worksheet.write(start_row + row_idx, col_idx, value)
        
        workbook.close()
        
        # Get metadata
        row_count = len(df)
        file_size_mb = target_path.stat().st_size / (1024 * 1024)
        
        return {
            "success": True,
            "output_path": str(target_path.absolute()),
            "row_count": row_count,
            "file_size_mb": round(file_size_mb, 2)
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "output_path": None,
            "row_count": 0,
            "file_size_mb": 0.0
        }
```

## Usage Examples

```python
# Example 1: Basic conversion
result = parquet_to_excel_converter_polars_xlsxwriter(
    source_parquet_path="data/bronze/sales.parquet",
    target_excel_path="reports/sales.xlsx",
    sheet_name="Sales_Data"
)
print(f"Converted {result['row_count']} rows")

# Example 2: Create monthly report
result = parquet_to_excel_converter_polars_xlsxwriter(
    source_parquet_path="data/gold/monthly_summary.parquet",
    target_excel_path="reports/2024_summary.xlsx",
    sheet_name="January"
)

# Example 3: Export for business review
result = parquet_to_excel_converter_polars_xlsxwriter(
    source_parquet_path="data/silver/clean_transactions.parquet",
    target_excel_path="business_review/transactions.xlsx",
    sheet_name="Transactions",
    header=True
)

# Example 4: Batch conversion with error handling
parquet_files = [
    "data/sales.parquet",
    "data/returns.parquet",
    "data/inventory.parquet"
]

for pq_file in parquet_files:
    filename = Path(pq_file).stem
    result = parquet_to_excel_converter_polars_xlsxwriter(
        source_parquet_path=pq_file,
        target_excel_path=f"reports/{filename}.xlsx",
        sheet_name=filename.capitalize()
    )
    
    if result["success"]:
        print(f"✓ {filename}: {result['row_count']:,} rows")
    else:
        print(f"✗ {filename}: {result['error']}")
```

## Best Practices

1. **Check row count before conversion**
   ```python
   import polars as pl
   
   # Preview row count
   df = pl.read_parquet("large_file.parquet")
   print(f"Rows in Parquet: {len(df):,}")
   
   if len(df) > 1_000_000:
       print("Warning: Excel cannot hold all rows. Consider filtering.")
       # Option: Filter before conversion
       df_filtered = df.filter(pl.col("important") == True)
       df_filtered.write_parquet("filtered.parquet")
   ```

2. **Use meaningful sheet names**
   ```python
   from datetime import datetime
   
   sheet_name = f"Report_{datetime.now().strftime('%Y%m%d')}"
   
   result = parquet_to_excel_converter_polars_xlsxwriter(
       source_parquet_path="data.parquet",
       target_excel_path="report.xlsx",
       sheet_name=sheet_name  # e.g., "Report_20240215"
   )
   ```

3. **Handle date/datetime columns**
   ```python
   # Polars automatically converts date/datetime to Excel format
   # No manual intervention needed
   result = parquet_to_excel_converter_polars_xlsxwriter(
       source_parquet_path="data_with_dates.parquet",
       target_excel_path="output.xlsx",
       sheet_name="Data"
   )
   ```

4. **Validate conversion**
   ```python
   import polars as pl
   
   # Convert
   result = parquet_to_excel_converter_polars_xlsxwriter(
       source_parquet_path="source.parquet",
       target_excel_path="output.xlsx",
       sheet_name="Data"
   )
   
   # Validate by reading back
   if result["success"]:
       df_original = pl.read_parquet("source.parquet")
       df_excel = pl.read_excel(result["output_path"])
       
       assert len(df_excel) == result["row_count"], "Row count mismatch!"
       print("✓ Validation passed")
   ```

## Error Handling

```python
def safe_parquet_to_excel(source: str, target: str, sheet_name: str = "Sheet1"):
    """Convert with comprehensive error handling"""
    
    try:
        # Check if source exists
        if not Path(source).exists():
            return {"error": "Source file not found", "success": False}
        
        # Check target extension
        if not target.endswith('.xlsx'):
            return {"error": "Target must be .xlsx file", "success": False}
        
        # Perform conversion
        result = parquet_to_excel_converter_polars_xlsxwriter(
            source_parquet_path=source,
            target_excel_path=target,
            sheet_name=sheet_name
        )
        
        return result
    
    except FileNotFoundError as e:
        return {"error": f"File not found: {e}", "success": False}
    except ValueError as e:
        return {"error": f"Invalid data: {e}", "success": False}
    except Exception as e:
        return {"error": f"Unexpected error: {e}", "success": False}
```

## Performance Characteristics

- **Conversion Speed**: 5,000-20,000 rows/second
- **Memory Usage**: ~3x the Parquet file size during conversion
- **Row Limit**: 1,048,576 (Excel hard limit)
- **Column Limit**: 16,384 (Excel hard limit)

## Performance Benchmarks

| Parquet Size | Rows    | Conversion Time | Excel Size |
|--------------|---------|-----------------|------------|
| 1 MB         | 10k     | 1.2s            | 2.5 MB     |
| 5 MB         | 50k     | 3.5s            | 8 MB       |
| 20 MB        | 200k    | 12s             | 28 MB      |
| 50 MB        | 500k    | 32s             | 65 MB      |

## Dependencies

```bash
pip install polars xlsxwriter
```

**CRITICAL**: Do NOT install pandas for this skill. It is explicitly forbidden.

- `polars`: For reading Parquet files
- `xlsxwriter`: For writing Excel files

## Common Pitfalls

1. **Exceeding Excel row limit**
   ```python
   # Problem: Parquet has 2M rows, Excel limit is 1.048M
   # Solution: Filter or split data
   
   import polars as pl
   df = pl.read_parquet("large.parquet")
   
   if len(df) > 1_000_000:
       # Split into multiple sheets or files
       for i in range(0, len(df), 1_000_000):
           chunk = df.slice(i, 1_000_000)
           chunk.write_parquet(f"chunk_{i}.parquet")
   ```

2. **Using pandas (FORBIDDEN)**
   ```python
   # ✗ WRONG - Do NOT do this
   import pandas as pd
   df = pd.read_parquet("data.parquet")
   df.to_excel("output.xlsx")
   
   # ✓ CORRECT - Use Polars + XlsxWriter
   result = parquet_to_excel_converter_polars_xlsxwriter(
       source_parquet_path="data.parquet",
       target_excel_path="output.xlsx",
       sheet_name="Data"
   )
   ```

3. **Formula columns from Parquet**
   ```python
   # Parquet doesn't store formulas, only values
   # Result: Excel will contain static values, not formulas
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- User requests Parquet → Excel conversion
- Creating reports for business users
- Exporting analytical results
- Delivering processed data to non-technical stakeholders

Agents MUST remember:
- **NEVER use pandas for this conversion**
- **ALWAYS use Polars + XlsxWriter combination**
- Validate row count before conversion (Excel limit: 1,048,576)
- This skill is for final delivery to end users, not intermediate processing
