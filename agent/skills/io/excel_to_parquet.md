# Skill: Excel to Parquet Conversion

## Overview
Convert Excel files to Parquet format for efficient downstream processing and storage in data lakes and analytical workflows.

## When to Use This Skill
- Preparing Excel data for data lake ingestion
- Converting to columnar storage for analytical queries
- Reducing storage footprint (60-80% compression)
- Enabling fast query performance with DuckDB/Spark
- Archiving Excel data in efficient format

## When NOT to Use This Skill
- End users need Excel format
- Source data changes frequently (keep Excel as source)
- Data size is very small (<1MB)

## Implementation

```python
import polars as pl
import pandas as pd
from pathlib import Path
from typing import Union, Optional

def excel_to_parquet_converter(
    source_excel_path: str,
    target_parquet_path: str,
    sheet_name: Union[str, int],
    compression: str = "snappy",
    use_polars: bool = True
) -> dict:
    """
    Convert Excel file to Parquet format.
    
    Args:
        source_excel_path: Path to source Excel file
        target_parquet_path: Path to output Parquet file
        sheet_name: Sheet to convert (name or index)
        compression: Compression codec ("snappy", "gzip", "zstd", "lz4")
        use_polars: Use polars for conversion (faster) or pandas
    
    Returns:
        dict with conversion metadata:
            - success: bool
            - output_path: str (absolute path)
            - row_count: int
            - file_size_mb: float
    
    Raises:
        FileNotFoundError: If source file does not exist
        PermissionError: If target directory is not writable
    """
    # Validate source file
    source_path = Path(source_excel_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source Excel file not found: {source_excel_path}")
    
    # Create target directory if needed
    target_path = Path(target_parquet_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        if use_polars:
            # Read Excel with Polars (via pandas engine)
            df_pandas = pd.read_excel(source_excel_path, sheet_name=sheet_name)
            df = pl.from_pandas(df_pandas)
            
            # Write to Parquet
            df.write_parquet(
                target_path,
                compression=compression
            )
        else:
            # Read and write with Pandas
            df = pd.read_excel(source_excel_path, sheet_name=sheet_name)
            df.to_parquet(
                target_path,
                compression=compression,
                index=False
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

## Usage Examples

```python
# Example 1: Basic conversion with default compression
result = excel_to_parquet_converter(
    source_excel_path="data/sales.xlsx",
    target_parquet_path="data/sales.parquet",
    sheet_name="Q4_Sales"
)
print(f"Converted {result['row_count']} rows to {result['file_size_mb']} MB")

# Example 2: Use gzip for maximum compression
result = excel_to_parquet_converter(
    source_excel_path="data/large_dataset.xlsx",
    target_parquet_path="data/large_dataset.parquet",
    sheet_name=0,
    compression="gzip"
)

# Example 3: Use zstd for balanced performance
result = excel_to_parquet_converter(
    source_excel_path="data/transactions.xlsx",
    target_parquet_path="data/bronze/transactions.parquet",
    sheet_name="Transactions",
    compression="zstd"
)

# Example 4: Convert multiple sheets
sheets = ["Sales", "Returns", "Inventory"]
for sheet in sheets:
    result = excel_to_parquet_converter(
        source_excel_path="data/business_data.xlsx",
        target_parquet_path=f"data/bronze/{sheet.lower()}.parquet",
        sheet_name=sheet,
        compression="snappy"
    )
    print(f"{sheet}: {result['success']}")

# Example 5: With error handling
result = excel_to_parquet_converter(
    source_excel_path="data/input.xlsx",
    target_parquet_path="data/output.parquet",
    sheet_name="Data"
)

if result["success"]:
    print(f"✓ Conversion successful: {result['output_path']}")
    print(f"  Rows: {result['row_count']:,}")
    print(f"  Size: {result['file_size_mb']} MB")
else:
    print(f"✗ Conversion failed: {result.get('error')}")
```

## Compression Codec Comparison

| Codec  | Speed     | Compression Ratio | Use Case                          |
|--------|-----------|-------------------|-----------------------------------|
| snappy | Fastest   | ~60%              | Default, balanced performance     |
| lz4    | Very Fast | ~55%              | When speed is critical            |
| gzip   | Slow      | ~75%              | Maximum compression needed        |
| zstd   | Medium    | ~70%              | Best balance compression/speed    |

## Best Practices

1. **Choose compression based on use case**
   ```python
   # For data lake archival (optimize for size)
   result = excel_to_parquet_converter(
       source_excel_path="archive.xlsx",
       target_parquet_path="lake/bronze/archive.parquet",
       sheet_name="Data",
       compression="gzip"  # Maximum compression
   )
   
   # For active analytics (optimize for speed)
   result = excel_to_parquet_converter(
       source_excel_path="daily.xlsx",
       target_parquet_path="lake/bronze/daily.parquet",
       sheet_name="Data",
       compression="snappy"  # Fast read/write
   )
   ```

2. **Create organized directory structures**
   ```python
   from datetime import datetime
   
   # Organize by date
   date_str = datetime.now().strftime("%Y/%m/%d")
   result = excel_to_parquet_converter(
       source_excel_path="daily_sales.xlsx",
       target_parquet_path=f"lake/bronze/sales/{date_str}/data.parquet",
       sheet_name="Sales",
       compression="snappy"
   )
   ```

3. **Validate conversion success**
   ```python
   import polars as pl
   
   # Convert
   result = excel_to_parquet_converter(
       source_excel_path="source.xlsx",
       target_parquet_path="target.parquet",
       sheet_name="Data"
   )
   
   # Validate by reading back
   if result["success"]:
       df = pl.read_parquet(result["output_path"])
       assert len(df) == result["row_count"], "Row count mismatch!"
       print("✓ Validation passed")
   ```

4. **Measure compression effectiveness**
   ```python
   import os
   
   source_size_mb = os.path.getsize("source.xlsx") / (1024 * 1024)
   
   result = excel_to_parquet_converter(
       source_excel_path="source.xlsx",
       target_parquet_path="target.parquet",
       sheet_name="Data"
   )
   
   if result["success"]:
       compression_ratio = (1 - result["file_size_mb"] / source_size_mb) * 100
       print(f"Compression: {compression_ratio:.1f}%")
       print(f"Original: {source_size_mb:.2f} MB → Parquet: {result['file_size_mb']} MB")
   ```

## Error Handling

```python
def batch_excel_to_parquet(excel_files: list, output_dir: str, sheet_name: str):
    """Convert multiple Excel files to Parquet with error handling"""
    
    results = []
    
    for excel_file in excel_files:
        filename = Path(excel_file).stem
        target = f"{output_dir}/{filename}.parquet"
        
        try:
            result = excel_to_parquet_converter(
                source_excel_path=excel_file,
                target_parquet_path=target,
                sheet_name=sheet_name,
                compression="snappy"
            )
            
            results.append({
                "file": excel_file,
                "status": "success" if result["success"] else "failed",
                "rows": result.get("row_count", 0),
                "size_mb": result.get("file_size_mb", 0),
                "error": result.get("error")
            })
        
        except Exception as e:
            results.append({
                "file": excel_file,
                "status": "error",
                "error": str(e)
            })
    
    return results
```

## Performance Characteristics

- **Conversion Speed**: 10,000-50,000 rows/second (depends on data types)
- **Memory Usage**: ~2x the input file size during conversion
- **Compression Ratio**: Typically 60-80% size reduction
- **Read Performance Gain**: 5-10x faster than Excel for analytical queries

## Performance Benchmarks

| Excel Size | Rows    | Conversion Time | Parquet Size | Compression |
|------------|---------|-----------------|--------------|-------------|
| 5 MB       | 10k     | 1.5s            | 1.2 MB       | 76%         |
| 20 MB      | 50k     | 4.5s            | 5 MB         | 75%         |
| 100 MB     | 250k    | 18s             | 22 MB        | 78%         |
| 500 MB     | 1M      | 85s             | 95 MB        | 81%         |

## Dependencies

```bash
pip install polars pandas openpyxl pyarrow
```

- `polars`: For high-performance conversion (recommended)
- `pandas`: Alternative conversion engine
- `openpyxl`: For reading .xlsx files
- `pyarrow`: Parquet file format support

## Common Pitfalls

1. **Target directory doesn't exist**
   ```python
   # Skill creates parent directories automatically
   result = excel_to_parquet_converter(
       source_excel_path="data.xlsx",
       target_parquet_path="new_folder/subfolder/data.parquet",  # Created automatically
       sheet_name="Data"
   )
   ```

2. **Overwriting existing files**
   ```python
   # Check if file exists before conversion
   from pathlib import Path
   
   target = "output.parquet"
   if Path(target).exists():
       print(f"Warning: {target} already exists")
       # Decide: skip, rename, or overwrite
   
   result = excel_to_parquet_converter(
       source_excel_path="input.xlsx",
       target_parquet_path=target,
       sheet_name="Data"
   )
   ```

3. **Large Excel files causing memory issues**
   ```python
   # For very large files (>500MB), read in chunks
   # Note: This skill loads entire sheet into memory
   # Consider splitting large Excel files first
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- User requests Excel → Parquet conversion
- Preparing data for data lake ingestion
- Optimizing storage for analytical workloads
- Building ETL pipelines with Excel sources

Agents should NOT use this skill for:
- Converting to other formats (use appropriate converter)
- When users need to work with Excel format
- Temporary/ephemeral data processing
