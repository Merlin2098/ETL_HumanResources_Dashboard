# Parquet Explorer

## Responsibility
Efficiently read Apache Parquet files, providing access to columnar data and schemas.

## Detailed Behavior
1.  **Schema Inspection**:
    -   Read the file footer/metadata.
    -   Display the schema: column names and data types.
2.  **Data Loading**:
    -   Read data into a Pandas DataFrame or PyArrow Table.
    -   Support reading specific columns (projection) to save memory.
    -   Support reading specific row groups.
3.  **Partition Handling**:
    -   Detect if the file is part of a partitioned directory structure (Hive-style partitioning).
    -   Read partitioned datasets as a single logical unit if pointing to a directory.
4.  **Metadata**:
    -   Extract row counts, compression types, and column statistics (min/max/nulls).

## Example Usage
```python
from agent.skills.file_exploration import ParquetExplorer

parquet_tool = ParquetExplorer()

# Inspect schema
schema = parquet_tool.get_schema("data.parquet")

# Read specific columns
df = parquet_tool.read("data.parquet", columns=["id", "timestamp", "value"])
```
