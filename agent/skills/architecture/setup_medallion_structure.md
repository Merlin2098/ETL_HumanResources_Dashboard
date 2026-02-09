# Skill: Medallion Architecture Directory Enforcer

## Overview
Create and enforce a Medallion Architecture directory structure (Bronze/Silver/Gold) for data lake organization. Medallion architecture provides progressive data quality and transformation layers.

## When to Use This Skill
- Setting up new data lake projects
- Enforcing data lake structure standards
- Validating directory structure exists
- Creating missing medallion layers
- Organizing data pipeline outputs

## When NOT to Use This Skill
- Managing data within layers (use other ETL skills)
- Deciding what data goes where (agent responsibility)
- Creating table schemas or databases

## Medallion Architecture Overview

**Bronze Layer**: Raw, unprocessed source data (source of truth)
**Silver Layer**: Cleaned, conformed, and validated data
**Gold Layer**: Business-level aggregates and analytics-ready data

## Implementation

```python
from pathlib import Path
from typing import List, Dict

def medallion_architecture_enforcer(
    base_path: str,
    layers: List[str] = None,
    create_if_missing: bool = True,
    validate_only: bool = False
) -> Dict:
    """
    Create and enforce Medallion Architecture directory structure.
    
    Args:
        base_path: Root directory for medallion structure
        layers: Layers to create (default: ["bronze", "silver", "gold"])
        create_if_missing: Create directories if they don't exist
        validate_only: Only check structure, don't create
    
    Returns:
        dict with:
            - bronze_path: Absolute path to Bronze layer
            - silver_path: Absolute path to Silver layer
            - gold_path: Absolute path to Gold layer
            - structure_valid: Whether structure matches spec
            - created_dirs: Directories created (if any)
    
    Raises:
        PermissionError: If directories cannot be created
    """
    # Set default layers if not provided
    if layers is None:
        layers = ["bronze", "silver", "gold"]
    
    # Validate layers
    valid_layers = ["bronze", "silver", "gold"]
    for layer in layers:
        if layer not in valid_layers:
            raise ValueError(f"Invalid layer: {layer}. Must be one of: {valid_layers}")
    
    # Create base path object
    base = Path(base_path).resolve()
    
    # Construct layer paths
    layer_paths = {
        "bronze": base / "bronze",
        "silver": base / "silver",
        "gold": base / "gold"
    }
    
    created_dirs = []
    structure_valid = True
    
    # Check if layers exist
    for layer in layers:
        layer_path = layer_paths[layer]
        
        if not layer_path.exists():
            structure_valid = False
            
            # Create if requested and not in validate-only mode
            if create_if_missing and not validate_only:
                try:
                    layer_path.mkdir(parents=True, exist_ok=True)
                    created_dirs.append(str(layer_path))
                except PermissionError as e:
                    raise PermissionError(
                        f"Permission denied creating directory: {layer_path}\n{str(e)}"
                    )
    
    # If we created directories, structure is now valid
    if created_dirs:
        structure_valid = True
    
    return {
        "bronze_path": str(layer_paths["bronze"].absolute()),
        "silver_path": str(layer_paths["silver"].absolute()),
        "gold_path": str(layer_paths["gold"].absolute()),
        "structure_valid": structure_valid,
        "created_dirs": created_dirs
    }

```

## Directory Structure Created

```
{base_path}/
├── bronze/          # Raw source data (source of truth)
│   ├── sales/
│   ├── customers/
│   └── products/
├── silver/          # Cleaned and conformed data
│   ├── sales_cleaned/
│   ├── customers_validated/
│   └── products_standardized/
└── gold/            # Business-level aggregates
    ├── customer_360/
    ├── daily_revenue/
    └── product_performance/
```

## Usage Examples

```python
# Example 1: Create full medallion structure
result = medallion_architecture_enforcer(
    base_path="data/lake"
)
print(f"Bronze: {result['bronze_path']}")
print(f"Silver: {result['silver_path']}")
print(f"Gold: {result['gold_path']}")
print(f"Created: {result['created_dirs']}")

# Example 2: Validate structure without creating
result = medallion_architecture_enforcer(
    base_path="data/lake",
    validate_only=True
)
if result['structure_valid']:
    print("✓ Structure is valid")
else:
    print("✗ Structure is invalid - missing layers")

# Example 3: Create only specific layers
result = medallion_architecture_enforcer(
    base_path="data/lake",
    layers=["bronze", "silver"]  # Skip gold
)

# Example 4: Don't create if missing (check only)
result = medallion_architecture_enforcer(
    base_path="data/lake",
    create_if_missing=False
)
if not result['structure_valid']:
    print("Warning: Medallion structure not found")

# Example 5: Complete ETL setup
def setup_data_lake(base_path: str):
    """Initialize complete data lake structure"""
    
    # Create medallion structure
    result = medallion_architecture_enforcer(base_path=base_path)
    
    # Create subdirectories within each layer
    bronze_path = Path(result['bronze_path'])
    silver_path = Path(result['silver_path'])
    gold_path = Path(result['gold_path'])
    
    # Bronze subdirectories (by source)
    (bronze_path / "erp_system").mkdir(exist_ok=True)
    (bronze_path / "crm_system").mkdir(exist_ok=True)
    (bronze_path / "web_analytics").mkdir(exist_ok=True)
    
    # Silver subdirectories (by domain)
    (silver_path / "sales").mkdir(exist_ok=True)
    (silver_path / "customers").mkdir(exist_ok=True)
    (silver_path / "products").mkdir(exist_ok=True)
    
    # Gold subdirectories (by business area)
    (gold_path / "marketing").mkdir(exist_ok=True)
    (gold_path / "finance").mkdir(exist_ok=True)
    (gold_path / "operations").mkdir(exist_ok=True)
    
    return result

result = setup_data_lake("data/lake")
```

## Advanced Usage Patterns

### Pattern 1: Data Lake Manager

```python
class DataLakeManager:
    """Manage medallion architecture data lake"""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.structure = None
    
    def initialize(self):
        """Create medallion structure"""
        self.structure = medallion_architecture_enforcer(
            base_path=self.base_path,
            create_if_missing=True
        )
        return self.structure
    
    def validate(self) -> bool:
        """Check if structure is valid"""
        result = medallion_architecture_enforcer(
            base_path=self.base_path,
            validate_only=True,
            create_if_missing=False
        )
        return result['structure_valid']
    
    def get_layer_path(self, layer: str) -> str:
        """Get path to specific layer"""
        if self.structure is None:
            self.initialize()
        return self.structure[f'{layer}_path']

# Usage
lake = DataLakeManager("data/analytics")
lake.initialize()
bronze_path = lake.get_layer_path("bronze")
```

### Pattern 2: Partitioned Data Organization

```python
from datetime import datetime

def organize_partitioned_data(base_path: str, layer: str, domain: str):
    """Create partitioned directory structure within layer"""
    
    # Ensure medallion structure exists
    result = medallion_architecture_enforcer(base_path=base_path)
    
    # Get layer path
    layer_path = Path(result[f'{layer}_path'])
    
    # Create partitioned structure: layer/domain/year=YYYY/month=MM/day=DD
    today = datetime.now()
    partition_path = (
        layer_path / 
        domain / 
        f"year={today.year}" / 
        f"month={today.month:02d}" / 
        f"day={today.day:02d}"
    )
    
    partition_path.mkdir(parents=True, exist_ok=True)
    
    return str(partition_path)

# Usage
output_path = organize_partitioned_data(
    base_path="data/lake",
    layer="bronze",
    domain="sales"
)
# Returns: data/lake/bronze/sales/year=2024/month=02/day=07
```

### Pattern 3: Data Quality Gates

```python
def enforce_data_quality_gates(base_path: str):
    """Ensure each layer has quality control directories"""
    
    result = medallion_architecture_enforcer(base_path=base_path)
    
    # Create quality control subdirectories
    for layer in ["bronze", "silver", "gold"]:
        layer_path = Path(result[f'{layer}_path'])
        
        # Create QC directories
        (layer_path / "_quarantine").mkdir(exist_ok=True)  # Failed records
        (layer_path / "_rejected").mkdir(exist_ok=True)    # Rejected data
        (layer_path / "_metadata").mkdir(exist_ok=True)    # Metadata
    
    return result

result = enforce_data_quality_gates("data/lake")
```

## Best Practices

1. **Bronze: Keep raw, immutable data**
   ```python
   # Bronze should be append-only, never modified
   bronze_path = result['bronze_path']
   
   # Good: Timestamped files
   filename = f"sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
   
   # Avoid: Overwriting files
   ```

2. **Silver: Clean and conform**
   ```python
   # Silver should have validated, standardized data
   silver_path = result['silver_path']
   
   # Apply:
   # - Data type standardization
   # - Deduplication
   # - Null handling
   # - Schema enforcement
   ```

3. **Gold: Business-ready aggregates**
   ```python
   # Gold should have aggregated, denormalized data
   gold_path = result['gold_path']
   
   # Create:
   # - KPI tables
   # - Dashboard datasets
   # - Report-ready views
   ```

4. **Document layer responsibilities**
   ```python
   # Create README files in each layer
   bronze_readme = Path(result['bronze_path']) / "README.md"
   bronze_readme.write_text("""
   # Bronze Layer
   
   ## Purpose
   Raw, immutable source data (source of truth)
   
   ## Data Characteristics
   - No transformations applied
   - Original file formats
   - Timestamped for traceability
   
   ## Retention
   - Keep indefinitely (or per compliance)
   """)
   ```

## Integration with ETL Pipeline

```python
def medallion_etl_pipeline(base_path: str, source_file: str):
    """Example ETL using medallion architecture"""
    
    # 1. Ensure structure exists
    structure = medallion_architecture_enforcer(base_path=base_path)
    
    # 2. Bronze: Copy raw data
    import shutil
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_file = f"{structure['bronze_path']}/raw_data_{timestamp}.parquet"
    shutil.copy(source_file, bronze_file)
    
    # 3. Silver: Clean and transform
    import polars as pl
    
    df_raw = pl.read_parquet(bronze_file)
    df_clean = (
        df_raw
        .filter(pl.col("amount") > 0)  # Remove invalid
        .drop_nulls(subset=["customer_id"])  # Remove nulls
        .unique(subset=["transaction_id"])  # Deduplicate
    )
    
    silver_file = f"{structure['silver_path']}/cleaned_data.parquet"
    df_clean.write_parquet(silver_file)
    
    # 4. Gold: Aggregate
    df_gold = (
        df_clean
        .group_by("customer_id")
        .agg([
            pl.sum("amount").alias("total_amount"),
            pl.count().alias("transaction_count")
        ])
    )
    
    gold_file = f"{structure['gold_path']}/customer_summary.parquet"
    df_gold.write_parquet(gold_file)
    
    return {
        "bronze": bronze_file,
        "silver": silver_file,
        "gold": gold_file
    }
```

## Error Handling

```python
def safe_medallion_enforcer(base_path: str, **kwargs):
    """Enforce with error handling"""
    
    try:
        result = medallion_architecture_enforcer(
            base_path=base_path,
            **kwargs
        )
        
        return {
            "success": True,
            **result
        }
    
    except PermissionError as e:
        return {
            "success": False,
            "error": "PermissionError",
            "message": str(e),
            "structure_valid": False
        }
    
    except ValueError as e:
        return {
            "success": False,
            "error": "ValueError",
            "message": str(e),
            "structure_valid": False
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": "UnexpectedError",
            "message": str(e),
            "structure_valid": False
        }
```

## Performance Characteristics

- **Creation Time**: <10ms for directory creation
- **Validation Time**: <5ms
- **Disk Usage**: Minimal (directory metadata only)

## Dependencies

```bash
# No additional dependencies required
# Uses Python standard library only
```

## Common Pitfalls

1. **Confusing layer purposes**
   ```python
   # Wrong: Cleaned data in Bronze
   # Bronze should be raw/immutable
   
   # Wrong: Aggregated data in Silver
   # Silver should be record-level, not aggregated
   
   # Correct: Follow layer definitions
   # Bronze: Raw → Silver: Cleaned → Gold: Aggregated
   ```

2. **Modifying Bronze data**
   ```python
   # Wrong: Overwriting Bronze files
   # Bronze should be append-only
   
   # Correct: Keep all versions
   bronze_file = f"bronze/data_{timestamp}.parquet"
   ```

3. **Not validating structure**
   ```python
   # Good: Always validate before use
   result = medallion_architecture_enforcer(
       base_path="data/lake",
       validate_only=True
   )
   if not result['structure_valid']:
       print("Warning: Invalid structure")
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- Setting up new data lake projects
- Validating data lake structure
- Creating missing medallion layers
- Organizing ETL pipeline outputs

Agents should remember:
- This skill only creates directory structure
- Does NOT decide what data goes where
- Does NOT manage data within layers
- Bronze = raw, Silver = cleaned, Gold = aggregated
