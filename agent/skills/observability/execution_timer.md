# Skill: Execution Timer

## Overview
Measure and report execution time for tasks and SQL queries with appropriate precision. Uses high-resolution timer for accurate measurements.

## When to Use This Skill
- Measuring function execution time
- Benchmarking SQL queries
- Performance profiling
- Tracking ETL pipeline duration
- Logging execution metrics

## When NOT to Use This Skill
- Micro-benchmarking (<1ms operations)
- Real-time monitoring (use dedicated monitoring tools)
- When timing overhead matters

## Implementation

```python
import time
from typing import Callable, Tuple, Any, Dict

def execution_timer(
    task_type: str,
    callable: Callable,
    args: Tuple = (),
    kwargs: Dict = None
) -> Dict:
    """
    Measure execution time for tasks and SQL queries.
    
    Args:
        task_type: "general" or "sql_query" (determines output format)
        callable: Function to execute and time
        args: Positional arguments for callable
        kwargs: Keyword arguments for callable
    
    Returns:
        dict with:
            - result: Return value from callable
            - execution_time: Formatted time string
                * "general" → "hh:mm:ss" format (e.g., "00:01:23")
                * "sql_query" → milliseconds precision (e.g., "1234.56 ms")
            - execution_time_seconds: Raw seconds (for programmatic use)
    
    Raises:
        Any exception raised by the callable (propagated up)
    """
    # Validate task_type
    valid_types = ["general", "sql_query"]
    if task_type not in valid_types:
        raise ValueError(
            f"Invalid task_type: {task_type}. Must be one of: {valid_types}"
        )
    
    # Set default kwargs
    if kwargs is None:
        kwargs = {}
    
    # Record start time
    start_time = time.perf_counter()
    
    # Execute callable
    try:
        result = callable(*args, **kwargs)
    except Exception as e:
        # Re-raise exception after recording time
        end_time = time.perf_counter()
        elapsed_seconds = end_time - start_time
        raise e
    
    # Record end time
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    
    # Format time based on task type
    if task_type == "general":
        # Format as hh:mm:ss
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        execution_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    elif task_type == "sql_query":
        # Format as milliseconds with 2 decimal places
        milliseconds = elapsed_seconds * 1000
        execution_time = f"{milliseconds:.2f} ms"
    
    return {
        "result": result,
        "execution_time": execution_time,
        "execution_time_seconds": elapsed_seconds
    }

```

## Usage Examples

```python
# Example 1: Time a general task (file processing)
def process_large_file(filepath):
    import polars as pl
    df = pl.read_parquet(filepath)
    return df.filter(pl.col("amount") > 100)

result = execution_timer(
    task_type="general",
    callable=process_large_file,
    args=("data/large_sales.parquet",)
)
print(f"Processing took: {result['execution_time']}")  # 00:02:15
print(f"Rows: {len(result['result'])}")

# Example 2: Time a SQL query
import duckdb

def run_query(conn, sql):
    return conn.execute(sql).fetchdf()

conn = duckdb.connect()
sql = "SELECT * FROM 'data/sales.parquet' WHERE amount > 1000"

result = execution_timer(
    task_type="sql_query",
    callable=run_query,
    args=(conn, sql)
)
print(f"Query took: {result['execution_time']}")  # 234.56 ms

# Example 3: Time with keyword arguments
def transform_data(df, column, threshold):
    return df.filter(df[column] > threshold)

import polars as pl
df = pl.read_parquet("data.parquet")

result = execution_timer(
    task_type="general",
    callable=transform_data,
    args=(df,),
    kwargs={"column": "amount", "threshold": 100}
)

# Example 4: Time ETL pipeline step
def bronze_to_silver(source_path, target_path):
    import polars as pl
    
    # Read, clean, write
    df = pl.read_parquet(source_path)
    df_clean = df.drop_nulls().unique()
    df_clean.write_parquet(target_path)
    
    return len(df_clean)

result = execution_timer(
    task_type="general",
    callable=bronze_to_silver,
    args=("bronze/sales.parquet", "silver/sales_clean.parquet")
)
print(f"ETL completed in: {result['execution_time']}")
print(f"Processed {result['result']} rows")

# Example 5: Compare performance
def benchmark_operations():
    """Compare different operations"""
    
    import polars as pl
    
    # Operation 1: Polars read
    result1 = execution_timer(
        task_type="sql_query",
        callable=pl.read_parquet,
        args=("data.parquet",)
    )
    
    # Operation 2: DuckDB query
    import duckdb
    conn = duckdb.connect()
    result2 = execution_timer(
        task_type="sql_query",
        callable=lambda: conn.execute("SELECT * FROM 'data.parquet'").fetchdf(),
        args=()
    )
    
    print(f"Polars read: {result1['execution_time']}")
    print(f"DuckDB query: {result2['execution_time']}")

benchmark_operations()
```

## Advanced Usage Patterns

### Pattern 1: Pipeline Timer

```python
class PipelineTimer:
    """Time multiple pipeline steps"""
    
    def __init__(self):
        self.timings = {}
    
    def time_step(self, step_name: str, callable, *args, **kwargs):
        """Time a single pipeline step"""
        result = execution_timer(
            task_type="general",
            callable=callable,
            args=args,
            kwargs=kwargs
        )
        
        self.timings[step_name] = {
            "execution_time": result["execution_time"],
            "seconds": result["execution_time_seconds"]
        }
        
        return result["result"]
    
    def report(self):
        """Print timing report"""
        print("\n=== Pipeline Timing Report ===")
        total_seconds = sum(t["seconds"] for t in self.timings.values())
        
        for step, timing in self.timings.items():
            percent = (timing["seconds"] / total_seconds) * 100
            print(f"{step:20s}: {timing['execution_time']:>10s} ({percent:>5.1f}%)")
        
        # Format total time
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        total_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        print(f"{'Total':20s}: {total_formatted:>10s}")

# Usage
timer = PipelineTimer()

# Time each step
df_bronze = timer.time_step("Extract", extract_data, "source.csv")
df_silver = timer.time_step("Transform", transform_data, df_bronze)
timer.time_step("Load", load_data, df_silver, "output.parquet")

# Print report
timer.report()
```

### Pattern 2: Decorator for Timing

```python
from functools import wraps

def time_execution(task_type="general"):
    """Decorator to time function execution"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = execution_timer(
                task_type=task_type,
                callable=func,
                args=args,
                kwargs=kwargs
            )
            print(f"{func.__name__} took: {result['execution_time']}")
            return result["result"]
        return wrapper
    return decorator

# Usage
@time_execution(task_type="general")
def process_data(filepath):
    import polars as pl
    return pl.read_parquet(filepath)

@time_execution(task_type="sql_query")
def run_query(conn, sql):
    return conn.execute(sql).fetchdf()

# Automatically timed
df = process_data("data.parquet")  # Prints: process_data took: 00:00:05
```

### Pattern 3: Performance Comparison

```python
def compare_performance(operations: dict, iterations: int = 3):
    """Compare performance of multiple operations"""
    
    results = {}
    
    for name, (task_type, callable, args, kwargs) in operations.items():
        timings = []
        
        for _ in range(iterations):
            result = execution_timer(
                task_type=task_type,
                callable=callable,
                args=args,
                kwargs=kwargs or {}
            )
            timings.append(result["execution_time_seconds"])
        
        avg_time = sum(timings) / len(timings)
        results[name] = {
            "average_seconds": avg_time,
            "min_seconds": min(timings),
            "max_seconds": max(timings)
        }
    
    # Print comparison
    print("\n=== Performance Comparison ===")
    for name, metrics in results.items():
        print(f"{name}:")
        print(f"  Average: {metrics['average_seconds']:.3f}s")
        print(f"  Min: {metrics['min_seconds']:.3f}s")
        print(f"  Max: {metrics['max_seconds']:.3f}s")
    
    return results

# Usage
import polars as pl

operations = {
    "Polars CSV": ("general", pl.read_csv, ("data.csv",), None),
    "Polars Parquet": ("general", pl.read_parquet, ("data.parquet",), None),
}

results = compare_performance(operations, iterations=5)
```

## Time Format Examples

### General Tasks (hh:mm:ss)

| Seconds | Formatted Output |
|---------|------------------|
| 5       | 00:00:05         |
| 65      | 00:01:05         |
| 125     | 00:02:05         |
| 3665    | 01:01:05         |
| 7325    | 02:02:05         |

### SQL Queries (milliseconds)

| Seconds | Formatted Output |
|---------|------------------|
| 0.001   | 1.00 ms          |
| 0.0234  | 23.40 ms         |
| 0.156   | 156.00 ms        |
| 1.234   | 1234.00 ms       |
| 5.678   | 5678.00 ms       |

## Best Practices

1. **Choose correct task_type**
   ```python
   # For long-running tasks (>1 second)
   result = execution_timer(
       task_type="general",
       callable=process_large_file,
       args=("data.parquet",)
   )
   
   # For quick queries (<10 seconds typically)
   result = execution_timer(
       task_type="sql_query",
       callable=run_query,
       args=(conn, sql)
   )
   ```

2. **Use lambdas for simple calls**
   ```python
   # Instead of creating a wrapper function
   result = execution_timer(
       task_type="sql_query",
       callable=lambda: conn.execute(sql).fetchdf(),
       args=()
   )
   ```

3. **Log execution times**
   ```python
   import logging
   
   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)
   
   result = execution_timer(
       task_type="general",
       callable=process_data,
       args=("data.parquet",)
   )
   
   logger.info(
       f"Processed data in {result['execution_time']} "
       f"({result['execution_time_seconds']:.2f}s)"
   )
   ```

4. **Handle exceptions**
   ```python
   try:
       result = execution_timer(
           task_type="general",
           callable=risky_operation,
           args=()
       )
   except Exception as e:
       print(f"Operation failed: {e}")
       # Note: timing still recorded up to failure point
   ```

## Error Handling

```python
def safe_execution_timer(task_type: str, callable, *args, **kwargs):
    """Timer with error handling"""
    
    try:
        result = execution_timer(
            task_type=task_type,
            callable=callable,
            args=args,
            kwargs=kwargs
        )
        
        return {
            "success": True,
            **result
        }
    
    except ValueError as e:
        return {
            "success": False,
            "error": "ValueError",
            "message": str(e)
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": "ExecutionError",
            "message": str(e)
        }
```

## Performance Characteristics

- **Timer Overhead**: <1 microsecond
- **Precision**: ~1 microsecond (depends on system)
- **Accuracy**: High-resolution performance counter
- **Memory**: Negligible

## Precision Comparison

| Timer Type         | Precision    | Use Case                |
|-------------------|--------------|-------------------------|
| time.time()       | ~15ms        | Low precision, avoid    |
| time.perf_counter()| ~1µs        | High precision (used)   |
| time.process_time()| ~1µs        | CPU time only          |

## Dependencies

```bash
# No additional dependencies required
# Uses Python standard library only
```

## Common Pitfalls

1. **Wrong task_type for duration**
   ```python
   # Bad: Using sql_query for long task
   result = execution_timer(
       task_type="sql_query",  # Will show "180000.00 ms" (3 min)
       callable=long_etl_job,
       args=()
   )
   
   # Good: Use general for long tasks
   result = execution_timer(
       task_type="general",  # Will show "00:03:00"
       callable=long_etl_job,
       args=()
   )
   ```

2. **Not handling exceptions**
   ```python
   # Exceptions propagate, but timing is lost
   try:
       result = execution_timer(
           task_type="general",
           callable=may_fail,
           args=()
       )
   except Exception as e:
       print(f"Failed: {e}")
       # You won't know how long it ran before failing
   ```

3. **Micro-benchmarking**
   ```python
   # Bad: Timing very fast operations (<1ms)
   result = execution_timer(
       task_type="sql_query",
       callable=lambda: x + 1,  # Too fast
       args=()
   )
   # Timer overhead may exceed operation time
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- Measuring function or query execution time
- Benchmarking performance
- Profiling ETL pipelines
- Logging execution metrics

Agents should remember:
- Use "general" for tasks likely >1 second
- Use "sql_query" for queries typically <10 seconds
- Exceptions from callable are propagated (not caught)
- Both formatted and raw seconds are returned
