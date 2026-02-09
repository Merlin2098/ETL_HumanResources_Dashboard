# Skill: performance_profiler

The agent identifies performance bottlenecks in CPU and memory usage, suggesting optimizations.

## Responsibility
Profile code execution to detect slow functions, memory leaks, and inefficient algorithms.

## Rules
- Use cProfile for CPU profiling
- Use memory_profiler for memory analysis
- Use timeit for micro-benchmarking
- Profile in production-like environments
- Focus on hot paths (frequently executed code)
- Measure before and after optimization
- Consider algorithmic improvements before micro-optimizations
- Document performance requirements and benchmarks

## Behavior

### Step 1: CPU Profiling
- Run cProfile on critical code paths
- Identify functions consuming most CPU time
- Analyze call counts and cumulative time
- Visualize with snakeviz or similar tools

### Step 2: Memory Profiling
- Use memory_profiler to track memory consumption
- Identify memory leaks and excessive allocations
- Check for unnecessary object retention
- Monitor peak memory usage

### Step 3: Micro-benchmarking
- Use timeit for small code snippets
- Compare alternative implementations
- Test with realistic data sizes
- Account for setup and teardown overhead

### Step 4: Suggest Optimizations
- Replace inefficient algorithms (e.g., O(n²) → O(n log n))
- Use appropriate data structures (lists vs sets vs dicts)
- Reduce redundant computations
- Leverage caching and memoization
- Use generators for large datasets
- Consider NumPy for numerical operations

## Example Usage

**CPU Profiling with cProfile:**
```python
import cProfile
import pstats
from pstats import SortKey

def profile_function():
    """Profile a function to find bottlenecks."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Code to profile
    result = expensive_operation()
    
    profiler.disable()
    
    # Print stats
    stats = pstats.Stats(profiler)
    stats.sort_stats(SortKey.CUMULATIVE)
    stats.print_stats(10)  # Top 10 functions

# Or profile entire script
if __name__ == "__main__":
    cProfile.run('main()', sort='cumtime')
```

**Memory Profiling:**
```python
from memory_profiler import profile

@profile
def process_large_dataset():
    """Track memory usage line-by-line."""
    data = [i for i in range(1000000)]  # Memory spike here
    processed = [x * 2 for x in data]   # Another spike
    return sum(processed)

# Run with: python -m memory_profiler script.py
```

**Micro-benchmarking with timeit:**
```python
import timeit

# Compare list comprehension vs map
setup = "data = range(10000)"

list_comp = timeit.timeit(
    "[x * 2 for x in data]",
    setup=setup,
    number=10000
)

map_version = timeit.timeit(
    "list(map(lambda x: x * 2, data))",
    setup=setup,
    number=10000
)

print(f"List comprehension: {list_comp:.4f}s")
print(f"Map: {map_version:.4f}s")
```

**Algorithm Optimization Example:**
```python
# ❌ Bad: O(n²) complexity
def find_duplicates_slow(items):
    duplicates = []
    for i, item in enumerate(items):
        for j, other in enumerate(items):
            if i != j and item == other and item not in duplicates:
                duplicates.append(item)
    return duplicates

# ✅ Good: O(n) complexity using set
def find_duplicates_fast(items):
    """Find duplicates efficiently using set."""
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    return list(duplicates)
```

**Data Structure Optimization:**
```python
# ❌ Bad: Using list for membership testing
def filter_allowed_slow(items, allowed_list):
    return [item for item in items if item in allowed_list]  # O(n*m)

# ✅ Good: Using set for O(1) lookup
def filter_allowed_fast(items, allowed_list):
    """Filter using set for faster lookup."""
    allowed_set = set(allowed_list)  # O(m)
    return [item for item in items if item in allowed_set]  # O(n)
```

**Caching with functools.lru_cache:**
```python
from functools import lru_cache

# ❌ Bad: Recomputing expensive results
def fibonacci_slow(n):
    if n < 2:
        return n
    return fibonacci_slow(n-1) + fibonacci_slow(n-2)

# ✅ Good: Cache results
@lru_cache(maxsize=None)
def fibonacci_fast(n):
    """Fibonacci with memoization."""
    if n < 2:
        return n
    return fibonacci_fast(n-1) + fibonacci_fast(n-2)
```

**Generator for Memory Efficiency:**
```python
# ❌ Bad: Loading entire file into memory
def process_file_slow(filename):
    with open(filename) as f:
        lines = f.readlines()  # Loads all lines into memory
    return [line.upper() for line in lines]

# ✅ Good: Process line by line
def process_file_fast(filename):
    """Process file using generator for memory efficiency."""
    with open(filename) as f:
        for line in f:  # Generator - one line at a time
            yield line.upper()
```

**Profiling Decorator:**
```python
import time
from functools import wraps

def profile_time(func):
    """Decorator to measure function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{func.__name__} took {end - start:.4f} seconds")
        return result
    return wrapper

@profile_time
def slow_function():
    time.sleep(1)
    return "Done"
```

## Notes
- Profile representative workloads, not toy examples
- Optimize hot paths first (80/20 rule)
- Don't sacrifice readability for negligible gains
- Use line_profiler for line-by-line CPU profiling
- Consider py-spy for production profiling without code changes
- Visualize profiles with tools like snakeviz or gprof2dot
