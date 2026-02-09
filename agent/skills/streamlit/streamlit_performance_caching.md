# Skill: streamlit_performance_caching

The agent optimizes Streamlit application performance and resource management using advanced caching strategies to ensure a responsive and efficient user experience.

## Responsibility
Implement caching for data processing and resource initialization to minimize latency and redundant computations.

## Rules
- Use `st.cache_data` for functions that return data (DataFrames, JSON, lists, etc.)
- Use `st.cache_resource` for global resources that should be shared across sessions (Database connections, ML models, API clients)
- Specify `ttl` (Time To Live) for data that expires or needs periodic refreshing
- Use `show_spinner=True` for long-running cached functions to provide visual feedback
- Ensure cached functions are deterministic (return the same output for same inputs/arguments)

## Behavior

### Step 1: Data Caching
- Wrap heavy data fetching or transformation logic with `@st.cache_data`
- Use hashable arguments for cached functions
- Implement cache clearing mechanisms if data source changes frequently

### Step 2: Resource Caching
- Wrap database connection factories or model loading with `@st.cache_resource`
- Ensure resource objects are thread-safe if shared across multiple user sessions

### Step 3: Optimization & Parameters
- Adjust `max_entries` to limit memory consumption for large cache sets
- Use `persist="disk"` for very large datasets that should survive application restarts

## Example Usage

**Caching Data Transformations:**
```python
import streamlit as st
import pandas as pd
import time

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_large_dataset(url: str):
    # Simulate heavy download and processing
    time.sleep(2)
    df = pd.read_csv(url)
    return df

data = get_large_dataset("https://example.com/data.csv")
st.dataframe(data)
```

**Caching Database Connections:**
```python
import streamlit as st
from sqlalchemy import create_engine

@st.cache_resource
def get_database_connection():
    # This engine will be created once and shared across all app users
    engine = create_engine("sqlite:///mydb.sqlite")
    return engine.connect()

conn = get_database_connection()
```

## Notes
- `st.cache_data` creates a copy of the returned object for each session to prevent cross-session mutation
- `st.cache_resource` returns the SAME object to all sessions
- Avoid caching objects that contain sensitive user-specific data unless handled carefully
- Use `st.cache_data.clear()` to manually invalidate the cache when necessary
