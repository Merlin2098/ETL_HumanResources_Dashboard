# Skill: async_concurrency_expert

The agent determines when to use asyncio, threading, or multiprocessing, and implements concurrent solutions efficiently.

## Responsibility
Choose appropriate concurrency model and implement non-blocking I/O, parallel processing, and thread-safe operations.

## Rules
- Use **asyncio** for I/O-bound operations (network, disk, databases)
- Use **threading** for I/O-bound operations with blocking libraries
- Use **multiprocessing** for CPU-bound operations
- Avoid mixing paradigms unnecessarily
- Ensure thread safety with locks, queues, or asyncio primitives
- Handle exceptions in concurrent tasks properly
- Use connection pools for database/API connections

## Behavior

### Step 1: Identify Workload Type
- **I/O-bound**: Network requests, file operations, database queries → Use asyncio or threading
- **CPU-bound**: Data processing, computations, encryption → Use multiprocessing

### Step 2: Implement Asyncio for I/O
- Use `async`/`await` syntax
- Use `asyncio.gather()` for concurrent tasks
- Implement proper error handling with try/except

### Step 3: Implement Threading for Blocking I/O
- Use `concurrent.futures.ThreadPoolExecutor`
- Ensure thread safety with locks or queues

### Step 4: Implement Multiprocessing for CPU Work
- Use `multiprocessing.Pool` or `ProcessPoolExecutor`
- Serialize data properly (pickle-compatible)

## Example Usage

```python
import asyncio
import aiohttp

async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    urls = ["https://example.com", "https://example.org"]
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
    return results
```

## Notes
- Python's GIL prevents true parallelism in threading
- Use asyncio for scalability with many concurrent I/O operations
- Use multiprocessing to fully utilize multiple CPU cores
