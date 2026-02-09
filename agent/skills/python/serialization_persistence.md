# Skill: serialization_persistence

The agent handles object serialization and temporary data persistence.

## Responsibility
Serialize Python objects and manage lightweight persistence using appropriate formats and storage mechanisms.

## Rules
- Use JSON for human-readable, portable data
- Use pickle for Python-specific object serialization
- Use msgpack for compact binary serialization
- Use SQLite, shelve, or TinyDB for lightweight persistence
- Never trust unserialized data from untrusted sources
- Handle serialization errors gracefully
- Document data format and schema

## Behavior

### Step 1: Choose Serialization Format
- **JSON**: Cross-language compatibility, human-readable
- **Pickle**: Python objects, not secure for untrusted data
- **Msgpack**: Compact binary, faster than JSON
- **Protocol Buffers/Avro**: Schema-based, versioned data

### Step 2: Implement Serialization
- Convert objects to serialized format
- Handle non-serializable types (datetime, custom objects)
- Implement custom encoders/decoders if needed

### Step 3: Choose Persistence Mechanism
- **SQLite**: SQL database for structured data
- **shelve**: Key-value store using pickle
- **TinyDB**: JSON-based document database
- **Redis**: In-memory cache with persistence

### Step 4: Implement Persistence Operations
- Save data to storage
- Load data with error handling
- Implement caching strategies
- Clean up old or temporary data

## Example Usage

**JSON Serialization:**
```python
import json
from datetime import datetime
from typing import Any

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def save_json(data: Any, filepath: str):
    with open(filepath, 'w') as f:
        json.dump(data, f, cls=CustomEncoder, indent=2)

def load_json(filepath: str) -> Any:
    with open(filepath, 'r') as f:
        return json.load(f)
```

**Pickle for Python Objects:**
```python
import pickle

def save_pickle(obj: Any, filepath: str):
    with open(filepath, 'wb') as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

def load_pickle(filepath: str) -> Any:
    with open(filepath, 'rb') as f:
        return pickle.load(f)
```

**TinyDB for Document Storage:**
```python
from tinydb import TinyDB, Query

class DataStore:
    def __init__(self, db_path: str):
        self.db = TinyDB(db_path)
    
    def insert(self, data: dict):
        return self.db.insert(data)
    
    def search(self, field: str, value: Any):
        Q = Query()
        return self.db.search(Q[field] == value)
    
    def update(self, field: str, value: Any, new_data: dict):
        Q = Query()
        self.db.update(new_data, Q[field] == value)
```

**SQLite for Structured Data:**
```python
import sqlite3
from typing import List, Dict

class SQLiteCache:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_table()
    
    def _create_table(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
    
    def set(self, key: str, value: str):
        self.conn.execute(
            'INSERT OR REPLACE INTO cache (key, value) VALUES (?, ?)',
            (key, value)
        )
        self.conn.commit()
    
    def get(self, key: str) -> str:
        cursor = self.conn.execute(
            'SELECT value FROM cache WHERE key = ?',
            (key,)
        )
        row = cursor.fetchone()
        return row['value'] if row else None
```

## Notes
- JSON is safe but limited to basic types
- Pickle is powerful but potentially insecure
- Always validate deserialized data
- Consider data versioning for evolving schemas
