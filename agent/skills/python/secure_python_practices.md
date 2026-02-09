# Skill: secure_python_practices

The agent implements security best practices to protect against common vulnerabilities.

## Responsibility
Avoid hardcoded secrets, sanitize inputs, handle files securely, and follow security principles.

## Rules
- Never hardcode secrets, API keys, or passwords
- Use environment variables or secret managers (e.g., dotenv, AWS Secrets Manager)
- Sanitize all user inputs to prevent injection attacks
- Use parameterized queries for SQL
- Validate and sanitize file paths
- Handle file operations securely (avoid arbitrary file access)
- Use HTTPS for network communications
- Keep dependencies updated and scan for vulnerabilities

## Behavior

### Step 1: Manage Secrets Securely
- Use `.env` files with python-dotenv
- Add `.env` to `.gitignore`
- Use secret managers in production
- Rotate secrets regularly

### Step 2: Sanitize Inputs
- Validate all user inputs
- Use parameterized queries for databases
- Escape special characters in outputs
- Prevent path traversal attacks

### Step 3: Secure File Operations
- Validate file paths and extensions
- Use safe file permissions
- Avoid executing user-provided code
- Limit file sizes

### Step 4: Network Security
- Use HTTPS/TLS for communications
- Verify SSL certificates
- Implement rate limiting
- Use secure authentication (OAuth2, JWT)

## Example Usage

**Environment Variables with dotenv:**
```python
import os
from dotenv import load_dotenv

load_dotenv()  # Load from .env file

API_KEY = os.getenv('API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')

if not API_KEY:
    raise ValueError("API_KEY not found in environment")
```

**Parameterized SQL Queries:**
```python
import sqlite3

# ❌ Bad: SQL injection vulnerability
def get_user_bad(username):
    query = f"SELECT * FROM users WHERE username = '{username}'"
    # User input: "admin' OR '1'='1" would expose all users

# ✅ Good: Parameterized query
def get_user_safe(username):
    conn = sqlite3.connect('database.db')
    cursor = conn.execute(
        "SELECT * FROM users WHERE username = ?",
        (username,)
    )
    return cursor.fetchone()
```

**Secure File Operations:**
```python
import os
from pathlib import Path

def read_user_file(filename: str, allowed_dir: str) -> str:
    """Safely read file preventing path traversal."""
    # Resolve to absolute path
    base_dir = Path(allowed_dir).resolve()
    file_path = (base_dir / filename).resolve()
    
    # Ensure file is within allowed directory
    if not file_path.is_relative_to(base_dir):
        raise ValueError("Access denied: path traversal detected")
    
    # Check file exists and is a file
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError("File not found")
    
    with open(file_path, 'r') as f:
        return f.read()
```

**Password Hashing:**
```python
from passlib.hash import bcrypt

def hash_password(password: str) -> str:
    """Hash password securely."""
    return bcrypt.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash."""
    return bcrypt.verify(password, hashed)
```

**Input Sanitization:**
```python
import html
from typing import str

def sanitize_html_input(user_input: str) -> str:
    """Escape HTML to prevent XSS attacks."""
    return html.escape(user_input)

def validate_filename(filename: str) -> bool:
    """Validate filename to prevent directory traversal."""
    if '..' in filename or '/' in filename or '\\' in filename:
        return False
    return True
```

## Notes
- Security is a process, not a one-time task
- Use security linters: bandit, safety
- Follow OWASP Top 10 guidelines
- Implement principle of least privilege
- Log security events for monitoring
