# Skill: linter_formatter_guru

The agent maintains consistent code style using automated formatters and linters.

## Responsibility
Enforce code style consistency using black, isort, ruff, and other formatting tools.

## Rules
- Use black for code formatting (opinionated, consistent)
- Use isort for import sorting
- Use ruff for fast linting (replaces flake8, pylint)
- Configure tools in pyproject.toml
- Run formatters before committing
- Integrate into pre-commit hooks and CI/CD
- Document style decisions in project README

## Behavior

### Step 1: Configure Black
- Set line length (default 88, or customize)
- Configure target Python version
- Exclude specific files/directories if needed

### Step 2: Configure isort
- Set profile to "black" for compatibility
- Configure import grouping and ordering

### Step 3: Configure Ruff
- Enable desired rule sets
- Configure line length to match black
- Set Python version target
- Exclude generated or vendored code

### Step 4: Integrate into Workflow
- Add to pre-commit hooks
- Run in CI/CD pipeline
- Configure IDE integration

## Example Usage

**pyproject.toml Configuration:**
```toml
[tool.black]
line-length = 88
target-version = ['py310']
include = '\.pyi?$'
exclude = '''
/(
    \.git
  | \.venv
  | build
  | dist
)/
'''

[tool.isort]
profile = "black"
line_length = 88
multi_line_output = 3
include_trailing_comma = true
force_grid_wrap = 0
use_parentheses = true
ensure_newline_before_comments = true

[tool.ruff]
line-length = 88
target-version = "py310"
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = [
    "E501",  # line too long (handled by black)
]
exclude = [
    ".git",
    ".venv",
    "__pycache__",
    "build",
    "dist",
]

[tool.ruff.per-file-ignores]
"__init__.py" = ["F401"]  # Allow unused imports in __init__.py
```

**Running Formatters:**
```bash
# Format code with black
black .

# Sort imports with isort
isort .

# Lint with ruff
ruff check .

# Auto-fix with ruff
ruff check --fix .

# Run all in sequence
black . && isort . && ruff check .
```

**Pre-commit Hook (.pre-commit-config.yaml):**
```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.11.0
    hooks:
      - id: black

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: ["--profile", "black"]

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.6
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]
```

**Makefile for Common Commands:**
```makefile
.PHONY: format lint check

format:
	black .
	isort .

lint:
	ruff check .

check: format lint
	@echo "Code formatting and linting complete!"

fix:
	black .
	isort .
	ruff check --fix .
```

**Python Script to Run All:**
```python
import subprocess
import sys

def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and report results."""
    print(f"\n{'='*50}")
    print(f"Running: {description}")
    print(f"{'='*50}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    
    return result.returncode == 0

def main():
    """Run all formatters and linters."""
    steps = [
        (["black", "."], "Black formatter"),
        (["isort", "."], "Import sorter"),
        (["ruff", "check", "."], "Ruff linter"),
    ]
    
    all_passed = True
    for cmd, desc in steps:
        if not run_command(cmd, desc):
            all_passed = False
    
    if all_passed:
        print("\n✅ All checks passed!")
        sys.exit(0)
    else:
        print("\n❌ Some checks failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

**CI/CD Integration (GitHub Actions):**
```yaml
name: Lint and Format

on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install black isort ruff
      - name: Check formatting with black
        run: black --check .
      - name: Check imports with isort
        run: isort --check-only .
      - name: Lint with ruff
        run: ruff check .
```

## Notes
- Black is opinionated and requires no configuration
- Ruff is significantly faster than flake8/pylint
- Consistency is more important than personal preference
- Use `# noqa` comments sparingly for legitimate exceptions
- Configure IDE to format on save for best experience
