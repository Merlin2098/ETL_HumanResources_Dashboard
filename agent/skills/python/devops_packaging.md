# Skill: devops_packaging

The agent manages dependencies, creates optimized Docker containers, configures pre-commit hooks, and generates executable bundles.

## Responsibility
Set up development workflow automation, containerization, and packaging for distribution.

## Rules
- Use Poetry for dependency management (or pip-tools as alternative)
- Create multi-stage Dockerfiles for optimized images
- Use slim or alpine base images
- Configure pre-commit hooks for code quality
- Generate standalone executables with PyInstaller when needed
- Pin dependency versions for reproducibility
- Separate dev dependencies from production dependencies
- Document installation and build processes

## Behavior

### Step 1: Dependency Management
- Initialize Poetry or requirements.txt with pinned versions
- Separate `requirements.txt` from `requirements-dev.txt`
- Use `poetry.lock` or `requirements.txt` with exact versions
- Regularly update dependencies and check for vulnerabilities

### Step 2: Docker Configuration
- Create multi-stage Dockerfile for smaller images
- Use appropriate base image (python:3.11-slim, python:3.11-alpine)
- Copy only necessary files
- Run as non-root user
- Use layer caching effectively
- Add healthcheck endpoint

### Step 3: Pre-commit Hooks
- Install pre-commit framework
- Configure hooks for: black, isort, ruff, mypy
- Add custom hooks if needed
- Ensure hooks run automatically before commits

### Step 4: PyInstaller Bundling
- Create spec file for PyInstaller
- Bundle application with all dependencies
- Test bundled executable on target platforms
- Optimize bundle size by excluding unnecessary modules

## Example Usage

**Poetry Setup (pyproject.toml):**
```toml
[tool.poetry]
name = "myapp"
version = "1.0.0"
description = "My Python application"
authors = ["Your Name <you@example.com>"]

[tool.poetry.dependencies]
python = "^3.10"
fastapi = "^0.104.0"
pydantic = "^2.5.0"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4.0"
black = "^23.0.0"
mypy = "^1.7.0"
ruff = "^0.1.0"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

**Multi-stage Dockerfile:**
```dockerfile
# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN useradd -m -u 1000 appuser

# Copy dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy application code
COPY --chown=appuser:appuser . .

# Set PATH
ENV PATH=/home/appuser/.local/bin:$PATH

# Switch to non-root user
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Pre-commit Configuration (.pre-commit-config.yaml):**
```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.11.0
    hooks:
      - id: black
        language_version: python3.11

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

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.7.1
    hooks:
      - id: mypy
        additional_dependencies: [types-all]

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
```

**PyInstaller Spec File (myapp.spec):**
```python
# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['src/main.py'],
    pathex=[],
    binaries=[],
    datas=[('config', 'config'), ('templates', 'templates')],
    hiddenimports=['pydantic'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='myapp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
```

**Setup Instructions:**
```bash
# Install pre-commit
pip install pre-commit
pre-commit install

# Poetry usage
poetry install
poetry run python main.py

# Docker build
docker build -t myapp:latest .
docker run -p 8000:8000 myapp:latest

# PyInstaller build
pyinstaller myapp.spec
```

## Notes
- Always test Docker images before deploying
- Keep Docker images small for faster deployments
- Use `.dockerignore` to exclude unnecessary files
- Run pre-commit hooks on all files: `pre-commit run --all-files`
- Test PyInstaller bundles on clean systems
