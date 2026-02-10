# Skill: Virtual Environment Python Executor

## Overview
Execute Python scripts and commands exclusively through virtual environment interpreter, never using global Python installation. **Global Python is strictly prohibited.**

## When to Use This Skill
- Running Python scripts in isolated environments
- Executing data processing pipelines
- Running tests in specific environments
- Checking package versions in venv
- Any Python execution requiring environment isolation

## When NOT to Use This Skill
- When global Python is explicitly needed (prohibited by skill design)
- Interactive Python sessions (use venv activation directly)
- Jupyter notebooks (use kernel selection)

## CRITICAL REQUIREMENTS

1. **This skill MUST use virtual environment Python. Global Python is NEVER used.**
2. **UTF-8 encoding is MANDATORY.** Every subprocess MUST set `PYTHONIOENCODING=utf-8` in its environment to prevent `UnicodeEncodeError` on Windows terminals with non-UTF-8 codepages.

## Implementation

```python
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Dict
import time

def venv_python_executor(
    venv_path: str,
    script_path: Optional[str] = None,
    command: Optional[str] = None,
    args: Optional[List[str]] = None,
    cwd: Optional[str] = None,
    capture_output: bool = True,
    timeout_seconds: Optional[int] = None
) -> Dict:
    """
    Execute Python scripts/commands exclusively through virtual environment.
    
    CRITICAL: Global Python is NEVER used. Virtual environment is mandatory.
    
    Args:
        venv_path: Path to virtual environment directory (REQUIRED)
        script_path: Path to Python script to execute (mutually exclusive with command)
        command: Python command to execute (mutually exclusive with script_path)
        args: Command-line arguments to pass
        cwd: Working directory for execution (default: current directory)
        capture_output: Capture stdout/stderr (default: True)
        timeout_seconds: Execution timeout (default: None = no timeout)
    
    Returns:
        dict with:
            - success: bool (True if exit code 0)
            - exit_code: int
            - stdout: str (if capture_output=True)
            - stderr: str (if capture_output=True)
            - execution_time_seconds: float
            - venv_python_used: str (path to venv Python)
    
    Raises:
        FileNotFoundError: If venv_path or script_path does not exist
        ValueError: If venv_path is invalid or both/neither script_path and command provided
        subprocess.TimeoutExpired: If timeout is exceeded
    """
    # Validate exactly one of script_path or command is provided
    if script_path is None and command is None:
        raise ValueError("Must provide either script_path or command")
    if script_path is not None and command is not None:
        raise ValueError("Cannot provide both script_path and command")
    
    # Validate venv exists
    venv_path_obj = Path(venv_path)
    if not venv_path_obj.exists():
        raise FileNotFoundError(f"Virtual environment not found: {venv_path}")
    
    # Locate venv Python interpreter
    if sys.platform == "win32":
        venv_python = venv_path_obj / "Scripts" / "python.exe"
    else:
        venv_python = venv_path_obj / "bin" / "python"
    
    # Validate venv Python exists
    if not venv_python.exists():
        raise ValueError(
            f"Invalid virtual environment: Python not found at {venv_python}\n"
            f"Expected: {venv_path}/bin/python (Linux/Mac) or "
            f"{venv_path}/Scripts/python.exe (Windows)"
        )
    
    # Validate pyvenv.cfg exists
    pyvenv_cfg = venv_path_obj / "pyvenv.cfg"
    if not pyvenv_cfg.exists():
        raise ValueError(
            f"Invalid virtual environment: pyvenv.cfg not found in {venv_path}"
        )
    
    # Validate script exists if provided
    if script_path:
        script_path_obj = Path(script_path)
        if not script_path_obj.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")
    
    # Build command
    cmd = [str(venv_python)]
    
    if script_path:
        cmd.append(str(Path(script_path).absolute()))
    elif command:
        # Split command (e.g., "-m pip list" → ["-m", "pip", "list"])
        cmd.extend(command.split())
    
    # Add arguments
    if args:
        cmd.extend(args)
    
    # Set working directory
    if cwd:
        cwd_path = Path(cwd)
        if not cwd_path.exists():
            raise FileNotFoundError(f"Working directory not found: {cwd}")
        cwd = str(cwd_path)
    
    # Execute command — force UTF-8 encoding to prevent UnicodeEncodeError
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    start_time = time.perf_counter()

    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            cwd=cwd,
            timeout=timeout_seconds,
            encoding='utf-8',
            env=env
        )
        
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        return {
            "success": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": result.stdout if capture_output else None,
            "stderr": result.stderr if capture_output else None,
            "execution_time_seconds": execution_time,
            "venv_python_used": str(venv_python.absolute())
        }
    
    except subprocess.TimeoutExpired as e:
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        
        raise subprocess.TimeoutExpired(
            cmd=e.cmd,
            timeout=e.timeout,
            output=f"Execution exceeded timeout of {timeout_seconds}s"
        )

```

## Usage Examples

```python
# Example 1: Run a Python script
result = venv_python_executor(
    venv_path="./venv",
    script_path="scripts/data_processor.py"
)

if result["success"]:
    print(f"✓ Script completed in {result['execution_time_seconds']:.2f}s")
    print(result["stdout"])
else:
    print(f"✗ Script failed with exit code {result['exit_code']}")
    print(result["stderr"])

# Example 2: Run script with arguments
result = venv_python_executor(
    venv_path="./venv",
    script_path="scripts/etl.py",
    args=["--input", "data/source.csv", "--output", "data/processed.parquet"]
)

# Example 3: Run Python command (check version)
result = venv_python_executor(
    venv_path="./venv",
    command="--version"
)
print(f"Python version: {result['stdout'].strip()}")

# Example 4: Run module command (pip list)
result = venv_python_executor(
    venv_path="./venv",
    command="-m pip list"
)
print("Installed packages:")
print(result["stdout"])

# Example 5: Execute inline code
result = venv_python_executor(
    venv_path="./venv",
    command='-c "import polars; print(polars.__version__)"'
)
print(f"Polars version: {result['stdout'].strip()}")

# Example 6: Run with working directory
result = venv_python_executor(
    venv_path="./venv",
    script_path="process.py",
    cwd="./data"  # Script runs in ./data directory
)

# Example 7: Run with timeout
result = venv_python_executor(
    venv_path="./venv",
    script_path="long_running_task.py",
    timeout_seconds=300  # 5 minutes max
)

# Example 8: Run data exploration script
result = venv_python_executor(
    venv_path="./venv",
    script_path="explore_data.py",
    args=["--file", "data.parquet", "--output", "report.html"]
)
```

## Advanced Usage Patterns

### Pattern 1: Pipeline Executor

```python
class VenvPipeline:
    """Execute multi-step pipeline in venv"""
    
    def __init__(self, venv_path: str):
        self.venv_path = venv_path
        self.results = []
    
    def run_step(self, step_name: str, script_path: str, args: List[str] = None):
        """Run a pipeline step"""
        print(f"Running {step_name}...")
        
        result = venv_python_executor(
            venv_path=self.venv_path,
            script_path=script_path,
            args=args or []
        )
        
        self.results.append({
            "step": step_name,
            "success": result["success"],
            "exit_code": result["exit_code"],
            "execution_time": result["execution_time_seconds"]
        })
        
        if not result["success"]:
            print(f"✗ {step_name} failed:")
            print(result["stderr"])
            return False
        
        print(f"✓ {step_name} completed in {result['execution_time_seconds']:.2f}s")
        return True
    
    def report(self):
        """Print pipeline report"""
        print("\n=== Pipeline Report ===")
        total_time = sum(r["execution_time"] for r in self.results)
        
        for r in self.results:
            status = "✓" if r["success"] else "✗"
            print(f"{status} {r['step']:20s}: {r['execution_time']:.2f}s")
        
        print(f"\nTotal time: {total_time:.2f}s")

# Usage
pipeline = VenvPipeline("./venv")
pipeline.run_step("Extract", "scripts/extract.py", ["--source", "api"])
pipeline.run_step("Transform", "scripts/transform.py")
pipeline.run_step("Load", "scripts/load.py", ["--target", "warehouse"])
pipeline.report()
```

### Pattern 2: Package Manager

```python
class VenvPackageManager:
    """Manage packages in virtual environment"""
    
    def __init__(self, venv_path: str):
        self.venv_path = venv_path
    
    def list_packages(self):
        """List installed packages"""
        result = venv_python_executor(
            venv_path=self.venv_path,
            command="-m pip list"
        )
        return result["stdout"]
    
    def install_package(self, package: str):
        """Install a package"""
        result = venv_python_executor(
            venv_path=self.venv_path,
            command=f"-m pip install {package}"
        )
        return result["success"]
    
    def check_version(self, package: str):
        """Check package version"""
        result = venv_python_executor(
            venv_path=self.venv_path,
            command=f'-c "import {package}; print({package}.__version__)"'
        )
        if result["success"]:
            return result["stdout"].strip()
        return None

# Usage
pm = VenvPackageManager("./venv")
print(pm.list_packages())
pm.install_package("polars")
version = pm.check_version("polars")
print(f"Polars version: {version}")
```

### Pattern 3: Test Runner

```python
def run_tests(venv_path: str, test_dir: str = "tests"):
    """Run pytest in virtual environment"""
    
    result = venv_python_executor(
        venv_path=venv_path,
        command=f"-m pytest {test_dir} -v"
    )
    
    print(result["stdout"])
    
    if result["success"]:
        print(f"✓ All tests passed ({result['execution_time_seconds']:.2f}s)")
    else:
        print(f"✗ Tests failed")
        print(result["stderr"])
    
    return result["success"]

# Usage
run_tests("./venv", "tests/")
```

## Script Types and Examples

### 1. Data Processing Script

```python
# scripts/process_data.py
import sys
import polars as pl

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pl.read_parquet(input_file)
df_clean = df.filter(pl.col("amount") > 0)
df_clean.write_parquet(output_file)

print(f"Processed {len(df_clean)} rows")
```

```python
# Run it
result = venv_python_executor(
    venv_path="./venv",
    script_path="scripts/process_data.py",
    args=["input.parquet", "output.parquet"]
)
```

### 2. ETL Script

```python
# scripts/etl_pipeline.py
import argparse
import polars as pl

parser = argparse.ArgumentParser()
parser.add_argument("--source", required=True)
parser.add_argument("--target", required=True)
args = parser.parse_args()

# ETL logic
df = pl.read_csv(args.source)
df_transformed = df.with_columns([
    pl.col("amount").cast(pl.Float64),
    pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
])
df_transformed.write_parquet(args.target)
```

```python
# Run it
result = venv_python_executor(
    venv_path="./venv",
    script_path="scripts/etl_pipeline.py",
    args=["--source", "data.csv", "--target", "data.parquet"]
)
```

## Best Practices

1. **Always validate venv before use**
   ```python
   from pathlib import Path
   
   venv_path = "./venv"
   if not (Path(venv_path) / "pyvenv.cfg").exists():
       print("Invalid venv, creating new one...")
       import subprocess
       subprocess.run([sys.executable, "-m", "venv", venv_path])
   
   # Now safe to use
   result = venv_python_executor(
       venv_path=venv_path,
       script_path="script.py"
   )
   ```

2. **Capture and log output**
   ```python
   import logging
   
   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)
   
   result = venv_python_executor(
       venv_path="./venv",
       script_path="process.py"
   )
   
   if result["success"]:
       logger.info(f"Script output:\n{result['stdout']}")
   else:
       logger.error(f"Script failed:\n{result['stderr']}")
   ```

3. **Handle timeouts gracefully**
   ```python
   try:
       result = venv_python_executor(
           venv_path="./venv",
           script_path="long_task.py",
           timeout_seconds=300
       )
   except subprocess.TimeoutExpired:
       print("Task exceeded 5-minute timeout")
   ```

4. **Pass configuration via arguments**
   ```python
   # Instead of environment variables, use arguments
   result = venv_python_executor(
       venv_path="./venv",
       script_path="script.py",
       args=["--config", "config.yaml", "--env", "production"]
   )
   ```

## Error Handling

```python
def safe_venv_executor(venv_path: str, script_path: str, **kwargs):
    """Execute with comprehensive error handling"""
    
    try:
        result = venv_python_executor(
            venv_path=venv_path,
            script_path=script_path,
            **kwargs
        )
        
        return result
    
    except FileNotFoundError as e:
        return {
            "success": False,
            "error": "FileNotFoundError",
            "message": str(e),
            "exit_code": -1
        }
    
    except ValueError as e:
        return {
            "success": False,
            "error": "ValueError",
            "message": str(e),
            "exit_code": -1
        }
    
    except subprocess.TimeoutExpired as e:
        return {
            "success": False,
            "error": "TimeoutExpired",
            "message": str(e),
            "exit_code": -1
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": "UnexpectedError",
            "message": str(e),
            "exit_code": -1
        }
```

## Performance Characteristics

- **Startup Overhead**: 50-200ms (venv activation)
- **Execution**: Same as script performance
- **Memory**: Script memory + subprocess overhead

## Dependencies

```bash
# No additional dependencies required
# Uses Python standard library only
```

## Common Pitfalls

1. **Using global Python (FORBIDDEN)**
   ```python
   # ✗ WRONG - This skill requires venv_path
   # Global Python is never used
   
   # ✓ CORRECT - Always specify venv_path
   result = venv_python_executor(
       venv_path="./venv",  # REQUIRED
       script_path="script.py"
   )
   ```

2. **Providing both script and command**
   ```python
   # Error: Cannot provide both
   result = venv_python_executor(
       venv_path="./venv",
       script_path="script.py",
       command="-c 'print(1)'"  # ERROR
   )
   
   # Correct: Choose one
   result = venv_python_executor(
       venv_path="./venv",
       script_path="script.py"  # OR command, not both
   )
   ```

3. **Relative paths in scripts**
   ```python
   # Scripts may have different working directory
   # Use absolute paths or specify cwd
   
   result = venv_python_executor(
       venv_path="./venv",
       script_path="scripts/process.py",
       cwd="./data"  # Script runs from here
   )
   ```

4. **Not handling exit codes**
   ```python
   result = venv_python_executor(
       venv_path="./venv",
       script_path="script.py"
   )
   
   if result["exit_code"] != 0:
       print(f"Script failed with code {result['exit_code']}")
       print(result["stderr"])
   ```

## Integration with Agents

This skill is **deterministic** and should be called by agents when:
- Running Python scripts in isolated environments
- Executing data processing pipelines
- Running tests or validations
- Checking package versions
- Any task requiring venv isolation

Agents MUST remember:
- **ALWAYS use virtual environment** (global Python forbidden)
- Exactly one of script_path or command must be provided
- Validate venv exists before use
- Script paths should be absolute or relative to cwd
