@echo off
setlocal
set PYTHONIOENCODING=utf-8
if not exist ".venv\Scripts\python.exe" (
  echo Missing .venv\Scripts\python.exe
  exit /b 1
)
if "%1"=="" (
  echo Usage: activate_kernel.cmd LITE^|STANDARD^|FULL
  exit /b 1
)
.\.venv\Scripts\python.exe agent_tools\activate_kernel.py --profile %1
endlocal
