---
name: root_cause_tracing
description: Use when a bug manifests deep in the call stack — traces backward through the call chain to find the original trigger, then fixes at the source. Companion to systematic_debugging.
---

# Skill: root_cause_tracing

The agent traces bugs backward through the call chain until finding the original trigger, then fixes at the source. This is a rigid skill — follow exactly.

## Responsibility

Find where bad data originates by tracing backward from the symptom through the call stack. Prevent fixing at the symptom point when the root cause is upstream.

## Rules

- Never fix just where the error appears — trace back to the source
- Trace at least one level up from every immediate cause found
- Add instrumentation when manual tracing reaches a dead end
- After fixing at source, add defense-in-depth validation at each layer
- Use `console.error()` in tests, not logger (logger may be suppressed)
- Log BEFORE the dangerous operation, not after it fails

## When to Use

- Error happens deep in execution (not at entry point)
- Stack trace shows long call chain
- Unclear where invalid data originated
- Need to find which test/code triggers the problem

## Behavior

### Step 1: Observe the Symptom

Identify the error and where it manifests.

```
Error: git init failed in /Users/project/packages/core
```

### Step 2: Find Immediate Cause

What code directly causes this?

```python
subprocess.run(['git', 'init'], cwd=project_dir)
# project_dir is wrong — but WHY is it wrong?
```

### Step 3: Ask "What Called This?"

Trace one level up in the call chain.

```
WorktreeManager.create_session(project_dir, session_id)
  → called by Session.initialize_workspace()
    → called by Session.create()
      → called by test at Project.create()
```

### Step 4: Keep Tracing Up

What value was passed at each level?

- `project_dir = ''` (empty string)
- Empty string as `cwd` resolves to `os.getcwd()`
- That is the source code directory — not intended

### Step 5: Find Original Trigger

Where did the empty string come from?

```python
context = setup_core_test()  # Returns {'temp_dir': ''}
Project.create('name', context['temp_dir'])  # Accessed before setup!
```

**Root cause:** Top-level variable initialization accessing empty value before setup runs.

## Adding Stack Traces

When manual tracing reaches a dead end, add instrumentation:

```python
import traceback

def git_init(directory: str):
    stack = traceback.format_stack()
    print(f"DEBUG git_init: directory={directory}, cwd={os.getcwd()}",
          file=sys.stderr)
    print("".join(stack), file=sys.stderr)

    subprocess.run(['git', 'init'], cwd=directory)
```

**Run and capture:**
```bash
python -m pytest 2>&1 | grep "DEBUG git_init"
```

**Analyze stack traces:**
- Look for test file names
- Find the line number triggering the call
- Identify the pattern (same test? same parameter?)

## Finding Which Test Causes Pollution

If something appears during tests but you don't know which test:

1. Run tests one-by-one in isolation
2. Binary search: run first half, then second half
3. Stop at first test that creates the pollution
4. Trace backward from that test

## Defense-in-Depth After Finding Root Cause

After fixing at the source, add validation at each layer to make the bug impossible:

```python
# Layer 1: Source function validates its own output
def setup_core_test():
    temp_dir = create_temp_directory()
    if not temp_dir:
        raise ValueError("temp_dir must not be empty")
    return {'temp_dir': temp_dir}

# Layer 2: Consumer validates input
def create_session(project_dir: str, session_id: str):
    if not project_dir:
        raise ValueError("project_dir must not be empty")
    ...

# Layer 3: Dangerous operation validates precondition
def git_init(directory: str):
    if not directory or not os.path.isabs(directory):
        raise ValueError(f"Invalid directory for git init: {directory}")
    ...
```

## Key Principle

```
Found immediate cause
  → Can trace one level up? YES → Trace backwards
    → Is this the source? NO → Keep tracing
    → Is this the source? YES → Fix at source → Add validation at each layer
  → Can trace one level up? NO → Add instrumentation, then trace
```

**NEVER fix just where the error appears.** Trace back to find the original trigger.

## Stack Trace Tips

- **In tests:** Use `print(..., file=sys.stderr)` not logger — logger may be suppressed
- **Before operation:** Log before the dangerous operation, not after it fails
- **Include context:** Directory, cwd, environment variables, timestamps
- **Capture stack:** `traceback.format_stack()` shows complete call chain

## Related Skills

- `systematic_debugging` — Parent methodology; this skill implements Phase 1, Step 5
- `verification_before_completion` — Verify the fix after applying it
