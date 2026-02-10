---
name: systematic_debugging
description: Use when encountering any bug, test failure, or unexpected behavior — requires completing root cause investigation before proposing any fixes. This is a rigid skill — follow exactly.
---

# Skill: systematic_debugging

The agent follows a structured 5-phase debugging methodology. Random fixes waste time and create new bugs. This is a rigid skill — follow exactly.

## Responsibility

Investigate bugs systematically by finding root cause before attempting fixes. Prevent guess-and-check debugging, symptom masking, and fix stacking.

## Rules

- Pre-flight environment check MUST pass before any investigation
- No fixes without root cause investigation first
- Complete each phase before proceeding to the next
- One hypothesis at a time, one variable at a time
- If 3+ fixes have failed, stop and question the architecture
- Read error messages completely — do not skip past them
- Read reference implementations completely — do not skim
- Say "I don't understand" when you don't — do not pretend

## Behavior

### Phase 0: Pre-flight Environment Check

**BEFORE starting any investigation, verify the execution environment:**

1. **Detect Operating System**
   - Determine if the environment is Windows, Linux, or macOS
   - Note path separator differences (`\` vs `/`)
   - Note shell differences (PowerShell/cmd vs bash/zsh)

2. **Verify Terminal Encoding**
   - Run: `python -c "import sys; print(sys.stdout.encoding)"`
   - If NOT `utf-8`: set `PYTHONIOENCODING=utf-8` before proceeding
   - On Windows, also check: `chcp` (should be `65001` for UTF-8)
   - **Do NOT skip this step** — encoding mismatches cause phantom `UnicodeEncodeError` that mask real bugs

3. **Verify Virtual Environment**
   - Confirm the active Python interpreter is from `.venv/`, NOT the global interpreter
   - Run: `python -c "import sys; print(sys.executable)"`
   - If the path does NOT contain `.venv` or `venv`: STOP — activate the correct environment first
   - Missing packages (`ModuleNotFoundError`) often mean the wrong interpreter is active

4. **Record Environment Baseline**
   - Python version, OS, encoding, interpreter path
   - Log this at the top of any debugging report
   - This prevents environment-related red herrings from polluting the root cause investigation

**If any pre-flight check fails, fix the environment FIRST before proceeding to Phase 1.**

### Phase 1: Root Cause Investigation

**BEFORE attempting ANY fix:**

1. **Read Error Messages Carefully**
   - Do not skip past errors or warnings
   - Read stack traces completely
   - Note line numbers, file paths, error codes

2. **Reproduce Consistently**
   - Can you trigger it reliably?
   - What are the exact steps?
   - If not reproducible, gather more data — do not guess

3. **Check Recent Changes**
   - What changed that could cause this?
   - Git diff, recent commits
   - New dependencies, config changes, environmental differences

4. **Gather Evidence in Multi-Component Systems**

   When a system has multiple components (CI → build → signing, API → service → database):

   ```
   For EACH component boundary:
     - Log what data enters the component
     - Log what data exits the component
     - Verify environment/config propagation
     - Check state at each layer

   Run once to gather evidence showing WHERE it breaks
   THEN analyze evidence to identify failing component
   THEN investigate that specific component
   ```

5. **Trace Data Flow**
   - Where does the bad value originate?
   - What called this with the bad value?
   - Keep tracing up until you find the source
   - Fix at source, not at symptom
   - See `root_cause_tracing` skill for the complete backward tracing technique

### Phase 2: Pattern Analysis

**Find the pattern before fixing:**

1. **Find Working Examples**
   - Locate similar working code in the same codebase
   - What works that is similar to what is broken?

2. **Compare Against References**
   - If implementing a pattern, read the reference implementation COMPLETELY
   - Do not skim — read every line
   - Understand the pattern fully before applying

3. **Identify Differences**
   - What is different between working and broken?
   - List every difference, however small
   - Do not assume "that can't matter"

4. **Understand Dependencies**
   - What other components does this need?
   - What settings, config, environment?
   - What assumptions does it make?

### Phase 3: Hypothesis and Testing

**Scientific method:**

1. **Form Single Hypothesis**
   - State clearly: "I think X is the root cause because Y"
   - Write it down
   - Be specific, not vague

2. **Test Minimally**
   - Make the SMALLEST possible change to test the hypothesis
   - One variable at a time
   - Do not fix multiple things at once

3. **Verify Before Continuing**
   - Did it work? YES → Phase 4
   - Did not work? Form NEW hypothesis
   - Do NOT add more fixes on top

4. **When You Don't Know**
   - Say "I don't understand X"
   - Do not pretend to know
   - Ask for help or research more

### Phase 4: Implementation

**Fix the root cause, not the symptom:**

1. **Create Failing Test Case**
   - Simplest possible reproduction
   - Automated test if possible
   - Use `testing_qa_mentor` skill for writing proper failing tests

2. **Implement Single Fix**
   - Address the root cause identified
   - ONE change at a time
   - No "while I'm here" improvements
   - No bundled refactoring

3. **Verify Fix**
   - Test passes now?
   - No other tests broken?
   - Issue actually resolved?
   - Use `verification_before_completion` skill before claiming success

4. **If Fix Doesn't Work**
   - STOP
   - Count: How many fixes have you tried?
   - If < 3: Return to Phase 1, re-analyze with new information
   - If >= 3: STOP and question the architecture (see step 5)

5. **If 3+ Fixes Failed: Question Architecture**

   Pattern indicating architectural problem:
   - Each fix reveals new shared state/coupling/problem in different place
   - Fixes require "massive refactoring" to implement
   - Each fix creates new symptoms elsewhere

   STOP and question fundamentals:
   - Is this pattern fundamentally sound?
   - Should we refactor architecture vs. continue fixing symptoms?

   **Discuss with the user before attempting more fixes.**

## Red Flags — STOP and Return to Phase 1

If you catch yourself thinking:
- "Quick fix for now, investigate later"
- "Just try changing X and see if it works"
- "Add multiple changes, run tests"
- "Skip the test, I'll manually verify"
- "It's probably X, let me fix that"
- "I don't fully understand but this might work"
- "Pattern says X but I'll adapt it differently"
- "Here are the main problems: [lists fixes without investigation]"
- Proposing solutions before tracing data flow
- "One more fix attempt" (when already tried 2+)
- Each fix reveals new problem in different place

**ALL of these mean: STOP. Return to Phase 1.**

## Rationalization Prevention

| Excuse | Reality |
|--------|---------|
| "Issue is simple, don't need process" | Simple issues have root causes too. Process is fast for simple bugs. |
| "Emergency, no time for process" | Systematic debugging is FASTER than guess-and-check. |
| "Just try this first, then investigate" | First fix sets the pattern. Do it right from the start. |
| "I'll write test after confirming fix works" | Untested fixes don't stick. Test first proves it. |
| "Multiple fixes at once saves time" | Can't isolate what worked. Causes new bugs. |
| "Reference too long, I'll adapt the pattern" | Partial understanding guarantees bugs. Read completely. |
| "I see the problem, let me fix it" | Seeing symptoms is not understanding root cause. |
| "One more fix attempt" (after 2+ failures) | 3+ failures = architectural problem. Question the pattern. |

## Quick Reference

| Phase | Key Activities | Success Criteria |
|-------|---------------|------------------|
| **0. Pre-flight** | Detect OS, verify encoding (UTF-8), verify venv, record baseline | Environment confirmed stable |
| **1. Root Cause** | Read errors, reproduce, check changes, gather evidence | Understand WHAT and WHY |
| **2. Pattern** | Find working examples, compare | Identify differences |
| **3. Hypothesis** | Form theory, test minimally | Confirmed or new hypothesis |
| **4. Implementation** | Create test, fix, verify | Bug resolved, tests pass |

## Related Skills

- `root_cause_tracing` — Trace bugs backward through call stack to find original trigger
- `testing_qa_mentor` — For creating failing test cases (Phase 4, Step 1)
- `verification_before_completion` — Verify fix worked before claiming success
