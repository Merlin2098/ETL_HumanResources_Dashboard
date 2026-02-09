---
name: verification_before_completion
description: Use when about to claim work is complete, fixed, or passing — requires running verification commands and confirming output before making any success claims. Evidence before assertions, always.
---

# Skill: verification_before_completion

The agent never claims completion without fresh verification evidence. This is a rigid skill — follow exactly.

## Rules

- No completion claims without running the verification command in the current message
- No expressions of satisfaction ("Great!", "Done!", "Perfect!") without evidence
- No committing, PR creation, or task transition without verification
- No trusting agent delegation reports without independent verification
- No partial verification — full command, full output, check exit code
- No weasel words: "should", "probably", "seems to" are forbidden without evidence

## The Gate Function

```
BEFORE claiming any status or expressing satisfaction:

1. IDENTIFY: What command proves this claim?
2. RUN: Execute the FULL command (fresh, complete)
3. READ: Full output, check exit code, count failures
4. VERIFY: Does output confirm the claim?
   - If NO → State actual status with evidence
   - If YES → State claim WITH evidence
5. ONLY THEN: Make the claim

Skipping any step = lying, not verifying
```

## Verification Requirements

| Claim | Requires | Not Sufficient |
|-------|----------|----------------|
| Tests pass | Test command output: 0 failures | Previous run, "should pass" |
| Linter clean | Linter output: 0 errors | Partial check, extrapolation |
| Build succeeds | Build command: exit 0 | Linter passing, logs look good |
| Bug fixed | Test original symptom: passes | Code changed, assumed fixed |
| Regression test works | Red-green cycle verified | Test passes once |
| Agent completed | VCS diff shows changes | Agent reports "success" |
| Requirements met | Line-by-line checklist | Tests passing |

## Red Flags — STOP

If you catch yourself doing any of these, STOP and run verification:

- Using "should", "probably", "seems to"
- Expressing satisfaction before verification
- About to commit/push/PR without verification
- Trusting agent success reports
- Relying on partial verification
- Thinking "just this once"

## Rationalization Prevention

| Excuse | Reality |
|--------|---------|
| "Should work now" | RUN the verification |
| "I'm confident" | Confidence is not evidence |
| "Just this once" | No exceptions |
| "Linter passed" | Linter is not compiler |
| "Agent said success" | Verify independently |
| "Partial check is enough" | Partial proves nothing |

## When to Apply

**ALWAYS before:**
- Any variation of success/completion claims
- Any expression of satisfaction about work state
- Any positive statement about work correctness
- Committing, PR creation, task completion
- Moving to next task
- Delegating to agents

## Key Patterns

**Tests:**
```
CORRECT: [Run test command] → [See: 34/34 pass] → "All tests pass"
WRONG:   "Should pass now" / "Looks correct"
```

**Build:**
```
CORRECT: [Run build] → [See: exit 0] → "Build passes"
WRONG:   "Linter passed" (linter does not check compilation)
```

**Requirements:**
```
CORRECT: Re-read plan → Create checklist → Verify each → Report gaps or completion
WRONG:   "Tests pass, phase complete"
```

**Agent delegation:**
```
CORRECT: Agent reports success → Check VCS diff → Verify changes → Report actual state
WRONG:   Trust agent report
```
