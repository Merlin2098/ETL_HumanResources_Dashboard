---
name: skill_authority_first
description: Use at the start of every task - establishes that skills must be discovered and invoked before any response or action, including clarifying questions.
---

# Skill: skill_authority_first

The agent treats skills as authoritative operational units. Skills are invoked BEFORE any response, including clarifying questions.

## Rules

- Invokes existing skills when responsibilities match
- Does not reimplement logic covered by a skill
- Avoids partial or approximate reproductions of skill behavior
- Treats skill definitions as the single source of truth
- Checks for applicable skills BEFORE any response or action
- Even a 1% chance a skill might apply means it MUST be invoked
- If an invoked skill turns out to be wrong for the situation, it does not need to be followed

## Invocation Flow

```
1. User message received
2. Check: Might any skill apply? (even 1% chance)
   - YES → Invoke the skill → Announce: "Using [skill] to [purpose]"
     → If skill has checklist → Create TodoWrite per item
     → Follow skill exactly
   - DEFINITELY NOT → Respond directly
```

## Skill Priority

When multiple skills could apply:

1. **Process skills first** (brainstorming, debugging) — these determine HOW to approach the task
2. **Implementation skills second** (domain-specific) — these guide execution

## Skill Types

- **Rigid** (debugging, verification): Follow exactly. Do not adapt away from the discipline.
- **Flexible** (patterns, conventions): Adapt principles to context.

The skill itself indicates which type it is.

## Red Flags — STOP and Invoke

These thoughts mean you are rationalizing away skill invocation:

| Thought | Reality |
|---------|---------|
| "This is just a simple question" | Questions are tasks. Check for skills. |
| "I need more context first" | Skill check comes BEFORE clarifying questions. |
| "Let me explore the codebase first" | Skills tell you HOW to explore. Check first. |
| "I can check git/files quickly" | Files lack conversation context. Check for skills. |
| "Let me gather information first" | Skills tell you HOW to gather information. |
| "This doesn't need a formal skill" | If a skill exists, use it. |
| "I remember this skill" | Skills evolve. Read current version. |
| "This doesn't count as a task" | Action = task. Check for skills. |
| "The skill is overkill" | Simple things become complex. Use it. |
| "I'll just do this one thing first" | Check BEFORE doing anything. |

## User Instructions

Instructions say WHAT, not HOW. "Add X" or "Fix Y" does not mean skip workflows defined by skills.
