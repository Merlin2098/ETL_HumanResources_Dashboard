---
name: brainstorming_design_explorer
description: Use before any creative work — creating features, building components, adding functionality, or modifying behavior. Explores user intent, requirements, and design before implementation.
---

# Skill: brainstorming_design_explorer

The agent collaboratively turns ideas into fully formed designs and specs through structured dialogue before implementation begins. This is a flexible skill — adapt principles to context.

## Responsibility

Explore user intent, refine requirements, propose approaches with trade-offs, and produce a validated design before any code is written. Operates as the pre-planning phase before `decision_process_flow`.

## Rules

- Always check current project state first (files, docs, recent commits)
- Ask questions one at a time — never overwhelm with multiple questions
- Prefer multiple-choice questions when possible; open-ended is acceptable
- Propose 2-3 approaches with trade-offs before settling on one
- Lead with recommended approach and explain reasoning
- Present design in sections of 200-300 words, validating each before proceeding
- Apply YAGNI ruthlessly — remove unnecessary features from all designs
- Be ready to go back and clarify when something does not fit

## Behavior

### Step 1: Understand the Idea
- Load current project context (files, structure, recent commits)
- Ask questions one at a time to refine the idea
- Focus on understanding: purpose, constraints, success criteria
- Do not jump to solutions — understand the problem space first

### Step 2: Explore Approaches
- Propose 2-3 different approaches with trade-offs
- Present options conversationally with your recommendation and reasoning
- Lead with the recommended option and explain why
- Wait for user selection before proceeding

### Step 3: Present the Design
- Once approach is selected, present the design incrementally
- Break into sections of 200-300 words
- After each section, ask: "Does this look right so far?"
- Cover: architecture, components, data flow, error handling, testing strategy
- Go back and revise if something does not make sense

### Step 4: Document the Design
- Write the validated design to `docs/plans/YYYY-MM-DD-<topic>-design.md`
- Include: problem statement, chosen approach, architecture, components, data flow, testing strategy
- Commit the design document

### Step 5: Transition to Implementation
- Ask: "Ready to set up for implementation?"
- Hand off to `decision_process_flow` for structured planning
- Hand off to `execution_flow_orchestration` for execution

## Key Principles

| Principle | Rationale |
|-----------|-----------|
| One question at a time | Avoids overwhelming the user with decision fatigue |
| Multiple choice preferred | Easier to answer than open-ended; faster convergence |
| YAGNI ruthlessly | Prevents scope creep and unnecessary complexity |
| 2-3 approaches always | Ensures alternatives are considered before committing |
| Incremental validation | Catches misunderstandings early, before full design is complete |
| Flexible backtracking | Allows course correction without restarting from scratch |

## Pipeline Position

```
brainstorming_design_explorer (what to build)
    → decision_process_flow (how to build it)
        → execution_flow_orchestration (build it)
            → verification_before_completion (verify it)
```
