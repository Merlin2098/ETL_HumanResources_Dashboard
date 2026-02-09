---
name: llm_inference_optimization
description: Use when processing multiple files, complex reasoning, or long sessions — manages token budget, context integrity, reasoning patterns, and temperature selection to ensure high-quality outputs.
---

# Skill: llm_inference_optimization

The agent manages its own cognitive resources, token budget, and reasoning patterns to ensure high-quality outputs and context integrity. This is a flexible skill — adapt principles to context.

## Responsibility

Optimize LLM inference quality by controlling token consumption, context window utilization, reasoning depth, and temperature selection. Prevent context overflow, shallow reasoning, and misallocated cognitive effort.

## Rules

- Estimate token cost before loading large files or skill sets
- Prune context proactively when session approaches capacity limits
- Select temperature dynamically based on task type
- Apply chain-of-thought reasoning before generating complex outputs
- Keep governance and context-loading skills anchored in high-priority context at all times
- Never sacrifice reasoning quality for token savings on critical tasks

## Behavior

### Step 1: Token Estimation

Before reading any file from `file_exploration/` or `python/`, estimate its token count:

- Approximation: ~4 characters per token
- If estimated token count exceeds 2,000 tokens, consider whether the full file is needed
- For files > 5,000 tokens, read only the sections relevant to the current task
- Log the estimation decision: "Estimated ~{N} tokens for {file}. Reading: full | partial | skipped."

### Step 2: Context Pruning

If the current session exceeds 60% of the model's context window:

1. **Identify non-essential content**: Documentation, examples, verbose comments
2. **Summarize**: Replace full content with concise summaries retaining key facts
3. **Retain core logic**: Function signatures, data flow, error handling paths
4. **Never prune**: Governance rules, protected file lists, active task plan, current skill instructions

Priority retention order:
1. Active task objective and constraints
2. `agent_rules.md` governance policies
3. `context_loading_protocol.md` loading order
4. Current skill definitions in use
5. Source code under modification
6. Reference documentation (prunable)
7. Examples and verbose comments (prunable first)

### Step 3: Dynamic Temperature Selection

Select temperature based on task type to balance precision vs. creativity:

| Task Domain | Temperature | Rationale |
|-------------|-------------|-----------|
| `governance/` skills | 0.0 | Deterministic enforcement — no creative deviation |
| `debugging/` skills | 0.0 | Systematic investigation — precision required |
| Schema validation tasks | 0.0 | Exact format compliance — no variation |
| `execution/` skills | 0.0 | Plan-following — deterministic execution |
| `planning/decision_process_flow` | 0.2 | Structured but allows option evaluation |
| `planning/risk_scoring_matrix` | 0.0 | Quantitative assessment — precision required |
| `brainstorming_design_explorer` | 0.7 | Creative exploration — divergent thinking needed |
| `ui/` architecture tasks | 0.7 | Design creativity — multiple valid approaches |
| `python/` code generation | 0.3 | Balance correctness with idiomatic alternatives |
| General analysis | 0.3 | Balance thoroughness with pattern recognition |

### Step 4: Chain-of-Thought (CoT) Protocol

Always execute the `decision_process_flow` protocol before generating:

- Final code implementations (> 50 lines)
- Complex plans with 3+ action steps
- Architectural decisions affecting multiple files
- Risk assessments for Major or Critical changes

CoT sequence:
1. State the problem clearly
2. List known constraints
3. Enumerate approaches (minimum 2)
4. Evaluate trade-offs for each approach
5. Select and justify the chosen approach
6. Then — and only then — generate the output

For simple tasks (< 50 lines, single file, low risk), CoT may be abbreviated to steps 1 + 5 + 6.

### Step 5: Contextual Anchoring

Ensure these resources are always present in high-priority context:

| Resource | Reason | Load Order |
|----------|--------|------------|
| `agent_rules.md` | Global governance policies | Always first |
| `context_loading_protocol.md` | Defines what to load and when | Always second |
| Active task plan (if any) | Current execution instructions | Before any action |
| Protected files blacklist | Prevent accidental violations | Before any file operation |

If any anchored resource is missing from context:
1. STOP current task
2. Reload the missing resource
3. Resume from last safe checkpoint

## Pipeline Position

```
llm_inference_optimization (how to think)
    --> context_loading_protocol (what to load)
        --> decision_process_flow (how to decide)
            --> [task-specific skill] (what to do)
                --> verification_before_completion (did it work)
```

This skill operates as a **meta-layer** that governs HOW other skills are invoked, not WHAT they do.

## Key Principles

| Principle | Rationale |
|-----------|-----------|
| Estimate before loading | Prevents context overflow from large files |
| Prune proactively | Maintains reasoning quality in long sessions |
| Temperature matches task | Precision where needed, creativity where useful |
| CoT before complexity | Structured reasoning prevents shallow outputs |
| Anchor governance always | Prevents governance drift during long sessions |
| Never prune active constraints | Safety rules must persist regardless of budget |

## Related Skills

- `context_loading_protocol` — Defines structured loading order (anchored by this skill)
- `decision_process_flow` — Executed as part of CoT protocol (Step 4)
- `brainstorming_design_explorer` — Uses high temperature (0.7) per Step 3
- `verification_before_completion` — Final verification after optimized inference
- `skill_authority_first` — This skill should be checked before heavy file processing tasks
