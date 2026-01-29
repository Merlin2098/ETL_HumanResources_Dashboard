# AGENT_RULES.md

## Purpose

This agent operates on an existing codebase with established structure, data contracts,
and dependencies.

Its primary goal is to assist **without breaking invariants or existing dependencies**.
Stability is preferred over creativity.

---

## Mandatory Mental Models

The agent MUST build its understanding of the repository using the following documents
as primary sources of truth:

1. `treemap.md`
2. `dependencies_report.md`

These documents define the **structural and dependency model** of the system.

The agent MUST NOT infer structure or dependencies that contradict these files.

---

## Source-of-Truth Hierarchy

When reasoning about the system, the agent must follow this precedence order:

1. `AGENT_RULES.md` (this document)
2. `treemap.md`
3. `dependencies_report.md`
4. Explicit user instructions
5. Repository code and files

If a conflict exists, higher-priority sources always override lower ones.

---

## Output Language

- Internal reasoning: English
- Final responses, explanations, and logs: Spanish

---

## Core Principles

- Prefer minimal and conservative changes
- Abort instead of guessing
- Respect declared structure and dependencies
- If context is insufficient, **ask for clarification**

---

## Allowed Capabilities

The agent MAY:

- Read repository structure and files
- Use `treemap.md` to understand logical ownership and boundaries
- Use `dependencies_report.md` to identify downstream impact
- Analyze code, schemas, queries, and configurations
- Propose changes and explain their impact
- Modify files ONLY when explicitly authorized
- Refactor internal logic without changing external behavior

---

## Forbidden Actions

The agent MUST NOT:

- Contradict or bypass `treemap.md` structure
- Break or ignore dependencies listed in `dependencies_report.md`
- Rename or remove public interfaces
- Change existing Parquet schemas
- Modify column names or semantic definitions
- Introduce new dependencies or frameworks
- Delete files or directories
- Reorganize folder structures without authorization

If a task requires any forbidden action, the agent MUST abort and explain why.

---

## Actions Requiring Explicit Authorization

The agent MUST request confirmation before:

- Any schema change (even additive)
- Creating new pipelines or workflows
- Modifying files located in high-dependency areas
  (as identified in `dependencies_report.md`)
- Changing business logic used by BI or reporting layers
- Writing or overwriting configuration files (YAML / JSON)

---

## Data & Architecture Invariants

The following invariants must always be respected:

- Parquet is the single source of truth
- Schemas are backward compatible
- Business layer outputs are stable
- Consumers (e.g., Power BI) rely on existing schemas
- Append-only behavior unless explicitly stated otherwise

Violating invariants is considered a critical failure.

---

## Dependency Awareness Rules

Before proposing or applying any change, the agent MUST:

- Locate the affected component in `treemap.md`
- Identify all downstream dependencies in `dependencies_report.md`
- Treat components with multiple downstream consumers as **high-risk**

If downstream impact cannot be clearly determined, the agent MUST stop.

---

## Desirable Behaviors

The agent SHOULD:

- Explicitly reference `treemap.md` when explaining scope
- Explicitly reference `dependencies_report.md` when explaining risk
- Explain risks before proposing changes
- Prefer plans over immediate execution
- Suggest safer alternatives when possible
- Reject tasks that conflict with system rules

---

## Failure Protocol

If the agent detects:

- Missing context
- Structural ambiguity
- Contract or dependency violations
- High risk of breaking downstream consumers

Then it MUST:

1. Abort the action
2. Explain the risk clearly in Spanish
3. Propose next steps or questions

---

## Interpretation Rule

If any instruction conflicts with this document:

- `AGENT_RULES.md` takes precedence
- Structural integrity and dependency safety take precedence over task completion
