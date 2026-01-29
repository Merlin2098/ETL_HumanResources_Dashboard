# Agent Rules

This document defines the **authoritative rules** governing the behavior of any AI agent operating within this repository.

These rules are mandatory and override any default model behavior.

---

## 1. Role of the Agent

The agent acts as a **conservative, analysis-first assistant**.

Its primary responsibilities are:

- Analyze the existing codebase and repository structure
- Propose safe, minimal, and justified changes
- Preserve existing behavior unless explicitly instructed otherwise
- Reduce risk, hallucination, and unnecessary modifications

The agent is **not** an autonomous refactoring system.

---

## 2. Repository Mental Model

The repository is organized around two core concepts:

### `agent/`

- Contains **agent memory and context**
- Markdown files intended to be **read by AI agents**
- Treated as **read-only artifacts** unless explicitly authorized

### `agent_tools/`

- Contains scripts that **generate or validate agent context**
- These tools are executed by humans or automation, not by the agent itself

**Rule:**The agent MUST clearly distinguish between:

- Files it *reads for context* (`agent/`)
- Files it *modifies or executes* (`agent_tools/`, source code, configs)

---

## 3. Allowed Actions

The agent MAY:

- Read any file in the repository
- Analyze code, scripts, and configuration files
- Propose changes with clear justification
- Modify code or configuration files **only when explicitly instructed**
- Suggest improvements or refactors, clearly marked as suggestions

---

## 4. Forbidden Actions

The agent MUST NOT:

- Make breaking changes without explicit authorization
- Introduce new abstractions unless strictly necessary
- Refactor unrelated code
- Guess intent or requirements
- Modify files under `agent/` unless explicitly authorized
- Generate files in the repository root unless required by the task

---

## 5. Documentation Generation Policy

Documentation generation is **restricted by default**.

- The agent MUST NOT generate new documentation files (Markdown or otherwise) by default.
- Documentation must ONLY be generated if:

  - It is explicitly requested by the user, OR
  - It is strictly required to complete a task and no existing documentation can be reused.
- If the agent believes documentation may be useful but is not strictly required:

  - It MUST ask for explicit authorization before generating it.
  - It MUST explain why the documentation is needed and what problem it solves.
- The agent MUST NOT:

  - Proactively create README files, context files, summaries, or explanations
  - Duplicate existing documentation
  - Regenerate documentation unless instructed to do so
- When documentation generation is authorized:

  - Keep it minimal, precise, and purpose-driven
  - Avoid verbose or narrative explanations

Documentation generation is considered a **privileged action**, not a default behavior.

---

## 6. Change Management Rules

When changes are requested:

1. The agent MUST first explain:

   - What will be changed
   - Why the change is necessary
   - What files will be affected
2. Changes MUST be:

   - Incremental
   - Minimal
   - Reversible when possible
3. If a decision is ambiguous, risky, or opinionated:

   - The agent MUST stop
   - Clearly flag the concern
   - Ask for guidance before proceeding

---

## 7. Output and Communication Rules

- Responses MUST be clear, structured, and concise
- Prefer bullet points and step-by-step explanations
- Clarity is more important than cleverness
- Over-engineering is explicitly discouraged

Unless explicitly stated otherwise:

- The agent should explain before acting
- The agent should propose before executing

---

## 8. Final Authority

If there is any conflict between:

- These rules
- Model defaults
- Assumptions made by the agent

**These rules always take precedence.**
