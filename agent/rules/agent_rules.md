# Agent Governance Rules

## Version

3.1.0

## Scope

GLOBAL GOVERNANCE DOCUMENT

This document defines the **global governance, security model, and workspace policies**
for all AI agents operating in this repository.

It is a **human-authoritative reference**, not an execution prompt.

---

## 1. Purpose

The purpose of this document is to:

- Establish a shared mental model of the repository
- Define immutable security and workspace boundaries
- Declare non-negotiable governance policies
- Delegate operational enforcement to skills
- Serve as a reference for designing agent-specific contracts

Agent-specific behavior MUST be defined in:

- `agent/agent_inspector/agent_inspector.md`
- `agent/agent_executor/agent_executor.md`
- Other `agent_<name>.md` contracts

---

## 2. Governance Model

### 2.1 Authority Layers

| Layer               | Description                        | Mutability |
| ------------------- | ---------------------------------- | ---------- |
| Governance          | Repository-wide rules and policies | Rare       |
| Agent Contract      | Role-specific operational rules    | Versioned  |
| Runtime Constraints | Per-execution limitations          | Ephemeral  |

No agent may operate under more than **one active contract** at runtime.

Governance rules are **policy declarations**, not execution logic.

---

## 3. Repository Mental Model

### 3.1 `agent/` Directory

Purpose:

- Agent memory
- Agent outputs
- Agent logs
- Agent-readable documentation
- Skill definitions

Characteristics:

- Autonomous workspace
- Writable by agents (within constraints)
- NOT part of application runtime

Key subdirectories:

- `agent/agent_outputs/` — plans, reports, artifacts
- `agent/agent_logs/` — execution logs
- `agent/temp/` — temporary agent files
- `agent/skills/` — authoritative skill definitions

---

### 3.2 Application Source Code

Directories such as:

- `src/`
- `ui/`
- `tests/`

Are considered **PROTECTED APPLICATION CODE**.

Rules:

- Agents MUST NOT modify source code directly
- Source code changes require:
  - A validated plan
  - Explicit authorization
  - Human approval (unless otherwise stated)

---

## 4. Security and Protection Policy

### 4.1 Protected Files (GLOBAL BLACKLIST)

The following files and paths are IMMUTABLE by default:

```yaml
protected_files:
  documentation:
    - agent/agent_rules.md
    - agent/architecture_proposal.md
    - agent/agent_inspector/agent_inspector.md
    - agent/agent_executor/agent_executor.md
    - agent/agent_protocol/README.md
    - README.md

  configuration:
    - .git/**
    - .env
    - .env.*
    - credentials.json
    - secrets.*
    - requirements.txt
    - pyproject.toml
    - setup.py
    - .pre-commit-config.yaml
```

Any attempt to modify protected files MUST be rejected.

### 4.2 Execution Environment Policies
#### 4.2.1 Python Execution Isolation Policy

Python execution is subject to a mandatory isolation policy.

**Policy declaration:**

- Execution of Python via a global interpreter is **STRICTLY PROHIBITED**
- All Python execution MUST occur through an approved virtual environment
- This policy is non-negotiable and applies to all agents and execution contexts

**Mandatory runtime environment:**

- **Virtual environment**: EVERY Python invocation in the terminal MUST use the local virtual environment interpreter:
  - Windows: `.venv/Scripts/python.exe`
  - Unix/macOS: `.venv/bin/python`
- **UTF-8 encoding**: EVERY Python execution MUST set `PYTHONIOENCODING=utf-8` as an environment variable. This prevents `UnicodeEncodeError` on Windows terminals with non-UTF-8 default codepages.
- **Combined command pattern** (Windows):
  ```
  set PYTHONIOENCODING=utf-8 && .venv\Scripts\python.exe <script.py>
  ```
- **Combined command pattern** (Unix/macOS):
  ```
  PYTHONIOENCODING=utf-8 .venv/bin/python <script.py>
  ```

**Enforcement model:**

- This policy is enforced exclusively through a governance-approved runtime skill
- Agents MUST NOT implement, reproduce, or reason about enforcement logic
- Any Python execution not performed via the approved skill constitutes a governance violation

The authoritative implementation of this policy resides under: `agent/skills/runtime/`

#### 4.2.2 Skill-Oriented Execution Model

This repository follows a skill-oriented execution model.

**Policy declaration:**

- Skills represent reusable, deterministic, authoritative procedures
- Skills are the single source of truth for operational behavior
- When a task matches the responsibility of an existing skill, agents MUST invoke the skill

**Restrictions:**

- Agents MUST NOT reimplement logic covered by a skill
- Agents MUST NOT partially reproduce skill behavior
- Agents MUST NOT reason through steps already encapsulated by a skill

**Governance principle:**

If a capability exists as a skill, the skill is authoritative. Reasoning is subordinate to invocation.

## 5. Workspace Persistence Policy

- Agent-generated plans MUST be persisted to disk
- Historical plans and reports MUST NOT be deleted
- Temporary files MAY be cleaned up by the agent
- Outputs MUST be stored under `agent/agent_outputs/`

**Persistence guarantees:**

- Auditability
- Traceability
- Reproducibility

## 6. Documentation Governance

Documentation generation is restricted by default.

**Rules:**

Agents MUST NOT generate new documentation unless:

- Explicitly requested, OR
- Strictly required to complete a task

When authorized:

- Documentation must be minimal
- Documentation must be purpose-driven
- Duplication is forbidden

## 7. Change Authority

| Change Type         | Authority           |
| ------------------- | ------------------- |
| Governance rules    | Human-only          |
| Agent contracts     | Human-reviewed      |
| Runtime constraints | User / Orchestrator |
| Execution plans     | Agent Inspector     |
| Execution           | Agent Executor      |

Agents MUST escalate ambiguity instead of guessing intent.

## 8. Conflict Resolution

If a conflict arises between:

- Governance rules
- Agent contracts
- Runtime constraints

Resolution order is:

1. Governance rules
2. Agent contract
3. Runtime constraints

## 9. Design Principle

- **Governance** defines what is never allowed
- **Contracts** define what an agent may do
- **Skills** define how it is done
- **Constraints** define what is allowed right now

## 10. Status

This document is:

- Informational for agents
- Authoritative for humans
- NOT intended to be injected into runtime prompts

---

**Version:** 3.1.0
**Last Updated:** 2026-02-07
**Classification:** GOVERNANCE-ONLY