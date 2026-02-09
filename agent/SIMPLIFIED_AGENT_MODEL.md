# Simplified Agent Model

**Version**: 1.0.0
**Date**: 2026-02-03
**Status**: ACTIVE

---

## Overview

This document defines a simplified agent model for the Certificate Generator project.
It supersedes the complex Inspector-Executor-Protocol architecture for most use cases.

---

## Agent Types

### 1. Exploration Agent

**Purpose**: Read-only analysis and understanding

| Aspect | Value |
|--------|-------|
| Trust Level | READ_ONLY |
| Can Modify Files | No |
| Can Execute Code | No |
| Output | Analysis reports, recommendations |

**Use Cases**:
- Codebase exploration
- Dependency analysis
- Architecture review
- Bug investigation

### 2. Targeted Change Agent

**Purpose**: Small, well-defined modifications

| Aspect | Value |
|--------|-------|
| Trust Level | WRITE_SCOPED |
| Can Modify Files | Yes (within scope) |
| Can Execute Code | No |
| Output | Modified files, change summary |

**Use Cases**:
- Fix typos or comments
- Update configuration values
- Add simple features
- Refactor small sections

**Constraints**:
- Maximum 5 files per operation
- Must explain changes before applying
- Cannot modify protected files (see agent_rules.md)

---

## When to Use Full Inspector-Executor

The complex Inspector-Executor pattern (defined in agent_inspector.md and agent_executor.md) is still appropriate for:

1. **Large-scale refactoring** (10+ files)
2. **Architectural changes** (new patterns, major restructuring)
3. **High-risk operations** (database changes, security modifications)
4. **Audit-required changes** (compliance, traceability needs)

---

## Decision Matrix

| Task Type | Agent Model | Artifacts Required |
|-----------|-------------|--------------------| | Read/explore codebase | Exploration | None |
| Fix typo | Targeted Change | None |
| Add simple feature (1-3 files) | Targeted Change | Brief summary |
| Add complex feature (4-10 files) | Targeted Change | task_plan.json |
| Major refactor (10+ files) | Inspector-Executor | Full plan + config |
| Security-sensitive change | Inspector-Executor | Full plan + approval |

---

## Project Context

This is a **Certificate Generator** application:
- Reads employee data from Excel
- Generates PowerPoint certificates from templates
- Converts to PDF for distribution
- Desktop application using PySide6

**Not an ETL system** - references to Bronze/Silver/Gold pipelines, SQL queries, or database schemas in older documentation are outdated.

---

## File Structure (Simplified)

```
agent/
├── agent_rules.md              # Core behavioral rules
├── SIMPLIFIED_AGENT_MODEL.md   # This document
├── agent_inspector/
│   └── agent_inspector.md      # Full inspector spec (complex tasks)
├── agent_executor/
│   └── agent_executor.md       # Full executor spec (complex tasks)
├── agent_protocol/
│   ├── README.md               # Protocol documentation
│   └── schemas/                # JSON/YAML schemas for validation
└── agent_outputs/
    ├── plans/                  # Active and pending plans
    ├── reports/                # Execution reports with backups
    └── archive/                # Archived plans (timestamped)
```

---

## Related Documents

- [agent_rules.md](agent_rules.md) - Core behavioral rules (authoritative)
- [agent_inspector.md](agent_inspector/agent_inspector.md) - Full inspector spec (for complex tasks)
- [agent_executor.md](agent_executor/agent_executor.md) - Full executor spec (for complex tasks)
