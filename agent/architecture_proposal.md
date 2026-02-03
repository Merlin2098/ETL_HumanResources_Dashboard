# Multi-Agent Architecture Proposal

## Document Information

| Field | Value |
|-------|-------|
| Version | 1.1.0 |
| Date | 2026-02-03 |
| Status | PROPOSAL |
| Author | Claude Agent |
| Project | ETL_KPIS_AdministracionPersonal |

---

## Executive Summary

This document proposes a transition from the current single-agent orchestration model to a two-agent architecture that separates **planning/analysis** (agent_inspector) from **execution** (agent_executor). The design prioritizes:

- **Separation of Concerns**: Clear boundaries between analysis and execution
- **Safety**: No direct file modifications by the inspector agent
- **Reversibility**: All executor actions must be reversible when possible
- **Traceability**: Complete audit trail of decisions and actions
- **Compatibility**: Preservation of existing system behavior

---

## Table of Contents

1. [Current State Analysis](#1-current-state-analysis)
2. [Proposed Architecture](#2-proposed-architecture)
3. [Folder Structure](#3-folder-structure)
4. [Agent Specifications](#4-agent-specifications)
5. [Communication Protocol](#5-communication-protocol)
6. [Schema Definitions](#6-schema-definitions)
7. [Agent Tools](#7-agent-tools)
8. [Migration Strategy](#8-migration-strategy)
9. [Risk Analysis](#9-risk-analysis)
10. [Usage Instructions](#10-usage-instructions)

---

## 1. Current State Analysis

### 1.1 Existing Architecture

The current system operates with a **single orchestration agent** defined in `agent_rules.md` with the following characteristics:

| Aspect | Current State |
|--------|---------------|
| Agent Count | 1 (monolithic orchestrator) |
| Responsibilities | Analysis + Planning + Execution |
| Context Files | `agent_rules.md`, `treemap.md`, `dependencies_report.md` |
| Tools Location | `agent_tools/` (human-executed scripts) |
| Safety Model | Conservative, analysis-first approach |
| Modification Policy | Read-only unless explicitly instructed |

### 1.2 Current Limitations

1. **No separation of concerns**: Planning and execution are conflated
2. **Limited traceability**: No structured decision logging
3. **No reversibility mechanism**: Changes cannot be automatically rolled back
4. **No formal task lifecycle**: Ad-hoc task management
5. **No inter-agent communication protocol**: Single agent handles everything

### 1.3 Components to Preserve

- ETL pipeline structure (Bronze → Silver → Gold)
- Schema validation system (`esquemas/*.json`)
- SQL transformation queries (`queries/*.sql`)
- UI architecture (Widget + Worker + Config pattern)
- Pre-commit hook automation
- Path caching and utility systems

---

## 2. Proposed Architecture

### 2.1 High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        USER / EXTERNAL TRIGGER                       │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AGENT_INSPECTOR                              │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  • System Analysis                                           │    │
│  │  • Planning & Task Decomposition                             │    │
│  │  • Behavioral Preservation Check                             │    │
│  │  • Decision Generation                                       │    │
│  │  • Risk Assessment                                           │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  READS: treemap.md, dependencies_report.md, esquemas/*.json          │
│  OUTPUTS: task_plan.json, system_config.yaml                         │
│  CONSTRAINTS: NEVER modifies project files directly                  │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  │ Task Envelope (JSON)
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       AGENT_PROTOCOL LAYER                           │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  • Message Validation                                        │    │
│  │  • Schema Enforcement                                        │    │
│  │  • Version Tracking                                          │    │
│  │  • Audit Logging                                             │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  │ Validated Task
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AGENT_EXECUTOR                               │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  • Execute Approved Actions                                  │    │
│  │  • Implement Changes Safely                                  │    │
│  │  • Generate Rollback Checkpoints                             │    │
│  │  • Report Execution Status                                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  READS: task_plan.json from inspector                                │
│  MODIFIES: Only files explicitly listed in task plan                 │
│  OUTPUTS: execution_report.json, rollback_manifest.json              │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          AGENT_OUTPUTS                               │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  • Execution Reports                                         │    │
│  │  • Change Logs                                               │    │
│  │  • Rollback Manifests                                        │    │
│  │  • Audit Trails                                              │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Agent Interaction Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           TASK LIFECYCLE                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. REQUEST_RECEIVED                                                     │
│     └─► Inspector receives task request                                  │
│                                                                          │
│  2. ANALYSIS_IN_PROGRESS                                                 │
│     └─► Inspector analyzes codebase, dependencies, risks                 │
│                                                                          │
│  3. PLAN_GENERATED                                                       │
│     └─► Inspector outputs task_plan.json + system_config.yaml            │
│                                                                          │
│  4. AWAITING_APPROVAL (optional)                                         │
│     └─► Human review for high-risk operations                            │
│                                                                          │
│  5. EXECUTION_QUEUED                                                     │
│     └─► Protocol validates and forwards to executor                      │
│                                                                          │
│  6. EXECUTION_IN_PROGRESS                                                │
│     └─► Executor performs actions with rollback checkpoints              │
│                                                                          │
│  7. EXECUTION_COMPLETED | EXECUTION_FAILED | EXECUTION_ROLLED_BACK       │
│     └─► Executor generates final report                                  │
│                                                                          │
│  8. ARCHIVED                                                             │
│     └─► Task moved to historical logs                                    │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Folder Structure

### 3.1 Proposed Directory Layout

```
project_root/
│
├── agent/                                    # CENTRALIZED AGENT SYSTEM
│   ├── agent_rules.md                        # DEPRECATED → retained for compatibility
│   ├── treemap.md                            # Auto-generated structure map
│   ├── dependencies_report.md                # Auto-generated dependency analysis
│   ├── architecture_proposal.md              # This document
│   │
│   ├── agent_inspector/                      # Inspector agent resources
│   │   ├── agent_inspector.md                # Inspector behavior specification
│   │   ├── context/                          # Additional read-only context
│   │   │   ├── analysis_templates/           # Templates for analysis reports
│   │   │   └── risk_matrices/                # Risk assessment criteria
│   │   └── schemas/                          # Inspector-specific schemas
│   │
│   ├── agent_executor/                       # Executor agent resources
│   │   ├── agent_executor.md                 # Executor behavior specification
│   │   ├── handlers/                         # Action handlers by type
│   │   └── rollback/                         # Rollback mechanisms
│   │
│   ├── agent_protocol/                       # Inter-agent communication
│   │   ├── README.md                         # Protocol documentation
│   │   ├── schemas/                          # Protocol message schemas
│   │   │   ├── task_envelope.schema.json     # Task message format
│   │   │   ├── task_plan.schema.json         # Task plan format
│   │   │   ├── execution_report.schema.json  # Report format
│   │   │   └── system_config.schema.yaml     # Config format
│   │   ├── message_queue/                    # Message storage
│   │   │   ├── pending/                      # Tasks awaiting execution
│   │   │   ├── in_progress/                  # Currently executing
│   │   │   └── completed/                    # Finished tasks
│   │   └── validators/                       # Validation scripts
│   │
│   ├── agent_outputs/                        # Generated artifacts
│   │   ├── plans/                            # Inspector-generated plans
│   │   ├── reports/                          # Executor-generated reports
│   │   └── archive/                          # Historical records
│   │
│   └── agent_logs/                           # Agent activity logs
│       ├── inspector/                        # Inspector logs
│       ├── executor/                         # Executor logs
│       ├── protocol/                         # Protocol logs
│       └── audit/                            # Audit trail logs
│
├── agent_tools/                              # EXTERNAL: Agent utilities (human-triggered)
│   ├── analyze_dependencies.py               # Existing: dependency analysis
│   ├── treemap.py                            # Existing: structure generation
│   ├── validate_message.py                   # Inter-agent message validator
│   ├── simulate_execution.py                 # Dry-run execution simulator
│   ├── generate_rollback.py                  # Rollback checkpoint creator
│   ├── schema_validator.py                   # JSON/YAML schema validator
│   └── audit_logger.py                       # Audit trail generator
│
└── [existing directories preserved]          # All other project folders
    ├── bd/
    ├── nomina/
    ├── pdt/
    ├── ui/
    ├── utils/
    ├── config/
    ├── esquemas/
    ├── queries/
    └── orquestadores/
```

### 3.2 Folder Purposes

| Folder | Purpose | Access Level |
|--------|---------|--------------|
| `agent/` | Centralized agent system container | Read-only (context files) |
| `agent/agent_inspector/` | Inspector agent definition and resources | Read-only (for inspector) |
| `agent/agent_executor/` | Executor agent definition and action handlers | Read-only (for executor) |
| `agent/agent_protocol/` | Inter-agent communication schemas and queue | Read/Write (protocol layer) |
| `agent/agent_outputs/` | Generated plans, reports, and manifests | Write (agents), Read (all) |
| `agent/agent_logs/` | Activity and audit logs | Append-only |
| `agent_tools/` | Helper scripts for validation, simulation, rollback | Execute-only (human-triggered) |

> **Note**: `agent_tools/` remains at the project root level to maintain separation between agent definitions (inside `agent/`) and human-executable utility scripts.

---

## 4. Agent Specifications

### 4.1 Agent Inspector

#### 4.1.1 Identity

```yaml
name: agent_inspector
version: 1.0.0
role: System Analyst and Planner
trust_level: READ_ONLY
```

#### 4.1.2 Responsibilities

| Responsibility | Description |
|----------------|-------------|
| System Analysis | Analyze codebase structure, dependencies, and patterns |
| Planning | Generate detailed task decomposition and action plans |
| Behavioral Preservation | Ensure proposed changes maintain system invariants |
| Risk Assessment | Identify potential risks and mitigation strategies |
| Decision Generation | Produce structured decisions for executor consumption |

#### 4.1.3 Context Files (Inputs)

| File | Purpose | Required |
|------|---------|----------|
| `agent_inspector.md` | Inspector behavior rules | Yes |
| `agent/treemap.md` | Project structure map | Yes |
| `agent/dependencies_report.md` | Dependency analysis | Yes |
| `esquemas/*.json` | Data validation schemas | As needed |
| `queries/*.sql` | SQL transformations | As needed |
| `orquestadores/*.yaml` | Pipeline definitions | As needed |

#### 4.1.4 Outputs

**Primary Output: `task_plan.json`**
```json
{
  "plan_id": "uuid",
  "version": "1.0.0",
  "created_at": "ISO-8601",
  "task_summary": "string",
  "decisions": [],
  "action_plan": [],
  "task_decomposition": [],
  "execution_instructions": [],
  "risk_assessment": {},
  "estimated_scope": {}
}
```

**Secondary Output: `system_config.yaml`**
```yaml
system_definitions:
  target_components: []
  affected_modules: []

workflow_configuration:
  execution_order: sequential | parallel
  requires_approval: boolean

execution_constraints:
  max_files_modified: number
  allowed_operations: []
  forbidden_patterns: []

tool_selection_policies:
  preferred_tools: []
  fallback_tools: []
```

#### 4.1.5 Constraints

| Constraint | Enforcement |
|------------|-------------|
| **NEVER** modify project files directly | Hard constraint |
| **NEVER** execute code or commands | Hard constraint |
| **MUST** produce structured outputs only | Validated by protocol |
| **MUST** assess risks for destructive operations | Required in output |
| **MUST** preserve behavioral invariants | Documented in plan |

### 4.2 Agent Executor

#### 4.2.1 Identity

```yaml
name: agent_executor
version: 1.0.0
role: Safe Action Implementer
trust_level: WRITE_CONTROLLED
```

#### 4.2.2 Responsibilities

| Responsibility | Description |
|----------------|-------------|
| Action Execution | Perform only actions defined in inspector plans |
| Safe Implementation | Implement changes with minimal footprint |
| Reversibility | Generate rollback checkpoints before modifications |
| Status Reporting | Produce detailed execution and error reports |
| Scope Enforcement | Operate only on explicitly listed files |

#### 4.2.3 Inputs

| Input | Source | Required |
|-------|--------|----------|
| `task_plan.json` | Inspector output | Yes |
| `system_config.yaml` | Inspector output | Yes |
| Protocol validation result | Protocol layer | Yes |

#### 4.2.4 Outputs

**Execution Report: `execution_report.json`**
```json
{
  "report_id": "uuid",
  "plan_id": "uuid (reference)",
  "status": "SUCCESS | PARTIAL | FAILED | ROLLED_BACK",
  "started_at": "ISO-8601",
  "completed_at": "ISO-8601",
  "actions_completed": [],
  "actions_failed": [],
  "errors": [],
  "rollback_available": boolean
}
```

**Change Log: `change_log.json`**
```json
{
  "log_id": "uuid",
  "plan_id": "uuid (reference)",
  "changes": [
    {
      "change_id": "uuid",
      "file_path": "string",
      "operation": "CREATE | MODIFY | DELETE | RENAME",
      "before_hash": "sha256 (null if CREATE)",
      "after_hash": "sha256 (null if DELETE)",
      "diff_preview": "string (first 500 chars)",
      "timestamp": "ISO-8601"
    }
  ]
}
```

**Rollback Manifest: `rollback_manifest.json`**
```json
{
  "manifest_id": "uuid",
  "plan_id": "uuid (reference)",
  "created_at": "ISO-8601",
  "checkpoints": [
    {
      "checkpoint_id": "uuid",
      "file_path": "string",
      "backup_path": "string",
      "original_hash": "sha256",
      "operation_to_reverse": "string"
    }
  ],
  "rollback_script": "string (path to generated script)"
}
```

#### 4.2.5 Constraints

| Constraint | Enforcement |
|------------|-------------|
| **ONLY** execute actions from validated plans | Protocol validation |
| **ONLY** modify files listed in plan | File path whitelist |
| **MUST** create rollback checkpoints | Pre-execution hook |
| **MUST** report all failures immediately | Error handler |
| **MUST NOT** interpret or extend instructions | Literal execution only |

---

## 5. Communication Protocol

### 5.1 Message Format

All inter-agent communication uses the **Task Envelope** format:

```json
{
  "$schema": "agent_protocol/schemas/task_envelope.schema.json",
  "envelope": {
    "envelope_id": "uuid-v4",
    "version": "1.0.0",
    "created_at": "2026-02-03T10:30:00Z",
    "source_agent": "agent_inspector",
    "target_agent": "agent_executor",
    "message_type": "TASK_REQUEST | TASK_RESPONSE | STATUS_UPDATE | ERROR",
    "priority": "LOW | NORMAL | HIGH | CRITICAL",
    "requires_ack": true,
    "ttl_seconds": 3600
  },
  "metadata": {
    "correlation_id": "uuid (links related messages)",
    "parent_envelope_id": "uuid | null",
    "sequence_number": 1,
    "retry_count": 0,
    "max_retries": 3,
    "tags": ["etl", "bd", "schema-update"]
  },
  "payload": {
    "task_id": "uuid",
    "task_type": "FILE_MODIFY | SCHEMA_UPDATE | SQL_EXECUTE | PIPELINE_RUN",
    "content": {}
  },
  "validation": {
    "checksum": "sha256 of payload",
    "signature": "optional digital signature"
  }
}
```

### 5.2 Task Lifecycle States

```
┌─────────────────────────────────────────────────────────────────┐
│                      TASK STATE MACHINE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐                                               │
│   │   CREATED    │ ─── Inspector generates task plan             │
│   └──────┬───────┘                                               │
│          │                                                       │
│          ▼                                                       │
│   ┌──────────────┐                                               │
│   │  VALIDATED   │ ─── Protocol validates envelope               │
│   └──────┬───────┘                                               │
│          │                                                       │
│          ├─────────────────────┐                                 │
│          │                     │ (if requires_approval)          │
│          ▼                     ▼                                 │
│   ┌──────────────┐      ┌──────────────┐                         │
│   │   QUEUED     │      │  PENDING_    │                         │
│   │              │      │  APPROVAL    │                         │
│   └──────┬───────┘      └──────┬───────┘                         │
│          │                     │                                 │
│          │                     │ (approved)                      │
│          ▼                     ▼                                 │
│   ┌──────────────────────────────┐                               │
│   │        IN_PROGRESS           │ ─── Executor processing       │
│   └──────────────┬───────────────┘                               │
│                  │                                               │
│        ┌─────────┼─────────┐                                     │
│        │         │         │                                     │
│        ▼         ▼         ▼                                     │
│  ┌──────────┐ ┌────────┐ ┌──────────┐                            │
│  │ COMPLETED│ │ FAILED │ │ ROLLED_  │                            │
│  │          │ │        │ │ BACK     │                            │
│  └────┬─────┘ └────┬───┘ └────┬─────┘                            │
│       │            │          │                                  │
│       └────────────┴──────────┘                                  │
│                    │                                             │
│                    ▼                                             │
│             ┌──────────────┐                                     │
│             │   ARCHIVED   │                                     │
│             └──────────────┘                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Error Handling Strategy

| Error Type | Handling Strategy | Retry Policy |
|------------|-------------------|--------------|
| **Validation Error** | Reject immediately, return to inspector | No retry |
| **Transient Error** | Retry with exponential backoff | Up to 3 retries |
| **File Access Error** | Log, attempt rollback, fail task | 1 retry |
| **Schema Violation** | Reject, preserve original state | No retry |
| **Timeout** | Cancel, rollback if partial | No retry |
| **Unknown Error** | Log full context, escalate to human | No retry |

### 5.4 Acknowledgment Protocol

```
Inspector                    Protocol                     Executor
    │                           │                            │
    │──── TaskEnvelope ────────►│                            │
    │                           │── Validate ──►             │
    │◄─── ACK_RECEIVED ─────────│                            │
    │                           │                            │
    │                           │──── TaskEnvelope ─────────►│
    │                           │◄─── ACK_RECEIVED ──────────│
    │                           │                            │
    │                           │                     (execute)
    │                           │                            │
    │                           │◄─── STATUS_UPDATE ─────────│
    │◄─── STATUS_UPDATE ────────│                            │
    │                           │                            │
    │                           │◄─── EXECUTION_REPORT ──────│
    │◄─── EXECUTION_REPORT ─────│                            │
    │                           │                            │
```

---

## 6. Schema Definitions

### 6.1 Task Plan Schema (JSON)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "task_plan.schema.json",
  "title": "Task Plan",
  "description": "Schema for inspector-generated task plans",
  "type": "object",
  "required": ["plan_id", "version", "created_at", "task_summary", "action_plan"],
  "properties": {
    "plan_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique identifier for this plan"
    },
    "version": {
      "type": "string",
      "pattern": "^\\d+\\.\\d+\\.\\d+$",
      "description": "Semantic version of the plan format"
    },
    "created_at": {
      "type": "string",
      "format": "date-time",
      "description": "ISO-8601 timestamp of plan creation"
    },
    "task_summary": {
      "type": "string",
      "minLength": 10,
      "maxLength": 500,
      "description": "Human-readable summary of the task"
    },
    "decisions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["decision_id", "description", "rationale"],
        "properties": {
          "decision_id": { "type": "string" },
          "description": { "type": "string" },
          "rationale": { "type": "string" },
          "alternatives_considered": {
            "type": "array",
            "items": { "type": "string" }
          }
        }
      }
    },
    "action_plan": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["action_id", "action_type", "target", "operation"],
        "properties": {
          "action_id": { "type": "string" },
          "action_type": {
            "type": "string",
            "enum": ["FILE_CREATE", "FILE_MODIFY", "FILE_DELETE", "FILE_RENAME", "SCHEMA_UPDATE", "SQL_EXECUTE", "PIPELINE_RUN"]
          },
          "target": { "type": "string" },
          "operation": { "type": "object" },
          "depends_on": {
            "type": "array",
            "items": { "type": "string" }
          },
          "reversible": { "type": "boolean" },
          "risk_level": {
            "type": "string",
            "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
          }
        }
      }
    },
    "task_decomposition": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "subtask_id": { "type": "string" },
          "description": { "type": "string" },
          "actions": {
            "type": "array",
            "items": { "type": "string" }
          }
        }
      }
    },
    "execution_instructions": {
      "type": "object",
      "properties": {
        "execution_order": {
          "type": "string",
          "enum": ["sequential", "parallel", "dependency_based"]
        },
        "stop_on_error": { "type": "boolean" },
        "rollback_on_failure": { "type": "boolean" },
        "human_approval_required": { "type": "boolean" }
      }
    },
    "risk_assessment": {
      "type": "object",
      "properties": {
        "overall_risk": {
          "type": "string",
          "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        },
        "risks_identified": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "risk_id": { "type": "string" },
              "description": { "type": "string" },
              "probability": { "type": "string" },
              "impact": { "type": "string" },
              "mitigation": { "type": "string" }
            }
          }
        }
      }
    }
  }
}
```

### 6.2 System Configuration Schema (YAML)

```yaml
# system_config.schema.yaml
$schema: "https://json-schema.org/draft/2020-12/schema"
$id: "system_config.schema.yaml"
title: "System Configuration"
description: "Schema for inspector-generated system configuration"
type: object
required:
  - config_id
  - version
  - system_definitions
  - workflow_configuration

properties:
  config_id:
    type: string
    format: uuid

  version:
    type: string
    pattern: "^\\d+\\.\\d+\\.\\d+$"

  system_definitions:
    type: object
    required:
      - target_components
    properties:
      target_components:
        type: array
        items:
          type: object
          properties:
            component_id:
              type: string
            component_type:
              type: string
              enum: [etl_module, ui_component, utility, schema, query]
            file_paths:
              type: array
              items:
                type: string

      affected_modules:
        type: array
        items:
          type: string

      dependencies:
        type: array
        items:
          type: object
          properties:
            from:
              type: string
            to:
              type: string
            type:
              type: string
              enum: [imports, uses, configures]

  workflow_configuration:
    type: object
    properties:
      execution_mode:
        type: string
        enum: [sequential, parallel, mixed]
        default: sequential

      requires_approval:
        type: boolean
        default: false

      approval_threshold:
        type: string
        enum: [LOW, MEDIUM, HIGH, CRITICAL]
        default: HIGH

      timeout_seconds:
        type: integer
        minimum: 60
        maximum: 3600
        default: 300

  execution_constraints:
    type: object
    properties:
      max_files_modified:
        type: integer
        minimum: 1
        maximum: 100
        default: 10

      allowed_operations:
        type: array
        items:
          type: string
          enum: [CREATE, MODIFY, DELETE, RENAME]
        default: [MODIFY]

      forbidden_patterns:
        type: array
        items:
          type: string
        description: "Glob patterns for files that must not be modified"
        default:
          - "**/.git/**"
          - "**/node_modules/**"
          - "**/__pycache__/**"

      protected_files:
        type: array
        items:
          type: string
        description: "Specific files that require elevated approval"
        default:
          - "agent/agent_rules.md"
          - ".pre-commit-config.yaml"
          - "requirements.txt"

  tool_selection_policies:
    type: object
    properties:
      preferred_tools:
        type: array
        items:
          type: object
          properties:
            operation:
              type: string
            tool:
              type: string
            reason:
              type: string

      fallback_tools:
        type: array
        items:
          type: object
          properties:
            primary:
              type: string
            fallback:
              type: string
            condition:
              type: string
```

### 6.3 Task Envelope Schema (JSON)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "task_envelope.schema.json",
  "title": "Task Envelope",
  "description": "Inter-agent communication message format",
  "type": "object",
  "required": ["envelope", "metadata", "payload"],
  "properties": {
    "envelope": {
      "type": "object",
      "required": ["envelope_id", "version", "created_at", "source_agent", "target_agent", "message_type"],
      "properties": {
        "envelope_id": {
          "type": "string",
          "format": "uuid"
        },
        "version": {
          "type": "string",
          "const": "1.0.0"
        },
        "created_at": {
          "type": "string",
          "format": "date-time"
        },
        "source_agent": {
          "type": "string",
          "enum": ["agent_inspector", "agent_executor", "agent_protocol"]
        },
        "target_agent": {
          "type": "string",
          "enum": ["agent_inspector", "agent_executor", "agent_protocol"]
        },
        "message_type": {
          "type": "string",
          "enum": ["TASK_REQUEST", "TASK_RESPONSE", "STATUS_UPDATE", "ERROR", "ACK"]
        },
        "priority": {
          "type": "string",
          "enum": ["LOW", "NORMAL", "HIGH", "CRITICAL"],
          "default": "NORMAL"
        },
        "requires_ack": {
          "type": "boolean",
          "default": true
        },
        "ttl_seconds": {
          "type": "integer",
          "minimum": 60,
          "maximum": 86400,
          "default": 3600
        }
      }
    },
    "metadata": {
      "type": "object",
      "required": ["correlation_id", "sequence_number"],
      "properties": {
        "correlation_id": {
          "type": "string",
          "format": "uuid"
        },
        "parent_envelope_id": {
          "type": ["string", "null"],
          "format": "uuid"
        },
        "sequence_number": {
          "type": "integer",
          "minimum": 1
        },
        "retry_count": {
          "type": "integer",
          "minimum": 0,
          "default": 0
        },
        "max_retries": {
          "type": "integer",
          "minimum": 0,
          "maximum": 10,
          "default": 3
        },
        "tags": {
          "type": "array",
          "items": { "type": "string" }
        }
      }
    },
    "payload": {
      "type": "object",
      "required": ["task_id", "task_type", "content"],
      "properties": {
        "task_id": {
          "type": "string",
          "format": "uuid"
        },
        "task_type": {
          "type": "string",
          "enum": ["FILE_MODIFY", "SCHEMA_UPDATE", "SQL_EXECUTE", "PIPELINE_RUN", "ROLLBACK", "STATUS_QUERY"]
        },
        "content": {
          "type": "object",
          "description": "Type-specific content based on task_type"
        }
      }
    },
    "validation": {
      "type": "object",
      "properties": {
        "checksum": {
          "type": "string",
          "pattern": "^[a-f0-9]{64}$",
          "description": "SHA-256 hash of payload"
        },
        "signature": {
          "type": ["string", "null"],
          "description": "Optional digital signature"
        }
      }
    }
  }
}
```

---

## 7. Agent Tools

### 7.1 Proposed Tools

| Tool | Purpose | Location |
|------|---------|----------|
| `validate_message.py` | Validate inter-agent messages against schemas | `agent_tools/` |
| `simulate_execution.py` | Dry-run execution plans without making changes | `agent_tools/` |
| `generate_rollback.py` | Create rollback checkpoints before execution | `agent_tools/` |
| `schema_validator.py` | Validate JSON/YAML against defined schemas | `agent_tools/` |
| `audit_logger.py` | Generate and manage audit trails | `agent_tools/` |
| `treemap_generator.py` | Enhanced treemap generation (rename from existing) | `agent_tools/` |
| `dependency_analyzer.py` | Enhanced dependency analysis (existing) | `agent_tools/` |

### 7.2 Tool Specifications

#### 7.2.1 validate_message.py

```python
"""
Inter-agent message validator.

Usage:
    python validate_message.py <message_file> [--schema <schema_file>]

Returns:
    Exit code 0 if valid, 1 if invalid
    Outputs validation report to stdout
"""
```

**Features:**
- Schema validation against JSON Schema
- Checksum verification
- Required field checking
- Enum value validation
- Cross-reference validation (e.g., action_id references)

#### 7.2.2 simulate_execution.py

```python
"""
Dry-run execution simulator.

Usage:
    python simulate_execution.py <task_plan.json> [--verbose]

Returns:
    Simulation report showing what would be changed
    No actual modifications are made
"""
```

**Features:**
- Parse task plan
- Identify all files that would be affected
- Check file existence and permissions
- Validate operation feasibility
- Generate diff previews
- Report potential conflicts

#### 7.2.3 generate_rollback.py

```python
"""
Rollback checkpoint generator.

Usage:
    python generate_rollback.py <task_plan.json> --output <checkpoint_dir>

Returns:
    Rollback manifest and file backups
"""
```

**Features:**
- Create timestamped checkpoint directory
- Backup all files to be modified
- Generate rollback manifest
- Create rollback script
- Calculate file hashes

#### 7.2.4 schema_validator.py

```python
"""
JSON/YAML schema validator.

Usage:
    python schema_validator.py <file> --schema <schema_file>
    python schema_validator.py <file> --type <task_plan|system_config|envelope>

Returns:
    Validation result with detailed error messages
"""
```

**Features:**
- Support both JSON and YAML inputs
- Built-in schema types
- Custom schema support
- Detailed error reporting with line numbers
- Suggested fixes for common errors

#### 7.2.5 audit_logger.py

```python
"""
Audit trail generator.

Usage:
    python audit_logger.py log <event_type> <event_data>
    python audit_logger.py query [--from <date>] [--to <date>] [--type <type>]
    python audit_logger.py export [--format json|csv]

Returns:
    Audit log entries
"""
```

**Features:**
- Structured event logging
- Query interface
- Export capabilities
- Log rotation
- Tamper-evident logging (hash chain)

### 7.3 Pre-commit Hook Updates

Update `.pre-commit-config.yaml` to include new validations:

```yaml
repos:
  - repo: local
    hooks:
      # Existing hooks
      - id: update-treemap
        name: Update Treemap
        entry: python agent_tools/treemap_generator.py
        language: python
        always_run: true
        pass_filenames: false

      - id: update-dependencies
        name: Update Dependencies Report
        entry: python agent_tools/dependency_analyzer.py
        language: python
        always_run: true
        pass_filenames: false

      # New hooks
      - id: validate-agent-schemas
        name: Validate Agent Schemas
        entry: python agent_tools/schema_validator.py
        language: python
        files: ^agent_(inspector|executor|protocol)/.*\.(json|yaml)$

      - id: validate-task-plans
        name: Validate Task Plans
        entry: python agent_tools/validate_message.py
        language: python
        files: ^agent_outputs/plans/.*\.json$
```

---

## 8. Migration Strategy

### 8.1 Migration Phases

```
┌─────────────────────────────────────────────────────────────────┐
│                    MIGRATION TIMELINE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PHASE 1: FOUNDATION (Week 1)                                    │
│  ├─ Create new folder structure                                  │
│  ├─ Implement agent_inspector.md                                 │
│  ├─ Implement agent_executor.md                                  │
│  └─ Set up protocol schemas                                      │
│                                                                  │
│  PHASE 2: TOOLING (Week 2)                                       │
│  ├─ Implement validation tools                                   │
│  ├─ Implement simulation tool                                    │
│  ├─ Implement rollback mechanism                                 │
│  └─ Update pre-commit hooks                                      │
│                                                                  │
│  PHASE 3: PARALLEL OPERATION (Week 3)                            │
│  ├─ Run both old and new systems in parallel                     │
│  ├─ Validate output equivalence                                  │
│  ├─ Monitor for discrepancies                                    │
│  └─ Document any behavioral differences                          │
│                                                                  │
│  PHASE 4: CUTOVER (Week 4)                                       │
│  ├─ Deprecate old agent_rules.md                                 │
│  ├─ Full switchover to two-agent system                          │
│  ├─ Remove parallel validation                                   │
│  └─ Update documentation                                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 Phase 1: Foundation

**Tasks:**

1. **Create folder structure**
   ```bash
   mkdir -p agent_inspector/context/analysis_templates
   mkdir -p agent_inspector/context/risk_matrices
   mkdir -p agent_inspector/schemas
   mkdir -p agent_executor/handlers
   mkdir -p agent_executor/rollback
   mkdir -p agent_protocol/schemas
   mkdir -p agent_protocol/message_queue/{pending,in_progress,completed}
   mkdir -p agent_protocol/validators
   mkdir -p agent_outputs/{plans,reports,archive}
   mkdir -p agent_logs/{inspector,executor,protocol}
   ```

2. **Create agent specification files**
   - `agent_inspector/agent_inspector.md`
   - `agent_executor/agent_executor.md`
   - `agent_protocol/README.md`

3. **Deploy schema files**
   - Copy schemas from Section 6 to respective locations

### 8.3 Phase 2: Tooling

**Tasks:**

1. **Implement core tools**
   - `validate_message.py`
   - `simulate_execution.py`
   - `generate_rollback.py`
   - `schema_validator.py`
   - `audit_logger.py`

2. **Update existing tools**
   - Rename/enhance `analyze_dependencies.py`
   - Add treemap generator if not present

3. **Update `.pre-commit-config.yaml`**
   - Add new validation hooks
   - Test hook execution

### 8.4 Phase 3: Parallel Operation

**Tasks:**

1. **Configure dual-mode operation**
   - Old system continues normal operation
   - New system runs in shadow mode

2. **Create comparison framework**
   - Compare outputs of both systems
   - Log discrepancies

3. **Validation criteria**
   - Same files modified
   - Same content changes
   - No additional side effects

### 8.5 Phase 4: Cutover

**Tasks:**

1. **Deprecation**
   - Add deprecation notice to `agent/agent_rules.md`
   - Point to new agent specifications

2. **Switchover**
   - Enable new system as primary
   - Disable old system

3. **Cleanup**
   - Remove parallel validation code
   - Update all documentation references

### 8.6 Rollback Plan

If migration fails at any phase:

| Phase | Rollback Action |
|-------|-----------------|
| Phase 1 | Delete new folders, no impact on existing system |
| Phase 2 | Disable new pre-commit hooks, tools are standalone |
| Phase 3 | Disable shadow mode, old system continues |
| Phase 4 | Re-enable old agent_rules.md, restore pre-commit hooks |

---

## 9. Risk Analysis

### 9.1 Risk Matrix

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Behavioral deviation** | Medium | High | Parallel operation phase with comparison |
| **Tool failures** | Low | Medium | Fallback to manual operations |
| **Schema incompatibility** | Low | Medium | Versioned schemas with migration support |
| **Performance degradation** | Low | Low | Profiling during parallel operation |
| **Learning curve** | Medium | Low | Documentation and examples |
| **Partial migration** | Medium | Medium | Phase gates with go/no-go criteria |

### 9.2 Critical Success Factors

1. **Complete schema coverage**: All message types must have validated schemas
2. **Comprehensive testing**: All tools must have test coverage
3. **Documentation quality**: Clear, complete documentation for both agents
4. **Rollback capability**: Verified rollback mechanism at each phase
5. **Stakeholder alignment**: All users understand the new workflow

### 9.3 Monitoring Requirements

| Metric | Threshold | Action |
|--------|-----------|--------|
| Validation failures | > 5% | Review schema definitions |
| Execution failures | > 1% | Investigate root cause |
| Rollback frequency | > 10% | Review planning quality |
| Message latency | > 5s | Optimize protocol layer |

---

## 10. Usage Instructions

This section provides detailed instructions on how to use each agent and the supporting tools.

### 10.1 Agent Inspector Usage

#### 10.1.1 Purpose

The Agent Inspector is the **planning and analysis** component. Use it when you need to:
- Analyze the impact of a proposed change
- Generate a structured execution plan
- Assess risks before making modifications
- Document decisions with rationale

#### 10.1.2 When to Use

| Scenario | Use Inspector? | Reason |
|----------|----------------|--------|
| Adding new feature | Yes | Plan impact analysis |
| Modifying schema | Yes | Risk assessment needed |
| Refactoring code | Yes | Dependency analysis |
| Simple typo fix | Optional | Low risk, simple change |
| Emergency hotfix | No | Direct execution acceptable |

#### 10.1.3 How to Invoke

**Step 1: Prepare the request**
```
Provide the Inspector with:
- Clear description of the desired change
- Context about affected components
- Any constraints or requirements
```

**Step 2: Inspector analyzes and generates plan**
```
The Inspector will:
1. Read treemap.md and dependencies_report.md
2. Analyze affected files and modules
3. Assess risks and dependencies
4. Generate task_plan.json and system_config.yaml
```

**Step 3: Review outputs**
```
Location: agent/agent_outputs/plans/{timestamp}_{task_id}/
Files:
  - task_plan.json      → Detailed action plan
  - system_config.yaml  → Execution configuration
```

#### 10.1.4 Example Workflow

```
USER REQUEST:
"Add a new validation field 'fecha_ingreso' to esquema_bd.json"

INSPECTOR ACTIONS:
1. Reads agent/treemap.md for project structure
2. Reads agent/dependencies_report.md for impact analysis
3. Examines esquemas/esquema_bd.json current structure
4. Identifies: bd/step2_capagold.py uses this schema
5. Assesses risk: LOW (additive change, optional field)

INSPECTOR OUTPUT:
agent/agent_outputs/plans/20260203_abc123/
├── task_plan.json
└── system_config.yaml
```

#### 10.1.5 Inspector Constraints

| What Inspector CAN Do | What Inspector CANNOT Do |
|----------------------|-------------------------|
| Read any file | Modify any file |
| Analyze dependencies | Execute code |
| Generate plans | Run scripts |
| Assess risks | Make network calls |
| Document decisions | Approve its own plans |

---

### 10.2 Agent Executor Usage

#### 10.2.1 Purpose

The Agent Executor is the **implementation** component. Use it when you need to:
- Execute a validated plan from the Inspector
- Make safe, reversible changes
- Generate rollback checkpoints
- Report execution status

#### 10.2.2 When to Use

| Scenario | Use Executor? | Prerequisite |
|----------|---------------|--------------|
| After Inspector plan | Yes | Validated plan required |
| Direct simple change | Optional | Low risk only |
| Rollback needed | Yes | Rollback manifest exists |
| Emergency fix | Yes (direct mode) | Human approval |

#### 10.2.3 How to Invoke

**Step 1: Ensure plan exists**
```
Verify: agent/agent_outputs/plans/{task_id}/task_plan.json
```

**Step 2: Validate plan (optional but recommended)**
```bash
python agent_tools/validate_message.py task_plan.json --type plan
```

**Step 3: Simulate execution (recommended)**
```bash
python agent_tools/simulate_execution.py task_plan.json --verbose
```

**Step 4: Generate rollback checkpoint**
```bash
python agent_tools/generate_rollback.py task_plan.json --output agent/agent_outputs/reports/{task_id}/
```

**Step 5: Execute**
```
Provide the Executor with the task_plan.json
The Executor will:
1. Create rollback checkpoint
2. Execute each action in order
3. Validate results
4. Generate execution report
```

#### 10.2.4 Example Workflow

```
EXECUTOR INPUT:
agent/agent_outputs/plans/20260203_abc123/task_plan.json

EXECUTOR ACTIONS:
1. Reads and validates task_plan.json
2. Creates backup of esquemas/esquema_bd.json
3. Applies modification (adds 'fecha_ingreso' field)
4. Validates schema syntax
5. Generates execution report

EXECUTOR OUTPUT:
agent/agent_outputs/reports/20260203_abc123/
├── execution_report.json   → Status: SUCCESS
├── change_log.json         → Detailed changes
└── rollback_manifest.json  → Rollback info
```

#### 10.2.5 Executor Constraints

| What Executor CAN Do | What Executor CANNOT Do |
|---------------------|------------------------|
| Modify listed files | Modify unlisted files |
| Create backups | Skip rollback creation |
| Execute plan actions | Interpret or extend plan |
| Generate reports | Ignore errors |
| Perform rollback | Delete rollback manifests |

---

### 10.3 Communication Protocol Usage

#### 10.3.1 Message Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    STANDARD MESSAGE FLOW                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. USER → INSPECTOR                                              │
│     "I need to modify the BD schema to add a new field"           │
│                                                                   │
│  2. INSPECTOR → PROTOCOL (task_envelope.json)                     │
│     Generates plan, wraps in task envelope                        │
│                                                                   │
│  3. PROTOCOL validates envelope                                   │
│     Checks schema compliance, checksums                           │
│                                                                   │
│  4. PROTOCOL → EXECUTOR (validated envelope)                      │
│     Forwards to executor for implementation                       │
│                                                                   │
│  5. EXECUTOR → PROTOCOL (execution_report.json)                   │
│     Reports completion status                                     │
│                                                                   │
│  6. PROTOCOL → USER                                               │
│     Final status notification                                     │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

#### 10.3.2 Validating Messages

```bash
# Validate a task envelope
python agent_tools/validate_message.py envelope.json --type envelope

# Validate a task plan
python agent_tools/validate_message.py task_plan.json --type plan

# Validate with verbose output
python agent_tools/validate_message.py message.json --type envelope --verbose
```

---

### 10.4 Agent Tools Usage

#### 10.4.1 validate_message.py

**Purpose**: Validate inter-agent messages against schemas

```bash
# Basic usage
python agent_tools/validate_message.py <file> --type <envelope|plan|report|config>

# Examples
python agent_tools/validate_message.py task.json --type plan
python agent_tools/validate_message.py message.json --type envelope --verbose
python agent_tools/validate_message.py config.yaml --schema custom.schema.json
```

**Output**: Validation report with errors/warnings

---

#### 10.4.2 simulate_execution.py

**Purpose**: Preview execution without making changes

```bash
# Basic usage
python agent_tools/simulate_execution.py <task_plan.json> [--verbose]

# Examples
python agent_tools/simulate_execution.py task_plan.json
python agent_tools/simulate_execution.py task_plan.json --verbose
python agent_tools/simulate_execution.py task_plan.json --output report.json
```

**Output**: Simulation report showing what would change

---

#### 10.4.3 generate_rollback.py

**Purpose**: Create rollback checkpoints before execution

```bash
# Basic usage
python agent_tools/generate_rollback.py <task_plan.json> --output <dir>

# Examples
python agent_tools/generate_rollback.py task_plan.json --output backups/
python agent_tools/generate_rollback.py task_plan.json --output backups/ --dry-run
python agent_tools/generate_rollback.py task_plan.json --output backups/ --generate-script
```

**Output**: Rollback manifest and file backups

---

#### 10.4.4 schema_validator.py

**Purpose**: Validate JSON/YAML files against schemas

```bash
# Basic usage
python agent_tools/schema_validator.py <file> --type <type>

# Examples
python agent_tools/schema_validator.py plan.json --type task_plan
python agent_tools/schema_validator.py config.yaml --type system_config
python agent_tools/schema_validator.py data.json --schema custom.schema.json
python agent_tools/schema_validator.py file.json --syntax-only
```

**Output**: Validation result with detailed error messages

---

#### 10.4.5 audit_logger.py

**Purpose**: Generate and manage audit trails

```bash
# Log an event
python agent_tools/audit_logger.py log <event_type> <data>

# Query logs
python agent_tools/audit_logger.py query [--from DATE] [--to DATE] [--type TYPE]

# Export logs
python agent_tools/audit_logger.py export [--format json|csv] [--output FILE]

# Examples
python agent_tools/audit_logger.py log PLAN_CREATED '{"plan_id": "abc123"}'
python agent_tools/audit_logger.py query --type EXECUTION_COMPLETED --from 2026-01-01
python agent_tools/audit_logger.py export --format csv --output audit.csv
```

**Output**: Audit log entries with hash-chain integrity

---

### 10.5 Complete Workflow Example

Here is a complete example of using the two-agent system:

```
┌─────────────────────────────────────────────────────────────────────┐
│              COMPLETE WORKFLOW: Adding Schema Field                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  STEP 1: USER REQUEST                                                │
│  ─────────────────────                                               │
│  "Add 'fecha_ingreso' field to esquema_bd.json"                      │
│                                                                      │
│  STEP 2: INVOKE INSPECTOR                                            │
│  ─────────────────────────                                           │
│  Inspector reads:                                                    │
│    - agent/treemap.md                                                │
│    - agent/dependencies_report.md                                    │
│    - esquemas/esquema_bd.json                                        │
│                                                                      │
│  Inspector generates:                                                │
│    - agent/agent_outputs/plans/20260203_task001/task_plan.json       │
│    - agent/agent_outputs/plans/20260203_task001/system_config.yaml   │
│                                                                      │
│  STEP 3: VALIDATE PLAN                                               │
│  ─────────────────────                                               │
│  $ python agent_tools/validate_message.py \                          │
│      agent/agent_outputs/plans/20260203_task001/task_plan.json \     │
│      --type plan --verbose                                           │
│                                                                      │
│  Output: "Validation Report: VALID"                                  │
│                                                                      │
│  STEP 4: SIMULATE EXECUTION                                          │
│  ──────────────────────────                                          │
│  $ python agent_tools/simulate_execution.py \                        │
│      agent/agent_outputs/plans/20260203_task001/task_plan.json       │
│                                                                      │
│  Output:                                                             │
│    "Simulation Report: SUCCESS"                                      │
│    "Files Affected: 1"                                               │
│    "  - esquemas/esquema_bd.json"                                    │
│                                                                      │
│  STEP 5: GENERATE ROLLBACK                                           │
│  ─────────────────────────                                           │
│  $ python agent_tools/generate_rollback.py \                         │
│      agent/agent_outputs/plans/20260203_task001/task_plan.json \     │
│      --output agent/agent_outputs/reports/20260203_task001/          │
│                                                                      │
│  Output:                                                             │
│    "Rollback Manifest Generated"                                     │
│    "Files Backed Up: 1"                                              │
│                                                                      │
│  STEP 6: INVOKE EXECUTOR                                             │
│  ────────────────────────                                            │
│  Executor reads task_plan.json and executes:                         │
│    1. Verifies rollback checkpoint exists                            │
│    2. Modifies esquemas/esquema_bd.json                              │
│    3. Validates JSON syntax                                          │
│    4. Generates execution_report.json                                │
│                                                                      │
│  STEP 7: VERIFY RESULTS                                              │
│  ─────────────────────                                               │
│  Check: agent/agent_outputs/reports/20260203_task001/                │
│    - execution_report.json → Status: SUCCESS                         │
│    - change_log.json → Details of changes                            │
│    - rollback_manifest.json → How to undo                            │
│                                                                      │
│  STEP 8: LOG AUDIT EVENT                                             │
│  ───────────────────────                                             │
│  $ python agent_tools/audit_logger.py log EXECUTION_COMPLETED \      │
│      '{"task_id": "task001", "status": "SUCCESS"}'                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.6 Rollback Procedure

If something goes wrong, use this procedure to rollback:

```bash
# Step 1: Locate the rollback manifest
ls agent/agent_outputs/reports/{task_id}/

# Step 2: Review what will be restored
cat agent/agent_outputs/reports/{task_id}/rollback_manifest.json

# Step 3: Execute rollback (if script was generated)
bash agent/agent_outputs/reports/{task_id}/rollback.sh

# Or manually restore files from backups:
cp agent/agent_outputs/reports/{task_id}/backups/{file}.bak {original_path}
```

### 10.7 Quick Reference Card

| Task | Command/Action |
|------|----------------|
| **Analyze change** | Invoke Inspector with request |
| **Validate plan** | `python agent_tools/validate_message.py plan.json --type plan` |
| **Preview changes** | `python agent_tools/simulate_execution.py plan.json` |
| **Create backup** | `python agent_tools/generate_rollback.py plan.json --output dir/` |
| **Execute plan** | Invoke Executor with task_plan.json |
| **Check status** | Read `execution_report.json` |
| **Rollback** | Use `rollback_manifest.json` or `rollback.sh` |
| **View audit log** | `python agent_tools/audit_logger.py query` |

---

## Appendix A: File Templates

### A.1 agent_inspector.md Template

See `agent/agent_inspector/agent_inspector.md` for the complete specification.

### A.2 agent_executor.md Template

See `agent/agent_executor/agent_executor.md` for the complete specification.

### A.3 Example Task Plan

```json
{
  "plan_id": "550e8400-e29b-41d4-a716-446655440000",
  "version": "1.0.0",
  "created_at": "2026-02-03T10:30:00Z",
  "task_summary": "Add new column 'fecha_actualizacion' to esquema_bd.json",
  "decisions": [
    {
      "decision_id": "d001",
      "description": "Add column as optional to maintain backward compatibility",
      "rationale": "Existing data may not have this field populated"
    }
  ],
  "action_plan": [
    {
      "action_id": "a001",
      "action_type": "FILE_MODIFY",
      "target": "esquemas/esquema_bd.json",
      "operation": {
        "type": "json_patch",
        "patches": [
          {
            "op": "add",
            "path": "/properties/fecha_actualizacion",
            "value": {
              "type": "string",
              "format": "date-time",
              "description": "Last update timestamp"
            }
          }
        ]
      },
      "reversible": true,
      "risk_level": "LOW"
    }
  ],
  "execution_instructions": {
    "execution_order": "sequential",
    "stop_on_error": true,
    "rollback_on_failure": true,
    "human_approval_required": false
  },
  "risk_assessment": {
    "overall_risk": "LOW",
    "risks_identified": [
      {
        "risk_id": "r001",
        "description": "Schema validation may fail for existing data",
        "probability": "LOW",
        "impact": "LOW",
        "mitigation": "Field is optional, existing data unaffected"
      }
    ]
  }
}
```

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| **Agent** | Autonomous component with defined responsibilities |
| **Inspector** | Analysis and planning agent (read-only) |
| **Executor** | Implementation agent (controlled write access) |
| **Protocol** | Communication layer between agents |
| **Envelope** | Structured message container |
| **Task Plan** | Inspector output defining actions to take |
| **Rollback** | Reverting changes to a previous state |
| **Checkpoint** | Saved state before modifications |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | 2026-02-03 | Claude Agent | Initial proposal |
| 1.1.0 | 2026-02-03 | Claude Agent | Reorganized folder structure (all agent components inside `agent/`), added Section 10: Usage Instructions |

