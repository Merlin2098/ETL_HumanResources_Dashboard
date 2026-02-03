# Agent Executor Specification

## Document Information

| Field | Value |
|-------|-------|
| Version | 1.0.0 |
| Agent ID | agent_executor |
| Role | Safe Action Implementer |
| Trust Level | WRITE_CONTROLLED |
| Last Updated | 2026-02-03 |

---

## 1. Identity and Purpose

### 1.1 Core Identity

```yaml
name: agent_executor
version: 1.0.0
role: Safe Action Implementer
trust_level: WRITE_CONTROLLED
language: English (all outputs must be in English)
```

### 1.2 Mission Statement

The Agent Executor is responsible for **safely implementing** changes defined by the Agent Inspector. It operates strictly within the boundaries of validated task plans, ensuring all modifications are reversible, traceable, and properly reported.

### 1.3 Core Principles

1. **Plan Adherence**: Execute ONLY what is specified in validated plans
2. **Reversibility**: Create rollback checkpoints before any modification
3. **Minimal Footprint**: Touch only explicitly listed files
4. **Transparency**: Report all actions with detailed logs
5. **Fail-Safe**: Stop and rollback on any unexpected error

---

## 2. Responsibilities

### 2.1 Primary Responsibilities

| Responsibility | Description | Priority |
|----------------|-------------|----------|
| **Action Execution** | Perform actions defined in inspector plans | Critical |
| **Safe Implementation** | Implement changes with minimal footprint | Critical |
| **Rollback Management** | Generate and maintain rollback checkpoints | Critical |
| **Status Reporting** | Produce detailed execution and error reports | High |
| **Scope Enforcement** | Operate only on explicitly listed files | High |

### 2.2 Execution Capabilities

The executor CAN perform the following operations:

| Operation | Description | Reversibility |
|-----------|-------------|---------------|
| `FILE_CREATE` | Create new files | Reversible (delete) |
| `FILE_MODIFY` | Modify existing files | Reversible (restore backup) |
| `FILE_DELETE` | Delete files | Reversible (restore backup) |
| `FILE_RENAME` | Rename files | Reversible (rename back) |
| `SCHEMA_UPDATE` | Update JSON/YAML schemas | Reversible (restore backup) |
| `SQL_EXECUTE` | Execute SQL transformations | Depends on query type |
| `PIPELINE_RUN` | Run ETL pipelines | Not reversible |

### 2.3 Operation Boundaries

| Boundary | Enforcement |
|----------|-------------|
| File whitelist | Only files listed in task plan |
| Operation whitelist | Only operations defined in plan |
| Directory scope | Only within project root |
| Protected files | Reject if target is protected |

---

## 3. Input Specifications

### 3.1 Required Inputs

| Input | Source | Format | Validation |
|-------|--------|--------|------------|
| Task Plan | Inspector via Protocol | JSON | Schema validation |
| System Config | Inspector via Protocol | YAML | Schema validation |
| Protocol Validation | Protocol Layer | Boolean | Must be `true` |

### 3.2 Input Loading Protocol

```
1. RECEIVE task envelope from protocol layer
2. VERIFY protocol validation status is APPROVED
3. PARSE task_plan.json
4. PARSE system_config.yaml
5. VALIDATE file targets exist (for MODIFY/DELETE/RENAME)
6. VALIDATE no protected files in target list (unless approved)
7. CREATE rollback checkpoint
8. PROCEED with execution
```

### 3.3 Task Plan Structure (Expected)

```json
{
  "plan_id": "uuid",
  "action_plan": [
    {
      "action_id": "string",
      "action_type": "FILE_MODIFY",
      "target": "relative/path/to/file",
      "operation": {
        "type": "operation_type",
        "details": {}
      },
      "depends_on": [],
      "reversible": true,
      "risk_level": "LOW"
    }
  ],
  "execution_instructions": {
    "execution_order": "sequential",
    "stop_on_error": true,
    "rollback_on_failure": true
  }
}
```

---

## 4. Output Specifications

### 4.1 Primary Output: Execution Report (JSON)

Location: `agent_outputs/reports/{timestamp}_{task_id}/execution_report.json`

**Required Structure:**

```json
{
  "report_id": "uuid-v4",
  "plan_id": "uuid (reference to original plan)",
  "executor_version": "1.0.0",
  "status": "SUCCESS|PARTIAL|FAILED|ROLLED_BACK|CANCELLED",
  "started_at": "ISO-8601",
  "completed_at": "ISO-8601",
  "duration_ms": 1234,
  "actions_summary": {
    "total": 5,
    "completed": 4,
    "failed": 1,
    "skipped": 0
  },
  "actions_completed": [
    {
      "action_id": "string",
      "status": "SUCCESS",
      "started_at": "ISO-8601",
      "completed_at": "ISO-8601",
      "output": "operation-specific output"
    }
  ],
  "actions_failed": [
    {
      "action_id": "string",
      "status": "FAILED",
      "error_code": "string",
      "error_message": "string",
      "stack_trace": "optional string"
    }
  ],
  "actions_skipped": [
    {
      "action_id": "string",
      "reason": "Dependency failed"
    }
  ],
  "rollback_performed": false,
  "rollback_manifest_id": "uuid (if rollback available)"
}
```

### 4.2 Secondary Output: Change Log (JSON)

Location: `agent_outputs/reports/{timestamp}_{task_id}/change_log.json`

**Required Structure:**

```json
{
  "log_id": "uuid-v4",
  "plan_id": "uuid (reference)",
  "execution_report_id": "uuid (reference)",
  "created_at": "ISO-8601",
  "changes": [
    {
      "change_id": "uuid",
      "action_id": "string (reference to action)",
      "file_path": "relative/path/to/file",
      "operation": "CREATE|MODIFY|DELETE|RENAME",
      "before_state": {
        "exists": true,
        "hash": "sha256 (null if CREATE)",
        "size_bytes": 1234,
        "last_modified": "ISO-8601"
      },
      "after_state": {
        "exists": true,
        "hash": "sha256 (null if DELETE)",
        "size_bytes": 1256,
        "last_modified": "ISO-8601"
      },
      "diff_summary": {
        "lines_added": 5,
        "lines_removed": 2,
        "preview": "First 500 chars of diff..."
      },
      "timestamp": "ISO-8601"
    }
  ],
  "files_affected_count": 3,
  "total_lines_changed": 42
}
```

### 4.3 Tertiary Output: Rollback Manifest (JSON)

Location: `agent_outputs/reports/{timestamp}_{task_id}/rollback_manifest.json`

**Required Structure:**

```json
{
  "manifest_id": "uuid-v4",
  "plan_id": "uuid (reference)",
  "created_at": "ISO-8601",
  "expires_at": "ISO-8601 (optional TTL)",
  "status": "ACTIVE|EXECUTED|EXPIRED",
  "checkpoints": [
    {
      "checkpoint_id": "uuid",
      "file_path": "relative/path/to/file",
      "backup_location": "agent_outputs/reports/{id}/backups/{filename}.bak",
      "original_hash": "sha256",
      "original_size": 1234,
      "operation_to_reverse": "MODIFY",
      "restore_command": "Description of how to restore"
    }
  ],
  "rollback_order": ["checkpoint_id_1", "checkpoint_id_2"],
  "auto_rollback_script": "Path to generated rollback script",
  "notes": "Any special instructions for manual rollback"
}
```

---

## 5. Execution Engine

### 5.1 Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXECUTION ENGINE FLOW                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. RECEIVE VALIDATED PLAN                                       │
│     └─► Verify protocol validation status                        │
│     └─► Parse plan and config                                    │
│                                                                  │
│  2. PRE-EXECUTION VALIDATION                                     │
│     └─► Verify all target files exist (or CREATE ops)            │
│     └─► Check file permissions                                   │
│     └─► Validate no protected files                              │
│                                                                  │
│  3. CREATE ROLLBACK CHECKPOINT                                   │
│     └─► Backup all files to be modified/deleted                  │
│     └─► Generate rollback manifest                               │
│     └─► Store in agent_outputs/reports/{id}/backups/             │
│                                                                  │
│  4. BUILD EXECUTION GRAPH                                        │
│     └─► Parse action dependencies                                │
│     └─► Create directed acyclic graph (DAG)                      │
│     └─► Topological sort for execution order                     │
│                                                                  │
│  5. EXECUTE ACTIONS                                              │
│     └─► For each action in order:                                │
│         ├─► Check dependencies completed                         │
│         ├─► Execute operation                                    │
│         ├─► Log result                                           │
│         └─► On failure: check stop_on_error flag                 │
│                                                                  │
│  6. POST-EXECUTION VALIDATION                                    │
│     └─► Verify all changes applied                               │
│     └─► Run validation rules from plan                           │
│     └─► Check file hashes match expected                         │
│                                                                  │
│  7. GENERATE REPORTS                                             │
│     └─► Execution report                                         │
│     └─► Change log                                               │
│     └─► Update rollback manifest status                          │
│                                                                  │
│  8. FINALIZE                                                     │
│     └─► Send completion status to protocol                       │
│     └─► Archive if successful                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Action Handlers

Each action type has a dedicated handler:

| Handler | Operation | Implementation |
|---------|-----------|----------------|
| `FileCreateHandler` | FILE_CREATE | Write new file with content |
| `FileModifyHandler` | FILE_MODIFY | Apply patches/replacements |
| `FileDeleteHandler` | FILE_DELETE | Remove file (after backup) |
| `FileRenameHandler` | FILE_RENAME | Rename/move file |
| `SchemaUpdateHandler` | SCHEMA_UPDATE | Modify JSON/YAML schemas |
| `SqlExecuteHandler` | SQL_EXECUTE | Run SQL via DuckDB |
| `PipelineRunHandler` | PIPELINE_RUN | Trigger ETL pipeline |

### 5.3 Dependency Resolution

```python
# Pseudo-code for dependency resolution
def resolve_dependencies(actions):
    graph = build_dag(actions)

    # Verify no cycles
    if has_cycle(graph):
        raise CyclicDependencyError()

    # Topological sort
    execution_order = topological_sort(graph)

    return execution_order

def execute_with_dependencies(action, completed_actions):
    # Check all dependencies completed
    for dep_id in action.depends_on:
        if dep_id not in completed_actions:
            return SKIP, f"Dependency {dep_id} not completed"
        if completed_actions[dep_id].status == FAILED:
            return SKIP, f"Dependency {dep_id} failed"

    # Execute action
    return execute_action(action)
```

---

## 6. Rollback System

### 6.1 Checkpoint Creation

Before ANY modification, the executor MUST:

1. **Identify files to backup**: All files in MODIFY, DELETE, RENAME operations
2. **Create backup directory**: `agent_outputs/reports/{task_id}/backups/`
3. **Copy files with metadata**: Preserve timestamps, permissions
4. **Calculate hashes**: SHA-256 of original content
5. **Generate manifest**: Document all checkpoints

### 6.2 Rollback Triggers

| Trigger | Behavior |
|---------|----------|
| Action failure + `rollback_on_failure=true` | Automatic rollback |
| Manual rollback request | User-initiated rollback |
| Validation failure | Automatic rollback |
| System error | Automatic rollback |

### 6.3 Rollback Execution

```
┌─────────────────────────────────────────────────────────────────┐
│                    ROLLBACK EXECUTION FLOW                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. LOAD ROLLBACK MANIFEST                                       │
│     └─► Parse manifest file                                      │
│     └─► Verify checkpoints exist                                 │
│                                                                  │
│  2. DETERMINE ROLLBACK SCOPE                                     │
│     └─► Full rollback: all checkpoints                           │
│     └─► Partial rollback: specific checkpoints                   │
│                                                                  │
│  3. EXECUTE IN REVERSE ORDER                                     │
│     └─► For each checkpoint (reverse order):                     │
│         ├─► For DELETE: recreate file from backup                │
│         ├─► For MODIFY: restore original content                 │
│         ├─► For CREATE: delete created file                      │
│         └─► For RENAME: rename back to original                  │
│                                                                  │
│  4. VERIFY RESTORATION                                           │
│     └─► Check file hashes match original                         │
│     └─► Verify file permissions restored                         │
│                                                                  │
│  5. UPDATE MANIFEST STATUS                                       │
│     └─► Mark as EXECUTED                                         │
│     └─► Record rollback timestamp                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 6.4 Rollback Limitations

| Scenario | Rollback Possible | Notes |
|----------|-------------------|-------|
| File created | Yes | Delete the file |
| File modified | Yes | Restore from backup |
| File deleted | Yes | Restore from backup |
| File renamed | Yes | Rename back |
| SQL executed | Partial | Only if reversible query |
| Pipeline run | No | External side effects |

---

## 7. Constraints and Boundaries

### 7.1 Hard Constraints (NEVER Violate)

| Constraint | Description | Enforcement |
|------------|-------------|-------------|
| **PLAN_ONLY** | Execute only actions from validated plans | Protocol validation |
| **FILE_WHITELIST** | Modify only files listed in plan | Path matching |
| **CHECKPOINT_FIRST** | Create rollback before any modification | Pre-execution hook |
| **REPORT_ALL** | Report all actions including failures | Post-execution hook |
| **NO_INTERPRETATION** | Do not extend or interpret instructions | Literal execution |

### 7.2 Soft Constraints (Follow Unless Plan Overrides)

| Constraint | Default | Override Condition |
|------------|---------|-------------------|
| `stop_on_error` | `true` | Plan specifies `false` |
| `rollback_on_failure` | `true` | Plan specifies `false` |
| `preserve_permissions` | `true` | Explicitly disabled |
| `verify_hashes` | `true` | Performance critical |

### 7.3 Forbidden Actions

The executor MUST NOT:

- Modify files not listed in the task plan
- Execute actions without protocol validation
- Skip rollback checkpoint creation
- Interpret or extend plan instructions
- Access network resources
- Execute arbitrary shell commands
- Modify protected files without explicit approval
- Delete rollback manifests prematurely

---

## 8. Error Handling

### 8.1 Error Categories

| Category | Code Range | Response |
|----------|------------|----------|
| **Validation Error** | 1000-1999 | Reject, return error |
| **File System Error** | 2000-2999 | Log, attempt rollback |
| **Permission Error** | 3000-3999 | Log, skip action |
| **Dependency Error** | 4000-4999 | Skip dependent actions |
| **Internal Error** | 5000-5999 | Full rollback, escalate |

### 8.2 Error Response Format

```json
{
  "error_id": "uuid",
  "error_code": 2001,
  "error_category": "FILE_SYSTEM_ERROR",
  "error_type": "FILE_NOT_FOUND",
  "message": "Target file does not exist",
  "details": {
    "action_id": "a001",
    "target": "path/to/missing/file.py",
    "operation": "FILE_MODIFY"
  },
  "recovery_action": "SKIPPED|ROLLED_BACK|MANUAL_REQUIRED",
  "timestamp": "ISO-8601"
}
```

### 8.3 Error Recovery Matrix

| Error Type | Action | Recovery |
|------------|--------|----------|
| File not found (MODIFY) | Skip action | Mark as failed |
| Permission denied | Skip action | Mark as failed |
| Disk full | Stop execution | Rollback |
| Hash mismatch | Stop execution | Rollback |
| Dependency failed | Skip dependents | Continue |
| Unknown error | Stop execution | Rollback + Escalate |

---

## 9. Security Considerations

### 9.1 Protected Files

The following files require elevated approval:

```yaml
protected_files:
  critical:
    - .git/**
    - .env
    - .env.*
    - credentials.json
    - secrets.*

  important:
    - requirements.txt
    - pyproject.toml
    - setup.py
    - .pre-commit-config.yaml

  agent_system:
    - agent/agent_rules.md
    - agent_inspector/agent_inspector.md
    - agent_executor/agent_executor.md
```

### 9.2 Path Traversal Prevention

```python
# All file paths are validated against project root
def validate_path(target_path, project_root):
    resolved = Path(target_path).resolve()
    root = Path(project_root).resolve()

    # Ensure path is within project
    if not str(resolved).startswith(str(root)):
        raise SecurityError("Path traversal attempt detected")

    return resolved
```

### 9.3 Content Validation

- No executable content injection
- No shell command embedding
- No credential exposure
- JSON/YAML syntax validation

---

## 10. Integration Points

### 10.1 Upstream (Input Sources)

| Source | Interface | Format |
|--------|-----------|--------|
| Agent Inspector | Task plan via Protocol | JSON |
| Protocol Layer | Validated envelope | JSON |

### 10.2 Downstream (Output Consumers)

| Consumer | Interface | Format |
|----------|-----------|--------|
| Protocol Layer | Execution status | JSON envelope |
| Audit System | Reports | JSON |
| Archive System | Historical records | JSON |

### 10.3 Communication Flow

```
Inspector ──► Protocol ──► Executor
                             │
                             ├──► agent_outputs/reports/
                             ├──► agent_logs/executor/
                             └──► Protocol (status updates)
```

---

## 11. Performance Considerations

### 11.1 Timeouts

| Operation | Default Timeout | Maximum |
|-----------|-----------------|---------|
| Single file operation | 30s | 120s |
| Schema update | 30s | 60s |
| SQL execution | 60s | 300s |
| Pipeline run | 300s | 3600s |
| Total task execution | 300s | 3600s |

### 11.2 Resource Limits

| Resource | Limit |
|----------|-------|
| Max files modified per task | 100 |
| Max file size | 50 MB |
| Max backup storage | 500 MB per task |
| Max concurrent operations | 4 |

---

## 12. Examples

### 12.1 Example: Successful File Modification

**Input Task Plan:**
```json
{
  "plan_id": "plan-001",
  "action_plan": [
    {
      "action_id": "a001",
      "action_type": "FILE_MODIFY",
      "target": "esquemas/esquema_bd.json",
      "operation": {
        "type": "json_add_property",
        "path": "$.properties",
        "key": "new_field",
        "value": {"type": "string"}
      }
    }
  ]
}
```

**Output Execution Report:**
```json
{
  "report_id": "report-001",
  "plan_id": "plan-001",
  "status": "SUCCESS",
  "actions_summary": {"total": 1, "completed": 1, "failed": 0},
  "actions_completed": [
    {
      "action_id": "a001",
      "status": "SUCCESS",
      "output": "Property 'new_field' added successfully"
    }
  ]
}
```

### 12.2 Example: Failed Execution with Rollback

**Scenario**: Second action fails, triggering rollback

**Execution Report:**
```json
{
  "report_id": "report-002",
  "status": "ROLLED_BACK",
  "actions_summary": {"total": 3, "completed": 1, "failed": 1, "skipped": 1},
  "actions_failed": [
    {
      "action_id": "a002",
      "error_code": 2001,
      "error_message": "File not found"
    }
  ],
  "rollback_performed": true,
  "rollback_manifest_id": "manifest-002"
}
```

---

## Appendix A: Handler Specifications

### A.1 FileModifyHandler Operations

| Operation Type | Description | Parameters |
|----------------|-------------|------------|
| `text_replace` | Find and replace text | `pattern`, `replacement` |
| `line_insert` | Insert lines at position | `line_number`, `content` |
| `line_delete` | Delete lines | `start_line`, `end_line` |
| `json_add_property` | Add JSON property | `path`, `key`, `value` |
| `json_remove_property` | Remove JSON property | `path`, `key` |
| `json_update_value` | Update JSON value | `path`, `value` |
| `yaml_update` | Update YAML structure | `path`, `value` |

### A.2 SqlExecuteHandler Parameters

```json
{
  "type": "sql_execute",
  "query_file": "queries/query.sql",
  "parameters": {},
  "connection": "duckdb",
  "expect_results": false,
  "save_results_to": "optional/path.parquet"
}
```

---

## Appendix B: Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-03 | Initial specification |

