# Agent Protocol Layer

## Overview

The Agent Protocol Layer is the communication and validation boundary between planning and execution agents.

It provides:
- Message validation
- Routing
- Lifecycle tracking
- Audit traceability

## Directory structure

```text
agent_protocol/
  README.md
  schemas/
    task_envelope.schema.json
    task_plan.schema.json
    execution_report.schema.json
    system_config.schema.yaml
    user_task.schema.yaml
    plan_doc.schema.yaml
  message_queue/
    pending/
    in_progress/
    completed/
```

## Message types

| Type | Direction | Purpose |
|------|-----------|---------|
| `TASK_REQUEST` | Inspector -> Executor | Request task execution |
| `TASK_RESPONSE` | Executor -> Inspector | Report execution result |
| `STATUS_UPDATE` | Executor -> Inspector | Progress update |
| `ERROR` | Any -> Any | Error notification |
| `ACK` | Receiver -> Sender | Acknowledgment |
| `ROLLBACK_REQUEST` | Any -> Executor | Request rollback |
| `CANCEL` | Any -> Executor | Cancel running task |

## Lifecycle

```text
CREATED -> VALIDATED -> QUEUED -> IN_PROGRESS -> COMPLETED/FAILED/ROLLED_BACK -> ARCHIVED
```

## Schema references

- Task Envelope: `schemas/task_envelope.schema.json`
- Task Plan: `schemas/task_plan.schema.json`
- Execution Report: `schemas/execution_report.schema.json`
- System Config: `schemas/system_config.schema.yaml`
- User Task Contract: `schemas/user_task.schema.yaml`
- Plan Doc (optional review/handoff artifact): `schemas/plan_doc.schema.yaml`

## Version

- Protocol Version: 1.1.0
- Last Updated: 2026-02-12
