# Agent Protocol Layer

## Overview

The Agent Protocol Layer serves as the communication middleware between `agent_inspector` and `agent_executor`. It provides message validation, routing, lifecycle management, and audit logging for all inter-agent communications.

## Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  agent_inspector │ ──► │  agent_protocol  │ ──► │  agent_executor  │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                │
                                ├── Validation
                                ├── Routing
                                ├── Lifecycle Management
                                └── Audit Logging
```

## Directory Structure

```
agent_protocol/
├── README.md                    # This file
├── schemas/                     # Message schemas
│   ├── task_envelope.schema.json
│   ├── task_plan.schema.json
│   ├── execution_report.schema.json
│   └── system_config.schema.yaml
├── message_queue/              # Message storage
│   ├── pending/                # Tasks awaiting execution
│   ├── in_progress/            # Currently executing
│   └── completed/              # Finished tasks
└── validators/                 # Validation scripts
    └── envelope_validator.py
```

## Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `TASK_REQUEST` | Inspector → Executor | Request task execution |
| `TASK_RESPONSE` | Executor → Inspector | Report execution result |
| `STATUS_UPDATE` | Executor → Inspector | Progress update |
| `ERROR` | Any → Any | Error notification |
| `ACK` | Receiver → Sender | Acknowledgment |
| `ROLLBACK_REQUEST` | Any → Executor | Request rollback |
| `CANCEL` | Any → Executor | Cancel running task |

## Message Lifecycle

```
CREATED → VALIDATED → QUEUED → IN_PROGRESS → COMPLETED/FAILED/ROLLED_BACK → ARCHIVED
```

## Usage

### Sending a Message

1. Create task envelope following `task_envelope.schema.json`
2. Place in `message_queue/pending/`
3. Protocol validates and moves to `in_progress/`
4. Executor processes and moves to `completed/`

### Validation

All messages are validated against schemas before processing:

```bash
python validators/envelope_validator.py message.json
```

## Schema References

- **Task Envelope**: `schemas/task_envelope.schema.json`
- **Task Plan**: `schemas/task_plan.schema.json`
- **Execution Report**: `schemas/execution_report.schema.json`
- **System Config**: `schemas/system_config.schema.yaml`

## Version

- Protocol Version: 1.0.0
- Last Updated: 2026-02-03
