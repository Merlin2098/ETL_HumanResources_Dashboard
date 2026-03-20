# Tinker Framework - Chat Bootstrap (Model-Agnostic)

Purpose: explicit command-driven setup for chat-based agents.

## Hard constraints
1. Validate `agent/user_task.yaml` against `agent/agent_protocol/schemas/user_task.schema.yaml`.
2. No field inference and no fabricated data.
3. Wrapper-first execution model.

## Chat Commands
Use these exact phrases in chat:
- `Tinker: validate`
- `Tinker: run`
- `Run task from agent/user_task.yaml`
- `commit`
- `sync`
- `commit and sync`

## Kernel commands (chat)
- `Kernel LITE`
- `Kernel STANDARD`
- `Kernel FULL`

## Command Runbook
1. Governance load command:
   - `Read .clinerules and agent/rules/agent_rules.md`
2. Kernel selection command:
   - `Kernel LITE|STANDARD|FULL`
3. Context refresh command:
   - `$env:PYTHONIOENCODING='utf-8'; ./.venv/Scripts/python.exe agent_tools/load_static_context.py`
4. Task contract build command (optional):
   - `./.venv/Scripts/python.exe agent_tools/user_task_builder.py --help`
5. Task validation command:
   - `$env:PYTHONIOENCODING='utf-8'; ./.venv/Scripts/python.exe agent_tools/schema_validator.py agent/user_task.yaml --type user_task`
6. Optional plan-doc command set:
   - `./.venv/Scripts/python.exe agent_tools/plan_doc.py --help`
7. Execution trigger command:
   - `Tinker: run` or `Run task from agent/user_task.yaml`

## Shortcut intent routing
- If the user says `commit`, run:
  - `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit"`
- If the user says `sync`, run:
  - `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "sync"`
- If the user says `commit and sync`, run:
  - `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit and sync"`
- Optional explicit commit message:
  - `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit" --message "<checkpoint message>"`

## Notes
- `mode_profile` in `agent/user_task.yaml` is optional; leave empty unless explicitly needed.
- Optional compatibility fields (`phase`, `validation`) are supported but not required.
- Plan docs under `agent/agent_outputs/plans/` are optional collaboration artifacts, not execution prerequisites.
