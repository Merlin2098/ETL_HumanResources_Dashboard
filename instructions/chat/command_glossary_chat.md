# Tinker Chat Command Glossary

Purpose: command-specific reference for chat operation.

## Kernel selection
- `Kernel LITE`
- `Kernel STANDARD`
- `Kernel FULL`

## Task triggers
- `Tinker: validate` -> validate task contract/readiness.
- `Tinker: run` -> execute wrapper-first flow.
- `Run task from agent/user_task.yaml` -> explicit task execution trigger.

## Contract commands
- `./.venv/Scripts/python.exe agent_tools/user_task_builder.py --input <yaml>`
- `./.venv/Scripts/python.exe agent_tools/user_task_builder.py --from-template <yaml> ...`
- `./.venv/Scripts/python.exe agent_tools/schema_validator.py agent/user_task.yaml --type user_task`

## Context refresh
- `./.venv/Scripts/python.exe agent_tools/load_static_context.py`
- `./.venv/Scripts/python.exe agent_tools/load_full_context.py --task-plan <path> --system-config <path> --summary <path>`
- `./.venv/Scripts/python.exe agent_tools/load_full_context.py --task-plan <path> --system-config <path> --summary <path> --on-demand treemap`

## Validation
- `./.venv/Scripts/python.exe agent_tools/schema_validator.py <file> --type user_task|task_plan|system_config|envelope|report|plan_doc`
- `./.venv/Scripts/python.exe agent_tools/schema_validator.py <file> --type plan|config` (alias types)
- `./.venv/Scripts/python.exe agent_tools/validate_message.py <file> --type plan|config|...` (compatibility validator)

## Optional plan docs
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py init --id <id> --title <title> --objective <text>`
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py add-step --file <plan.yaml> --step-id <id> --description <text>`
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py approve --file <plan.yaml> --by <name> [--note <text>]`
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py handoff --file <plan.yaml> --to <target> --notes <text>`
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py validate --file <plan.yaml>`
- `./.venv/Scripts/python.exe agent_tools/plan_doc.py archive --file <plan.yaml>`

## Wrapper execution
- `./.venv/Scripts/python.exe agent_tools/run_wrapper.py --skill <skill> --args-file <args.json>`
- `./.venv/Scripts/python.exe agent_tools/run_wrapper.py --skill <skill> --args-json '{\"key\":\"value\"}'`

## Kernel/profile utilities
- `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile LITE|STANDARD|FULL`
- `./.venv/Scripts/python.exe agent_tools/mode_selector.py --write-state`
- `./.venv/Scripts/python.exe agent_tools/kernel_guard.py --prefer active|task`

## Install/upgrade Tinker
- `./.venv/Scripts/python.exe install_tinker.py <dest1> [dest2 ...]`
- `./.venv/Scripts/python.exe install_tinker.py --pick-dest`
- `./.venv/Scripts/python.exe install_tinker.py --dry-run <dest>`
- `./.venv/Scripts/python.exe install_tinker.py --overwrite-framework --backup <dest>`

## Git checkpoints
- `./.venv/Scripts/python.exe agent_tools/git_checkpoint.py commit --message "<checkpoint message>"`
- `./.venv/Scripts/python.exe agent_tools/git_checkpoint.py sync`
- `./.venv/Scripts/python.exe agent_tools/git_checkpoint.py commit-sync --message "<checkpoint message>"`

## Natural-language shortcut intents
- `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit"`
- `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "sync"`
- `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit and sync"`
- `./.venv/Scripts/python.exe agent_tools/chat_shortcuts.py "commit" --message "<checkpoint message>"`

## Notes
- `mode_profile` is optional.
- `phase` and `validation` are optional compatibility fields, not hard requirements.
- Plan docs are optional collaboration artifacts and are not required to execute `agent/user_task.yaml`.
