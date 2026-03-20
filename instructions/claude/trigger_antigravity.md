# Tinker Framework - Antigravity Bootstrap (Model-Agnostic)

Purpose: explicit, repeatable setup in Antigravity without vendor lock-in.

## Hard constraints
1. Validate `agent/user_task.yaml` against `agent/agent_protocol/schemas/user_task.schema.yaml`.
2. No field inference and no data invention.
3. Wrapper-first execution model.
4. Governance is internal to Tinker.

## Explicit bootstrap steps
1. Read governance:
   - `Read .clinerules and agent/rules/agent_rules.md`
2. Activate kernel profile:
   - `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile LITE|STANDARD|FULL`
3. Load static context:
   - PowerShell: `$env:PYTHONIOENCODING='utf-8'; ./.venv/Scripts/python.exe agent_tools/load_static_context.py`
4. Edit `agent/user_task.yaml`:
   - Required: `mode`, `objective`, `files`, `constraints`
   - Optional compatibility fields: `mode_profile`, `config`, `risk_tolerance`, `phase`, `validation`
5. Trigger:
   - `Run task from agent/user_task.yaml`

## Skill loading protocol
1. `_index.yaml`
2. `.meta.yaml` for shortlisted skills
3. `.md` for invoked skills

Do not bulk-load skill bodies.
