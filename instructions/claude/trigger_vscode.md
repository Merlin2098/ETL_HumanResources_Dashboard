# Tinker Framework - VS Code Bootstrap (Model-Agnostic)

Purpose: explicit, repeatable setup in VS Code without vendor lock-in.

## Non-negotiable rules
1. Validate `agent/user_task.yaml` against `agent/agent_protocol/schemas/user_task.schema.yaml`.
2. Do not infer missing required fields.
3. Do not invent data, paths, or outcomes.
4. Keep execution wrapper-first (`agent_tools/run_wrapper.py`, `agent_tools/wrappers/*`).
5. Do not depend on external IDE state for governance/runtime state.

## Explicit bootstrap steps
1. Read governance:
   - `Read .clinerules and agent/rules/agent_rules.md`
2. Activate kernel profile (choose one):
   - `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile LITE`
   - `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile STANDARD`
   - `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile FULL`
3. Load static context:
   - PowerShell: `$env:PYTHONIOENCODING='utf-8'; ./.venv/Scripts/python.exe agent_tools/load_static_context.py`
4. Edit `agent/user_task.yaml`:
   - Required: `mode`, `objective`, `files`, `constraints`
   - Optional: `mode_profile`, `config`, `risk_tolerance`, `phase`, `validation`
5. Trigger:
   - `Run task from agent/user_task.yaml`

## Skill loading protocol
1. `agent/skills/_index.yaml`
2. shortlisted `agent/skills/<category>/<skill>.meta.yaml`
3. invoked `agent/skills/<category>/<skill>.md`

Never bulk-load skill bodies.
