# Instrucciones Dummies (simple)

## 0. Reglas base
`Read .clinerules and agent/rules/agent_rules.md`

## 1. Activar kernel
- LITE: `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile LITE`
- STANDARD: `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile STANDARD`
- FULL: `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile FULL`

## 2. Campos de `agent/user_task.yaml`
Requeridos:
1. `mode`
2. `objective`
3. `files`
4. `constraints`

Opcionales:
- `mode_profile`
- `config.sources`
- `risk_tolerance`
- `phase`
- `validation`

## 3. Trigger exacto
`Run task from agent/user_task.yaml`

## 4. Regla practica
- Usa skills finos + wrappers canonicos.
- No inventes datos ni cambies fuera del alcance.
