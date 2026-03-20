# Kernel Profiles (LITE vs STANDARD vs FULL)

Kernel behavior in Tinker is allowlist-only.
Profiles select capability allowlists; they do not define roles or workflows.

## LITE
Use when: small, focused tasks.
- Narrow skill allowlist.
- Minimal capability surface.

## STANDARD
Use when: regular implementation and analysis.
- Medium allowlist.
- Broader exploration/planning/debugging capabilities.

## FULL
Use when: complex tasks needing widest approved capability surface.
- Maximum allowlist.
- Still constrained by explicit allowlists.

## How profile is chosen
1. `mode_profile` in `agent/user_task.yaml` (if set)
2. `agent/agent_outputs/runtime/active_profile.<agent-id>.json` (if present)
3. `agent/agent_outputs/runtime/active_profile.json`
4. default `LITE`

## Activation
- `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile LITE`
- `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile STANDARD`
- `./.venv/Scripts/python.exe agent_tools/activate_kernel.py --profile FULL`
