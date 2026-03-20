# Portable Deployment Guide - Tinker Framework

This guide explains how to deploy the framework into a host Python project.
Status reflects framework state as of 2026-02-12.

## 1. Current Status

- Skill catalog: defined by `agent/skills/_index.yaml`.
- Three-layer loading is mandatory:
  - Layer 1: `agent/skills/_index.yaml`
  - Layer 2: `*.meta.yaml`
  - Layer 3: `*.md`
- New quality/debug capabilities included:
  - `debugger_orchestrator`
  - `regression_test_automation`
  - `code_analysis_qa_gate`
- Wrapper generation is deterministic:
  - `agent_tools/generate_skill_wrappers.py` only rewrites wrappers when content changes.

## 2. Target Layout

Tinker must live at host project root.

```text
host-project/
  .gitignore
  .clinerules
  agent_framework_config.yaml
  agent/
  agent_tools/
instructions/
    claude/
      trigger_vscode.md
      trigger_antigravity.md
    chat/
      trigger_chat.md
      command_glossary_chat.md
    model_agnostic/
      trigger_generic.md
      PORTABLE_DEPLOYMENT.md
      kernel_profiles.md
      instructions_dummies.md
      activate_kernel.cmd
      activate_kernel.sh
      .gitignore.host
```

Important:
- `agent_tools/` must be one level below host root.
- `load_static_context.py` resolves root as `parent(parent(__file__))`.

## 3. Copy Into Host Project

Copy these assets:
- `agent/`
- `agent_tools/`
- `.clinerules`
- `agent_framework_config.yaml`
- `instructions/`
- `AGENTS.md` and `agent.md` if your runner uses them.

Optional: use the installer (recommended when applying Tinker to multiple projects):

```powershell
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe install_tinker.py C:\path\to\host-project
```

## 4. Update Host .gitignore

Use `instructions/model_agnostic/.gitignore.host`.

PowerShell:

```powershell
Get-Content .\instructions\model_agnostic\.gitignore.host | Add-Content C:\path\to\host-project\.gitignore
```

Bash:

```bash
cat instructions/model_agnostic/.gitignore.host >> /path/to/host-project/.gitignore
```

This keeps framework internals out of host git history and host code analysis.

## 5. Bootstrap Runtime

Use venv Python and UTF-8:

```powershell
$env:PYTHONIOENCODING='utf-8'
.\.venv\Scripts\python.exe agent_tools/load_static_context.py
```

Generated output:
- `agent/agent_outputs/context.json`

## 6. Kernel + Profiles

Kernel and profiles are part of the framework:
- Kernel: `agent/kernel/kernel.yaml`
- Profiles: `agent/profiles/lite.yaml`, `agent/profiles/standard.yaml`, `agent/profiles/full.yaml`

Activate a profile before running tasks:

```powershell
.\.venv\Scripts\python.exe agent_tools/activate_kernel.py --profile LITE
```

`load_static_context.py` resolves profile in this priority:
1. CLI argument: `--profile <name>`
2. Env var: `TINKER_CONTEXT_PROFILE`
3. Config: `active_profile` in `agent_framework_config.yaml`
4. `profile_detection` rules
5. Default `static_context`

Use `instructions/claude/trigger_vscode.md`, `instructions/claude/trigger_antigravity.md`, or
`instructions/model_agnostic/trigger_generic.md` for explicit bootstrap steps.

## 7. Data Path Governance

`agent_framework_config.yaml` now includes `data_governance`:
- `dev`/`test`: local debug data roots allowed.
- `prod`: data roots must be external to `project_root`.
- `source` and `output` separation is enforced.
- Final reports should consume previously materialized artifacts.
- Output format is declared per use case (not globally fixed to parquet).

## 8. Context Size Governance

`context.json` is intentionally compact:
- skills metadata only
- file tree summary
- python signatures summary
- schema key summaries
- on-demand pointers for heavy files

Hard limit:
- `static_context.max_lines` is enforced.
- deterministic truncation applies when needed.
- generation fails if still above budget.

Policy location:
- `static_context.truncation_policy`

## 9. On-Demand Files

Heavy files are not embedded in initial context:
- `agent/analysis/treemap.md`
- `agent/analysis/dependencies_report.md`

Load on demand via:
- `agent_tools/context_loader.py`

## 10. Post-Deploy Check

After deployment verify:
1. `agent_tools/load_static_context.py` runs successfully.
2. Output prints active profile info.
3. `agent/agent_outputs/context.json` is generated.
4. Host `.gitignore` includes template entries.
5. Running `agent_tools/generate_skill_wrappers.py` twice yields `Wrappers updated: 0` on second run.

