# Workflow - Agent Executor

## Step 1: Context & Input Loading
**Role:** agent_executor
**Description:**
1. Run `agent_tools/load_static_context.py` to get authoritative context.
2. Check for `agent/user_task.yaml`. If present and requested, load `objective`, `files`, and `skill_hints` from it.
3. Load primary inputs: `task_plan.json`, `system_config.yaml`, (optional) `IMPLEMENTATION_SUMMARY.md`.
4. **Skill Index Loading:** Load `agent/skills/_index.yaml` to verify skill references in the task plan.
5. **Validate:** Key context keys exist (`skills_registry`, `agent_rules`, etc.).

## Step 2: Pre-Execution Protocol
**Role:** agent_executor
**Description:**
1. **Archive Active Plans:** Move existing files in `agent/agent_outputs/plans/plan_active/` to archive.
2. **Validate Inputs:**
   - Parse plan and config.
   - Verify all target files exist (for MODIFY/DELETE).
   - **CRITICAL:** Check targets against **Protected Files Blacklist**.
3. **Skill Loading:** For each action in the plan, load the bound skill's `.meta.yaml` and `.md` body on demand (Layer 2 → Layer 3).
4. **Create Checkpoint:**
   - Ensure clean git working directory.
   - Record current commit hash as rollback point.

## Step 3: Execution Engine
**Role:** agent_executor
**Description:**
Execute the `action_plan` from `task_plan.json` sequentially:
1. **Build Graph:** dependency check (DAG).
2. **Loop Actions:**
   - Verify preconditions.
   - Load bound skill body if not already in context.
   - Execute operation (`FILE_MODIFY`, `FILE_CREATE`, etc.).
   - Validate result.
   - Log to internal change log.
   - Update status.
3. **Handle Errors:** If `stop_on_error` is true, halt. If `rollback_on_failure` is true, trigger Step 4.

## Step 4: Rollback (Conditional)
**Role:** agent_executor
**Description:**
**Trigger:** Critical error + `rollback_on_failure=true` OR User Request.
1. Identify commit(s) to revert.
2. Execute `git revert`.
3. Log revert operation.
4. Update status to `ROLLED_BACK`.

## Step 5: Reporting & Persistence
**Role:** agent_executor
**Description:**
Generate and save mandatory reports to `agent/agent_outputs/reports/{timestamp}_{task_id}/`:
1. `execution_report.json`
2. `change_log.json`
3. `rollback_manifest.json`
4. `executor_prompt.txt`
