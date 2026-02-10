# Workflow - Agent Inspector

## Step 1: Context & Skill Loading
**Role:** agent_inspector
**Description:**
1. Run `agent_tools/load_static_context.py` to get authoritative context.
2. Load core maps: `treemap.md` (structure), `dependencies_report.md` (relationships).
3. Load `skills_registry` and `agent_rules`.
4. **Skill Index Loading:** Load `agent/skills/_index.yaml` for skill-aware planning.
5. **Trigger Evaluation:** Use `_trigger_engine.yaml` to identify deterministic skill candidates from file extensions, pipeline phases, and error patterns.
6. Load specific source files ONLY as needed for the task.

## Step 2: Analysis & Impact Assessment
**Role:** agent_inspector
**Description:**
1. **Trace Dependencies:** Use the loaded maps to see what is affected.
2. **Behavioral Check:** Analyze function contracts and data flows.
3. **Risk Check:** Identify potential breaking changes or data issues.
4. **Skill Relevance:** For each identified skill candidate, load its `.meta.yaml` header to validate applicability and check for exclusive groups.

## Step 3: Option Evaluation
**Role:** agent_inspector
**Description:**
1. Generate 2-3 alternative approaches.
2. Score each by Risk, Complexity, and Reversibility.
3. Select the best approach and document the Rationale (`decisions` field).
4. **Skill Selection:** Include selected skills in the plan with their dependencies (`requires` field) and pipeline order.

## Step 4: Plan Generation
**Role:** agent_inspector
**Description:**
1. **Decompose:** Break the selected approach into atomic actions.
2. **Structure:** Create the `action_plan` array (CREATE, MODIFY, DELETE, etc.).
3. **Sequence:** Establish dependency order.
4. **Skill Binding:** Annotate each action with the applicable skill(s) to load at execution time.

## Step 5: Validation & Persistence
**Role:** agent_inspector
**Description:**
1. **Validate:** Check against Schema requirements and Protected Files Blacklist.
2. **Persist:** Write `task_plan.json` and `system_config.yaml` to `agent/agent_outputs/plans/{timestamp}_{task_id}/`.
3. **Confirm:** Return the path of the generated plan to the user.
