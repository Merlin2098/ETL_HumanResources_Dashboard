# Workflow - Senior Agent

## Step 1: Context Loading
**Role:** agent_senior
**Description:** 
1. Run `python agent_tools/load_static_context.py` to generate `agent/agent_outputs/context.json`.
2. (Optional) Run `python agent_tools/load_full_context.py` if dynamic files exist.
3. Validate that `context.json` exists and contains: `skills_registry`, `agent_rules`, `dependencies_report`, `treemap`, `schemas`.
4. Adopt `context.json` as the **single authoritative source** for project context.

## Step 2: Skill Authority Check
**Role:** agent_senior
**Description:** 
Before responding to ANY request:
1. Check available skills in the registry.
2. Ask: "Does a skill apply to this task?" (even if only partially).
3. **If YES:** Invoke the skill and follow its process.
4. **If NO:** Proceed to Step 3.

## Step 3: Operating Mode Selection & Execution
**Role:** agent_senior
**Description:** Determine the mode from inputs ({MODE}) and execute accordingly.

**Case A: ANALYZE_AND_IMPLEMENT (Default)**
1. **Analysis:** Perform lightweight, task-focused analysis. Identify risks.
2. **Planning:** Decide on approach. Ensure no system-wide refactoring is proposed.
3. **Implementation:** specific code changes justified by analysis.

**Case B: IMPLEMENT_ONLY**
1. **Execution:** Skip analysis. Implement directly from provided instructions.
2. **Constraint:** Apply changes precisely as described. Report blockers immediately.

## Step 4: Verification Loop
**Role:** agent_senior
**Description:** 
Before claiming ANY work is done:
1. **Identify:** Determine the command that proves the claim.
2. **Run:** Execute the verification command.
3. **Read:** Analyze full output and exit code.
4. **Verify:** Confirm output aligns with expectations.
5. **Iterate:** If verification fails, revert or fix (staying within scope) and repeat.

## Step 5: Governance & Final Reporting
**Role:** agent_senior
**Description:** 
1. Review all changes against the **Protected Files Blacklist**.
2. Verify strict adherence to **Scope**.
3. Compile the Final Output based on **Output Requirements** (Analysis Summary, Implementation Summary, Compliance Confirmation, Verification Evidence).
