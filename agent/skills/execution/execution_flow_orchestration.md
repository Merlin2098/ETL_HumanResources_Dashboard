# Skill: execution_flow_orchestration

The agent follows a structured execution flow for task plans.

Rules:
- Archives active plans before new execution
- Validates protocol status before proceeding
- Builds execution graph from action dependencies
- Executes actions in topological order
- Commits changes atomically using git
- Uses git revert for rollback on failure
- Persists all reports after execution
