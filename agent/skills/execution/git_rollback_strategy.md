# Skill: git_rollback_strategy

The agent uses git for all rollback operations.

Rules:
- Commits changes atomically before execution completes
- Uses git revert for rollback instead of custom backups
- Does not maintain separate backup files or manifests
- Leverages git history for auditability and traceability
- Requires clean working directory before execution
