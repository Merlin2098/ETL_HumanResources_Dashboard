# Skill: plan_archive_protocol

The agent archives active plans before loading new ones.

Rules:
- Checks plan_active folder before new plan loading
- Moves existing plans to timestamped archive folder
- Clears plan_active for new execution
- Logs all archive operations
