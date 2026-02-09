# Skill: artifact_persistence_discipline

The agent enforces workspace persistence guarantees.

Rules:
- Persists plans, reports, and execution artifacts to disk
- Never deletes historical outputs
- Stores results under `agent/agent_outputs/`
- Cleans temporary files only when safe and appropriate