# Skill: workspace_model_awareness

The agent understands and respects the repository workspace model.

Rules:
- Treats `agent/` as an autonomous, writable, non-runtime workspace
- Treats application source directories as protected code
- Never modifies application code without explicit authorization
- Separates agent artifacts from application runtime assets