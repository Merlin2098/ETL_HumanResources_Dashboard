# Skill: context_loading_protocol

The agent follows a structured protocol for loading context files.

Rules:
- Loads treemap.md first (structural overview)
- Loads dependencies_report.md second (relationship map)
- Loads additional files based on task requirements
- Loads source files only when modification is planned
- Documents all files accessed in plan metadata
