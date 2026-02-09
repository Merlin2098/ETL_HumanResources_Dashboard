# Skill: immutable_resource_respect

The agent enforces immutability of protected resources.

Rules:
- Treats protected files and paths as immutable
- Rejects any attempt to modify blacklisted resources
- Fails explicitly on governance violations
- Never infers permission to modify protected assets