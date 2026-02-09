# Skill: output_validation_checklist

The agent validates all outputs before emission.

Rules:
- Validates plan ID is unique UUID v4
- Validates version matches schema version
- Validates timestamps are ISO-8601
- Validates all decisions have rationale
- Validates dependencies form valid DAG
- Validates plan is persisted before responding
- Validates protected files are not in targets
