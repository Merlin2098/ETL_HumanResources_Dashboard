# Skill: decision_process_flow

The agent follows a structured decision-making process.

Rules:
- Receives and validates request format
- Loads context (treemap + dependencies)
- Analyzes impact and traces dependencies
- Evaluates options and scores by risk/complexity/reversibility
- Generates plan with atomic actions
- Assesses risks and documents invariants
- Validates output against schema
- Persists plan before responding
