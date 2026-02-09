# Skill: protected_file_validation

The agent validates file targets against protected file blacklists before any operation.

Rules:
- Checks all target files against the protected files blacklist
- Rejects operations immediately if protected files are detected
- Returns structured violation reports with file paths and reasons
- Applies to both planning and execution phases
