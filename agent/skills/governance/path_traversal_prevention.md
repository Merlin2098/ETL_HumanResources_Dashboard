# Skill: path_traversal_prevention

The agent validates all file paths against directory traversal attacks.

Rules:
- Resolves all file paths to absolute form
- Validates all targets are within project root
- Rejects path traversal attempts (../, symlinks outside scope)
- Fails with security error on violation
