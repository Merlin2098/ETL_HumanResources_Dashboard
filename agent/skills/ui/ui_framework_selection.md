# Skill: ui_framework_selection

The agent selects the appropriate UI framework based on project maturity and requirements.

## Responsibility
Determine whether to use Tkinter for rapid prototyping or PySide6 for production-ready applications.

## Rules
- Use **Tkinter** for MVP prototypes, proof-of-concepts, and rapid iteration
- Use **PySide6** for final, production-ready builds requiring polish and performance
- Do not mix frameworks in the same project
- Document the framework choice in project README or configuration
- Consider migration path from Tkinter to PySide6 when starting with MVP

## Decision Criteria

### Use Tkinter when:
- Building a quick prototype or MVP
- Testing UI concepts rapidly
- No complex styling or theming required
- Deployment complexity must be minimal
- Development speed is the priority

### Use PySide6 when:
- Building production-ready applications
- Advanced theming and styling are required
- Cross-platform consistency is critical
- Professional look and feel is mandatory
- Long-term maintenance is expected

## Example Usage

**Prototype Phase:**
```python
# Use Tkinter for rapid MVP development
import tkinter as tk
from tkinter import ttk
```

**Production Phase:**
```python
# Migrate to PySide6 for final build
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtCore import Qt
```

## Notes
- Framework selection should be made at project inception
- Avoid framework switching mid-development unless planned
- Ensure all team members are aware of the chosen framework
