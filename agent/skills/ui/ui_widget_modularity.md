# Skill: ui_widget_modularity

The agent creates self-contained, reusable widgets that are decoupled from global layout logic.

## Responsibility
Ensure each widget is modular, composable, and independent of its parent container or application context.

## Rules
- Each widget must be self-contained and reusable
- Widgets should not know about or depend on the global layout
- Avoid hardcoding colors, sizes, or themes within widgets
- Use dependency injection for configuration and styling
- Widgets should expose clear, minimal interfaces
- Widgets should be testable in isolation

## Implementation Guidelines

### Widget Structure
1. **Encapsulation**: Widget logic stays within the widget class
2. **Configuration**: Accept configuration via constructor or setter methods
3. **Events**: Emit signals/events rather than directly calling parent methods
4. **Styling**: Accept style parameters; do not hardcode appearance

### Anti-Patterns to Avoid
- ❌ Hardcoded dimensions: `width=800, height=600`
- ❌ Direct parent access: `self.parent.update_status()`
- ❌ Global state dependency: `from config import THEME`
- ❌ Layout assumptions: `grid(row=0, column=0)`

### Best Practices
- ✅ Proportional sizing: `relwidth=0.8, relheight=0.6`
- ✅ Event emission: `self.on_change.emit(value)`
- ✅ Injected styling: `__init__(self, style_config)`
- ✅ Layout agnostic: Widget doesn't call `.grid()` or `.pack()` on itself

## Example Usage

**Bad - Tightly Coupled Widget:**
```python
class StatusBar(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg="#333333", height=30)  # Hardcoded
        self.pack(side="bottom", fill="x")  # Assumes layout
        self.parent.status = self  # Tight coupling
```

**Good - Modular Widget:**
```python
class StatusBar(tk.Frame):
    def __init__(self, parent, style=None):
        super().__init__(parent)
        self.style = style or {}
        self.configure(bg=self.style.get("bg", "#333333"))
        # Parent controls layout, not the widget
        
    def set_message(self, message):
        # Widget exposes clear interface
        self.label.config(text=message)
```

## Notes
- Modularity enables reuse across different projects
- Well-designed widgets can be extracted into shared libraries
- Testing is simplified when widgets are self-contained
