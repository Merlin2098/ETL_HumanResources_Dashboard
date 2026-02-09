# Skill: ui_theme_binding

The agent implements reactive theme systems where widgets automatically respond to theme changes.

## Responsibility
Establish a centralized theme management system that widgets subscribe to, enabling dynamic theme switching without code duplication.

## Rules
- Widgets subscribe to theme changes and re-render automatically
- Theme logic must live outside widgets in a centralized theme manager
- Avoid duplicating style logic across widgets
- Support runtime theme switching (e.g., light/dark mode)
- Theme changes should propagate to all subscribed widgets
- Use observer pattern or event-driven architecture

## Implementation Guidelines

### Theme Manager Structure
1. **Centralized Storage**: Single source of truth for all theme values
2. **Subscription System**: Widgets register for theme updates
3. **Change Notification**: Broadcast theme changes to subscribers
4. **Validation**: Ensure theme values are valid before applying

### Theme Properties
- Colors (background, foreground, accent, etc.)
- Fonts (family, size, weight)
- Spacing (padding, margins)
- Border styles
- Animation timings

## Example Usage

**Theme Manager:**
```python
class ThemeManager:
    def __init__(self):
        self.subscribers = []
        self.current_theme = "light"
        self.themes = {
            "light": {
                "bg": "#FFFFFF",
                "fg": "#000000",
                "accent": "#007ACC"
            },
            "dark": {
                "bg": "#1E1E1E",
                "fg": "#D4D4D4",
                "accent": "#0098FF"
            }
        }
    
    def subscribe(self, callback):
        self.subscribers.append(callback)
    
    def set_theme(self, theme_name):
        if theme_name in self.themes:
            self.current_theme = theme_name
            self._notify_subscribers()
    
    def _notify_subscribers(self):
        theme_data = self.themes[self.current_theme]
        for callback in self.subscribers:
            callback(theme_data)
    
    def get_theme(self):
        return self.themes[self.current_theme]
```

**Widget with Theme Subscription:**
```python
class ThemedButton(tk.Button):
    def __init__(self, parent, theme_manager, **kwargs):
        super().__init__(parent, **kwargs)
        self.theme_manager = theme_manager
        self.theme_manager.subscribe(self.apply_theme)
        self.apply_theme(self.theme_manager.get_theme())
    
    def apply_theme(self, theme):
        self.configure(
            bg=theme["accent"],
            fg=theme["fg"],
            activebackground=theme["bg"]
        )
```

**Usage:**
```python
theme_mgr = ThemeManager()
button1 = ThemedButton(root, theme_mgr, text="Click Me")
button2 = ThemedButton(root, theme_mgr, text="Another")

# Both buttons update automatically
theme_mgr.set_theme("dark")
```

## Anti-Patterns to Avoid
- ❌ Hardcoding colors in widgets: `bg="#FFFFFF"`
- ❌ Duplicating theme logic: Each widget defines its own colors
- ❌ Manual updates: Calling `update_theme()` on each widget individually
- ❌ No centralization: Theme values scattered across files

## Notes
- Theme binding reduces maintenance burden significantly
- Enables features like user-selectable themes
- Improves consistency across the application
- Consider persisting user theme preference
