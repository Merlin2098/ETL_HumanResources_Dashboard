# Skill: ui_identity_policy

The agent implements consistent application identity presentation without redundant information.

## Responsibility
Display application identity (name, version, author) in a non-redundant, professional manner using dedicated widgets.

## Rules
- Do not repeat the app name as a subtitle or secondary header
- Use an Author Info Widget to show identity information
- Application name should appear once, prominently (e.g., window title or main header)
- Version information should be accessible but not intrusive
- Author/company information should be in About dialog or footer
- Avoid cluttering the main UI with redundant branding

## Implementation Guidelines

### Identity Placement Strategy

#### Window Title
```python
# Primary location for app name
root.title("MyApp v1.2.3")
```

#### Main Header (Optional)
- Use if window title is not prominent enough
- Include logo/icon alongside name
- Do not duplicate in subtitle

#### Author Info Widget
- Dedicated widget for author, company, copyright
- Typically placed in:
  - About dialog
  - Footer/status bar
  - Help menu

### Information Hierarchy
1. **Primary**: Application name (window title or main header)
2. **Secondary**: Version number (subtle, non-intrusive)
3. **Tertiary**: Author/company (About dialog or footer)

## Example Usage

**Bad - Redundant Identity:**
```python
# ❌ Redundant and cluttered
root.title("MyApp")
header_label = tk.Label(root, text="MyApp", font=("Arial", 24))
subtitle_label = tk.Label(root, text="MyApp - Data Processor")
footer_label = tk.Label(root, text="MyApp v1.0 by John Doe")
```

**Good - Clean Identity:**
```python
# ✅ Clean and professional
root.title("MyApp v1.2.3")

# Main UI - no redundant app name
header_label = tk.Label(root, text="Data Processor", font=("Arial", 18))

# Author info in dedicated widget/dialog
class AuthorInfoWidget(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        info = "© 2026 John Doe\nVersion 1.2.3"
        tk.Label(self, text=info, font=("Arial", 8)).pack()

# Place in footer or About dialog
author_widget = AuthorInfoWidget(root)
author_widget.pack(side="bottom", fill="x")
```

**About Dialog Pattern:**
```python
def show_about_dialog():
    dialog = tk.Toplevel(root)
    dialog.title("About MyApp")
    
    tk.Label(dialog, text="MyApp", font=("Arial", 16, "bold")).pack(pady=10)
    tk.Label(dialog, text="Version 1.2.3").pack()
    tk.Label(dialog, text="© 2026 John Doe").pack()
    tk.Label(dialog, text="A professional data processing tool").pack(pady=10)
    
    tk.Button(dialog, text="Close", command=dialog.destroy).pack(pady=10)

# Accessible via Help menu
menu_bar = tk.Menu(root)
help_menu = tk.Menu(menu_bar, tearoff=0)
help_menu.add_command(label="About", command=show_about_dialog)
menu_bar.add_cascade(label="Help", menu=help_menu)
root.config(menu=menu_bar)
```

**Footer Author Widget:**
```python
class FooterAuthorWidget(tk.Frame):
    def __init__(self, parent, app_name, version, author):
        super().__init__(parent, bg="#2C2C2C", height=25)
        
        info_text = f"{app_name} v{version} | © {author}"
        label = tk.Label(
            self, 
            text=info_text, 
            bg="#2C2C2C", 
            fg="#CCCCCC",
            font=("Arial", 8)
        )
        label.pack(side="right", padx=10)

# Usage
footer = FooterAuthorWidget(root, "MyApp", "1.2.3", "2026 John Doe")
footer.pack(side="bottom", fill="x")
```

## Anti-Patterns to Avoid
- ❌ App name in window title AND main header AND subtitle
- ❌ Large, intrusive branding that takes up valuable screen space
- ❌ Version number in multiple locations
- ❌ Author info cluttering the main workspace

## Best Practices
- ✅ Single, prominent app name placement
- ✅ Subtle version indicator
- ✅ Author info in dedicated, non-intrusive location
- ✅ Professional, clean presentation
- ✅ Easy access to full identity via About dialog

## Notes
- Users care about functionality, not repeated branding
- Professional apps show restraint in self-promotion
- Identity information should be accessible but not intrusive
- Consider user's workflow and screen real estate
