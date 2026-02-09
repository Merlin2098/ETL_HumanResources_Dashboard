# Skill: ui_layout_proportionality

The agent creates adaptive, proportional layouts that respond to window resizing and different screen resolutions.

## Responsibility
Ensure UI layouts are responsive and proportional rather than fixed-pixel, supporting multiple resolutions and DPI scaling.

## Rules
- Layouts should be proportional and adaptive, not pixel-fixed
- Handle window resizing gracefully
- Support multiple resolutions and aspect ratios
- Account for DPI scaling on high-resolution displays
- Use relative units (percentages, weights) over absolute pixels
- Test on different screen sizes and resolutions

## Implementation Guidelines

### Layout Strategies

#### Tkinter
- Use `relwidth`, `relheight`, `relx`, `rely` for proportional placement
- Use `weight` in grid layout for responsive columns/rows
- Avoid fixed `width` and `height` where possible

#### PySide6
- Use layouts: `QVBoxLayout`, `QHBoxLayout`, `QGridLayout`
- Set size policies: `QSizePolicy.Expanding`, `QSizePolicy.Preferred`
- Use stretch factors for proportional distribution
- Leverage `QSplitter` for user-adjustable sections

### Responsive Design Principles
1. **Minimum Sizes**: Set minimum dimensions to prevent unusable layouts
2. **Maximum Sizes**: Set maximum dimensions where appropriate
3. **Aspect Ratios**: Maintain aspect ratios for critical elements
4. **Breakpoints**: Adjust layout for very small or very large windows
5. **Font Scaling**: Scale fonts proportionally with window size

## Example Usage

**Tkinter - Fixed Layout (Bad):**
```python
# ❌ Breaks on different resolutions
button = tk.Button(root, text="Click", width=200, height=50)
button.place(x=100, y=100)
```

**Tkinter - Proportional Layout (Good):**
```python
# ✅ Adapts to window size
button = tk.Button(root, text="Click")
button.place(relx=0.5, rely=0.5, anchor="center", 
             relwidth=0.3, relheight=0.1)
```

**Tkinter - Grid with Weights:**
```python
# ✅ Responsive grid layout
root.grid_rowconfigure(0, weight=1)
root.grid_rowconfigure(1, weight=3)
root.grid_columnconfigure(0, weight=1)
root.grid_columnconfigure(1, weight=2)

header = tk.Frame(root, bg="blue")
header.grid(row=0, column=0, columnspan=2, sticky="nsew")

sidebar = tk.Frame(root, bg="gray")
sidebar.grid(row=1, column=0, sticky="nsew")

content = tk.Frame(root, bg="white")
content.grid(row=1, column=1, sticky="nsew")
```

**PySide6 - Proportional Layout:**
```python
from PySide6.QtWidgets import QVBoxLayout, QHBoxLayout, QSplitter
from PySide6.QtCore import Qt

# ✅ Responsive layout with splitter
splitter = QSplitter(Qt.Horizontal)
splitter.addWidget(sidebar_widget)
splitter.addWidget(content_widget)
splitter.setStretchFactor(0, 1)  # Sidebar gets 1 part
splitter.setStretchFactor(1, 3)  # Content gets 3 parts

main_layout = QVBoxLayout()
main_layout.addWidget(header_widget, stretch=1)
main_layout.addWidget(splitter, stretch=9)
```

## DPI Scaling Considerations

**Tkinter:**
```python
# Get DPI scaling factor
scaling = root.winfo_fpixels('1i') / 96.0

# Apply to font sizes
base_font_size = 10
scaled_font_size = int(base_font_size * scaling)
```

**PySide6:**
```python
# Qt handles DPI scaling automatically
# Enable high DPI support
QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
```

## Testing Checklist
- [ ] Test on 1920x1080 (Full HD)
- [ ] Test on 1366x768 (Common laptop)
- [ ] Test on 3840x2160 (4K)
- [ ] Test with 125%, 150%, 200% DPI scaling
- [ ] Test window resize from minimum to maximum
- [ ] Test on different aspect ratios (16:9, 16:10, 4:3)

## Notes
- Proportional layouts require more planning but provide better UX
- Consider using responsive design frameworks or libraries
- Document minimum supported resolution in project README
