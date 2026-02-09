# Skill: ui_splash_and_lazy_loading

The agent implements splash screens and lazy loading to improve perceived performance and user experience during application startup.

## Responsibility
Display a splash screen during initialization while loading heavy resources, configuration, and themes in the background.

## Rules
- Always use a splash screen during app startup for non-trivial applications
- Load heavy resources, configuration, and themes lazily
- The main UI should only render when fully ready
- Splash screen should show loading progress when possible
- Minimum splash display time to avoid flicker (e.g., 500ms)
- Gracefully handle initialization errors with user feedback

## Implementation Guidelines

### Splash Screen Requirements
1. **Lightweight**: Minimal dependencies, fast to display
2. **Informative**: Show app name, logo, version
3. **Progress Indicator**: Loading bar or spinner
4. **Status Messages**: Optional text showing current loading step
5. **Professional**: Consistent with app branding

### Lazy Loading Strategy
1. **Phase 1 - Immediate**: Show splash screen
2. **Phase 2 - Background**: Load configuration, themes, assets
3. **Phase 3 - Heavy Resources**: Database connections, external APIs
4. **Phase 4 - UI Construction**: Build main window
5. **Phase 5 - Transition**: Hide splash, show main UI

### Loading Priorities
- **Critical**: Configuration, theme, core modules
- **Important**: Database connections, user preferences
- **Deferred**: Non-essential features, plugins, help documentation

## Example Usage

**Tkinter Splash Screen:**
```python
import tkinter as tk
from tkinter import ttk
import threading
import time

class SplashScreen:
    def __init__(self):
        self.root = tk.Tk()
        self.root.overrideredirect(True)  # Remove window decorations
        self.root.attributes('-topmost', True)
        
        # Center on screen
        width, height = 400, 300
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        
        # Content
        tk.Label(
            self.root, 
            text="MyApp", 
            font=("Arial", 24, "bold")
        ).pack(pady=20)
        
        tk.Label(
            self.root, 
            text="Version 1.2.3", 
            font=("Arial", 10)
        ).pack()
        
        self.status_label = tk.Label(
            self.root, 
            text="Initializing...", 
            font=("Arial", 9)
        )
        self.status_label.pack(pady=20)
        
        self.progress = ttk.Progressbar(
            self.root, 
            mode='indeterminate', 
            length=300
        )
        self.progress.pack(pady=10)
        self.progress.start()
        
    def update_status(self, message):
        self.status_label.config(text=message)
        self.root.update()
    
    def close(self):
        self.root.destroy()

def load_application(splash):
    """Background loading function"""
    try:
        splash.update_status("Loading configuration...")
        time.sleep(0.5)  # Simulate loading
        # load_config()
        
        splash.update_status("Initializing theme...")
        time.sleep(0.5)
        # load_theme()
        
        splash.update_status("Connecting to database...")
        time.sleep(0.5)
        # connect_database()
        
        splash.update_status("Building interface...")
        time.sleep(0.5)
        # build_main_ui()
        
        return True
    except Exception as e:
        splash.update_status(f"Error: {str(e)}")
        time.sleep(3)
        return False

def main():
    # Show splash
    splash = SplashScreen()
    
    # Load in background
    def background_load():
        success = load_application(splash)
        
        # Ensure minimum display time
        time.sleep(0.5)
        
        # Close splash
        splash.root.after(0, splash.close)
        
        if success:
            # Show main window
            splash.root.after(0, show_main_window)
    
    thread = threading.Thread(target=background_load, daemon=True)
    thread.start()
    
    splash.root.mainloop()

def show_main_window():
    main_window = tk.Tk()
    main_window.title("MyApp v1.2.3")
    main_window.geometry("800x600")
    # Build main UI...
    main_window.mainloop()

if __name__ == "__main__":
    main()
```

**PySide6 Splash Screen:**
```python
from PySide6.QtWidgets import QApplication, QSplashScreen, QMainWindow
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QPixmap
import sys

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MyApp v1.2.3")
        self.setGeometry(100, 100, 800, 600)

def load_resources(splash):
    """Simulated resource loading"""
    steps = [
        "Loading configuration...",
        "Initializing theme...",
        "Connecting to database...",
        "Building interface..."
    ]
    
    for i, step in enumerate(steps):
        splash.showMessage(
            step,
            Qt.AlignBottom | Qt.AlignCenter,
            Qt.white
        )
        QApplication.processEvents()
        # Simulate loading time
        QTimer.singleShot(500, lambda: None)

def main():
    app = QApplication(sys.argv)
    
    # Create splash screen
    pixmap = QPixmap(400, 300)
    pixmap.fill(Qt.darkGray)
    splash = QSplashScreen(pixmap)
    splash.show()
    
    # Load resources
    load_resources(splash)
    
    # Create main window
    window = MainWindow()
    
    # Close splash and show main window
    splash.finish(window)
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
```

## Progressive Loading Pattern

```python
class LazyLoader:
    def __init__(self, splash):
        self.splash = splash
        self.loaded = {
            'config': False,
            'theme': False,
            'database': False,
            'ui': False
        }
    
    def load_all(self):
        self.load_config()
        self.load_theme()
        self.load_database()
        self.load_ui()
        return all(self.loaded.values())
    
    def load_config(self):
        self.splash.update_status("Loading configuration...")
        # Load config
        self.loaded['config'] = True
    
    def load_theme(self):
        if not self.loaded['config']:
            raise Exception("Config must be loaded first")
        self.splash.update_status("Loading theme...")
        # Load theme
        self.loaded['theme'] = True
    
    # ... other loading methods
```

## Error Handling

```python
def safe_load(splash):
    try:
        loader = LazyLoader(splash)
        success = loader.load_all()
        return success
    except Exception as e:
        splash.update_status(f"Startup failed: {str(e)}")
        # Show error dialog
        messagebox.showerror("Startup Error", str(e))
        return False
```

## Notes
- Splash screens significantly improve perceived performance
- Users tolerate loading better when they see progress
- Lazy loading reduces initial startup time
- Consider caching to speed up subsequent launches
- Test startup performance on slower hardware
