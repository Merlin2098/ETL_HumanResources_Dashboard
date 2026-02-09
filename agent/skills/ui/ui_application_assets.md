# Skill: ui_application_assets

The agent manages application assets (icons, images, fonts) in a centralized, cross-platform manner.

## Responsibility
Centralize all application assets and ensure they are accessible across platforms without hardcoded paths.

## Rules
- Include `app.ico` in every build for Windows applications
- Centralize all assets (icons, images, fonts) in a dedicated directory
- Avoid hardcoded paths; use relative or computed paths
- Ensure cross-platform compatibility (Windows, macOS, Linux)
- Include assets in distribution packages (e.g., PyInstaller)
- Version control all assets
- Document asset requirements and formats

## Implementation Guidelines

### Asset Directory Structure
```
project/
├── src/
│   └── main.py
├── assets/
│   ├── icons/
│   │   ├── app.ico          # Windows icon
│   │   ├── app.icns         # macOS icon
│   │   └── app.png          # Linux icon / fallback
│   ├── images/
│   │   ├── logo.png
│   │   ├── splash.png
│   │   └── backgrounds/
│   ├── fonts/
│   │   └── custom_font.ttf
│   └── styles/
│       └── theme.qss        # Qt stylesheet
└── README.md
```

### Path Resolution

**Cross-Platform Asset Loading:**
```python
import os
import sys
from pathlib import Path

def get_asset_path(relative_path):
    """
    Get absolute path to asset, works for dev and PyInstaller builds.
    
    Args:
        relative_path: Path relative to assets directory
        
    Returns:
        Absolute path to asset
    """
    # PyInstaller creates a temp folder and stores path in _MEIPASS
    if getattr(sys, 'frozen', False):
        base_path = Path(sys._MEIPASS)
    else:
        base_path = Path(__file__).parent.parent
    
    asset_path = base_path / 'assets' / relative_path
    
    if not asset_path.exists():
        raise FileNotFoundError(f"Asset not found: {asset_path}")
    
    return str(asset_path)

# Usage
icon_path = get_asset_path('icons/app.ico')
logo_path = get_asset_path('images/logo.png')
```

### Application Icon Setup

**Tkinter:**
```python
import tkinter as tk

root = tk.Tk()
root.title("MyApp")

# Set window icon (cross-platform)
try:
    icon_path = get_asset_path('icons/app.ico')
    root.iconbitmap(icon_path)
except Exception as e:
    print(f"Could not load icon: {e}")
```

**PySide6:**
```python
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtGui import QIcon

app = QApplication(sys.argv)
window = QMainWindow()

# Set application icon
icon_path = get_asset_path('icons/app.png')
app.setWindowIcon(QIcon(icon_path))
```

### PyInstaller Asset Inclusion

**spec file configuration:**
```python
# myapp.spec
a = Analysis(
    ['src/main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('assets/icons/*', 'assets/icons'),
        ('assets/images/*', 'assets/images'),
        ('assets/fonts/*', 'assets/fonts'),
    ],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MyApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon='assets/icons/app.ico',  # Application icon
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MyApp',
)
```

### Asset Manager Class

```python
class AssetManager:
    """Centralized asset management"""
    
    def __init__(self, assets_dir='assets'):
        self.base_path = self._get_base_path()
        self.assets_dir = self.base_path / assets_dir
        self._validate_assets()
    
    def _get_base_path(self):
        if getattr(sys, 'frozen', False):
            return Path(sys._MEIPASS)
        return Path(__file__).parent.parent
    
    def _validate_assets(self):
        """Ensure critical assets exist"""
        if not self.assets_dir.exists():
            raise FileNotFoundError(f"Assets directory not found: {self.assets_dir}")
    
    def get_icon(self, name):
        """Get icon path"""
        return str(self.assets_dir / 'icons' / name)
    
    def get_image(self, name):
        """Get image path"""
        return str(self.assets_dir / 'images' / name)
    
    def get_font(self, name):
        """Get font path"""
        return str(self.assets_dir / 'fonts' / name)
    
    def get_style(self, name):
        """Get stylesheet path"""
        return str(self.assets_dir / 'styles' / name)

# Usage
assets = AssetManager()
icon = assets.get_icon('app.ico')
logo = assets.get_image('logo.png')
```

### Icon Format Requirements

| Platform | Format | Recommended Sizes |
|----------|--------|-------------------|
| Windows  | .ico   | 16x16, 32x32, 48x48, 256x256 |
| macOS    | .icns  | 16x16 to 1024x1024 |
| Linux    | .png   | 48x48, 128x128, 256x256 |

### Creating Multi-Size Icons

**Using Pillow:**
```python
from PIL import Image

def create_ico(source_png, output_ico):
    """Create Windows .ico from PNG"""
    img = Image.open(source_png)
    sizes = [(16, 16), (32, 32), (48, 48), (256, 256)]
    img.save(output_ico, format='ICO', sizes=sizes)

create_ico('logo.png', 'app.ico')
```

## Asset Checklist

### Required Assets
- [ ] `app.ico` - Windows application icon
- [ ] `app.icns` - macOS application icon (if targeting macOS)
- [ ] `app.png` - Fallback icon / Linux icon
- [ ] `logo.png` - Application logo for splash/about
- [ ] `splash.png` - Splash screen background (if applicable)

### Optional Assets
- [ ] Custom fonts
- [ ] Theme stylesheets
- [ ] Background images
- [ ] Button icons
- [ ] Status icons

## Anti-Patterns to Avoid
- ❌ Hardcoded absolute paths: `C:/Users/Dev/project/assets/icon.ico`
- ❌ Missing assets in distribution builds
- ❌ Platform-specific paths: `assets\\icons\\app.ico` (Windows only)
- ❌ No fallback for missing assets
- ❌ Assets scattered across multiple directories

## Best Practices
- ✅ Use path resolution functions for all asset access
- ✅ Validate critical assets at startup
- ✅ Provide fallbacks for optional assets
- ✅ Test asset loading in both dev and built environments
- ✅ Document asset requirements in README
- ✅ Use version control for all assets

## Notes
- Asset management is critical for professional applications
- Missing icons create poor first impressions
- Cross-platform compatibility requires careful path handling
- PyInstaller and other packagers need explicit asset inclusion
- Consider asset optimization (compression, format conversion)
