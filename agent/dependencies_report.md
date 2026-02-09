# Project Dependency Analysis

> **Purpose**: This document maps dependencies between Python modules, configuration files, and external libraries. Use it to understand the architecture and relationships between components.

## Executive Summary

- **Total Python modules**: 31
- **Project entry points**: 31
- **Configuration files**: 15
- **Unique external libraries**: 37

---

## 1. Project Entry Points

These modules are the **main scripts** that initiate execution (they are not imported by other modules):

### `agent_tools.analyze_dependencies`

**Direct dependencies**: 7 (0 modules, 0 configs, 7 libraries)

- **External libraries**: `re`, `os`, `ast`, `pathspec`, `sys` (+2 more)

### `agent_tools.generate_rollback`

**Direct dependencies**: 12 (0 modules, 1 configs, 11 libraries)

- **Config files**: `rollback_manifest.json`
- **External libraries**: `shutil`, `typing`, `hashlib`, `dataclasses`, `json` (+6 more)

### `agent_tools.schema_validator`

**Direct dependencies**: 13 (0 modules, 4 configs, 9 libraries)

- **Config files**: `task_envelope.schema.json`, `system_config.schema.yaml`, `task_plan.schema.json`, `execution_report.schema.json`
- **External libraries**: `typing`, `yaml`, `jsonschema`, `json`, `sys` (+4 more)

### `agent_tools.treemap`

**Direct dependencies**: 3 (0 modules, 0 configs, 3 libraries)

- **External libraries**: `sys`, `os`, `pathspec`

### `agent_tools.validate_message`

**Direct dependencies**: 14 (0 modules, 4 configs, 10 libraries)

- **Config files**: `task_envelope.schema.json`, `system_config.schema.yaml`, `task_plan.schema.json`, `execution_report.schema.json`
- **External libraries**: `typing`, `yaml`, `hashlib`, `jsonschema`, `json` (+5 more)

### `debug.debug_ppt`

**Direct dependencies**: 8 (0 modules, 2 configs, 6 libraries)

- **Config files**: `🎯 RECOMENDACIONES PARA mapping.yaml`, `   3. Debes usar el NOMBRE REAL de la forma en mapping.yaml`
- **External libraries**: `re`, `traceback`, `tkinter`, `os`, `xml` (+1 more)

### `generar_exe`

**Direct dependencies**: 8 (0 modules, 3 configs, 5 libraries)

- **Config files**: `          │       └── theme_dark.json`, `mapping.yaml`, `          │   ├── mapping.yaml`
- **External libraries**: `PyInstaller`, `shutil`, `traceback`, `sys`, `pathlib`

### `main`

**Direct dependencies**: 7 (5 modules, 0 configs, 2 libraries)

- **Internal modules**: `utils`, `utils`, `utils`, `ui`, `ui`
- **External libraries**: `PySide6`, `sys`

### `src.__init__`

**Direct dependencies**: 3 (3 modules, 0 configs, 0 libraries)

- **Internal modules**: `excel_reader`, `ppt_generator`, `ppt_converter`

### `src.excel_reader`

**Direct dependencies**: 3 (0 modules, 0 configs, 3 libraries)

- **External libraries**: `typing`, `pathlib`, `openpyxl`

### `src.pdf_worker`

**Direct dependencies**: 6 (0 modules, 0 configs, 6 libraries)

- **External libraries**: `time`, `subprocess`, `os`, `platform`, `pathlib` (+1 more)

### `src.ppt_converter`

**Direct dependencies**: 8 (0 modules, 0 configs, 8 libraries)

- **External libraries**: `time`, `subprocess`, `typing`, `os`, `platform` (+3 more)

### `src.ppt_generator`

**Direct dependencies**: 9 (1 modules, 0 configs, 8 libraries)

- **Internal modules**: `src`
- **External libraries**: `re`, `multiprocessing`, `typing`, `pptx`, `functools` (+3 more)

### `src.ppt_worker`

**Direct dependencies**: 4 (0 modules, 0 configs, 4 libraries)

- **External libraries**: `re`, `pptx`, `pathlib`, `typing`

### `ui.__init__`

**Direct dependencies**: 2 (2 modules, 0 configs, 0 libraries)

- **Internal modules**: `main_window`, `splash_screen`

### `ui.main_window`

**Direct dependencies**: 15 (10 modules, 0 configs, 5 libraries)

- **Internal modules**: `ui`, `ui`, `ui`, `ui`, `ui`, `ui`, `utils`, `utils`, `src`, `src`
- **External libraries**: `subprocess`, `platform`, `PySide6`, `datetime`, `pathlib`

### `ui.splash_screen`

**Direct dependencies**: 2 (1 modules, 0 configs, 1 libraries)

- **Internal modules**: `utils`
- **External libraries**: `PySide6`

### `ui.widgets.__init__`

**Direct dependencies**: 4 (4 modules, 0 configs, 0 libraries)

- **Internal modules**: `monitoring_panel`, `console_widget`, `author_info_widget`, `template_selector_widget`

### `ui.widgets.author_info_widget`

**Direct dependencies**: 1 (0 modules, 0 configs, 1 libraries)

- **External libraries**: `PySide6`

### `ui.widgets.console_widget`

**Direct dependencies**: 2 (0 modules, 0 configs, 2 libraries)

- **External libraries**: `PySide6`, `datetime`

### `ui.widgets.monitoring_panel`

**Direct dependencies**: 2 (0 modules, 0 configs, 2 libraries)

- **External libraries**: `PySide6`, `datetime`

### `ui.widgets.template_selector_widget`

**Direct dependencies**: 1 (0 modules, 0 configs, 1 libraries)

- **External libraries**: `PySide6`

### `ui.workers.__init__`

**Direct dependencies**: 2 (2 modules, 0 configs, 0 libraries)

- **Internal modules**: `generation_worker`, `conversion_worker`

### `ui.workers.conversion_worker`

**Direct dependencies**: 7 (2 modules, 0 configs, 5 libraries)

- **Internal modules**: `src`, `utils`
- **External libraries**: `time`, `typing`, `glob`, `PySide6`, `pathlib`

### `ui.workers.generation_worker`

**Direct dependencies**: 7 (3 modules, 0 configs, 4 libraries)

- **Internal modules**: `src`, `src`, `utils`
- **External libraries**: `PySide6`, `time`, `pathlib`, `typing`

### `utils.__init__`

**Direct dependencies**: 0 (0 modules, 0 configs, 0 libraries)


### `utils.config_manager`

**Direct dependencies**: 8 (1 modules, 0 configs, 7 libraries)

- **Internal modules**: `utils`
- **External libraries**: `re`, `typing`, `yaml`, `dataclasses`, `json` (+2 more)

### `utils.logger`

**Direct dependencies**: 5 (1 modules, 0 configs, 4 libraries)

- **Internal modules**: `utils`
- **External libraries**: `multiprocessing`, `datetime`, `pathlib`, `logging`

### `utils.path_manager`

**Direct dependencies**: 6 (0 modules, 2 configs, 4 libraries)

- **Config files**: `mapping.yaml`, `paths_cache.json`
- **External libraries**: `sys`, `os`, `pathlib`, `typing`

### `utils.power_manager`

**Direct dependencies**: 4 (0 modules, 0 configs, 4 libraries)

- **External libraries**: `ctypes`, `os`, `subprocess`, `platform`

### `utils.theme_manager`

**Direct dependencies**: 10 (1 modules, 4 configs, 5 libraries)

- **Internal modules**: `utils`
- **Config files**: `theme_config.json`, `theme_*.json`, `Carga la configuración del usuario desde theme_config.json`, `Guarda la configuración del usuario en theme_config.json`
- **External libraries**: `typing`, `json`, `PySide6`, `datetime`, `pathlib`

---

## 2. Full Dependency Map

This tree shows **all recursive dependencies** for each entry point:

**Legend**:
- 📦 Project Python Module
- 📄 Configuration File (JSON, YAML, SQL, etc.)
- 🔗 External Library (installed via pip)

### agent_tools.analyze_dependencies

```
agent_tools.analyze_dependencies
├── 🔗 re
├── 🔗 os
├── 🔗 ast
├── 🔗 pathspec
├── 🔗 sys
├── 🔗 collections
└── 🔗 pathlib
```

### agent_tools.generate_rollback

```
agent_tools.generate_rollback
├── 📄 rollback_manifest.json
├── 🔗 shutil
├── 🔗 typing
├── 🔗 hashlib
├── 🔗 dataclasses
├── 🔗 json
├── 🔗 sys
├── 🔗 argparse
├── 🔗 __future__
├── 🔗 datetime
├── 🔗 uuid
└── 🔗 pathlib
```

### agent_tools.schema_validator

```
agent_tools.schema_validator
├── 📄 task_envelope.schema.json
├── 📄 system_config.schema.yaml
├── 📄 task_plan.schema.json
├── 📄 execution_report.schema.json
├── 🔗 typing
├── 🔗 yaml
├── 🔗 jsonschema
├── 🔗 json
├── 🔗 sys
├── 🔗 argparse
├── 🔗 __future__
├── 🔗 datetime
└── 🔗 pathlib
```

### agent_tools.treemap

```
agent_tools.treemap
├── 🔗 sys
├── 🔗 os
└── 🔗 pathspec
```

### agent_tools.validate_message

```
agent_tools.validate_message
├── 📄 task_envelope.schema.json
├── 📄 system_config.schema.yaml
├── 📄 task_plan.schema.json
├── 📄 execution_report.schema.json
├── 🔗 typing
├── 🔗 yaml
├── 🔗 hashlib
├── 🔗 jsonschema
├── 🔗 json
├── 🔗 sys
├── 🔗 argparse
├── 🔗 __future__
├── 🔗 datetime
└── 🔗 pathlib
```

### debug.debug_ppt

```
debug.debug_ppt
├── 📄 🎯 RECOMENDACIONES PARA mapping.yaml
├── 📄    3. Debes usar el NOMBRE REAL de la forma en mapping.yaml
├── 🔗 re
├── 🔗 traceback
├── 🔗 tkinter
├── 🔗 os
├── 🔗 xml
└── 🔗 zipfile
```

### generar_exe

```
generar_exe
├── 📄           │       └── theme_dark.json
├── 📄 mapping.yaml
├── 📄           │   ├── mapping.yaml
├── 🔗 PyInstaller
├── 🔗 shutil
├── 🔗 traceback
├── 🔗 sys
└── 🔗 pathlib
```

### main

```
main
├── 📦 utils
├── 📦 utils
├── 📦 utils
├── 📦 ui
├── 📦 ui
├── 🔗 PySide6
└── 🔗 sys
```

### src.__init__

```
src.__init__
├── 📦 excel_reader
├── 📦 ppt_generator
└── 📦 ppt_converter
```

### src.excel_reader

```
src.excel_reader
├── 🔗 typing
├── 🔗 pathlib
└── 🔗 openpyxl
```

### src.pdf_worker

```
src.pdf_worker
├── 🔗 time
├── 🔗 subprocess
├── 🔗 os
├── 🔗 platform
├── 🔗 pathlib
└── 🔗 win32com
```

### src.ppt_converter

```
src.ppt_converter
├── 🔗 time
├── 🔗 subprocess
├── 🔗 typing
├── 🔗 os
├── 🔗 platform
├── 🔗 threading
├── 🔗 pathlib
└── 🔗 win32com
```

### src.ppt_generator

```
src.ppt_generator
├── 📦 src
├── 🔗 re
├── 🔗 multiprocessing
├── 🔗 typing
├── 🔗 pptx
├── 🔗 functools
├── 🔗 datetime
├── 🔗 pathlib
└── 🔗 concurrent
```

### src.ppt_worker

```
src.ppt_worker
├── 🔗 re
├── 🔗 pptx
├── 🔗 pathlib
└── 🔗 typing
```

### ui.__init__

```
ui.__init__
├── 📦 main_window
└── 📦 splash_screen
```

### ui.main_window

```
ui.main_window
├── 📦 ui
├── 📦 ui
├── 📦 ui
├── 📦 ui
├── 📦 ui
├── 📦 ui
├── 📦 utils
├── 📦 utils
├── 📦 src
├── 📦 src
├── 🔗 subprocess
├── 🔗 platform
├── 🔗 PySide6
├── 🔗 datetime
└── 🔗 pathlib
```

### ui.splash_screen

```
ui.splash_screen
├── 📦 utils
└── 🔗 PySide6
```

### ui.widgets.__init__

```
ui.widgets.__init__
├── 📦 monitoring_panel
├── 📦 console_widget
├── 📦 author_info_widget
└── 📦 template_selector_widget
```

### ui.widgets.author_info_widget

```
ui.widgets.author_info_widget
└── 🔗 PySide6
```

### ui.widgets.console_widget

```
ui.widgets.console_widget
├── 🔗 PySide6
└── 🔗 datetime
```

### ui.widgets.monitoring_panel

```
ui.widgets.monitoring_panel
├── 🔗 PySide6
└── 🔗 datetime
```

### ui.widgets.template_selector_widget

```
ui.widgets.template_selector_widget
└── 🔗 PySide6
```

### ui.workers.__init__

```
ui.workers.__init__
├── 📦 generation_worker
└── 📦 conversion_worker
```

### ui.workers.conversion_worker

```
ui.workers.conversion_worker
├── 📦 src
├── 📦 utils
├── 🔗 time
├── 🔗 typing
├── 🔗 glob
├── 🔗 PySide6
└── 🔗 pathlib
```

### ui.workers.generation_worker

```
ui.workers.generation_worker
├── 📦 src
├── 📦 src
├── 📦 utils
├── 🔗 PySide6
├── 🔗 time
├── 🔗 pathlib
└── 🔗 typing
```

### utils.__init__

```
utils.__init__

```

### utils.config_manager

```
utils.config_manager
├── 📦 utils
├── 🔗 re
├── 🔗 typing
├── 🔗 yaml
├── 🔗 dataclasses
├── 🔗 json
├── 🔗 datetime
└── 🔗 pathlib
```

### utils.logger

```
utils.logger
├── 📦 utils
├── 🔗 multiprocessing
├── 🔗 datetime
├── 🔗 pathlib
└── 🔗 logging
```

### utils.path_manager

```
utils.path_manager
├── 📄 mapping.yaml
├── 📄 paths_cache.json
├── 🔗 sys
├── 🔗 os
├── 🔗 pathlib
└── 🔗 typing
```

### utils.power_manager

```
utils.power_manager
├── 🔗 ctypes
├── 🔗 os
├── 🔗 subprocess
└── 🔗 platform
```

### utils.theme_manager

```
utils.theme_manager
├── 📦 utils
├── 📄 theme_config.json
├── 📄 theme_*.json
├── 📄 Carga la configuración del usuario desde theme_config.json
├── 📄 Guarda la configuración del usuario en theme_config.json
├── 🔗 typing
├── 🔗 json
├── 🔗 PySide6
├── 🔗 datetime
└── 🔗 pathlib
```

---

## 3. All Modules Index

Tabular view of all modules and their dependency counts:

| Module | Type | Local Deps. | Config Files | External Libs |
|--------|------|---------------|-----------------|---------------|
| agent_tools.analyze_dependencies | Entry Point | 0 | 0 | 7 |
| agent_tools.generate_rollback | Entry Point | 0 | 1 | 11 |
| agent_tools.schema_validator | Entry Point | 0 | 4 | 9 |
| agent_tools.treemap | Entry Point | 0 | 0 | 3 |
| agent_tools.validate_message | Entry Point | 0 | 4 | 10 |
| debug.debug_ppt | Entry Point | 0 | 2 | 6 |
| generar_exe | Entry Point | 0 | 3 | 5 |
| main | Entry Point | 5 | 0 | 2 |
| src.__init__ | Entry Point | 3 | 0 | 0 |
| src.excel_reader | Entry Point | 0 | 0 | 3 |
| src.pdf_worker | Entry Point | 0 | 0 | 6 |
| src.ppt_converter | Entry Point | 0 | 0 | 8 |
| src.ppt_generator | Entry Point | 1 | 0 | 8 |
| src.ppt_worker | Entry Point | 0 | 0 | 4 |
| ui.__init__ | Entry Point | 2 | 0 | 0 |
| ui.main_window | Entry Point | 10 | 0 | 5 |
| ui.splash_screen | Entry Point | 1 | 0 | 1 |
| ui.widgets.__init__ | Entry Point | 4 | 0 | 0 |
| ui.widgets.author_info_widget | Entry Point | 0 | 0 | 1 |
| ui.widgets.console_widget | Entry Point | 0 | 0 | 2 |
| ui.widgets.monitoring_panel | Entry Point | 0 | 0 | 2 |
| ui.widgets.template_selector_widget | Entry Point | 0 | 0 | 1 |
| ui.workers.__init__ | Entry Point | 2 | 0 | 0 |
| ui.workers.conversion_worker | Entry Point | 2 | 0 | 5 |
| ui.workers.generation_worker | Entry Point | 3 | 0 | 4 |
| utils.__init__ | Entry Point | 0 | 0 | 0 |
| utils.config_manager | Entry Point | 1 | 0 | 7 |
| utils.logger | Entry Point | 1 | 0 | 4 |
| utils.path_manager | Entry Point | 0 | 2 | 4 |
| utils.power_manager | Entry Point | 0 | 0 | 4 |
| utils.theme_manager | Entry Point | 1 | 4 | 5 |

---

## 4. Configuration Files

Data/configuration files detected in code and modules using them:

- **`          │       └── theme_dark.json`** → Used by: `generar_exe`
- **`          │   ├── mapping.yaml`** → Used by: `generar_exe`
- **`   3. Debes usar el NOMBRE REAL de la forma en mapping.yaml`** → Used by: `debug.debug_ppt`
- **`Carga la configuración del usuario desde theme_config.json`** → Used by: `utils.theme_manager`
- **`Guarda la configuración del usuario en theme_config.json`** → Used by: `utils.theme_manager`
- **`execution_report.schema.json`** → Used by: `agent_tools.schema_validator`, `agent_tools.validate_message`
- **`mapping.yaml`** → Used by: `generar_exe`, `utils.path_manager`
- **`paths_cache.json`** → Used by: `utils.path_manager`
- **`rollback_manifest.json`** → Used by: `agent_tools.generate_rollback`
- **`system_config.schema.yaml`** → Used by: `agent_tools.schema_validator`, `agent_tools.validate_message`
- **`task_envelope.schema.json`** → Used by: `agent_tools.schema_validator`, `agent_tools.validate_message`
- **`task_plan.schema.json`** → Used by: `agent_tools.schema_validator`, `agent_tools.validate_message`
- **`theme_*.json`** → Used by: `utils.theme_manager`
- **`theme_config.json`** → Used by: `utils.theme_manager`
- **`🎯 RECOMENDACIONES PARA mapping.yaml`** → Used by: `debug.debug_ppt`

---

## Notes

- This file is **automatically generated** via a pre-commit hook.
- Imports are detected through static analysis (AST) of Python code.
- Configuration files are detected via regex of common patterns (`open()`, `read_csv()`, etc.).
- Circular dependencies might cause some modules to be missing from the full tree.
