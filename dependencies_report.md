# Análisis de Dependencias del Proyecto

> **Propósito**: Este documento mapea las dependencias entre módulos Python, archivos de configuración y librerías externas del proyecto. Úsalo para entender la arquitectura y las relaciones entre componentes.

## Resumen Ejecutivo

- **Total de módulos Python**: 70
- **Entry points del proyecto**: 70
- **Archivos de configuración**: 22
- **Librerías externas únicas**: 27

---

## 1. Entry Points (Puntos de Entrada)

Estos módulos son los **scripts principales** que inician la ejecución del proyecto (no son importados por otros módulos):

### `__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `analyze_dependencies`

**Dependencias directas**: 7 (0 módulos, 0 configs, 7 librerías)

- **Librerías externas**: `pathlib`, `collections`, `os`, `ast`, `pathspec` (+2 más)

### `bd.step1.5_centrosdecosto`

**Dependencias directas**: 8 (0 módulos, 2 configs, 6 librerías)

- **Archivos de config**: `*.json`, `esquema_cc.json`
- **Librerías externas**: `pathlib`, `traceback`, `datetime`, `json`, `polars` (+1 más)

### `bd.step1_capasilver`

**Dependencias directas**: 8 (0 módulos, 0 configs, 8 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+3 más)

### `bd.step2_capagold`

**Dependencias directas**: 9 (0 módulos, 2 configs, 7 librerías)

- **Archivos de config**: `esquema_bd.json`, `*.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+2 más)

### `bd.step3_flags_empleados`

**Dependencias directas**: 10 (0 módulos, 2 configs, 8 librerías)

- **Archivos de config**: `*.sql`, `queries_flags_gold.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+3 más)

### `control_practicantes.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `control_practicantes.step1_controlpracticantes`

**Dependencias directas**: 10 (0 módulos, 1 configs, 9 librerías)

- **Archivos de config**: `esquema_control_practicantes.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `sys` (+4 más)

### `control_practicantes.step2_controlpracticantes`

**Dependencias directas**: 9 (0 módulos, 1 configs, 8 librerías)

- **Archivos de config**: `query_control_practicantes_gold.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+3 más)

### `etl_manager`

**Dependencias directas**: 6 (2 módulos, 0 configs, 4 librerías)

- **Módulos internos**: `ui`, `utils`
- **Librerías externas**: `traceback`, `pathlib`, `sys`, `PySide6`

### `examen_retiro.step1_clean`

**Dependencias directas**: 8 (0 módulos, 0 configs, 8 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `sys` (+3 más)

### `examen_retiro.step2_gold`

**Dependencias directas**: 10 (0 módulos, 2 configs, 8 librerías)

- **Archivos de config**: `esquema_examen_retiro.json`, `*.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+3 más)

### `examen_retiro.step3_join`

**Dependencias directas**: 9 (0 módulos, 2 configs, 7 librerías)

- **Archivos de config**: `query_cc_join.sql`, `*.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+2 más)

### `generar_exe`

**Dependencias directas**: 9 (0 módulos, 0 configs, 9 librerías)

- **Librerías externas**: `time`, `pathlib`, `shutil`, `traceback`, `os` (+4 más)

### `licencias.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `licencias.step1_consolidar_licencias`

**Dependencias directas**: 10 (0 módulos, 1 configs, 9 librerías)

- **Archivos de config**: `esquema_licencias.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `sys` (+4 más)

### `licencias.step2_enriquecer_nomina`

**Dependencias directas**: 9 (0 módulos, 1 configs, 8 librerías)

- **Archivos de config**: `query_licencias_agregadas.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+3 más)

### `nomina.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `nomina.step1_consolidar_planillas`

**Dependencias directas**: 8 (0 módulos, 0 configs, 8 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `tkinter` (+3 más)

### `nomina.step2_exportar`

**Dependencias directas**: 13 (1 módulos, 2 configs, 10 librerías)

- **Módulos internos**: `utils`
- **Archivos de config**: `esquema_nominas.json`, `*.json`
- **Librerías externas**: `time`, `pathlib`, `shutil`, `traceback`, `os` (+5 más)

### `nomina_regimen_minero.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `nomina_regimen_minero.step1_consolidar_regimen_minero`

**Dependencias directas**: 7 (0 módulos, 0 configs, 7 librerías)

- **Librerías externas**: `time`, `pathlib`, `datetime`, `tkinter`, `re` (+2 más)

### `nomina_regimen_minero.step2_exportar_regimen_minero`

**Dependencias directas**: 9 (0 módulos, 1 configs, 8 librerías)

- **Archivos de config**: `*.json`
- **Librerías externas**: `pathlib`, `shutil`, `traceback`, `datetime`, `json` (+3 más)

### `orquestadores.__init__`

**Dependencias directas**: 1 (1 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `pipeline_nomina_executor`

### `orquestadores.pipeline_control_practicantes_executor`

**Dependencias directas**: 9 (0 módulos, 0 configs, 9 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `importlib`, `yaml` (+4 más)

### `orquestadores.pipeline_nomina_executor`

**Dependencias directas**: 8 (0 módulos, 0 configs, 8 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `importlib`, `yaml` (+3 más)

### `pdt.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `pdt.step1_consolidar_ingresos`

**Dependencias directas**: 8 (0 módulos, 0 configs, 8 librerías)

- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `sys` (+3 más)

### `pdt.step2_exportar_ingresos`

**Dependencias directas**: 10 (0 módulos, 2 configs, 8 librerías)

- **Archivos de config**: `esquema_relacion_ingresos.json`, `*.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+3 más)

### `pdt.step3_exportar_practicantes`

**Dependencias directas**: 10 (0 módulos, 2 configs, 8 librerías)

- **Archivos de config**: `esquema_ingresos_practicantes.json`, `*.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+3 más)

### `treemap`

**Dependencias directas**: 3 (0 módulos, 0 configs, 3 librerías)

- **Librerías externas**: `pathspec`, `sys`, `os`

### `ui.__init__`

**Dependencias directas**: 2 (2 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `main_app`, `etl_registry`

### `ui.etl_registry`

**Dependencias directas**: 5 (1 módulos, 0 configs, 4 librerías)

- **Módulos internos**: `utils`
- **Librerías externas**: `pathlib`, `traceback`, `typing`, `importlib`

### `ui.etls.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `ui.etls.bd.__init__`

**Dependencias directas**: 3 (3 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `config`, `widget`, `worker`

### `ui.etls.bd.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.bd.widget`

**Dependencias directas**: 4 (3 módulos, 0 configs, 1 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`

### `ui.etls.bd.worker`

**Dependencias directas**: 20 (3 módulos, 6 configs, 11 librerías)

- **Módulos internos**: `ui`, `utils`, `utils`
- **Archivos de config**: `esquema_cc.json`, `No se encontró queries_flags_gold.sql`, `esquema_bd.json`, `No se encontró esquema_bd.json`, `No se encontró esquema_cc.json`, `queries_flags_gold.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+6 más)

### `ui.etls.control_practicantes.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `ui.etls.control_practicantes.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.control_practicantes.widget`

**Dependencias directas**: 6 (3 módulos, 0 configs, 3 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`, `sys`, `openpyxl`

### `ui.etls.control_practicantes.worker`

**Dependencias directas**: 9 (3 módulos, 1 configs, 5 librerías)

- **Módulos internos**: `ui`, `orquestadores`, `utils`
- **Archivos de config**: `pipeline_control_practicantes.yaml`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `sys`, `typing`

### `ui.etls.examen_retiro.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `ui.etls.examen_retiro.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.examen_retiro.widget`

**Dependencias directas**: 7 (3 módulos, 0 configs, 4 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`, `sys`, `typing`, `PySide6`

### `ui.etls.examen_retiro.worker`

**Dependencias directas**: 14 (4 módulos, 2 configs, 8 librerías)

- **Módulos internos**: `utils`, `utils`, `utils`, `utils`
- **Archivos de config**: `esquema_examen_retiro.json`, `query_cc_join.sql`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `json`, `polars` (+3 más)

### `ui.etls.nomina.__init__`

**Dependencias directas**: 3 (3 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `config`, `widget`, `worker`

### `ui.etls.nomina.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.nomina.widget`

**Dependencias directas**: 5 (3 módulos, 0 configs, 2 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`, `sys`

### `ui.etls.nomina.worker`

**Dependencias directas**: 9 (3 módulos, 1 configs, 5 librerías)

- **Módulos internos**: `ui`, `orquestadores`, `utils`
- **Archivos de config**: `pipeline_nomina_licencias.yaml`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `sys`, `typing`

### `ui.etls.nomina_regimen_minero.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `ui.etls.nomina_regimen_minero.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.nomina_regimen_minero.widget`

**Dependencias directas**: 5 (3 módulos, 0 configs, 2 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`, `sys`

### `ui.etls.nomina_regimen_minero.worker`

**Dependencias directas**: 12 (3 módulos, 1 configs, 8 librerías)

- **Módulos internos**: `ui`, `utils`, `utils`
- **Archivos de config**: `esquema_regimen_minero.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `datetime`, `json` (+3 más)

### `ui.etls.pdt.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `ui.etls.pdt.config`

**Dependencias directas**: 1 (0 módulos, 0 configs, 1 librerías)

- **Librerías externas**: `dataclasses`

### `ui.etls.pdt.widget`

**Dependencias directas**: 5 (3 módulos, 0 configs, 2 librerías)

- **Módulos internos**: `ui`, `worker`, `utils`
- **Librerías externas**: `pathlib`, `sys`

### `ui.etls.pdt.worker`

**Dependencias directas**: 14 (5 módulos, 2 configs, 7 librerías)

- **Módulos internos**: `utils`, `ui`, `utils`, `utils`, `utils`
- **Archivos de config**: `esquema_ingresos_practicantes.json`, `esquema_relacion_ingresos.json`
- **Librerías externas**: `time`, `pathlib`, `traceback`, `json`, `polars` (+2 más)

### `ui.main_app`

**Dependencias directas**: 6 (3 módulos, 1 configs, 2 librerías)

- **Módulos internos**: `ui`, `ui`, `utils`
- **Archivos de config**: `theme_light.json`
- **Librerías externas**: `traceback`, `PySide6`

### `ui.theme_loader`

**Dependencias directas**: 4 (1 módulos, 1 configs, 2 librerías)

- **Módulos internos**: `utils`
- **Archivos de config**: `theme_light.json`
- **Librerías externas**: `pathlib`, `json`

### `ui.widgets.__init__`

**Dependencias directas**: 1 (1 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `base_etl_widget`

### `ui.widgets.base_etl_widget`

**Dependencias directas**: 6 (1 módulos, 0 configs, 5 librerías)

- **Módulos internos**: `utils`
- **Librerías externas**: `pathlib`, `abc`, `sys`, `typing`, `PySide6`

### `ui.workers.__init__`

**Dependencias directas**: 1 (1 módulos, 0 configs, 0 librerías)

- **Módulos internos**: `base_worker`

### `ui.workers.base_worker`

**Dependencias directas**: 7 (1 módulos, 0 configs, 6 librerías)

- **Módulos internos**: `utils`
- **Librerías externas**: `time`, `pathlib`, `abc`, `sys`, `typing` (+1 más)

### `utils.__init__`

**Dependencias directas**: 0 (0 módulos, 0 configs, 0 librerías)


### `utils.file_selector_qt`

**Dependencias directas**: 5 (2 módulos, 0 configs, 3 librerías)

- **Módulos internos**: `path_cache`, `path_cache`
- **Librerías externas**: `pathlib`, `typing`, `PySide6`

### `utils.lazy_loader`

**Dependencias directas**: 6 (0 módulos, 0 configs, 6 librerías)

- **Librerías externas**: `time`, `pathlib`, `typing`, `functools`, `sys` (+1 más)

### `utils.logger_qt`

**Dependencias directas**: 6 (0 módulos, 0 configs, 6 librerías)

- **Librerías externas**: `pathlib`, `traceback`, `datetime`, `typing`, `PySide6` (+1 más)

### `utils.path_cache`

**Dependencias directas**: 6 (1 módulos, 1 configs, 4 librerías)

- **Módulos internos**: `utils`
- **Archivos de config**: `path_cache.json`
- **Librerías externas**: `pathlib`, `typing`, `json`, `datetime`

### `utils.paths`

**Dependencias directas**: 3 (0 módulos, 0 configs, 3 librerías)

- **Librerías externas**: `pathlib`, `sys`, `os`

---

## 1. Módulos Principales (Entry Points)

Estos son los módulos que no son importados por ningún otro módulo:

- **__init__** → 0 dependencias
- **analyze_dependencies** → 0 dependencias
- **bd.step1.5_centrosdecosto** → 2 dependencias
- **bd.step1_capasilver** → 0 dependencias
- **bd.step2_capagold** → 2 dependencias
- **bd.step3_flags_empleados** → 2 dependencias
- **control_practicantes.__init__** → 0 dependencias
- **control_practicantes.step1_controlpracticantes** → 1 dependencias
- **control_practicantes.step2_controlpracticantes** → 1 dependencias
- **etl_manager** → 2 dependencias
- **examen_retiro.step1_clean** → 0 dependencias
- **examen_retiro.step2_gold** → 2 dependencias
- **examen_retiro.step3_join** → 2 dependencias
- **generar_exe** → 0 dependencias
- **licencias.__init__** → 0 dependencias
- **licencias.step1_consolidar_licencias** → 1 dependencias
- **licencias.step2_enriquecer_nomina** → 1 dependencias
- **nomina.__init__** → 0 dependencias
- **nomina.step1_consolidar_planillas** → 0 dependencias
- **nomina.step2_exportar** → 3 dependencias
- **nomina_regimen_minero.__init__** → 0 dependencias
- **nomina_regimen_minero.step1_consolidar_regimen_minero** → 0 dependencias
- **nomina_regimen_minero.step2_exportar_regimen_minero** → 1 dependencias
- **orquestadores.__init__** → 1 dependencias
- **orquestadores.pipeline_control_practicantes_executor** → 0 dependencias
- **orquestadores.pipeline_nomina_executor** → 0 dependencias
- **pdt.__init__** → 0 dependencias
- **pdt.step1_consolidar_ingresos** → 0 dependencias
- **pdt.step2_exportar_ingresos** → 2 dependencias
- **pdt.step3_exportar_practicantes** → 2 dependencias
- **treemap** → 0 dependencias
- **ui.__init__** → 2 dependencias
- **ui.etl_registry** → 1 dependencias
- **ui.etls.__init__** → 0 dependencias
- **ui.etls.bd.__init__** → 3 dependencias
- **ui.etls.bd.config** → 0 dependencias
- **ui.etls.bd.widget** → 3 dependencias
- **ui.etls.bd.worker** → 9 dependencias
- **ui.etls.control_practicantes.__init__** → 0 dependencias
- **ui.etls.control_practicantes.config** → 0 dependencias
- **ui.etls.control_practicantes.widget** → 3 dependencias
- **ui.etls.control_practicantes.worker** → 4 dependencias
- **ui.etls.examen_retiro.__init__** → 0 dependencias
- **ui.etls.examen_retiro.config** → 0 dependencias
- **ui.etls.examen_retiro.widget** → 3 dependencias
- **ui.etls.examen_retiro.worker** → 6 dependencias
- **ui.etls.nomina.__init__** → 3 dependencias
- **ui.etls.nomina.config** → 0 dependencias
- **ui.etls.nomina.widget** → 3 dependencias
- **ui.etls.nomina.worker** → 4 dependencias
- **ui.etls.nomina_regimen_minero.__init__** → 0 dependencias
- **ui.etls.nomina_regimen_minero.config** → 0 dependencias
- **ui.etls.nomina_regimen_minero.widget** → 3 dependencias
- **ui.etls.nomina_regimen_minero.worker** → 4 dependencias
- **ui.etls.pdt.__init__** → 0 dependencias
- **ui.etls.pdt.config** → 0 dependencias
- **ui.etls.pdt.widget** → 3 dependencias
- **ui.etls.pdt.worker** → 7 dependencias
- **ui.main_app** → 4 dependencias
- **ui.theme_loader** → 2 dependencias
- **ui.widgets.__init__** → 1 dependencias
- **ui.widgets.base_etl_widget** → 1 dependencias
- **ui.workers.__init__** → 1 dependencias
- **ui.workers.base_worker** → 1 dependencias
- **utils.__init__** → 0 dependencias
- **utils.file_selector_qt** → 2 dependencias
- **utils.lazy_loader** → 0 dependencias
- **utils.logger_qt** → 0 dependencias
- **utils.path_cache** → 2 dependencias
- **utils.paths** → 0 dependencias

---

## 2. Mapa Completo de Dependencias

Este árbol muestra **todas las dependencias recursivas** de cada entry point:

**Leyenda**:
- 📦 Módulo Python del proyecto
- 📄 Archivo de configuración (JSON, YAML, SQL, CSV, etc.)
- 🔗 Librería externa (instalada vía pip)

### __init__

```
__init__

```

### analyze_dependencies

```
analyze_dependencies
├── 🔗 pathlib
├── 🔗 collections
├── 🔗 os
├── 🔗 ast
├── 🔗 pathspec
├── 🔗 re
└── 🔗 sys
```

### bd.step1.5_centrosdecosto

```
bd.step1.5_centrosdecosto
├── 📄 *.json
├── 📄 esquema_cc.json
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
└── 🔗 tkinter
```

### bd.step1_capasilver

```
bd.step1_capasilver
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 polars
├── 🔗 re
└── 🔗 openpyxl
```

### bd.step2_capagold

```
bd.step2_capagold
├── 📄 esquema_bd.json
├── 📄 *.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
└── 🔗 tkinter
```

### bd.step3_flags_empleados

```
bd.step3_flags_empleados
├── 📄 *.sql
├── 📄 queries_flags_gold.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 polars
├── 🔗 openpyxl
└── 🔗 duckdb
```

### control_practicantes.__init__

```
control_practicantes.__init__

```

### control_practicantes.step1_controlpracticantes

```
control_practicantes.step1_controlpracticantes
├── 📄 esquema_control_practicantes.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 sys
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 openpyxl
```

### control_practicantes.step2_controlpracticantes

```
control_practicantes.step2_controlpracticantes
├── 📄 query_control_practicantes_gold.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 polars
├── 🔗 sys
└── 🔗 duckdb
```

### etl_manager

```
etl_manager
├── 📦 ui
├── 📦 utils
├── 🔗 traceback
├── 🔗 pathlib
├── 🔗 sys
└── 🔗 PySide6
```

### examen_retiro.step1_clean

```
examen_retiro.step1_clean
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 sys
├── 🔗 tkinter
├── 🔗 polars
└── 🔗 openpyxl
```

### examen_retiro.step2_gold

```
examen_retiro.step2_gold
├── 📄 esquema_examen_retiro.json
├── 📄 *.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 sys
```

### examen_retiro.step3_join

```
examen_retiro.step3_join
├── 📄 query_cc_join.sql
├── 📄 *.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 polars
└── 🔗 duckdb
```

### generar_exe

```
generar_exe
├── 🔗 time
├── 🔗 pathlib
├── 🔗 shutil
├── 🔗 traceback
├── 🔗 os
├── 🔗 pkg_resources
├── 🔗 subprocess
├── 🔗 threading
└── 🔗 sys
```

### licencias.__init__

```
licencias.__init__

```

### licencias.step1_consolidar_licencias

```
licencias.step1_consolidar_licencias
├── 📄 esquema_licencias.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 sys
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 openpyxl
```

### licencias.step2_enriquecer_nomina

```
licencias.step2_enriquecer_nomina
├── 📄 query_licencias_agregadas.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 polars
├── 🔗 sys
└── 🔗 duckdb
```

### nomina.__init__

```
nomina.__init__

```

### nomina.step1_consolidar_planillas

```
nomina.step1_consolidar_planillas
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 re
├── 🔗 polars
└── 🔗 openpyxl
```

### nomina.step2_exportar

```
nomina.step2_exportar
├── 📦 utils
├── 📄 esquema_nominas.json
├── 📄 *.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 shutil
├── 🔗 traceback
├── 🔗 os
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 openpyxl
```

### nomina_regimen_minero.__init__

```
nomina_regimen_minero.__init__

```

### nomina_regimen_minero.step1_consolidar_regimen_minero

```
nomina_regimen_minero.step1_consolidar_regimen_minero
├── 🔗 time
├── 🔗 pathlib
├── 🔗 datetime
├── 🔗 tkinter
├── 🔗 re
├── 🔗 polars
└── 🔗 openpyxl
```

### nomina_regimen_minero.step2_exportar_regimen_minero

```
nomina_regimen_minero.step2_exportar_regimen_minero
├── 📄 *.json
├── 🔗 pathlib
├── 🔗 shutil
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 openpyxl
```

### orquestadores.__init__

```
orquestadores.__init__
└── 📦 pipeline_nomina_executor
```

### orquestadores.pipeline_control_practicantes_executor

```
orquestadores.pipeline_control_practicantes_executor
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 importlib
├── 🔗 yaml
├── 🔗 openpyxl
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### orquestadores.pipeline_nomina_executor

```
orquestadores.pipeline_nomina_executor
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 importlib
├── 🔗 yaml
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### pdt.__init__

```
pdt.__init__

```

### pdt.step1_consolidar_ingresos

```
pdt.step1_consolidar_ingresos
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 sys
├── 🔗 tkinter
├── 🔗 polars
└── 🔗 openpyxl
```

### pdt.step2_exportar_ingresos

```
pdt.step2_exportar_ingresos
├── 📄 esquema_relacion_ingresos.json
├── 📄 *.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 sys
```

### pdt.step3_exportar_practicantes

```
pdt.step3_exportar_practicantes
├── 📄 esquema_ingresos_practicantes.json
├── 📄 *.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 tkinter
└── 🔗 sys
```

### treemap

```
treemap
├── 🔗 pathspec
├── 🔗 sys
└── 🔗 os
```

### ui.__init__

```
ui.__init__
├── 📦 main_app
└── 📦 etl_registry
```

### ui.etl_registry

```
ui.etl_registry
├── 📦 utils
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 typing
└── 🔗 importlib
```

### ui.etls.__init__

```
ui.etls.__init__

```

### ui.etls.bd.__init__

```
ui.etls.bd.__init__
├── 📦 config
├── 📦 widget
└── 📦 worker
```

### ui.etls.bd.config

```
ui.etls.bd.config
└── 🔗 dataclasses
```

### ui.etls.bd.widget

```
ui.etls.bd.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
└── 🔗 pathlib
```

### ui.etls.bd.worker

```
ui.etls.bd.worker
├── 📦 ui
├── 📦 utils
├── 📦 utils
├── 📄 esquema_cc.json
├── 📄 No se encontró queries_flags_gold.sql
├── 📄 esquema_bd.json
├── 📄 No se encontró esquema_bd.json
├── 📄 No se encontró esquema_cc.json
├── 📄 queries_flags_gold.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 re
├── 🔗 openpyxl
├── 🔗 polars
├── 🔗 sys
├── 🔗 typing
└── 🔗 duckdb
```

### ui.etls.control_practicantes.__init__

```
ui.etls.control_practicantes.__init__

```

### ui.etls.control_practicantes.config

```
ui.etls.control_practicantes.config
└── 🔗 dataclasses
```

### ui.etls.control_practicantes.widget

```
ui.etls.control_practicantes.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
├── 🔗 pathlib
├── 🔗 sys
└── 🔗 openpyxl
```

### ui.etls.control_practicantes.worker

```
ui.etls.control_practicantes.worker
├── 📦 ui
├── 📦 orquestadores
├── 📦 utils
├── 📄 pipeline_control_practicantes.yaml
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 sys
└── 🔗 typing
```

### ui.etls.examen_retiro.__init__

```
ui.etls.examen_retiro.__init__

```

### ui.etls.examen_retiro.config

```
ui.etls.examen_retiro.config
└── 🔗 dataclasses
```

### ui.etls.examen_retiro.widget

```
ui.etls.examen_retiro.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
├── 🔗 pathlib
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### ui.etls.examen_retiro.worker

```
ui.etls.examen_retiro.worker
├── 📦 utils
├── 📦 utils
├── 📦 utils
├── 📦 utils
├── 📄 esquema_examen_retiro.json
├── 📄 query_cc_join.sql
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 json
├── 🔗 polars
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### ui.etls.nomina.__init__

```
ui.etls.nomina.__init__
├── 📦 config
├── 📦 widget
└── 📦 worker
```

### ui.etls.nomina.config

```
ui.etls.nomina.config
└── 🔗 dataclasses
```

### ui.etls.nomina.widget

```
ui.etls.nomina.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
├── 🔗 pathlib
└── 🔗 sys
```

### ui.etls.nomina.worker

```
ui.etls.nomina.worker
├── 📦 ui
├── 📦 orquestadores
├── 📦 utils
├── 📄 pipeline_nomina_licencias.yaml
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 sys
└── 🔗 typing
```

### ui.etls.nomina_regimen_minero.__init__

```
ui.etls.nomina_regimen_minero.__init__

```

### ui.etls.nomina_regimen_minero.config

```
ui.etls.nomina_regimen_minero.config
└── 🔗 dataclasses
```

### ui.etls.nomina_regimen_minero.widget

```
ui.etls.nomina_regimen_minero.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
├── 🔗 pathlib
└── 🔗 sys
```

### ui.etls.nomina_regimen_minero.worker

```
ui.etls.nomina_regimen_minero.worker
├── 📦 ui
├── 📦 utils
├── 📦 utils
├── 📄 esquema_regimen_minero.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 json
├── 🔗 polars
├── 🔗 sys
└── 🔗 typing
```

### ui.etls.pdt.__init__

```
ui.etls.pdt.__init__

```

### ui.etls.pdt.config

```
ui.etls.pdt.config
└── 🔗 dataclasses
```

### ui.etls.pdt.widget

```
ui.etls.pdt.widget
├── 📦 ui
├── 📦 worker
├── 📦 utils
├── 🔗 pathlib
└── 🔗 sys
```

### ui.etls.pdt.worker

```
ui.etls.pdt.worker
├── 📦 utils
├── 📦 ui
├── 📦 utils
├── 📦 utils
├── 📦 utils
├── 📄 esquema_ingresos_practicantes.json
├── 📄 esquema_relacion_ingresos.json
├── 🔗 time
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 json
├── 🔗 polars
├── 🔗 sys
└── 🔗 typing
```

### ui.main_app

```
ui.main_app
├── 📦 ui
├── 📦 ui
├── 📦 utils
├── 📄 theme_light.json
├── 🔗 traceback
└── 🔗 PySide6
```

### ui.theme_loader

```
ui.theme_loader
├── 📦 utils
├── 📄 theme_light.json
├── 🔗 pathlib
└── 🔗 json
```

### ui.widgets.__init__

```
ui.widgets.__init__
└── 📦 base_etl_widget
```

### ui.widgets.base_etl_widget

```
ui.widgets.base_etl_widget
├── 📦 utils
├── 🔗 pathlib
├── 🔗 abc
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### ui.workers.__init__

```
ui.workers.__init__
└── 📦 base_worker
```

### ui.workers.base_worker

```
ui.workers.base_worker
├── 📦 utils
├── 🔗 time
├── 🔗 pathlib
├── 🔗 abc
├── 🔗 sys
├── 🔗 typing
└── 🔗 PySide6
```

### utils.__init__

```
utils.__init__

```

### utils.file_selector_qt

```
utils.file_selector_qt
├── 📦 path_cache
├── 📦 path_cache
├── 🔗 pathlib
├── 🔗 typing
└── 🔗 PySide6
```

### utils.lazy_loader

```
utils.lazy_loader
├── 🔗 time
├── 🔗 pathlib
├── 🔗 typing
├── 🔗 functools
├── 🔗 sys
└── 🔗 importlib
```

### utils.logger_qt

```
utils.logger_qt
├── 🔗 pathlib
├── 🔗 traceback
├── 🔗 datetime
├── 🔗 typing
├── 🔗 PySide6
└── 🔗 logging
```

### utils.path_cache

```
utils.path_cache
├── 📦 utils
├── 📄 path_cache.json
├── 🔗 pathlib
├── 🔗 typing
├── 🔗 json
└── 🔗 datetime
```

### utils.paths

```
utils.paths
├── 🔗 pathlib
├── 🔗 sys
└── 🔗 os
```

---

## 3. Índice de Todos los Módulos

Vista tabular de todos los módulos con sus dependencias:

| Módulo | Tipo | Deps. Locales | Archivos Config | Libs Externas |
|--------|------|---------------|-----------------|---------------|
| __init__ | Principal | 0 | 0 | 0 |
| analyze_dependencies | Principal | 0 | 0 | 7 |
| bd.step1.5_centrosdecosto | Principal | 0 | 2 | 6 |
| bd.step1_capasilver | Principal | 0 | 0 | 8 |
| bd.step2_capagold | Principal | 0 | 2 | 7 |
| bd.step3_flags_empleados | Principal | 0 | 2 | 8 |
| control_practicantes.__init__ | Principal | 0 | 0 | 0 |
| control_practicantes.step1_controlpracticantes | Principal | 0 | 1 | 9 |
| control_practicantes.step2_controlpracticantes | Principal | 0 | 1 | 8 |
| etl_manager | Principal | 2 | 0 | 4 |
| examen_retiro.step1_clean | Principal | 0 | 0 | 8 |
| examen_retiro.step2_gold | Principal | 0 | 2 | 8 |
| examen_retiro.step3_join | Principal | 0 | 2 | 7 |
| generar_exe | Principal | 0 | 0 | 9 |
| licencias.__init__ | Principal | 0 | 0 | 0 |
| licencias.step1_consolidar_licencias | Principal | 0 | 1 | 9 |
| licencias.step2_enriquecer_nomina | Principal | 0 | 1 | 8 |
| nomina.__init__ | Principal | 0 | 0 | 0 |
| nomina.step1_consolidar_planillas | Principal | 0 | 0 | 8 |
| nomina.step2_exportar | Principal | 1 | 2 | 10 |
| nomina_regimen_minero.__init__ | Principal | 0 | 0 | 0 |
| nomina_regimen_minero.step1_consolidar_regimen_minero | Principal | 0 | 0 | 7 |
| nomina_regimen_minero.step2_exportar_regimen_minero | Principal | 0 | 1 | 8 |
| orquestadores.__init__ | Principal | 1 | 0 | 0 |
| orquestadores.pipeline_control_practicantes_executor | Principal | 0 | 0 | 9 |
| orquestadores.pipeline_nomina_executor | Principal | 0 | 0 | 8 |
| pdt.__init__ | Principal | 0 | 0 | 0 |
| pdt.step1_consolidar_ingresos | Principal | 0 | 0 | 8 |
| pdt.step2_exportar_ingresos | Principal | 0 | 2 | 8 |
| pdt.step3_exportar_practicantes | Principal | 0 | 2 | 8 |
| treemap | Principal | 0 | 0 | 3 |
| ui.__init__ | Principal | 2 | 0 | 0 |
| ui.etl_registry | Principal | 1 | 0 | 4 |
| ui.etls.__init__ | Principal | 0 | 0 | 0 |
| ui.etls.bd.__init__ | Principal | 3 | 0 | 0 |
| ui.etls.bd.config | Principal | 0 | 0 | 1 |
| ui.etls.bd.widget | Principal | 3 | 0 | 1 |
| ui.etls.bd.worker | Principal | 3 | 6 | 11 |
| ui.etls.control_practicantes.__init__ | Principal | 0 | 0 | 0 |
| ui.etls.control_practicantes.config | Principal | 0 | 0 | 1 |
| ui.etls.control_practicantes.widget | Principal | 3 | 0 | 3 |
| ui.etls.control_practicantes.worker | Principal | 3 | 1 | 5 |
| ui.etls.examen_retiro.__init__ | Principal | 0 | 0 | 0 |
| ui.etls.examen_retiro.config | Principal | 0 | 0 | 1 |
| ui.etls.examen_retiro.widget | Principal | 3 | 0 | 4 |
| ui.etls.examen_retiro.worker | Principal | 4 | 2 | 8 |
| ui.etls.nomina.__init__ | Principal | 3 | 0 | 0 |
| ui.etls.nomina.config | Principal | 0 | 0 | 1 |
| ui.etls.nomina.widget | Principal | 3 | 0 | 2 |
| ui.etls.nomina.worker | Principal | 3 | 1 | 5 |
| ui.etls.nomina_regimen_minero.__init__ | Principal | 0 | 0 | 0 |
| ui.etls.nomina_regimen_minero.config | Principal | 0 | 0 | 1 |
| ui.etls.nomina_regimen_minero.widget | Principal | 3 | 0 | 2 |
| ui.etls.nomina_regimen_minero.worker | Principal | 3 | 1 | 8 |
| ui.etls.pdt.__init__ | Principal | 0 | 0 | 0 |
| ui.etls.pdt.config | Principal | 0 | 0 | 1 |
| ui.etls.pdt.widget | Principal | 3 | 0 | 2 |
| ui.etls.pdt.worker | Principal | 5 | 2 | 7 |
| ui.main_app | Principal | 3 | 1 | 2 |
| ui.theme_loader | Principal | 1 | 1 | 2 |
| ui.widgets.__init__ | Principal | 1 | 0 | 0 |
| ui.widgets.base_etl_widget | Principal | 1 | 0 | 5 |
| ui.workers.__init__ | Principal | 1 | 0 | 0 |
| ui.workers.base_worker | Principal | 1 | 0 | 6 |
| utils.__init__ | Principal | 0 | 0 | 0 |
| utils.file_selector_qt | Principal | 2 | 0 | 3 |
| utils.lazy_loader | Principal | 0 | 0 | 6 |
| utils.logger_qt | Principal | 0 | 0 | 6 |
| utils.path_cache | Principal | 1 | 1 | 4 |
| utils.paths | Principal | 0 | 0 | 3 |

---

## 4. Archivos de Configuración

Archivos de datos/configuración detectados en el código y qué módulos los utilizan:

- **`*.json`** → Usado por: `bd.step1.5_centrosdecosto`, `bd.step2_capagold`, `examen_retiro.step2_gold`, `nomina.step2_exportar`, `nomina_regimen_minero.step2_exportar_regimen_minero`, `pdt.step2_exportar_ingresos`, `pdt.step3_exportar_practicantes`
- **`*.sql`** → Usado por: `bd.step3_flags_empleados`, `examen_retiro.step3_join`
- **`No se encontró esquema_bd.json`** → Usado por: `ui.etls.bd.worker`
- **`No se encontró esquema_cc.json`** → Usado por: `ui.etls.bd.worker`
- **`No se encontró queries_flags_gold.sql`** → Usado por: `ui.etls.bd.worker`
- **`esquema_bd.json`** → Usado por: `bd.step2_capagold`, `ui.etls.bd.worker`
- **`esquema_cc.json`** → Usado por: `bd.step1.5_centrosdecosto`, `ui.etls.bd.worker`
- **`esquema_control_practicantes.json`** → Usado por: `control_practicantes.step1_controlpracticantes`
- **`esquema_examen_retiro.json`** → Usado por: `examen_retiro.step2_gold`, `ui.etls.examen_retiro.worker`
- **`esquema_ingresos_practicantes.json`** → Usado por: `pdt.step3_exportar_practicantes`, `ui.etls.pdt.worker`
- **`esquema_licencias.json`** → Usado por: `licencias.step1_consolidar_licencias`
- **`esquema_nominas.json`** → Usado por: `nomina.step2_exportar`
- **`esquema_regimen_minero.json`** → Usado por: `ui.etls.nomina_regimen_minero.worker`
- **`esquema_relacion_ingresos.json`** → Usado por: `pdt.step2_exportar_ingresos`, `ui.etls.pdt.worker`
- **`path_cache.json`** → Usado por: `utils.path_cache`
- **`pipeline_control_practicantes.yaml`** → Usado por: `ui.etls.control_practicantes.worker`
- **`pipeline_nomina_licencias.yaml`** → Usado por: `ui.etls.nomina.worker`
- **`queries_flags_gold.sql`** → Usado por: `bd.step3_flags_empleados`, `ui.etls.bd.worker`
- **`query_cc_join.sql`** → Usado por: `examen_retiro.step3_join`, `ui.etls.examen_retiro.worker`
- **`query_control_practicantes_gold.sql`** → Usado por: `control_practicantes.step2_controlpracticantes`
- **`query_licencias_agregadas.sql`** → Usado por: `licencias.step2_enriquecer_nomina`
- **`theme_light.json`** → Usado por: `ui.main_app`, `ui.theme_loader`

---

## Notas

- Este archivo es **generado automáticamente** mediante pre-commit hook
- Los imports se detectan mediante análisis estático (AST) del código Python
- Los archivos de configuración se detectan mediante regex de patrones comunes (`open()`, `read_csv()`, etc.)
- Las dependencias circulares pueden causar que algunos módulos no aparezcan en el árbol completo
