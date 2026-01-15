# ETL_KPIS_AdministracionPersonal

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)](https://www.microsoft.com/windows)

Sistema ETL integral para procesamiento y consolidación de datos de Administración de Personal, desarrollado 100% en Python con arquitectura de capas Bronze → Silver → Gold y almacenamiento OLAP basado en Parquet.

## 📋 Descripción

Este proyecto resuelve la problemática de integrar múltiples fuentes de datos heterogéneas (nóminas, declaraciones tributarias, bases de datos de empleados, exámenes médicos) en un único reporte de Power BI estable y mantenible.

**Problema resuelto**: Anteriormente, el procesamiento mediante Power Query y DAX era frágil, requería rehacerse constantemente y tomaba aproximadamente 1 semana de trabajo en cada actualización. Esta solución automatiza el proceso completo mediante pipelines ETL robustos con validación de esquemas y transformaciones SQL.

**Usuarios finales**:

- **Ejecución**: Analistas y Coordinadores del área de negocio
- **Consumo**: Gerencia a través de dashboards de Power BI

## ✨ Características Principales

- **5 Pipelines ETL completos**: BD (Base de Datos Empleados), Nómina, Nómina Régimen Minero, PDT (Declaraciones Tributarias), Examen de Retiro
- **Arquitectura de capas**: Bronze (raw) → Silver (limpio) → Gold (analytics-ready)
- **Validación de esquemas**: Schemas JSON para garantizar calidad de datos en capa Gold
- **Business Rules Engine**: Generación de flags mediante queries SQL (alertas de edad de jubilación, duración de contratos, etc.)
- **Dual Output Format**: Parquet para procesamiento OLAP + Excel para revisión humana
- **Path Caching**: Sistema de caché en JSON para facilitar navegación de carpetas frecuentes
- **Interfaz gráfica amigable**: GUI tipo menú ERP desarrollada en PySide6
- **Versionamiento dual**: Archivos actuales en `/actual` y timestamped en `/historico` para auditoría

## 🛠️ Stack Tecnológico

- **Python 3.13+**
- **Polars**: Manipulación de datos de alto rendimiento
- **DuckDB**: Motor SQL serverless para transformaciones complejas
- **PySide6**: Interfaz gráfica de usuario
- **Openpyxl**: Lectura/escritura de archivos Excel
- **Pydantic**: Validación de datos
- **PyInstaller**: Empaquetado de ejecutable standalone

## 📦 Requisitos del Sistema

- **Sistema Operativo**: Windows
- **Python**: 3.13 o superior
- **Permisos**: Nivel usuario estándar (no requiere privilegios administrativos)

## 🚀 Instalación

1. Descargar el archivo ZIP del proyecto desde GitHub
2. Descomprimir en la ubicación deseada
3. Ejecutar el archivo `.exe` incluido

**No se requiere instalación de Python ni dependencias** - el ejecutable es standalone y contiene todo lo necesario.

### Instalación para Desarrollo

Si deseas modificar el código fuente:

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/ETL_KPIS_AdministracionPersonal.git
cd ETL_KPIS_AdministracionPersonal

# Crear entorno virtual
python -m venv .venv
.venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Configurar pre-commit hooks
pre-commit install
```

## 📁 Estructura del Proyecto

```
ETL_KPIS_AdministracionPersonal/
├── bd/                          # ETL Base de Datos de Empleados
├── nomina/                      # ETL Nóminas estándar
├── nomina_regimen_minero/       # ETL Nóminas régimen minero
├── pdt/                         # ETL Declaraciones tributarias
├── examen_retiro/               # ETL Exámenes médicos
├── esquemas/                    # Schemas JSON para validación
├── queries/                     # Queries SQL para transformaciones
├── ui/                          # Interfaz gráfica (PySide6)
│   ├── etls/                    # Módulos ETL por tipo
│   ├── widgets/                 # Componentes UI reutilizables
│   └── workers/                 # Workers para procesamiento async
├── utils/                       # Utilidades comunes
└── config/                      # Configuración y temas
```

## 💻 Uso

### Ejecución de la Aplicación

1. Doble clic en el ejecutable `.exe`
2. La aplicación se abre con un menú tipo ERP mostrando las 5 pestañas ETL
3. Seleccionar la pestaña correspondiente al tipo de datos a procesar
4. Usar el explorador de archivos para seleccionar la carpeta o archivos fuente
5. Hacer clic en "Procesar"
6. Los resultados se guardan automáticamente en las carpetas de salida

### Flujo de Trabajo Típico

```
1. Usuario actualiza archivos Excel fuente
2. Ejecuta la aplicación
3. Procesa cada ETL desde su pestaña correspondiente
4. Ingresa a Power BI
5. Actualiza el dashboard (los paths son estables)
```

### Organización de Archivos Fuente

**Recomendación**: Cada tipo de fuente debe estar en su propia carpeta para evitar sobrescrituras entre ETLs.

```
Mis_Datos/
├── BD_Empleados/           # Archivos de base de datos
├── Nominas/                # Planillas de nómina estándar
├── Nominas_Minero/         # Planillas régimen minero
├── PDT/                    # Declaraciones tributarias
└── Examenes/               # Reportes de exámenes médicos
```

## 🏗️ Arquitectura de Capas

### Bronze (Raw)

- Conversión de Excel a Parquet
- Limpieza inicial de filas y columnas vacías
- Preservación de datos originales

### Silver (Cleaned)

- Consolidación de múltiples archivos fuente
- Normalización de tipos de datos
- Estandarización de formatos

### Gold (Analytics-Ready)

- Validación contra schemas JSON
- Aplicación de transformaciones SQL (joins, agregaciones)
- Generación de flags de negocio
- **Output dual**:
  - **Parquet** → `/actual/` y `/historico/` (para Power BI)
  - **Excel** → Revisión y validación humana

### Versionamiento Dual

- **`/actual/`**: Archivos sin timestamp con path estable para Power BI
- **`/historico/`**: Archivos timestamped para auditoría y control de cambios

## 🔧 Configuración Interna

El proyecto incluye configuraciones pre-establecidas:

- **Schemas JSON** (`/esquemas/`): Definen estructura esperada de datos Gold
- **Queries SQL** (`/queries/`): Transformaciones complejas y generación de flags
- **Temas UI** (`/config/`): Personalización de interfaz gráfica
- **Path Cache**: Almacena rutas frecuentes en JSON para agilizar navegación

No se requiere configuración de variables de entorno ni archivos externos.

## 🔒 Consideraciones de Seguridad

Por motivos de confidencialidad:

- ❌ No se incluyen archivos fuente de datos reales
- ❌ No se comparte el dashboard de Power BI
- ✅ El código está disponible para demostrar arquitectura y mejores prácticas

**Objetivo del repositorio**: Mostrar arquitectura profesional de ETL local con:

- Python puro
- Parquet como base OLAP
- Configuración JSON
- Queries SQL serverless (DuckDB)

## 📝 Generación de Ejecutable

Para generar el ejecutable standalone:

```bash
python generar_exe.py
```

Esto crea un `.exe` empaquetado con PyInstaller que incluye:

- Runtime de Python
- Todas las dependencias
- Recursos estáticos (iconos, temas, schemas)
- Utilidades de path resolution

## 🤝 Contribución

Este es un proyecto privado de Metso. Para consultas o colaboración:

**LinkedIn**: [Ricardo Uculmana Quispe](https://pe.linkedin.com/in/ricardouculmanaquispe)

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.
