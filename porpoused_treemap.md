/project_root
|-- assets/                # Schemas, SQL queries, and icons
|-- src/
|   |-- core/              # Utils, base classes (workers/widgets), and shared logic
|   |-- modules/           # Domain-based folders (e.g., nomina, pdt, licencias)
|   |   |-- [module_name]/
|   |   |   |-- steps/     # ETL logic scripts
|   |   |   |-- ui/        # Module-specific widgets/workers
|   |   |   `-- __init__.py |   |-- orchestrators/     # YAML pipelines and executors |   `-- app_main.py        # Main GUI logic
|-- etl_manager.py         # Root entry point
|-- generar_exe.py         # Root build script
