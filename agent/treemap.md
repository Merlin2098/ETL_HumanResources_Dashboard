## Project Structure

```
├── .gitignore
├── .pre-commit-config.yaml
├── agent/
│   ├── SIMPLIFIED_AGENT_MODEL.md
│   ├── agent_executor/
│   │   └── agent_executor.md
│   ├── agent_inspector/
│   │   └── agent_inspector.md
│   ├── agent_outputs/
│   ├── agent_protocol/
│   │   ├── README.md
│   │   ├── schemas/
│   │   │   ├── execution_report.schema.json
│   │   │   ├── system_config.schema.yaml
│   │   │   ├── task_envelope.schema.json
│   │   │   └── task_plan.schema.json
│   │   └── validators/
│   └── agent_rules.md
├── agent_tools/
│   ├── analyze_dependencies.py
│   ├── generate_rollback.py
│   ├── schema_validator.py
│   └── validate_message.py
├── config/
│   ├── app.ico
│   ├── mapping.yaml
│   ├── paths_cache.json
│   └── themes/
│       └── theme_dark.json
├── debug/
│   └── debug_ppt.py
├── generar_exe.py
├── main.py
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── excel_reader.py
│   ├── pdf_worker.py
│   ├── ppt_converter.py
│   ├── ppt_generator.py
│   └── ppt_worker.py
├── ui/
│   ├── __init__.py
│   ├── main_window.py
│   ├── splash_screen.py
│   ├── widgets/
│   │   ├── __init__.py
│   │   ├── author_info_widget.py
│   │   ├── console_widget.py
│   │   ├── monitoring_panel.py
│   │   └── template_selector_widget.py
│   └── workers/
│       ├── __init__.py
│       ├── conversion_worker.py
│       └── generation_worker.py
└── utils/
    ├── __init__.py
    ├── config_manager.py
    ├── logger.py
    ├── path_manager.py
    ├── power_manager.py
    └── theme_manager.py
```

## Cache Artifacts

- path_cache.json: single runtime cache file
- Host-specific or suffixed cache files are not supported
- Any additional cache files should be considered accidental artifacts
