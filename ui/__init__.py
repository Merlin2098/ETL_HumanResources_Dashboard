# ui/__init__.py
"""
Módulo UI para ETL Manager con PySide6
"""
from .main_app import ETLManagerWindow
from .etl_registry import get_registry

__all__ = ['ETLManagerWindow', 'get_registry']