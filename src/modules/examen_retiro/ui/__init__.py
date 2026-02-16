"""
UI package for Examen Retiro ETL.
"""

from .config import CONFIG
from .widget import ExamenRetiroWidget
from .worker import ExamenRetiroWorker

__all__ = ["CONFIG", "ExamenRetiroWidget", "ExamenRetiroWorker"]
