"""
UI package for Control de Practicantes ETL.
"""

from .config import CONFIG
from .widget import ControlPracticantesWidget
from .worker import ControlPracticantesWorker

__all__ = ["CONFIG", "ControlPracticantesWidget", "ControlPracticantesWorker"]
