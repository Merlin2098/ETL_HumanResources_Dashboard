"""
UI package for Nomina Regimen Minero ETL.
"""

from .config import CONFIG
from .widget import NominaRegimenMineroWidget
from .worker import NominaRegimenMineroWorker

__all__ = ["CONFIG", "NominaRegimenMineroWidget", "NominaRegimenMineroWorker"]
