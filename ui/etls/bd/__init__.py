"""
Módulo ETL para Base de Datos Consolidada
Procesa datos de empleados con arquitectura Bronze → Silver → Gold
Incluye extracción de centros de costo y aplicación de flags de negocio
"""

from .config import ETL_CONFIG
from .widget import BDWidget
from .worker import BDWorker

__all__ = ['ETL_CONFIG', 'BDWidget', 'BDWorker']