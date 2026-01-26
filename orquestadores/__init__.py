"""
Módulo de Orquestadores de Pipelines
Ejecuta pipelines ETL definidos en archivos YAML
"""

from .pipeline_nomina_executor import PipelineNominaExecutor

__all__ = ['PipelineNominaExecutor']