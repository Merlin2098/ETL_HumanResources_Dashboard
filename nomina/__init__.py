"""
Paquete de procesamiento de Nóminas (Planillas)
Pipeline Bronze → Silver → Gold
"""

from .api_step1 import (
    extraer_periodo,
    leer_archivo_planilla,
    consolidar_planillas_bronze_to_silver
)

from .api_step2 import (
    aplicar_transformaciones_gold,
    agregar_nombre_mes,
    validar_constraints,
    gestionar_versionamiento_gold,
    generar_excel_visualizacion,
    exportar_silver_to_gold
)

__all__ = [
    # Step 1: Bronze → Silver
    'extraer_periodo',
    'leer_archivo_planilla',
    'consolidar_planillas_bronze_to_silver',
    
    # Step 2: Silver → Gold
    'aplicar_transformaciones_gold',
    'agregar_nombre_mes',
    'validar_constraints',
    'gestionar_versionamiento_gold',
    'generar_excel_visualizacion',
    'exportar_silver_to_gold',
]