"""
Configuración del ETL de Base de Datos Consolidada
Define metadata y parámetros para registro en el sistema
"""

ETL_CONFIG = {
    'id': 'bd',
    'nombre': 'Base de Datos',
    'descripcion': 'Procesamiento completo de BD: Bronze→Silver→Gold + Centros de Costo + Flags',
    'icono': '🗄️',
    'color': '#7C3AED',  # Púrpura
    'orden': 4,
    'tags': ['bd', 'empleados', 'practicantes', 'centros_costo', 'flags']
}