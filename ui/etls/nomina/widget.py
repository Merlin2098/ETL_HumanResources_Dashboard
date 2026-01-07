# ui/etls/nomina/widget.py
"""
Widget específico para ETL de Nómina
"""
import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ui.widgets.base_etl_widget import BaseETLWidget
from .worker import NominaWorker


class NominaWidget(BaseETLWidget):
    """Widget para procesamiento de nóminas"""
    
    def __init__(self):
        super().__init__(title="Procesamiento de Planillas")
    
    def _get_worker_class(self):
        """Retorna la clase Worker de Nómina"""
        return NominaWorker
    
    def _get_file_filter(self) -> str:
        """Filtro para archivos Excel de planillas"""
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"
    
    def _get_select_button_text(self) -> str:
        """Texto personalizado para botón de selección"""
        return "📁 Seleccionar Planillas"