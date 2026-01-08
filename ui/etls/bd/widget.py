# ui/etls/bd/widget.py
"""
Widget para ETL de Base de Datos (Steps 1 + 1.5 + 2)
Hereda de BaseETLWidget para comportamiento estándar
"""
from ui.widgets.base_etl_widget import BaseETLWidget
from .worker import BDWorker


class BDWidget(BaseETLWidget):
    """Widget para procesamiento completo de BD"""
    
    def __init__(self):
        super().__init__("Procesamiento BD - ETL Completo")
    
    def _get_worker_class(self):
        """Retorna la clase Worker específica"""
        return BDWorker
    
    def _get_file_filter(self) -> str:
        """Filtro para seleccionar archivos Excel"""
        return "Archivos Excel (*.xlsx *.xlsm *.xls);;Todos los archivos (*.*)"
    
    def _get_no_files_message(self) -> str:
        return "No se ha seleccionado archivo Excel Bronze"
    
    def _get_select_button_text(self) -> str:
        return "📂 Seleccionar Archivo Excel Bronze"
    
    def _select_files(self):
        """
        Sobrescribe para permitir solo 1 archivo (no múltiple)
        """
        from pathlib import Path
        from utils.file_selector_qt import quick_file_select_qt
        
        files = quick_file_select_qt(
            parent=self,
            title="Seleccionar archivo Excel Bronze - BD",
            file_filter=self._get_file_filter(),
            multiple=False,  # Solo 1 archivo
            cache_key="bd_bronze"  # 🔑 Cache key para recordar la ruta
        )
        
        if files:
            # quick_file_select_qt siempre retorna una lista
            archivo = files[0]
            self.archivos_seleccionados = [archivo]
            
            self.label_files.setText(
                f"✓ Archivo seleccionado:\n  • {archivo.name}"
            )
            self.btn_process.setEnabled(True)
            self.btn_clear.setEnabled(True)
            self._log(f"📂 Archivo seleccionado: {archivo.name}")
            self._log("ℹ️  Se ejecutarán automáticamente:")
            self._log("   Step 1: Bronze → Silver")
            self._log("   Step 1.5: Extracción de Centros de Costo")
            self._log("   Step 2: Silver → Gold (Empleados + Practicantes)")
            self._log("   Step 3: Aplicación de Flags")