# ui/etls/control_practicantes/widget.py
"""
Widget específico para ETL de Control de Practicantes
Selecciona archivo Excel de control y valida estructura
"""
import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.utils.ui.widgets.base_etl_widget import BaseETLWidget
from .worker import ControlPracticantesWorker
from src.utils.ui.file_selector_qt import quick_file_select_qt


class ControlPracticantesWidget(BaseETLWidget):
    """Widget para procesamiento de control de practicantes"""
    
    def __init__(self):
        super().__init__(title="Procesamiento de Control de Practicantes")
    
    def _get_worker_class(self):
        """Retorna la clase Worker de Control de Practicantes"""
        return ControlPracticantesWorker
    
    def _get_file_filter(self) -> str:
        """Filtro para archivos Excel"""
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"
    
    def _get_select_button_text(self) -> str:
        """Texto personalizado para botón de selección"""
        return "📄 Seleccionar Archivo de Control"
    
    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay archivo seleccionado"""
        return "No hay archivo seleccionado. Seleccione el archivo de control de practicantes."
    
    def _select_files(self):
        """
        SOBRESCRITURA: Selecciona UN archivo Excel
        Valida que sea el archivo correcto de control de practicantes
        """
        # Abrir selector de archivo
        resultado = quick_file_select_qt(
            parent=self,
            title="Seleccionar archivo de control de practicantes",
            file_filter="Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)",
            cache_key="control_practicantes_archivo"
        )
        
        if not resultado:
            return
        
        # quick_file_select_qt puede retornar lista o Path
        # Extraer el primer archivo si es lista
        if isinstance(resultado, list):
            if len(resultado) == 0:
                return
            archivo = resultado[0]
        else:
            archivo = resultado
        
        # Validar extensión
        if archivo.suffix.lower() not in ['.xlsx', '.xls', '.xlsm']:
            self._log(f"⚠️  Archivo no es Excel: {archivo.name}")
            self.label_files.setText(
                f"⚠️  Archivo seleccionado: {archivo.name}\n"
                f"❌ El archivo debe ser Excel (.xlsx, .xls)"
            )
            self.btn_process.setEnabled(False)
            return
        
        # Validar nombre del archivo
        nombre_esperado = "LISTA DE CONTRATOS Y PRACTICANTES - CONTROL"
        nombre_archivo = archivo.stem
        
        nombre_valido = nombre_esperado in nombre_archivo.upper()
        
        if not nombre_valido:
            self._log(f"⚠️  Nombre de archivo no coincide con el esperado")
            
            mensaje_warning = f"⚠️  Archivo: {archivo.name}\n\n"
            mensaje_warning += f"⚠️  Advertencia: El nombre del archivo no coincide\n"
            mensaje_warning += f"   con el esperado:\n"
            mensaje_warning += f"   '{nombre_esperado}.xlsx'\n\n"
            mensaje_warning += f"   ¿Desea continuar de todas formas?"
            
            self.label_files.setText(mensaje_warning)
            
            # Permitir continuar pero con advertencia
            self.archivos_seleccionados = [archivo]
            self.btn_process.setEnabled(True)
            self.btn_clear.setEnabled(True)
            return
        
        # VALIDAR PESTAÑA: Debe existir pestaña "Practicantes"
        try:
            import openpyxl
            wb = openpyxl.load_workbook(archivo, read_only=True)
            
            tiene_practicantes = "Practicantes" in wb.sheetnames
            wb.close()
            
            if not tiene_practicantes:
                self._log(f"❌ El archivo no contiene la pestaña 'Practicantes'")
                
                mensaje_error = f"❌ Archivo: {archivo.name}\n\n"
                mensaje_error += f"❌ Error: No se encontró la pestaña 'Practicantes'\n"
                mensaje_error += f"   El archivo debe contener una hoja llamada\n"
                mensaje_error += f"   'Practicantes' con los datos a procesar."
                
                self.label_files.setText(mensaje_error)
                self.btn_process.setEnabled(False)
                return
            
        except Exception as e:
            self._log(f"❌ Error al validar archivo: {e}")
            self.label_files.setText(
                f"❌ Error al validar archivo:\n{str(e)}"
            )
            self.btn_process.setEnabled(False)
            return
        
        # Archivo válido - guardar
        self.archivos_seleccionados = [archivo]
        
        # Actualizar UI con validación exitosa
        self.label_files.setText(
            f"✓ Archivo seleccionado:\n"
            f"  • {archivo.name}\n\n"
            f"✓ Pestaña 'Practicantes' encontrada\n"
            f"✓ Archivo Excel válido\n\n"
            f"📊 Pipeline:\n"
            f"  1. Bronze → Silver (Procesamiento Excel)\n"
            f"  2. Silver → Gold (Flags y tiempo de servicio)\n\n"
            f"📁 La estructura silver/gold/ se creará en:\n"
            f"  {archivo.parent.name}/"
        )
        
        self.btn_process.setEnabled(True)
        self.btn_clear.setEnabled(True)
        
        self._log(f"📄 Archivo seleccionado: {archivo.name}")
        self._log(f"✓ Validación exitosa")