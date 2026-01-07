# ui/workers/base_worker.py
"""
Worker base abstracto para todos los ETLs
"""
from PySide6.QtCore import QThread, Signal
from pathlib import Path
from typing import List, Dict
from abc import abstractmethod
import sys

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logger_qt import UILogger


class BaseETLWorker(QThread):
    """
    Worker base para ejecutar ETLs en background.
    Todos los workers heredan de esta clase.
    """
    
    # Señales comunes
    progress_updated = Signal(int, str)  # (porcentaje, mensaje)
    finished = Signal(bool, str, dict)   # (éxito, mensaje, resultados)
    
    def __init__(self, archivos: List[Path], output_dir: Path):
        super().__init__()
        self.archivos = archivos
        self.output_dir = output_dir
        
        # Logger con señales
        self.logger = UILogger(pipeline_name=self.get_pipeline_name())
        self.logger.progress_update.connect(self._emit_progress)
        
        self.resultado = {}
    
    @abstractmethod
    def get_pipeline_name(self) -> str:
        """Nombre del pipeline para logger"""
        pass
    
    @abstractmethod
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo.
        Debe retornar dict con resultados.
        """
        pass
    
    def run(self):
        """Ejecuta el ETL (llamado por QThread.start())"""
        try:
            self.resultado = self.execute_etl()
            
            if self.resultado.get('success', False):
                self.finished.emit(
                    True,
                    "✅ ETL completado exitosamente",
                    self.resultado
                )
            else:
                error = self.resultado.get('error', 'Error desconocido')
                self.finished.emit(
                    False,
                    f"❌ Error: {error}",
                    self.resultado
                )
                
        except Exception as e:
            self.logger.error(f"Error en ETL: {str(e)}")
            import traceback
            traceback.print_exc()
            self.finished.emit(
                False,
                f"❌ Error: {str(e)}",
                {'success': False, 'error': str(e)}
            )
    
    def _emit_progress(self, percentage: int, message: str):
        """Callback del logger para progreso"""
        self.progress_updated.emit(percentage, message)