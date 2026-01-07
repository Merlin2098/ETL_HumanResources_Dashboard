# ui/workers/base_worker.py
"""
Worker base abstracto para todos los ETLs

MEJORADO con:
- Timer automático de ejecución
- Tracking de duración por fase
- Mejor manejo de errores
- Resumen automático con estadísticas
"""
from PySide6.QtCore import QThread, Signal
from pathlib import Path
from typing import List, Dict, Optional
from abc import abstractmethod
import sys
import time

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logger_qt import UILogger


class BaseETLWorker(QThread):
    """
    Worker base para ejecutar ETLs en background.
    Todos los workers heredan de esta clase.
    
    Características:
    - Timer automático de ejecución
    - Logger integrado con señales Qt
    - Manejo robusto de errores
    - Resumen automático al finalizar
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
        
        # Timers
        self._start_time = None
        self._end_time = None
        self.phase_timers = {}  # {phase_name: duration}
        
        self.resultado = {}
    
    @abstractmethod
    def get_pipeline_name(self) -> str:
        """Nombre del pipeline para logger"""
        pass
    
    @abstractmethod
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo.
        Debe retornar dict con resultados incluyendo:
        {
            'success': bool,
            'error': str (opcional),
            'timers': dict (opcional),
            ...otros datos...
        }
        """
        pass
    
    def run(self):
        """Ejecuta el ETL (llamado por QThread.start())"""
        self._start_time = time.time()
        
        try:
            self.logger.info("=" * 70)
            self.logger.info(f"🚀 Iniciando ETL: {self.get_pipeline_name()}")
            self.logger.info("=" * 70)
            self.logger.info(f"📂 Archivos de entrada: {len(self.archivos)}")
            self.logger.info(f"📁 Directorio de salida: {self.output_dir}")
            self.logger.info("")
            
            # Ejecutar ETL
            self.resultado = self.execute_etl()
            
            self._end_time = time.time()
            total_duration = self._end_time - self._start_time
            
            # Agregar duración total si no está
            if 'timers' not in self.resultado:
                self.resultado['timers'] = {}
            if 'total' not in self.resultado['timers']:
                self.resultado['timers']['total'] = total_duration
            
            # Procesar resultado
            if self.resultado.get('success', False):
                self._log_success_summary()
                
                mensaje = self.resultado.get('mensaje', '✅ ETL completado exitosamente')
                
                self.finished.emit(True, mensaje, self.resultado)
            else:
                self._log_error_summary()
                
                error = self.resultado.get('error', 'Error desconocido')
                mensaje = f"❌ Error en ETL: {error}"
                
                self.finished.emit(False, mensaje, self.resultado)
                
        except Exception as e:
            self._end_time = time.time()
            
            self.logger.log_error_details(e, "ejecución del ETL")
            
            total_duration = self._end_time - self._start_time
            
            self.finished.emit(
                False,
                f"❌ Error crítico: {str(e)}",
                {
                    'success': False, 
                    'error': str(e),
                    'timers': {'total': total_duration}
                }
            )
    
    def _log_success_summary(self):
        """Registra resumen de éxito con estadísticas"""
        timers = self.resultado.get('timers', {})
        total_duration = timers.get('total', 0)
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("✅ ETL COMPLETADO EXITOSAMENTE".center(70))
        self.logger.info("=" * 70)
        
        # Resumen de tiempos
        self.logger.info("")
        self.logger.info("⏱️  Tiempos de ejecución:")
        self.logger.info(f"  • Total: {self.logger.format_duration(total_duration)}")
        
        # Tiempos por fase
        for phase_name, duration in timers.items():
            if phase_name != 'total':
                self.logger.info(f"  • {phase_name}: {self.logger.format_duration(duration)}")
        
        # Estadísticas de datos (si existen)
        if 'step1' in self.resultado:
            step1 = self.resultado['step1']
            self.logger.info("")
            self.logger.info("📊 Datos procesados:")
            if 'registros' in step1:
                self.logger.info(f"  • Silver: {step1['registros']:,} registros")
            if 'columnas' in step1:
                self.logger.info(f"  • Columnas: {step1['columnas']}")
        
        if 'step2' in self.resultado and 'registros' in self.resultado['step2']:
            step2 = self.resultado['step2']
            self.logger.info(f"  • Gold: {step2['registros']:,} registros")
            if 'columnas' in step2:
                self.logger.info(f"  • Columnas finales: {step2['columnas']}")
        
        # Estadísticas del logger
        stats = self.logger.get_stats()
        if stats['warnings'] > 0 or stats['errors'] > 0:
            self.logger.info("")
            self.logger.info("📋 Resumen de logging:")
            if stats['warnings'] > 0:
                self.logger.warning(f"Warnings: {stats['warnings']}")
            if stats['errors'] > 0:
                self.logger.error(f"Errores: {stats['errors']}")
        
        if stats['validations_passed'] > 0 or stats['validations_failed'] > 0:
            total_validations = stats['validations_passed'] + stats['validations_failed']
            self.logger.info(f"  • Validaciones: {stats['validations_passed']}/{total_validations} exitosas")
        
        self.logger.info("")
        self.logger.info("=" * 70)
        
        # Ruta del log
        log_path = self.logger.get_log_path()
        if log_path:
            self.logger.info(f"📄 Log completo guardado en: {log_path}")
    
    def _log_error_summary(self):
        """Registra resumen de error"""
        timers = self.resultado.get('timers', {})
        total_duration = timers.get('total', 0)
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.error("ETL FINALIZADO CON ERRORES".center(70))
        self.logger.info("=" * 70)
        
        error_msg = self.resultado.get('error', 'Error desconocido')
        self.logger.error(f"Causa: {error_msg}")
        
        self.logger.info("")
        self.logger.info(f"⏱️  Tiempo transcurrido: {self.logger.format_duration(total_duration)}")
        
        # Estadísticas del logger
        stats = self.logger.get_stats()
        if stats['errors'] > 0:
            self.logger.info(f"📋 Total de errores registrados: {stats['errors']}")
        
        self.logger.info("")
        self.logger.info("=" * 70)
        
        # Ruta del log
        log_path = self.logger.get_log_path()
        if log_path:
            self.logger.info(f"📄 Log de errores guardado en: {log_path}")
    
    def _emit_progress(self, percentage: int, message: str):
        """Callback del logger para progreso"""
        self.progress_updated.emit(percentage, message)
    
    def start_phase(self, phase_name: str) -> float:
        """
        Inicia timer para una fase específica
        
        Args:
            phase_name: Nombre de la fase
            
        Returns:
            Timestamp de inicio (para usar en end_phase)
        """
        start_time = time.time()
        self.logger.log_step_start(phase_name)
        return start_time
    
    def end_phase(self, phase_name: str, start_time: float, success: bool = True):
        """
        Finaliza timer para una fase y registra duración
        
        Args:
            phase_name: Nombre de la fase
            start_time: Timestamp del start_phase
            success: Si la fase fue exitosa
        """
        duration = time.time() - start_time
        self.phase_timers[phase_name] = duration
        self.logger.log_step_end(phase_name, success, duration)
    
    def get_total_duration(self) -> Optional[float]:
        """Retorna duración total de ejecución (si ya finalizó)"""
        if self._start_time and self._end_time:
            return self._end_time - self._start_time
        elif self._start_time:
            return time.time() - self._start_time
        return None
    
    def cleanup(self):
        """Limpieza al finalizar (sobrescribir si es necesario)"""
        self.logger.close()


# ============================================================================
# CLASE AUXILIAR PARA TRACKING DE FASES
# ============================================================================

class PhaseTimer:
    """
    Context manager para timing automático de fases
    
    Uso:
        with PhaseTimer(worker, "Step 1: Consolidación"):
            # código de la fase
            pass
        # Automáticamente registra duración al salir
    """
    
    def __init__(self, worker: BaseETLWorker, phase_name: str):
        self.worker = worker
        self.phase_name = phase_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = self.worker.start_phase(self.phase_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        self.worker.end_phase(self.phase_name, self.start_time, success)
        return False  # No suprimir excepciones