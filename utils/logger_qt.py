# utils/logger_qt.py
"""
Logger adaptado para interfaces PySide6
Emite señales para actualizar QTextEdit en tiempo real
"""
from pathlib import Path
from typing import Optional
from PySide6.QtCore import QObject, Signal
import logging


class UILogger(QObject):
    """Logger que emite señales Qt para integración con QTextEdit"""
    
    log_message = Signal(str, str)  # (nivel, mensaje)
    step_started = Signal(str, str)  # (nombre_paso, descripción)
    step_completed = Signal(str, bool)  # (nombre_paso, éxito)
    progress_update = Signal(int, str)  # (porcentaje, mensaje)
    
    def __init__(
        self,
        pipeline_name: str,
        log_to_file: bool = True,
        log_dir: Path = Path("logs")
    ):
        """
        Inicializa logger para UI
        
        Args:
            pipeline_name: Nombre del pipeline
            log_to_file: También escribir a archivo
            log_dir: Directorio de logs
        """
        super().__init__()
        self.pipeline_name = pipeline_name
        self.log_to_file = log_to_file
        
        if log_to_file:
            self.log_dir = Path(log_dir)
            self.log_dir.mkdir(exist_ok=True, parents=True)
            self.log_file = self.log_dir / f"{pipeline_name}.log"
            self._setup_file_logger()
        else:
            self.file_logger = None
    
    def _setup_file_logger(self):
        """Configura logger de archivo"""
        self.file_logger = logging.getLogger(f"ui_pipeline.{self.pipeline_name}")
        self.file_logger.setLevel(logging.DEBUG)
        
        if self.file_logger.handlers:
            self.file_logger.handlers.clear()
        
        handler = logging.FileHandler(self.log_file, encoding='utf-8', mode='w')
        handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.file_logger.addHandler(handler)
    
    def _emit_and_log(self, level: str, message: str):
        """Emite señal y escribe a archivo"""
        self.log_message.emit(level, message)
        
        if self.file_logger:
            log_method = getattr(self.file_logger, level.lower(), self.file_logger.info)
            log_method(message)
    
    def debug(self, message: str):
        self._emit_and_log("DEBUG", message)
    
    def info(self, message: str):
        self._emit_and_log("INFO", message)
    
    def warning(self, message: str):
        self._emit_and_log("WARNING", message)
    
    def error(self, message: str):
        self._emit_and_log("ERROR", message)
    
    def critical(self, message: str):
        self._emit_and_log("CRITICAL", message)
    
    def log_step_start(self, step_name: str, description: str = ""):
        """Registra inicio de paso"""
        self.step_started.emit(step_name, description)
        separator = "=" * 60
        self._emit_and_log("INFO", "")
        self._emit_and_log("INFO", separator)
        self._emit_and_log("INFO", f"INICIANDO: {step_name}")
        if description:
            self._emit_and_log("INFO", f"Descripción: {description}")
        self._emit_and_log("INFO", separator)
    
    def log_step_end(self, step_name: str, success: bool = True):
        """Registra fin de paso"""
        self.step_completed.emit(step_name, success)
        status = "✓ COMPLETADO" if success else "✗ FALLIDO"
        separator = "=" * 60
        self._emit_and_log("INFO", f"{status}: {step_name}")
        self._emit_and_log("INFO", separator)
        self._emit_and_log("INFO", "")
    
    def log_progress(self, percentage: int, message: str):
        """Registra progreso"""
        self.progress_update.emit(percentage, message)
        self._emit_and_log("INFO", f"[{percentage}%] {message}")
    
    def log_file_processing(self, file_path: Path, operation: str = "Procesando"):
        """Registra procesamiento de archivo"""
        self._emit_and_log("INFO", f"{operation}: {file_path.name}")
    
    def log_dataframe_info(self, name: str, rows: int, cols: int):
        """Registra info de DataFrame"""
        self._emit_and_log("INFO", f"DataFrame '{name}': {rows:,} filas x {cols} columnas")
    
    def log_performance(self, operation: str, duration_seconds: float):
        """Registra métricas de rendimiento"""
        if duration_seconds < 1:
            time_str = f"{duration_seconds*1000:.0f}ms"
        elif duration_seconds < 60:
            time_str = f"{duration_seconds:.2f}s"
        else:
            minutes = int(duration_seconds // 60)
            seconds = duration_seconds % 60
            time_str = f"{minutes}m {seconds:.2f}s"
        
        self._emit_and_log("INFO", f"⏱️ {operation}: {time_str}")
    
    def get_log_path(self) -> Optional[Path]:
        """Retorna ruta del log de archivo"""
        return self.log_file if self.log_to_file else None


def get_ui_logger(
    pipeline_name: str,
    log_to_file: bool = True,
    log_dir: Path = Path("logs")
) -> UILogger:
    """Factory para obtener UILogger configurado"""
    return UILogger(
        pipeline_name=pipeline_name,
        log_to_file=log_to_file,
        log_dir=log_dir
    )