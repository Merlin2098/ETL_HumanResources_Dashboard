"""
Sistema de logging robusto para pipelines ETL
Proporciona logging multinivel con Rich para consola y archivos detallados
"""
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console

console = Console()


class PipelineLogger:
    """
    Logger personalizado para pipelines ETL con soporte Rich
    """
    
    def __init__(
        self,
        pipeline_name: str,
        log_dir: Path = Path("logs"),
        console_level: int = logging.WARNING,
        file_level: int = logging.DEBUG,
        keep_last_n_logs: int = 10
    ):
        """
        Inicializa el logger del pipeline
        
        Args:
            pipeline_name: Nombre del pipeline (ej: 'nomina', 'examen_retiro')
            log_dir: Directorio donde se guardarán los logs
            console_level: Nivel de logging para consola (default: WARNING)
            file_level: Nivel de logging para archivo (default: DEBUG)
            keep_last_n_logs: Cantidad de logs históricos a mantener
        """
        self.pipeline_name = pipeline_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True, parents=True)
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{pipeline_name}.log"
        
        self.logger = self._setup_logger(console_level, file_level)
    
    def _setup_logger(self, console_level: int, file_level: int) -> logging.Logger:
        """
        Configura el logger con handler solo para archivo (sin consola)
        """
        logger = logging.getLogger(f"pipeline.{self.pipeline_name}")
        logger.setLevel(logging.DEBUG)
        
        # Evitar duplicación de handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # Handler para archivo con formato detallado
        file_handler = logging.FileHandler(
            self.log_file,
            encoding='utf-8',
            mode='w'
        )
        file_handler.setLevel(file_level)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Solo agregar handler de archivo (sin consola)
        logger.addHandler(file_handler)
        
        return logger
    
    def log_step_start(self, step_name: str, description: str = ""):
        """
        Registra el inicio de un paso del pipeline
        """
        separator = "=" * 60
        self.logger.info("")
        self.logger.info(separator)
        self.logger.info(f"[bold cyan]INICIANDO:[/bold cyan] {step_name}")
        if description:
            self.logger.info(f"Descripción: {description}")
        self.logger.info(separator)
    
    def log_step_end(self, step_name: str, success: bool = True):
        """
        Registra el fin de un paso del pipeline
        """
        status = "[bold green]✓ COMPLETADO[/bold green]" if success else "[bold red]✗ FALLIDO[/bold red]"
        separator = "=" * 60
        self.logger.info(f"{status}: {step_name}")
        self.logger.info(separator)
        self.logger.info("")
    
    def log_file_processing(self, file_path: Path, operation: str = "Procesando"):
        """
        Registra el procesamiento de un archivo
        """
        self.logger.info(f"{operation}: [cyan]{file_path.name}[/cyan]")
        self.logger.debug(f"Ruta completa: {file_path.resolve()}")
    
    def log_dataframe_info(self, name: str, rows: int, cols: int):
        """
        Registra información sobre un DataFrame procesado
        """
        self.logger.info(f"DataFrame '{name}': {rows:,} filas x {cols} columnas")
    
    def log_validation_result(self, passed: bool, message: str):
        """
        Registra resultado de validación
        """
        if passed:
            self.logger.info(f"[green]✓[/green] Validación exitosa: {message}")
        else:
            self.logger.warning(f"[yellow]⚠[/yellow] Validación con advertencias: {message}")
    
    def log_error_with_context(self, error: Exception, context: str = ""):
        """
        Registra un error con contexto adicional
        """
        if context:
            self.logger.error(f"Error en {context}")
        self.logger.exception(f"[bold red]ERROR:[/bold red] {str(error)}")
    
    def log_performance(self, operation: str, duration_seconds: float):
        """
        Registra métricas de rendimiento
        """
        if duration_seconds < 1:
            time_str = f"{duration_seconds*1000:.0f}ms"
        elif duration_seconds < 60:
            time_str = f"{duration_seconds:.2f}s"
        else:
            minutes = int(duration_seconds // 60)
            seconds = duration_seconds % 60
            time_str = f"{minutes}m {seconds:.2f}s"
        
        self.logger.info(f"⏱️  {operation}: {time_str}")
    
    def debug(self, message: str):
        """Mensaje de debug"""
        self.logger.debug(message)
    
    def info(self, message: str):
        """Mensaje informativo"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Mensaje de advertencia"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Mensaje de error"""
        self.logger.error(message)
    
    def critical(self, message: str):
        """Mensaje crítico"""
        self.logger.critical(message)
    
    def get_log_path(self) -> Path:
        """Retorna la ruta del archivo de log actual"""
        return self.log_file


def get_logger(
    pipeline_name: str,
    log_dir: Path = Path("logs"),
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
) -> PipelineLogger:
    """
    Factory function para obtener un logger configurado
    
    Args:
        pipeline_name: Nombre del pipeline
        log_dir: Directorio de logs
        console_level: Nivel para consola
        file_level: Nivel para archivo
    
    Returns:
        PipelineLogger configurado
    """
    return PipelineLogger(
        pipeline_name=pipeline_name,
        log_dir=log_dir,
        console_level=console_level,
        file_level=file_level
    )