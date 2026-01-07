"""
Comando CLI para el pipeline de Exámenes de Retiro
Template base - adaptar según la implementación real del pipeline
"""
from pathlib import Path
from typing import Optional
import typer
from rich.console import Console

from utils import (
    get_logger,
    get_global_loader,
    quick_file_select,
    quick_dir_select
)

console = Console()


def run_pipeline(
    input_file: Optional[Path] = typer.Option(
        None,
        "--input",
        "-i",
        help="Archivo de entrada"
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Directorio de salida"
    )
):
    """
    🏥 Pipeline de Exámenes de Retiro
    
    Procesa archivos de programación de exámenes médicos de retiro.
    Incluye limpieza, enriquecimiento y joins.
    """
    run_pipeline_with_params(input_file, output_dir)


def run_pipeline_interactive():
    """Versión interactiva para menú TUI"""
    run_pipeline_with_params(None, None)


def run_pipeline_with_params(
    input_file: Optional[Path],
    output_dir: Optional[Path]
):
    """Lógica principal del pipeline"""
    logger = get_logger("examen_retiro", console_level=20)
    loader = get_global_loader(logger)
    
    console.print("\n[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]   PIPELINE DE EXÁMENES DE RETIRO                          [/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]\n")
    
    try:
        # Validar dependencias
        logger.log_step_start("Validación de Dependencias", "Verificando módulos")
        
        required_modules = [
            'examen_retiro.step1_clean',
            'examen_retiro.step2_gold',
            'examen_retiro.step3_join'
        ]
        
        for module in required_modules:
            if not loader.validate_dependencies(module):
                logger.error(f"✗ Módulo faltante: {module}")
                return
        
        logger.log_step_end("Validación de Dependencias", success=True)
        
        # Selección de archivos
        if input_file is None:
            input_file = quick_file_select(
                cache_key="examen_retiro_input",
                prompt="📄 Selecciona el archivo de entrada",
                allowed_extensions=['.xlsx', '.csv'],
                logger=logger
            )
            if input_file is None:
                return
        
        if output_dir is None:
            output_dir = quick_dir_select(
                cache_key="examen_retiro_output",
                prompt="📁 Selecciona el directorio de salida",
                logger=logger
            )
            if output_dir is None:
                return
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Ejecutar steps con lazy loading
        logger.log_step_start("STEP 1: Limpieza", "Procesando datos")
        
        # Aquí irían las llamadas reales a tus funciones
        # clean_func = loader.import_function('examen_retiro.step1_clean', 'clean_data')
        # df_clean = clean_func(input_file)
        
        logger.info("✓ Limpieza completada (simulado)")
        logger.log_step_end("STEP 1: Limpieza", success=True)
        
        logger.log_step_start("STEP 2: Gold", "Enriquecimiento de datos")
        # gold_func = loader.import_function('examen_retiro.step2_gold', 'to_gold')
        # df_gold = gold_func(df_clean)
        logger.info("✓ Capa Gold generada (simulado)")
        logger.log_step_end("STEP 2: Gold", success=True)
        
        logger.log_step_start("STEP 3: Join", "Uniendo con datos médicos")
        # join_func = loader.import_function('examen_retiro.step3_join', 'join_data')
        # df_final = join_func(df_gold)
        logger.info("✓ Join completado (simulado)")
        logger.log_step_end("STEP 3: Join", success=True)
        
        console.print("\n[bold green]✓ PIPELINE COMPLETADO[/bold green]\n")
        console.print(f"📝 Log: [cyan]{logger.get_log_path()}[/cyan]\n")
        
    except Exception as e:
        logger.log_error_with_context(e, "Pipeline examen retiro")
        console.print(f"\n[red]✗ Error: {e}[/red]\n")