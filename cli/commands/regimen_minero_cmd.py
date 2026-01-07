"""
Comando CLI para el pipeline de Régimen Minero
Template base - adaptar según implementación real
"""
from pathlib import Path
from typing import Optional
import typer
from rich.console import Console

from utils import get_logger, get_global_loader, quick_file_select, quick_dir_select

console = Console()


def run_pipeline(
    input_file: Optional[Path] = typer.Option(None, "--input", "-i"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o")
):
    """⛏️  Pipeline de Régimen Minero"""
    run_pipeline_with_params(input_file, output_dir)


def run_pipeline_interactive():
    run_pipeline_with_params(None, None)


def run_pipeline_with_params(input_file: Optional[Path], output_dir: Optional[Path]):
    logger = get_logger("regimen_minero", console_level=20)
    loader = get_global_loader(logger)
    
    console.print("\n[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]   PIPELINE DE RÉGIMEN MINERO                              [/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]\n")
    
    try:
        # Validación
        required_modules = [
            'nomina_regimen_minero.step1_consolidar_regimen_minero',
            'nomina_regimen_minero.step2_exportar_regimen_minero'
        ]
        
        for module in required_modules:
            if not loader.validate_dependencies(module):
                logger.error(f"✗ Módulo faltante: {module}")
                return
        
        # Selección de archivos
        if input_file is None:
            input_file = quick_file_select(
                cache_key="regimen_minero_input",
                prompt="📄 Selecciona el archivo de entrada",
                logger=logger
            )
            if input_file is None:
                return
        
        if output_dir is None:
            output_dir = quick_dir_select(
                cache_key="regimen_minero_output",
                prompt="📁 Directorio de salida",
                logger=logger
            )
            if output_dir is None:
                return
        
        # Ejecutar pipeline
        logger.log_step_start("STEP 1: Consolidación", "")
        logger.info("✓ Consolidación completada (simulado)")
        logger.log_step_end("STEP 1: Consolidación", True)
        
        logger.log_step_start("STEP 2: Exportación", "")
        logger.info("✓ Exportación completada (simulado)")
        logger.log_step_end("STEP 2: Exportación", True)
        
        console.print("\n[bold green]✓ PIPELINE COMPLETADO[/bold green]\n")
        
    except Exception as e:
        logger.log_error_with_context(e, "Pipeline régimen minero")
        console.print(f"\n[red]✗ Error: {e}[/red]\n")