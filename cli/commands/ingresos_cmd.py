"""
Comando CLI para el pipeline de Relación de Ingresos (PDT)
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
    """📋 Pipeline de Relación de Ingresos (PDT)"""
    run_pipeline_with_params(input_file, output_dir)


def run_pipeline_interactive():
    run_pipeline_with_params(None, None)


def run_pipeline_with_params(input_file: Optional[Path], output_dir: Optional[Path]):
    logger = get_logger("ingresos", console_level=20)
    loader = get_global_loader(logger)
    
    console.print("\n[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]   PIPELINE DE RELACIÓN DE INGRESOS (PDT)                  [/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════════════[/bold cyan]\n")
    
    try:
        required_modules = [
            'pdt.step1_consolidar_ingresos',
            'pdt.step2_exportar_ingresos'
        ]
        
        for module in required_modules:
            if not loader.validate_dependencies(module):
                logger.error(f"✗ Módulo faltante: {module}")
                return
        
        if input_file is None:
            input_file = quick_file_select(
                cache_key="ingresos_input",
                prompt="📄 Selecciona el archivo",
                logger=logger
            )
            if input_file is None:
                return
        
        if output_dir is None:
            output_dir = quick_dir_select(
                cache_key="ingresos_output",
                prompt="📁 Directorio de salida",
                logger=logger
            )
            if output_dir is None:
                return
        
        logger.log_step_start("STEP 1: Consolidación", "")
        logger.info("✓ Completado (simulado)")
        logger.log_step_end("STEP 1: Consolidación", True)
        
        logger.log_step_start("STEP 2: Exportación", "")
        logger.info("✓ Completado (simulado)")
        logger.log_step_end("STEP 2: Exportación", True)
        
        console.print("\n[bold green]✓ PIPELINE COMPLETADO[/bold green]\n")
        
    except Exception as e:
        logger.log_error_with_context(e, "Pipeline ingresos")
        console.print(f"\n[red]✗ Error: {e}[/red]\n")