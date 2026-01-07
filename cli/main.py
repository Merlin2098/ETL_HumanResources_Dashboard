"""
Entry point principal del CLI - Sistema ETL Tawa Consulting
Interfaz de línea de comandos con soporte para modo interactivo y comandos directos
"""
import typer
from rich.console import Console
from rich.panel import Panel
from rich import box
from pathlib import Path
from typing import Optional

# Importar submódulos de comandos (lazy import al final del archivo)
from cli import menu

console = Console()
app = typer.Typer(
    name="tawa-etl",
    help="Sistema ETL para HR Data Analytics - Tawa Consulting",
    add_completion=False,
    rich_markup_mode="rich"
)


def version_callback(value: bool):
    """Callback para mostrar versión"""
    if value:
        console.print("[cyan]Tawa ETL[/cyan] - versión [bold]1.0.0[/bold]")
        console.print("Sistema de pipelines ETL para HR Data Analytics")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Mostrar versión del sistema"
    )
):
    """
    Sistema ETL Tawa Consulting
    
    Administra pipelines de datos para:
    - Nóminas (Planillas)
    - Relación de Ingresos (PDT)
    - Exámenes de Retiro
    - Régimen Minero
    - Base de Datos (Centros de Costo)
    """
    pass


@app.command(name="menu")
def interactive_menu():
    """
    🎯 Inicia el menú interactivo guiado (TUI)
    
    Interfaz visual que permite seleccionar y ejecutar pipelines
    de forma guiada con opciones interactivas.
    """
    console.print(Panel.fit(
        "[bold cyan]Iniciando menú interactivo...[/bold cyan]",
        border_style="cyan",
        box=box.ROUNDED
    ))
    menu.show_main_menu()


@app.command(name="cache")
def manage_cache(
    action: str = typer.Argument(
        ...,
        help="Acción a realizar: [show|clear|stats]"
    ),
    key: Optional[str] = typer.Option(
        None,
        "--key",
        "-k",
        help="Key específica del cache a gestionar"
    )
):
    """
    🗄️ Gestiona el cache de rutas del sistema
    
    Acciones disponibles:
    - show: Muestra el contenido del cache
    - clear: Limpia el cache (todo o una key específica)
    - stats: Muestra estadísticas del cache
    """
    from utils import get_path_cache
    from rich.table import Table
    
    cache = get_path_cache()
    
    if action == "show":
        console.print("\n" + cache.export_readable())
        
    elif action == "clear":
        if key:
            cache.clear_key(key)
            console.print(f"[green]✓[/green] Cache limpiado para key: [cyan]{key}[/cyan]")
        else:
            confirm = typer.confirm("¿Limpiar TODO el cache?", default=False)
            if confirm:
                cache.clear_all()
                console.print("[green]✓[/green] Cache completamente limpiado")
            else:
                console.print("[yellow]Operación cancelada[/yellow]")
                
    elif action == "stats":
        stats = cache.get_statistics()
        
        table = Table(title="Estadísticas del Cache", show_header=True, box=box.ROUNDED)
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="white")
        
        table.add_row("Total de rutas", str(stats['total_last_paths']))
        table.add_row("Rutas válidas", str(stats['valid_last_paths']))
        table.add_row("Keys frecuentes", str(stats['total_frequent_keys']))
        table.add_row("Última actualización", stats['metadata']['last_updated'][:19])
        
        console.print(table)
        
    else:
        console.print(f"[red]✗[/red] Acción desconocida: {action}")
        console.print("Acciones disponibles: show, clear, stats")


@app.command(name="logs")
def view_logs(
    pipeline: Optional[str] = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Filtrar logs por pipeline específico"
    ),
    last: int = typer.Option(
        10,
        "--last",
        "-n",
        help="Cantidad de logs recientes a mostrar"
    ),
    tail: bool = typer.Option(
        False,
        "--tail",
        "-t",
        help="Mostrar las últimas líneas del log más reciente"
    )
):
    """
    📝 Visualiza logs del sistema
    
    Muestra logs de ejecuciones previas organizados por pipeline.
    """
    from rich.table import Table
    import os
    from datetime import datetime
    
    logs_dir = Path("logs")
    
    if not logs_dir.exists():
        console.print("[yellow]⚠[/yellow] No hay logs disponibles")
        return
    
    # Obtener archivos de log
    log_files = list(logs_dir.glob("*.log"))
    
    if pipeline:
        log_files = [f for f in log_files if f.stem.startswith(pipeline)]
    
    if not log_files:
        console.print(f"[yellow]⚠[/yellow] No se encontraron logs para: {pipeline or 'cualquier pipeline'}")
        return
    
    # Ordenar por fecha de modificación (más reciente primero)
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    log_files = log_files[:last]
    
    if tail:
        # Mostrar últimas líneas del log más reciente
        latest_log = log_files[0]
        console.print(f"\n[cyan]Log:[/cyan] {latest_log.name}\n")
        
        with open(latest_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[-50:]:  # Últimas 50 líneas
                console.print(line.rstrip())
    else:
        # Mostrar tabla de logs disponibles
        table = Table(
            title=f"Logs Disponibles{f' - {pipeline}' if pipeline else ''}",
            show_header=True,
            box=box.ROUNDED
        )
        table.add_column("Pipeline", style="cyan")
        table.add_column("Fecha", style="white")
        table.add_column("Tamaño", style="yellow")
        table.add_column("Archivo", style="dim")
        
        for log_file in log_files:
            # Extraer info del nombre
            parts = log_file.stem.split('_', 1)
            pipeline_name = parts[0]
            timestamp = parts[1] if len(parts) > 1 else "unknown"
            
            # Formatear fecha
            try:
                dt = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                date_str = timestamp
            
            # Tamaño
            size_kb = log_file.stat().st_size / 1024
            size_str = f"{size_kb:.1f} KB"
            
            table.add_row(pipeline_name, date_str, size_str, log_file.name)
        
        console.print(table)
        console.print(f"\n[dim]Tip: Usa --tail para ver el contenido del log más reciente[/dim]")


@app.command(name="validate")
def validate_setup():
    """
    ✅ Valida la configuración del sistema
    
    Verifica:
    - Dependencias de Python instaladas
    - Estructura de directorios
    - Archivos de configuración
    - Esquemas JSON disponibles
    """
    from utils import get_logger, get_global_loader
    from rich.table import Table
    
    logger = get_logger("validation")
    loader = get_global_loader(logger)
    
    console.print("\n[bold cyan]Validando configuración del sistema...[/bold cyan]\n")
    
    issues = []
    
    # 1. Validar dependencias
    console.print("📦 [bold]Validando dependencias de Python...[/bold]")
    required_packages = ['polars', 'openpyxl', 'duckdb', 'typer', 'rich', 'questionary']
    
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Status", style="white")
    table.add_column("Package", style="white")
    
    for package in required_packages:
        is_valid = loader.validate_dependencies(package)
        status = "[green]✓[/green]" if is_valid else "[red]✗[/red]"
        table.add_row(status, package)
        
        if not is_valid:
            issues.append(f"Paquete faltante: {package}")
    
    console.print(table)
    
    # 2. Validar estructura de directorios
    console.print("\n📁 [bold]Validando estructura de directorios...[/bold]")
    required_dirs = [
        'bd', 'examen_retiro', 'nomina', 'nomina_regimen_minero', 'pdt',
        'esquemas', 'queries', 'utils'
    ]
    
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Status", style="white")
    table.add_column("Directory", style="white")
    
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        exists = dir_path.exists() and dir_path.is_dir()
        status = "[green]✓[/green]" if exists else "[red]✗[/red]"
        table.add_row(status, dir_name)
        
        if not exists:
            issues.append(f"Directorio faltante: {dir_name}")
    
    console.print(table)
    
    # 3. Validar esquemas JSON
    console.print("\n📋 [bold]Validando esquemas JSON...[/bold]")
    schemas_dir = Path("esquemas")
    
    if schemas_dir.exists():
        schemas = list(schemas_dir.glob("*.json"))
        console.print(f"   Encontrados: [cyan]{len(schemas)}[/cyan] esquemas")
    else:
        console.print("   [red]Directorio de esquemas no encontrado[/red]")
        issues.append("Directorio 'esquemas' no existe")
    
    # Resumen final
    console.print("\n" + "="*60)
    if not issues:
        console.print("[bold green]✓ Configuración válida - Sistema listo para usar[/bold green]")
    else:
        console.print("[bold red]✗ Se encontraron problemas:[/bold red]\n")
        for issue in issues:
            console.print(f"  • {issue}")
        console.print("\n[yellow]Soluciona estos problemas antes de ejecutar pipelines[/yellow]")
    console.print("="*60 + "\n")


# Lazy import de comandos de pipelines (se cargan cuando se necesitan)
def register_pipeline_commands():
    """Registra comandos de pipelines de forma lazy"""
    from cli.commands import (
        nomina_cmd,
        examen_retiro_cmd,
        regimen_minero_cmd,
        ingresos_cmd,
        bd_cmd
    )
    
    # Registrar cada comando
    app.command(name="nomina")(nomina_cmd.run_pipeline)
    app.command(name="examen-retiro")(examen_retiro_cmd.run_pipeline)
    app.command(name="regimen-minero")(regimen_minero_cmd.run_pipeline)
    app.command(name="ingresos")(ingresos_cmd.run_pipeline)
    app.command(name="bd")(bd_cmd.run_pipeline)


if __name__ == "__main__":
    # Registrar comandos de pipelines
    register_pipeline_commands()
    
    # Ejecutar app
    app()