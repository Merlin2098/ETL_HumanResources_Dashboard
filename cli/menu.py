"""
Menú interactivo TUI para el sistema ETL
Interfaz guiada usando Questionary para selección de pipelines y opciones
"""
import questionary
from questionary import Style
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from pathlib import Path
from typing import Optional

from utils import get_path_cache, get_logger

console = Console()

# Estilo personalizado para el menú
custom_style = Style([
    ('qmark', 'fg:#673ab7 bold'),
    ('question', 'bold'),
    ('answer', 'fg:#00bcd4 bold'),
    ('pointer', 'fg:#673ab7 bold'),
    ('highlighted', 'fg:#673ab7 bold'),
    ('selected', 'fg:#00bcd4'),
    ('separator', 'fg:#cc5454'),
    ('instruction', 'fg:#858585'),
    ('text', ''),
])


def show_welcome_banner():
    """Muestra banner de bienvenida"""
    banner_text = """
[bold cyan]╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║          SISTEMA ETL - TAWA CONSULTING                    ║
║          HR Data Analytics Pipeline Manager               ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝[/bold cyan]

[dim]Versión 1.0.0 | Bronze → Silver → Gold Architecture[/dim]
"""
    console.print(banner_text)


def show_pipeline_info(pipeline_name: str, description: str):
    """Muestra información sobre un pipeline"""
    console.print(Panel(
        f"[bold]{pipeline_name}[/bold]\n\n{description}",
        border_style="cyan",
        box=box.ROUNDED,
        padding=(1, 2)
    ))


def show_main_menu():
    """
    Menú principal interactivo
    """
    show_welcome_banner()
    
    while True:
        console.print()  # Espacio en blanco
        
        choices = [
            questionary.Choice(
                title="📊 Nóminas (Planillas)",
                value="nomina",
                shortcut_key="1"
            ),
            questionary.Choice(
                title="📋 Relación de Ingresos (PDT)",
                value="ingresos",
                shortcut_key="2"
            ),
            questionary.Choice(
                title="🏥 Exámenes de Retiro",
                value="examen_retiro",
                shortcut_key="3"
            ),
            questionary.Choice(
                title="⛏️  Régimen Minero",
                value="regimen_minero",
                shortcut_key="4"
            ),
            questionary.Choice(
                title="🗄️  Base de Datos (Centros de Costo)",
                value="bd",
                shortcut_key="5"
            ),
            questionary.Separator(),
            questionary.Choice(
                title="🔧 Configuración y Utilidades",
                value="config",
                shortcut_key="c"
            ),
            questionary.Choice(
                title="📊 Ver Estadísticas",
                value="stats",
                shortcut_key="s"
            ),
            questionary.Choice(
                title="❓ Ayuda",
                value="help",
                shortcut_key="h"
            ),
            questionary.Choice(
                title="🚪 Salir",
                value="exit",
                shortcut_key="x"
            )
        ]
        
        selection = questionary.select(
            "Selecciona una opción:",
            choices=choices,
            style=custom_style,
            use_shortcuts=True,
            show_selected=True
        ).ask()
        
        if selection is None or selection == "exit":
            console.print("\n[yellow]¡Hasta pronto![/yellow]\n")
            break
        
        # Ejecutar acción según selección
        if selection == "nomina":
            handle_nomina_pipeline()
        elif selection == "ingresos":
            handle_ingresos_pipeline()
        elif selection == "examen_retiro":
            handle_examen_retiro_pipeline()
        elif selection == "regimen_minero":
            handle_regimen_minero_pipeline()
        elif selection == "bd":
            handle_bd_pipeline()
        elif selection == "config":
            show_config_menu()
        elif selection == "stats":
            show_statistics()
        elif selection == "help":
            show_help()


def handle_nomina_pipeline():
    """Maneja el pipeline de nóminas"""
    show_pipeline_info(
        "Pipeline de Nóminas (Planillas)",
        "Procesa archivos de planillas desde Bronze hasta Gold.\n"
        "Incluye consolidación, validación de esquema y exportación."
    )
    
    # Lazy import del comando
    from cli.commands.nomina_cmd import run_pipeline_interactive
    
    try:
        run_pipeline_interactive()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Pipeline cancelado por el usuario[/yellow]")
    except Exception as e:
        console.print(f"\n[red]✗ Error durante la ejecución: {e}[/red]")
    
    # Pausa antes de volver al menú
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def handle_ingresos_pipeline():
    """Maneja el pipeline de relación de ingresos"""
    show_pipeline_info(
        "Pipeline de Relación de Ingresos (PDT)",
        "Procesa archivos de relación de ingresos para reportes PDT.\n"
        "Transformación Bronze → Silver → Gold."
    )
    
    from cli.commands.ingresos_cmd import run_pipeline_interactive
    
    try:
        run_pipeline_interactive()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Pipeline cancelado por el usuario[/yellow]")
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
    
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def handle_examen_retiro_pipeline():
    """Maneja el pipeline de exámenes de retiro"""
    show_pipeline_info(
        "Pipeline de Exámenes de Retiro",
        "Procesa archivos de exámenes médicos de retiro.\n"
        "Incluye limpieza, enriquecimiento y joins con datos médicos."
    )
    
    from cli.commands.examen_retiro_cmd import run_pipeline_interactive
    
    try:
        run_pipeline_interactive()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Pipeline cancelado por el usuario[/yellow]")
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
    
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def handle_regimen_minero_pipeline():
    """Maneja el pipeline de régimen minero"""
    show_pipeline_info(
        "Pipeline de Régimen Minero",
        "Procesa nóminas específicas del régimen minero.\n"
        "Consolidación y exportación con reglas especiales."
    )
    
    from cli.commands.regimen_minero_cmd import run_pipeline_interactive
    
    try:
        run_pipeline_interactive()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Pipeline cancelado por el usuario[/yellow]")
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
    
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def handle_bd_pipeline():
    """Maneja el pipeline de base de datos"""
    show_pipeline_info(
        "Pipeline de Base de Datos (Centros de Costo)",
        "Gestiona datos maestros de centros de costo.\n"
        "Incluye joins y generación de flags de empleados."
    )
    
    from cli.commands.bd_cmd import run_pipeline_interactive
    
    try:
        run_pipeline_interactive()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Pipeline cancelado por el usuario[/yellow]")
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
    
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def show_config_menu():
    """Menú de configuración y utilidades"""
    console.print()
    
    choices = [
        questionary.Choice("🗄️  Ver cache de rutas", value="cache_show"),
        questionary.Choice("🗑️  Limpiar cache", value="cache_clear"),
        questionary.Choice("📊 Estadísticas del cache", value="cache_stats"),
        questionary.Choice("📝 Ver logs recientes", value="logs"),
        questionary.Choice("✅ Validar configuración", value="validate"),
        questionary.Separator(),
        questionary.Choice("⬅️  Volver al menú principal", value="back")
    ]
    
    selection = questionary.select(
        "Configuración y Utilidades:",
        choices=choices,
        style=custom_style
    ).ask()
    
    if selection == "cache_show":
        cache = get_path_cache()
        console.print("\n" + cache.export_readable())
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()
        
    elif selection == "cache_clear":
        cache = get_path_cache()
        confirm = questionary.confirm(
            "¿Limpiar TODO el cache de rutas?",
            default=False
        ).ask()
        
        if confirm:
            cache.clear_all()
            console.print("[green]✓ Cache limpiado exitosamente[/green]")
        else:
            console.print("[yellow]Operación cancelada[/yellow]")
        
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()
        
    elif selection == "cache_stats":
        cache = get_path_cache()
        stats = cache.get_statistics()
        
        table = Table(title="Estadísticas del Cache", show_header=True, box=box.ROUNDED)
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="white")
        
        table.add_row("Total de rutas guardadas", str(stats['total_last_paths']))
        table.add_row("Rutas válidas", str(stats['valid_last_paths']))
        table.add_row("Keys con directorios frecuentes", str(stats['total_frequent_keys']))
        table.add_row("Última actualización", stats['metadata']['last_updated'][:19])
        
        console.print()
        console.print(table)
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()
        
    elif selection == "logs":
        show_recent_logs()
        
    elif selection == "validate":
        # Importar y ejecutar validación
        import cli.main as main_module
        main_module.validate_setup()
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()


def show_recent_logs():
    """Muestra logs recientes"""
    logs_dir = Path("logs")
    
    if not logs_dir.exists():
        console.print("\n[yellow]⚠ No hay logs disponibles[/yellow]")
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()
        return
    
    log_files = sorted(
        logs_dir.glob("*.log"),
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )[:10]
    
    if not log_files:
        console.print("\n[yellow]⚠ No hay logs disponibles[/yellow]")
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()
        return
    
    # Crear opciones
    choices = []
    for log_file in log_files:
        size_kb = log_file.stat().st_size / 1024
        choices.append(
            questionary.Choice(
                title=f"{log_file.name} ({size_kb:.1f} KB)",
                value=log_file
            )
        )
    choices.append(questionary.Choice("⬅️ Volver", value=None))
    
    selected_log = questionary.select(
        "Selecciona un log para ver:",
        choices=choices,
        style=custom_style
    ).ask()
    
    if selected_log:
        console.print(f"\n[cyan]Mostrando:[/cyan] {selected_log.name}\n")
        console.print("─" * 70)
        
        with open(selected_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[-100:]:  # Últimas 100 líneas
                console.print(line.rstrip())
        
        console.print("─" * 70)
        questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()


def show_statistics():
    """Muestra estadísticas generales del sistema"""
    from utils import get_global_loader
    
    console.print()
    console.print(Panel(
        "[bold cyan]Estadísticas del Sistema[/bold cyan]",
        box=box.ROUNDED
    ))
    
    # Estadísticas del cache
    cache = get_path_cache()
    stats = cache.get_statistics()
    
    table = Table(title="Cache de Rutas", show_header=True, box=box.SIMPLE)
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", style="white", justify="right")
    
    table.add_row("Rutas guardadas", str(stats['total_last_paths']))
    table.add_row("Rutas válidas", str(stats['valid_last_paths']))
    table.add_row("Directorios frecuentes", str(stats['total_frequent_keys']))
    
    console.print(table)
    
    # Estadísticas de logs
    logs_dir = Path("logs")
    if logs_dir.exists():
        log_count = len(list(logs_dir.glob("*.log")))
        total_size = sum(f.stat().st_size for f in logs_dir.glob("*.log"))
        
        table = Table(title="Logs del Sistema", show_header=True, box=box.SIMPLE)
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="white", justify="right")
        
        table.add_row("Total de logs", str(log_count))
        table.add_row("Tamaño total", f"{total_size / (1024*1024):.2f} MB")
        
        console.print(table)
    
    # Performance de lazy loading
    loader = get_global_loader()
    perf_stats = loader.get_performance_stats()
    
    if perf_stats['total_modules_loaded'] > 0:
        table = Table(title="Lazy Loading Performance", show_header=True, box=box.SIMPLE)
        table.add_column("Métrica", style="cyan")
        table.add_column("Valor", style="white", justify="right")
        
        table.add_row("Módulos cargados", str(perf_stats['total_modules_loaded']))
        table.add_row("Tiempo total", f"{perf_stats['total_load_time']*1000:.1f} ms")
        table.add_row("Tiempo promedio", f"{perf_stats['average_load_time']*1000:.1f} ms")
        
        console.print(table)
    
    console.print()
    questionary.press_any_key_to_continue("Presiona cualquier tecla para continuar...").ask()


def show_help():
    """Muestra ayuda del sistema"""
    help_text = """
[bold cyan]Sistema ETL - Tawa Consulting[/bold cyan]
[bold]Guía de Uso[/bold]

[yellow]PIPELINES DISPONIBLES:[/yellow]

  📊 [bold]Nóminas (Planillas)[/bold]
     Procesa archivos de planillas mensuales.
     Bronze → Silver (consolidación) → Gold (analítica)

  📋 [bold]Relación de Ingresos (PDT)[/bold]
     Genera reportes de relación de ingresos.
     Transformaciones específicas para PDT.

  🏥 [bold]Exámenes de Retiro[/bold]
     Procesa programación de exámenes médicos.
     Incluye limpieza y joins con datos médicos.

  ⛏️  [bold]Régimen Minero[/bold]
     Nóminas con reglas especiales del régimen minero.
     Validaciones y cálculos específicos.

  🗄️  [bold]Base de Datos (Centros de Costo)[/bold]
     Gestión de datos maestros y flags de empleados.
     Joins con información organizacional.

[yellow]CARACTERÍSTICAS:[/yellow]

  • Cache inteligente de rutas frecuentes
  • Logging detallado de cada ejecución
  • Validación de esquemas JSON
  • Lazy loading de módulos para mejor performance
  • Sistema de archivos versionado (actual + histórico)

[yellow]COMANDOS CLI:[/yellow]

  [cyan]tawa-etl menu[/cyan]         Menú interactivo (este menú)
  [cyan]tawa-etl nomina[/cyan]       Ejecutar pipeline de nóminas
  [cyan]tawa-etl cache show[/cyan]   Ver cache de rutas
  [cyan]tawa-etl logs[/cyan]         Ver logs del sistema
  [cyan]tawa-etl validate[/cyan]     Validar configuración

[dim]Tip: Usa --help en cualquier comando para ver opciones adicionales[/dim]
"""
    
    console.print(Panel(
        help_text,
        border_style="cyan",
        box=box.ROUNDED,
        padding=(1, 2)
    ))
    
    questionary.press_any_key_to_continue("\nPresiona cualquier tecla para continuar...").ask()


if __name__ == "__main__":
    # Para testing
    show_main_menu()