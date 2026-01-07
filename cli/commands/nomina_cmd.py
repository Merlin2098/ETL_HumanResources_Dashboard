"""
Comando CLI para el pipeline de Nóminas (Planillas)
Bronze → Silver → Gold completo
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
    input_dir: Optional[Path] = typer.Option(
        None,
        "--input-dir",
        "-i",
        help="[Ignorado] Siempre usa explorador para carpeta Bronze"
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Carpeta base de salida (se crearán silver/ y gold/)"
    ),
    schema_json: Optional[Path] = typer.Option(
        None,
        "--schema",
        "-s",
        help="Archivo JSON con esquema Gold"
    ),
    skip_validation: bool = typer.Option(
        False,
        "--skip-validation",
        help="Omitir validación de constraints"
    ),
    only_bronze_to_silver: bool = typer.Option(
        False,
        "--only-bronze-silver",
        help="Ejecutar solo Bronze → Silver (sin Gold)"
    ),
    only_silver_to_gold: bool = typer.Option(
        False,
        "--only-silver-gold",
        help="Ejecutar solo Silver → Gold"
    ),
    export_excel: bool = typer.Option(
        True,
        "--excel/--no-excel",
        help="Exportar también en formato Excel"
    )
):
    """
    📊 Pipeline de Nóminas (Planillas)
    
    Pipeline completo Bronze → Silver → Gold:
    - Selecciona la carpeta con archivos Excel Bronze
    - Se crean automáticamente carpetas silver/ y gold/ en esa ubicación
    - Consolidación de múltiples Excel mensuales
    - Validación de esquema (usa esquema_nominas.json automáticamente si existe)
    - Generación de columnas derivadas (MES, AÑO, NOMBRE_MES)
    - Versionamiento automático (actual/ + historico/)
    - Exportación a Parquet y Excel
    
    Estructura generada:
        carpeta_bronze/
        ├── archivo1.xlsx, archivo2.xlsx... (Bronze)
        ├── silver/                  (se crea automáticamente)
        │   └── Planilla Metso Consolidado.parquet/.xlsx
        └── gold/                    (se crea automáticamente)
            ├── actual/
            │   └── Planilla Metso BI_Gold.parquet/.xlsx
            └── historico/
                └── (versiones anteriores)
    
    Ejemplos:
        # Pipeline completo (se abre explorador para carpeta Bronze)
        tawa-etl nomina
        
        # Solo Bronze → Silver
        tawa-etl nomina --only-bronze-silver
        
        # Solo Silver → Gold (se abre explorador para parquet Silver)
        tawa-etl nomina --only-silver-gold
    """
    run_pipeline_with_params(
        input_dir=input_dir,
        output_dir=output_dir,
        schema_json=schema_json,
        skip_validation=skip_validation,
        only_bronze_to_silver=only_bronze_to_silver,
        only_silver_to_gold=only_silver_to_gold,
        export_excel=export_excel
    )


def run_pipeline_interactive():
    """
    Versión interactiva del pipeline (llamada desde el menú TUI)
    """
    run_pipeline_with_params(
        input_dir=None,
        output_dir=None,
        schema_json=None,
        skip_validation=False,
        only_bronze_to_silver=False,
        only_silver_to_gold=False,
        export_excel=True
    )


def run_pipeline_with_params(
    input_dir: Optional[Path],
    output_dir: Optional[Path],
    schema_json: Optional[Path],
    skip_validation: bool,
    only_bronze_to_silver: bool,
    only_silver_to_gold: bool,
    export_excel: bool
):
    """
    Lógica principal del pipeline con parámetros configurables
    """
    # Inicializar logger
    logger = get_logger("nomina", console_level=20)
    loader = get_global_loader(logger)
    
    console.print("\n[bold cyan]╔═══════════════════════════════════════════════════════╗[/bold cyan]")
    console.print("[bold cyan]║   PIPELINE DE NÓMINAS - PLANILLAS METSO              ║[/bold cyan]")
    console.print("[bold cyan]╚═══════════════════════════════════════════════════════╝[/bold cyan]\n")
    
    try:
        # VALIDAR DEPENDENCIAS
        logger.log_step_start(
            "Validación de Dependencias",
            "Verificar módulos requeridos están disponibles"
        )
        
        required_modules = ['nomina.api_step1', 'nomina.api_step2']
        
        for module in required_modules:
            if not loader.validate_dependencies(module):
                logger.error(f"✗ Módulo faltante: {module}")
                console.print(f"\n[red]Error: El módulo {module} no está disponible[/red]")
                console.print("[yellow]Verifica que la carpeta 'nomina' contenga api_step1.py y api_step2.py[/yellow]\n")
                return
        
        logger.log_step_end("Validación de Dependencias", success=True)
        
        # CONFIGURACIÓN DE RUTAS (SIEMPRE CON EXPLORADOR)
        if only_silver_to_gold:
            # Modo: Solo Silver → Gold
            logger.log_step_start(
                "Modo: Silver → Gold",
                "Ejecutar solo transformación Gold"
            )
            
            # SIEMPRE abrir explorador para seleccionar parquet Silver
            parquet_silver = quick_file_select(
                cache_key="nomina_silver_parquet",
                prompt="📄 Selecciona el archivo Parquet Silver",
                allowed_extensions=['.parquet'],
                logger=logger
            )
            
            if parquet_silver is None:
                logger.error("No se seleccionó archivo Parquet Silver")
                console.print("\n[yellow]⚠ Operación cancelada[/yellow]\n")
                return
            
            logger.log_file_processing(parquet_silver, "Archivo Silver")
            
            # Carpeta base para gold (un nivel arriba de silver)
            carpeta_base = parquet_silver.parent.parent
            
        else:
            # Modo: Bronze → Silver (o completo)
            logger.log_step_start(
                "Configuración de Rutas",
                "Selección de archivos de entrada y salida"
            )
            
            # SIEMPRE abrir explorador para seleccionar carpeta Bronze
            input_dir = quick_dir_select(
                cache_key="nomina_bronze_dir",
                prompt="📁 Selecciona la carpeta con archivos Excel Bronze",
                logger=logger
            )
            
            if input_dir is None:
                logger.error("No se seleccionó carpeta de entrada")
                console.print("\n[yellow]⚠ Operación cancelada[/yellow]\n")
                return
            
            input_dir = Path(input_dir)
            logger.info(f"Carpeta Bronze: [cyan]{input_dir}[/cyan]")
            
            # Buscar archivos Excel
            archivos_excel = list(input_dir.glob('*.xlsx')) + list(input_dir.glob('*.xls'))
            archivos_excel = [
                f for f in archivos_excel 
                if not f.name.startswith('~$') 
                and not f.name.startswith('Planilla Metso Consolidado')
            ]
            
            if not archivos_excel:
                logger.error("No se encontraron archivos Excel en la carpeta")
                console.print("\n[red]Error: No hay archivos Excel válidos en la carpeta seleccionada[/red]\n")
                return
            
            logger.info(f"Archivos Excel encontrados: [cyan]{len(archivos_excel)}[/cyan]")
            
            # output_dir es la misma carpeta donde están los archivos Bronze
            output_dir = input_dir
            carpeta_base = output_dir
            
            logger.log_step_end("Configuración de Rutas", success=True)
        
        # STEP 1: BRONZE → SILVER
        if not only_silver_to_gold:
            logger.log_step_start(
                "STEP 1: Bronze → Silver",
                f"Consolidar {len(archivos_excel)} archivo(s) Excel"
            )
            
            # Lazy import
            consolidar_func = loader.import_function(
                'nomina.api_step1',
                'consolidar_planillas_bronze_to_silver'
            )
            
            if consolidar_func is None:
                raise ImportError("No se pudo importar consolidar_planillas_bronze_to_silver")
            
            # Ejecutar consolidación
            df_silver, parquet_silver, excel_silver = consolidar_func(
                archivos_bronze=archivos_excel,
                output_dir=carpeta_base,
                logger=logger
            )
            
            logger.log_step_end("STEP 1: Bronze → Silver", success=True)
            
            console.print(f"\n[green]✓[/green] Silver generado: [cyan]{parquet_silver}[/cyan]")
            
            if only_bronze_to_silver:
                # Resumen y salir
                console.print("\n[bold green]✓ PIPELINE COMPLETADO: Bronze → Silver[/bold green]\n")
                console.print("📊 [bold]Archivos generados:[/bold]")
                console.print(f"   • {parquet_silver}")
                console.print(f"   • {excel_silver}")
                console.print(f"\n📝 [bold]Log:[/bold] [cyan]{logger.get_log_path()}[/cyan]\n")
                loader.print_performance_report()
                return
        
        # STEP 2: SILVER → GOLD
        logger.log_step_start(
            "STEP 2: Silver → Gold",
            "Aplicar esquema y validaciones"
        )
        
        # Seleccionar esquema JSON
        if schema_json is None:
            # Buscar carpeta esquemas
            carpeta_esquemas = None
            carpeta_actual = Path.cwd()
            
            for _ in range(4):
                posible_esquemas = carpeta_actual / "esquemas"
                if posible_esquemas.exists() and posible_esquemas.is_dir():
                    carpeta_esquemas = posible_esquemas
                    break
                carpeta_actual = carpeta_actual.parent
            
            if carpeta_esquemas is None:
                logger.error("No se encontró carpeta 'esquemas'")
                console.print("\n[red]Error: No se encontró la carpeta 'esquemas'[/red]")
                console.print("[yellow]Coloca los archivos JSON de esquemas en una carpeta 'esquemas/'[/yellow]\n")
                return
            
            # Buscar esquema de nóminas
            esquema_nominas = carpeta_esquemas / "esquema_nominas.json"
            
            if not esquema_nominas.exists():
                logger.warning("esquema_nominas.json no encontrado, seleccionando manualmente...")
                schema_json = quick_file_select(
                    cache_key="nomina_schema_json",
                    prompt="📋 Selecciona el esquema JSON Gold",
                    allowed_extensions=['.json'],
                    logger=logger
                )
                
                if schema_json is None:
                    logger.error("No se seleccionó esquema JSON")
                    console.print("\n[yellow]⚠ Operación cancelada[/yellow]\n")
                    return
            else:
                schema_json = esquema_nominas
        
        schema_json = Path(schema_json)
        logger.log_file_processing(schema_json, "Esquema Gold")
        
        # Lazy import
        exportar_func = loader.import_function(
            'nomina.api_step2',
            'exportar_silver_to_gold'
        )
        
        if exportar_func is None:
            raise ImportError("No se pudo importar exportar_silver_to_gold")
        
        # Ejecutar transformación Gold
        df_gold, parquet_gold, excel_gold = exportar_func(
            parquet_silver=parquet_silver,
            schema_json=schema_json,
            carpeta_base=carpeta_base,
            skip_validation=skip_validation,
            export_excel=export_excel,
            logger=logger
        )
        
        logger.log_step_end("STEP 2: Silver → Gold", success=True)
        
        # RESUMEN FINAL
        console.print("\n[bold green]✓ PIPELINE COMPLETADO EXITOSAMENTE[/bold green]\n")
        
        console.print("📊 [bold]Estadísticas:[/bold]")
        console.print(f"   • Registros finales: [cyan]{len(df_gold):,}[/cyan]")
        console.print(f"   • Columnas Gold: [cyan]{len(df_gold.columns)}[/cyan]")
        
        if not only_silver_to_gold:
            console.print(f"   • Archivos Excel procesados: [cyan]{len(archivos_excel)}[/cyan]")
        
        console.print(f"\n📁 [bold]Archivos generados:[/bold]")
        
        if not only_silver_to_gold:
            console.print(f"\n   [dim]Silver:[/dim]")
            console.print(f"   • {parquet_silver}")
        
        console.print(f"\n   [dim]Gold (actual/):[/dim]")
        console.print(f"   • {parquet_gold}")
        if excel_gold:
            console.print(f"   • {excel_gold}")
        
        console.print(f"\n📝 [bold]Log:[/bold] [cyan]{logger.get_log_path()}[/cyan]")
        
        console.print(f"\n💡 [dim]Power BI debe apuntar a: {parquet_gold.parent}/[/dim]")
        console.print(f"💡 [dim]Versiones históricas en: {parquet_gold.parent.parent}/historico/[/dim]\n")
        
        # Estadísticas de performance
        loader.print_performance_report()
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrumpido por el usuario")
        console.print("\n[yellow]⚠ Pipeline cancelado[/yellow]\n")
        
    except Exception as e:
        logger.log_error_with_context(e, "Pipeline de nóminas")
        console.print("\n[bold red]✗ PIPELINE FINALIZADO CON ERRORES[/bold red]")
        console.print(f"[red]Error: {str(e)}[/red]\n")
        console.print(f"[dim]Ver detalles en: {logger.get_log_path()}[/dim]\n")
        raise