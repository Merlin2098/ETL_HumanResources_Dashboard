#!/usr/bin/env python3
"""
Script: Generación de Flags en Capa Gold
Aplica reglas de negocio para edad y contratos sobre parquet Gold de empleados
Toda la lógica de cálculo se ejecuta en DuckDB mediante queries SQL externas
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime
import time
import tkinter as tk
from tkinter import filedialog
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment


def select_file(title: str = "Seleccionar archivo") -> Path | None:
    """Abre un explorador de archivos para seleccionar un archivo."""
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    file_path = filedialog.askopenfilename(
        title=title,
        filetypes=[
            ("Parquet files", "*.parquet"),
            ("All files", "*.*")
        ]
    )
    
    root.destroy()
    
    return Path(file_path) if file_path else None


def find_queries_folder() -> Path | None:
    """
    Busca la carpeta 'queries' en el directorio actual y hasta 3 niveles arriba.
    
    Returns:
        Path de la carpeta queries o None si no se encuentra
    """
    carpeta_actual = Path.cwd()
    
    # Buscar en el directorio actual y hasta 3 niveles arriba
    for _ in range(4):
        posible_queries = carpeta_actual / "queries"
        if posible_queries.exists() and posible_queries.is_dir():
            return posible_queries
        carpeta_actual = carpeta_actual.parent
    
    return None


def load_sql_query() -> str:
    """
    Busca la carpeta 'queries' y abre un explorador de archivos para que el usuario
    seleccione el archivo SQL con las queries de flags.
    
    Returns:
        String con el contenido del archivo SQL
    """
    # Buscar carpeta de queries
    carpeta_queries = find_queries_folder()
    
    if carpeta_queries is None:
        raise FileNotFoundError(
            f"No se encontró la carpeta 'queries' en el proyecto.\n"
            f"Asegúrate de que exista la carpeta 'queries' en la raíz del proyecto."
        )
    
    # Listar archivos SQL disponibles
    queries_disponibles = list(carpeta_queries.glob("*.sql"))
    
    if queries_disponibles:
        print(f"📁 Carpeta de queries: {carpeta_queries}")
        print(f"✓ Archivos SQL disponibles:")
        for i, query_file in enumerate(queries_disponibles, 1):
            print(f"   {i}. {query_file.name}")
        print()
    
    # Abrir explorador para seleccionar el archivo SQL
    print("📄 Seleccione el archivo SQL de queries de flags...")
    
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    ruta_sql = filedialog.askopenfilename(
        title="Seleccione el archivo SQL de queries",
        initialdir=str(carpeta_queries) if carpeta_queries else None,
        filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
    )
    root.destroy()
    
    if not ruta_sql:
        raise ValueError("No se seleccionó archivo SQL. Operación cancelada.")
    
    sql_path = Path(ruta_sql)
    print(f"✓ Query SQL seleccionada: {sql_path.name}")
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    return query


def validar_columnas_requeridas(df: pl.DataFrame) -> bool:
    """
    Valida que el DataFrame tenga las columnas requeridas para aplicar flags.
    
    Args:
        df: DataFrame de Polars
    
    Returns:
        True si todas las columnas requeridas existen
    """
    columnas_requeridas = [
        "NUMERO DE DOC",
        "FECHA DE NAC.",
        "FECH_INGR.",
        "Fecha de Termino",
        "Modalidad de Contrato"
    ]
    
    columnas_presentes = df.columns
    columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_presentes]
    
    if columnas_faltantes:
        print(f"\n❌ COLUMNAS FALTANTES:")
        for col in columnas_faltantes:
            print(f"   • {col}")
        return False
    
    print(f"✓ Todas las columnas requeridas están presentes")
    return True


def aplicar_flags_duckdb(df: pl.DataFrame, query: str) -> pl.DataFrame:
    """
    Ejecuta las queries SQL en DuckDB para calcular todas las flags.
    
    Args:
        df: DataFrame de Polars con datos originales
        query: String con la query SQL completa
    
    Returns:
        DataFrame con las flags aplicadas
    """
    print(f"\n📊 Ejecutando queries SQL en DuckDB...")
    print("-" * 70)
    
    # Conectar a DuckDB
    con = duckdb.connect(":memory:")
    
    # Registrar el DataFrame de Polars en DuckDB
    con.register("empleados", df)
    
    # Ejecutar query y obtener resultado
    df_con_flags = con.execute(query).pl()
    
    # Contar flags aplicadas (solo booleanas)
    flags_cols = [col for col in df_con_flags.columns if col not in df.columns]
    
    print(f"✓ Query ejecutada exitosamente")
    print(f"✓ Columnas de flags generadas: {len(flags_cols)}")
    
    for flag_col in flags_cols:
        # Verificar si es booleana antes de hacer sum
        if df_con_flags[flag_col].dtype == pl.Boolean:
            count = df_con_flags[flag_col].sum()
            total = len(df_con_flags)
            porcentaje = (count / total * 100) if total > 0 else 0
            print(f"   • {flag_col}: {count} empleados ({porcentaje:.2f}%)")
        else:
            # Para columnas no booleanas (como tiempo_servicio_texto)
            print(f"   • {flag_col}: columna de texto generada")
    
    # Cerrar conexión
    con.close()
    
    print("-" * 70)
    
    return df_con_flags


def generar_resumen_flags(df: pl.DataFrame, columnas_originales: list) -> dict:
    """
    Genera un resumen estadístico de las flags aplicadas.
    
    Args:
        df: DataFrame con flags aplicadas
        columnas_originales: Lista de columnas originales (antes de flags)
    
    Returns:
        Diccionario con estadísticas
    """
    stats = {
        "total_registros": len(df),
        "columnas_originales": len(columnas_originales),
        "columnas_totales": len(df.columns),
        "flags_generadas": {},
        "modalidades": {}
    }
    
    # Identificar columnas de flags (nuevas columnas)
    flags_cols = [col for col in df.columns if col not in columnas_originales]
    
    # Estadísticas de cada flag (solo booleanas, excluir texto)
    for flag_col in flags_cols:
        # Verificar si la columna es booleana
        if df[flag_col].dtype == pl.Boolean:
            count = df[flag_col].sum()
            stats["flags_generadas"][flag_col] = {
                "count": count,
                "porcentaje": round(count / len(df) * 100, 2)
            }
    
    # Distribución por modalidad
    if "Modalidad de Contrato" in df.columns:
        modalidades = df.group_by("Modalidad de Contrato").agg(
            pl.len().alias("cantidad")
        ).sort("cantidad", descending=True)
        
        for row in modalidades.iter_rows(named=True):
            modalidad = row["Modalidad de Contrato"]
            cantidad = row["cantidad"]
            stats["modalidades"][modalidad] = {
                "count": cantidad,
                "porcentaje": round(cantidad / len(df) * 100, 2)
            }
    
    return stats


def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """Guarda el DataFrame en formato Parquet."""
    print(f"\n💾 Guardando archivo Parquet: {output_path.name}")
    df.write_parquet(output_path)
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Archivo Parquet guardado ({file_size_mb:.2f} MB)")


def save_to_excel_polars(df: pl.DataFrame, output_path: Path) -> None:
    """
    Guarda el DataFrame en formato Excel usando Polars y openpyxl directamente.
    No convierte a Pandas para evitar carga innecesaria en RAM.
    
    Args:
        df: DataFrame de Polars
        output_path: Ruta del archivo Excel de salida
    """
    print(f"\n📊 Generando archivo Excel: {output_path.name}")
    
    # Crear workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Empleados_Flags"
    
    # Formato de encabezado
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    header_alignment = Alignment(horizontal="center", vertical="center")
    
    # Escribir encabezados
    for col_idx, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment
    
    # Escribir datos fila por fila (más eficiente en memoria que convertir todo)
    for row_idx, row_data in enumerate(df.iter_rows(), 2):
        for col_idx, value in enumerate(row_data, 1):
            ws.cell(row=row_idx, column=col_idx, value=value)
    
    # Ajustar ancho de columnas
    for col_idx, col_name in enumerate(df.columns, 1):
        # Calcular ancho basado en el nombre de columna
        max_length = len(str(col_name))
        # Muestrear algunos valores para estimar ancho
        sample_values = df[col_name].head(100)
        for val in sample_values:
            val_len = len(str(val))
            if val_len > max_length:
                max_length = val_len
        
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = adjusted_width
    
    # Guardar archivo
    wb.save(output_path)
    
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"✓ Archivo Excel guardado ({file_size_mb:.2f} MB)")


def imprimir_resumen(stats: dict, elapsed_time: float, output_dir: Path) -> None:
    """Imprime un resumen completo del procesamiento."""
    print("\n" + "=" * 70)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 70)
    
    print(f"\n📊 Estadísticas Generales:")
    print(f"   • Total de registros procesados: {stats['total_registros']:,}")
    print(f"   • Columnas originales: {stats['columnas_originales']}")
    print(f"   • Columnas totales: {stats['columnas_totales']}")
    print(f"   • Flags generadas: {len(stats['flags_generadas'])}")
    print(f"   • Tiempo de ejecución: {elapsed_time:.2f} segundos")
    
    print(f"\n🏷️  Flags Aplicadas:")
    for flag_name, flag_data in stats["flags_generadas"].items():
        print(f"   • {flag_name}: {flag_data['count']:,} empleados ({flag_data['porcentaje']}%)")
    
    if stats["modalidades"]:
        print(f"\n🏢 Distribución por Modalidad de Contrato:")
        for modalidad, modalidad_data in stats["modalidades"].items():
            print(f"   • {modalidad}: {modalidad_data['count']:,} ({modalidad_data['porcentaje']}%)")
    
    print(f"\n📂 Ubicación de archivos: {output_dir}")
    print("=" * 70)


def main():
    """Función principal del script."""
    print("=" * 70)
    print("  GENERACIÓN DE FLAGS EN CAPA GOLD - EMPLEADOS")
    print("=" * 70)
    
    # Paso 1: Seleccionar archivo Parquet Gold
    print("\n📂 Selecciona el archivo Parquet Gold de empleados...")
    input_file = select_file("Seleccionar archivo Parquet Gold de Empleados")
    
    if not input_file:
        print("\n❌ No se seleccionó ningún archivo. Proceso cancelado.")
        return
    
    if not input_file.exists():
        print(f"\n❌ El archivo no existe: {input_file}")
        return
    
    # Iniciar temporizador DESPUÉS de seleccionar archivo
    start_time = time.time()
    
    print(f"✓ Archivo seleccionado: {input_file.name}")
    
    # Paso 2: Cargar query SQL
    try:
        query = load_sql_query()
        print(f"✓ Query SQL cargada exitosamente")
    except Exception as e:
        print(f"\n❌ Error al cargar query SQL: {e}")
        return
    
    # Paso 3: Cargar Parquet Gold
    print(f"\n📂 Cargando Parquet Gold: {input_file.name}")
    try:
        df = pl.read_parquet(input_file)
        print(f"✓ Datos cargados: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        columnas_originales = df.columns.copy()
    except Exception as e:
        print(f"\n❌ Error al cargar Parquet: {e}")
        return
    
    # Paso 4: Validar columnas requeridas
    print(f"\n🔍 Validando columnas requeridas...")
    if not validar_columnas_requeridas(df):
        print(f"\n❌ El archivo no contiene las columnas requeridas. Proceso cancelado.")
        return
    
    # Paso 5: Aplicar flags mediante DuckDB
    try:
        df_con_flags = aplicar_flags_duckdb(df, query)
        print(f"✓ Dataset resultante: {df_con_flags.shape[0]:,} filas × {df_con_flags.shape[1]} columnas")
    except Exception as e:
        print(f"\n❌ Error al aplicar flags: {e}")
        return
    
    # Paso 6: Generar resumen de flags
    print(f"\n📈 Generando resumen de flags...")
    stats = generar_resumen_flags(df_con_flags, columnas_originales)
    
    # Paso 7: Generar nombres de archivo de salida
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    output_dir = input_file.parent
    
    parquet_output = output_dir / f"BD_Empleados_Flags_{timestamp}_gold.parquet"
    excel_output = output_dir / f"BD_Empleados_Flags_{timestamp}_gold.xlsx"
    
    # Paso 8: Guardar archivos
    try:
        save_to_parquet(df_con_flags, parquet_output)
        save_to_excel_polars(df_con_flags, excel_output)
    except Exception as e:
        print(f"\n❌ Error al guardar archivos: {e}")
        return
    
    # Calcular tiempo de ejecución
    elapsed_time = time.time() - start_time
    
    # Paso 9: Imprimir resumen final
    imprimir_resumen(stats, elapsed_time, output_dir)
    
    print(f"\n💡 Tip: Revisa el archivo Excel para validación visual de las flags")


if __name__ == "__main__":
    main()