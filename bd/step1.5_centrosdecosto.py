"""
Script: Procesar Centros de Costo desde BD Silver
Objetivo: Extraer tabla única de centros de costo desde el parquet silver
Arquitectura: Silver -> Centros de Costo (tabla única)
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import filedialog

def seleccionar_archivo(titulo: str, tipos: list) -> Path:
    """
    Abre un diálogo para seleccionar un archivo.
    
    Args:
        titulo: Título de la ventana
        tipos: Lista de tuplas (descripción, extensión)
    
    Returns:
        Path del archivo seleccionado
    """
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title=titulo,
        filetypes=tipos
    )
    
    root.destroy()
    
    if not archivo:
        raise ValueError("No se seleccionó ningún archivo")
    
    return Path(archivo)


def cargar_esquema(ruta_esquema: Path) -> dict:
    """
    Carga el esquema JSON de centros de costo.
    
    Args:
        ruta_esquema: Path al archivo JSON del esquema
    
    Returns:
        Diccionario con la configuración del esquema
    """
    print(f"\n📄 Cargando esquema desde: {ruta_esquema}")
    
    with open(ruta_esquema, 'r', encoding='utf-8') as f:
        esquema = json.load(f)
    
    print(f"✓ Esquema cargado correctamente")
    print(f"  - Columnas requeridas: {len(esquema['columnas_requeridas'])}")
    print(f"  - Columna de deduplicación: {esquema['columna_deduplicacion']}")
    
    return esquema


def cargar_parquet_silver(ruta_parquet: Path) -> pl.DataFrame:
    """
    Carga el parquet silver de BD.
    
    Args:
        ruta_parquet: Path al archivo parquet silver
    
    Returns:
        DataFrame de Polars con los datos silver
    """
    print(f"\n📊 Cargando parquet silver desde: {ruta_parquet}")
    
    df = pl.read_parquet(ruta_parquet)
    
    print(f"✓ Parquet cargado correctamente")
    print(f"  - Filas totales: {len(df):,}")
    print(f"  - Columnas: {len(df.columns)}")
    
    return df


def extraer_centros_costo(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Extrae y procesa la tabla de centros de costo.
    
    Args:
        df: DataFrame con los datos silver
        esquema: Diccionario con la configuración del esquema
    
    Returns:
        DataFrame con centros de costo únicos
    """
    print("\n🔄 Procesando centros de costo...")
    
    # Verificar que existan las columnas requeridas
    columnas_faltantes = set(esquema['columnas_requeridas']) - set(df.columns)
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes en el parquet: {columnas_faltantes}")
    
    # Seleccionar solo las columnas requeridas
    df_cc = df.select(esquema['columnas_requeridas'])
    
    print(f"  - Filas antes de deduplicación: {len(df_cc):,}")
    
    # Eliminar duplicados basándose en la columna CC
    columna_dedupe = esquema['columna_deduplicacion']
    df_cc = df_cc.unique(subset=[columna_dedupe], keep='first')
    
    print(f"  - Filas después de deduplicación: {len(df_cc):,}")
    print(f"  - Centros de costo únicos: {df_cc[columna_dedupe].n_unique()}")
    
    # Ordenar por CC
    df_cc = df_cc.sort(columna_dedupe)
    
    return df_cc


def guardar_centros_costo(df: pl.DataFrame, ruta_base: Path) -> tuple[Path, Path]:
    """
    Guarda el DataFrame de centros de costo en formato parquet y Excel.
    
    Args:
        df: DataFrame con los centros de costo procesados
        ruta_base: Path base del parquet silver
    
    Returns:
        Tupla con (Path del parquet, Path del Excel)
    """
    print("\n💾 Guardando centros de costo...")
    
    # Crear carpeta "centros de costo" en el mismo directorio del parquet silver
    carpeta_cc = ruta_base.parent / "centros de costo"
    carpeta_cc.mkdir(exist_ok=True)
    
    # Generar timestamp para el nombre del archivo
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    nombre_parquet = f"CC_{timestamp}.parquet"
    nombre_excel = f"CC_{timestamp}.xlsx"
    
    # Paths completos
    ruta_parquet = carpeta_cc / nombre_parquet
    ruta_excel = carpeta_cc / nombre_excel
    
    # Guardar parquet
    df.write_parquet(ruta_parquet, compression="snappy")
    print(f"✓ Parquet guardado:")
    print(f"  - Ubicación: {ruta_parquet}")
    print(f"  - Tamaño: {ruta_parquet.stat().st_size / 1024:.2f} KB")
    
    # Guardar Excel para visualización
    df.write_excel(ruta_excel)
    print(f"✓ Excel guardado:")
    print(f"  - Ubicación: {ruta_excel}")
    print(f"  - Tamaño: {ruta_excel.stat().st_size / 1024:.2f} KB")
    
    return ruta_parquet, ruta_excel


def main():
    """
    Función principal del script.
    """
    print("=" * 60)
    print("PROCESADOR DE CENTROS DE COSTO")
    print("Silver -> Centros de Costo")
    print("=" * 60)
    
    inicio = datetime.now()
    
    try:
        # 1. Seleccionar esquema JSON
        print("\n[1/3] Selección de Esquema JSON")
        ruta_esquema = seleccionar_archivo(
            titulo="Seleccione el esquema JSON de Centros de Costo",
            tipos=[("Archivos JSON", "*.json"), ("Todos los archivos", "*.*")]
        )
        esquema = cargar_esquema(ruta_esquema)
        
        # 2. Seleccionar parquet silver
        print("\n[2/3] Selección de Parquet Silver")
        ruta_parquet = seleccionar_archivo(
            titulo="Seleccione el parquet Silver de BD",
            tipos=[("Archivos Parquet", "*.parquet"), ("Todos los archivos", "*.*")]
        )
        df_silver = cargar_parquet_silver(ruta_parquet)
        
        # 3. Procesar centros de costo
        print("\n[3/3] Procesamiento de Centros de Costo")
        df_cc = extraer_centros_costo(df_silver, esquema)
        
        # 4. Guardar resultado
        ruta_parquet, ruta_excel = guardar_centros_costo(df_cc, ruta_parquet)
        
        # Resumen final
        duracion = (datetime.now() - inicio).total_seconds()
        print("\n" + "=" * 60)
        print("✓ PROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        print(f"⏱️  Tiempo de ejecución: {duracion:.2f} segundos")
        print(f"📊 Centros de costo procesados: {len(df_cc):,}")
        print(f"📁 Archivos generados:")
        print(f"   - Parquet: {ruta_parquet.name}")
        print(f"   - Excel: {ruta_excel.name}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise


if __name__ == "__main__":
    main()