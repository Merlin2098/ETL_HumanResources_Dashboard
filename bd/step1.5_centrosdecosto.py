"""
Script: step1.5_centrosdecosto.py
Descripción: Extrae tabla única de Centros de Costo desde BD Silver
             
Arquitectura:
- Input: Silver BD (bd_silver.parquet)
- Output: Archivos con timestamp en centros_costo/

Nota importante:
- TODOS los archivos se guardan CON TIMESTAMP
- Usuario selecciona manualmente cuál es CC_ACTUAL y cuál(es) son CC_OLD
- Cada ejecución genera un nuevo archivo versionado
- No hay sobreescritura automática

Autor: Richi
Fecha: 06.01.2025
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog


def seleccionar_archivo(titulo: str, tipos: list) -> Path:
    """
    Abre un diálogo para seleccionar un archivo.
    
    Args:
        titulo: Título de la ventana
        tipos: Lista de tuplas (descripción, extensión)
    
    Returns:
        Path del archivo seleccionado
    """
    root = Tk()
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


def buscar_esquema_json() -> Path | None:
    """Busca el archivo esquema_cc.json en ubicaciones comunes"""
    rutas_posibles = [
        Path("../esquemas/esquema_cc.json"),
        Path("esquemas/esquema_cc.json"),
        Path("../../esquemas/esquema_cc.json"),
        Path("esquema_cc.json"),
    ]
    
    for ruta in rutas_posibles:
        if ruta.exists():
            return ruta
    
    return None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """
    Carga el esquema JSON de centros de costo.
    
    Args:
        ruta_esquema: Path al archivo JSON del esquema
    
    Returns:
        Diccionario con la configuración del esquema
    """
    print(f"\n📋 Cargando esquema: {ruta_esquema.name}")
    
    with open(ruta_esquema, 'r', encoding='utf-8') as f:
        esquema = json.load(f)
    
    print(f"  ✓ Esquema cargado correctamente")
    print(f"  ✓ Columnas requeridas: {len(esquema['columnas_requeridas'])}")
    print(f"  ✓ Columna de deduplicación: {esquema['columna_deduplicacion']}")
    
    return esquema


def cargar_parquet_silver(ruta_parquet: Path) -> pl.DataFrame:
    """
    Carga el parquet silver de BD.
    
    Args:
        ruta_parquet: Path al archivo parquet silver
    
    Returns:
        DataFrame de Polars con los datos silver
    """
    print(f"\n📊 Cargando parquet silver: {ruta_parquet.name}")
    
    df = pl.read_parquet(ruta_parquet)
    
    print(f"  ✓ Parquet cargado correctamente")
    print(f"  ✓ Filas totales: {len(df):,}")
    print(f"  ✓ Columnas: {len(df.columns)}")
    
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


def guardar_centros_costo(df: pl.DataFrame, carpeta_trabajo: Path) -> tuple[Path, Path]:
    """
    Guarda el DataFrame de centros de costo con timestamp.
    TODOS los archivos se guardan con timestamp para historial completo.
    
    Args:
        df: DataFrame con los centros de costo procesados
        carpeta_trabajo: Path de la carpeta de trabajo
    
    Returns:
        Tupla con (Path del parquet, Path del Excel)
    """
    print("\n💾 Guardando centros de costo...")
    
    # Crear carpeta "centros_costo" en el mismo nivel que silver/
    carpeta_cc = carpeta_trabajo / "centros_costo"
    carpeta_cc.mkdir(exist_ok=True)
    
    # Generar timestamp para el nombre del archivo
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    nombre_parquet = f"cc_{timestamp}.parquet"
    nombre_excel = f"cc_{timestamp}.xlsx"
    
    # Paths completos
    ruta_parquet = carpeta_cc / nombre_parquet
    ruta_excel = carpeta_cc / nombre_excel
    
    print(f"  📁 Carpeta: {carpeta_cc}")
    
    # Guardar parquet
    print(f"  - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet, compression="snappy")
    tamanio_kb = ruta_parquet.stat().st_size / 1024
    print(f" ✓")
    print(f"    Ubicación: {ruta_parquet.name}")
    print(f"    Tamaño: {tamanio_kb:.2f} KB")
    
    # Guardar Excel para visualización
    print(f"  - Guardando Excel...", end='', flush=True)
    df.write_excel(ruta_excel)
    tamanio_kb = ruta_excel.stat().st_size / 1024
    print(f" ✓")
    print(f"    Ubicación: {ruta_excel.name}")
    print(f"    Tamaño: {tamanio_kb:.2f} KB")
    
    return ruta_parquet, ruta_excel


def main():
    """Función principal del script"""
    print("=" * 80)
    print(" EXTRACTOR DE CENTROS DE COSTO - SILVER → CC ".center(80, "="))
    print("=" * 80)
    
    inicio = datetime.now()
    
    try:
        # 1. Buscar esquema JSON
        print("\n[1/3] Carga de Esquema JSON")
        ruta_esquema = buscar_esquema_json()
        
        if not ruta_esquema:
            print("⚠️  No se encontró el esquema JSON automáticamente.")
            print("   Buscando manualmente...")
            
            ruta_esquema = seleccionar_archivo(
                titulo="Seleccionar esquema JSON - Centros de Costo",
                tipos=[("JSON files", "*.json"), ("All files", "*.*")]
            )
        
        esquema = cargar_esquema(ruta_esquema)
        
        # 2. Seleccionar parquet silver
        print("\n[2/3] Selección de Parquet Silver")
        ruta_parquet = seleccionar_archivo(
            titulo="Seleccionar parquet Silver de BD",
            tipos=[("Parquet files", "*.parquet"), ("All files", "*.*")]
        )
        print(f"  ✓ Seleccionado: {ruta_parquet.name}")
        
        df_silver = cargar_parquet_silver(ruta_parquet)
        
        # 3. Procesar centros de costo
        print("\n[3/3] Procesamiento de Centros de Costo")
        df_cc = extraer_centros_costo(df_silver, esquema)
        
        # 4. Guardar resultado
        carpeta_trabajo = ruta_parquet.parent.parent  # Subir de silver/ a bd/
        ruta_parquet_cc, ruta_excel_cc = guardar_centros_costo(df_cc, carpeta_trabajo)
        
        # Resumen final
        duracion = (datetime.now() - inicio).total_seconds()
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Proceso completado exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"  - Centros de costo procesados: {len(df_cc):,}")
        print(f"  - Tiempo de ejecución: {duracion:.2f}s")
        
        print(f"\n📁 Archivos generados:")
        print(f"  - Parquet: {ruta_parquet_cc.name}")
        print(f"  - Excel:   {ruta_excel_cc.name}")
        
        print(f"\n📂 Ubicación: {ruta_parquet_cc.parent}")
        
        print("\n💡 Notas:")
        print("  - Todos los archivos se guardan CON TIMESTAMP")
        print("  - No hay sobreescritura automática")
        print("  - Usuario selecciona manualmente CC_ACTUAL y CC_OLD(s)")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()