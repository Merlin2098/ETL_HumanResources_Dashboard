"""
Script: relacion_ingresos_silver_to_gold.py
Descripción: Transforma datos de empleados de la capa Silver a Gold
             aplicando selección de columnas, tipado, columnas calculadas
             y particionamiento por periodo.
             
Autor: Richi
Fecha: 30.12.2024
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog
import sys

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Buscar esquema JSON
def buscar_esquema_json() -> Path | None:
    """Busca el archivo de esquema JSON en la carpeta esquemas"""
    # Desde movimientos/step2_gold.py, buscar en esquemas/
    rutas_posibles = [
        Path("../esquemas/esquema_movimientos.json"),  # Desde movimientos/ hacia esquemas/
        Path("esquemas/esquema_movimientos.json"),      # Si se ejecuta desde raíz
        Path("../../esquemas/esquema_movimientos.json"), # Si hay más niveles
        Path("esquema_movimientos.json"),               # En el mismo directorio
    ]
    
    for ruta in rutas_posibles:
        if ruta.exists():
            return ruta
    
    return None

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def obtener_timestamp() -> str:
    """Genera timestamp en formato dd.mm.yyyy_hh.mm.ss"""
    return datetime.now().strftime("%d.%m.%Y_%H.%M.%S")


def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet Silver"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver - Empleados",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """Carga el esquema JSON"""
    print(f"📋 Cargando esquema: {ruta_esquema.name}")
    
    with open(ruta_esquema, 'r', encoding='utf-8') as f:
        esquema = json.load(f)
    
    # Mostrar metadata si existe
    if 'metadata' in esquema:
        metadata = esquema['metadata']
        print(f"   ✓ Versión: {metadata.get('version', 'N/A')}")
        print(f"   ✓ Última modificación: {metadata.get('ultima_modificacion', 'N/A')}")
    
    print(f"   ✓ Columnas definidas: {len(esquema['columnas'])}")
    
    return esquema


def seleccionar_y_convertir_columnas(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """Selecciona columnas y convierte tipos de datos según el esquema"""
    print(f"\n📊 Procesando columnas...")
    
    columnas = esquema['columnas']
    print(f"   Columnas a procesar: {len(columnas)}")
    
    # Verificar que todas las columnas existen
    columnas_faltantes = [col for col in columnas.keys() if col not in df.columns]
    if columnas_faltantes:
        print(f"   ❌ Columnas faltantes: {columnas_faltantes}")
        raise ValueError(f"Columnas faltantes en el DataFrame: {columnas_faltantes}")
    
    # Seleccionar y convertir en una sola operación
    expresiones = []
    for columna, tipo_str in columnas.items():
        # Mapear tipos
        tipo_polars = {
            'Int64': pl.Int64,
            'Float64': pl.Float64,
            'String': pl.Utf8,
            'Boolean': pl.Boolean,
            'Date': pl.Date,
        }.get(tipo_str, pl.Utf8)
        
        expresiones.append(pl.col(columna).cast(tipo_polars))
        print(f"   ✓ {columna}: {tipo_str}")
    
    df_resultado = df.select(expresiones)
    print(f"   ✓ Resultado: {df_resultado.height:,} filas × {df_resultado.width} columnas")
    
    return df_resultado


def generar_metricas_basicas(df: pl.DataFrame):
    """Genera métricas básicas de calidad"""
    print(f"\n📊 MÉTRICAS DE CALIDAD")
    print("=" * 80)
    print(f"Total de registros: {df.height:,}")
    print(f"Total de columnas: {df.width}")
    
    # Nulos por columna
    print(f"\nValores nulos:")
    tiene_nulos = False
    for col in df.columns:
        nulos = df[col].is_null().sum()
        if nulos > 0:
            pct = (nulos / df.height * 100) if df.height > 0 else 0
            print(f"   {col:25}: {nulos:4} ({pct:5.2f}%)")
            tiene_nulos = True
    
    if not tiene_nulos:
        print("   ✓ Sin valores nulos")
    
    print("=" * 80)



# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print("🚀 TRANSFORMACIÓN SILVER → GOLD - EMPLEADOS")
    print("=" * 80)
    
    # 1. Buscar esquema JSON
    ruta_esquema = buscar_esquema_json()
    
    if not ruta_esquema:
        print("\n⚠️  No se encontró el esquema JSON.")
        print("   Buscando manualmente...")
        
        root = Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        ruta_esquema = filedialog.askopenfilename(
            title="Seleccionar esquema JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        root.destroy()
        
        if not ruta_esquema:
            print("❌ No se seleccionó esquema. Proceso cancelado.")
            return
        
        ruta_esquema = Path(ruta_esquema)
    
    # 2. Cargar esquema
    try:
        esquema = cargar_esquema(ruta_esquema)
    except Exception as e:
        print(f"❌ Error al cargar esquema: {e}")
        return
    
    # 3. Seleccionar archivo Parquet Silver
    archivo_silver = seleccionar_archivo_parquet()
    
    if not archivo_silver:
        print("❌ No se seleccionó archivo. Proceso cancelado.")
        return
    
    print(f"\n📖 Cargando archivo Silver: {archivo_silver.name}")
    
    try:
        # 4. Leer datos Silver
        df_silver = pl.read_parquet(archivo_silver)
        print(f"   ✓ Datos cargados: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # 5. Seleccionar columnas y convertir tipos
        df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)
        
        # 6. Generar métricas
        generar_metricas_basicas(df_gold)
        
        # 7. Guardar archivos
        timestamp = obtener_timestamp()
        carpeta_gold = archivo_silver.parent  # Misma carpeta que el archivo Silver
        
        print(f"\n💾 GUARDANDO ARCHIVOS GOLD")
        print("=" * 80)
        print(f"Carpeta de destino: {carpeta_gold}")
        
        # Guardar Parquet
        ruta_parquet = carpeta_gold / f"empleados_gold_{timestamp}.parquet"
        df_gold.write_parquet(ruta_parquet, compression="snappy")
        print(f"\n✅ Archivo Parquet:")
        print(f"   {ruta_parquet.name}")
        print(f"   Registros: {df_gold.height:,}")
        
        # Guardar Excel
        ruta_excel = carpeta_gold / f"empleados_gold_{timestamp}.xlsx"
        df_gold.write_excel(ruta_excel)
        print(f"\n✅ Archivo Excel:")
        print(f"   {ruta_excel.name}")
        print(f"   Registros: {df_gold.height:,}")
        
        # Resumen final
        print("\n" + "=" * 80)
        print("✨ TRANSFORMACIÓN COMPLETADA")
        print("=" * 80)
        print(f"Archivos generados: 2")
        print(f"Total de registros: {df_gold.height:,}")
        print(f"Columnas en Gold: {df_gold.width}")
        print(f"\nUbicación: {carpeta_gold}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)