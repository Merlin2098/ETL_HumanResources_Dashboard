"""
Script de transformación Silver → Gold
Reporte: Movimiento de Renovaciones
Arquitectura: Bronze-Silver-Gold

Usa Polars puro (sin DuckDB) para:
- Seleccionar solo columnas definidas en esquema_renovaciones.json
- Validar tipos de datos
- Generar archivos optimizados Parquet y Excel

Esquema requerido: esquema_renovaciones.json
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
import sys


def cargar_esquema_gold(ruta_esquema: Path) -> dict:
    """
    Carga el esquema JSON de la capa Gold.
    """
    print(f"\n📋 Cargando esquema Gold: {ruta_esquema.name}")
    
    with open(ruta_esquema, 'r', encoding='utf-8') as f:
        esquema = json.load(f)
    
    metadata = esquema.get('metadata', {})
    columnas = esquema.get('columnas', [])
    
    print(f"  ├─ Versión: {metadata.get('version', 'N/A')}")
    print(f"  ├─ Última modificación: {metadata.get('fecha_modificacion', 'N/A')}")
    print(f"  └─ Columnas definidas: {len(columnas)}")
    
    return esquema


def validar_esquema(df: pl.DataFrame, esquema: dict) -> tuple[bool, list]:
    """
    Valida que el DataFrame contenga todas las columnas del esquema.
    """
    print(f"\n🔍 Validando esquema...")
    
    columnas_esquema = [col['nombre'] for col in esquema['columnas']]
    columnas_df = df.columns
    
    columnas_faltantes = [col for col in columnas_esquema if col not in columnas_df]
    
    if columnas_faltantes:
        print(f"  ❌ Faltan {len(columnas_faltantes)} columnas:")
        for col in columnas_faltantes:
            print(f"      • {col}")
        return False, columnas_faltantes
    else:
        print(f"  ✓ Todas las columnas del esquema están presentes")
        return True, []


def transformar_silver_to_gold(df_silver: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Transforma el DataFrame Silver seleccionando solo las columnas Gold.
    """
    print(f"\n🔧 Transformando Silver → Gold...")
    
    # Obtener lista de columnas del esquema
    columnas_gold = [col['nombre'] for col in esquema['columnas']]
    
    print(f"  ├─ Registros de entrada: {len(df_silver)}")
    print(f"  ├─ Columnas de entrada: {len(df_silver.columns)}")
    print(f"  └─ Columnas de salida: {len(columnas_gold)}")
    
    # Seleccionar solo columnas Gold
    df_gold = df_silver.select(columnas_gold)
    
    # Validar tipos de datos según esquema
    print(f"\n  📊 Validando tipos de datos...")
    for col_def in esquema['columnas']:
        nombre = col_def['nombre']
        tipo_esperado = col_def['tipo']
        tipo_actual = str(df_gold[nombre].dtype)
        
        # Mapeo de tipos
        tipo_ok = False
        if tipo_esperado == 'integer' and ('Int' in tipo_actual or 'UInt' in tipo_actual):
            tipo_ok = True
        elif tipo_esperado == 'string' and tipo_actual == 'String':
            tipo_ok = True
        elif tipo_esperado == 'number' and 'Float' in tipo_actual:
            tipo_ok = True
        elif tipo_esperado == 'date' and tipo_actual == 'Date':
            tipo_ok = True
        elif tipo_esperado == 'datetime' and 'Datetime' in tipo_actual:
            tipo_ok = True
        elif tipo_esperado == 'boolean' and tipo_actual == 'Boolean':
            tipo_ok = True
        
        status = "✓" if tipo_ok else "⚠️"
        if not tipo_ok:
            print(f"      {status} {nombre}: esperado={tipo_esperado}, actual={tipo_actual}")
    
    return df_gold


def generar_reporte_calidad_gold(df: pl.DataFrame, esquema: dict) -> dict:
    """
    Genera métricas de calidad para la capa Gold.
    """
    print(f"\n📊 Generando reporte de calidad Gold...")
    
    metricas = {
        "total_registros": len(df),
        "total_columnas": len(df.columns),
        "esquema_version": esquema['metadata']['version'],
        "columnas_con_nulos": {}
    }
    
    print(f"  ├─ Total de registros: {metricas['total_registros']}")
    print(f"  ├─ Total de columnas: {metricas['total_columnas']}")
    print(f"  └─ Versión de esquema: {metricas['esquema_version']}")
    
    # Analizar nulos
    print(f"\n  📋 Análisis de valores nulos:")
    
    for col in df.columns:
        nulos = df[col].null_count()
        if nulos > 0:
            porcentaje = round(nulos / len(df) * 100, 2)
            metricas['columnas_con_nulos'][col] = {
                "cantidad": nulos,
                "porcentaje": porcentaje
            }
            print(f"      ⚠️  {col}: {nulos} ({porcentaje}%)")
    
    if not metricas['columnas_con_nulos']:
        print(f"      ✓ No hay valores nulos en ninguna columna")
    
    # Distribución por año/mes
    if 'AÑO' in df.columns and 'MES' in df.columns:
        print(f"\n  📈 Distribución por periodo:")
        dist = df.group_by(['AÑO', 'MES']).agg(pl.len().alias('Registros')).sort(['AÑO', 'MES'])
        
        for row in dist.head(5).iter_rows():
            print(f"      • {row[0]}-{row[1]:02d}: {row[2]} registros")
        
        if len(dist) > 5:
            print(f"      • ... y {len(dist) - 5} periodos más")
    
    return metricas


def guardar_gold(df: pl.DataFrame, directorio_salida: Path, nombre_base: str):
    """
    Guarda el DataFrame en formato Parquet y Excel en la capa Gold.
    """
    print(f"\n💾 Guardando archivos en capa Gold...")
    
    directorio_salida.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    # Guardar Parquet
    archivo_parquet = directorio_salida / f"{nombre_base}_gold_{timestamp}.parquet"
    df.write_parquet(archivo_parquet, compression="snappy")
    print(f"  ✓ Parquet: {archivo_parquet.name}")
    
    # Guardar Excel
    archivo_excel = directorio_salida / f"{nombre_base}_gold_{timestamp}.xlsx"
    df.write_excel(archivo_excel)
    print(f"  ✓ Excel: {archivo_excel.name}")
    
    return archivo_parquet, archivo_excel


def main():
    """
    Función principal del script.
    """
    print("="*80)
    print("TRANSFORMACIÓN SILVER → GOLD")
    print("Reporte: Movimiento de Renovaciones")
    print("="*80)
    
    # Configuración de rutas
    try:
        # Solicitar archivo Silver
        import tkinter as tk
        from tkinter import filedialog
        
        root = tk.Tk()
        root.withdraw()
        
        archivo_silver = filedialog.askopenfilename(
            title="Seleccionar archivo Silver Parquet (Movimiento de Renovaciones)",
            filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
        )
        
        if not archivo_silver:
            print("❌ No se seleccionó archivo Silver")
            return
        
        archivo_silver = Path(archivo_silver)
        
        # Solicitar esquema JSON
        archivo_esquema = filedialog.askopenfilename(
            title="Seleccionar esquema JSON (esquema_renovaciones.json)",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="esquema_renovaciones.json"
        )
        
        if not archivo_esquema:
            print("❌ No se seleccionó esquema JSON")
            return
        
        archivo_esquema = Path(archivo_esquema)
        
        # Solicitar directorio de salida
        directorio_salida = filedialog.askdirectory(
            title="Seleccionar directorio para archivos Gold"
        )
        
        if not directorio_salida:
            print("❌ No se seleccionó directorio de salida")
            return
        
        directorio_salida = Path(directorio_salida)
        
    except Exception as e:
        print(f"❌ Error en selección de archivos: {e}")
        return
    
    try:
        # 1. Cargar esquema
        esquema = cargar_esquema_gold(archivo_esquema)
        
        # 2. Cargar datos Silver
        print(f"\n📂 Cargando archivo Silver: {archivo_silver.name}")
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Cargado: {len(df_silver)} registros, {len(df_silver.columns)} columnas")
        
        # 3. Validar esquema
        valido, faltantes = validar_esquema(df_silver, esquema)
        
        if not valido:
            print(f"\n❌ ERROR: El archivo Silver no contiene todas las columnas requeridas")
            sys.exit(1)
        
        # 4. Transformar a Gold
        df_gold = transformar_silver_to_gold(df_silver, esquema)
        
        # 5. Generar reporte de calidad
        metricas = generar_reporte_calidad_gold(df_gold, esquema)
        
        # 6. Guardar archivos Gold
        archivo_parquet, archivo_excel = guardar_gold(
            df_gold,
            directorio_salida,
            "movimiento_renovaciones"
        )
        
        # Resumen final
        print("\n" + "="*80)
        print("✅ TRANSFORMACIÓN COMPLETADA EXITOSAMENTE")
        print("="*80)
        print(f"📁 Archivos generados:")
        print(f"   • {archivo_parquet.name}")
        print(f"   • {archivo_excel.name}")
        print(f"\n📊 Estadísticas:")
        print(f"   • Registros: {metricas['total_registros']}")
        print(f"   • Columnas: {metricas['total_columnas']}")
        print(f"   • Esquema versión: {metricas['esquema_version']}")
        
        if metricas['columnas_con_nulos']:
            print(f"   • Columnas con nulos: {len(metricas['columnas_con_nulos'])}")
        else:
            print(f"   • Calidad: Excelente (sin nulos)")
        
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ ERROR durante la transformación: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()