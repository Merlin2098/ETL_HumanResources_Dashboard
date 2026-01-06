"""
ETL Silver → Gold para Programación de Exámenes de Retiro
Filtra y selecciona columnas según esquema definido
Agrega columna NOMBRE_MES derivada de FECHA DE CESE

Arquitectura:
- Silver: Parquet con todas las columnas
- Gold: Parquet con columnas filtradas según esquema + NOMBRE_MES
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
import time
import sys


def cargar_esquema(ruta_esquema: str) -> dict:
    """
    Carga el esquema JSON que define las columnas para la capa Gold
    """
    print("📋 Cargando esquema...")
    
    try:
        with open(ruta_esquema, 'r', encoding='utf-8') as f:
            esquema = json.load(f)
        
        columnas = [col['nombre'] for col in esquema['columnas']]
        print(f"   ✓ Esquema cargado: {esquema['nombre_reporte']}")
        print(f"   ✓ {len(columnas)} columnas definidas")
        
        return esquema
        
    except Exception as e:
        print(f"❌ Error al cargar esquema: {str(e)}")
        raise


def transformar_silver_a_gold(df_silver: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Transforma la capa Silver a Gold aplicando el esquema definido
    Genera columna NOMBRE_MES automáticamente desde FECHA DE CESE
    """
    print("\n✨ Transformando Silver → Gold...")
    
    try:
        # Extraer nombres de columnas del esquema (excluyendo derivadas)
        columnas_base = [col['nombre'] for col in esquema['columnas'] 
                        if 'derivado_de' not in col]
        
        # Verificar que todas las columnas existen en Silver
        columnas_faltantes = [col for col in columnas_base if col not in df_silver.columns]
        if columnas_faltantes:
            print(f"⚠️  Advertencia: Columnas no encontradas en Silver: {columnas_faltantes}")
        
        # Seleccionar solo las columnas base del esquema que existen
        columnas_disponibles = [col for col in columnas_base if col in df_silver.columns]
        df_gold = df_silver.select(columnas_disponibles)
        
        # Aplicar tipos de datos según esquema
        for col_def in esquema['columnas']:
            col_nombre = col_def['nombre']
            col_tipo = col_def['tipo']
            
            # Saltar columnas derivadas por ahora
            if 'derivado_de' in col_def:
                continue
            
            if col_nombre not in df_gold.columns:
                continue
            
            # Convertir tipos según esquema
            if col_tipo == 'DATE':
                df_gold = df_gold.with_columns(
                    pl.col(col_nombre).cast(pl.Date, strict=False).alias(col_nombre)
                )
            elif col_tipo == 'STRING':
                df_gold = df_gold.with_columns(
                    pl.col(col_nombre).cast(pl.Utf8, strict=False).alias(col_nombre)
                )
            elif col_tipo == 'INTEGER':
                df_gold = df_gold.with_columns(
                    pl.col(col_nombre).cast(pl.Int64, strict=False).alias(col_nombre)
                )
            elif col_tipo == 'FLOAT':
                df_gold = df_gold.with_columns(
                    pl.col(col_nombre).cast(pl.Float64, strict=False).alias(col_nombre)
                )
        
        # Generar columnas derivadas del esquema
        for col_def in esquema['columnas']:
            if 'derivado_de' not in col_def:
                continue
            
            col_nombre = col_def['nombre']
            col_origen = col_def['derivado_de']
            col_tipo = col_def['tipo']
            
            if col_origen not in df_gold.columns:
                print(f"⚠️  No se puede derivar {col_nombre}: columna origen {col_origen} no existe")
                continue
            
            # Generar columna derivada según el nombre
            if col_nombre == 'AÑO' or col_nombre == 'ANO':
                df_gold = df_gold.with_columns(
                    pl.col(col_origen).dt.year().alias(col_nombre)
                )
            elif col_nombre == 'MES':
                df_gold = df_gold.with_columns(
                    pl.col(col_origen).dt.month().alias(col_nombre)
                )
            
            print(f"   ✓ Columna derivada generada: {col_nombre} (desde {col_origen})")
        
        # GENERAR COLUMNA NOMBRE_MES desde FECHA DE CESE
        if 'FECHA DE CESE' in df_gold.columns:
            # Mapeo de número de mes a nombre en español
            meses_espanol = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
                5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
                9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }
            
            # Extraer número de mes y convertir a nombre
            df_gold = df_gold.with_columns(
                pl.col('FECHA DE CESE').dt.month().alias('_mes_num')
            )
            
            # Crear columna NOMBRE_MES usando replace
            df_gold = df_gold.with_columns(
                pl.col('_mes_num').replace(meses_espanol, default=None).alias('NOMBRE_MES')
            )
            
            # Eliminar columna temporal
            df_gold = df_gold.drop('_mes_num')
            
            print(f"   ✓ Columna derivada generada: NOMBRE_MES (desde FECHA DE CESE)")
        else:
            print(f"   ⚠️  No se puede generar NOMBRE_MES: columna FECHA DE CESE no existe")
        
        # Aplicar filtros definidos en el esquema
        if 'filtros' in esquema:
            for filtro in esquema['filtros']:
                col_filtro = filtro['columna']
                condicion = filtro['condicion']
                valor = filtro['valor']
                descripcion = filtro.get('descripcion', '')
                
                if col_filtro not in df_gold.columns:
                    print(f"⚠️  No se puede aplicar filtro: columna {col_filtro} no existe")
                    continue
                
                registros_antes = df_gold.height
                
                # Aplicar condición
                if condicion == 'NOT_EQUAL':
                    df_gold = df_gold.filter(pl.col(col_filtro) != valor)
                elif condicion == 'EQUAL':
                    df_gold = df_gold.filter(pl.col(col_filtro) == valor)
                elif condicion == 'IS_NOT_NULL':
                    df_gold = df_gold.filter(pl.col(col_filtro).is_not_null())
                elif condicion == 'IS_NULL':
                    df_gold = df_gold.filter(pl.col(col_filtro).is_null())
                
                registros_filtrados = registros_antes - df_gold.height
                print(f"   ✓ Filtro aplicado: {descripcion}")
                print(f"     • {registros_filtrados} registros excluidos ({col_filtro} {condicion} '{valor}')")
        
        print(f"   ✓ {df_gold.height} registros en Gold final")
        print(f"   ✓ {df_gold.width} columnas en Gold final")
        
        return df_gold
        
    except Exception as e:
        print(f"❌ Error en transformación Gold: {str(e)}")
        raise


def guardar_gold_parquet(df_gold: pl.DataFrame, ruta_salida: str) -> None:
    """
    Guarda el DataFrame Gold en formato Parquet
    """
    print(f"\n💾 Guardando capa Gold (Parquet)...")
    
    try:
        df_gold.write_parquet(ruta_salida)
        
        # Verificar tamaño del archivo
        tamanio_mb = Path(ruta_salida).stat().st_size / (1024 * 1024)
        print(f"   ✓ Archivo guardado: {Path(ruta_salida).name}")
        print(f"   ✓ Tamaño: {tamanio_mb:.2f} MB")
        
    except Exception as e:
        print(f"❌ Error al guardar Gold Parquet: {str(e)}")
        raise


def exportar_gold_excel(df_gold: pl.DataFrame, ruta_salida: str) -> None:
    """
    Exporta el DataFrame Gold a Excel para visualización
    """
    print(f"\n📊 Exportando Gold a Excel...")
    
    try:
        # Convertir fechas a string para Excel
        df_export = df_gold.clone()
        
        for col in df_export.columns:
            if df_export[col].dtype == pl.Date:
                df_export = df_export.with_columns(
                    pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                )
        
        # Exportar a Excel
        df_export.write_excel(ruta_salida)
        
        print(f"   ✓ Excel guardado: {Path(ruta_salida).name}")
        print(f"   ✓ {df_export.height} filas exportadas")
        
    except Exception as e:
        print(f"❌ Error al exportar Gold Excel: {str(e)}")
        raise


def ejecutar_etl_silver_to_gold(ruta_silver: str, 
                                  ruta_esquema: str,
                                  carpeta_gold: str = None) -> None:
    """
    Ejecuta el pipeline ETL de Silver a Gold
    """
    inicio = time.time()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print("=" * 80)
    print("ETL SILVER → GOLD - PROGRAMACIÓN DE EXÁMENES DE RETIRO")
    print("=" * 80)
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Definir carpeta gold
        if carpeta_gold is None:
            carpeta_gold = Path(ruta_silver).parent.parent / 'gold'
        
        # Crear carpeta si no existe
        Path(carpeta_gold).mkdir(parents=True, exist_ok=True)
        
        nombre_base = f"examenes_retiro_gold_{timestamp}"
        ruta_parquet_gold = Path(carpeta_gold) / f"{nombre_base}.parquet"
        ruta_excel_gold = Path(carpeta_gold) / f"{nombre_base}.xlsx"
        
        # 1. Cargar esquema
        esquema = cargar_esquema(ruta_esquema)
        
        # 2. Leer Silver
        print(f"\n📂 Leyendo capa Silver...")
        df_silver = pl.read_parquet(ruta_silver)
        print(f"   ✓ {df_silver.height} registros leídos")
        print(f"   ✓ {df_silver.width} columnas disponibles")
        
        # 3. Transformar a Gold (incluye generación de NOMBRE_MES)
        df_gold = transformar_silver_a_gold(df_silver, esquema)
        
        # 4. Guardar Gold Parquet
        guardar_gold_parquet(df_gold, str(ruta_parquet_gold))
        
        # 5. Exportar Gold Excel
        exportar_gold_excel(df_gold, str(ruta_excel_gold))
        
        # Resumen final
        duracion = time.time() - inicio
        print("\n" + "=" * 80)
        print("✅ ETL SILVER → GOLD COMPLETADO")
        print("=" * 80)
        print(f"Registros procesados: {df_gold.height}")
        print(f"Columnas Gold: {df_gold.width}")
        print(f"Tiempo de ejecución: {duracion:.2f} segundos")
        print(f"\nArchivos generados en: {carpeta_gold}")
        print(f"  📦 Parquet: {ruta_parquet_gold.name}")
        print(f"  📊 Excel:   {ruta_excel_gold.name}")
        print("\n📌 Columnas adicionales generadas: NOMBRE_MES")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR EN ETL SILVER → GOLD: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    import tkinter as tk
    from tkinter import filedialog
    
    # Ocultar ventana principal de tkinter
    root = tk.Tk()
    root.withdraw()
    
    print("=" * 80)
    print("ETL SILVER → GOLD - EXÁMENES DE RETIRO")
    print("=" * 80)
    
    # Paso 1: Seleccionar archivo Silver (Parquet)
    print("\n📂 Paso 1: Seleccione el archivo Parquet de la capa Silver...\n")
    ruta_silver = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver",
        filetypes=[
            ("Archivos Parquet", "*.parquet"),
            ("Todos los archivos", "*.*")
        ]
    )
    
    if not ruta_silver:
        print("❌ No se seleccionó archivo Silver. Proceso cancelado.")
        sys.exit(0)
    
    print(f"✓ Silver seleccionado: {Path(ruta_silver).name}\n")
    
    # Paso 2: Seleccionar esquema JSON
    print("📋 Paso 2: Seleccione el esquema JSON...\n")
    ruta_esquema = filedialog.askopenfilename(
        title="Seleccionar esquema JSON",
        filetypes=[
            ("Archivos JSON", "*.json"),
            ("Todos los archivos", "*.*")
        ]
    )
    
    if not ruta_esquema:
        print("❌ No se seleccionó esquema. Proceso cancelado.")
        sys.exit(0)
    
    print(f"✓ Esquema seleccionado: {Path(ruta_esquema).name}\n")
    
    # Definir carpeta gold (mismo nivel que silver)
    carpeta_silver = Path(ruta_silver).parent
    carpeta_base = carpeta_silver.parent
    carpeta_gold = carpeta_base / 'gold'
    
    print(f"📁 Estructura de salida:")
    print(f"   📦 Carpeta Silver: {carpeta_silver}")
    print(f"   ✨ Carpeta Gold:   {carpeta_gold} (parquet + excel)")
    print(f"   📌 Nueva columna:  NOMBRE_MES (derivada de FECHA DE CESE)\n")
    
    # Ejecutar ETL
    ejecutar_etl_silver_to_gold(ruta_silver, ruta_esquema, str(carpeta_gold))