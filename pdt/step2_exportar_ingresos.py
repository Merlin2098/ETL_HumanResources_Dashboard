"""
Script: step2_exportar_ingresos.py
Descripción: Transforma datos de EMPLEADOS de la capa Silver a Gold
             aplicando selección de columnas, tipado y validaciones.
             
             NOTA: Solo procesa EMPLEADOS. PRACTICANTES se mantiene en Silver.
             
Proceso:
    1. Lee silver/Relacion Ingresos EMPLEADOS.parquet
    2. Aplica esquema JSON (selección de columnas y tipado)
    3. Genera métricas de calidad
    4. Guarda en gold/ sin timestamp

Autor: Richi
Fecha: 06.01.2025
"""

import polars as pl
import json
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog
import sys
import time

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

def buscar_esquema_json() -> Path | None:
    """Busca el archivo de esquema JSON en ubicaciones comunes"""
    # Desde queries/step2_exportar_ingresos.py, buscar en esquemas/
    rutas_posibles = [
        Path("../esquemas/esquema_relacion_ingresos.json"),  # Desde queries/ hacia esquemas/
        Path("esquemas/esquema_relacion_ingresos.json"),      # Si se ejecuta desde raíz
        Path("../../esquemas/esquema_relacion_ingresos.json"), # Si hay más niveles
        Path("esquema_relacion_ingresos.json"),               # En el mismo directorio
    ]
    
    for ruta in rutas_posibles:
        if ruta.exists():
            return ruta
    
    return None

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet Silver"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver - EMPLEADOS",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """Carga el esquema JSON y extrae configuración de EMPLEADOS"""
    print(f"📋 Cargando esquema: {ruta_esquema.name}")
    
    with open(ruta_esquema, 'r', encoding='utf-8') as f:
        esquema_completo = json.load(f)
    
    # Extraer solo la configuración de EMPLEADOS
    if 'hojas' not in esquema_completo or 'EMPLEADOS' not in esquema_completo['hojas']:
        raise ValueError("El esquema no contiene configuración para EMPLEADOS")
    
    esquema = esquema_completo['hojas']['EMPLEADOS']
    
    # Mostrar metadata si existe
    if 'metadata' in esquema_completo:
        metadata = esquema_completo['metadata']
        print(f"   ✓ Versión: {metadata.get('version', 'N/A')}")
        print(f"   ✓ Última modificación: {metadata.get('fecha_actualizacion', 'N/A')}")
    
    print(f"   ✓ Columnas definidas: {len(esquema['schema'])}")
    
    return esquema


def seleccionar_y_convertir_columnas(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """Selecciona columnas y convierte tipos de datos según el esquema"""
    print(f"\n[2/3] Procesando columnas según esquema...")
    
    schema_def = esquema['schema']
    print(f"  - Columnas a procesar: {len(schema_def)}")
    
    # Verificar que todas las columnas existen
    columnas_faltantes = [col for col in schema_def.keys() if col not in df.columns]
    if columnas_faltantes:
        print(f"  ❌ Columnas faltantes en Silver: {columnas_faltantes}")
        raise ValueError(f"Columnas faltantes en el DataFrame: {columnas_faltantes}")
    
    # Mapeo de tipos del esquema a Polars
    tipo_map = {
        'string': pl.Utf8,
        'integer': pl.Int64,
        'float': pl.Float64,
        'boolean': pl.Boolean,
        'date': pl.Date,
    }
    
    # Seleccionar y convertir en una sola operación
    expresiones = []
    conversiones_aplicadas = 0
    
    for columna, config in schema_def.items():
        tipo_str = config.get('type', 'string').lower()
        tipo_polars = tipo_map.get(tipo_str, pl.Utf8)
        
        # Manejo especial para fechas: convertir de string con timestamp a Date
        if tipo_str == 'date':
            # Convertir string a datetime primero, luego extraer solo la fecha
            expresion = (
                pl.col(columna)
                .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
                .cast(pl.Date, strict=False)
                .alias(columna)
            )
        else:
            # Conversión normal para otros tipos
            expresion = pl.col(columna).cast(tipo_polars, strict=False).alias(columna)
        
        expresiones.append(expresion)
        conversiones_aplicadas += 1
    
    df_resultado = df.select(expresiones)
    
    print(f"  ✓ Conversiones de tipo aplicadas: {conversiones_aplicadas}")
    print(f"  ✓ Resultado: {df_resultado.height:,} filas × {df_resultado.width} columnas")
    
    return df_resultado


def generar_metricas_basicas(df: pl.DataFrame):
    """Genera métricas básicas de calidad"""
    print(f"\n📊 MÉTRICAS DE CALIDAD")
    print("=" * 80)
    print(f"Total de registros: {df.height:,}")
    print(f"Total de columnas: {df.width}")
    
    # Periodos únicos
    if "PERIODO" in df.columns:
        periodos = df["PERIODO"].unique().sort().to_list()
        print(f"Periodos únicos: {len(periodos)}")
        if len(periodos) <= 12:
            print(f"  → {', '.join(periodos)}")
    
    # Nulos por columna
    print(f"\nValores nulos por columna:")
    tiene_nulos = False
    for col in df.columns:
        nulos = df[col].is_null().sum()
        if nulos > 0:
            pct = (nulos / df.height * 100) if df.height > 0 else 0
            print(f"   {col:30}: {nulos:4} ({pct:5.2f}%)")
            tiene_nulos = True
    
    if not tiene_nulos:
        print("   ✓ Sin valores nulos")
    
    print("=" * 80)


def guardar_resultados(df: pl.DataFrame, carpeta_silver: Path):
    """
    Guarda el DataFrame en carpeta gold/ con sistema de versionamiento:
    - Archivos actuales sin timestamp en gold/
    - Copia con timestamp en gold/historico/
    
    Args:
        df: DataFrame a guardar
        carpeta_silver: Path de la carpeta donde está el archivo Silver
        
    Returns:
        tuple: (ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico)
    """
    # Crear carpeta gold/ un nivel arriba de silver/
    carpeta_trabajo = carpeta_silver.parent
    carpeta_gold = carpeta_trabajo / "gold"
    carpeta_gold.mkdir(exist_ok=True)
    
    # Crear carpeta historico/ dentro de gold/
    carpeta_historico = carpeta_gold / "historico"
    carpeta_historico.mkdir(exist_ok=True)
    
    # Timestamp para archivo histórico
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    print(f"\n[3/3] Guardando resultados en capa Gold...")
    print(f"  📁 Carpeta Gold: {carpeta_gold}")
    print(f"  📁 Carpeta Histórico: {carpeta_historico}")
    
    # === ARCHIVOS ACTUALES (sin timestamp) ===
    nombre_actual = "empleados_gold"
    ruta_parquet_actual = carpeta_gold / f"{nombre_actual}.parquet"
    ruta_excel_actual = carpeta_gold / f"{nombre_actual}.xlsx"
    
    print(f"\n  📄 Archivos actuales (se sobreescriben):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_actual, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    df.write_excel(ruta_excel_actual)
    print(f" ✓")
    
    # === ARCHIVOS HISTÓRICOS (con timestamp) ===
    nombre_historico = f"empleados_gold_{timestamp}"
    ruta_parquet_historico = carpeta_historico / f"{nombre_historico}.parquet"
    ruta_excel_historico = carpeta_historico / f"{nombre_historico}.xlsx"
    
    print(f"\n  📦 Archivos históricos (con timestamp):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_historico, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    df.write_excel(ruta_excel_historico)
    print(f" ✓")
    
    return ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print(" TRANSFORMACIÓN SILVER → GOLD - EMPLEADOS ".center(80, "="))
    print("=" * 80)
    print("\n💡 Nota: Solo EMPLEADOS se procesa a Gold")
    print("   PRACTICANTES permanece en Silver para consultas\n")
    
    # Iniciar cronómetro
    tiempo_inicio = time.time()
    
    # 1. Buscar esquema JSON
    ruta_esquema = buscar_esquema_json()
    
    if not ruta_esquema:
        print("⚠️  No se encontró el esquema JSON automáticamente.")
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
    print("\n[PASO 1] Selecciona el archivo Parquet Silver - EMPLEADOS...")
    archivo_silver = seleccionar_archivo_parquet()
    
    if not archivo_silver:
        print("❌ No se seleccionó archivo. Proceso cancelado.")
        return
    
    print(f"✓ Archivo seleccionado: {archivo_silver.name}")
    print(f"  Ubicación: {archivo_silver.parent}")
    
    # 4. Leer datos Silver
    print(f"\n[1/3] Cargando archivo Silver...")
    print(f"  📖 Archivo: {archivo_silver.name}")
    
    try:
        # 5. Leer datos Silver
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Datos cargados: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # 6. Seleccionar columnas y convertir tipos
        df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)

        # 6.1 Agregar columna enriquecida NOMBRE_MES
        df_gold = df_gold.with_columns([
            pl.col("MES").map_elements(
                lambda m: {
                    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
                    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
                }.get(m, ""),
                return_dtype=pl.Utf8
            ).alias("NOMBRE_MES")
        ])

        # Reordenar para que NOMBRE_MES esté después de MES
        columnas_ordenadas = []
        for col in df_gold.columns:
            columnas_ordenadas.append(col)
            if col == "MES":
                columnas_ordenadas.append("NOMBRE_MES")

        # Eliminar duplicado de NOMBRE_MES al final si existe
        columnas_ordenadas = [col for i, col in enumerate(columnas_ordenadas) 
                            if col != "NOMBRE_MES" or columnas_ordenadas[:i].count("NOMBRE_MES") == 0 
                            or (i > 0 and columnas_ordenadas[i-1] == "MES")]

        df_gold = df_gold.select(columnas_ordenadas)

        # 7. Generar métricas
        generar_metricas_basicas(df_gold)
        
        # 8. Guardar archivos (en la misma carpeta que el archivo Silver)
        carpeta_trabajo = archivo_silver.parent
        ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados(df_gold, carpeta_trabajo)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 9. Resumen final
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Transformación completada exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"  - Total de registros: {df_gold.height:,}")
        print(f"  - Columnas en Gold: {df_gold.width}")
        
        print(f"\n📁 Archivos generados:")
        print(f"\n  Actuales (para Power BI):")
        print(f"    - {ruta_parquet_actual.name}")
        print(f"    - {ruta_excel_actual.name}")
        
        print(f"\n  Históricos (con timestamp):")
        print(f"    - {ruta_parquet_historico.name}")
        print(f"    - {ruta_excel_historico.name}")
        
        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")
        
        print("\n💡 Notas:")
        print("  - Archivos actuales: se sobreescriben en cada ejecución (rutas estables para Power BI)")
        print("  - Archivos históricos: se archivan con timestamp para auditoría")
        print(f"  - Conectar Power BI a: {ruta_parquet_actual}")
        
        print("\n📂 Estructura de carpetas:")
        print(f"  carpeta_trabajo/")
        print(f"  ├── silver/")
        print(f"  │   └── {archivo_silver.name}")
        print(f"  └── gold/")
        print(f"      ├── {ruta_parquet_actual.name}")
        print(f"      ├── {ruta_excel_actual.name}")
        print(f"      └── historico/")
        print(f"          ├── {ruta_parquet_historico.name}")
        print(f"          └── {ruta_excel_historico.name}")
        
        print("\n" + "=" * 80)
        
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