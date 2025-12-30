import polars as pl
import json
from pathlib import Path
from tkinter import Tk, filedialog
from datetime import datetime
import unicodedata
import re

def seleccionar_archivo(titulo, tipos):
    """Abre un diálogo para seleccionar archivo"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title=titulo,
        filetypes=tipos
    )
    root.destroy()
    return archivo

def aplicar_transformaciones_gold(df, schema):
    """Aplica transformaciones según el esquema gold"""
    
    # NO normalizar columnas - mantener nombres originales del parquet
    print(f"\n📋 Seleccionando columnas del schema...")
    print("-" * 70)
    
    # Seleccionar solo las columnas que existen en el schema
    columnas_schema = list(schema["schema"].keys())
    columnas_disponibles = [col for col in columnas_schema if col in df.columns]
    columnas_faltantes = [col for col in columnas_schema if col not in df.columns]
    
    for col in columnas_disponibles:
        print(f"✓ {col}")
    
    if columnas_faltantes:
        print(f"\n⚠️  Columnas del schema no encontradas en el parquet:")
        for col in columnas_faltantes:
            print(f"  ✗ {col}")
    
    print("-" * 70)
    print(f"Total: {len(columnas_disponibles)} de {len(columnas_schema)} columnas")
    print()
    
    # Seleccionar columnas
    df = df.select(columnas_disponibles)
    
    # Aplicar tipos de datos según schema
    print("🔄 Aplicando tipos de datos...")
    for col_name in columnas_disponibles:
        col_spec = schema["schema"][col_name]
        col_type = col_spec["type"]
        
        try:
            if col_type == "string":
                df = df.with_columns(pl.col(col_name).cast(pl.Utf8))
            elif col_type == "integer":
                df = df.with_columns(pl.col(col_name).cast(pl.Int64))
            elif col_type == "float":
                df = df.with_columns(pl.col(col_name).cast(pl.Float64))
            elif col_type == "date":
                # Manejar fechas que vienen como datetime string
                df = df.with_columns(
                    pl.col(col_name).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=False).cast(pl.Date)
                )
        except Exception as e:
            print(f"  ⚠️  Error al convertir columna {col_name}: {e}")
    
    # Aplicar string trimming
    print("✂️  Limpiando espacios en strings...")
    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.strip_chars())
    
    return df

def validar_constraints(df, schema):
    """Valida constraints del schema"""
    errores = []
    warnings = []
    
    print("\n✅ Validando constraints...")
    print("-" * 70)
    
    # Validar primary key
    pk_cols = schema["constraints"]["primary_key"]
    pk_cols_existentes = [col for col in pk_cols if col in df.columns]
    
    if len(pk_cols_existentes) != len(pk_cols):
        faltantes = set(pk_cols) - set(pk_cols_existentes)
        warnings.append(f"⚠️  Columnas de primary key faltantes: {faltantes}")
        pk_cols = pk_cols_existentes
    
    if pk_cols:
        duplicados = df.group_by(pk_cols).agg(pl.len().alias("count")).filter(pl.col("count") > 1)
        
        if len(duplicados) > 0:
            errores.append(f"❌ Se encontraron {len(duplicados)} registros duplicados en primary key: {pk_cols}")
        else:
            print(f"✓ Primary key sin duplicados: {pk_cols}")
    
    # Validar nulls en columnas no nullable
    nulls_encontrados = False
    for col_name, col_spec in schema["schema"].items():
        if col_name not in df.columns:
            continue
            
        if not col_spec.get("nullable", True):
            nulls = df.filter(pl.col(col_name).is_null()).height
            if nulls > 0:
                errores.append(f"❌ Columna '{col_name}' tiene {nulls} valores nulos (no permitidos)")
                nulls_encontrados = True
    
    if not nulls_encontrados:
        print("✓ Validación de nulls correcta")
    
    # Validar valores permitidos
    valores_invalidos_encontrados = False
    for col_name, col_spec in schema["schema"].items():
        if col_name not in df.columns:
            continue
            
        if "allowed_values" in col_spec:
            valores_invalidos = df.filter(
                ~pl.col(col_name).is_in(col_spec["allowed_values"]) & 
                pl.col(col_name).is_not_null()
            )
            if len(valores_invalidos) > 0:
                valores_unicos = valores_invalidos[col_name].unique().to_list()[:5]
                warnings.append(f"⚠️  Columna '{col_name}' tiene {len(valores_invalidos)} valores fuera del rango permitido: {valores_unicos}")
                valores_invalidos_encontrados = True
    
    if not valores_invalidos_encontrados:
        print("✓ Validación de valores permitidos correcta")
    
    # Validar rangos numéricos
    rangos_invalidos_encontrados = False
    for col_name, col_spec in schema["schema"].items():
        if col_name not in df.columns:
            continue
            
        if "min_value" in col_spec:
            fuera_rango = df.filter(
                (pl.col(col_name) < col_spec["min_value"]) & 
                pl.col(col_name).is_not_null()
            ).height
            if fuera_rango > 0:
                warnings.append(f"⚠️  Columna '{col_name}' tiene {fuera_rango} valores menores al mínimo permitido ({col_spec['min_value']})")
                rangos_invalidos_encontrados = True
    
    if not rangos_invalidos_encontrados:
        print("✓ Validación de rangos numéricos correcta")
    
    print("-" * 70)
    
    return errores, warnings

def generar_excel_visualizacion(df, ruta_salida):
    """Genera Excel con formato para visualización"""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils.dataframe import dataframe_to_rows
    
    print("\n📊 Generando Excel de visualización...")
    
    df_pandas = df.to_pandas()
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Planilla Gold"
    
    # Agregar datos
    for r_idx, row in enumerate(dataframe_to_rows(df_pandas, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            
            if r_idx == 1:
                cell.font = Font(bold=True, color="FFFFFF", size=11)
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            else:
                cell.alignment = Alignment(horizontal="left", vertical="center")
            
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            cell.border = thin_border
    
    # Ajustar ancho de columnas
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    # Congelar primera fila
    ws.freeze_panes = "A2"
    
    wb.save(ruta_salida)
    print(f"✓ Excel guardado exitosamente")

def main():
    print("=" * 70)
    print("TRANSFORMACIÓN SILVER → GOLD - REPORTES DE PLANILLA")
    print("=" * 70)
    print()
    
    # Seleccionar parquet silver
    print("📁 Seleccione el archivo Parquet Silver...")
    ruta_parquet = seleccionar_archivo(
        "Seleccione el archivo Parquet Silver",
        [("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    if not ruta_parquet:
        print("❌ No se seleccionó archivo parquet. Operación cancelada.")
        return
    
    print(f"✓ Archivo seleccionado: {Path(ruta_parquet).name}")
    print()
    
    # Buscar carpeta de esquemas - buscar en directorio actual y niveles superiores
    carpeta_actual = Path.cwd()
    carpeta_esquemas = None
    
    # Buscar en el directorio actual y hasta 3 niveles arriba
    for _ in range(4):
        posible_esquemas = carpeta_actual / "esquemas"
        if posible_esquemas.exists() and posible_esquemas.is_dir():
            carpeta_esquemas = posible_esquemas
            break
        carpeta_actual = carpeta_actual.parent
    
    # Si no se encontró, crear en el directorio donde se ejecuta el script
    if carpeta_esquemas is None:
        carpeta_proyecto = Path.cwd()
        carpeta_esquemas = carpeta_proyecto / "esquemas"
        
        if not carpeta_esquemas.exists():
            print(f"⚠️  No se encontró la carpeta 'esquemas'")
            print(f"📁 Creando carpeta 'esquemas' en: {carpeta_proyecto}")
            carpeta_esquemas.mkdir(exist_ok=True)
            print(f"✓ Carpeta creada: {carpeta_esquemas}")
            print()
            print("❌ Por favor, coloca los archivos JSON de esquemas en esta carpeta y ejecuta nuevamente.")
            return
    
    # Listar archivos JSON en la carpeta de esquemas
    esquemas_disponibles = list(carpeta_esquemas.glob("*.json"))
    
    if not esquemas_disponibles:
        print(f"❌ No se encontraron archivos JSON en: {carpeta_esquemas}")
        print(f"   Por favor, coloca los archivos de esquemas (.json) en esta carpeta.")
        return
    
    print(f"📁 Carpeta de esquemas: {carpeta_esquemas}")
    print(f"✓ Esquemas disponibles:")
    for i, esquema in enumerate(esquemas_disponibles, 1):
        print(f"   {i}. {esquema.name}")
    print()
    
    # Seleccionar schema JSON de la carpeta de esquemas
    print("📁 Seleccione el archivo JSON del esquema Gold...")
    
    # Cambiar directorio inicial del diálogo a la carpeta de esquemas
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    ruta_schema = filedialog.askopenfilename(
        title="Seleccione el esquema JSON Gold",
        initialdir=str(carpeta_esquemas),
        filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
    )
    root.destroy()
    
    if not ruta_schema:
        print("❌ No se seleccionó archivo schema. Operación cancelada.")
        return
    
    print(f"✓ Schema seleccionado: {Path(ruta_schema).name}")
    print()
    
    # Cargar datos
    print("📊 Cargando datos silver...")
    inicio = datetime.now()
    df = pl.read_parquet(ruta_parquet)
    print(f"✓ Datos cargados: {df.shape[0]:,} filas × {df.shape[1]} columnas")
    
    # Cargar schema
    print("\n📋 Cargando esquema gold...")
    with open(ruta_schema, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    print(f"✓ Schema versión {schema['metadata']['version']} cargado")
    print(f"✓ Columnas esperadas: {len(schema['schema'])}")
    
    # Aplicar transformaciones
    print("\n" + "=" * 70)
    print("APLICANDO TRANSFORMACIONES GOLD")
    print("=" * 70)
    
    try:
        df_gold = aplicar_transformaciones_gold(df, schema)
        print(f"\n✓ Transformaciones aplicadas exitosamente")
        print(f"  - Columnas finales: {df_gold.shape[1]}")
        print(f"  - Registros: {df_gold.shape[0]:,}")
    except Exception as e:
        print(f"\n❌ Error al aplicar transformaciones: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Validar constraints
    errores, warnings = validar_constraints(df_gold, schema)
    
    if errores:
        print("\n⚠️  ERRORES CRÍTICOS ENCONTRADOS:")
        for error in errores:
            print(f"  {error}")
        print()
        respuesta = input("¿Desea continuar de todos modos? (s/n): ")
        if respuesta.lower() != 's':
            print("❌ Operación cancelada por el usuario.")
            return
    
    if warnings:
        print("\n⚠️  ADVERTENCIAS:")
        for warning in warnings:
            print(f"  {warning}")
        print()
    
    # Preparar rutas de salida
    ruta_base = Path(ruta_parquet).parent
    nombre_base = Path(ruta_parquet).stem
    
    # Extraer timestamp del nombre original
    # Formato esperado: "Planilla_Metso_Consolidado_29_12_2025_14_23_21"
    import re
    match = re.search(r'(\d{2})_(\d{2})_(\d{4})_(\d{2})_(\d{2})_(\d{2})', nombre_base)
    
    if match:
        # Usar timestamp del archivo original en formato dd.mm.yyyy_hh.mm.ss
        dia, mes, anio, hora, minuto, segundo = match.groups()
        timestamp = f"{dia}.{mes}.{anio}_{hora}.{minuto}.{segundo}"
    else:
        # Si no hay timestamp, crear uno nuevo
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    # Nombre fijo: "Planilla Metso BI" con espacios
    nombre_final = f"Planilla Metso BI_{timestamp}_gold"
    
    ruta_parquet_gold = ruta_base / f"{nombre_final}.parquet"
    ruta_excel_gold = ruta_base / f"{nombre_final}.xlsx"
    
    # Guardar parquet gold
    print("\n💾 Guardando archivos...")
    print("-" * 70)
    df_gold.write_parquet(ruta_parquet_gold)
    print(f"✓ Parquet gold: {ruta_parquet_gold.name}")
    
    # Generar Excel de visualización
    try:
        generar_excel_visualizacion(df_gold, ruta_excel_gold)
        print(f"✓ Excel gold: {ruta_excel_gold.name}")
    except Exception as e:
        print(f"⚠️  Error al generar Excel: {e}")
    
    # Resumen final
    duracion = (datetime.now() - inicio).total_seconds()
    print("\n" + "=" * 70)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 70)
    print(f"⏱️  Tiempo de ejecución: {duracion:.2f} segundos")
    print(f"📊 Registros procesados: {df_gold.shape[0]:,}")
    print(f"📋 Schema utilizado: {Path(ruta_schema).name}")
    print(f"📁 Archivos generados en:")
    print(f"   {ruta_base}")
    print("=" * 70)

if __name__ == "__main__":
    main()