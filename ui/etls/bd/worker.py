"""
Worker para procesamiento ETL de Base de Datos
Implementa Bronze→Silver→Gold + Centros de Costo + Flags
"""

import polars as pl
import openpyxl
import json
import re
from pathlib import Path
from datetime import datetime

from ui.workers.base_worker import BaseWorker
from utils.lazy_loader import LazyLoader


class BDWorker(BaseWorker):
    """
    Worker para procesamiento de Base de Datos
    
    Modos:
    - 'etl_completo': Ejecuta Steps 1 + 1.5 + 2
    - 'flags': Ejecuta Step 3 (requiere archivos CC)
    """
    
    def __init__(self, archivo_excel=None, modo='etl_completo', archivos_cc=None):
        super().__init__()
        self.archivo_excel = archivo_excel
        self.modo = modo
        self.archivos_cc = archivos_cc
        
        # Lazy loading de DuckDB (solo si se ejecuta step 3)
        self.duckdb = None
    
    def run(self):
        """Ejecuta el procesamiento según el modo"""
        try:
            if self.modo == 'etl_completo':
                self.ejecutar_etl_completo()
            elif self.modo == 'flags':
                self.ejecutar_flags()
            else:
                raise ValueError(f"Modo desconocido: {self.modo}")
            
            self.signals.finished.emit()
            
        except Exception as e:
            self.signals.error.emit(str(e))
    
    def ejecutar_etl_completo(self):
        """Ejecuta Steps 1 + 1.5 + 2"""
        self.emit_log("=" * 80)
        self.emit_log(" PROCESAMIENTO BD - ETL COMPLETO ".center(80, "="))
        self.emit_log("=" * 80)
        
        inicio_total = datetime.now()
        
        # Step 1: Bronze → Silver
        self.emit_log("\n[STEP 1/3] Bronze → Silver")
        self.emit_progress(10)
        carpeta_trabajo = self.step1_bronze_to_silver()
        self.emit_progress(35)
        
        # Step 1.5: Extracción de Centros de Costo
        self.emit_log("\n[STEP 2/3] Extracción de Centros de Costo")
        self.emit_progress(40)
        self.step1_5_extraer_centros_costo(carpeta_trabajo)
        self.emit_progress(60)
        
        # Step 2: Silver → Gold
        self.emit_log("\n[STEP 3/3] Silver → Gold")
        self.emit_progress(65)
        self.step2_silver_to_gold(carpeta_trabajo)
        self.emit_progress(100)
        
        # Resumen final
        duracion = (datetime.now() - inicio_total).total_seconds()
        self.emit_log("\n" + "=" * 80)
        self.emit_log(" RESUMEN FINAL ".center(80, "="))
        self.emit_log("=" * 80)
        self.emit_log(f"\n✅ ETL Completo finalizado en {duracion:.2f}s")
    
    def ejecutar_flags(self):
        """Ejecuta Step 3: Aplicación de flags"""
        self.emit_log("=" * 80)
        self.emit_log(" APLICACIÓN DE FLAGS - BD EMPLEADOS ".center(80, "="))
        self.emit_log("=" * 80)
        
        inicio = datetime.now()
        
        # Lazy load de DuckDB
        if not self.duckdb:
            self.emit_log("\n📦 Cargando DuckDB...")
            self.duckdb = LazyLoader.load_duckdb()
        
        self.emit_progress(10)
        self.step3_aplicar_flags()
        self.emit_progress(100)
        
        duracion = (datetime.now() - inicio).total_seconds()
        self.emit_log(f"\n✅ Flags aplicados en {duracion:.2f}s")
    
    # ========================================================================
    # STEP 1: BRONZE → SILVER
    # ========================================================================
    
    def step1_bronze_to_silver(self):
        """
        Procesa Excel Bronze → Parquet Silver
        
        Returns:
            Path: Carpeta de trabajo donde se guardó Silver
        """
        inicio = datetime.now()
        
        self.emit_log(f"\n📂 Archivo: {self.archivo_excel.name}")
        carpeta_trabajo = self.archivo_excel.parent
        
        # Extraer datos
        self.emit_log("  📊 Extrayendo datos del Excel...")
        headers, data_rows = self._extraer_datos_excel(
            self.archivo_excel,
            header_row=10,
            data_start_row=11
        )
        
        # Crear DataFrame
        self.emit_log("  🔄 Creando DataFrame...")
        df = self._crear_dataframe_polars(headers, data_rows)
        
        # Guardar Silver
        self.emit_log("  💾 Guardando en capa Silver...")
        self._guardar_silver(df, carpeta_trabajo)
        
        duracion = (datetime.now() - inicio).total_seconds()
        self.emit_log(f"  ✅ Step 1 completado en {duracion:.2f}s")
        
        return carpeta_trabajo
    
    def _extraer_datos_excel(self, file_path, header_row=10, data_start_row=11):
        """
        Extrae encabezados y datos del Excel usando openpyxl
        Optimizado con iter_rows
        """
        wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        ws = wb.active
        
        # Extraer encabezados
        headers = []
        for cell in ws[header_row]:
            if cell.value:
                headers.append(str(cell.value))
            else:
                break
        
        self.emit_log(f"    ✓ {len(headers)} columnas detectadas")
        
        # Buscar columna NUMERO DE DOC
        numero_doc_col_idx = None
        for idx, header in enumerate(headers, 1):
            if "NUMERO" in header.upper() and "DOC" in header.upper():
                numero_doc_col_idx = idx
                break
        
        if not numero_doc_col_idx:
            raise ValueError("No se encontró la columna 'NUMERO DE DOC'")
        
        # Patrón para detectar fechas DD/MM/YYYY
        date_pattern = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')
        conversiones_fecha = 0
        
        # Extraer datos
        data_rows = []
        for row in ws.iter_rows(min_row=data_start_row, max_col=len(headers), values_only=True):
            # Verificar si hay datos en NUMERO DE DOC
            if row[numero_doc_col_idx - 1] is None or str(row[numero_doc_col_idx - 1]).strip() == "":
                break
            
            row_data = []
            for cell_value in row:
                # Convertir fechas DD/MM/YYYY a datetime
                if isinstance(cell_value, str):
                    cell_value = cell_value.strip()
                    if date_pattern.match(cell_value):
                        try:
                            cell_value = datetime.strptime(cell_value, "%d/%m/%Y")
                            conversiones_fecha += 1
                        except ValueError:
                            pass
                
                row_data.append(cell_value)
            
            data_rows.append(row_data)
        
        wb.close()
        
        self.emit_log(f"    ✓ {len(data_rows):,} filas extraídas")
        if conversiones_fecha > 0:
            self.emit_log(f"    ✓ {conversiones_fecha} fechas convertidas")
        
        return headers, data_rows
    
    def _crear_dataframe_polars(self, headers, data_rows):
        """Crea DataFrame Polars con conversión de tipos"""
        # Procesar data_rows: datetime → string YYYY-MM-DD HH:MM:SS
        processed_rows = []
        for row in data_rows:
            processed_row = []
            for value in row:
                if isinstance(value, datetime):
                    processed_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
                else:
                    processed_row.append(value)
            processed_rows.append(processed_row)
        
        # Crear diccionario
        data_dict = {
            header: [row[i] if i < len(row) else None for row in processed_rows]
            for i, header in enumerate(headers)
        }
        
        # Convertir todo a string
        for key in data_dict:
            data_dict[key] = [str(v) if v is not None else None for v in data_dict[key]]
        
        # Reemplazar 'None'/'nan' por None real
        for key in data_dict:
            data_dict[key] = [None if v in ['None', 'nan', 'NaT'] else v for v in data_dict[key]]
        
        df = pl.DataFrame(data_dict, strict=False)
        
        self.emit_log(f"    ✓ DataFrame: {df.height:,} filas × {df.width} columnas")
        
        return df
    
    def _guardar_silver(self, df, carpeta_trabajo):
        """Guarda Silver sin timestamp (se sobrescribe)"""
        carpeta_silver = carpeta_trabajo / "silver"
        carpeta_silver.mkdir(exist_ok=True)
        
        ruta_parquet = carpeta_silver / "bd_silver.parquet"
        ruta_excel = carpeta_silver / "bd_silver.xlsx"
        
        df.write_parquet(ruta_parquet, compression="snappy")
        self.emit_log(f"    ✓ Parquet: {ruta_parquet.name}")
        
        df.write_excel(ruta_excel)
        self.emit_log(f"    ✓ Excel: {ruta_excel.name}")
    
    # ========================================================================
    # STEP 1.5: EXTRACCIÓN DE CENTROS DE COSTO
    # ========================================================================
    
    def step1_5_extraer_centros_costo(self, carpeta_trabajo):
        """
        Extrae tabla única de Centros de Costo desde Silver
        SIEMPRE genera archivos con timestamp
        """
        inicio = datetime.now()
        
        # Cargar esquema CC
        ruta_esquema = self._buscar_esquema('esquema_cc.json')
        if not ruta_esquema:
            raise FileNotFoundError("No se encontró esquema_cc.json")
        
        with open(ruta_esquema, 'r', encoding='utf-8') as f:
            esquema_cc = json.load(f)
        
        self.emit_log(f"  📋 Esquema CC cargado")
        
        # Cargar Silver
        ruta_silver = carpeta_trabajo / "silver" / "bd_silver.parquet"
        df_silver = pl.read_parquet(ruta_silver)
        self.emit_log(f"  📊 Silver cargado: {len(df_silver):,} filas")
        
        # Extraer y deduplicar
        columnas_cc = esquema_cc['columnas_requeridas']
        columna_dedupe = esquema_cc['columna_deduplicacion']
        
        df_cc = df_silver.select(columnas_cc)
        df_cc = df_cc.unique(subset=[columna_dedupe], keep='first')
        df_cc = df_cc.sort(columna_dedupe)
        
        self.emit_log(f"  🔄 Centros de costo únicos: {len(df_cc):,}")
        
        # Guardar con timestamp
        carpeta_cc = carpeta_trabajo / "centros_costo"
        carpeta_cc.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        ruta_parquet = carpeta_cc / f"cc_{timestamp}.parquet"
        ruta_excel = carpeta_cc / f"cc_{timestamp}.xlsx"
        
        df_cc.write_parquet(ruta_parquet, compression="snappy")
        self.emit_log(f"  ✓ Parquet: {ruta_parquet.name}")
        
        df_cc.write_excel(ruta_excel)
        self.emit_log(f"  ✓ Excel: {ruta_excel.name}")
        
        duracion = (datetime.now() - inicio).total_seconds()
        self.emit_log(f"  ✅ Step 1.5 completado en {duracion:.2f}s")
        self.emit_log("  ℹ️  Usuario debe seleccionar manualmente CC_ACTUAL y CC_OLD(s)")
    
    # ========================================================================
    # STEP 2: SILVER → GOLD
    # ========================================================================
    
    def step2_silver_to_gold(self, carpeta_trabajo):
        """Transforma Silver → Gold con división Empleados/Practicantes"""
        inicio = datetime.now()
        
        # Cargar esquema BD
        ruta_esquema = self._buscar_esquema('esquema_bd.json')
        if not ruta_esquema:
            raise FileNotFoundError("No se encontró esquema_bd.json")
        
        with open(ruta_esquema, 'r', encoding='utf-8') as f:
            esquema_bd = json.load(f)
        
        self.emit_log(f"  📋 Esquema BD cargado ({len(esquema_bd['columns'])} columnas)")
        
        # Cargar Silver
        ruta_silver = carpeta_trabajo / "silver" / "bd_silver.parquet"
        df_silver = pl.read_parquet(ruta_silver)
        self.emit_log(f"  📊 Silver cargado: {df_silver.height:,} × {df_silver.width}")
        
        # Validar y filtrar columnas
        self.emit_log("  🔍 Validando columnas...")
        df_gold = self._validar_y_filtrar_columnas(df_silver, esquema_bd)
        
        # Convertir fechas
        self.emit_log("  📅 Convirtiendo fechas...")
        df_gold = self._convertir_columnas_fecha(df_gold, esquema_bd)
        
        # Dividir por modalidad
        self.emit_log("  📂 Dividiendo por modalidad de contrato...")
        df_empleados, df_practicantes = self._dividir_por_modalidad(df_gold)
        
        # Guardar resultados
        self.emit_log("  💾 Guardando archivos Gold...")
        self._guardar_gold(df_empleados, df_practicantes, carpeta_trabajo)
        
        duracion = (datetime.now() - inicio).total_seconds()
        self.emit_log(f"  ✅ Step 2 completado en {duracion:.2f}s")
    
    def _validar_y_filtrar_columnas(self, df, esquema):
        """Filtra columnas según esquema preservando orden original"""
        required_columns = [col["name"] for col in esquema["columns"]]
        
        # Filtrar en orden original del DataFrame
        columns_to_select = [col for col in df.columns if col in required_columns]
        df_filtered = df.select(columns_to_select)
        
        present = len(columns_to_select)
        total = len(required_columns)
        missing = total - present
        
        self.emit_log(f"    ✓ Columnas: {present}/{total} presentes")
        if missing > 0:
            self.emit_log(f"    ⚠️  {missing} columnas faltantes")
        
        return df_filtered
    
    def _convertir_columnas_fecha(self, df, esquema):
        """Convierte columnas de tipo date desde String YYYY-MM-DD HH:MM:SS"""
        date_columns = [col["name"] for col in esquema["columns"] if col["type"] == "date"]
        
        for col_name in date_columns:
            if col_name in df.columns:
                try:
                    df = df.with_columns(
                        pl.col(col_name)
                        .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
                        .cast(pl.Date, strict=False)
                        .alias(col_name)
                    )
                    self.emit_log(f"    ✓ {col_name} → Date")
                except Exception as e:
                    self.emit_log(f"    ⚠️  Error en {col_name}: {str(e)}")
        
        return df
    
    def _dividir_por_modalidad(self, df):
        """Divide en Empleados y Practicantes según Modalidad de Contrato"""
        if "Modalidad de Contrato" not in df.columns:
            raise ValueError("Columna 'Modalidad de Contrato' no encontrada")
        
        df_practicantes = df.filter(
            pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
        )
        
        df_empleados = df.filter(
            ~pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
        )
        
        self.emit_log(f"    ✓ Practicantes: {df_practicantes.height:,}")
        self.emit_log(f"    ✓ Empleados: {df_empleados.height:,}")
        
        return df_empleados, df_practicantes
    
    def _guardar_gold(self, df_empleados, df_practicantes, carpeta_trabajo):
        """Guarda Gold con versionamiento dual (actual + histórico)"""
        carpeta_gold = carpeta_trabajo / "gold"
        carpeta_gold.mkdir(exist_ok=True)
        
        carpeta_historico = carpeta_gold / "historico"
        carpeta_historico.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        
        # EMPLEADOS
        if df_empleados.height > 0:
            # Actual
            ruta_emp_actual = carpeta_gold / "bd_empleados_gold.parquet"
            df_empleados.write_parquet(ruta_emp_actual, compression="snappy")
            self.emit_log(f"    ✓ Empleados (actual): {ruta_emp_actual.name}")
            
            ruta_emp_excel = carpeta_gold / "bd_empleados_gold.xlsx"
            df_empleados.write_excel(ruta_emp_excel)
            
            # Histórico
            ruta_emp_hist = carpeta_historico / f"bd_empleados_gold_{timestamp}.parquet"
            df_empleados.write_parquet(ruta_emp_hist, compression="snappy")
            self.emit_log(f"    ✓ Empleados (histórico): {ruta_emp_hist.name}")
        
        # PRACTICANTES
        if df_practicantes.height > 0:
            # Actual
            ruta_prac_actual = carpeta_gold / "bd_practicantes_gold.parquet"
            df_practicantes.write_parquet(ruta_prac_actual, compression="snappy")
            self.emit_log(f"    ✓ Practicantes (actual): {ruta_prac_actual.name}")
            
            ruta_prac_excel = carpeta_gold / "bd_practicantes_gold.xlsx"
            df_practicantes.write_excel(ruta_prac_excel)
            
            # Histórico
            ruta_prac_hist = carpeta_historico / f"bd_practicantes_gold_{timestamp}.parquet"
            df_practicantes.write_parquet(ruta_prac_hist, compression="snappy")
            self.emit_log(f"    ✓ Practicantes (histórico): {ruta_prac_hist.name}")
    
    # ========================================================================
    # STEP 3: APLICACIÓN DE FLAGS
    # ========================================================================
    
    def step3_aplicar_flags(self):
        """Aplica flags de negocio a empleados usando DuckDB"""
        inicio = datetime.now()
        
        # Validar que se hayan seleccionado archivos CC
        if not self.archivos_cc or 'cc_actual' not in self.archivos_cc:
            raise ValueError("No se seleccionaron archivos de Centros de Costo")
        
        cc_actual_path = self.archivos_cc['cc_actual']
        cc_old_paths = self.archivos_cc.get('cc_old', [])
        
        self.emit_log(f"  📂 CC_ACTUAL: {cc_actual_path.name}")
        self.emit_log(f"  📂 CC_OLD: {len(cc_old_paths)} archivo(s)")
        
        # Buscar bd_empleados_gold.parquet
        carpeta_trabajo = cc_actual_path.parent.parent
        ruta_empleados = carpeta_trabajo / "gold" / "bd_empleados_gold.parquet"
        
        if not ruta_empleados.exists():
            raise FileNotFoundError(f"No se encontró {ruta_empleados}")
        
        # Cargar datos
        self.emit_log("  📊 Cargando datos...")
        df_empleados = pl.read_parquet(ruta_empleados)
        df_cc_actual = pl.read_parquet(cc_actual_path)
        
        self.emit_log(f"    ✓ Empleados: {len(df_empleados):,} registros")
        self.emit_log(f"    ✓ CC_ACTUAL: {len(df_cc_actual):,} registros")
        
        # Cargar CC_OLD si existen
        df_cc_old_list = []
        for cc_old_path in cc_old_paths:
            df_cc_old = pl.read_parquet(cc_old_path)
            df_cc_old_list.append(df_cc_old)
            self.emit_log(f"    ✓ CC_OLD: {cc_old_path.name} ({len(df_cc_old):,} registros)")
        
        # Registrar tablas en DuckDB
        self.emit_log("  🦆 Registrando tablas en DuckDB...")
        conn = self.duckdb.connect()
        
        conn.execute("CREATE OR REPLACE TABLE empleados AS SELECT * FROM df_empleados")
        conn.execute("CREATE OR REPLACE TABLE cc_actual AS SELECT * FROM df_cc_actual")
        
        for i, df_cc_old in enumerate(df_cc_old_list):
            conn.execute(f"CREATE OR REPLACE TABLE cc_old_{i} AS SELECT * FROM df_cc_old")
        
        # Cargar queries SQL
        self.emit_log("  📜 Cargando queries SQL...")
        ruta_query_flags = self._buscar_query('queries_flags_gold.sql')
        
        if not ruta_query_flags:
            raise FileNotFoundError("No se encontró queries_flags_gold.sql")
        
        with open(ruta_query_flags, 'r', encoding='utf-8') as f:
            query_flags = f.read()
        
        # Ejecutar query
        self.emit_log("  ⚙️  Aplicando flags de negocio...")
        result = conn.execute(query_flags).pl()
        
        conn.close()
        
        self.emit_log(f"  ✓ Flags aplicados: {len(result):,} registros")
        
        # Guardar resultado con timestamp
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        ruta_output = carpeta_trabajo / "gold" / f"bd_empleados_gold_flags_{timestamp}.parquet"
        ruta_excel = carpeta_trabajo / "gold" / f"bd_empleados_gold_flags_{timestamp}.xlsx"
        
        result.write_parquet(ruta_output, compression="snappy")
        self.emit_log(f"  ✓ Parquet: {ruta_output.name}")
        
        result.write_excel(ruta_excel)
        self.emit_log(f"  ✓ Excel: {ruta_excel.name}")
        
        duracion = (datetime.now() - inicio).total_seconds()
        self.emit_log(f"  ✅ Step 3 completado en {duracion:.2f}s")
    
    # ========================================================================
    # UTILIDADES
    # ========================================================================
    
    def _buscar_esquema(self, nombre_archivo):
        """Busca archivo de esquema en ubicaciones comunes"""
        rutas_posibles = [
            Path(f"../esquemas/{nombre_archivo}"),
            Path(f"esquemas/{nombre_archivo}"),
            Path(f"../../esquemas/{nombre_archivo}"),
            Path(nombre_archivo),
        ]
        
        for ruta in rutas_posibles:
            if ruta.exists():
                return ruta
        
        return None
    
    def _buscar_query(self, nombre_archivo):
        """Busca archivo SQL en ubicaciones comunes"""
        rutas_posibles = [
            Path(f"../queries/{nombre_archivo}"),
            Path(f"queries/{nombre_archivo}"),
            Path(f"../../queries/{nombre_archivo}"),
            Path(nombre_archivo),
        ]
        
        for ruta in rutas_posibles:
            if ruta.exists():
                return ruta
        
        return None