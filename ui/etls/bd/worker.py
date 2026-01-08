# ui/etls/bd/worker.py
"""
Worker para ETL de Base de Datos
Ejecuta: Bronze → Silver → Centros de Costo → Gold → Flags

ARQUITECTURA:
- Step 1: Bronze → Silver (Excel con headers en fila 10)
- Step 1.5: Extracción de Centros de Costo (con timestamp)
- Step 2: Silver → Gold (Empleados + Practicantes)
- Step 3: Aplicación de Flags (opcional, requiere archivos CC)
"""
from pathlib import Path
from typing import Dict
import sys
import time
import json
import re
from datetime import datetime

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ui.workers.base_worker import BaseETLWorker


class BDWorker(BaseETLWorker):
    """Worker para procesamiento de BD con lazy loading"""
    
    def __init__(self, archivos, output_dir):
        """
        Args:
            archivos: List[Path] - Lista con 1 archivo Excel
            output_dir: Path - Directorio de salida (carpeta de trabajo)
        """
        super().__init__(archivos, output_dir)
        
        # Lazy loading de DuckDB (solo si se ejecuta step 3)
        self.duckdb = None
        
        # Timers
        self.timers = {
            'total': 0,
            'step1': 0,
            'step1.5': 0,
            'step2': 0,
            'step3': 0
        }
    
    def get_pipeline_name(self) -> str:
        return "bd"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo de BD (Steps 1 + 1.5 + 2 + 3)
        
        Returns:
            dict con resultados del proceso
        """
        tiempo_inicio_total = time.time()
        
        try:
            return self._ejecutar_etl_completo(tiempo_inicio_total)
                
        except Exception as e:
            self.logger.error(f"❌ Error crítico en ETL: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            return {
                'success': False,
                'error': str(e),
                'timers': self.timers
            }
    
    # ========================================================================
    # MODO: ETL COMPLETO (Steps 1 + 1.5 + 2)
    # ========================================================================
    
    def _ejecutar_etl_completo(self, tiempo_inicio_total) -> Dict:
        """Ejecuta Steps 1 + 1.5 + 2 + 3"""
        resultado = {}
        
        # Validar que haya archivo
        if not self.archivos or len(self.archivos) == 0:
            raise ValueError("No se proporcionó archivo Excel")
        
        archivo_excel = self.archivos[0]  # BD usa solo 1 archivo
        
        self.logger.info("="*70)
        self.logger.info("PROCESAMIENTO BD - ETL COMPLETO")
        self.logger.info("="*70)
        self.logger.info(f"📂 Archivo: {archivo_excel.name}")
        self.logger.info(f"📁 Carpeta de trabajo: {self.output_dir}")
        self.logger.info("")
        
        # Step 1: Bronze → Silver
        self.progress_updated.emit(5, "🔄 Step 1: Bronze → Silver")
        resultado['step1'] = self._step1_bronze_to_silver(archivo_excel)
        self.progress_updated.emit(25, "✓ Step 1 completado")
        
        # Step 1.5: Extracción de Centros de Costo
        self.progress_updated.emit(30, "🔄 Step 1.5: Centros de Costo")
        resultado['step1.5'] = self._step1_5_extraer_centros_costo(resultado['step1']['parquet'])
        self.progress_updated.emit(50, "✓ Step 1.5 completado")
        
        # Step 2: Silver → Gold
        self.progress_updated.emit(55, "🔄 Step 2: Silver → Gold")
        resultado['step2'] = self._step2_silver_to_gold(resultado['step1']['parquet'])
        self.progress_updated.emit(75, "✓ Step 2 completado")
        
        # Step 3: Aplicación de Flags (usa el CC_ACTUAL recién generado)
        self.progress_updated.emit(80, "🔄 Step 3: Aplicando Flags")
        try:
            resultado['step3'] = self._step3_aplicar_flags_automatico(resultado['step1.5']['parquet'])
            self.progress_updated.emit(100, "✓ Step 3 completado")
        except Exception as e:
            self.logger.warning(f"⚠️  Step 3 (Flags) falló: {str(e)}")
            resultado['step3'] = {'error': str(e)}
            self.progress_updated.emit(100, "⚠️  ETL completado (Step 3 con errores)")
        
        # Resultado final
        self.timers['total'] = time.time() - tiempo_inicio_total
        resultado['success'] = True
        resultado['timers'] = self.timers
        
        mensaje_base = (
            f"ETL Completo finalizado:\n"
            f"  • Silver: {resultado['step1']['registros']:,} registros\n"
            f"  • Centros de Costo: {resultado['step1.5']['registros']:,} únicos\n"
            f"  • Empleados: {resultado['step2']['empleados']:,} registros\n"
            f"  • Practicantes: {resultado['step2']['practicantes']:,} registros\n"
        )
        
        if 'error' not in resultado['step3']:
            mensaje_base += f"  • Flags aplicados: {resultado['step3']['registros']:,} registros\n"
        else:
            mensaje_base += f"  ⚠️  Flags: Error (ver log)\n"
        
        mensaje_base += f"  ⏱️  Tiempo total: {self.logger.format_duration(self.timers['total'])}"
        
        resultado['mensaje'] = mensaje_base
        self.logger.info("\n" + "="*70)
        self.logger.info(mensaje_base)
        self.logger.info("="*70)
        
        return resultado
    
    # ========================================================================
    # STEP 1: BRONZE → SILVER
    # ========================================================================
    
    def _step1_bronze_to_silver(self, archivo_excel):
        """Procesa Excel Bronze → Parquet Silver"""
        tiempo_inicio = time.time()
        
        self.logger.info("="*70)
        self.logger.info("STEP 1: BRONZE → SILVER")
        self.logger.info("="*70)
        self.logger.info(f"📂 Archivo: {archivo_excel.name}")
        
        # Lazy load de openpyxl y polars
        import openpyxl
        import polars as pl
        
        # Extraer datos
        self.logger.info("📊 Extrayendo datos del Excel...")
        headers, data_rows = self._extraer_datos_excel(archivo_excel, openpyxl)
        
        # Crear DataFrame
        self.logger.info("🔄 Creando DataFrame...")
        df = self._crear_dataframe_polars(headers, data_rows, pl)
        
        # Guardar Silver
        self.logger.info("💾 Guardando en capa Silver...")
        ruta_parquet, ruta_excel = self._guardar_silver(df, self.output_dir)
        
        self.timers['step1'] = time.time() - tiempo_inicio
        
        self.logger.info("-"*70)
        self.logger.info(f"✓ Step 1 completado")
        self.logger.info(f"  • Registros: {len(df):,}")
        self.logger.info(f"  • Columnas: {len(df.columns)}")
        self.logger.info(f"  • Parquet: {ruta_parquet.name}")
        self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(self.timers['step1'])}")
        self.logger.info("-"*70)
        
        return {
            'registros': len(df),
            'columnas': len(df.columns),
            'parquet': ruta_parquet,
            'excel': ruta_excel,
            'duracion': self.timers['step1']
        }
    
    def _extraer_datos_excel(self, file_path, openpyxl):
        """Extrae encabezados y datos del Excel usando openpyxl"""
        wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
        ws = wb.active
        
        # Extraer encabezados (fila 10)
        headers = []
        for cell in ws[10]:
            if cell.value:
                headers.append(str(cell.value))
            else:
                break
        
        self.logger.info(f"  ✓ {len(headers)} columnas detectadas")
        
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
        
        # Extraer datos (desde fila 11)
        data_rows = []
        for row in ws.iter_rows(min_row=11, max_col=len(headers), values_only=True):
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
        
        self.logger.info(f"  ✓ {len(data_rows):,} filas extraídas")
        if conversiones_fecha > 0:
            self.logger.info(f"  ✓ {conversiones_fecha} fechas convertidas")
        
        return headers, data_rows
    
    def _crear_dataframe_polars(self, headers, data_rows, pl):
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
        
        self.logger.info(f"  ✓ DataFrame: {df.height:,} filas × {df.width} columnas")
        
        return df
    
    def _guardar_silver(self, df, carpeta_trabajo):
        """Guarda Silver sin timestamp (se sobrescribe)"""
        carpeta_silver = carpeta_trabajo / "silver"
        carpeta_silver.mkdir(exist_ok=True)
        
        ruta_parquet = carpeta_silver / "bd_silver.parquet"
        ruta_excel = carpeta_silver / "bd_silver.xlsx"
        
        df.write_parquet(ruta_parquet, compression="snappy")
        self.logger.info(f"  ✓ Parquet: {ruta_parquet.name}")
        
        df.write_excel(ruta_excel)
        self.logger.info(f"  ✓ Excel: {ruta_excel.name}")
        
        return ruta_parquet, ruta_excel
    
    # ========================================================================
    # STEP 1.5: EXTRACCIÓN DE CENTROS DE COSTO
    # ========================================================================
    
    def _step1_5_extraer_centros_costo(self, ruta_silver):
        """Extrae tabla única de Centros de Costo desde Silver"""
        tiempo_inicio = time.time()
        
        self.logger.info("")
        self.logger.info("="*70)
        self.logger.info("STEP 1.5: EXTRACCIÓN DE CENTROS DE COSTO")
        self.logger.info("="*70)
        
        import polars as pl
        
        # Cargar esquema CC
        esquema_path = self._buscar_esquema('esquema_cc.json')
        if not esquema_path:
            raise FileNotFoundError("No se encontró esquema_cc.json")
        
        with open(esquema_path, 'r', encoding='utf-8') as f:
            esquema_cc = json.load(f)
        
        self.logger.info(f"📋 Esquema CC cargado")
        
        # Cargar Silver
        df_silver = pl.read_parquet(ruta_silver)
        self.logger.info(f"📊 Silver cargado: {len(df_silver):,} filas")
        
        # Extraer y deduplicar
        columnas_cc = esquema_cc['columnas_requeridas']
        columna_dedupe = esquema_cc['columna_deduplicacion']
        
        df_cc = df_silver.select(columnas_cc)
        df_cc = df_cc.unique(subset=[columna_dedupe], keep='first')
        df_cc = df_cc.sort(columna_dedupe)
        
        self.logger.info(f"🔄 Centros de costo únicos: {len(df_cc):,}")
        
        # Guardar con timestamp
        carpeta_trabajo = ruta_silver.parent.parent
        carpeta_cc = carpeta_trabajo / "centros_costo"
        carpeta_cc.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        ruta_parquet = carpeta_cc / f"cc_{timestamp}.parquet"
        ruta_excel = carpeta_cc / f"cc_{timestamp}.xlsx"
        
        df_cc.write_parquet(ruta_parquet, compression="snappy")
        self.logger.info(f"  ✓ Parquet: {ruta_parquet.name}")
        
        df_cc.write_excel(ruta_excel)
        self.logger.info(f"  ✓ Excel: {ruta_excel.name}")
        
        self.timers['step1.5'] = time.time() - tiempo_inicio
        
        self.logger.info("-"*70)
        self.logger.info(f"✓ Step 1.5 completado")
        self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(self.timers['step1.5'])}")
        self.logger.info("  ℹ️  Usuario debe seleccionar manualmente CC_ACTUAL y CC_OLD(s)")
        self.logger.info("-"*70)
        
        return {
            'registros': len(df_cc),
            'parquet': ruta_parquet,
            'excel': ruta_excel,
            'duracion': self.timers['step1.5']
        }
    
    # ========================================================================
    # STEP 2: SILVER → GOLD
    # ========================================================================
    
    def _step2_silver_to_gold(self, ruta_silver):
        """Transforma Silver → Gold con división Empleados/Practicantes"""
        tiempo_inicio = time.time()
        
        self.logger.info("")
        self.logger.info("="*70)
        self.logger.info("STEP 2: SILVER → GOLD")
        self.logger.info("="*70)
        
        import polars as pl
        
        # Cargar esquema BD
        esquema_path = self._buscar_esquema('esquema_bd.json')
        if not esquema_path:
            raise FileNotFoundError("No se encontró esquema_bd.json")
        
        with open(esquema_path, 'r', encoding='utf-8') as f:
            esquema_bd = json.load(f)
        
        self.logger.info(f"📋 Esquema BD cargado ({len(esquema_bd['columns'])} columnas)")
        
        # Cargar Silver
        df_silver = pl.read_parquet(ruta_silver)
        self.logger.info(f"📊 Silver cargado: {df_silver.height:,} × {df_silver.width}")
        
        # Validar y filtrar columnas
        self.logger.info("🔍 Validando columnas...")
        df_gold = self._validar_y_filtrar_columnas(df_silver, esquema_bd, pl)
        
        # Convertir fechas
        self.logger.info("📅 Convirtiendo fechas...")
        df_gold = self._convertir_columnas_fecha(df_gold, esquema_bd, pl)
        
        # Dividir por modalidad
        self.logger.info("📂 Dividiendo por modalidad de contrato...")
        df_empleados, df_practicantes = self._dividir_por_modalidad(df_gold, pl)
        
        # Guardar resultados
        self.logger.info("💾 Guardando archivos Gold...")
        carpeta_trabajo = ruta_silver.parent.parent
        self._guardar_gold(df_empleados, df_practicantes, carpeta_trabajo)
        
        self.timers['step2'] = time.time() - tiempo_inicio
        
        self.logger.info("-"*70)
        self.logger.info(f"✓ Step 2 completado")
        self.logger.info(f"  • Empleados: {len(df_empleados):,}")
        self.logger.info(f"  • Practicantes: {len(df_practicantes):,}")
        self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(self.timers['step2'])}")
        self.logger.info("-"*70)
        
        return {
            'empleados': len(df_empleados),
            'practicantes': len(df_practicantes),
            'duracion': self.timers['step2']
        }
    
    def _validar_y_filtrar_columnas(self, df, esquema, pl):
        """Filtra columnas según esquema preservando orden original"""
        required_columns = [col["name"] for col in esquema["columns"]]
        
        # Filtrar en orden original del DataFrame
        columns_to_select = [col for col in df.columns if col in required_columns]
        df_filtered = df.select(columns_to_select)
        
        present = len(columns_to_select)
        total = len(required_columns)
        missing = total - present
        
        self.logger.info(f"  ✓ Columnas: {present}/{total} presentes")
        if missing > 0:
            self.logger.warning(f"  ⚠️  {missing} columnas faltantes")
        
        return df_filtered
    
    def _convertir_columnas_fecha(self, df, esquema, pl):
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
                    self.logger.info(f"  ✓ {col_name} → Date")
                except Exception as e:
                    self.logger.warning(f"  ⚠️  Error en {col_name}: {str(e)}")
        
        return df
    
    def _dividir_por_modalidad(self, df, pl):
        """Divide en Empleados y Practicantes según Modalidad de Contrato"""
        if "Modalidad de Contrato" not in df.columns:
            raise ValueError("Columna 'Modalidad de Contrato' no encontrada")
        
        df_practicantes = df.filter(
            pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
        )
        
        df_empleados = df.filter(
            ~pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
        )
        
        self.logger.info(f"  ✓ Practicantes: {df_practicantes.height:,}")
        self.logger.info(f"  ✓ Empleados: {df_empleados.height:,}")
        
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
            self.logger.info(f"  ✓ Empleados (actual): {ruta_emp_actual.name}")
            
            ruta_emp_excel = carpeta_gold / "bd_empleados_gold.xlsx"
            df_empleados.write_excel(ruta_emp_excel)
            
            # Histórico
            ruta_emp_hist = carpeta_historico / f"bd_empleados_gold_{timestamp}.parquet"
            df_empleados.write_parquet(ruta_emp_hist, compression="snappy")
            self.logger.info(f"  ✓ Empleados (histórico): {ruta_emp_hist.name}")
        
        # PRACTICANTES
        if df_practicantes.height > 0:
            # Actual
            ruta_prac_actual = carpeta_gold / "bd_practicantes_gold.parquet"
            df_practicantes.write_parquet(ruta_prac_actual, compression="snappy")
            self.logger.info(f"  ✓ Practicantes (actual): {ruta_prac_actual.name}")
            
            ruta_prac_excel = carpeta_gold / "bd_practicantes_gold.xlsx"
            df_practicantes.write_excel(ruta_prac_excel)
            
            # Histórico
            ruta_prac_hist = carpeta_historico / f"bd_practicantes_gold_{timestamp}.parquet"
            df_practicantes.write_parquet(ruta_prac_hist, compression="snappy")
            self.logger.info(f"  ✓ Practicantes (histórico): {ruta_prac_hist.name}")
    
    # ========================================================================
    # STEP 3: APLICACIÓN DE FLAGS
    # ========================================================================
    
    def _step3_aplicar_flags_automatico(self, ruta_cc_actual):
        """
        Aplica flags de negocio a empleados
        Solo necesita bd_empleados_gold.parquet, NO usa CC
        """
        tiempo_inicio = time.time()
        
        self.logger.info("")
        self.logger.info("="*70)
        self.logger.info("STEP 3: APLICACIÓN DE FLAGS")
        self.logger.info("="*70)
        
        import polars as pl
        
        # Cargar DuckDB (lazy: solo se importa aquí, no al inicio)
        if not self.duckdb:
            self.logger.info("📦 Cargando DuckDB...")
            try:
                import duckdb
                self.duckdb = duckdb
                self.logger.info("✓ DuckDB cargado")
            except ImportError:
                raise ImportError("DuckDB no está instalado. Instala con: pip install duckdb")
        
        # Buscar bd_empleados_gold.parquet
        carpeta_trabajo = ruta_cc_actual.parent.parent
        ruta_empleados = carpeta_trabajo / "gold" / "bd_empleados_gold.parquet"
        
        if not ruta_empleados.exists():
            raise FileNotFoundError(f"No se encontró {ruta_empleados}")
        
        # Cargar datos de empleados
        self.logger.info("📊 Cargando datos de empleados...")
        df_empleados = pl.read_parquet(ruta_empleados)
        self.logger.info(f"  ✓ Empleados: {len(df_empleados):,} registros")
        
        # Registrar tabla en DuckDB
        self.logger.info("🦆 Registrando tabla en DuckDB...")
        conn = self.duckdb.connect()
        conn.execute("CREATE OR REPLACE TABLE empleados AS SELECT * FROM df_empleados")
        
        # Cargar query SQL
        self.logger.info("📜 Cargando query SQL...")
        ruta_query_flags = self._buscar_query('queries_flags_gold.sql')
        
        if not ruta_query_flags:
            raise FileNotFoundError("No se encontró queries_flags_gold.sql")
        
        with open(ruta_query_flags, 'r', encoding='utf-8') as f:
            query_flags = f.read()
        
        # Ejecutar query
        self.logger.info("⚙️  Aplicando flags de negocio...")
        result = conn.execute(query_flags).pl()
        
        conn.close()
        
        self.logger.info(f"  ✓ Flags aplicados: {len(result):,} registros")
        
        # Guardar resultado con timestamp en carpeta historico
        timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
        nombre_archivo = f"bd_empleados_gold_flags_{timestamp}.parquet"
        
        # Crear carpeta historico si no existe
        carpeta_historico = carpeta_trabajo / "gold" / "historico"
        carpeta_historico.mkdir(parents=True, exist_ok=True)
        
        ruta_output = carpeta_historico / nombre_archivo
        ruta_excel = carpeta_historico / f"bd_empleados_gold_flags_{timestamp}.xlsx"
        
        result.write_parquet(ruta_output, compression="snappy")
        self.logger.info(f"  ✓ Parquet: {ruta_output.name}")
        
        result.write_excel(ruta_excel)
        self.logger.info(f"  ✓ Excel: {ruta_excel.name}")
        self.logger.info(f"  📁 Ubicación: gold/historico/")
        
        self.timers['step3'] = time.time() - tiempo_inicio
        
        self.logger.info("-"*70)
        self.logger.info(f"✓ Step 3 completado")
        self.logger.info(f"  • Registros con flags: {len(result):,}")
        self.logger.info(f"  • Archivo: {nombre_archivo}")
        self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(self.timers['step3'])}")
        self.logger.info("-"*70)
        
        return {
            'registros': len(result),
            'archivo': nombre_archivo,
            'duracion': self.timers['step3']
        }
    
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