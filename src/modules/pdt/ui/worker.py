# ui/etls/pdt/worker.py
"""
Worker para ETL de PDT - Relación de Ingresos
Ejecuta: Bronze → Silver → Gold

Características especiales:
- Procesa UN ARCHIVO con 2 hojas (EMPLEADOS y PRACTICANTES)
- Step 1: Genera 2 archivos en Silver
- Step 2: EMPLEADOS va a Gold
- Step 3: PRACTICANTES va a Gold

Implementa:
- Lazy loading de módulos
- Timer de ejecución por fase
- Manejo robusto de errores
- Logs detallados de validaciones
"""
from pathlib import Path
from typing import Dict
import sys
import time
from src.utils.paths import get_resource_path
from src.utils.validate_source import SourceValidationError, validate_all_sources_for_etl

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))

from src.utils.ui.workers.base_worker import BaseETLWorker
from src.utils.lazy_loader import create_etl_loader


class PDTWorker(BaseETLWorker):
    """Worker para procesamiento de Relación de Ingresos (PDT) con lazy loading"""
    
    def __init__(self, archivos, output_dir):
        super().__init__(archivos, output_dir)
        
        # Configurar lazy loader para este ETL
        self.loader = create_etl_loader('pdt', {
            'step1': 'src.modules.pdt.steps.step1_consolidar_ingresos',
            'step2': 'src.modules.pdt.steps.step2_exportar_ingresos',
            'step3': 'src.modules.pdt.steps.step3_exportar_practicantes'
        })
        
        # Timers
        self.timers = {
            'total': 0,
            'step1': 0,
            'step2': 0,
            'step3': 0
        }
    
    def get_pipeline_name(self) -> str:
        return "pdt"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo de PDT - Relación de Ingresos:
        Step 1: Procesar Excel (Bronze → Silver) - genera EMPLEADOS y PRACTICANTES
        Step 2: Transformar EMPLEADOS a Gold (Silver → Gold)
        Step 3: Transformar PRACTICANTES a Gold (Silver → Gold)
        
        Returns:
            dict con resultados del proceso
        """
        tiempo_inicio_total = time.time()
        
        try:
            resultado = {}
            
            # Validar que solo haya un archivo
            if len(self.archivos) != 1:
                return self.build_error_result(
                    stage_name='Validación de entrada',
                    error=f'Se esperaba 1 archivo, se recibieron {len(self.archivos)}',
                    timers=self.timers
                )
            
            archivo_bronze = self.archivos[0]

            # Preflight / Validate Source (antes de cualquier stage)
            self.progress_updated.emit(2, "🔎 Preflight: validando archivo fuente...")
            self.logger.info("🔎 PRE-FLIGHT: validando contrato de fuente...")
            preflight = validate_all_sources_for_etl("ingresos", archivo_bronze)
            preflight.raise_if_failed()
            self.logger.info(f"✓ Preflight válido ({archivo_bronze.name})")
            self.progress_updated.emit(4, "✓ Preflight completado")
            
            # ============ STEP 1: Bronze → Silver ============
            self.logger.info("="*70)
            self.logger.info("STEP 1: PROCESAMIENTO EXCEL (Bronze → Silver)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(5, "🔥 Iniciando procesamiento...")
            
            tiempo_inicio_step1 = time.time()
            
            try:
                self.logger.info(f"Archivo a procesar: {archivo_bronze.name}")
                self.logger.info(f"  📄 Hojas esperadas: EMPLEADOS, PRACTICANTES")
                
                self.progress_updated.emit(10, "🔥 Cargando módulo de procesamiento...")
                
                # LAZY LOADING: step1 se carga AQUÍ
                leer_hoja_excel = self.loader.step1.leer_hoja_excel
                limpiar_datos = self.loader.step1.limpiar_datos
                generar_reporte_calidad = self.loader.step1.generar_reporte_calidad
                guardar_resultados = self.loader.step1.guardar_resultados
                CONFIGURACION_HOJAS = self.loader.step1.CONFIGURACION_HOJAS
                
                self.logger.info("✓ Módulo step1 cargado exitosamente")
                
                # Procesar cada hoja
                resultados_hojas = {}
                total_hojas = len(CONFIGURACION_HOJAS)
                
                for idx, (nombre_hoja, config) in enumerate(CONFIGURACION_HOJAS.items(), 1):
                    progreso_base = 15 + (idx - 1) * 15
                    self.progress_updated.emit(
                        progreso_base, 
                        f"📄 Procesando hoja {idx}/{total_hojas}: {nombre_hoja}..."
                    )
                    
                    self.logger.info("")
                    self.logger.info(f"[{idx}/{total_hojas}] PROCESANDO HOJA: {nombre_hoja}")
                    self.logger.info("-"*70)
                    
                    try:
                        # Leer datos
                        df_original = leer_hoja_excel(archivo_bronze, nombre_hoja, config)
                        
                        if df_original.is_empty():
                            self.logger.warning(f"⚠️ No se encontraron datos en {nombre_hoja}")
                            continue
                        
                        # Limpiar datos
                        df_limpio = limpiar_datos(df_original, nombre_hoja)
                        
                        # Generar reporte de calidad
                        generar_reporte_calidad(df_original, df_limpio, nombre_hoja)
                        
                        # Guardar para escritura posterior
                        resultados_hojas[nombre_hoja] = {
                            "df": df_limpio,
                            "registros": df_limpio.height
                        }
                        
                        self.logger.info(f"✓ Hoja {nombre_hoja} procesada: {df_limpio.height:,} registros")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Error procesando {nombre_hoja}: {e}")
                        continue
                
                if not resultados_hojas:
                    return self.build_error_result(
                        stage_name='Step 1: Bronze → Silver',
                        error='No se procesaron datos de ninguna hoja',
                        timers=self.timers,
                        stage_index=1,
                        total_stages=3
                    )
                
                self.progress_updated.emit(40, "💾 Guardando resultados en Silver...")
                
                # Guardar archivos en Silver
                rutas_guardadas = guardar_resultados(resultados_hojas, self.output_dir)
                
                # Calcular tiempo step1
                self.timers['step1'] = time.time() - tiempo_inicio_step1
                
                # Encontrar rutas de archivos Silver
                ruta_empleados_silver = rutas_guardadas.get('EMPLEADOS', {}).get('parquet')
                ruta_practicantes_silver = rutas_guardadas.get('PRACTICANTES', {}).get('parquet')
                
                resultado['step1'] = {
                    'hojas_procesadas': list(resultados_hojas.keys()),
                    'registros_por_hoja': {
                        hoja: datos['registros'] 
                        for hoja, datos in resultados_hojas.items()
                    },
                    'rutas': rutas_guardadas,
                    'ruta_empleados_silver': ruta_empleados_silver,
                    'ruta_practicantes_silver': ruta_practicantes_silver,
                    'duracion': self.timers['step1']
                }
                
                self.logger.info("-"*70)
                self.logger.info("✓ Step 1 completado exitosamente")
                self.logger.info(f"  • Hojas procesadas: {len(resultados_hojas)}")
                for hoja, datos in resultados_hojas.items():
                    self.logger.info(f"    - {hoja}: {datos['registros']:,} registros")
                self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step1'])}")
                self.logger.info("-"*70)
                
                self.progress_updated.emit(
                    45, 
                    f"✓ Silver generado: {sum(d['registros'] for d in resultados_hojas.values()):,} registros totales"
                )
            
            except Exception as e:
                self.logger.error(f"❌ Error crítico en Step 1: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())

                return self.build_error_result(
                    stage_name='Step 1: Bronze → Silver',
                    error=f'Error en Step 1: {str(e)}',
                    timers=self.timers,
                    stage_index=1,
                    total_stages=3,
                    module_path='src.modules.pdt.steps.step1_consolidar_ingresos'
                )
            
            # ============ STEP 2: Silver → Gold (EMPLEADOS) ============
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("STEP 2: TRANSFORMACIÓN GOLD (Silver → Gold) - EMPLEADOS")
            self.logger.info("="*70)
            
            self.progress_updated.emit(50, "🔄 Iniciando transformación Gold - EMPLEADOS...")
            
            tiempo_inicio_step2 = time.time()
            
            try:
                # Buscar y cargar esquema
                from src.utils.paths import get_resource_path
                import json
                
                esquema_path = get_resource_path("assets/esquemas/esquema_relacion_ingresos.json")
                
                self.logger.info(f"Buscando esquema en: {esquema_path}")
                
                if not esquema_path.exists():
                    raise FileNotFoundError(f"No se encontró esquema en: {esquema_path}")
                
                self.progress_updated.emit(55, "📋 Cargando esquema EMPLEADOS...")
                
                with open(esquema_path, 'r', encoding='utf-8') as f:
                    esquema_completo = json.load(f)
                
                if 'hojas' not in esquema_completo or 'EMPLEADOS' not in esquema_completo['hojas']:
                    raise ValueError("Esquema no contiene configuración para EMPLEADOS")
                
                esquema = esquema_completo['hojas']['EMPLEADOS']
                
                self.logger.info(f"✓ Esquema cargado: v{esquema_completo['metadata']['version']}")
                self.logger.info(f"  • Columnas esperadas para EMPLEADOS: {len(esquema['schema'])}")
                
                self.progress_updated.emit(60, "📊 Cargando datos Silver - EMPLEADOS...")
                
                # Verificar que tenemos la ruta de EMPLEADOS
                if not ruta_empleados_silver or not ruta_empleados_silver.exists():
                    raise FileNotFoundError("No se encontró el archivo Silver de EMPLEADOS")
                
                # Leer datos silver
                import polars as pl
                df_silver = pl.read_parquet(ruta_empleados_silver)
                
                self.logger.info(f"✓ Datos silver cargados: {len(df_silver):,} registros")
                
                self.progress_updated.emit(65, "⚙️ Cargando módulo de transformación...")
                
                # LAZY LOADING: step2 se carga AQUÍ
                seleccionar_y_convertir_columnas = self.loader.step2.seleccionar_y_convertir_columnas
                guardar_resultados_gold = self.loader.step2.guardar_resultados
                
                self.logger.info("✓ Módulo step2 cargado exitosamente")
                
                self.progress_updated.emit(70, "🔄 Aplicando transformaciones Gold...")
                
                # Transformar a gold
                df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)
                
                self.progress_updated.emit(75, "🔄 Agregando columnas enriquecidas...")
                
                # Agregar columna enriquecida NOMBRE_MES
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
                
                self.logger.info(f"✓ Transformaciones aplicadas")
                self.logger.info(f"  • Columna NOMBRE_MES agregada después de MES")
                self.logger.info(f"  • Registros finales: {len(df_gold):,}")
                self.logger.info(f"  • Columnas finales: {len(df_gold.columns)}")
                
                self.progress_updated.emit(78, "💾 Guardando archivos Gold - EMPLEADOS...")
                
                # Guardar gold (con versionamiento automático)
                carpeta_silver = ruta_empleados_silver.parent
                ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados_gold(
                    df_gold, 
                    carpeta_silver
                )
                
                # Calcular tiempo step2
                self.timers['step2'] = time.time() - tiempo_inicio_step2
                
                resultado['step2'] = {
                    'registros': len(df_gold),
                    'columnas': len(df_gold.columns),
                    'parquet_actual': ruta_parquet_actual,
                    'excel_actual': ruta_excel_actual,
                    'parquet_historico': ruta_parquet_historico,
                    'excel_historico': ruta_excel_historico,
                    'duracion': self.timers['step2']
                }
                
                self.logger.info("-"*70)
                self.logger.info(f"✓ Step 2 completado exitosamente")
                self.logger.info(f"  • Registros Gold: {len(df_gold):,}")
                self.logger.info(f"  • Columnas Gold: {len(df_gold.columns)}")
                self.logger.info(f"  • Parquet (actual): {ruta_parquet_actual.name}")
                self.logger.info(f"  • Excel (actual): {ruta_excel_actual.name}")
                self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step2'])}")
                self.logger.info("-"*70)
                
                self.progress_updated.emit(80, f"✓ EMPLEADOS Gold generado: {len(df_gold):,} registros")
            
            except ImportError as e:
                self.logger.warning(f"⚠️ Step 2 no disponible: {e}")
                self.progress_updated.emit(80, "⚠️ Step 2 (EMPLEADOS) no disponible")
                resultado['step2'] = {'warning': f'Step 2 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"❌ Error en Step 2: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                resultado['step2'] = {
                    'error': str(e),
                    'error_details': self.build_error_details(
                        stage_name='Step 2: Silver → Gold (EMPLEADOS)',
                        error=e,
                        stage_index=2,
                        total_stages=3,
                        module_path='src.modules.pdt.steps.step2_exportar_ingresos'
                    )
                }
                # No retornar error aquí, silver ya fue generado exitosamente
            
            # ============ STEP 3: Silver → Gold (PRACTICANTES) ============
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("STEP 3: TRANSFORMACIÓN GOLD (Silver → Gold) - PRACTICANTES")
            self.logger.info("="*70)
            
            self.progress_updated.emit(82, "🔄 Iniciando transformación Gold - PRACTICANTES...")
            
            tiempo_inicio_step3 = time.time()
            
            try:
                # Buscar y cargar esquema
                from src.utils.paths import get_resource_path
                import json
                
                esquema_path = get_resource_path("assets/esquemas/esquema_ingresos_practicantes.json")
                
                self.logger.info(f"Buscando esquema en: {esquema_path}")
                
                if not esquema_path.exists():
                    raise FileNotFoundError(f"No se encontró esquema en: {esquema_path}")
                
                self.progress_updated.emit(84, "📋 Cargando esquema PRACTICANTES...")
                
                with open(esquema_path, 'r', encoding='utf-8') as f:
                    esquema_completo = json.load(f)
                
                if 'hojas' not in esquema_completo or 'PRACTICANTES' not in esquema_completo['hojas']:
                    raise ValueError("Esquema no contiene configuración para PRACTICANTES")
                
                esquema = esquema_completo['hojas']['PRACTICANTES']
                
                self.logger.info(f"✓ Esquema cargado: v{esquema_completo['metadata']['version']}")
                self.logger.info(f"  • Columnas esperadas para PRACTICANTES: {len(esquema['schema'])}")
                
                self.progress_updated.emit(86, "📊 Cargando datos Silver - PRACTICANTES...")
                
                # Verificar que tenemos la ruta de PRACTICANTES
                if not ruta_practicantes_silver or not ruta_practicantes_silver.exists():
                    raise FileNotFoundError("No se encontró el archivo Silver de PRACTICANTES")
                
                # Leer datos silver
                import polars as pl
                df_silver = pl.read_parquet(ruta_practicantes_silver)
                
                self.logger.info(f"✓ Datos silver cargados: {len(df_silver):,} registros")
                
                self.progress_updated.emit(88, "⚙️ Cargando módulo de transformación...")
                
                # LAZY LOADING: step3 se carga AQUÍ
                seleccionar_y_convertir_columnas_prac = self.loader.step3.seleccionar_y_convertir_columnas
                aplicar_business_rules = self.loader.step3.aplicar_business_rules
                guardar_resultados_gold_prac = self.loader.step3.guardar_resultados
                
                self.logger.info("✓ Módulo step3 cargado exitosamente")
                
                self.progress_updated.emit(90, "🔄 Aplicando transformaciones Gold...")
                
                # Transformar a gold
                df_gold = seleccionar_y_convertir_columnas_prac(df_silver, esquema)
                
                # Aplicar business rules
                df_gold = aplicar_business_rules(df_gold)
                
                self.progress_updated.emit(92, "🔄 Agregando columnas enriquecidas...")
                
                # Agregar columna enriquecida NOMBRE_MES
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
                
                self.logger.info(f"✓ Transformaciones aplicadas")
                self.logger.info(f"  • Business rules aplicadas (Universidad de Procedencia)")
                self.logger.info(f"  • Columna NOMBRE_MES agregada después de MES")
                self.logger.info(f"  • Registros finales: {len(df_gold):,}")
                self.logger.info(f"  • Columnas finales: {len(df_gold.columns)}")
                
                self.progress_updated.emit(95, "💾 Guardando archivos Gold - PRACTICANTES...")
                
                # Guardar gold (con versionamiento automático)
                carpeta_silver = ruta_practicantes_silver.parent
                ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados_gold_prac(
                    df_gold, 
                    carpeta_silver
                )
                
                # Calcular tiempo step3
                self.timers['step3'] = time.time() - tiempo_inicio_step3
                
                resultado['step3'] = {
                    'registros': len(df_gold),
                    'columnas': len(df_gold.columns),
                    'parquet_actual': ruta_parquet_actual,
                    'excel_actual': ruta_excel_actual,
                    'parquet_historico': ruta_parquet_historico,
                    'excel_historico': ruta_excel_historico,
                    'duracion': self.timers['step3']
                }
                
                self.logger.info("-"*70)
                self.logger.info(f"✓ Step 3 completado exitosamente")
                self.logger.info(f"  • Registros Gold: {len(df_gold):,}")
                self.logger.info(f"  • Columnas Gold: {len(df_gold.columns)}")
                self.logger.info(f"  • Parquet (actual): {ruta_parquet_actual.name}")
                self.logger.info(f"  • Excel (actual): {ruta_excel_actual.name}")
                self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step3'])}")
                self.logger.info("-"*70)
                
                self.progress_updated.emit(98, f"✓ PRACTICANTES Gold generado: {len(df_gold):,} registros")
            
            except ImportError as e:
                self.logger.warning(f"⚠️ Step 3 no disponible: {e}")
                self.progress_updated.emit(98, "⚠️ Step 3 (PRACTICANTES) no disponible")
                resultado['step3'] = {'warning': f'Step 3 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"❌ Error en Step 3: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                resultado['step3'] = {
                    'error': str(e),
                    'error_details': self.build_error_details(
                        stage_name='Step 3: Silver → Gold (PRACTICANTES)',
                        error=e,
                        stage_index=3,
                        total_stages=3,
                        module_path='src.modules.pdt.steps.step3_exportar_practicantes'
                    )
                }
                # No retornar error aquí, steps previos ya fueron generados exitosamente
            
            # ============ RESULTADO FINAL ============
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            resultado['success'] = True
            resultado['timers'] = self.timers
            
            # Log resumen final
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("RESUMEN FINAL")
            self.logger.info("="*70)
            
            # Mensaje resumen
            hojas_procesadas = resultado['step1']['hojas_procesadas']
            registros_por_hoja = resultado['step1']['registros_por_hoja']
            
            # Construir mensaje con información de todos los steps
            lineas_mensaje = [
                "ETL completado exitosamente:",
                f"  • Hojas procesadas: {', '.join(hojas_procesadas)}",
                f"  • EMPLEADOS (Silver): {registros_por_hoja.get('EMPLEADOS', 0):,} registros",
                f"  • PRACTICANTES (Silver): {registros_por_hoja.get('PRACTICANTES', 0):,} registros",
            ]
            
            if 'step2' in resultado and 'registros' in resultado['step2']:
                lineas_mensaje.append(f"  • EMPLEADOS (Gold): {resultado['step2']['registros']:,} registros")
            
            if 'step3' in resultado and 'registros' in resultado['step3']:
                lineas_mensaje.append(f"  • PRACTICANTES (Gold): {resultado['step3']['registros']:,} registros")
            
            lineas_mensaje.append(f"  ⏱️ Tiempo total: {self.logger.format_duration(self.timers['total'])}")
            lineas_mensaje.append(f"    - Step 1 (Bronze→Silver): {self.logger.format_duration(self.timers['step1'])}")
            
            if self.timers['step2'] > 0:
                lineas_mensaje.append(f"    - Step 2 (EMPLEADOS→Gold): {self.logger.format_duration(self.timers['step2'])}")
            
            if self.timers['step3'] > 0:
                lineas_mensaje.append(f"    - Step 3 (PRACTICANTES→Gold): {self.logger.format_duration(self.timers['step3'])}")
            
            mensaje = "\n".join(lineas_mensaje)
            
            resultado['mensaje'] = mensaje
            self.logger.info(mensaje)
            self.logger.info("="*70)
            
            # Verificar qué módulos fueron cargados
            modulos_cargados = self.loader.get_loaded_modules()
            self.logger.info(f"\n📦 Módulos cargados: {', '.join(modulos_cargados)}")
            
            self.progress_updated.emit(100, "✓ Procesamiento completo")
            
            return resultado
            
        except SourceValidationError as e:
            self.logger.error(str(e))
            self.timers['total'] = time.time() - tiempo_inicio_total

            return self.build_error_result(
                stage_name='Preflight / Validate Source',
                error=str(e),
                timers=self.timers,
                stage_index=1,
                total_stages=3,
                module_path='src.utils.validate_source',
                function_name='validate_all_sources_for_etl'
            )

        except Exception as e:
            self.logger.error(f"❌ Error crítico en ETL: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            self.timers['total'] = time.time() - tiempo_inicio_total

            return self.build_error_result(
                stage_name='ETL completo PDT',
                error=str(e),
                timers=self.timers
            )
