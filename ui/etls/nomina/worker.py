# ui/etls/nomina/worker.py
"""
Worker para ETL de Nómina
Ejecuta: Bronze → Silver → Gold

ACTUALIZADO con:
- Lazy loading de módulos
- Timer de ejecución por fase
- Manejo robusto de errores
- Logs detallados de validaciones
"""
from pathlib import Path
from typing import Dict
import sys
import time

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ui.workers.base_worker import BaseETLWorker
from utils.lazy_loader import create_etl_loader


class NominaWorker(BaseETLWorker):
    """Worker para procesamiento de nóminas con lazy loading"""
    
    def __init__(self, archivos, output_dir):
        super().__init__(archivos, output_dir)
        
        # Configurar lazy loader para este ETL
        self.loader = create_etl_loader('nomina', {
            'step1': 'nomina.step1_consolidar_planillas',
            'step2': 'nomina.step2_exportar'
        })
        
        # Timers
        self.timers = {
            'total': 0,
            'step1': 0,
            'step2': 0
        }
    
    def get_pipeline_name(self) -> str:
        return "nomina"
    
    def _format_duration(self, seconds: float) -> str:
        """Formatea duración en hh:mm:ss.ms"""
        horas, resto = divmod(seconds, 3600)
        minutos, segundos_rest = divmod(resto, 60)
        return f"{int(horas):02d}:{int(minutos):02d}:{segundos_rest:05.2f}"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo de nómina:
        Step 1: Consolidar planillas (Bronze → Silver)
        Step 2: Exportar a Gold (Silver → Gold)
        
        Returns:
            dict con resultados del proceso
        """
        tiempo_inicio_total = time.time()
        
        try:
            resultado = {}
            
            # ============ STEP 1: Bronze → Silver ============
            self.logger.info("="*70)
            self.logger.info("STEP 1: CONSOLIDACIÓN (Bronze → Silver)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(5, "📥 Iniciando consolidación...")
            
            tiempo_inicio_step1 = time.time()
            
            try:
                self.logger.info(f"Archivos a procesar: {len(self.archivos)}")
                for idx, archivo in enumerate(self.archivos, 1):
                    self.logger.info(f"  {idx}. {archivo.name}")
                
                self.progress_updated.emit(10, "📥 Cargando módulo de consolidación...")
                
                # LAZY LOADING: step1 se carga AQUÍ, no al inicio
                consolidar_archivos = self.loader.step1.consolidar_archivos
                guardar_resultados = self.loader.step1.guardar_resultados
                
                self.logger.info("✓ Módulo step1 cargado exitosamente")
                self.progress_updated.emit(15, "🔄 Consolidando archivos...")
                
                # Ejecutar consolidación
                df_consolidado = consolidar_archivos(self.archivos, self.output_dir)
                
                self.progress_updated.emit(40, "💾 Guardando resultados en Silver...")
                
                # Guardar resultados
                ruta_parquet, ruta_excel = guardar_resultados(df_consolidado, self.output_dir)
                
                # Calcular tiempo step1
                self.timers['step1'] = time.time() - tiempo_inicio_step1
                
                resultado['step1'] = {
                    'dataframe': df_consolidado,
                    'parquet': ruta_parquet,
                    'excel': ruta_excel,
                    'registros': len(df_consolidado),
                    'columnas': len(df_consolidado.columns),
                    'duracion': self.timers['step1']
                }
                
                self.logger.info("-"*70)
                self.logger.info(f"✓ Step 1 completado exitosamente")
                self.logger.info(f"  • Registros: {len(df_consolidado):,}")
                self.logger.info(f"  • Columnas: {len(df_consolidado.columns)}")
                self.logger.info(f"  • Parquet: {ruta_parquet.name}")
                self.logger.info(f"  • Excel: {ruta_excel.name}")
                self.logger.info(f"  ⏱️  Duración: {self._format_duration(self.timers['step1'])}")
                self.logger.info("-"*70)
                
                self.progress_updated.emit(50, f"✓ Consolidadas {len(df_consolidado):,} filas")
                
            except ImportError as e:
                self.logger.error(f"❌ No se pudo importar step1: {e}")
                return {
                    'success': False,
                    'error': f'No se encontró nomina/step1_consolidar_planillas.py: {e}',
                    'timers': self.timers
                }
            except Exception as e:
                self.logger.error(f"❌ Error en Step 1: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                return {
                    'success': False,
                    'error': f'Error en consolidación: {str(e)}',
                    'timers': self.timers
                }
            
            # ============ STEP 2: Silver → Gold ============
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("STEP 2: TRANSFORMACIÓN (Silver → Gold)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(55, "🔍 Buscando esquema...")
            
            tiempo_inicio_step2 = time.time()
            
            try:
                # Buscar esquema
                esquema_path = Path(project_root) / "esquemas" / "esquema_nominas.json"
                
                if not esquema_path.exists():
                    self.logger.warning("⚠️  Esquema no encontrado, saltando Step 2")
                    self.logger.warning(f"   Ruta esperada: {esquema_path}")
                    self.progress_updated.emit(100, "✓ Consolidación completada (sin Gold)")
                    resultado['step2'] = {'warning': 'Esquema no encontrado'}
                else:
                    self.logger.info(f"✓ Esquema encontrado: {esquema_path.name}")
                    
                    # Cargar esquema
                    import json
                    with open(esquema_path, 'r', encoding='utf-8') as f:
                        esquema = json.load(f)
                    
                    self.logger.info(f"✓ Esquema cargado: v{esquema['metadata']['version']}")
                    self.logger.info(f"  • Columnas esperadas: {len(esquema['schema'])}")
                    
                    self.progress_updated.emit(60, "📊 Cargando datos Silver...")
                    
                    # Leer datos silver
                    import polars as pl
                    df_silver = pl.read_parquet(ruta_parquet)
                    
                    self.logger.info(f"✓ Datos silver cargados: {len(df_silver):,} registros")
                    
                    self.progress_updated.emit(65, "⚙️  Cargando módulo de transformación...")
                    
                    # LAZY LOADING: step2 se carga AQUÍ
                    seleccionar_y_convertir_columnas = self.loader.step2.seleccionar_y_convertir_columnas
                    guardar_gold = self.loader.step2.guardar_resultados
                    
                    self.logger.info("✓ Módulo step2 cargado exitosamente")
                    
                    self.progress_updated.emit(70, "🔄 Aplicando transformaciones Gold...")
                    
                    # Transformar a gold (aplica transformaciones + NOMBRE_MES + validaciones)
                    df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)
                    
                    self.logger.info(f"✓ Transformaciones aplicadas")
                    self.logger.info(f"  • Registros finales: {len(df_gold):,}")
                    self.logger.info(f"  • Columnas finales: {len(df_gold.columns)}")
                    
                    self.progress_updated.emit(85, "💾 Guardando archivos Gold...")
                    
                    # Guardar gold (con versionamiento automático)
                    carpeta_silver = ruta_parquet.parent
                    rutas_gold = guardar_gold(df_gold, carpeta_silver)
                    
                    # Calcular tiempo step2
                    self.timers['step2'] = time.time() - tiempo_inicio_step2
                    
                    resultado['step2'] = {
                        'registros': len(df_gold),
                        'columnas': len(df_gold.columns),
                        'parquet': rutas_gold['parquet'],
                        'excel': rutas_gold['excel'],
                        'carpeta_actual': rutas_gold['carpeta_actual'],
                        'carpeta_historico': rutas_gold['carpeta_historico'],
                        'duracion': self.timers['step2']
                    }
                    
                    self.logger.info("-"*70)
                    self.logger.info(f"✓ Step 2 completado exitosamente")
                    self.logger.info(f"  • Registros Gold: {len(df_gold):,}")
                    self.logger.info(f"  • Columnas Gold: {len(df_gold.columns)}")
                    self.logger.info(f"  • Parquet: {rutas_gold['parquet'].name}")
                    self.logger.info(f"  • Excel: {rutas_gold['excel'].name}")
                    self.logger.info(f"  ⏱️  Duración: {self._format_duration(self.timers['step2'])}")
                    self.logger.info("-"*70)
                    
                    self.progress_updated.emit(100, f"✓ Gold generado: {len(df_gold):,} registros")
                
            except ImportError as e:
                self.logger.warning(f"⚠️  Step 2 no disponible: {e}")
                self.progress_updated.emit(100, "✓ Consolidación completada (Step 2 no disponible)")
                resultado['step2'] = {'warning': f'Step 2 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"❌ Error en Step 2: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                resultado['step2'] = {'error': str(e)}
                # No retornar error aquí, silver ya fue generado exitosamente
            
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
            if 'step2' in resultado and 'registros' in resultado['step2']:
                mensaje = (
                    f"ETL completado exitosamente:\n"
                    f"  • Silver: {resultado['step1']['registros']:,} registros, "
                    f"{resultado['step1']['columnas']} columnas\n"
                    f"  • Gold: {resultado['step2']['registros']:,} registros, "
                    f"{resultado['step2']['columnas']} columnas\n"
                    f"  ⏱️  Tiempo total: {self._format_duration(self.timers['total'])}\n"
                    f"    - Step 1 (Bronze→Silver): {self._format_duration(self.timers['step1'])}\n"
                    f"    - Step 2 (Silver→Gold): {self._format_duration(self.timers['step2'])}"
                )
            else:
                mensaje = (
                    f"Consolidación completada:\n"
                    f"  • Silver: {resultado['step1']['registros']:,} registros\n"
                    f"  ⏱️  Tiempo total: {self._format_duration(self.timers['total'])}"
                )
            
            resultado['mensaje'] = mensaje
            self.logger.info(mensaje)
            self.logger.info("="*70)
            
            # Verificar qué módulos fueron cargados
            modulos_cargados = self.loader.get_loaded_modules()
            self.logger.info(f"\n📦 Módulos cargados: {', '.join(modulos_cargados)}")
            
            return resultado
            
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