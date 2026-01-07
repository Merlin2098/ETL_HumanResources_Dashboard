# ui/etls/nomina/worker.py
"""
Worker para ETL de Nómina
Ejecuta: Bronze → Silver → Gold
"""
from pathlib import Path
from typing import Dict, List
import sys

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ui.workers.base_worker import BaseETLWorker


class NominaWorker(BaseETLWorker):
    """Worker para procesamiento de nóminas"""
    
    def get_pipeline_name(self) -> str:
        return "nomina"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo de nómina:
        Step 1: Consolidar planillas (Bronze → Silver)
        Step 2: Exportar a Gold (Silver → Gold)
        
        Returns:
            dict con resultados del proceso
        """
        try:
            resultado = {}
            
            # ============ STEP 1: Bronze → Silver ============
            self.progress_updated.emit(10, "Consolidando planillas...")
            self.logger.log_step_start("Consolidación", "Bronze → Silver")
            
            # Importar step1
            try:
                from nomina.step1_consolidar_planillas import consolidar_archivos, guardar_resultados
                
                # Ejecutar consolidación
                df_consolidado = consolidar_archivos(self.archivos, self.output_dir)
                
                # Guardar resultados
                ruta_parquet, ruta_excel = guardar_resultados(df_consolidado, self.output_dir)
                
                resultado['step1'] = {
                    'dataframe': df_consolidado,
                    'parquet': ruta_parquet,
                    'excel': ruta_excel,
                    'registros': len(df_consolidado)
                }
                
                self.logger.log_step_end("Consolidación", success=True)
                self.progress_updated.emit(50, f"✓ Consolidadas {len(df_consolidado):,} filas")
                
            except ImportError as e:
                self.logger.error(f"No se pudo importar step1: {e}")
                return {
                    'success': False,
                    'error': f'No se encontró nomina/step1_consolidar_planillas.py: {e}'
                }
            
            # ============ STEP 2: Silver → Gold ============
            self.progress_updated.emit(60, "Procesando capa Gold...")
            self.logger.log_step_start("Exportación", "Silver → Gold")
            
            try:
                from nomina.step2_exportar import seleccionar_y_convertir_columnas, guardar_resultados as guardar_gold
                
                # Buscar esquema
                from pathlib import Path
                esquema_path = Path(project_root) / "esquemas" / "esquema_nominas.json"
                
                if not esquema_path.exists():
                    self.logger.warning("Esquema no encontrado, saltando Step 2")
                    self.progress_updated.emit(100, "✓ Consolidación completada (sin Gold)")
                    resultado['step2'] = {'warning': 'Esquema no encontrado'}
                else:
                    import json
                    with open(esquema_path, 'r', encoding='utf-8') as f:
                        esquema = json.load(f)
                    
                    # Leer silver
                    import polars as pl
                    df_silver = pl.read_parquet(ruta_parquet)
                    
                    # Transformar a gold
                    df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)
                    
                    # Guardar gold
                    carpeta_silver = ruta_parquet.parent
                    rutas_gold = guardar_gold(df_gold, carpeta_silver)
                    
                    resultado['step2'] = {
                        'registros': len(df_gold),
                        'archivos': rutas_gold
                    }
                    
                    self.logger.log_step_end("Exportación", success=True)
                    self.progress_updated.emit(100, "✓ Proceso completado")
                
            except ImportError as e:
                self.logger.warning(f"Step 2 no disponible: {e}")
                self.progress_updated.emit(100, "✓ Consolidación completada (Step 2 no disponible)")
                resultado['step2'] = {'warning': f'Step 2 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"Error en Step 2: {e}")
                resultado['step2'] = {'error': str(e)}
            
            resultado['success'] = True
            mensaje = f"ETL completado: {resultado['step1']['registros']:,} registros procesados"
            resultado['mensaje'] = mensaje
            
            return resultado
            
        except Exception as e:
            self.logger.error(f"Error en ETL: {str(e)}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': str(e)
            }