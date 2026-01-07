# ui/etls/nomina/worker.py
"""
Worker para ETL de Nómina
Ejecuta: Bronze → Silver → Gold
"""
from pathlib import Path
from typing import Dict, List
import sys

# Importar el worker base
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from ui.workers.base_worker import BaseETLWorker

# Importar funciones core de nómina
from nomina.api_step1 import consolidar_planillas_bronze_to_silver


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
            
            df_consolidado, ruta_parquet, ruta_excel = consolidar_planillas_bronze_to_silver(
                archivos_bronze=self.archivos,
                output_dir=self.output_dir,
                logger=self.logger
            )
            
            resultado['step1'] = {
                'dataframe': df_consolidado,
                'parquet': ruta_parquet,
                'excel': ruta_excel,
                'registros': len(df_consolidado)
            }
            
            self.logger.log_step_end("Consolidación", success=True)
            self.progress_updated.emit(50, f"✓ Consolidadas {len(df_consolidado):,} filas")
            
            # ============ STEP 2: Silver → Gold ============
            self.progress_updated.emit(60, "Procesando capa Gold...")
            self.logger.log_step_start("Exportación", "Silver → Gold")
            
            # Importar función de step2
            try:
                from nomina.step2_exportar import procesar_silver_to_gold
                
                resultado_gold = procesar_silver_to_gold(
                    archivo_silver=ruta_parquet,
                    output_dir=self.output_dir,
                    logger=self.logger
                )
                
                resultado['step2'] = resultado_gold
                
                self.logger.log_step_end("Exportación", success=True)
                self.progress_updated.emit(100, "✓ Proceso completado")
                
            except ImportError:
                # Si no existe step2, marcar como éxito parcial
                self.logger.warning("Step 2 no disponible, solo se ejecutó consolidación")
                self.progress_updated.emit(100, "✓ Consolidación completada (Step 2 no disponible)")
                resultado['step2'] = {'warning': 'Step 2 no implementado'}
            
            resultado['success'] = True
            resultado['mensaje'] = f"ETL completado: {len(df_consolidado):,} registros procesados"
            
            return resultado
            
        except Exception as e:
            self.logger.error(f"Error en ETL: {str(e)}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': str(e)
            }