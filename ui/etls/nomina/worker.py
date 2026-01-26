# ui/etls/nomina/worker.py
"""
Worker para ETL de Nómina con Licencias
Ejecuta pipeline completo: Bronze → Silver → Gold → Gold Enriquecido
Llama a pipeline_nomina_executor.py para orquestar los 4 stages
"""
from pathlib import Path
from typing import Dict
import sys
import time

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from ui.workers.base_worker import BaseETLWorker
from orquestadores.pipeline_nomina_executor import PipelineNominaExecutor
from utils.paths import get_resource_path


class NominaWorker(BaseETLWorker):
    """Worker para procesamiento de nóminas con pipeline completo"""
    
    def __init__(self, archivos, output_dir):
        super().__init__(archivos, output_dir)
        self.pipeline_executor = None
        
        # Timers
        self.timers = {
            'total': 0
        }
    
    def get_pipeline_name(self) -> str:
        return "nomina_pipeline"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el pipeline completo de nómina con licencias
        usando el executor basado en YAML
        
        Returns:
            dict con resultados del proceso
        """
        tiempo_inicio_total = time.time()
        
        try:
            # Obtener ruta del YAML del pipeline
            yaml_path = get_resource_path("orquestadores/pipeline_nomina_licencias.yaml")
            
            if not yaml_path.exists():
                self.logger.error(f"❌ No se encontró el archivo YAML del pipeline: {yaml_path}")
                return {
                    'success': False,
                    'error': f'Archivo pipeline YAML no encontrado: {yaml_path}',
                    'timers': self.timers
                }
            
            self.logger.info("=" * 70)
            self.logger.info("PIPELINE NÓMINA + LICENCIAS")
            self.logger.info("=" * 70)
            self.logger.info(f"YAML: {yaml_path.name}")
            self.logger.info(f"Archivos de planilla: {len(self.archivos)}")
            self.logger.info(f"Directorio de trabajo: {self.output_dir}")
            self.logger.info("=" * 70)
            
            # Crear executor del pipeline
            self.pipeline_executor = PipelineNominaExecutor(
                yaml_path=yaml_path,
                archivos=self.archivos,
                output_dir=self.output_dir
            )
            
            # Conectar señales del executor con las del worker
            self.pipeline_executor.log_message.connect(self._on_executor_log)
            self.pipeline_executor.progress_update.connect(self._on_executor_progress)
            self.pipeline_executor.stage_started.connect(self._on_stage_started)
            self.pipeline_executor.stage_completed.connect(self._on_stage_completed)
            
            # Ejecutar pipeline
            self.logger.info("")
            self.logger.info("🚀 Iniciando ejecución del pipeline...")
            self.logger.info("")
            
            resultado = self.pipeline_executor.execute()
            
            # Calcular tiempo total
            self.timers['total'] = time.time() - tiempo_inicio_total
            resultado['timers'] = self.timers
            
            if resultado['success']:
                # Log resumen final
                self.logger.info("")
                self.logger.info("=" * 70)
                self.logger.info("RESUMEN FINAL")
                self.logger.info("=" * 70)
                
                stages_completados = resultado.get('completed_stages', 0)
                duracion = resultado.get('duracion_total', self.timers['total'])
                
                mensaje = (
                    f"✓ Pipeline completado exitosamente\n"
                    f"  • Stages ejecutados: {stages_completados}/4\n"
                    f"  • Archivos procesados: {len(self.archivos)}\n"
                    f"  ⏱️  Tiempo total: {self.logger.format_duration(duracion)}\n"
                    f"\n📊 Outputs generados:\n"
                    f"  • Silver: Planilla Metso Consolidado.parquet\n"
                    f"  • Silver: licencias_consolidadas.parquet\n"
                    f"  • Gold: Planilla_Metso_Consolidado.parquet\n"
                    f"  • Gold: Planilla Metso BI_Gold_Con_Licencias.parquet\n"
                    f"  • Gold: Planilla Metso BI_Gold_Con_Licencias.xlsx"
                )
                
                resultado['mensaje'] = mensaje
                self.logger.info(mensaje)
                self.logger.info("=" * 70)
                
                self.progress_updated.emit(100, "✓ Pipeline completado")
            else:
                error_msg = resultado.get('error', 'Error desconocido')
                self.logger.error(f"❌ Pipeline falló: {error_msg}")
                
                self.progress_updated.emit(0, f"❌ Error: {error_msg}")
            
            return resultado
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en pipeline: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            return {
                'success': False,
                'error': str(e),
                'timers': self.timers
            }
    
    def _on_executor_log(self, nivel: str, mensaje: str):
        """
        Callback cuando el executor emite un log
        Reenvía al logger del worker
        """
        log_method = getattr(self.logger, nivel.lower(), self.logger.info)
        log_method(mensaje)
    
    def _on_executor_progress(self, porcentaje: int, mensaje: str):
        """
        Callback cuando el executor emite progreso
        Reenvía como señal del worker
        """
        self.progress_updated.emit(porcentaje, mensaje)
    
    def _on_stage_started(self, stage_name: str, descripcion: str):
        """
        Callback cuando inicia un stage
        """
        self.logger.info("")
        self.logger.info(f"🚀 Iniciando: {stage_name}")
        if descripcion:
            self.logger.info(f"   {descripcion}")
    
    def _on_stage_completed(self, stage_name: str, exito: bool, duracion: float):
        """
        Callback cuando termina un stage
        """
        if exito:
            self.logger.info(f"✓ {stage_name} completado")
            self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(duracion)}")
        else:
            self.logger.error(f"❌ {stage_name} falló")