"""
Sistema de lazy loading para módulos ETL
Proporciona carga dinámica de módulos con cache, logging y manejo de errores
"""
import importlib
import sys
from pathlib import Path
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime
import time
from functools import wraps

from .logger import PipelineLogger


class LazyLoader:
    """
    Gestor de carga lazy de módulos con cache y logging
    """
    
    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
        enable_cache: bool = True,
        measure_performance: bool = True
    ):
        """
        Inicializa el lazy loader
        
        Args:
            logger: Logger para registrar operaciones de carga
            enable_cache: Habilitar cache de módulos cargados
            measure_performance: Medir tiempo de carga de módulos
        """
        self.logger = logger
        self.enable_cache = enable_cache
        self.measure_performance = measure_performance
        
        # Cache de módulos y funciones cargadas
        self._module_cache: Dict[str, Any] = {}
        self._function_cache: Dict[str, Callable] = {}
        
        # Métricas de performance
        self._load_times: Dict[str, float] = {}
        self._load_count: Dict[str, int] = {}
    
    def import_module(
        self,
        module_path: str,
        reload: bool = False
    ) -> Optional[Any]:
        """
        Importa un módulo de forma lazy
        
        Args:
            module_path: Ruta del módulo (ej: 'nomina.step1_consolidar_planillas')
            reload: Forzar recarga del módulo aunque esté en cache
        
        Returns:
            Módulo importado o None si falla
        """
        # Verificar cache
        if not reload and self.enable_cache and module_path in self._module_cache:
            if self.logger:
                self.logger.debug(f"📦 Módulo '{module_path}' cargado desde cache")
            self._load_count[module_path] = self._load_count.get(module_path, 0) + 1
            return self._module_cache[module_path]
        
        # Medir tiempo de carga
        start_time = time.time() if self.measure_performance else None
        
        try:
            if self.logger:
                self.logger.debug(f"⏳ Importando módulo: {module_path}")
            
            # Importar módulo
            module = importlib.import_module(module_path)
            
            # Calcular tiempo
            if start_time:
                load_time = time.time() - start_time
                self._load_times[module_path] = load_time
                
                if self.logger:
                    self.logger.debug(f"✓ Módulo '{module_path}' cargado en {load_time*1000:.1f}ms")
            
            # Guardar en cache
            if self.enable_cache:
                self._module_cache[module_path] = module
            
            # Actualizar contador
            self._load_count[module_path] = self._load_count.get(module_path, 0) + 1
            
            return module
            
        except ImportError as e:
            if self.logger:
                self.logger.error(f"✗ Error al importar módulo '{module_path}': {e}")
            return None
        except Exception as e:
            if self.logger:
                self.logger.error(f"✗ Error inesperado al importar '{module_path}': {e}")
            return None
    
    def import_function(
        self,
        module_path: str,
        function_name: str,
        reload: bool = False
    ) -> Optional[Callable]:
        """
        Importa una función específica de un módulo
        
        Args:
            module_path: Ruta del módulo
            function_name: Nombre de la función a importar
            reload: Forzar recarga
        
        Returns:
            Función importada o None si falla
        """
        cache_key = f"{module_path}.{function_name}"
        
        # Verificar cache de funciones
        if not reload and self.enable_cache and cache_key in self._function_cache:
            if self.logger:
                self.logger.debug(f"📦 Función '{cache_key}' cargada desde cache")
            return self._function_cache[cache_key]
        
        # Importar módulo
        module = self.import_module(module_path, reload=reload)
        
        if module is None:
            return None
        
        # Obtener función del módulo
        try:
            func = getattr(module, function_name)
            
            if self.logger:
                self.logger.debug(f"✓ Función '{function_name}' obtenida de '{module_path}'")
            
            # Guardar en cache
            if self.enable_cache:
                self._function_cache[cache_key] = func
            
            return func
            
        except AttributeError:
            if self.logger:
                self.logger.error(
                    f"✗ Función '{function_name}' no encontrada en módulo '{module_path}'"
                )
            return None
    
    def import_and_execute(
        self,
        module_path: str,
        function_name: str,
        *args,
        **kwargs
    ) -> Any:
        """
        Importa y ejecuta una función en un solo paso
        
        Args:
            module_path: Ruta del módulo
            function_name: Nombre de la función
            *args: Argumentos posicionales
            **kwargs: Argumentos con nombre
        
        Returns:
            Resultado de la ejecución de la función
        
        Raises:
            ImportError: Si no se puede importar
            RuntimeError: Si la función no se puede ejecutar
        """
        func = self.import_function(module_path, function_name)
        
        if func is None:
            raise ImportError(
                f"No se pudo importar {function_name} desde {module_path}"
            )
        
        try:
            if self.logger:
                self.logger.debug(f"▶️  Ejecutando: {module_path}.{function_name}")
            
            result = func(*args, **kwargs)
            
            if self.logger:
                self.logger.debug(f"✓ Ejecución completada: {function_name}")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"✗ Error al ejecutar {function_name}: {e}")
            raise RuntimeError(
                f"Error ejecutando {module_path}.{function_name}: {e}"
            ) from e
    
    def preload_modules(self, module_paths: List[str]) -> Dict[str, bool]:
        """
        Precarga múltiples módulos de forma anticipada
        
        Args:
            module_paths: Lista de rutas de módulos a precargar
        
        Returns:
            Diccionario con resultado de cada precarga {module_path: success}
        """
        results = {}
        
        if self.logger:
            self.logger.info(f"🔄 Precargando {len(module_paths)} módulos...")
        
        for module_path in module_paths:
            module = self.import_module(module_path)
            results[module_path] = module is not None
        
        successful = sum(1 for success in results.values() if success)
        
        if self.logger:
            self.logger.info(
                f"✓ Precarga completada: {successful}/{len(module_paths)} exitosos"
            )
        
        return results
    
    def validate_dependencies(self, module_path: str) -> bool:
        """
        Valida que un módulo puede ser importado sin errores
        
        Args:
            module_path: Ruta del módulo a validar
        
        Returns:
            True si el módulo es válido y puede importarse
        """
        try:
            module = self.import_module(module_path)
            return module is not None
        except Exception:
            return False
    
    def clear_cache(self, module_path: Optional[str] = None):
        """
        Limpia el cache de módulos
        
        Args:
            module_path: Ruta específica a limpiar, o None para limpiar todo
        """
        if module_path:
            # Limpiar módulo específico
            if module_path in self._module_cache:
                del self._module_cache[module_path]
                if self.logger:
                    self.logger.debug(f"🗑️  Cache limpiado: {module_path}")
            
            # Limpiar funciones relacionadas
            keys_to_remove = [
                k for k in self._function_cache.keys()
                if k.startswith(module_path)
            ]
            for key in keys_to_remove:
                del self._function_cache[key]
        else:
            # Limpiar todo
            self._module_cache.clear()
            self._function_cache.clear()
            
            if self.logger:
                self.logger.debug("🗑️  Cache completamente limpiado")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de performance de carga de módulos
        
        Returns:
            Diccionario con métricas de performance
        """
        if not self._load_times:
            return {
                "total_modules_loaded": 0,
                "total_load_time": 0,
                "average_load_time": 0,
                "modules": {}
            }
        
        total_time = sum(self._load_times.values())
        avg_time = total_time / len(self._load_times)
        
        # Ordenar por tiempo de carga (más lentos primero)
        sorted_modules = sorted(
            self._load_times.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            "total_modules_loaded": len(self._load_times),
            "total_load_time": total_time,
            "average_load_time": avg_time,
            "slowest_modules": sorted_modules[:5],
            "modules": {
                module: {
                    "load_time_ms": time * 1000,
                    "load_count": self._load_count.get(module, 0),
                    "cached": module in self._module_cache
                }
                for module, time in self._load_times.items()
            }
        }
    
    def print_performance_report(self):
        """
        Imprime reporte de performance en consola
        """
        stats = self.get_performance_stats()
        
        if stats["total_modules_loaded"] == 0:
            print("📊 No hay estadísticas de carga disponibles")
            return
        
        print("\n" + "="*60)
        print("📊 REPORTE DE PERFORMANCE - LAZY LOADING")
        print("="*60)
        
        print(f"\nMódulos cargados: {stats['total_modules_loaded']}")
        print(f"Tiempo total de carga: {stats['total_load_time']*1000:.1f}ms")
        print(f"Tiempo promedio por módulo: {stats['average_load_time']*1000:.1f}ms")
        
        if stats.get('slowest_modules'):
            print("\n🐌 Módulos más lentos:")
            for i, (module, time) in enumerate(stats['slowest_modules'], 1):
                count = self._load_count.get(module, 0)
                cached = "📦" if module in self._module_cache else "  "
                print(f"  {i}. {cached} {module}: {time*1000:.1f}ms (cargado {count}x)")
        
        print("="*60 + "\n")


def lazy_import(
    module_path: str,
    function_name: Optional[str] = None,
    logger: Optional[PipelineLogger] = None
) -> Any:
    """
    Función helper para lazy import rápido sin instanciar LazyLoader
    
    Args:
        module_path: Ruta del módulo
        function_name: Nombre de función opcional
        logger: Logger opcional
    
    Returns:
        Módulo o función importada
    """
    loader = LazyLoader(logger=logger)
    
    if function_name:
        return loader.import_function(module_path, function_name)
    else:
        return loader.import_module(module_path)


def create_lazy_wrapper(
    module_path: str,
    function_name: str,
    logger: Optional[PipelineLogger] = None
) -> Callable:
    """
    Crea un wrapper lazy para una función
    La función no se importa hasta que se llame por primera vez
    
    Args:
        module_path: Ruta del módulo
        function_name: Nombre de la función
        logger: Logger opcional
    
    Returns:
        Wrapper function que importa lazily
    """
    loader = LazyLoader(logger=logger)
    
    @wraps(lambda: None)  # Placeholder para preservar metadata
    def wrapper(*args, **kwargs):
        func = loader.import_function(module_path, function_name)
        if func is None:
            raise ImportError(
                f"No se pudo importar {function_name} desde {module_path}"
            )
        return func(*args, **kwargs)
    
    # Agregar metadata
    wrapper.__name__ = function_name
    wrapper.__doc__ = f"Lazy wrapper for {module_path}.{function_name}"
    wrapper._lazy_module = module_path
    wrapper._lazy_function = function_name
    
    return wrapper


# Instancia global opcional para uso singleton
_global_loader: Optional[LazyLoader] = None


def get_global_loader(
    logger: Optional[PipelineLogger] = None
) -> LazyLoader:
    """
    Obtiene instancia global singleton del LazyLoader
    
    Args:
        logger: Logger opcional (solo usado en primera inicialización)
    
    Returns:
        Instancia global de LazyLoader
    """
    global _global_loader
    if _global_loader is None:
        _global_loader = LazyLoader(logger=logger)
    return _global_loader