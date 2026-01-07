# ui/etl_registry.py
"""
Registro automático de ETLs disponibles.
Lee la carpeta etls/ y descubre qué ETLs están disponibles.
"""
from pathlib import Path
from typing import List, Dict
from importlib import import_module


class ETLRegistry:
    """
    Registro de ETLs disponibles.
    Descubre automáticamente los ETLs en la carpeta etls/
    """
    
    def __init__(self):
        self.etls: Dict[str, Dict] = {}
        self.etls_dir = Path(__file__).parent / "etls"
    
    def discover_etls(self) -> List[Dict]:
        """
        Descubre todos los ETLs disponibles en la carpeta etls/
        
        Returns:
            Lista de diccionarios con metadata de cada ETL
        """
        if not self.etls_dir.exists():
            print("⚠️ No se encontró la carpeta etls/")
            return []
        
        discovered_etls = []
        
        # Buscar subcarpetas en etls/
        for etl_dir in self.etls_dir.iterdir():
            if not etl_dir.is_dir():
                continue
            
            if etl_dir.name.startswith('_'):  # Ignorar __pycache__, etc.
                continue
            
            # Buscar config.py
            config_file = etl_dir / "config.py"
            widget_file = etl_dir / "widget.py"
            
            if not config_file.exists() or not widget_file.exists():
                print(f"⚠️ ETL incompleto: {etl_dir.name} (falta config.py o widget.py)")
                continue
            
            try:
                # Importar configuración
                module_path = f"ui.etls.{etl_dir.name}.config"
                config_module = import_module(module_path)
                
                if not hasattr(config_module, 'CONFIG'):
                    print(f"⚠️ {etl_dir.name}/config.py no tiene CONFIG")
                    continue
                
                config = config_module.CONFIG
                
                # Verificar si está habilitado
                if not config.enabled:
                    print(f"⏸️ ETL deshabilitado: {config.name}")
                    continue
                
                # Importar widget
                widget_module_path = f"ui.etls.{etl_dir.name}.widget"
                widget_module = import_module(widget_module_path)
                
                # Buscar clase que termine en "Widget"
                widget_class = None
                for item_name in dir(widget_module):
                    if item_name.endswith('Widget') and item_name != 'BaseETLWidget':
                        widget_class = getattr(widget_module, item_name)
                        break
                
                if not widget_class:
                    print(f"⚠️ No se encontró clase Widget en {etl_dir.name}")
                    continue
                
                # Registrar ETL
                etl_info = {
                    'id': config.id,
                    'name': config.name,
                    'icon': config.icon,
                    'description': config.description,
                    'order': config.order,
                    'widget_class': widget_class,
                    'config': config
                }
                
                discovered_etls.append(etl_info)
                print(f"✅ ETL descubierto: {config.icon} {config.name}")
                
            except Exception as e:
                print(f"❌ Error cargando ETL {etl_dir.name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Ordenar por 'order'
        discovered_etls.sort(key=lambda x: x['order'])
        
        return discovered_etls
    
    def get_etl(self, etl_id: str) -> Dict:
        """Obtiene información de un ETL específico"""
        return self.etls.get(etl_id)
    
    def list_etls(self) -> List[str]:
        """Lista IDs de todos los ETLs registrados"""
        return list(self.etls.keys())


# Instancia global
_registry = None


def get_registry() -> ETLRegistry:
    """Obtiene instancia singleton del registry"""
    global _registry
    if _registry is None:
        _registry = ETLRegistry()
    return _registry