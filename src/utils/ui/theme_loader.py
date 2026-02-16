# src/utils/ui/theme_loader.py
import json
from pathlib import Path
from src.utils.paths import get_resource_path

def load_theme(theme_filename: str = "theme_light.json") -> str:
    """
    Carga stylesheet desde archivo JSON.
    
    Args:
        theme_filename: Nombre del archivo (ej: 'theme_light.json')
                       o ruta relativa 'assets/config/theme_light.json'
        
    Returns:
        String con el stylesheet QSS
    """
    # Si viene solo el nombre, asumimos que está en assets/config/
    if "/" not in theme_filename and "\\" not in theme_filename:
        relative_path = Path("assets/config") / theme_filename
    else:
        relative_path = Path(theme_filename)
    
    # Resolver ruta absoluta compatible con EXE
    full_path = get_resource_path(relative_path)
    
    if not full_path.exists():
        # Fallback: intentar buscar en raíz si falló config/
        full_path_root = get_resource_path(theme_filename)
        if full_path_root.exists():
            full_path = full_path_root
        else:
            raise FileNotFoundError(f"Tema no encontrado en: {full_path}")
    
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            theme_data = json.load(f)
        return theme_data['pyqt5']['stylesheet']
    except Exception as e:
        print(f"Error cargando tema: {e}")
        return "" # Retorno seguro para no crashear la UI
