# utils/paths.py
import sys
import os
from pathlib import Path

def get_project_root() -> Path:
    """
    Retorna la ruta base del proyecto.
    - En Dev: La raíz del proyecto (donde está main.py).
    - En Exe (Onedir): La carpeta donde está el .exe.
    """
    if getattr(sys, 'frozen', False):
        # Si es ejecutable (PyInstaller)
        return Path(sys.executable).parent
    else:
        # Si es desarrollo (asumiendo que este archivo está en /utils)
        # Subimos un nivel desde utils/ para llegar a la raíz
        return Path(__file__).parent.parent

def get_resource_path(relative_path: str) -> Path:
    """
    Resuelve la ruta absoluta para recursos (lectura).
    Maneja la diferencia entre estructura dev y estructura interna de PyInstaller (_internal).
    """
    base_path = get_project_root()
    
    # En PyInstaller v6+ onedir, a veces los recursos van a _internal
    # Verificamos si existe en la raíz, sino intentamos _internal (si aplica)
    full_path = base_path / relative_path
    
    # Si no existe en la raíz del exe y estamos congelados, 
    # podría estar en la carpeta temporal o _internal (depende de tu config de pyinstaller)
    if not full_path.exists() and getattr(sys, 'frozen', False):
        if hasattr(sys, '_MEIPASS'):
             # _MEIPASS se usa más en onefile, pero a veces en onedir para temporales
            temp_path = Path(sys._MEIPASS) / relative_path
            if temp_path.exists():
                return temp_path
                
    return full_path