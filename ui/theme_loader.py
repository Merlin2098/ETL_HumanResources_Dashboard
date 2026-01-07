# ui/theme_loader.py
"""
Carga y aplica tema desde JSON
"""
import json
from pathlib import Path


def load_theme(json_path: str) -> str:
    """
    Carga stylesheet desde archivo JSON.
    
    Args:
        json_path: Path al archivo theme_light.json
        
    Returns:
        String con el stylesheet QSS
    """
    path = Path(json_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Tema no encontrado: {json_path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        theme_data = json.load(f)
    
    return theme_data['pyqt5']['stylesheet']