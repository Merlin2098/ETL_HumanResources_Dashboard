"""
Launcher Simplificado - Sistema ETL Tawa Consulting
Inicia directamente el menú interactivo
Doble clic en este archivo para ejecutar el sistema
"""
import sys
from pathlib import Path

# Asegurar que el directorio raíz está en el path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    # Importar y mostrar menú directamente
    from cli.menu import show_main_menu
    
    # Ejecutar menú interactivo
    show_main_menu()
    
except KeyboardInterrupt:
    print("\n\nSistema cerrado por el usuario.\n")
    sys.exit(0)
    
except Exception as e:
    print(f"\n❌ Error al iniciar el sistema: {e}")
    print("\nVerifica que todas las dependencias estén instaladas:")
    print("  pip install typer rich questionary polars openpyxl duckdb\n")
    input("Presiona Enter para salir...")
    sys.exit(1)