#!/usr/bin/env python3
# etl_manager.py
"""
ETL Manager - Tawa Consulting
Entry point principal de la aplicación
"""
import sys
from pathlib import Path

# Agregar directorio raíz al path
project_root = Path(__file__).parent.resolve()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
from ui.main_app import ETLManagerWindow  # ✅ Import absoluto correcto
from utils.paths import get_resource_path  # ✅ Para rutas en producción


def main():
    """Función principal"""
    print("="*60)
    print(" ETL MANAGER - TAWA CONSULTING ".center(60))
    print("="*60)
    
    app = QApplication(sys.argv)
    app.setApplicationName("ETL Manager - Tawa Consulting")
    app.setOrganizationName("Tawa Consulting")
    
    # Configurar ícono de la aplicación
    try:
        icon_path = get_resource_path("config/app.ico")  # ✅ Usar helper
        if icon_path.exists():
            app.setWindowIcon(QIcon(str(icon_path)))
            print("✅ Ícono de aplicación configurado")
        else:
            print(f"⚠️ Ícono no encontrado en: {icon_path}")
    except Exception as e:
        print(f"⚠️ No se pudo configurar el ícono: {e}")
    
    window = ETLManagerWindow()
    window.show()
    
    print("\n✅ Aplicación iniciada correctamente\n")
    
    sys.exit(app.exec())


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)