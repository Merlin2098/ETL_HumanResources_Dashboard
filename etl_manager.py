#!/usr/bin/env python3
# etl_manager.py
"""
ETL Manager - Tawa Consulting
Entry point principal de la aplicación
"""
import sys
from pathlib import Path

# Agregar directorio actual al path
sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication
from ui import ETLManagerWindow


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
        from PySide6.QtGui import QIcon
        icon_path = Path(__file__).parent / "config" / "app.ico"
        if icon_path.exists():
            app.setWindowIcon(QIcon(str(icon_path)))
            print("✅ Ícono de aplicación configurado")
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