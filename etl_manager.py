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
from src.app_main import ETLManagerWindow  # ✅ Import absoluto correcto
from src.utils.paths import get_resource_path  # ✅ Para rutas en producción
from src.utils.ui.splash_screen import StartupSplashScreen

def main():
    """Función principal"""
    print("="*60)
    print(" ETL MANAGER - TAWA CONSULTING ".center(60))
    print("="*60)
    
    app = QApplication(sys.argv)
    app.setApplicationName("ETL Manager - Tawa Consulting")
    app.setOrganizationName("Tawa Consulting")

    splash = StartupSplashScreen("ETL Manager - Tawa Consulting")
    splash.show()
    splash.update_status(5, "Iniciando aplicacion")
    
    # Configurar ícono de la aplicación
    try:
        icon_path = get_resource_path("assets/config/app.ico")  # ✅ Usar helper
        if icon_path.exists():
            app.setWindowIcon(QIcon(str(icon_path)))
            print("✅ Ícono de aplicación configurado")
            splash.update_status(10, "Icono de aplicacion cargado")
        else:
            print(f"⚠️ Ícono no encontrado en: {icon_path}")
            splash.update_status(10, "Icono no encontrado, continuando")
    except Exception as e:
        print(f"⚠️ No se pudo configurar el ícono: {e}")
        splash.update_status(10, "Error cargando icono, continuando")

    def on_startup_progress(progress: int, message: str):
        splash.update_status(progress, message)
    
    splash.update_status(15, "Construyendo ventana principal")
    window = ETLManagerWindow(startup_progress_callback=on_startup_progress)
    window.show()
    splash.update_status(100, "Aplicacion lista")
    splash.finish(window)
    
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
