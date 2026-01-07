# ui/main_app.py
"""
Ventana principal con tabs dinámicos.
Los tabs se cargan automáticamente desde el registry.
"""
from PySide6.QtWidgets import QMainWindow, QTabWidget, QMessageBox
from pathlib import Path

from .theme_loader import load_theme
from .etl_registry import get_registry


class ETLManagerWindow(QMainWindow):
    """Ventana principal con tabs dinámicos para ETLs."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Manager - Tawa Consulting")
        self.setMinimumSize(900, 650)
        
        self._setup_ui()
        self._apply_theme()
    
    def _setup_ui(self):
        """Configura la interfaz con tabs dinámicos."""
        # Tab widget principal
        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(False)
        
        # Cargar ETLs dinámicamente
        self._load_etl_tabs()
        
        self.setCentralWidget(self.tabs)
    
    def _load_etl_tabs(self):
        """
        Descubre y carga automáticamente todos los ETLs disponibles.
        """
        print("\n" + "="*60)
        print(" DESCUBRIENDO ETLs DISPONIBLES ".center(60, "="))
        print("="*60 + "\n")
        
        registry = get_registry()
        etls = registry.discover_etls()
        
        if not etls:
            # Si no hay ETLs, mostrar mensaje
            print("⚠️ No se encontraron ETLs disponibles")
            QMessageBox.warning(
                self,
                "Sin ETLs",
                "No se encontraron ETLs disponibles.\n"
                "Verifica la carpeta ui/etls/"
            )
            return
        
        print(f"\n📦 Total de ETLs encontrados: {len(etls)}\n")
        
        # Crear tabs dinámicamente
        for etl_info in etls:
            try:
                # Instanciar widget
                widget = etl_info['widget_class']()
                
                # Agregar tab con ícono + nombre
                tab_label = f"{etl_info['icon']} {etl_info['name']}"
                self.tabs.addTab(widget, tab_label)
                
                print(f"  ✅ Tab creado: {tab_label}")
                
            except Exception as e:
                print(f"  ❌ Error creando tab para {etl_info['name']}: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n{'='*60}\n")
    
    def _apply_theme(self):
        """Aplica tema desde JSON."""
        try:
            theme_path = Path(__file__).parent.parent / "config" / "theme_light.json"
            stylesheet = load_theme(str(theme_path))
            self.setStyleSheet(stylesheet)
            print("✅ Tema aplicado correctamente")
        except Exception as e:
            print(f"⚠️ Error cargando tema: {e}")