"""
Widget de interfaz para ETL de Base de Datos
Proporciona controles para ejecutar el procesamiento completo y aplicación de flags
"""

from PySide6.QtWidgets import (
    QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QFileDialog, QMessageBox
)
from PySide6.QtCore import Qt
from pathlib import Path

from ui.widgets.base_etl_widget import BaseETLWidget
from .worker import BDWorker


class BDWidget(BaseETLWidget):
    """
    Widget para procesamiento de Base de Datos Consolidada
    
    Flujo:
    1. Botón "Ejecutar ETL Completo": Bronze→Silver + Centros de Costo + Gold
    2. Botón "Aplicar Flags": Requiere selección manual de archivos CC
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.archivo_excel = None
        self.archivos_cc = []  # Para step 3
        self.setup_ui()
    
    def setup_ui(self):
        """Configura la interfaz del widget"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # ============================================================
        # SECCIÓN 1: Selección de archivo Excel Bronze
        # ============================================================
        archivo_layout = QHBoxLayout()
        
        self.label_archivo = QLabel("📂 Archivo Excel Bronze:")
        self.label_archivo.setStyleSheet("font-weight: bold;")
        archivo_layout.addWidget(self.label_archivo)
        
        self.btn_seleccionar = QPushButton("Seleccionar Excel")
        self.btn_seleccionar.clicked.connect(self.seleccionar_archivo)
        archivo_layout.addWidget(self.btn_seleccionar)
        
        self.label_ruta = QLabel("Ningún archivo seleccionado")
        self.label_ruta.setStyleSheet("color: #666; font-style: italic;")
        self.label_ruta.setWordWrap(True)
        archivo_layout.addWidget(self.label_ruta, stretch=1)
        
        layout.addLayout(archivo_layout)
        
        # ============================================================
        # SECCIÓN 2: Botón ETL Completo
        # ============================================================
        self.btn_etl_completo = QPushButton("▶️  Ejecutar ETL Completo")
        self.btn_etl_completo.setStyleSheet("""
            QPushButton {
                background-color: #7C3AED;
                color: white;
                font-weight: bold;
                padding: 12px;
                border-radius: 6px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #6D28D9;
            }
            QPushButton:disabled {
                background-color: #CCC;
            }
        """)
        self.btn_etl_completo.clicked.connect(self.ejecutar_etl_completo)
        self.btn_etl_completo.setEnabled(False)
        layout.addWidget(self.btn_etl_completo)
        
        # Info ETL Completo
        info_completo = QLabel(
            "✓ Step 1: Bronze → Silver\n"
            "✓ Step 1.5: Extracción de Centros de Costo (con timestamp)\n"
            "✓ Step 2: Silver → Gold (Empleados + Practicantes)"
        )
        info_completo.setStyleSheet("color: #555; font-size: 11px; margin-left: 10px;")
        layout.addWidget(info_completo)
        
        # Separador
        separador = QLabel("━" * 80)
        separador.setStyleSheet("color: #DDD;")
        layout.addWidget(separador)
        
        # ============================================================
        # SECCIÓN 3: Botón Aplicar Flags
        # ============================================================
        self.btn_flags = QPushButton("🏴  Aplicar Flags a Empleados")
        self.btn_flags.setStyleSheet("""
            QPushButton {
                background-color: #059669;
                color: white;
                font-weight: bold;
                padding: 12px;
                border-radius: 6px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #047857;
            }
            QPushButton:disabled {
                background-color: #CCC;
            }
        """)
        self.btn_flags.clicked.connect(self.ejecutar_flags)
        layout.addWidget(self.btn_flags)
        
        # Info Flags
        info_flags = QLabel(
            "ℹ️  Requiere selección manual de archivos Centros de Costo:\n"
            "   • CC_ACTUAL (obligatorio)\n"
            "   • CC_OLD (opcional, múltiples permitidos)"
        )
        info_flags.setStyleSheet("color: #555; font-size: 11px; margin-left: 10px;")
        layout.addWidget(info_flags)
        
        # Espaciador
        layout.addStretch()
    
    def seleccionar_archivo(self):
        """Abre diálogo para seleccionar archivo Excel Bronze"""
        archivo, _ = QFileDialog.getOpenFileName(
            self,
            "Seleccionar archivo Excel Bronze - BD",
            "",
            "Excel Files (*.xlsx *.xlsm *.xls);;All Files (*)"
        )
        
        if archivo:
            self.archivo_excel = Path(archivo)
            self.label_ruta.setText(f"✓ {self.archivo_excel.name}")
            self.label_ruta.setStyleSheet("color: #059669; font-weight: bold;")
            self.btn_etl_completo.setEnabled(True)
        else:
            self.archivo_excel = None
            self.label_ruta.setText("Ningún archivo seleccionado")
            self.label_ruta.setStyleSheet("color: #666; font-style: italic;")
            self.btn_etl_completo.setEnabled(False)
    
    def ejecutar_etl_completo(self):
        """Ejecuta el ETL completo: Steps 1 + 1.5 + 2"""
        if not self.archivo_excel:
            QMessageBox.warning(
                self,
                "Archivo no seleccionado",
                "Por favor, selecciona un archivo Excel Bronze."
            )
            return
        
        # Deshabilitar botones durante ejecución
        self.btn_etl_completo.setEnabled(False)
        self.btn_seleccionar.setEnabled(False)
        self.btn_flags.setEnabled(False)
        
        # Crear y configurar worker
        worker = BDWorker(
            archivo_excel=self.archivo_excel,
            modo='etl_completo'
        )
        
        # Conectar señales
        worker.signals.log.connect(self.agregar_log)
        worker.signals.progress.connect(self.actualizar_progreso)
        worker.signals.finished.connect(self.on_etl_completo_finished)
        worker.signals.error.connect(self.on_error)
        
        # Ejecutar en thread pool
        self.thread_pool.start(worker)
    
    def ejecutar_flags(self):
        """Ejecuta Step 3: Aplicación de flags con selección de archivos CC"""
        # Mostrar diálogo para seleccionar archivos CC
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle("Selección de Archivos CC")
        msg.setText(
            "A continuación, selecciona los archivos de Centros de Costo:\n\n"
            "1. Primero: CC_ACTUAL (obligatorio)\n"
            "2. Luego: CC_OLD (opcional, puedes seleccionar múltiples)"
        )
        msg.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        
        if msg.exec() == QMessageBox.Cancel:
            return
        
        # Seleccionar CC_ACTUAL
        cc_actual, _ = QFileDialog.getOpenFileName(
            self,
            "Seleccionar CC_ACTUAL (obligatorio)",
            "",
            "Parquet Files (*.parquet);;All Files (*)"
        )
        
        if not cc_actual:
            QMessageBox.warning(self, "Cancelado", "No se seleccionó CC_ACTUAL. Proceso cancelado.")
            return
        
        # Seleccionar CC_OLD (múltiples opcionales)
        cc_old_files, _ = QFileDialog.getOpenFileNames(
            self,
            "Seleccionar CC_OLD (opcional, múltiples permitidos)",
            "",
            "Parquet Files (*.parquet);;All Files (*)"
        )
        
        # Preparar lista de archivos CC
        self.archivos_cc = {
            'cc_actual': Path(cc_actual),
            'cc_old': [Path(f) for f in cc_old_files] if cc_old_files else []
        }
        
        # Confirmar selección
        msg_confirmacion = (
            f"✓ CC_ACTUAL: {Path(cc_actual).name}\n"
            f"✓ CC_OLD: {len(self.archivos_cc['cc_old'])} archivo(s)\n\n"
            "¿Proceder con la aplicación de flags?"
        )
        
        reply = QMessageBox.question(
            self,
            "Confirmar Selección",
            msg_confirmacion,
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.No:
            return
        
        # Deshabilitar botones durante ejecución
        self.btn_etl_completo.setEnabled(False)
        self.btn_seleccionar.setEnabled(False)
        self.btn_flags.setEnabled(False)
        
        # Crear y configurar worker
        worker = BDWorker(
            modo='flags',
            archivos_cc=self.archivos_cc
        )
        
        # Conectar señales
        worker.signals.log.connect(self.agregar_log)
        worker.signals.progress.connect(self.actualizar_progreso)
        worker.signals.finished.connect(self.on_flags_finished)
        worker.signals.error.connect(self.on_error)
        
        # Ejecutar en thread pool
        self.thread_pool.start(worker)
    
    def on_etl_completo_finished(self):
        """Callback cuando termina el ETL completo"""
        self.agregar_log("\n✅ ETL Completo finalizado exitosamente\n")
        
        # Rehabilitar botones
        self.btn_etl_completo.setEnabled(True)
        self.btn_seleccionar.setEnabled(True)
        self.btn_flags.setEnabled(True)
        
        QMessageBox.information(
            self,
            "Proceso Completado",
            "ETL Completo ejecutado exitosamente.\n\n"
            "Archivos generados:\n"
            "• bd_silver.parquet (carpeta silver/)\n"
            "• cc_{timestamp}.parquet (carpeta centros_costo/)\n"
            "• bd_empleados_gold.parquet (carpeta gold/)\n"
            "• bd_practicantes_gold.parquet (carpeta gold/)"
        )
    
    def on_flags_finished(self):
        """Callback cuando termina la aplicación de flags"""
        self.agregar_log("\n✅ Aplicación de Flags finalizada exitosamente\n")
        
        # Rehabilitar botones
        self.btn_etl_completo.setEnabled(True)
        self.btn_seleccionar.setEnabled(True)
        self.btn_flags.setEnabled(True)
        
        QMessageBox.information(
            self,
            "Proceso Completado",
            "Flags aplicados exitosamente.\n\n"
            "Archivo generado:\n"
            "• bd_empleados_gold_flags_{timestamp}.parquet"
        )
    
    def on_error(self, error_msg):
        """Callback cuando ocurre un error"""
        self.agregar_log(f"\n❌ ERROR: {error_msg}\n")
        
        # Rehabilitar botones
        self.btn_etl_completo.setEnabled(True)
        self.btn_seleccionar.setEnabled(True)
        self.btn_flags.setEnabled(True)
        
        QMessageBox.critical(
            self,
            "Error en Procesamiento",
            f"Ocurrió un error durante la ejecución:\n\n{error_msg}"
        )