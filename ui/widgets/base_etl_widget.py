# ui/widgets/base_etl_widget.py
"""
Widget base reutilizable para todos los ETLs
"""
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QProgressBar, QLabel, QTextEdit, QGroupBox, QMessageBox
)
from PySide6.QtCore import Qt
from pathlib import Path
from typing import List, Optional
from abc import abstractmethod
import sys

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.file_selector_qt import quick_file_select_qt


class BaseETLWidget(QWidget):
    """Widget base para ETLs (sin ABC para evitar conflicto de metaclases)"""
    
    def __init__(self, title: str):
        super().__init__()
        self.title = title
        self.archivos_seleccionados: List[Path] = []
        self.worker = None
        self._setup_ui()
    
    def _setup_ui(self):
        """Configura UI común"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # Header
        header = QLabel(self.title)
        header.setProperty("labelStyle", "title")
        header.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header)
        
        # Card: Selección de archivos
        group_files = QGroupBox("1. Selección de Archivos")
        layout_files = QVBoxLayout()
        
        self.label_files = QLabel(self._get_no_files_message())
        self.label_files.setProperty("labelStyle", "secondary")
        self.label_files.setWordWrap(True)
        
        btn_layout = QHBoxLayout()
        self.btn_select = QPushButton(self._get_select_button_text())
        self.btn_select.clicked.connect(self._select_files)
        self.btn_clear = QPushButton("🗑️ Limpiar")
        self.btn_clear.clicked.connect(self._clear_files)
        self.btn_clear.setEnabled(False)
        
        btn_layout.addWidget(self.btn_select)
        btn_layout.addWidget(self.btn_clear)
        
        layout_files.addWidget(self.label_files)
        layout_files.addLayout(btn_layout)
        group_files.setLayout(layout_files)
        main_layout.addWidget(group_files)
        
        # Card: Progreso
        group_progress = QGroupBox("2. Procesamiento")
        layout_progress = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        
        self.status_label = QLabel("")
        self.status_label.setProperty("labelStyle", "secondary")
        
        self.btn_process = QPushButton("▶️ Iniciar Procesamiento")
        self.btn_process.clicked.connect(self._start_processing)
        self.btn_process.setEnabled(False)
        
        layout_progress.addWidget(self.progress_bar)
        layout_progress.addWidget(self.status_label)
        layout_progress.addWidget(self.btn_process)
        group_progress.setLayout(layout_progress)
        main_layout.addWidget(group_progress)
        
        # Card: Log
        group_log = QGroupBox("3. Log de Actividad")
        layout_log = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(150)
        self.log_text.append(f"📋 Sistema {self.title} listo")
        
        layout_log.addWidget(self.log_text)
        group_log.setLayout(layout_log)
        main_layout.addWidget(group_log)
        
        main_layout.addStretch()
    
    @abstractmethod
    def _get_worker_class(self):
        """Retorna la clase Worker específica del ETL"""
        pass
    
    @abstractmethod
    def _get_file_filter(self) -> str:
        """Retorna filtro para diálogo de archivos"""
        pass
    
    def _get_no_files_message(self) -> str:
        return "No hay archivos seleccionados"
    
    def _get_select_button_text(self) -> str:
        return "📁 Seleccionar Archivos"
    
    def _select_files(self):
        """Abre diálogo para seleccionar archivos"""
        files = quick_file_select_qt(
            parent=self,
            title=f"Seleccionar archivos - {self.title}",
            file_filter=self._get_file_filter(),
            multiple=True
        )
        
        if files:
            self.archivos_seleccionados = files
            count = len(files)
            self.label_files.setText(
                f"✓ {count} archivo{'s' if count > 1 else ''} seleccionado{'s' if count > 1 else ''}:\n" +
                "\n".join([f"  • {f.name}" for f in files[:5]]) +
                (f"\n  ... y {count - 5} más" if count > 5 else "")
            )
            self.btn_process.setEnabled(True)
            self.btn_clear.setEnabled(True)
            self._log(f"📂 Seleccionados {count} archivo(s)")
    
    def _clear_files(self):
        """Limpia la selección de archivos"""
        self.archivos_seleccionados = []
        self.label_files.setText(self._get_no_files_message())
        self.btn_process.setEnabled(False)
        self.btn_clear.setEnabled(False)
        self._log("🗑️ Selección limpiada")
    
    def _start_processing(self):
        """Inicia el procesamiento ETL"""
        if not self.archivos_seleccionados:
            return
        
        self.btn_select.setEnabled(False)
        self.btn_process.setEnabled(False)
        self.btn_clear.setEnabled(False)
        
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        self._log("🚀 Iniciando procesamiento ETL...")
        
        # Obtener directorio de salida (padre del primer archivo)
        output_dir = self.archivos_seleccionados[0].parent
        
        # Crear worker específico
        WorkerClass = self._get_worker_class()
        self.worker = WorkerClass(self.archivos_seleccionados, output_dir)
        self.worker.progress_updated.connect(self._on_progress)
        self.worker.finished.connect(self._on_finished)
        self.worker.start()
    
    def _on_progress(self, value: int, message: str):
        """Actualiza barra de progreso"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
        self._log(f"[{value}%] {message}")
    
    def _on_finished(self, success: bool, message: str, results: dict):
        """Maneja finalización del proceso"""
        self._log(message)
        
        self.btn_select.setEnabled(True)
        self.btn_clear.setEnabled(True)
        self.btn_process.setEnabled(True)
        
        if success:
            QMessageBox.information(self, "Proceso Completado", message)
            self.progress_bar.setVisible(False)
            self.status_label.setText("")
        else:
            QMessageBox.critical(self, "Error en Procesamiento", message)
    
    def _log(self, message: str):
        """Agrega mensaje al log"""
        self.log_text.append(message)