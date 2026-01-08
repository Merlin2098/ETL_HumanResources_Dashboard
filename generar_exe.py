"""
Script de Generación de Ejecutable Onedir
Proyecto: Sistema ETL / Nóminas
Genera un ejecutable Windows con carpeta distribuible que incluye esquemas y queries.

Adaptado para estructura modular (BD, Nómina, PDT, Examen Retiro).
"""

import os
import sys
import pkg_resources
import subprocess
import shutil
from pathlib import Path
import time
import threading

# ==========================================================
# CONFIGURACIÓN GENERAL
# ==========================================================
# Nombre del ejecutable final
NOMBRE_EXE = "GestorETL.exe" 

# Script principal que lanza la interfaz gráfica
# NOTA: Ajusta esto si tu punto de entrada es etl_manager.py
MAIN_SCRIPT = "etl_manager.py" 

DIST_PATH = "dist"
BUILD_PATH = "build"
SPEC_PATH = "spec"

# Exclusiones para reducir tamaño (Librerías pesadas no usadas habitualmente en UI simple)
EXCLUSIONES = [
    "tkinter", "test", "unittest",
    "scipy", "matplotlib", "notebook", "jupyter",
    "numpy.testing", "pandas.tests"
]

# ==========================================================
# 1. VALIDACIONES
# ==========================================================
def validar_entorno_virtual():
    """Verifica que se esté ejecutando dentro de un entorno virtual"""
    print("=" * 60)
    print("🔍 VALIDACIÓN DE ENTORNO VIRTUAL")
    print("=" * 60)

    if sys.prefix == sys.base_prefix:
        print("❌ ERROR: No estás dentro de un entorno virtual (venv).")
        print("   Activa uno antes de continuar para evitar incluir librerías del sistema.")
        sys.exit(1)

    print(f"✅ Entorno virtual detectado: {sys.prefix}\n")

def verificar_estructura():
    """Verifica que existan las carpetas y archivos necesarios del proyecto"""
    print("🔍 Verificando estructura del proyecto...")
    base_dir = Path.cwd()
    
    # Carpetas que contienen datos o código dinámico
    carpetas_requeridas = [
        "bd", "config", "esquemas", "examen_retiro", 
        "nomina", "nomina_regimen_minero", "pdt", 
        "queries", "ui", "utils"
    ]
    
    missing = []
    for carpeta in carpetas_requeridas:
        if not (base_dir / carpeta).exists():
            missing.append(carpeta)
    
    # Verificar script principal
    if not (base_dir / MAIN_SCRIPT).exists():
        print(f"❌ ERROR: No se encuentra el script principal: {MAIN_SCRIPT}")
        sys.exit(1)

    if missing:
        print(f"❌ ERROR: Faltan carpetas críticas: {missing}")
        sys.exit(1)
        
    print("✅ Estructura de archivos validada correctamente.\n")

# ==========================================================
# 2. LIMPIEZA
# ==========================================================
def limpiar_builds():
    """Elimina carpetas de compilaciones anteriores"""
    print("🧹 Limpiando builds anteriores...")
    for carpeta in [DIST_PATH, BUILD_PATH, SPEC_PATH]:
        if os.path.exists(carpeta):
            try:
                shutil.rmtree(carpeta)
            except Exception as e:
                print(f"   ⚠️  No se pudo eliminar {carpeta}: {e}")
    print("   ✅ Limpieza completada.\n")

# ==========================================================
# 3. CONSTRUCCIÓN DE COMANDO PYINSTALLER
# ==========================================================
def construir_comando():
    """Construye el comando completo de PyInstaller"""
    base_dir = Path.cwd()

    comando = [
        sys.executable, "-m", "PyInstaller",
        "--onedir",              # Carpeta distribuible (más fácil de depurar)
        "--windowed",            # Sin consola negra
        "--clean",               # Limpiar caché
        "--noconfirm",           
        "--log-level", "WARN",
        "--distpath", DIST_PATH,
        "--workpath", BUILD_PATH,
        "--specpath", SPEC_PATH,
        "--name", NOMBRE_EXE.replace(".exe", ""),
    ]

    # --- RUTAS DE BÚSQUEDA (PYTHONPATH) ---
    # Añadimos la raíz y subcarpetas clave para asegurar que encuentre los módulos
    comando += ["--paths", str(base_dir)]
    comando += ["--paths", str(base_dir / "ui")]

    # --- HIDDEN IMPORTS ---
    # Imports que PyInstaller podría no ver (especialmente en cargas dinámicas o SQL)
    hidden_imports = [
        # Librerías de UI
        "PySide6.QtCore", "PySide6.QtGui", "PySide6.QtWidgets",
        
        # Procesamiento de Datos (Asumidos por nombres de archivos)
        "pandas", "openpyxl", "json", "sqlite3", "decimal", "datetime",
        
        # Módulos internos del proyecto (Para asegurar su inclusión)
        "bd", "bd.step1_capasilver", "bd.step2_capagold",
        "nomina", "nomina.step1_consolidar_planillas",
        "pdt", "pdt.step1_consolidar_ingresos",
        "examen_retiro",
        "utils", "utils.logger_qt", "utils.file_selector_qt",
        "ui.widgets", "ui.workers", "ui.theme_loader"
    ]
    
    for imp in hidden_imports:
        comando += ["--hidden-import", imp]

    # --- DATA FILES (RECURSOS) ---
    # Sintaxis: "origen;destino" (Windows usa ;)
    print("📁 Configurando recursos estáticos...")
    
    # 1. Config (Iconos, Temas, JSONs)
    config_path = base_dir / "config"
    if config_path.exists():
        comando += ["--add-data", f"{config_path}{os.pathsep}config"]
    
    # 2. Esquemas (JSONs críticos)
    esquemas_path = base_dir / "esquemas"
    if esquemas_path.exists():
        comando += ["--add-data", f"{esquemas_path}{os.pathsep}esquemas"]

    # 3. Queries (Archivos SQL)
    queries_path = base_dir / "queries"
    if queries_path.exists():
        comando += ["--add-data", f"{queries_path}{os.pathsep}queries"]

    # --- ICONO ---
    ico_path = base_dir / "config" / "app.ico"
    if ico_path.exists():
        comando += ["--icon", str(ico_path)]
    
    # --- EXCLUSIONES ---
    for excl in EXCLUSIONES:
        comando += ["--exclude-module", excl]

    # Script principal
    comando.append(str(base_dir / MAIN_SCRIPT))
    
    return comando

# ==========================================================
# 4. EJECUCIÓN
# ==========================================================
def generar_exe():
    limpiar_builds()
    cmd = construir_comando()
    
    print("\n" + "=" * 60)
    print("🔨 EJECUTANDO PYINSTALLER")
    print("=" * 60)
    print("Este proceso puede tardar unos minutos...\n")
    
    # Animación simple de progreso
    proceso_completado = [False]
    def mostrar_spinner():
        simbolos = ['|', '/', '-', '\\']
        idx = 0
        while not proceso_completado[0]:
            print(f"\r⏳ Generando... {simbolos[idx]}", end="", flush=True)
            idx = (idx + 1) % len(simbolos)
            time.sleep(0.1)
            
    thread = threading.Thread(target=mostrar_spinner, daemon=True)
    thread.start()
    
    # Ejecutar
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proceso_completado[0] = True
        time.sleep(0.2) # Limpiar buffer visual
        
        print("\r" + " " * 30 + "\r", end="") # Limpiar línea

        if result.returncode == 0:
            carpeta_final = Path(DIST_PATH) / NOMBRE_EXE.replace(".exe", "")
            exe_final = carpeta_final / NOMBRE_EXE
            
            print(f"✅ ¡ÉXITO! Ejecutable generado correctamente.")
            print("=" * 60)
            print(f"📂 Ubicación: {carpeta_final.absolute()}")
            print(f"🚀 Ejecutable: {exe_final.name}")
            print("\nNOTA IMPORTANTE:")
            print("   Debes distribuir la CARPETA COMPLETA, no solo el .exe.")
            print("   La carpeta '_internal' contiene tus esquemas, queries y configuraciones.")
        else:
            print("❌ ERROR EN LA COMPILACIÓN")
            print("=" * 60)
            print(result.stderr)
            
    except Exception as e:
        proceso_completado[0] = True
        print(f"\n❌ Error de ejecución: {e}")

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    try:
        validar_entorno_virtual()
        verificar_estructura()
        
        confirm = input(f"¿Generar '{NOMBRE_EXE}' ahora? (S/N): ").lower()
        if confirm in ["s", "si"]:
            generar_exe()
        else:
            print("Cancelado.")
            
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as e:
        print(f"\nError inesperado: {e}")