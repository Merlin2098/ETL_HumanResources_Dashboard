"""
Script de Generación de Ejecutable Onedir
Proyecto: Sistema ETL / Nóminas
Genera un ejecutable Windows con carpeta distribuible que incluye esquemas y queries.

Adaptado para estructura modular (BD, Nómina, PDT, Examen Retiro, Régimen Minero, Licencias, Orquestadores).
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
MAIN_SCRIPT = "etl_manager.py"  # ✅ Punto de entrada correcto

DIST_PATH = "dist"
BUILD_PATH = "build"
SPEC_PATH = "spec"

# Exclusiones para reducir tamaño (Librerías pesadas no usadas)
# NOTA: tkinter NO se excluye porque algunos ETLs lo necesitan
EXCLUSIONES = [
    "test", "unittest",
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
        "licencias", "orquestadores",  # ✅ NUEVAS
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
                print(f"   ⚠️ No se pudo eliminar {carpeta}: {e}")
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
        
        # Procesamiento de Datos
        "pandas", "openpyxl", "json", "sqlite3", "decimal", "datetime",
        "polars", "duckdb", "yaml",  # ✅ YAML para orquestadores
        
        # ✅ UI ETLs (módulos principales)
        "ui.etls",
        "ui.etls.bd", "ui.etls.bd.config", "ui.etls.bd.widget", "ui.etls.bd.worker",
        "ui.etls.nomina", "ui.etls.nomina.config", "ui.etls.nomina.widget", "ui.etls.nomina.worker",
        "ui.etls.pdt", "ui.etls.pdt.config", "ui.etls.pdt.widget", "ui.etls.pdt.worker",
        "ui.etls.nomina_regimen_minero", "ui.etls.nomina_regimen_minero.config", 
        "ui.etls.nomina_regimen_minero.widget", "ui.etls.nomina_regimen_minero.worker",
        "ui.etls.examen_retiro", "ui.etls.examen_retiro.config", 
        "ui.etls.examen_retiro.widget", "ui.etls.examen_retiro.worker",
        
        # ✅ CRÍTICO: TODOS los steps de cada ETL (imports dinámicos)
        # BD
        "bd", 
        "bd.step1_capasilver", 
        "bd.step1.5_centrosdecosto", 
        "bd.step2_capagold", 
        "bd.step3_flags_empleados",
        
        # Nómina
        "nomina", 
        "nomina.step1_consolidar_planillas", 
        "nomina.step2_exportar",
        
        # Nómina Régimen Minero
        "nomina_regimen_minero", 
        "nomina_regimen_minero.step1_consolidar_regimen_minero",
        "nomina_regimen_minero.step2_exportar_regimen_minero",
        
        # PDT
        "pdt", 
        "pdt.step1_consolidar_ingresos",
        "pdt.step2_exportar_ingresos",
        "pdt.step3_exportar_practicantes",
        
        # Examen Retiro
        "examen_retiro", 
        "examen_retiro.step1_clean",
        "examen_retiro.step2_gold", 
        "examen_retiro.step3_join",
        
        # ✅ LICENCIAS (nuevo módulo)
        "licencias",
        "licencias.step1_consolidar_licencias",
        "licencias.step2_enriquecer_nomina",
        
        # ✅ ORQUESTADORES (nuevo módulo)
        "orquestadores",
        "orquestadores.pipeline_nomina_executor",
        
        # Utils
        "utils", 
        "utils.logger_qt", 
        "utils.file_selector_qt",
        "utils.paths",  # ← CRÍTICO para get_resource_path
        "utils.lazy_loader", 
        "utils.path_cache",
        
        # UI Base
        "ui.widgets", 
        "ui.widgets.base_etl_widget",
        "ui.workers", 
        "ui.workers.base_worker",
        "ui.theme_loader", 
        "ui.etl_registry"
    ]
    
    for imp in hidden_imports:
        comando += ["--hidden-import", imp]

    # --- DATA FILES (RECURSOS) ---
    # Sintaxis: "origen;destino" (Windows usa ;)
    print("📦 Configurando recursos estáticos...")
    
    # 1. Config (Iconos, Temas, JSONs)
    config_path = base_dir / "config"
    if config_path.exists():
        comando += ["--add-data", f"{config_path}{os.pathsep}config"]
        print(f"   ✅ Agregando config: {config_path}")
    
    # 2. Esquemas (JSONs críticos)
    esquemas_path = base_dir / "esquemas"
    if esquemas_path.exists():
        comando += ["--add-data", f"{esquemas_path}{os.pathsep}esquemas"]
        print(f"   ✅ Agregando esquemas: {esquemas_path}")

    # 3. Queries (Archivos SQL)
    queries_path = base_dir / "queries"
    if queries_path.exists():
        comando += ["--add-data", f"{queries_path}{os.pathsep}queries"]
        print(f"   ✅ Agregando queries: {queries_path}")

    # 4. ✅ Orquestadores (YAML files)
    orquestadores_path = base_dir / "orquestadores"
    if orquestadores_path.exists():
        comando += ["--add-data", f"{orquestadores_path}{os.pathsep}orquestadores"]
        print(f"   ✅ Agregando orquestadores: {orquestadores_path}")

    # 5. ✅ CRÍTICO: Carpeta ui/etls completa (para auto-discovery)
    etls_path = base_dir / "ui" / "etls"
    if etls_path.exists():
        comando += ["--add-data", f"{etls_path}{os.pathsep}ui/etls"]
        print(f"   ✅ Agregando ui/etls: {etls_path}")

    # --- ICONO ---
    ico_path = base_dir / "config" / "app.ico"
    if ico_path.exists():
        comando += ["--icon", str(ico_path)]
        print(f"   ✅ Icono configurado: {ico_path}")
    
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
            print("\n" + "=" * 60)
            print("📋 NOTAS IMPORTANTES:")
            print("=" * 60)
            print("1. Debes distribuir la CARPETA COMPLETA, no solo el .exe")
            print("2. La carpeta '_internal' contiene:")
            print("   • Esquemas JSON (validación de datos)")
            print("   • Queries SQL (transformaciones)")
            print("   • Configuración YAML (pipelines)")
            print("   • Configuración y temas")
            print("   • Módulos ETL (BD, Nómina, PDT, Licencias, etc.)")
            print("\n3. Para probar, ejecuta directamente el .exe desde la carpeta")
            print("=" * 60)
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
        
        print("\n" + "=" * 60)
        print(" CONFIGURACIÓN DEL EJECUTABLE ".center(60))
        print("=" * 60)
        print(f"📦 Nombre: {NOMBRE_EXE}")
        print(f"🎯 Entry Point: {MAIN_SCRIPT}")
        print(f"📂 Salida: {DIST_PATH}/")
        print("=" * 60 + "\n")
        
        confirm = input(f"¿Generar '{NOMBRE_EXE}' ahora? (S/N): ").lower()
        if confirm in ["s", "si", "y", "yes"]:
            generar_exe()
        else:
            print("❌ Cancelado por el usuario.")
            
    except KeyboardInterrupt:
        print("\n\n❌ Interrumpido por el usuario.")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()