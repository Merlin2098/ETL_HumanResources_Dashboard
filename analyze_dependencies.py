import os
import sys
import ast
import re
from pathlib import Path
from collections import defaultdict

try:
    import pathspec
except ImportError:
    print("Error: pathspec no está instalado")
    print("Ejecuta: pip install pathspec")
    sys.exit(1)


def cargar_gitignore(directorio):
    """
    Lee el .gitignore y retorna un objeto pathspec para matching.
    """
    gitignore_path = os.path.join(directorio, ".gitignore")
    
    if not os.path.exists(gitignore_path):
        return None
    
    with open(gitignore_path, "r", encoding="utf-8") as f:
        patterns = f.read().splitlines()
    
    patterns = [p for p in patterns if p.strip() and not p.startswith("#")]
    return pathspec.PathSpec.from_lines("gitwildmatch", patterns)


def obtener_archivos_python(raiz, gitignore_spec=None):
    """
    Obtiene todos los archivos .py del proyecto respetando .gitignore.
    """
    archivos_py = []
    
    for root, dirs, files in os.walk(raiz):
        # Filtrar directorios ignorados
        dirs[:] = [d for d in dirs if d not in {'.git', '__pycache__', '.pytest_cache'}]
        
        for file in files:
            if file.endswith('.py'):
                ruta_completa = os.path.join(root, file)
                ruta_relativa = os.path.relpath(ruta_completa, raiz).replace(os.sep, "/")
                
                # Verificar gitignore
                if gitignore_spec and gitignore_spec.match_file(ruta_relativa):
                    continue
                
                archivos_py.append(ruta_completa)
    
    return archivos_py


def analizar_imports(archivo, raiz_proyecto):
    """
    Analiza los imports de un archivo Python y retorna:
    - imports locales (módulos del proyecto)
    - imports de librerías externas
    """
    imports_locales = []
    imports_externos = []
    
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        tree = ast.parse(contenido, filename=archivo)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    modulo = alias.name.split('.')[0]
                    imports_externos.append(modulo)
            
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    modulo = node.module.split('.')[0]
                    
                    # Verificar si es import relativo
                    if node.level > 0:
                        imports_locales.append(modulo if modulo else ".")
                    else:
                        # Verificar si el módulo existe en el proyecto
                        posible_archivo = raiz_proyecto / f"{modulo}.py"
                        posible_paquete = raiz_proyecto / modulo / "__init__.py"
                        
                        if posible_archivo.exists() or posible_paquete.exists():
                            imports_locales.append(modulo)
                        else:
                            imports_externos.append(modulo)
    
    except Exception as e:
        # Silenciar errores de parsing
        pass
    
    return imports_locales, imports_externos


def analizar_archivos_configuracion(archivo):
    """
    Analiza si el archivo Python accede a archivos de configuración.
    Busca patrones como: open(), json.load(), yaml.load(), pd.read_csv(), etc.
    """
    archivos_config = []
    
    # Extensiones de configuración comunes
    extensiones_config = {'.json', '.yaml', '.yml', '.sql', '.txt', '.csv', '.ini', '.toml', '.env'}
    
    try:
        with open(archivo, 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        # Buscar patrones de acceso a archivos
        # Patrón: open("archivo.ext"), Path("archivo.ext"), "archivo.ext"
        patrones = [
            r'open\s*\(\s*["\']([^"\']+)["\']',
            r'Path\s*\(\s*["\']([^"\']+)["\']',
            r'read_(?:csv|json|sql|excel|parquet)\s*\(\s*["\']([^"\']+)["\']',
            r'load\s*\(\s*["\']([^"\']+)["\']',
            r'["\']([^"\']+\.(?:json|yaml|yml|sql|txt|csv|ini|toml|env))["\']',
        ]
        
        for patron in patrones:
            coincidencias = re.findall(patron, contenido, re.IGNORECASE)
            for coincidencia in coincidencias:
                # Filtrar solo archivos con extensiones de configuración
                if any(coincidencia.endswith(ext) for ext in extensiones_config):
                    # Normalizar ruta
                    archivo_normalizado = coincidencia.replace('\\', '/').split('/')[-1]
                    archivos_config.append(archivo_normalizado)
    
    except Exception as e:
        pass
    
    return list(set(archivos_config))  # Eliminar duplicados


def construir_grafo_dependencias(raiz_proyecto, gitignore_spec=None):
    """
    Construye el grafo completo de dependencias del proyecto.
    """
    archivos_py = obtener_archivos_python(raiz_proyecto, gitignore_spec)
    
    grafo = defaultdict(lambda: {
        'imports_locales': [],
        'imports_externos': [],
        'archivos_config': []
    })
    
    # Mapeo de rutas absolutas a nombres relativos
    nombres_modulos = {}
    for archivo in archivos_py:
        nombre_relativo = os.path.relpath(archivo, raiz_proyecto)
        nombre_modulo = nombre_relativo.replace(os.sep, '.').replace('.py', '')
        nombres_modulos[archivo] = nombre_modulo
    
    # Analizar cada archivo
    for archivo in archivos_py:
        nombre_modulo = nombres_modulos[archivo]
        
        imports_locales, imports_externos = analizar_imports(archivo, raiz_proyecto)
        archivos_config = analizar_archivos_configuracion(archivo)
        
        grafo[nombre_modulo]['imports_locales'] = imports_locales
        grafo[nombre_modulo]['imports_externos'] = list(set(imports_externos))
        grafo[nombre_modulo]['archivos_config'] = archivos_config
    
    return grafo


def generar_arbol_ascii(grafo, modulo, nivel=0, visitados=None, prefijo="", mostrar_externos=False):
    """
    Genera un árbol ASCII de las dependencias de un módulo específico.
    """
    if visitados is None:
        visitados = set()
    
    if modulo in visitados:
        return []
    
    visitados.add(modulo)
    lineas = []
    
    if modulo not in grafo:
        return lineas
    
    deps = grafo[modulo]
    
    # Dependencias locales (otros módulos)
    imports_locales = deps['imports_locales']
    archivos_config = deps['archivos_config']
    imports_externos = deps['imports_externos'] if mostrar_externos else []
    
    elementos = []
    
    # Agregar imports locales
    for imp in imports_locales:
        elementos.append(('modulo', imp))
    
    # Agregar archivos de configuración
    for archivo in archivos_config:
        elementos.append(('config', archivo))
    
    # Agregar imports externos (opcional)
    for imp in imports_externos:
        elementos.append(('externo', imp))
    
    for i, (tipo, nombre) in enumerate(elementos):
        es_ultimo = i == len(elementos) - 1
        conector = "└── " if es_ultimo else "├── "
        
        if tipo == 'modulo':
            lineas.append(f"{prefijo}{conector}📦 {nombre}")
            extension = "    " if es_ultimo else "│   "
            lineas.extend(generar_arbol_ascii(grafo, nombre, nivel + 1, visitados, prefijo + extension, mostrar_externos))
        
        elif tipo == 'config':
            lineas.append(f"{prefijo}{conector}📄 {nombre}")
        
        elif tipo == 'externo':
            lineas.append(f"{prefijo}{conector}🔗 {nombre}")
    
    return lineas


def generar_reporte_markdown(grafo, raiz_proyecto, archivo_salida="dependencies_report.md"):
    """
    Generates a Markdown report with the project's dependency analysis.
    Format optimized for use as context by LLMs.
    """
    contenido = []
    contenido.append("# Project Dependency Analysis\n\n")
    contenido.append("> **Purpose**: This document maps dependencies between Python modules, configuration files, and external libraries. Use it to understand the architecture and relationships between components.\n\n")
    
    # Identify root modules (not imported by anyone)
    todos_modulos = set(grafo.keys())
    modulos_importados = set()
    
    for deps in grafo.values():
        modulos_importados.update(deps['imports_locales'])
    
    modulos_raiz = sorted(todos_modulos - modulos_importados)
    
    # General statistics
    total_archivos_config = set()
    total_libs_externas = set()
    for deps in grafo.values():
        total_archivos_config.update(deps['archivos_config'])
        total_libs_externas.update(deps['imports_externos'])
    
    contenido.append("## Executive Summary\n\n")
    contenido.append(f"- **Total Python modules**: {len(grafo)}\n")
    contenido.append(f"- **Project entry points**: {len(modulos_raiz)}\n")
    contenido.append(f"- **Configuration files**: {len(total_archivos_config)}\n")
    contenido.append(f"- **Unique external libraries**: {len(total_libs_externas)}\n\n")
    contenido.append("---\n\n")
    
    # Section 1: Entry Points details
    contenido.append("## 1. Project Entry Points\n\n")
    contenido.append("These modules are the **main scripts** that initiate execution (they are not imported by other modules):\n\n")
    
    for modulo in modulos_raiz:
        deps = grafo[modulo]
        contenido.append(f"### `{modulo}`\n\n")
        
        n_locales = len(deps['imports_locales'])
        n_config = len(deps['archivos_config'])
        n_externos = len(deps['imports_externos'])
        
        contenido.append(f"**Direct dependencies**: {n_locales + n_config + n_externos} ({n_locales} modules, {n_config} configs, {n_externos} libraries)\n\n")
        
        if deps['imports_locales']:
            contenido.append(f"- **Internal modules**: {', '.join([f'`{m}`' for m in deps['imports_locales']])}\n")
        if deps['archivos_config']:
            contenido.append(f"- **Config files**: {', '.join([f'`{a}`' for a in deps['archivos_config']])}\n")
        if deps['imports_externos']:
            libs_principales = deps['imports_externos'][:5]
            resto = len(deps['imports_externos']) - 5
            libs_str = ', '.join([f'`{lib}`' for lib in libs_principales])
            if resto > 0:
                libs_str += f" (+{resto} more)"
            contenido.append(f"- **External libraries**: {libs_str}\n")
        
        contenido.append("\n")
    
    contenido.append("---\n\n")
    
    # Section 2: Dependency Map
    contenido.append("## 2. Full Dependency Map\n\n")
    contenido.append("This tree shows **all recursive dependencies** for each entry point:\n\n")
    contenido.append("**Legend**:\n")
    contenido.append("- 📦 Project Python Module\n")
    contenido.append("- 📄 Configuration File (JSON, YAML, SQL, etc.)\n")
    contenido.append("- 🔗 External Library (installed via pip)\n\n")
    
    for modulo in modulos_raiz:
        contenido.append(f"### {modulo}\n\n")
        contenido.append("```\n")
        contenido.append(f"{modulo}\n")
        
        arbol = generar_arbol_ascii(grafo, modulo, mostrar_externos=True)
        contenido.append("\n".join(arbol))
        contenido.append("\n```\n\n")
    
    contenido.append("---\n\n")
    
    # Section 3: Modules Index
    contenido.append("## 3. All Modules Index\n\n")
    contenido.append("Tabular view of all modules and their dependency counts:\n\n")
    contenido.append("| Module | Type | Local Deps. | Config Files | External Libs |\n")
    contenido.append("|--------|------|---------------|-----------------|---------------|\n")
    
    for modulo in sorted(grafo.keys()):
        deps = grafo[modulo]
        tipo = "Entry Point" if modulo in modulos_raiz else "Imported"
        
        n_locales = len(deps['imports_locales'])
        n_config = len(deps['archivos_config'])
        n_externos = len(deps['imports_externos'])
        
        contenido.append(f"| {modulo} | {tipo} | {n_locales} | {n_config} | {n_externos} |\n")
    
    contenido.append("\n---\n\n")
    
    # Section 4: Configuration Files
    archivos_config_totales = set()
    for deps in grafo.values():
        archivos_config_totales.update(deps['archivos_config'])
    
    if archivos_config_totales:
        contenido.append("## 4. Configuration Files\n\n")
        contenido.append("Data/configuration files detected in code and modules using them:\n\n")
        
        for archivo in sorted(archivos_config_totales):
            modulos_que_usan = [mod for mod, deps in grafo.items() if archivo in deps['archivos_config']]
            contenido.append(f"- **`{archivo}`** → Used by: {', '.join([f'`{m}`' for m in modulos_que_usan])}\n")
        
        contenido.append("\n")
    
    # Final notes
    contenido.append("---\n\n")
    contenido.append("## Notes\n\n")
    contenido.append("- This file is **automatically generated** via a pre-commit hook.\n")
    contenido.append("- Imports are detected through static analysis (AST) of Python code.\n")
    contenido.append("- Configuration files are detected via regex of common patterns (`open()`, `read_csv()`, etc.).\n")
    contenido.append("- Circular dependencies might cause some modules to be missing from the full tree.\n")
    
    # Save file
    try:
        with open(archivo_salida, "w", encoding="utf-8") as f:
            f.writelines(contenido)
        print(f"Report generated: {archivo_salida}")
        return True
    except Exception as e:
        print(f"Error generating report: {e}")
        return False

def main():
    raiz = Path(__file__).parent
    
    print("Analizando estructura del proyecto...")
    
    # Cargar .gitignore
    gitignore_spec = cargar_gitignore(raiz)
    if gitignore_spec:
        print("Patrones de .gitignore cargados correctamente")
    else:
        print("Advertencia: No se encontró .gitignore")
    
    # Construir grafo de dependencias
    grafo = construir_grafo_dependencias(raiz, gitignore_spec)
    
    if not grafo:
        print("No se encontraron módulos Python en el proyecto")
        return 1
    
    print(f"Se analizaron {len(grafo)} módulos Python")
    
    # Generar reporte
    archivo_salida = raiz / "dependencies_report.md"
    exito = generar_reporte_markdown(grafo, raiz, str(archivo_salida))
    
    return 0 if exito else 1


if __name__ == "__main__":
    sys.exit(main())