import os
import sys

try:
    import pathspec
except ImportError:
    print("Error: pathspec no esta instalado")
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
    
    # Filtrar líneas vacías y comentarios
    patterns = [p for p in patterns if p.strip() and not p.startswith("#")]
    
    return pathspec.PathSpec.from_lines("gitwildmatch", patterns)


def generar_arbol(directorio, prefijo="", excluidos=None, carpetas_ignoradas=None, gitignore_spec=None, raiz_proyecto=None):
    """
    Genera la estructura tipo árbol del proyecto.
    """
    if excluidos is None:
        excluidos = set()
    if carpetas_ignoradas is None:
        carpetas_ignoradas = set()
    if raiz_proyecto is None:
        raiz_proyecto = directorio

    contenido = []

    try:
        elementos = sorted(os.listdir(directorio))
    except PermissionError:
        return contenido

    # Filtrar excluidos básicos
    elementos = [
        e for e in elementos
        if e not in excluidos
        and e not in carpetas_ignoradas
        and not e.endswith(".spec")
    ]

    for index, nombre in enumerate(elementos):
        ruta = os.path.join(directorio, nombre)
        
        # Calcular ruta relativa para gitignore matching
        if gitignore_spec:
            ruta_relativa = os.path.relpath(ruta, raiz_proyecto)
            # Normalizar separadores para Windows
            ruta_relativa = ruta_relativa.replace(os.sep, "/")
            
            # Si está en gitignore, saltar
            if gitignore_spec.match_file(ruta_relativa):
                continue
            
            # Si es directorio, también verificar con trailing slash
            if os.path.isdir(ruta):
                if gitignore_spec.match_file(ruta_relativa + "/"):
                    continue
        
        es_ultimo = index == len([
            e for e in elementos 
            if e not in excluidos 
            and e not in carpetas_ignoradas 
            and not e.endswith(".spec")
        ]) - 1
        
        conector = "└── " if es_ultimo else "├── "

        if os.path.isdir(ruta):
            contenido.append(f"{prefijo}{conector}{nombre}/")
            extension = "    " if es_ultimo else "│   "
            contenido.extend(
                generar_arbol(ruta, prefijo + extension, excluidos, carpetas_ignoradas, gitignore_spec, raiz_proyecto)
            )
        else:
            contenido.append(f"{prefijo}{conector}{nombre}")

    return contenido


def main():
    raiz = os.path.dirname(os.path.abspath(__file__))

    # Exclusiones manuales adicionales (mínimas, .gitignore hace el resto)
    script_actual = os.path.basename(__file__)
    excluidos = {
        script_actual,
    }

    # Carpetas siempre ignoradas (seguridad adicional)
    carpetas_ignoradas = {
        ".git",
    }

    # Cargar patrones de .gitignore
    gitignore_spec = cargar_gitignore(raiz)
    
    if gitignore_spec:
        print("Patrones de .gitignore cargados correctamente")
    else:
        print("Advertencia: No se encontro .gitignore, procesando todos los archivos")

    # Generar árbol
    arbol = generar_arbol(
        raiz, 
        excluidos=excluidos, 
        carpetas_ignoradas=carpetas_ignoradas,
        gitignore_spec=gitignore_spec,
        raiz_proyecto=raiz
    )
    salida = "\n".join(arbol)

    # Guardar en treemap.md
    archivo_salida = os.path.join(raiz, "treemap.md")
    existe = os.path.exists(archivo_salida)
    
    with open(archivo_salida, "w", encoding="utf-8") as f:
        f.write("## Estructura del Proyecto\n\n```\n")
        f.write(salida)
        f.write("\n```\n")

    if existe:
        print("treemap.md actualizado correctamente")
    else:
        print("treemap.md creado por primera vez")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())