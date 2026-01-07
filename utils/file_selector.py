"""
Selector interactivo de archivos con integración de cache
Proporciona diálogos intuitivos para selección de archivos usando Questionary
"""
from pathlib import Path
from typing import Optional, List, Tuple
import questionary
from questionary import Style
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .path_cache import get_path_cache
from .logger import PipelineLogger

console = Console()

# Estilo personalizado para Questionary
custom_style = Style([
    ('qmark', 'fg:#673ab7 bold'),
    ('question', 'bold'),
    ('answer', 'fg:#00bcd4 bold'),
    ('pointer', 'fg:#673ab7 bold'),
    ('highlighted', 'fg:#673ab7 bold'),
    ('selected', 'fg:#00bcd4'),
    ('separator', 'fg:#cc5454'),
    ('instruction', ''),
    ('text', ''),
])


class FileSelector:
    """
    Selector interactivo de archivos con cache y validación
    """
    
    def __init__(
        self,
        cache_key: str,
        allowed_extensions: Optional[List[str]] = None,
        logger: Optional[PipelineLogger] = None
    ):
        """
        Inicializa el selector de archivos
        
        Args:
            cache_key: Key para identificar el contexto en el cache
            allowed_extensions: Lista de extensiones permitidas (ej: ['.xlsx', '.csv'])
            logger: Logger opcional para registrar operaciones
        """
        self.cache_key = cache_key
        self.allowed_extensions = allowed_extensions or ['.xlsx', '.xls', '.csv']
        self.cache = get_path_cache()
        self.logger = logger
    
    def select_file(
        self,
        prompt: str = "Selecciona el archivo a procesar",
        allow_manual_path: bool = True,
        must_exist: bool = True
    ) -> Optional[Path]:
        """
        Selector interactivo de archivos con opciones de cache
        
        Args:
            prompt: Mensaje a mostrar al usuario
            allow_manual_path: Permitir ingreso manual de ruta
            must_exist: El archivo debe existir
        
        Returns:
            Path del archivo seleccionado o None si se cancela
        """
        while True:
            # Construir opciones del menú
            choices = self._build_file_choices()
            
            if not choices:
                # No hay cache, ir directo a ingreso manual
                return self._manual_file_input(must_exist)
            
            # Mostrar selector
            selection = questionary.select(
                prompt,
                choices=choices,
                style=custom_style,
                use_indicator=True,
                use_shortcuts=True
            ).ask()
            
            if selection is None:
                return None  # Usuario canceló
            
            # Procesar selección
            if selection == "🔍 Buscar archivo manualmente...":
                file_path = self._manual_file_input(must_exist)
                if file_path:
                    return file_path
                continue  # Volver al menú si no seleccionó nada
            
            elif selection == "📁 Explorar directorio frecuente...":
                file_path = self._browse_frequent_dir()
                if file_path:
                    return file_path
                continue
            
            elif selection == "❌ Cancelar":
                return None
            
            else:
                # Seleccionó un archivo del cache
                file_path = self._extract_path_from_choice(selection)
                if file_path and file_path.exists():
                    return file_path
                else:
                    console.print("[yellow]⚠ El archivo ya no existe en esa ubicación[/yellow]")
                    self.cache.clear_key(self.cache_key)
                    continue
    
    def select_directory(
        self,
        prompt: str = "Selecciona el directorio",
        must_exist: bool = True
    ) -> Optional[Path]:
        """
        Selector interactivo de directorios
        
        Args:
            prompt: Mensaje a mostrar
            must_exist: El directorio debe existir
        
        Returns:
            Path del directorio o None si se cancela
        """
        while True:
            choices = self._build_dir_choices()
            
            if not choices:
                return self._manual_dir_input(must_exist)
            
            selection = questionary.select(
                prompt,
                choices=choices,
                style=custom_style
            ).ask()
            
            if selection is None or selection == "❌ Cancelar":
                return None
            
            elif selection == "🔍 Ingresar ruta manualmente...":
                dir_path = self._manual_dir_input(must_exist)
                if dir_path:
                    return dir_path
                continue
            
            else:
                dir_path = self._extract_path_from_choice(selection)
                if dir_path and dir_path.exists():
                    return dir_path
                else:
                    console.print("[yellow]⚠ El directorio ya no existe[/yellow]")
                    continue
    
    def _build_file_choices(self) -> List[str]:
        """
        Construye lista de opciones para selector de archivos
        """
        choices = []
        
        # Última ruta usada
        last_path = self.cache.get_last_path(self.cache_key)
        if last_path and last_path.exists():
            ext = last_path.suffix
            size_mb = last_path.stat().st_size / (1024 * 1024)
            choices.append(f"📄 Último usado: {last_path.name} ({size_mb:.1f} MB) - {ext}")
        
        # Directorios frecuentes (solo mostrar si hay)
        frequent_dirs = self.cache.get_frequent_dirs(self.cache_key, limit=3)
        if frequent_dirs:
            choices.append("📁 Explorar directorio frecuente...")
        
        # Opciones adicionales
        choices.extend([
            "🔍 Buscar archivo manualmente...",
            "❌ Cancelar"
        ])
        
        return choices
    
    def _build_dir_choices(self) -> List[str]:
        """
        Construye lista de opciones para selector de directorios
        """
        choices = []
        
        # Directorios frecuentes
        frequent_dirs = self.cache.get_frequent_dirs(self.cache_key, limit=5)
        for i, dir_path in enumerate(frequent_dirs, 1):
            choices.append(f"📁 {dir_path}")
        
        # Opciones adicionales
        choices.extend([
            "🔍 Ingresar ruta manualmente...",
            "❌ Cancelar"
        ])
        
        return choices
    
    def _browse_frequent_dir(self) -> Optional[Path]:
        """
        Permite explorar archivos dentro de directorios frecuentes
        """
        frequent_dirs = self.cache.get_frequent_dirs(self.cache_key, limit=5)
        
        if not frequent_dirs:
            return None
        
        # Seleccionar directorio
        dir_choices = [f"📁 {d}" for d in frequent_dirs] + ["⬅️ Volver"]
        
        dir_selection = questionary.select(
            "Selecciona un directorio frecuente:",
            choices=dir_choices,
            style=custom_style
        ).ask()
        
        if dir_selection is None or dir_selection == "⬅️ Volver":
            return None
        
        selected_dir = self._extract_path_from_choice(dir_selection)
        
        # Listar archivos en el directorio
        return self._browse_files_in_dir(selected_dir)
    
    def _browse_files_in_dir(self, directory: Path) -> Optional[Path]:
        """
        Explora y selecciona archivos dentro de un directorio
        """
        try:
            # Filtrar por extensiones permitidas
            files = []
            for ext in self.allowed_extensions:
                files.extend(directory.glob(f"*{ext}"))
            
            files = sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)
            
            if not files:
                console.print(f"[yellow]No se encontraron archivos {', '.join(self.allowed_extensions)} en el directorio[/yellow]")
                return None
            
            # Crear opciones con información del archivo
            file_choices = []
            for file in files[:20]:  # Limitar a 20 archivos
                size_mb = file.stat().st_size / (1024 * 1024)
                file_choices.append(f"📄 {file.name} ({size_mb:.1f} MB)")
            
            file_choices.append("⬅️ Volver")
            
            file_selection = questionary.select(
                f"Archivos en {directory.name}:",
                choices=file_choices,
                style=custom_style
            ).ask()
            
            if file_selection is None or file_selection == "⬅️ Volver":
                return None
            
            # Extraer nombre de archivo y construir path
            filename = file_selection.split(" ", 1)[1].split(" (")[0]
            return directory / filename
            
        except Exception as e:
            console.print(f"[red]Error al explorar directorio: {e}[/red]")
            return None
    
    def _manual_file_input(self, must_exist: bool = True) -> Optional[Path]:
        """
        Permite ingresar ruta de archivo manualmente
        """
        while True:
            path_input = questionary.path(
                "Ingresa la ruta del archivo:",
                style=custom_style,
                only_directories=False
            ).ask()
            
            if path_input is None:
                return None
            
            file_path = Path(path_input)
            
            # Validar existencia
            if must_exist and not file_path.exists():
                console.print("[red]✗ El archivo no existe[/red]")
                retry = questionary.confirm("¿Reintentar?", default=True).ask()
                if not retry:
                    return None
                continue
            
            # Validar extensión
            if self.allowed_extensions and file_path.suffix not in self.allowed_extensions:
                console.print(f"[red]✗ Extensión no permitida. Usa: {', '.join(self.allowed_extensions)}[/red]")
                retry = questionary.confirm("¿Reintentar?", default=True).ask()
                if not retry:
                    return None
                continue
            
            # Archivo válido
            self._update_cache(file_path)
            return file_path
    
    def _manual_dir_input(self, must_exist: bool = True) -> Optional[Path]:
        """
        Permite ingresar ruta de directorio manualmente
        """
        while True:
            path_input = questionary.path(
                "Ingresa la ruta del directorio:",
                style=custom_style,
                only_directories=True
            ).ask()
            
            if path_input is None:
                return None
            
            dir_path = Path(path_input)
            
            if must_exist and not dir_path.exists():
                console.print("[red]✗ El directorio no existe[/red]")
                retry = questionary.confirm("¿Reintentar?", default=True).ask()
                if not retry:
                    return None
                continue
            
            if must_exist and not dir_path.is_dir():
                console.print("[red]✗ La ruta no es un directorio[/red]")
                retry = questionary.confirm("¿Reintentar?", default=True).ask()
                if not retry:
                    return None
                continue
            
            self._update_cache_dir(dir_path)
            return dir_path
    
    def _extract_path_from_choice(self, choice: str) -> Optional[Path]:
        """
        Extrae Path desde una opción del menú
        """
        # Remover emojis y metadata
        if ":" in choice:
            # Formato: "📄 Último usado: archivo.xlsx (10.5 MB) - .xlsx"
            parts = choice.split(":", 1)[1].strip()
            filename = parts.split("(")[0].strip()
            
            last_path = self.cache.get_last_path(self.cache_key)
            if last_path and last_path.name == filename:
                return last_path
        
        elif choice.startswith("📁"):
            # Formato: "📁 C:/path/to/dir"
            path_str = choice.replace("📁", "").strip()
            return Path(path_str)
        
        return None
    
    def _update_cache(self, file_path: Path):
        """
        Actualiza cache con archivo seleccionado
        """
        self.cache.set_last_path(self.cache_key, file_path)
        self.cache.add_to_frequent(self.cache_key, file_path)
        
        if self.logger:
            self.logger.debug(f"Cache actualizado para '{self.cache_key}': {file_path}")
    
    def _update_cache_dir(self, dir_path: Path):
        """
        Actualiza cache con directorio seleccionado
        """
        self.cache.add_to_frequent(self.cache_key, dir_path)
        
        if self.logger:
            self.logger.debug(f"Cache de directorio actualizado para '{self.cache_key}': {dir_path}")
    
    def show_cache_info(self):
        """
        Muestra información del cache para este contexto
        """
        table = Table(title=f"Cache Info: {self.cache_key}", show_header=True)
        table.add_column("Tipo", style="cyan")
        table.add_column("Valor", style="white")
        
        # Última ruta
        last_path = self.cache.get_last_path(self.cache_key)
        table.add_row(
            "Última ruta",
            str(last_path) if last_path else "[dim]No disponible[/dim]"
        )
        
        # Directorios frecuentes
        frequent = self.cache.get_frequent_dirs(self.cache_key, limit=5)
        if frequent:
            for i, dir_path in enumerate(frequent, 1):
                table.add_row(
                    f"Frecuente #{i}",
                    str(dir_path)
                )
        
        console.print(table)


def quick_file_select(
    cache_key: str,
    prompt: str = "Selecciona un archivo",
    allowed_extensions: Optional[List[str]] = None,
    logger: Optional[PipelineLogger] = None
) -> Optional[Path]:
    """
    Función helper para selección rápida de archivos
    
    Args:
        cache_key: Identificador del contexto
        prompt: Mensaje para el usuario
        allowed_extensions: Extensiones permitidas
        logger: Logger opcional
    
    Returns:
        Path del archivo o None
    """
    selector = FileSelector(
        cache_key=cache_key,
        allowed_extensions=allowed_extensions,
        logger=logger
    )
    return selector.select_file(prompt=prompt)


def quick_dir_select(
    cache_key: str,
    prompt: str = "Selecciona un directorio",
    logger: Optional[PipelineLogger] = None
) -> Optional[Path]:
    """
    Función helper para selección rápida de directorios
    
    Args:
        cache_key: Identificador del contexto
        prompt: Mensaje para el usuario
        logger: Logger opcional
    
    Returns:
        Path del directorio o None
    """
    selector = FileSelector(
        cache_key=cache_key,
        logger=logger
    )
    return selector.select_directory(prompt=prompt)