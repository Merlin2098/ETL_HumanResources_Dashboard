# Skill: Input File Handler

## Responsibility
Manage input file selection and validation for scripts, avoiding hardcoded paths, ensuring proper configuration loading, and supporting both interface-based and standalone execution.

## Behavior

### 1. File Selection
- **Always use the File Explorer**: Leverage the system's file dialog or explorer capabilities to select input files dynamically.
- **No Hardcoded Paths**: Avoid embedding absolute or relative file paths directly in the code, except for default configuration locations.
- **Format Support**: Delegate to format-specific file explorer skills (e.g., for PDF, Excel, CSV, JSON, YAML) to handle the nuances of different file types.

### 2. Configuration Preloading
- **Search Location**: Prioritize searching for configuration files in the `project_root/config/` directory.
- **Validation**:
  - Check the file extension and format (YAML, JSON, SQL, etc.) before attempting to load.
  - Ensure the configuration file structure matches expected schemas.
- **Auto-Load**: Automatically parse and apply settings found in valid configuration files before prompting for or processing other input files.

### 3. Interface vs Standalone
- **Dual Mode Support**:
  - **Interface Mode**: If a graphical user interface (GUI) or interactive terminal is available, use it to prompt the user for file selection.
  - **Standalone Mode**: If running in a headless environment or automated script, accept input paths via command-line arguments or environment variables, or fallback to standard input specific to the OS file explorer integration if automation allows.
- **Seamless Integration**: Ensure the core logic for file handling remains the same regardless of the interaction mode.

## Benefits
- **Error Prevention**: Eliminates runtime errors caused by missing or moved files referenced by hardcoded paths.
- **Flexibility**: Facilitates easy testing, debugging, and automation without code modification.
- **Consistency**: Enforces a standard approach to configuration and input file loading across all scripts and agents.

## Usage Example

```python
import os
import sys
import tkinter as tk
from tkinter import filedialog
import json
import yaml

class InputFileHandler:
    def __init__(self, config_dir="config", interface_mode=True):
        self.project_root = os.getcwd()
        self.config_dir = os.path.join(self.project_root, config_dir)
        self.interface_mode = interface_mode
        self.config = {}

    def load_configuration(self):
        """Searches for and loads configuration files from the config directory."""
        if not os.path.exists(self.config_dir):
            print(f"Config: No config directory found at {self.config_dir}")
            return

        for filename in os.listdir(self.config_dir):
            file_path = os.path.join(self.config_dir, filename)
            if filename.endswith('.json'):
                try:
                    with open(file_path, 'r') as f:
                        self.config.update(json.load(f))
                    print(f"Loaded JSON config: {filename}")
                except json.JSONDecodeError:
                    print(f"Error decoding JSON: {filename}")
            elif filename.endswith(('.yaml', '.yml')):
                try:
                    with open(file_path, 'r') as f:
                        self.config.update(yaml.safe_load(f))
                    print(f"Loaded YAML config: {filename}")
                except yaml.YAMLError:
                    print(f"Error decoding YAML: {filename}")

    def select_file(self, file_types=[("All Files", "*.*")]):
        """
        Selects a file using a file dialog in interface mode, 
        or raises an error/expects an arg in standalone mode.
        """
        if self.interface_mode:
            root = tk.Tk()
            root.withdraw()  # Hide the main window
            file_path = filedialog.askopenfilename(title="Select Input File", filetypes=file_types)
            if not file_path:
                print("No file selected.")
                return None
            return file_path
        else:
            # Standalone mode: Expect file path as a command line argument
            if len(sys.argv) > 1:
                return sys.argv[1]
            else:
                # Could also check environment variables here
                env_path = os.environ.get("INPUT_FILE_PATH")
                if env_path:
                    return env_path
                raise ValueError("Standalone mode requires a file path argument or INPUT_FILE_PATH env var.")

# Usage
if __name__ == "__main__":
    # Example usage
    handler = InputFileHandler(interface_mode=True) # Set to False for headless
    handler.load_configuration()
    print("Current Configuration:", handler.config)
    
    try:
        selected_file = handler.select_file(file_types=[("JSON Files", "*.json"), ("Text Files", "*.txt")])
        if selected_file:
            print(f"Processing file: {selected_file}")
    except Exception as e:
        print(f"Error: {e}")
```
