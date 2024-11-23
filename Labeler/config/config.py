from pathlib import Path
from dataclasses import dataclass

@dataclass
class ProcessingConfig:
    data_path: Path          # Ruta del archivo de datos
    labels_dir: Path         # Directorio de etiquetas
    output_path: Path        # Archivo de salida
    config_path: Path        # Archivo de configuración JSON
    debug: bool              # Modo debug activado o no
