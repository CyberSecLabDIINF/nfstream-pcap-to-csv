from pathlib import Path
from dataclasses import dataclass

@dataclass
class ProcessingConfig:
    data_path: Path          # Ruta del archivo de datos
    labels_dir: Path         # Directorio de etiquetas
    output_path: Path        # Archivo de salida
    config_path: Path        # Archivo de configuración JSON
    debug: bool              # Modo debug activado o no

    def __post_init__(self):
        # Validar que los paths sean objetos Path
        self.data_path = Path(self.data_path)
        self.labels_dir = Path(self.labels_dir)
        self.output_path = Path(self.output_path)
        self.config_path = Path(self.config_path)