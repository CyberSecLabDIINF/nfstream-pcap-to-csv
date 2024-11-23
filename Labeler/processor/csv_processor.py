import pandas as pd
import json
from pathlib import Path
from typing import Optional, Dict, List
from config.config import ProcessingConfig


class CSVProcessor:
    def __init__(self, config: ProcessingConfig, logger):
        self.config = config
        self.logger = logger
        self.processing_config = None

        # Asignar valor predeterminado solo si no existe labels_dir
        if not hasattr(self.config, 'labels_dir'):
            self.logger.warning("labels_dir no está definido en la configuración, usando valor por defecto")
            self.config.labels_dir = Path("/data/labels/")

        # Construir label_path a partir de labels_dir si existe
        if hasattr(self.config, 'labels_dir'):
            self.config.label_path = self.config.labels_dir / "Keylogging.csv"
            self.logger.info(f"Label path configurado en: {self.config.label_path}")

    def initialize(self) -> bool:
        """Inicializa el procesador cargando la configuración necesaria."""
        self._log_initialization_details()
        self.processing_config = self._load_config()
        return self.processing_config is not None

    def _log_initialization_details(self) -> None:
        """Registra detalles iniciales sobre la configuración."""
        self.logger.info(f"Archivo de datos: {self.config.data_path}")
        self.logger.info(f"Directorio de etiquetas: {self.config.labels_dir}")
        self.logger.info(f"Archivo de configuración: {self.config.config_path}")

    def _load_config(self) -> Optional[Dict]:
        """Carga la configuración desde el archivo JSON."""
        try:
            with open(self.config.config_path, 'r') as f:
                config = json.load(f)
            self.logger.info("Configuración cargada exitosamente")
            return config
        except Exception as e:
            self.logger.error(f"Error al cargar la configuración: {str(e)}")
            return None

    def _get_label_list(self) -> List[str]:
        """Obtiene la lista de etiquetas disponibles."""
        if not hasattr(self.config, 'labels_dir'):
            self.logger.error("labels_dir no está definido en la configuración")
            return []

        if not self.config.labels_dir.exists():
            self.logger.error(f"El directorio de etiquetas no existe: {self.config.labels_dir}")
            return []
        return [f.stem for f in self.config.labels_dir.glob("*.csv")]

    def _determine_label(self, label_list: List[str]) -> Optional[str]:
        """Determina la etiqueta correspondiente al archivo de datos."""
        if not hasattr(self.config, 'data_path'):
            self.logger.error("data_path no está definido en la configuración")
            return None

        file_name = self.config.data_path.name.lower()
        label_name = self._check_label_exceptions(file_name)

        if label_name:
            return label_name

        matching_labels = self._find_matching_labels(file_name, label_list)

        if not matching_labels:
            self.logger.warning(f"No se encontró una etiqueta para el archivo: {self.config.data_path.name}")
            return None

        if len(matching_labels) > 1:
            self.logger.warning(f"Se encontraron múltiples etiquetas para {file_name}: {matching_labels}")

        return matching_labels[0]

    def _check_label_exceptions(self, file_name: str) -> Optional[str]:
        """Verifica excepciones específicas de etiquetas en los nombres de archivo."""
        label_exceptions = {
            "data_theft": "Data_exfiltration",
            "keylog": "Keylogging",
        }

        for exception, label in label_exceptions.items():
            if exception.lower() in file_name:
                self.logger.info(f"Excepción detectada: {file_name} asignado a la etiqueta '{label}'")
                return label
        return None

    def _find_matching_labels(self, file_name: str, label_list: List[str]) -> List[str]:
        """Encuentra etiquetas en la lista que coincidan con el nombre del archivo."""
        return [label for label in label_list if label.lower() in file_name]

    def _load_label_file(self, label_path: Path) -> Optional[Dict]:
        """Carga las etiquetas desde un archivo CSV."""
        if not label_path.exists():
            self.logger.error(f"El archivo de etiquetas no existe: {label_path}")
            return None

        if not self.processing_config:
            self.logger.error("processing_config no está inicializado")
            return None

        try:
            label_config = self.processing_config['file_config']['label_file']
            chunk_iterator = self._create_chunk_iterator(label_path, label_config)
            label_dict = self._build_label_dictionary(chunk_iterator, label_config)

            self.logger.info(f"Archivo de etiquetas cargado: {label_path}")
            return label_dict
        except Exception as e:
            self.logger.error(f"Error al cargar el archivo de etiquetas {label_path}: {str(e)}")
            return None

    def _create_chunk_iterator(self, file_path: Path, config: Dict) -> pd.io.parsers.TextFileReader:
        """Crea un iterador para leer el archivo en chunks."""
        return pd.read_csv(
            file_path,
            sep=config['separator'],
            chunksize=config['chunk_size'],
            usecols=config['columns'],
            encoding='utf-8'
        )

    def _build_label_dictionary(self, chunk_iterator: pd.io.parsers.TextFileReader, config: Dict) -> Dict:
        """Construye un diccionario de etiquetas a partir de los chunks leídos."""
        label_dict = {}
        src_col, dst_col = config['key_columns']['source_ip'], config['key_columns']['destination_ip']
        label_columns = config['label_columns']

        for chunk in chunk_iterator:
            for _, row in chunk.iterrows():
                key = self._build_label_key(row, src_col, dst_col)
                if key:
                    label_dict[key] = {
                        col_name: row[col]
                        for col_name, col in label_columns.items()
                    }
        return label_dict

    def _build_label_key(self, row: pd.Series, src_col: str, dst_col: str) -> Optional[tuple]:
        """Construye una clave para el diccionario de etiquetas."""
        src_ip = str(row[src_col]).strip('"').strip("'") if not pd.isna(row[src_col]) else None
        dst_ip = str(row[dst_col]).strip('"').strip("'") if not pd.isna(row[dst_col]) else None

        if src_ip and dst_ip:
            return (src_ip, dst_ip)
        return None

    def process(self) -> bool:
        """Ejecuta el proceso principal de etiquetado"""
        if not self.initialize():
            self.logger.error("Falló la inicialización")
            return False

        # 1. Obtener y validar etiquetas
        label_list = self._get_label_list()
        if not label_list:
            self.logger.error("No hay etiquetas disponibles para procesar.")
            return False

        # 2. Determinar la etiqueta correcta
        label_name = self._determine_label(label_list)
        if not label_name:
            self.logger.error("No se pudo determinar la etiqueta para el archivo.")
            return False

        # 3. Cargar y procesar los datos
        label_dict = self._load_label_file(self.config.label_path)
        if not label_dict:
            self.logger.error("No se pudo cargar el archivo de etiquetas.")
            return False

        processed_data = self._process_file(label_dict)
        if processed_data is None:
            return False

        self._save_output(processed_data)
        return True

    def _process_file(self, label_dict: Dict) -> Optional[pd.DataFrame]:
        """Procesa el archivo de datos en chunks."""
        if not hasattr(self.config, 'data_path') or not self.config.data_path.exists():
            self.logger.error(f"El archivo de datos no existe: {getattr(self.config, 'data_path', 'No definido')}")
            return None

        try:
            chunk_iterator = self._create_data_chunk_iterator()
            chunks = []
            for chunk in chunk_iterator:
                processed_chunk = self._process_chunk(chunk, label_dict)
                if not processed_chunk.empty:
                    chunks.append(processed_chunk)

            if not chunks:
                self.logger.error("No se pudieron procesar chunks del archivo.")
                return None

            result = pd.concat(chunks, ignore_index=True)
            self.logger.info(f"Archivo procesado exitosamente: {self.config.data_path}")
            return result
        except Exception as e:
            self.logger.error(f"Error al procesar el archivo {self.config.data_path}: {str(e)}")
            return None

    def _create_data_chunk_iterator(self) -> pd.io.parsers.TextFileReader:
        """Crea un iterador para leer el archivo de datos en chunks."""
        if not self.processing_config:
            raise ValueError("processing_config no está inicializado")

        data_config = self.processing_config['file_config']['data_file']
        dtype_dict = {col: str for col in self.processing_config['column_dtypes'].keys()}

        try:
            return pd.read_csv(
                self.config.data_path,
                sep=data_config['separator'],
                chunksize=data_config['chunk_size'],
                dtype=dtype_dict,
                low_memory=False,
                encoding='utf-8'
            )
        except Exception as e:
            self.logger.error(f"Error al crear el iterador de chunks para {self.config.data_path}: {str(e)}")
            raise

    def _save_output(self, processed_data: pd.DataFrame) -> None:
        """Guarda el DataFrame procesado en el archivo de salida."""
        if not hasattr(self.config, 'output_path'):
            self.logger.error("output_path no está definido en la configuración")
            return

        try:
            # Asegúrate de que el directorio exista
            self.config.output_path.parent.mkdir(parents=True, exist_ok=True)

            # Guardar en CSV
            processed_data.to_csv(
                self.config.output_path,
                index=False,
                sep=',',
                encoding='utf-8'
            )
            self.logger.info(f"Archivo procesado guardado exitosamente en: {self.config.output_path}")
        except Exception as e:
            self.logger.error(f"Error al guardar el archivo procesado: {str(e)}")