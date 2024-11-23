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

    def initialize(self) -> bool:
        """Inicializa el procesador cargando la configuración necesaria"""
        self.logger.info(f"Archivo de datos: {self.config.data_path}")
        self.logger.info(f"Directorio de etiquetas: {self.config.labels_dir}")
        self.logger.info(f"Archivo de configuración: {self.config.config_path}")

        self.processing_config = self._load_config()
        return self.processing_config is not None

    def _load_config(self) -> Optional[Dict]:
        """Carga la configuración desde el archivo JSON"""
        try:
            with open(self.config.config_path, 'r') as f:
                config = json.load(f)
            self.logger.info("Configuración cargada exitosamente")
            return config
        except Exception as e:
            self.logger.error(f"Error al cargar la configuración: {str(e)}")
            return None

    def process(self) -> bool:
        """Ejecuta el proceso principal de etiquetado"""
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
        success = self._process_data(label_name)
        return success

    def _get_label_list(self) -> List[str]:
        """Obtiene la lista de etiquetas disponibles"""
        if not self.config.labels_dir.exists():
            self.logger.error(f"El directorio de etiquetas no existe: {self.config.labels_dir}")
            return []
        return [f.stem for f in self.config.labels_dir.glob("*.csv")]

    def _determine_label(self, label_list: List[str]) -> Optional[str]:
        """Determina la etiqueta correspondiente al archivo"""
        file_name = self.config.data_path.name
        label_exceptions = {
            "data_theft": "Data_exfiltration",
            "keylog": "Keylogging",
        }

        file_name_lower = file_name.lower()

        # Verificar excepciones
        for exception, label in label_exceptions.items():
            if exception.lower() in file_name_lower:
                self.logger.info(f"Excepción detectada: {file_name} asignado a la etiqueta '{label}'")
                return label

        # Buscar coincidencias en la lista de etiquetas
        matching_labels = [label for label in label_list if label.lower() in file_name_lower]

        if not matching_labels:
            self.logger.warning(f"No se encontró una etiqueta para el archivo: {file_name}")
            return None

        if len(matching_labels) > 1:
            self.logger.warning(f"Se encontraron múltiples etiquetas para {file_name}: {matching_labels}")

        return matching_labels[0]

    def _process_data(self, label_name: str) -> bool:
        """Procesa los datos con la etiqueta determinada"""
        # 1. Cargar archivo de etiquetas
        label_path = self.config.labels_dir / f"{label_name}.csv"
        label_dict = self._load_label_file(label_path)
        if label_dict is None:
            return False

        # 2. Procesar archivo de datos
        processed_df = self._process_file(label_dict)
        if processed_df is None:
            self.logger.error("Error al procesar el archivo de datos.")
            return False

        # 3. Guardar resultados
        self._save_results(processed_df)
        return True

    def _load_label_file(self, label_path: Path) -> Optional[Dict]:
        """Carga el archivo de etiquetas"""
        if not label_path.exists():
            self.logger.error(f"El archivo de etiquetas no existe: {label_path}")
            return None

        try:
            label_config = self.processing_config['file_config']['label_file']
            label_dict = {}

            chunk_iterator = pd.read_csv(
                label_path,
                sep=label_config['separator'],
                chunksize=label_config['chunk_size'],
                usecols=label_config['columns'],
                encoding='utf-8'
            )

            src_col = label_config['key_columns']['source_ip']
            dst_col = label_config['key_columns']['destination_ip']

            for chunk in chunk_iterator:
                for _, row in chunk.iterrows():
                    src_ip = str(row[src_col]).strip('"').strip("'")
                    dst_ip = str(row[dst_col]).strip('"').strip("'")

                    if pd.isna(src_ip) or pd.isna(dst_ip):
                        continue

                    key = (src_ip, dst_ip)
                    label_dict[key] = {
                        col_name: row[col]
                        for col_name, col in label_config['label_columns'].items()
                    }

            self.logger.info(f"Archivo de etiquetas cargado: {label_path}")
            return label_dict
        except Exception as e:
            self.logger.error(f"Error al cargar el archivo de etiquetas {label_path}: {str(e)}")
            return None

    def _process_file(self, label_dict: Dict) -> Optional[pd.DataFrame]:
        """Procesa el archivo de datos"""
        if not self.config.data_path.exists():
            self.logger.error(f"El archivo de datos no existe: {self.config.data_path}")
            return None

        try:
            data_config = self.processing_config['file_config']['data_file']
            dtype_dict = {col: str for col in self.processing_config['column_dtypes'].keys()}

            chunks = []
            try:
                chunk_iterator = pd.read_csv(
                    self.config.data_path,
                    sep=data_config['separator'],
                    chunksize=data_config['chunk_size'],
                    dtype=dtype_dict,
                    low_memory=False,
                    encoding='utf-8',
                    on_bad_lines='skip'
                )
            except UnicodeDecodeError:
                chunk_iterator = pd.read_csv(
                    self.config.data_path,
                    sep=data_config['separator'],
                    chunksize=data_config['chunk_size'],
                    dtype=dtype_dict,
                    low_memory=False,
                    encoding='latin1',
                    on_bad_lines='skip'
                )

            src_col = data_config['key_columns']['source_ip']
            dst_col = data_config['key_columns']['destination_ip']
            label_config = self.processing_config['file_config']['label_file']

            for chunk in chunk_iterator:
                processed_chunk = self._process_chunk(chunk, label_dict, src_col, dst_col, label_config)
                chunks.append(processed_chunk)

            if not chunks:
                self.logger.error("No se pudieron procesar chunks del archivo")
                return None

            result = pd.concat(chunks, ignore_index=True)
            self.logger.info(f"Archivo procesado exitosamente: {self.config.data_path}")
            return result

        except Exception as e:
            self.logger.error(f"Error al procesar el archivo {self.config.data_path}: {str(e)}")
            import traceback
            self.logger.debug(f"Traceback completo: {traceback.format_exc()}")
            return None

    def _process_chunk(self, chunk: pd.DataFrame, label_dict: Dict,
                       src_col: str, dst_col: str, label_config: Dict) -> pd.DataFrame:
        """Procesa un chunk individual de datos"""
        # Manejar columnas faltantes
        for label_col in label_config['label_columns'].keys():
            if label_col not in chunk.columns:
                chunk[label_col] = 'Unclear'

        # Validar columnas IP
        if src_col not in chunk.columns or dst_col not in chunk.columns:
            self.logger.error(f"Columnas requeridas no encontradas. Buscando {src_col} y {dst_col}")
            self.logger.debug(f"Columnas disponibles: {chunk.columns.tolist()}")
            return chunk

        # Limpiar IPs
        chunk[src_col] = chunk[src_col].fillna('').map(self._clean_ip)
        chunk[dst_col] = chunk[dst_col].fillna('').map(self._clean_ip)

        # Añadir etiquetas
        for idx, row in chunk.iterrows():
            src_ip = row[src_col]
            dst_ip = row[dst_col]

            if not src_ip or not dst_ip:
                continue

            key = (src_ip, dst_ip)
            if key in label_dict:
                labels = label_dict[key]
                for col_name, value in labels.items():
                    chunk.at[idx, col_name] = value

        return chunk

    @staticmethod
    def _clean_ip(ip: str) -> str:
        """Limpia y valida una dirección IP"""
        if pd.isna(ip) or ip == '':
            return ''
        return str(ip).strip('"').strip("'").strip()

    def _save_results(self, df: pd.DataFrame) -> None:
        """Guarda los resultados procesados"""
        try:
            self.config.output_path.parent.mkdir(parents=True, exist_ok=True)

            if len(df) > 100000:
                output_path = self.config.output_path.with_suffix('.csv.gz')
                df.to_csv(
                    output_path,
                    sep=self.processing_config['file_config']['label_file']['separator'],
                    index=False,
                    compression='gzip',
                    encoding='utf-8'
                )
            else:
                df.to_csv(
                    self.config.output_path,
                    sep=self.processing_config['file_config']['label_file']['separator'],
                    index=False,
                    encoding='utf-8'
                )

            self.logger.info(f"Archivo procesado guardado en: {self.config.output_path}")
        except Exception as e:
            self.logger.error(f"Error al guardar el archivo {self.config.output_path}: {str(e)}")
