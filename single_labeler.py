import pandas as pd
import argparse
import logging
from charset_normalizer import detect
from typing import Optional, List, Dict
from pathlib import Path

# Uso de ejemplo:
# python single_labeler.py --data-file ".\CSVs\Bot-IoT\Scan\OS\4\IoT_Dataset_OSScan__00001_20180522190501.csv" --labels-dir ".\CSVs\Labeling\Bot-IoT\" --output-file ".\CSVs\Labeling\Results\osscan_test.csv"

def setup_logging(debug: bool = False) -> logging.Logger:
    """Configura el sistema de logging básico."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def get_label_list(labels_dir: Path, logger: logging.Logger) -> List[str]:
    """Obtiene el listado de etiquetas disponibles en el directorio."""
    if not labels_dir.exists():
        logger.error(f"El directorio de etiquetas no existe: {labels_dir}")
        return []

    return [f.stem for f in labels_dir.glob("*.csv")]


def get_label_exceptions() -> Dict[str, str]:
    """Define excepciones para nombres de archivos específicos y sus etiquetas."""
    return {
        "data_theft": "Data_exfiltration",
        # Agrega más casos según sea necesario
    }


def determine_label(file_name: str, label_list: List[str], logger: logging.Logger) -> Optional[str]:
    """Determina la etiqueta que corresponde al archivo de datos, considerando excepciones."""
    # Obtener excepciones
    label_exceptions = get_label_exceptions()

    # Buscar si el nombre del archivo coincide con alguna excepción
    for exception, label in label_exceptions.items():
        if exception in file_name:
            logger.info(f"Excepción detectada: {file_name} asignado a la etiqueta '{label}'")
            return label

    # Si no hay excepciones, proceder con la lógica normal
    matching_labels = [label for label in label_list if label in file_name]

    if not matching_labels:
        logger.warning(f"No se encontró una etiqueta para el archivo: {file_name}")
        return None

    if len(matching_labels) > 1:
        logger.warning(f"Se encontraron múltiples etiquetas para {file_name}: {matching_labels}")

    return matching_labels[0]


def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        result = detect(file.read())
        return result['encoding']


def load_label_file(label_path: Path, logger: logging.Logger) -> Optional[Dict]:
    """
    Carga y preprocesa el archivo de etiquetas para optimizar búsquedas posteriores.
    """
    if not label_path.exists():
        logger.error(f"El archivo de etiquetas no existe: {label_path}")
        return None

    try:
        # Detectar codificación
        encoding = detect_encoding(label_path)

        label_dict = {}
        chunks = pd.read_csv(
            label_path,
            sep=";",
            chunksize=10000,
            usecols=['saddr', 'daddr', 'attack', 'category', 'subcategory'],
            encoding=encoding  # Aquí se incluye la codificación detectada
        )

        for chunk in chunks:
            for _, row in chunk.iterrows():
                key = (
                    str(row['saddr']).strip('"'),
                    str(row['daddr']).strip('"')
                )
                label_dict[key] = {
                    'attack': row['attack'],
                    'category': row['category'],
                    'subcategory': row['subcategory']
                }

        logger.info(f"Archivo de etiquetas cargado: {label_path}")
        return label_dict
    except Exception as e:
        logger.error(f"Error al cargar el archivo de etiquetas {label_path}: {str(e)}")
        return None


def get_column_dtypes() -> Dict:
    """Define los tipos de datos para las columnas problemáticas."""
    return {
        'src_ip': str,
        'dst_ip': str,
        # Columnas problemáticas (82-85)
        'src_tcp_flags_ack': str,
        'src_tcp_flags_push': str,
        'src_tcp_flags_reset': str,
        'src_tcp_flags_syn': str,
        'src_tcp_flags_fin': str,
        'dst_tcp_flags_ack': str,
        'dst_tcp_flags_push': str,
        'dst_tcp_flags_reset': str,
        'dst_tcp_flags_syn': str,
        'dst_tcp_flags_fin': str
    }


def process_file(data_path: Path, label_dict: Dict, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Procesa un archivo de datos usando el diccionario de etiquetas."""
    if not data_path.exists():
        logger.error(f"El archivo de datos no existe: {data_path}")
        return None

    try:
        # Detectar codificación
        encoding = detect_encoding(data_path)
        #logger.debug(f"Encoding detection: {encoding} is most likely the one.")

        # Definir tipos de datos específicos para evitar warnings
        dtype_dict = get_column_dtypes()

        chunks = []
        chunk_iterator = pd.read_csv(
            data_path,
            sep=",",
            chunksize=10000,
            dtype=dtype_dict,
            low_memory=False,  # Previene warnings de tipos mixtos
            encoding=encoding  # Usar la codificación detectada
        )

        for chunk in chunk_iterator:
            # Preparar columnas de etiquetas
            chunk['attack'] = 'Unclear'
            chunk['category'] = 'Unclear'
            chunk['subcategory'] = 'Unclear'

            # Limpiar IPs
            chunk['src_ip'] = chunk['src_ip'].str.strip('"')
            chunk['dst_ip'] = chunk['dst_ip'].str.strip('"')

            # Vectorizar la búsqueda de etiquetas
            for idx, row in chunk.iterrows():
                if pd.isna(row['src_ip']) or pd.isna(row['dst_ip']):
                    continue

                key = (row['src_ip'], row['dst_ip'])
                if key in label_dict:
                    labels = label_dict[key]
                    chunk.at[idx, 'attack'] = labels['attack']
                    chunk.at[idx, 'category'] = labels['category']
                    chunk.at[idx, 'subcategory'] = labels['subcategory']

            chunks.append(chunk)

        result = pd.concat(chunks, ignore_index=True)
        logger.info(f"Archivo procesado exitosamente: {data_path}")
        return result

    except Exception as e:
        logger.error(f"Error al procesar el archivo {data_path}: {str(e)}")
        return None


def save_csv(df: pd.DataFrame, output_path: Path, logger: logging.Logger) -> None:
    """Guarda un DataFrame en un archivo CSV."""
    try:
        # Crear directorio si no existe
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Usar compression si el archivo es grande
        if len(df) > 100000:
            output_path = output_path.with_suffix('.csv.gz')
            df.to_csv(output_path, sep=";", index=False, compression='gzip')
        else:
            df.to_csv(output_path, sep=";", index=False)

        logger.info(f"Archivo procesado guardado en: {output_path}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo {output_path}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Etiquetador optimizado de archivos CSV")
    parser.add_argument("--data-file", required=True, help="Archivo de datos a procesar (CSV)")
    parser.add_argument("--labels-dir", required=True, help="Directorio de etiquetas")
    parser.add_argument("--output-file", required=True, help="Archivo de salida procesado (CSV)")
    parser.add_argument("--debug", action="store_true", help="Activa el modo debug")
    args = parser.parse_args()

    # Usar Path para manejo más seguro de rutas
    labels_dir = Path(args.labels_dir).resolve()
    data_path = Path(args.data_file).resolve()
    output_path = Path(args.output_file).resolve()

    logger = setup_logging(args.debug)
    logger.info(f"Archivo de datos: {data_path}")
    logger.info(f"Directorio de etiquetas: {labels_dir}")

    label_list = get_label_list(labels_dir, logger)
    if not label_list:
        logger.error("No hay etiquetas disponibles para procesar.")
        return

    label_name = determine_label(data_path.name, label_list, logger)
    if not label_name:
        logger.error("No se pudo determinar la etiqueta para el archivo.")
        return

    label_dict = load_label_file(labels_dir / f"{label_name}.csv", logger)
    if label_dict is None:
        logger.error("No se pudo cargar el archivo de etiquetas.")
        return

    processed_df = process_file(data_path, label_dict, logger)
    if processed_df is not None:
        save_csv(processed_df, output_path, logger)
    else:
        logger.error("Error al procesar el archivo de datos.")


if __name__ == "__main__":
    main()