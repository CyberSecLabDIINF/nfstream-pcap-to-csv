import os
import argparse
import logging
import pandas as pd


def setup_logging():
    """Configura el sistema de logging básico"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def get_available_labels(labels_dir, logger):
    """
    Obtiene las etiquetas disponibles recursivamente del directorio y sus subcarpetas.

    Args:
        labels_dir: Ruta absoluta al directorio de etiquetas.
        logger: Logger para registrar eventos.

    Returns:
        Lista de tuplas (ruta_relativa, nombre_etiqueta, dataframe).
    """
    labels_dir = os.path.abspath(labels_dir)  # Asegurarse de que la ruta sea absoluta
    if not os.path.exists(labels_dir):
        raise FileNotFoundError(f"El directorio de etiquetas {labels_dir} no existe")

    label_matrix = []
    for root, dirs, files in os.walk(labels_dir):
        for file in files:
            if file.endswith('.csv'):
                full_path = os.path.abspath(os.path.join(root, file))
                relative_path = os.path.relpath(full_path, labels_dir)
                label_name = os.path.splitext(file)[0]
                try:
                    df = pd.read_csv(full_path, sep=';')  # Leer usando ;
                    label_matrix.append((relative_path, label_name, df))
                    logger.debug(f"Cargado archivo de etiquetas: {full_path}")
                except Exception as e:
                    logger.error(f"Error al leer el archivo de etiquetas {full_path}: {str(e)}")
    return label_matrix


def process_csv_file(file_path, label_matrix, logger):
    """
    Procesa un archivo CSV y añade las etiquetas correspondientes.

    Args:
        file_path: Ruta absoluta al archivo CSV a procesar.
        label_matrix: Lista de tuplas (ruta, nombre, dataframe) con las etiquetas.
        logger: Logger para registrar eventos.

    Returns:
        DataFrame procesado con las nuevas columnas.
    """
    file_path = os.path.abspath(file_path)  # Asegurar ruta absoluta
    if not os.path.exists(file_path):
        logger.error(f"El archivo no existe: {file_path}")
        return None

    try:
        file_name = os.path.basename(file_path)
        matching_labels = [label_name for _, label_name, _ in label_matrix if label_name in file_name]
        if not matching_labels:
            logger.warning(f"No se encontraron etiquetas para el archivo: {file_name}")
            return None
        if len(matching_labels) > 1:
            logger.warning(f"Múltiples coincidencias para el archivo {file_name}: {matching_labels}")

        file_label = matching_labels[0]
        label_df = next((df for _, name, df in label_matrix if name == file_label), None)

        csv_df = pd.read_csv(file_path, sep=';')  # Leer usando ;
        csv_df['attack'] = 'Unclear'
        csv_df['category'] = 'Unclear'
        csv_df['subcategory'] = 'Unclear'

        for idx, row in csv_df.iterrows():
            src_ip = row.get('src_ip')
            dst_ip = row.get('dst_ip')
            if not src_ip or not dst_ip:
                continue

            for _, label_row in label_df.iterrows():
                if src_ip == label_row['saddr'] and dst_ip == label_row['daddr']:
                    csv_df.at[idx, 'attack'] = label_row['attack']
                    csv_df.at[idx, 'category'] = label_row['category']
                    csv_df.at[idx, 'subcategory'] = label_row['subcategory']
                    break
        return csv_df

    except Exception as e:
        logger.error(f"Error al procesar el archivo {file_path}: {str(e)}")
        return None


def process_files(target_dir, label_list, logger):
    """
    Procesa todos los archivos CSV en el directorio objetivo.

    Args:
        target_dir: Ruta absoluta al directorio de archivos a procesar.
        label_list: Lista de tuplas (ruta, nombre, dataframe) con las etiquetas.
        logger: Logger para registrar eventos.
    """
    target_dir = os.path.abspath(target_dir)  # Asegurar ruta absoluta
    if not os.path.exists(target_dir):
        raise FileNotFoundError(f"El directorio objetivo {target_dir} no existe")

    output_dir = os.path.abspath(os.path.join(target_dir, "../processed_files"))
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
    for file_name in files:
        file_path = os.path.abspath(os.path.join(target_dir, file_name))
        logger.info(f"Procesando archivo: {file_path}")
        processed_df = process_csv_file(file_path, label_list, logger)
        if processed_df is not None:
            output_path = os.path.join(output_dir, f"processed_{file_name}")
            processed_df.to_csv(output_path, sep=';', index=False)  # Escribir usando ;
            logger.info(f"Archivo procesado guardado en: {output_path}")
        else:
            logger.error(f"Error al procesar el archivo: {file_name}")


def main():
    parser = argparse.ArgumentParser(description='Sistema de etiquetado de archivos CSV')
    parser.add_argument('--labels-dir', required=True, help='Directorio de etiquetas')
    parser.add_argument('--target-dir', required=True, help='Directorio de archivos a etiquetar')
    parser.add_argument('--debug', action='store_true', help='Activa el modo debug')
    args = parser.parse_args()

    logger = setup_logging()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    labels_dir = os.path.abspath(args.labels_dir)
    target_dir = os.path.abspath(args.target_dir)

    logger.info(f"Directorio de etiquetas: {labels_dir}")
    logger.info(f"Directorio de archivos a procesar: {target_dir}")

    try:
        label_list = get_available_labels(labels_dir, logger)
        logger.info(f"Etiquetas cargadas: {len(label_list)}")
        process_files(target_dir, label_list, logger)
    except Exception as e:
        logger.error(f"Error en la ejecución: {str(e)}")


if __name__ == "__main__":
    main()
