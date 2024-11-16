import os
import pandas as pd
import argparse
import logging


def setup_logging():
    """Configura el sistema de logging básico."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def get_label_list(labels_dir, logger):
    """
    Obtiene el listado de etiquetas disponibles en el directorio.

    Args:
        labels_dir: Ruta del directorio de etiquetas.
        logger: Logger para registrar eventos.

    Returns:
        Lista de nombres de etiquetas (archivos sin extensión).
    """
    labels_dir = os.path.abspath(labels_dir)
    if not os.path.exists(labels_dir):
        logger.error(f"El directorio de etiquetas no existe: {labels_dir}")
        return []

    label_files = [f for f in os.listdir(labels_dir) if f.endswith('.csv')]
    label_names = [os.path.splitext(f)[0] for f in label_files]
    logger.info(f"Etiquetas disponibles: {label_names}")
    return label_names


def determine_label(file_name, label_list, logger):
    """
    Determina la etiqueta que corresponde al archivo de datos en base al nombre.

    Args:
        file_name: Nombre del archivo de datos.
        label_list: Lista de etiquetas disponibles.
        logger: Logger para registrar eventos.

    Returns:
        Nombre de la etiqueta encontrada o None si no hay coincidencia.
    """
    matching_labels = [label for label in label_list if label in file_name]
    if not matching_labels:
        logger.warning(f"No se encontró una etiqueta para el archivo: {file_name}")
        return None
    if len(matching_labels) > 1:
        logger.warning(f"Se encontraron múltiples etiquetas para {file_name}: {matching_labels}")
    logger.info(f"Etiqueta seleccionada: {matching_labels[0]}")
    return matching_labels[0]


def load_label_file(label_dir, label_name, logger):
    """
    Carga el archivo de etiquetas correspondiente.

    Args:
        label_dir: Directorio de etiquetas.
        label_name: Nombre de la etiqueta a cargar.
        logger: Logger para registrar eventos.

    Returns:
        DataFrame con las etiquetas o None si ocurre un error.
    """
    label_file = os.path.abspath(os.path.join(label_dir, f"{label_name}.csv"))
    if not os.path.exists(label_file):
        logger.error(f"El archivo de etiquetas no existe: {label_file}")
        return None

    try:
        label_df = pd.read_csv(label_file, sep=";")
        logger.info(f"Archivo de etiquetas cargado: {label_file}")
        return label_df
    except Exception as e:
        logger.error(f"Error al cargar el archivo de etiquetas {label_file}: {str(e)}")
        return None


def process_file(data_file, label_df, logger):
    """
    Procesa un archivo de datos usando las etiquetas proporcionadas.

    Args:
        data_file: Ruta del archivo de datos.
        label_df: DataFrame con las etiquetas.
        logger: Logger para registrar eventos.

    Returns:
        DataFrame procesado con las etiquetas añadidas.
    """
    data_file = os.path.abspath(data_file)
    if not os.path.exists(data_file):
        logger.error(f"El archivo de datos no existe: {data_file}")
        return None

    try:
        # Cargar el archivo de datos
        data_df = pd.read_csv(data_file, sep=",")  # Cambié el separador a coma
        logger.info(f"Archivo de datos cargado: {data_file}")

        # Agregar columnas de etiquetas por defecto
        data_df["attack"] = "Unclear"
        data_df["category"] = "Unclear"
        data_df["subcategory"] = "Unclear"

        # Limpiar las IP y realizar comparaciones robustas
        label_df["saddr"] = label_df["saddr"].astype(str).str.strip('"')
        label_df["daddr"] = label_df["daddr"].astype(str).str.strip('"')

        data_df["src_ip"] = data_df["src_ip"].astype(str).str.strip('"')
        data_df["dst_ip"] = data_df["dst_ip"].astype(str).str.strip('"')

        # Recorrer el archivo de datos y etiquetar
        for idx, row in data_df.iterrows():
            src_ip = row.get("src_ip")
            dst_ip = row.get("dst_ip")

            # Evitar procesamiento de filas con IPs faltantes
            if pd.isna(src_ip) or pd.isna(dst_ip):
                continue

            # Buscar coincidencias con el archivo de etiquetas
            for _, label_row in label_df.iterrows():
                if src_ip == label_row["saddr"] and dst_ip == label_row["daddr"]:
                    data_df.at[idx, "attack"] = label_row["attack"]
                    data_df.at[idx, "category"] = label_row["category"]
                    data_df.at[idx, "subcategory"] = label_row["subcategory"]
                    break

        logger.info(f"Archivo procesado exitosamente: {data_file}")
        return data_df
    except Exception as e:
        logger.error(f"Error al procesar el archivo {data_file}: {str(e)}")
        return None


def save_csv(df, output_file, logger):
    """
    Guarda un DataFrame en un archivo CSV.

    Args:
        df: DataFrame a guardar.
        output_file: Ruta del archivo de salida.
        logger: Logger para registrar eventos.
    """
    try:
        df.to_csv(output_file, sep=";", index=False)
        logger.info(f"Archivo procesado guardado en: {output_file}")
    except Exception as e:
        logger.error(f"Error al guardar el archivo {output_file}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Etiquetador individual de archivos CSV")
    parser.add_argument("--data-file", required=True, help="Archivo de datos a procesar (CSV)")
    parser.add_argument("--labels-dir", required=True, help="Directorio de etiquetas")
    parser.add_argument("--output-file", required=True, help="Archivo de salida procesado (CSV)")
    parser.add_argument("--debug", action="store_true", help="Activa el modo debug")
    args = parser.parse_args()

    logger = setup_logging()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Obtener lista de etiquetas disponibles
    labels_dir = os.path.abspath(args.labels_dir)
    data_file = os.path.abspath(args.data_file)
    output_file = os.path.abspath(args.output_file)

    logger.info(f"Archivo de datos: {data_file}")
    logger.info(f"Directorio de etiquetas: {labels_dir}")

    label_list = get_label_list(labels_dir, logger)
    if not label_list:
        logger.error("No hay etiquetas disponibles para procesar.")
        return

    # Determinar etiqueta correspondiente
    file_name = os.path.basename(data_file)
    label_name = determine_label(file_name, label_list, logger)
    if not label_name:
        logger.error("No se pudo determinar la etiqueta para el archivo.")
        return

    # Cargar archivo de etiquetas
    label_df = load_label_file(labels_dir, label_name, logger)
    if label_df is None:
        logger.error("No se pudo cargar el archivo de etiquetas.")
        return

    # Procesar el archivo
    processed_df = process_file(data_file, label_df, logger)
    if processed_df is not None:
        save_csv(processed_df, output_file, logger)
    else:
        logger.error("Error al procesar el archivo de datos.")


if __name__ == "__main__":
    main()
