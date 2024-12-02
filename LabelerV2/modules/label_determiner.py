import os
from .logger import setup_logger

# Configurar el logger
logger = setup_logger()

def determine_labeling_file(csv_to_process, labels_directory, config_dictionary):
    """
    Determina el archivo de etiquetas adecuado para un archivo CSV objetivo basándose
    en el nombre del archivo y el diccionario de configuración.

    Args:
        csv_to_process (str): Ruta del archivo CSV objetivo.
        labels_directory (str): Ruta del directorio con archivos CSV de referencia.
        config_dictionary (dict): Diccionario de configuración para el dataset.

    Returns:
        label_file (str): Ruta del archivo CSV de etiquetas correspondiente.

    Raises:
        ValueError: Si no se encuentra un archivo de referencia adecuado.
        FileNotFoundError: Si el directorio de etiquetas no existe.
    """

    try:
        # Validar que el diccionario de configuración sea de tipo dict
        if not isinstance(config_dictionary, dict):
            raise ValueError("El diccionario de configuración no es válido.")

        # Obtener el nombre del archivo CSV objetivo
        csv_to_process_name = os.path.basename(csv_to_process)
        csv_to_process_name = os.path.splitext(csv_to_process_name)[0].lower()

        # Obtener el nombre del dataset
        dataset_type = next(iter(config_dictionary.keys()))
        dataset_config = config_dictionary[dataset_type]  # Acceder a la configuración del dataset

        # Concatenar el tipo del dataset al directorio de etiquetas
        labels_directory = os.path.join(labels_directory, dataset_type)

        # Validar que el directorio de etiquetas exista
        if not os.path.exists(labels_directory):
            raise FileNotFoundError(f"No se encontró el directorio de etiquetas: {labels_directory}")

        # Listar archivos en el directorio
        #reference_files = [f for f in os.listdir(labels_directory) if f.endswith(".csv")]
        #logger.info(f"Archivos de referencia encontrados: {reference_files}")

        # Buscar archivo de referencia en el diccionario de labeling_files del dataset
        for key, value in dataset_config["labeling_files"].items():
            if key in csv_to_process_name:
                label_file = os.path.join(labels_directory, f"{value}.csv")
                logger.debug(f"Archivo de referencia encontrado: {label_file}")
                return label_file

        raise ValueError("No se encontró un archivo de referencia adecuado para el archivo objetivo.")
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise RuntimeError("Ocurrió un error inesperado al determinar el archivo de etiquetado.")
