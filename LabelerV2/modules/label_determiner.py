import os

from .logger import setup_logger

# Configurar el logger
logger = setup_logger()

def process_tagging(target_csv, labels_directory):
    """
    Determina el archivo de etiquetas adecuado para un archivo CSV objetivo dependiendo del
    nombre del archivo y los archivos de referencia disponibles.

    Args:
        target_csv (str): Ruta del archivo CSV objetivo.
        labels_directory (str): Ruta del directorio con archivos CSV de referencia.

    Returns:
        label_file (str): Ruta del archivo CSV de etiquetas correspondiente.

    Raises:
        ValueError: Si no se encuentra un archivo de referencia adecuado.
    """
    logger.debug(f"Procesando archivo objetivo: {target_csv}")
    logger.debug(f"Directorio de etiquetas: {labels_directory}")

    # Extraer el nombre del archivo objetivo
    target_file_name = os.path.basename(target_csv)
    logger.debug(f"Nombre del archivo objetivo: {target_file_name}")

    # Extraer el nombre del archivo objetivo sin extensión
    target_file_name = os.path.splitext(target_file_name)[0]
    logger.debug(f"Nombre del archivo objetivo sin extensión: {target_file_name}")

    # Pasar a minúsculas
    target_file_name = target_file_name.lower()

    # Extraer el tipo de ataque del nombre del archivo objetivo
    attack_type = target_file_name.split('__')[0].split('_')[1]
    attack_type = attack_type.lower()
    logger.debug(f"Tipo de ataque extraído: {attack_type}")

    # Listar archivos de referencia en el directorio
    reference_files = [f for f in os.listdir(labels_directory) if f.endswith(".csv")]
    logger.debug(f"Archivos de referencia encontrados: {reference_files}")

    # Buscar un archivo de referencia que coincida con el tipo de ataque
    for ref_file in reference_files:
        if attack_type in ref_file.lower():
            label_file = os.path.join(labels_directory, ref_file)
            logger.debug(f"Archivo de etiquetas encontrado: {label_file}")
            return label_file

    logger.error("No se encontró un archivo de referencia adecuado para el archivo objetivo.")
    logger.info("Archivos de referencia cargados:")
    logger.info(labels_directory)
    logger.info("Archivos de referencia encontrados:")
    for file in reference_files:
        logger.info(f" - {file}")
    raise ValueError("No se encontró un archivo de referencia adecuado para el archivo objetivo.")