import os
from LabelerV2.modules.logger import setup_logger
from LabelerV2.modules.file_handler import load_csv, save_csv
from LabelerV2.modules.config_loader import load_config
from LabelerV2.modules.matcher import match_columns

# Configurar el logger
logger = setup_logger(log_dir="logs", log_file="tagger.log")

def tag_csv(target_csv_path, reference_dir, config_path, output_dir):
    """
    Orquesta el flujo completo de etiquetado de un archivo CSV.

    Args:
        target_csv_path (str): Ruta del archivo CSV a etiquetar.
        reference_dir (str): Ruta de la carpeta que contiene archivos CSV de referencia.
        config_path (str): Ruta del archivo JSON con las configuraciones.
        output_dir (str): Ruta de la carpeta donde se guardarán los archivos etiquetados.

    Returns:
        str: Ruta completa del archivo etiquetado.
    """
    # Validar que los archivos y carpetas existen
    if not os.path.exists(target_csv_path):
        logger.error(f"El archivo CSV objetivo no existe: {target_csv_path}")
        raise FileNotFoundError(f"El archivo CSV objetivo no existe: {target_csv_path}")

    if not os.path.exists(reference_dir):
        logger.error(f"La carpeta de referencia no existe: {reference_dir}")
        raise FileNotFoundError(f"La carpeta de referencia no existe: {reference_dir}")

    if not os.path.exists(config_path):
        logger.error(f"El archivo JSON de configuración no existe: {config_path}")
        raise FileNotFoundError(f"El archivo JSON de configuración no existe: {config_path}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"La carpeta de salida fue creada: {output_dir}")

    # Cargar archivo de configuración
    config = load_config(config_path)
    logger.info(f"Configuración cargada desde {config_path}")

    # Cargar archivo objetivo
    df_to_label = load_csv(target_csv_path)
    logger.info(f"Archivo CSV objetivo cargado: {target_csv_path}")

    # Determinar archivo de referencia correspondiente
    target_name = os.path.basename(target_csv_path)
    reference_path = os.path.join(reference_dir, target_name)
    if not os.path.exists(reference_path):
        logger.error(f"No se encontró el archivo de referencia correspondiente: {reference_path}")
        raise FileNotFoundError(f"No se encontró el archivo de referencia correspondiente: {reference_path}")

    # Cargar archivo de referencia
    df_reference = load_csv(reference_path)
    logger.info(f"Archivo de referencia cargado: {reference_path}")

    # Generar etiquetas usando el matcher
    df_labeled = match_columns(df_to_label, df_reference, config)
    logger.info(f"Se etiquetaron {len(df_labeled)} filas.")

    # Guardar el archivo etiquetado
    output_path = os.path.join(output_dir, f"labeled_{target_name}")
    save_csv(df_labeled, output_path)
    logger.info(f"Archivo etiquetado guardado en: {output_path}")

    return output_path
