import logging
import os

def setup_logger(log_dir="logs", log_file="app.log"):
    """
    Configura un logger que registra mensajes en un archivo y en la consola.

    Args:
        log_dir (str): Directorio donde se guardará el archivo de log.
        log_file (str): Nombre del archivo de log.

    Returns:
        Logger: Objeto logger configurado.
    """
    # Crear el directorio de logs si no existe
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configurar el logger
    logger = logging.getLogger("csv_processor")

    # Verificar si ya se configuraron handlers para evitar duplicación
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)  # Nivel mínimo de logging

        # Formato para los mensajes de log
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler para escribir en archivo
        file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
        file_handler.setLevel(logging.DEBUG)  # Registra todos los niveles
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Handler para la consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # Muestra mensajes INFO y superiores
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
