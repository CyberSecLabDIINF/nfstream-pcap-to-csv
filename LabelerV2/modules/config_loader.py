import os
import json

from pathlib import Path

from .file_handler import load_json

def load_config(file_path, dataset_type):
    """
    Carga y valida un archivo JSON de configuración para un tipo de dataset.

    Args:
        file_path (str | pathlib.Path): Ruta del archivo JSON.
        dataset_type (str): Tipo de dataset (por ejemplo, "Bot-IoT").

    Returns:
        dict: Configuración específica para el dataset solicitado.

    Raises:
        FileNotFoundError: Si el archivo JSON no existe.
        ValueError: Si el dataset o su configuración no existen.
    """
    # Convertir a string si es un objeto Path
    if isinstance(file_path, Path):
        file_path = str(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo JSON no existe: {file_path}")

    try:
        # Pasar la ruta directamente a load_json
        config = load_json(file_path)
    except json.JSONDecodeError as e:
        raise ValueError(f"No se pudo parsear el archivo JSON: {e}")

    if "datasets" not in config:
        raise ValueError("El archivo de configuración debe contener una clave 'datasets'.")

    if dataset_type not in config["datasets"]:
        raise ValueError(f"No se encontró configuración para el dataset '{dataset_type}'.")

    dataset_config = config["datasets"][dataset_type]
    validate_dataset_config(dataset_config)
    return dataset_config



def validate_dataset_config(dataset_config):
    """
    Valida la estructura de la configuración para un dataset específico.

    Args:
        dataset_config (dict): Configuración del dataset.

    Raises:
        ValueError: Si falta alguna clave requerida o tiene formato incorrecto.
    """
    required_keys = ["columns_to_tag", "reference_columns", "column_mapping", "columns_to_copy", "labeling_files"]

    for key in required_keys:
        if key not in dataset_config:
            raise ValueError(f"Falta la clave requerida: {key}")
        if key in ["columns_to_tag", "reference_columns", "columns_to_copy"] and not isinstance(dataset_config[key],
                                                                                                list):
            raise ValueError(f"La clave '{key}' debe ser una lista.")
        if key == "column_mapping" and not isinstance(dataset_config[key], dict):
            raise ValueError(f"La clave '{key}' debe ser un diccionario.")


def print_config_summary(config):
    """
    Imprime un resumen de la configuración cargada para facilitar la depuración.

    Args:
        config (dict): Diccionario de configuración.
    """
    print("Resumen de la configuración:")
    print(f"- Columnas a etiquetar: {config['columns_to_tag']}")
    print(f"- Columnas de referencia: {config['reference_columns']}")
    print(f"- Columnas de salida: {config['output_columns']}")
