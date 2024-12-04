import os
import pandas as pd
import json

def load_csv(file_path):
    """
    Carga un archivo CSV como un DataFrame de pandas, detectando automáticamente el delimitador y las comillas.

    Args:
        file_path (str): Ruta del archivo CSV.

    Returns:
        pd.DataFrame: DataFrame con el contenido del archivo.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo CSV no existe: {file_path}")

    try:
        # Detecta automáticamente el separador y maneja las comillas
        return pd.read_csv(file_path, sep=None, engine='python', quotechar='"', encoding='utf-8')
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo CSV {file_path}: {e}")



def save_csv(dataframe, file_path):
    """
    Guarda un DataFrame de pandas en un archivo CSV.

    Args:
        dataframe (pd.DataFrame): DataFrame a guardar.
        file_path (str): Ruta del archivo CSV de salida.
    """
    try:
        # Crear directorios si no existen
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        dataframe.to_csv(file_path, index=False)
    except Exception as e:
        raise ValueError(f"No se pudo guardar el archivo CSV en {file_path}: {e}")

def load_json(file_path):
    """
    Carga un archivo JSON como un diccionario de Python.

    Args:
        file_path (str): Ruta del archivo JSON.

    Returns:
        dict: Diccionario con el contenido del archivo JSON.

    Raises:
        FileNotFoundError: Si el archivo JSON no existe.
        ValueError: Si no se puede parsear el JSON o si no es un diccionario.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo JSON no existe: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError(f"El contenido del archivo JSON no es un diccionario: {file_path}")

        return data

    except json.JSONDecodeError as e:
        raise ValueError(f"Error al parsear el archivo JSON {file_path}: {e}")
    except Exception as e:
        raise ValueError(f"Ocurrió un error al cargar el archivo JSON {file_path}: {e}")



def list_csv_files(directory):
    """
    Lista los archivos CSV en un directorio (recursivamente).

    Args:
        directory (str): Ruta del directorio.

    Returns:
        list[str]: Lista de rutas de los archivos CSV encontrados.
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"El directorio no existe: {directory}")

    csv_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    return csv_files
