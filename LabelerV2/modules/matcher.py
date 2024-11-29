import pandas as pd

def match_columns(df_to_label, df_reference, config):
    """
    Compara las columnas de un archivo CSV a etiquetar con las columnas de un archivo CSV de referencia según la configuración.

    Args:
        df_to_label (pd.DataFrame): DataFrame del archivo CSV que se etiquetará.
        df_reference (pd.DataFrame): DataFrame del archivo CSV de referencia.
        config (dict): Configuración de etiquetado, que incluye columnas a etiquetar, columnas de referencia y mapeo de columnas.

    Returns:
        pd.DataFrame: DataFrame etiquetado.
    """
    # Verificar las columnas a etiquetar y las de referencia en el archivo a etiquetar
    columns_to_label = config.get('columns_to_label', [])
    columns_reference = config.get('columns_reference', [])

    # Asegurarse de que todas las columnas requeridas existen en los DataFrames
    if not all(col in df_to_label.columns for col in columns_to_label):
        raise ValueError(f"El archivo a etiquetar no tiene las columnas esperadas: {columns_to_label}")
    if not all(col in df_reference.columns for col in columns_reference):
        raise ValueError(f"El archivo de referencia no tiene las columnas esperadas: {columns_reference}")

    # Mapear las columnas a etiquetar con las de referencia, si existen en la configuración
    mapped_columns = config.get('column_mappings', {})
    for label_col, reference_col in mapped_columns.items():
        if label_col in df_to_label.columns and reference_col in df_reference.columns:
            # Hacer la comparación y aplicar la etiqueta
            df_to_label[label_col] = df_to_label[label_col].apply(
                lambda x: df_reference[reference_col].eq(x).any()
                # Verifica si el valor está en la columna de referencia
            )

    # Copiar columnas de referencia al archivo etiquetado si hay coincidencia
    copy_columns = config.get('columns_to_copy', [])
    for col in copy_columns:
        if col in df_reference.columns:
            df_to_label[col] = df_reference[col]

    return df_to_label


def apply_labels(df_to_label, config):
    """
    Aplica etiquetas a un DataFrame según un criterio definido en la configuración.

    Args:
        df_to_label (pd.DataFrame): DataFrame que será etiquetado.
        config (dict): Configuración con criterios de etiquetado.

    Returns:
        pd.DataFrame: DataFrame etiquetado.
    """
    # Extraer el nombre de la columna que contiene el valor que queremos evaluar
    required_column = config["criteria"]["column"]

    # Extraer el valor del criterio definido en la configuración
    target_value = config["criteria"]["value"]

    # Validar que la columna requerida exista en el DataFrame
    if required_column not in df_to_label.columns:
        raise KeyError(f"La columna requerida '{required_column}' no está presente en el DataFrame.")

    # Mapeo de equivalencias para protocolos comunes
    # Esto se usa para manejar casos donde el protocolo está representado como texto ("TCP", "UDP")
    # pero el criterio usa valores numéricos (43419 para "TCP", 17 para "UDP")
    protocol_mapping = {"TCP": 43419, "UDP": 17}

    # Si el valor objetivo (target_value) es un texto que coincide con una clave en el mapeo,
    # lo convertimos a su valor numérico correspondiente.
    if isinstance(target_value, str) and target_value in protocol_mapping:
        target_value = protocol_mapping[target_value]

    # Crear una nueva columna en el DataFrame con las etiquetas ("malicious" o "benign")
    # Si el valor en la columna especificada (required_column) coincide con el valor objetivo (target_value),
    # se etiqueta como "malicious"; de lo contrario, como "benign".
    df_to_label[config["label_column"]] = df_to_label.apply(
        lambda row: "malicious" if row[required_column] == target_value else "benign",
        axis=1
    )

    # Retornar el DataFrame etiquetado
    return df_to_label
