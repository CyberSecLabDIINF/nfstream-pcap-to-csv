import pandas as pd
from .logger import setup_logger

logger = setup_logger()


def match_columns_optimized(df_to_label, df_reference, dataset_type, config_dictionary):
    """
    Compara las columnas de un archivo CSV a etiquetar con las columnas de un archivo CSV de referencia
    según la configuración, optimizando el proceso mediante merge y operaciones vectorizadas.

    Args:
        df_to_label (pd.DataFrame): DataFrame del archivo CSV que se etiquetará.
        df_reference (pd.DataFrame): DataFrame del archivo CSV de referencia.
        dataset_type (str): Tipo de dataset.
        config_dictionary (dict): Configuración de etiquetado, que incluye columnas a etiquetar,
                                  columnas de referencia y mapeo de columnas.

    Returns:
        pd.DataFrame: DataFrame etiquetado.
    """
    logger.info("Iniciando el proceso de etiquetado...")

    # Verificar que el diccionario de configuración sea válido
    if not isinstance(config_dictionary, dict):
        raise ValueError("El diccionario de configuración no es válido.")

    logger.info(f"Tipo de dataset recibido: {dataset_type}")

    # Obtener la configuración específica del dataset
    dataset_config = config_dictionary.get(dataset_type, {})
    if not dataset_config:
        raise ValueError(f"No se encontró configuración para el tipo de dataset: {dataset_type}")

    logger.info(f"Configuración cargada para {dataset_type}: {dataset_config}")

    # Columnas clave y columnas que se copiarán
    target_csv_key_columns = dataset_config['target_csv_key_columns']
    reference_csv_key_columns = dataset_config['reference_csv_key_columns']
    column_mapping = dataset_config['column_mapping']
    columns_to_copy = dataset_config['columns_to_copy']

    logger.info(f"Columnas clave en df_to_label: {target_csv_key_columns}")
    logger.info(f"Columnas clave en df_reference: {reference_csv_key_columns}")
    logger.info(f"Mapeo de columnas: {column_mapping}")
    logger.info(f"Columnas a copiar: {columns_to_copy}")

    # Verificar que las columnas necesarias estén presentes en los DataFrames
    missing_target_columns = set(target_csv_key_columns) - set(df_to_label.columns)
    missing_reference_columns = set(reference_csv_key_columns) - set(df_reference.columns)

    if missing_target_columns:
        raise ValueError(f"El archivo a etiquetar no tiene las columnas esperadas: {missing_target_columns}")
    if missing_reference_columns:
        raise ValueError(f"El archivo de referencia no tiene las columnas esperadas: {missing_reference_columns}")

    logger.info("Todas las columnas necesarias están presentes.")

    # Convertir las columnas clave a tipo string para evitar errores de tipo en el merge
    logger.info("Convirtiendo las columnas clave a tipo string para evitar errores de tipo...")
    for col in target_csv_key_columns:
        df_to_label[col] = df_to_label[col].astype(str)
        logger.info(f"Columna {col} de df_to_label convertida a string.")
    for col in reference_csv_key_columns:
        df_reference[col] = df_reference[col].astype(str)
        logger.info(f"Columna {col} de df_reference convertida a string.")


    # Renombrar columnas en df_reference para que coincidan con df_to_label según el mapeo
    logger.info("Renombrando columnas en df_reference según el mapeo definido...")
    df_reference_renamed = df_reference.rename(columns=column_mapping)

    # Realizar el merge utilizando las columnas clave
    logger.info("Realizando merge entre df_to_label y df_reference_renamed...")
    merged_df = df_to_label.merge(
        df_reference_renamed,
        how='left',
        left_on=target_csv_key_columns,
        right_on=reference_csv_key_columns,
        suffixes=('', '_ref')
    )
    logger.info("Merge completado.")
    logger.info(f"columas en merged_df: {merged_df.columns}")

    # Copiar las columnas de referencia en caso de coincidencias
    logger.info("Copiando columnas en caso de coincidencia...")
    for col in columns_to_copy:
        ref_col = col + '_ref'
        if ref_col in merged_df.columns:
            logger.info(f"Copiando valores de {ref_col} a {col} si es necesario.")
            merged_df[col] = merged_df[col].combine_first(merged_df[ref_col])
            merged_df.drop(columns=[ref_col], inplace=True)
            logger.info(f"Columna {col} actualizada y columna {ref_col} eliminada.")

    logger.info("Proceso de etiquetado completado.")
    return merged_df


# Función para comparar dos filas de acuerdo al column_mapping
def compare_rows(row_to_label, row_reference, column_mapping):
    """
    Compara dos filas de acuerdo al mapeo de columnas.

    Args:
        row_to_label (pd.Series): Fila del archivo CSV que se etiquetará.
        row_reference (pd.Series): Fila del archivo CSV de referencia.
        column_mapping (dict): Mapeo de columnas.

    Returns:
        bool: True si las filas coinciden, False en caso contrario.
    """
    for label_col, reference_col in column_mapping.items():
        # Detallado de comparaciones
        logger.info(f"Comparando {label_col} con {reference_col}...")
        logger.info(f"{row_to_label[label_col]} == {row_reference[reference_col]}")
        if row_to_label[label_col] != row_reference[reference_col]:
            return False
    return True

# Función para copiar las columnas de columns_to_copy de una fila a otra
def copy_columns(row_to_label, row_reference, columns_to_copy):
    """
    Copia las columnas de una fila a otra.

    Args:
        row_to_label (pd.Series): Fila del archivo CSV que se etiquetará.
        row_reference (pd.Series): Fila del archivo CSV de referencia.
        columns_to_copy (list): Lista de columnas a copiar.

    Returns:
        pd.Series: Fila con las columnas copiadas.
    """
    for column in columns_to_copy:
        row_to_label[column] = row_reference[column]
    return row_to_label



'''
    # Por cada fila de df_to_label, comparar con df_reference ocupando las columnas de column_mapping
    # y si hay coincidencia, copiar las columnas de columns_to_copy a df_to_label
    for index, row in df_to_label.iterrows():
        # Fila a etiquetar
        logger.info(f"Comparando fila {index}...")
        # Mostrar cada los valores de las columnas key de cada fila
        for column in target_csv_key_columns:
            logger.info(f"{column}: {row[column]}")
            # Comparar con cada fila del archivo de referencia
            for index_reference, row_reference in df_reference.iterrows():
                logger.info(f"Comparando con fila {index_reference}...")
                # Si las filas coinciden, copiar las columnas de referencia a la fila a etiquetar
                if compare_rows(row, row_reference, dataset_config['column_mapping']):
                    logger.info(f"Coincidencia encontrada en fila {index_reference}.")
                    df_to_label.loc[index] = copy_columns(row, row_reference, dataset_config['columns_to_copy'])
                    break
'''