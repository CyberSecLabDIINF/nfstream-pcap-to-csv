import pandas as pd

from .logger import setup_logger

logger = setup_logger()

def validate_config(config_dictionary, dataset_type):
    """
    Valida el diccionario de configuración para un tipo de dataset específico.

    Args:
        config_dictionary (dict): Diccionario de configuración.
        dataset_type (str): Tipo de dataset.

    Returns:
        dict: Configuración específica del dataset.

    Raises:
        ValueError: Si el diccionario de configuración no es válido.
    """
    if not isinstance(config_dictionary, dict):
        raise ValueError("El diccionario de configuración no es válido.")

    dataset_config = config_dictionary.get(dataset_type, {})
    if not dataset_config:
        raise ValueError(f"No se encontró configuración para el tipo de dataset: {dataset_type}")

    #logger.info(f"Configuración cargada para {dataset_type}: {dataset_config}")
    return dataset_config

def check_required_columns(df, required_columns, df_name):
    """
    Verifica que las columnas requeridas estén presentes en un DataFrame.

    Args:
        df (pd.DataFrame): DataFrame a verificar.
        required_columns (list): Lista de columnas requeridas.
        df_name (str): Nombre descriptivo del DataFrame para los mensajes de error.

    Raises:
        ValueError: Si faltan columnas en el DataFrame.
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"El {df_name} no tiene las columnas esperadas: {missing_columns}")

def clean_data(df, columns):
    """
    Convierte las columnas especificadas a tipo string.

    Args:
        df (pd.DataFrame): DataFrame en el que se realizarán las conversiones.
        columns (list): Lista de columnas a convertir.
    """
    for col in columns:
        df[col] = df[col].astype(str)
        #logger.info(f"Columna {col} convertida a string.")

def clean_column_values(df, columns_config, ports_columns):
    """
    Limpia los valores de las columnas especificadas, eliminando puntos y otros.

    Args:
        df (pd.DataFrame): DataFrame en el que se limpiarán los valores.
        columns_config (dict): Configuración de limpieza de columnas.
        ports_columns (list): Columnas de puertos.

    """
    for column in columns_config:
        df[column] = df[column].str.replace(" ", "")
        if column in ports_columns:
            df[column] = df[column].str.replace(".0", "")
        df[column] = df[column].str.replace(".", "")
        #logger.info(f"Valores de columna {column} limpiados.")
    return df


def rename_columns(df, column_mapping):
    """
    Renombra las columnas de un DataFrame según un mapeo dado.

    Args:
        df (pd.DataFrame): DataFrame a renombrar.
        column_mapping (dict): Mapeo de columnas.

    Returns:
        pd.DataFrame: DataFrame con las columnas renombradas.
    """
    logger.info("Renombrando columnas según el mapeo definido...")
    return df.rename(columns=column_mapping)

def operate_dataframes(df_to_label, df_reference, config_dictionary):
    """

    """
    logger.info("Operando los DataFrames...")

    # Seteo de indices para joins
    #df_to_label.set_index(config_dictionary['target_csv_key_columns'], inplace=True)
    #df_reference.set_index(config_dictionary['reference_csv_key_columns'], inplace=True)

    # Concatenación de DataFrames
    #operated_df = pd.concat([df_to_label, df_reference], join='inner', axis=1, verify_integrity=False)

    # Join df_to_label con df_reference por los índices
    #operated_df = df_to_label.join(df_reference.set_index(config_dictionary['reference_csv_key_columns']), on=config_dictionary['target_csv_key_columns'], how='left')

    # Merge de los DataFrames
    operated_df = pd.merge(df_to_label, df_reference, how='inner', left_on=config_dictionary['target_csv_key_columns'], right_on=config_dictionary['reference_csv_key_columns'], validate="many_to_many")

    return operated_df


def copy_reference_columns(merged_df, columns_to_copy):
    """
    Copia las columnas de referencia en caso de coincidencias.

    Args:
        merged_df (pd.DataFrame): DataFrame combinado después del merge.
        columns_to_copy (list): Columnas a copiar desde el sufijo "_ref".

    Returns:
        pd.DataFrame: DataFrame con las columnas copiadas.
    """
    logger.info("Copiando columnas en caso de coincidencia...")
    for col in columns_to_copy:
        ref_col = col + '_ref'
        if ref_col in merged_df.columns:
            logger.info(f"Copiando valores de {ref_col} a {col} si es necesario.")
            merged_df[col] = merged_df[col].combine_first(merged_df[ref_col])
            merged_df.drop(columns=[ref_col], inplace=True)
            logger.info(f"Columna {col} actualizada y columna {ref_col} eliminada.")
    return merged_df

def match_columns_optimized(df_to_label, df_reference, dataset_type, config_dictionary):
    """
    Proceso principal de etiquetado optimizado.

    Args:
        df_to_label (pd.DataFrame): DataFrame del archivo CSV que se etiquetará.
        df_reference (pd.DataFrame): DataFrame del archivo CSV de referencia.
        dataset_type (str): Tipo de dataset.
        config_dictionary (dict): Configuración de etiquetado.

    Returns:
        pd.DataFrame: DataFrame etiquetado.
    """
    logger.info("Iniciando el proceso de etiquetado...")

    # Validar configuración y obtener detalles del dataset
    dataset_config = validate_config(config_dictionary, dataset_type)

    # Verificar columnas requeridas
    check_required_columns(df_to_label, dataset_config['target_csv_key_columns'], "archivo a etiquetar")
    check_required_columns(df_reference, dataset_config['reference_csv_key_columns'], "archivo de referencia")

    # Convertir columnas clave a tipo string
    clean_data(df_to_label, dataset_config['target_csv_key_columns'])
    clean_data(df_reference, dataset_config['reference_csv_key_columns'])

    # Limpiar valores de las columnas clave
    clean_column_values(df_to_label, dataset_config['target_csv_key_columns'], dataset_config['ports_columns'])
    clean_column_values(df_reference, dataset_config['reference_csv_key_columns'], dataset_config['ports_columns'])

    # Operar entre los DataFrames
    final_df = operate_dataframes(df_to_label, df_reference, dataset_config)

    # Limpieza de columnas extra
    final_columns = df_to_label.columns.tolist() + dataset_config['columns_to_copy']
    final_df = final_df[final_columns]

    logger.info("Proceso de etiquetado completado.")
    return final_df

# Uso del proceso actualizado
def process_tagging(df_for_tagging, target_df, dataset_type, dataset_config_dictionary):
    """
    Función de alto nivel para procesar el etiquetado de un DataFrame objetivo.

    Args:
        df_for_tagging (pd.DataFrame): DataFrame de referencia para el etiquetado.
        target_df (pd.DataFrame): DataFrame objetivo a etiquetar.
        dataset_type (str): Tipo de dataset.
        dataset_config_dictionary (dict): Configuración del etiquetado.

    Returns:
        pd.DataFrame: DataFrame etiquetado.
    """
    if df_for_tagging is not None and target_df is not None:
        try:
            logger.info("Iniciando proceso de etiquetado...")
            return match_columns_optimized(target_df, df_for_tagging, dataset_type, dataset_config_dictionary)
        except Exception as exp:
            logger.error(f"Error durante el proceso de etiquetado: {exp}")
            raise
    else:
        raise ValueError("Los DataFrames de entrada no deben ser None.")


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