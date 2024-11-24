import pytest
import pandas as pd

from LabelerV2.modules.matcher import match_columns, apply_labels
from LabelerV2.modules.logger import setup_logger

# Configuración del logger para pruebas
logger = setup_logger(log_dir="test_logs", log_file="test_matcher.log")


@pytest.fixture
def sample_data():
    """Fixture con datos de prueba para el archivo a etiquetar y de referencia."""
    df_to_label = pd.DataFrame({
        "saddr": ["192.168.1.1", "10.0.0.1", "172.16.0.1"],
        "daddr": ["192.168.1.2", "10.0.0.2", "172.16.0.2"],
        "sport": [12345, 54321, 12345],
        "dport": [80, 8080, 443],
    })
    # TCP = 43419, UDP = 17
    df_reference = pd.DataFrame({
        "src_ip": ["192.168.1.1", "10.0.0.1"],
        "dst_ip": ["192.168.1.2", "10.0.0.2"],
        "src_port": [12345, 54321],
        "dst_port": [80, 8080],
        "protocol": [43419, 17],
        "attack_type": ["DDoS", "PortScan"],
    })

    return df_to_label, df_reference


@pytest.fixture
def sample_config():
    """Fixture con configuración de prueba."""
    return {
        "columns_to_label": ["saddr", "daddr", "sport", "dport"],
        "columns_reference": ["src_ip", "dst_ip", "src_port", "dst_port"],
        "column_mappings": {
            "saddr": "src_ip",
            "daddr": "dst_ip",
            "sport": "src_port",
            "dport": "dst_port"
        },
        "columns_to_copy": ["protocol", "attack_type"],
        "criteria": {
            "column": "protocol",
            "value": "TCP"
        },
        "label_column": "label"
    }


def test_match_columns(sample_data, sample_config):
    """Prueba la función match_columns."""
    df_to_label, df_reference = sample_data

    # Ejecutar la función
    result_df = match_columns(df_to_label, df_reference, sample_config)

    # Verificar columnas añadidas desde el archivo de referencia
    assert "protocol" in result_df.columns
    assert "attack_type" in result_df.columns

    # Verificar que las columnas mapeadas coinciden correctamente
    assert result_df["protocol"].iloc[0] == 43419  # Primera fila, debe coincidir con df_reference
    assert result_df["attack_type"].iloc[1] == "PortScan"  # Segunda fila, misma lógica

    logger.info("test_match_columns pasó correctamente.")


def test_apply_labels(sample_data, sample_config):
    """Prueba la función apply_labels."""
    df_to_label, df_reference = sample_data

    # Aplicar match_columns para agregar columnas necesarias
    df_labeled = match_columns(df_to_label, df_reference, sample_config)

    # Aplicar etiquetas
    result_df = apply_labels(df_labeled, sample_config)

    # Verificar si se agregó la columna de etiqueta
    assert "label" in result_df.columns

    # Verificar que el etiquetado cumple con el criterio
    assert result_df["label"].iloc[0] == "malicious"  # Primera fila: protocolo es "TCP"
    assert result_df["label"].iloc[1] == "benign"     # Segunda fila: protocolo no es "TCP"

    logger.info("test_apply_labels pasó correctamente.")


def test_invalid_columns(sample_data, sample_config):
    """Prueba el manejo de columnas faltantes."""
    df_to_label, df_reference = sample_data

    # Eliminar una columna requerida del archivo a etiquetar
    df_to_label.drop(columns=["saddr"], inplace=True)

    with pytest.raises(ValueError, match="El archivo a etiquetar no tiene las columnas esperadas"):
        match_columns(df_to_label, df_reference, sample_config)

    logger.warning("test_invalid_columns capturó correctamente el error de columnas faltantes.")


def test_missing_reference_columns(sample_data, sample_config):
    """Prueba el manejo de columnas faltantes en el archivo de referencia."""
    df_to_label, df_reference = sample_data

    # Eliminar una columna requerida del archivo de referencia
    df_reference.drop(columns=["src_ip"], inplace=True)

    with pytest.raises(ValueError, match="El archivo de referencia no tiene las columnas esperadas"):
        match_columns(df_to_label, df_reference, sample_config)

    logger.warning("test_missing_reference_columns capturó correctamente el error de columnas faltantes.")


def test_empty_dataframe(sample_config):
    """Prueba el manejo de DataFrames vacíos."""
    df_to_label = pd.DataFrame()
    df_reference = pd.DataFrame()

    with pytest.raises(ValueError, match="El archivo a etiquetar no tiene las columnas esperadas"):
        match_columns(df_to_label, df_reference, sample_config)

    logger.warning("test_empty_dataframe capturó correctamente el error de DataFrame vacío.")
