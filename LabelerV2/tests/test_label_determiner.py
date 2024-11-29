import os
import pytest
from unittest.mock import patch

from LabelerV2.modules.label_determiner import process_tagging
from LabelerV2.modules.logger import setup_logger

# Configurar el logger para las pruebas
logger = setup_logger()

@patch("modules.label_determiner.os.listdir")
def test_process_tagging_success(mock_listdir):
    target_csv = "IoT_Keylogging__00002_20180619140719.csv"
    labels_directory = "/path/to/labels"

    # Simulación de archivos en el directorio de etiquetas
    mock_listdir.return_value = ["Keylogging.csv", "Data_exfiltration.csv"]

    expected_label_file = os.path.join(labels_directory, "Keylogging.csv")
    result = process_tagging(target_csv, labels_directory)

    logger.debug(f"Resultado esperado: {expected_label_file}, Resultado obtenido: {result}")
    assert result == expected_label_file

@patch("modules.label_determiner.os.listdir")
def test_process_tagging_no_matching_reference(mock_listdir):
    target_csv = "IoT_Keylogging__00002_20180619140719.csv"
    labels_directory = "/path/to/labels"

    # Simulación de archivos en el directorio de etiquetas
    mock_listdir.return_value = ["Data_exfiltration.csv"]

    with pytest.raises(ValueError) as exc_info:
        process_tagging(target_csv, labels_directory)
    logger.debug(f"Error esperado: {exc_info.value}")

@patch("modules.label_determiner.os.listdir")
def test_process_tagging_empty_directory(mock_listdir):
    target_csv = "IoT_Keylogging__00002_20180619140719.csv"
    labels_directory = "/path/to/labels"

    # Simulación de un directorio vacío
    mock_listdir.return_value = []

    with pytest.raises(ValueError) as exc_info:
        process_tagging(target_csv, labels_directory)
    logger.debug(f"Error esperado: {exc_info.value}")