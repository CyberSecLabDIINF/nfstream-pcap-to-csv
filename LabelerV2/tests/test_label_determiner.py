import pytest
import os

from unittest.mock import patch, mock_open
from LabelerV2.modules.label_determiner import determine_labeling_file

@pytest.fixture
def mock_config_bot_iot():
    """Fixture que proporciona un diccionario de configuración de prueba."""
    return {
        "datasets": "Bot-IoT",
        "labeling_files": {
            "data_theft": "data_exfiltration",
            "keylogging": "keylogging",
            "http_ddos": "ddos_http",
        }
    }

@pytest.fixture
def mock_labels_directory(tmp_path):
    """Crea un directorio temporal con archivos de etiquetas simulados."""
    labels_dir = tmp_path / "Bot-IoT"
    labels_dir.mkdir(parents=True)
    (labels_dir / "data_exfiltration.csv").write_text("sample data")
    (labels_dir / "keylogging.csv").write_text("sample data")
    (labels_dir / "ddos_http.csv").write_text("sample data")
    return str(labels_dir)

def test_determine_labeling_file_match(mock_labels_directory, mock_config_bot_iot):
    """Prueba que se encuentra el archivo de etiquetas correcto."""
    csv_to_process = "data/data_theft_attack.csv"
    result = determine_labeling_file(csv_to_process, mock_labels_directory, mock_config_bot_iot)
    assert os.path.basename(result) == "data_exfiltration.csv"

def test_determine_labeling_file_case_insensitive(mock_labels_directory, mock_config_bot_iot):
    """Prueba que la función no es sensible a mayúsculas/minúsculas."""
    csv_to_process = "data/KEYLOGGING_Attack.csv"
    result = determine_labeling_file(csv_to_process, mock_labels_directory, mock_config_bot_iot)
    assert os.path.basename(result) == "keylogging.csv"

def test_determine_labeling_file_no_match(mock_labels_directory, mock_config_bot_iot):
    """Prueba que lanza ValueError si no se encuentra un archivo de referencia adecuado."""
    csv_to_process = "data/unknown_attack.csv"
    with pytest.raises(ValueError, match="No se encontró un archivo de referencia adecuado"):
        determine_labeling_file(csv_to_process, mock_labels_directory, mock_config_bot_iot)

def test_determine_labeling_file_directory_not_found(mock_config_bot_iot):
    """Prueba que lanza FileNotFoundError si el directorio de etiquetas no existe."""
    csv_to_process = "data/data_theft_attack.csv"
    labels_directory = "non_existent_directory"
    with pytest.raises(FileNotFoundError, match="No se encontró el directorio de etiquetas"):
        determine_labeling_file(csv_to_process, labels_directory, mock_config_bot_iot)

def test_determine_labeling_file_partial_match(mock_labels_directory, mock_config_bot_iot):
    """Prueba que encuentra coincidencias parciales en el nombre del archivo CSV."""
    csv_to_process = "data/http_ddos_attack.csv"
    result = determine_labeling_file(csv_to_process, mock_labels_directory, mock_config_bot_iot)
    assert os.path.basename(result) == "ddos_http.csv"

# --------------------------------------------------------------------------------------------------------------
# Pruebas con otro dataset de ejemplo
# --------------------------------------------------------------------------------------------------------------

@pytest.fixture
def mock_labels_directory_netscan(tmp_path):
    """Crea un directorio temporal con archivos de etiquetas para el dataset NetScan."""
    labels_directory = tmp_path / "NetScan"
    labels_directory.mkdir(parents=True, exist_ok=True)

    # Crear archivos de referencia
    (labels_directory / "port_scan.csv").touch()
    (labels_directory / "syn_flood.csv").touch()
    (labels_directory / "icmp_flood.csv").touch()

    return str(labels_directory)

@pytest.fixture
def mock_config_netscan():
    """Devuelve una configuración ficticia para el dataset NetScan."""
    return {
        "datasets": "NetScan",
        "labeling_files": {
            "portscan": "port_scan",
            "synflood": "syn_flood",
            "icmpflood": "icmp_flood",
        }
    }

def test_determine_labeling_file_portscan(mock_labels_directory_netscan, mock_config_netscan):
    """Prueba para verificar que encuentra el archivo de etiquetas correcto para un portscan."""
    csv_to_process = "data/portscan_activity.csv"
    result = determine_labeling_file(csv_to_process, mock_labels_directory_netscan, mock_config_netscan)
    assert "port_scan.csv" in result

def test_determine_labeling_file_synflood(mock_labels_directory_netscan, mock_config_netscan):
    """Prueba para verificar que encuentra el archivo de etiquetas correcto para un syn flood."""
    csv_to_process = "data/synflood_attack.csv"
    result = determine_labeling_file(csv_to_process, mock_labels_directory_netscan, mock_config_netscan)
    assert "syn_flood.csv" in result

def test_determine_labeling_file_not_found(mock_labels_directory_netscan, mock_config_netscan):
    """Prueba para verificar que lanza una excepción cuando no se encuentra un archivo de etiquetas."""
    csv_to_process = "data/unknown_activity.csv"
    with pytest.raises(ValueError, match="No se encontró un archivo de referencia adecuado"):
        determine_labeling_file(csv_to_process, mock_labels_directory_netscan, mock_config_netscan)
