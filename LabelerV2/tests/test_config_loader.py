import pytest
import json

from LabelerV2.modules.config_loader import load_config, validate_dataset_config
from LabelerV2.modules.logger import setup_logger

logger = setup_logger()

@pytest.fixture
def valid_config(tmp_path):
    """Crea un archivo JSON válido para pruebas."""
    config_path = tmp_path / "valid_config.json"
    config_content = {
        "datasets": {
            "Bot-IoT": {
                "columns_to_tag": ["saddr", "daddr"],
                "reference_columns": ["src_ip", "dst_ip"],
                "column_mapping": {"saddr": "src_ip", "daddr": "dst_ip"},
                "columns_to_copy": ["label", "attack_type"]
            }
        }
    }
    config_path.write_text(json.dumps(config_content))
    return config_path

@pytest.fixture
def invalid_config(tmp_path):
    """Crea un archivo JSON inválido para pruebas."""
    config_path = tmp_path / "invalid_config.json"
    config_content = {
        "datasets": {
            "Bot-IoT": {
                "columns_to_tag": ["saddr", "daddr"],
                # Falta "reference_columns" y otros campos
            }
        }
    }
    config_path.write_text(json.dumps(config_content))
    return config_path

def test_load_config_valid(valid_config):
    """Prueba cargar un archivo JSON válido."""
    dataset_type = "Bot-IoT"
    config = load_config(valid_config, dataset_type)
    logger.info(f"Configuración cargada: {config}")

    assert "columns_to_tag" in config
    assert config["columns_to_tag"] == ["saddr", "daddr"]

def test_load_config_invalid(invalid_config):
    """Prueba cargar un archivo JSON inválido."""
    dataset_type = "Bot-IoT"
    with pytest.raises(ValueError) as exc_info:
        load_config(invalid_config, dataset_type)
    logger.error(f"Error al cargar configuración: {exc_info.value}")

    assert "Falta la clave requerida" in str(exc_info.value)

def test_validate_dataset_config():
    """Prueba la validación de configuración para un dataset."""
    valid_dataset_config = {
        "columns_to_tag": ["saddr", "daddr"],
        "reference_columns": ["src_ip", "dst_ip"],
        "column_mapping": {"saddr": "src_ip", "daddr": "dst_ip"},
        "columns_to_copy": ["attack", "category", "subcategory"]
    }
    try:
        validate_dataset_config(valid_dataset_config)
        logger.info("Validación exitosa de la configuración del dataset.")
    except ValueError as e:
        pytest.fail(f"Validación fallida: {e}")

    invalid_dataset_config = {
        "columns_to_tag": ["saddr"],
        # Falta "reference_columns"
    }
    with pytest.raises(ValueError) as exc_info:
        validate_dataset_config(invalid_dataset_config)
    logger.error(f"Error de validación: {exc_info.value}")

    assert "Falta la clave requerida" in str(exc_info.value)
