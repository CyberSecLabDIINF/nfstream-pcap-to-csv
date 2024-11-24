import os
import pytest
import pandas as pd

from unittest.mock import patch
from LabelerV2.modules.tagger import tag_csv
from LabelerV2.modules.file_handler import save_csv

# Configuración básica para las pruebas
@pytest.fixture
def setup_temp_dirs(tmp_path):
    """Crea una estructura temporal de directorios y archivos para las pruebas."""
    target_dir = tmp_path / "target_csvs"
    reference_dir = tmp_path / "reference"
    output_dir = tmp_path / "output"
    config_path = tmp_path / "config.json"

    # Crear directorios
    target_dir.mkdir()
    reference_dir.mkdir()
    output_dir.mkdir()

    # Crear archivos de ejemplo
    target_csv_path = target_dir / "example.csv"
    reference_csv_path = reference_dir / "example.csv"
    config_path.write_text(
        """{
            "match_columns": {
                "target": ["saddr"],
                "reference": ["src_ip"],
                "copy": ["label", "Category", "Type"]
            }
        }"""
    )

    # Escribir datos de prueba
    target_data = pd.DataFrame({
        "saddr": ["192.168.0.1", "10.0.0.2", "192.168.1.1"],
        "sport": [1234, 5678, 9101]
    })
    reference_data = pd.DataFrame({
        "src_ip": ["192.168.0.1", "10.0.0.2"],
        "label": ["malicious", "benign"],
        "Category": ["DoS", "benign"],
        "Type": ["UDP Flood", "benign"]
    })
    save_csv(target_data, target_csv_path)
    save_csv(reference_data, reference_csv_path)

    return {
        "target_csv_path": target_csv_path,
        "reference_dir": reference_dir,
        "output_dir": output_dir,
        "config_path": config_path
    }

def test_tag_csv_success(setup_temp_dirs):
    """Prueba el etiquetado exitoso de un archivo CSV."""
    dirs = setup_temp_dirs

    # Ejecutar la función
    output_path = tag_csv(
        str(dirs["target_csv_path"]),
        str(dirs["reference_dir"]),
        str(dirs["config_path"]),
        str(dirs["output_dir"])
    )

    # Verificar que el archivo etiquetado existe
    assert os.path.exists(output_path), "El archivo etiquetado no se generó correctamente."

    # Cargar y verificar el contenido del archivo etiquetado
    df_labeled = pd.read_csv(output_path)
    assert "label" in df_labeled.columns, "La columna 'label' no fue añadida."
    assert "Category" in df_labeled.columns, "La columna 'Category' no fue añadida."
    assert "Type" in df_labeled.columns, "La columna 'Type' no fue añadida."
    assert df_labeled["label"].iloc[0] == "malicious", "La etiqueta de la primera fila no es correcta."
    assert df_labeled["label"].iloc[2] == "benign", "La etiqueta de una fila sin coincidencias no es 'benign'."

def test_tag_csv_missing_reference(setup_temp_dirs):
    """Prueba el caso donde no se encuentra el archivo de referencia."""
    dirs = setup_temp_dirs

    # Eliminar el archivo de referencia
    os.remove(dirs["reference_dir"] / "example.csv")

    # Intentar ejecutar la función
    with pytest.raises(FileNotFoundError, match="No se encontró el archivo de referencia correspondiente"):
        tag_csv(
            str(dirs["target_csv_path"]),
            str(dirs["reference_dir"]),
            str(dirs["config_path"]),
            str(dirs["output_dir"])
        )

def test_tag_csv_invalid_config(setup_temp_dirs):
    """Prueba el caso donde el archivo de configuración no es válido."""
    dirs = setup_temp_dirs

    # Crear un archivo JSON de configuración inválido
    invalid_config_path = dirs["config_path"]
    invalid_config_path.write_text("{malformed json}")

    # Intentar ejecutar la función
    with pytest.raises(Exception, match="Error al cargar la configuración"):
        tag_csv(
            str(dirs["target_csv_path"]),
            str(dirs["reference_dir"]),
            str(invalid_config_path),
            str(dirs["output_dir"])
        )

def test_tag_csv_output_dir_creation(setup_temp_dirs):
    """Prueba que se cree automáticamente la carpeta de salida si no existe."""
    dirs = setup_temp_dirs

    # Eliminar la carpeta de salida
    os.rmdir(dirs["output_dir"])

    # Ejecutar la función
    output_path = tag_csv(
        str(dirs["target_csv_path"]),
        str(dirs["reference_dir"]),
        str(dirs["config_path"]),
        str(dirs["output_dir"])
    )

    # Verificar que la carpeta de salida fue creada
    assert os.path.exists(dirs["output_dir"]), "La carpeta de salida no fue creada."
    assert os.path.exists(output_path), "El archivo etiquetado no se generó correctamente."

