import os
import pandas as pd
import pytest
import json

from LabelerV2.modules.file_handler import load_csv, save_csv, load_json, list_csv_files
from LabelerV2.modules.logger import setup_logger

logger = setup_logger()

@pytest.fixture
def temp_csv_file(tmp_path):
    """Crea un archivo CSV temporal para pruebas."""
    file_path = tmp_path / "test_file.csv"
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    return file_path

@pytest.fixture
def temp_json_file(tmp_path):
    """Crea un archivo JSON temporal para pruebas."""
    file_path = tmp_path / "test_file.json"
    data = {"key1": "value1", "key2": [1, 2, 3]}
    with open(file_path, "w") as f:
        json.dump(data, f)
    return file_path

@pytest.fixture
def temp_dir_with_csvs(tmp_path):
    """Crea un directorio temporal con varios archivos CSV."""
    csv_file1 = tmp_path / "file1.csv"
    csv_file2 = tmp_path / "subdir/file2.csv"
    non_csv_file = tmp_path / "file3.txt"

    csv_file1.touch()
    csv_file2.parent.mkdir(parents=True)
    csv_file2.touch()
    non_csv_file.touch()

    return tmp_path

def test_load_csv(temp_csv_file):
    """Prueba la carga de un archivo CSV."""
    df = load_csv(temp_csv_file)
    logger.info(f"Contenido del archivo CSV cargado: {df}")

    assert "col1" in df.columns
    assert "col2" in df.columns
    assert len(df) == 3

def test_load_csv_not_found():
    """Prueba cargar un archivo CSV inexistente."""
    with pytest.raises(FileNotFoundError) as exc_info:
        load_csv("/nonexistent_dir/nonexistent_file.csv")
    logger.error(f"Error esperado: {exc_info.value}")

    assert "El archivo CSV no existe" in str(exc_info.value)

def test_save_csv(tmp_path):
    """Prueba guardar un DataFrame en un archivo CSV."""
    output_file = tmp_path / "output.csv"
    data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    df = pd.DataFrame(data)

    save_csv(df, output_file)
    logger.info(f"Archivo CSV guardado en: {output_file}")

    assert os.path.exists(output_file)

    # Validar contenido
    loaded_df = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(df, loaded_df)

def test_load_json(temp_json_file):
    """Prueba la carga de un archivo JSON."""
    data = load_json(temp_json_file)
    logger.info(f"Contenido del archivo JSON cargado: {data}")

    assert "key1" in data
    assert data["key1"] == "value1"

def test_load_json_not_found():
    """Prueba cargar un archivo JSON inexistente."""
    with pytest.raises(FileNotFoundError) as exc_info:
        load_json("/nonexistent_dir/nonexistent_file.json")
    logger.error(f"Error esperado: {exc_info.value}")

    assert "El archivo JSON no existe" in str(exc_info.value)

def test_list_csv_files(temp_dir_with_csvs):
    """Prueba listar archivos CSV en un directorio."""
    found_files = list_csv_files(temp_dir_with_csvs)
    logger.info(f"Archivos CSV encontrados: {found_files}")

    # Archivos esperados
    expected_files = [
        str(temp_dir_with_csvs / "file1.csv"),
        str(temp_dir_with_csvs / "subdir/file2.csv"),
    ]

    assert len(found_files) == len(expected_files)
    for expected in expected_files:
        assert expected in found_files
