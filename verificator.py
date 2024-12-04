import argparse
import pandas as pd
from pathlib import Path
from collections import defaultdict


def analyze_labels_in_csvs(folder_path: Path):
    """
    Analiza los archivos CSV en la carpeta dada para encontrar etiquetas únicas
    en las columnas 'attack', 'category' y 'subcategory'.
    """
    label_counts = {
        'attack': defaultdict(int),
        'category': defaultdict(int),
        'subcategory': defaultdict(int)
    }

    # Validar que la carpeta existe
    if not folder_path.exists() or not folder_path.is_dir():
        print(f"La carpeta proporcionada no es válida: {folder_path}")
        return label_counts

    # Procesar todos los archivos CSV en la carpeta
    csv_files = list(folder_path.glob("*.csv"))
    if not csv_files:
        print(f"No se encontraron archivos CSV en la carpeta: {folder_path}")
        return label_counts

    print(f"Analizando {len(csv_files)} archivos CSV en la carpeta {folder_path}...")

    for csv_file in csv_files:
        try:
            print(f"Procesando archivo: {csv_file.name}")

            # Leer el CSV sin especificar delimitador, pandas lo detectará automáticamente
            df = pd.read_csv(csv_file, sep=None, engine='python', usecols=['attack', 'category', 'subcategory'])

            # Contar valores únicos en las columnas relevantes
            for column in ['attack', 'category', 'subcategory']:
                if column in df.columns:
                    counts = df[column].value_counts(dropna=False).to_dict()
                    for label, count in counts.items():
                        label_counts[column][label] += count
        except Exception as e:
            print(f"Error al procesar el archivo {csv_file.name}: {e}")

    return label_counts


def display_label_counts(label_counts):
    """
    Imprime el conteo de etiquetas por columna.

    Args:
        label_counts (dict): Diccionario con las etiquetas y sus conteos.
    """
    for column, labels in label_counts.items():
        print(f"\nEtiquetas en la columna '{column}':")
        for label, count in sorted(labels.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {label}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Analiza etiquetas únicas en columnas específicas de archivos CSV.")
    parser.add_argument("--folder", required=True, help="Ruta de la carpeta que contiene los CSVs")
    args = parser.parse_args()

    folder_path = Path(args.folder).resolve()
    label_counts = analyze_labels_in_csvs(folder_path)
    display_label_counts(label_counts)


if __name__ == "__main__":
    main()
