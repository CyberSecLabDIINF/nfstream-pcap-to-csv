import argparse
import pandas as pd
from pathlib import Path
from collections import defaultdict
import logging
from tabulate import tabulate

def setup_logging():
    """
    Configura el registro de logs para el programa.
    """
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

def filter_labeled_files(csv_files):
    """
    Filtra archivos CSV cuyo nombre termina en '_labeled.csv'.

    Args:
        csv_files (list[Path]): Lista de archivos CSV.

    Returns:
        list[Path]: Archivos que cumplen con el criterio.
    """
    return [csv_file for csv_file in csv_files if csv_file.name.endswith("_labeled.csv")]

def analyze_labels_in_dataframe(df):
    """
    Analiza un DataFrame para contar etiquetas únicas en columnas específicas.

    Args:
        df (pd.DataFrame): DataFrame a analizar.

    Returns:
        dict: Diccionario con los conteos por columna.
    """
    file_counts = {
        'attack': defaultdict(int),
        'category': defaultdict(int),
        'subcategory': defaultdict(int)
    }

    for column in ['attack', 'category', 'subcategory']:
        if column in df.columns:
            counts = df[column].value_counts(dropna=False).to_dict()
            for label, count in counts.items():
                file_counts[column][label] += count

    return file_counts

def analyze_labels_in_file(csv_file):
    """
    Analiza un archivo CSV para contar etiquetas únicas en columnas específicas.

    Args:
        csv_file (Path): Ruta al archivo CSV.

    Returns:
        dict: Diccionario con los conteos por columna.
    """
    try:
        df = pd.read_csv(csv_file, sep=None, engine='python', usecols=['attack', 'category', 'subcategory'])
        return analyze_labels_in_dataframe(df)
    except Exception as e:
        logging.error(f"Error al procesar el archivo {csv_file.name}: {e}")
        return {
            'attack': defaultdict(int),
            'category': defaultdict(int),
            'subcategory': defaultdict(int)
        }

def merge_counts(total_counts, file_counts):
    """
    Combina los conteos de etiquetas de un archivo con el total.

    Args:
        total_counts (dict): Conteos totales acumulados.
        file_counts (dict): Conteos del archivo actual.

    Returns:
        dict: Conteos totales actualizados.
    """
    for column, labels in file_counts.items():
        for label, count in labels.items():
            total_counts[column][label] += count
    return total_counts

def create_summary_table(results_by_file):
    """
    Crea una tabla con los resultados por archivo, ordenada en orden descendente.

    Args:
        results_by_file (list): Lista de resultados por archivo.

    Returns:
        str: Tabla formateada.
    """
    table = []
    for result in results_by_file:
        file_name = result['file']
        total_attack = sum(result['counts']['attack'].values())
        total_category = sum(result['counts']['category'].values())
        total_subcategory = sum(result['counts']['subcategory'].values())
        table.append([file_name, total_attack, total_category, total_subcategory])

    table = sorted(table, key=lambda row: row[1], reverse=True)
    headers = ["Archivo", "Total Attack", "Total Category", "Total Subcategory"]
    return tabulate(table, headers=headers, tablefmt="grid")

def display_label_counts(label_counts, header):
    """
    Imprime el conteo de etiquetas por columna, ordenado por cantidad.

    Args:
        label_counts (dict): Diccionario con las etiquetas y sus conteos.
        header (str): Encabezado para los resultados.
    """
    print(f"\n{header}")
    for column, labels in label_counts.items():
        print(f"Etiquetas en la columna '{column}':")
        for label, count in sorted(labels.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {label}: {count}")

def analyze_folder(folder_path, show_individual):
    """
    Analiza los archivos CSV en una carpeta para encontrar etiquetas únicas.

    Args:
        folder_path (Path): Ruta de la carpeta con los CSVs.
        show_individual (bool): Si se deben mostrar resultados por archivo.

    Returns:
        tuple: Conteos totales de etiquetas y resultados individuales por archivo.
    """
    total_counts = {
        'attack': defaultdict(int),
        'category': defaultdict(int),
        'subcategory': defaultdict(int)
    }
    results_by_file = []

    if not folder_path.exists() or not folder_path.is_dir():
        logging.error(f"La carpeta proporcionada no es válida: {folder_path}")
        return total_counts, results_by_file

    csv_files = filter_labeled_files(list(folder_path.glob("*.csv")))
    if not csv_files:
        logging.info(f"No se encontraron archivos '_labeled.csv' en la carpeta: {folder_path}")
        return total_counts, results_by_file

    logging.info(f"Analizando {len(csv_files)} archivos '_labeled.csv' en la carpeta {folder_path}...")

    for csv_file in csv_files:
        logging.info(f"Procesando archivo: {csv_file.name}")
        file_counts = analyze_labels_in_file(csv_file)

        if show_individual:
            display_label_counts(file_counts, f"Resultados del archivo '{csv_file.name}'")

        results_by_file.append({
            'file': csv_file.name,
            'counts': file_counts
        })

        total_counts = merge_counts(total_counts, file_counts)

    return total_counts, results_by_file

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Analiza etiquetas únicas en columnas específicas de archivos CSV.")
    parser.add_argument("--folder", required=True, help="Ruta de la carpeta que contiene los CSVs")
    parser.add_argument("--show-individual", action="store_true", help="Muestra resultados individuales por archivo")
    args = parser.parse_args()

    folder_path = Path(args.folder).resolve()
    total_counts, results_by_file = analyze_folder(folder_path, args.show_individual)

    print("\nTabla de resultados por archivo:")
    summary_table = create_summary_table(results_by_file)
    print(summary_table)

    display_label_counts(total_counts, "Resultados Totales")

if __name__ == "__main__":
    main()
