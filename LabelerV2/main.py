import argparse
from LabelerV2.modules.tagger import tag_csv

def main():
    # Configuración de argparse
    parser = argparse.ArgumentParser(description="Script para etiquetar archivos CSV basados en configuraciones y referencias.")
    parser.add_argument("target_csv", help="Ruta del archivo CSV a etiquetar.")
    parser.add_argument("reference_dir", help="Ruta de la carpeta con archivos CSV de referencia.")
    parser.add_argument("config", help="Ruta del archivo JSON con configuraciones de etiquetado.")
    parser.add_argument("output_dir", help="Ruta de la carpeta donde se guardarán los archivos etiquetados.")

    args = parser.parse_args()

    # Llamar a la función de etiquetado
    try:
        labeled_file_path = tag_csv(args.target_csv, args.reference_dir, args.config, args.output_dir)
        print(f"Proceso completado. Archivo etiquetado guardado en: {labeled_file_path}")
    except Exception as e:
        print(f"Error durante el proceso de etiquetado: {e}")

if __name__ == "__main__":
    main()
