import argparse
import os

from modules.logger import setup_logger
from modules.config_loader import load_config
from modules.file_handler import save_csv, load_csv
from modules.label_determiner import determine_labeling_file
from modules.matcher import match_columns_optimized

def main():
    # Configurar el logger
    logger = setup_logger(log_dir="logs", log_file="main.log")

    # Configuración de argparse
    parser = argparse.ArgumentParser(description="Script para etiquetar archivos CSV basados en configuraciones y referencias.")
    parser.add_argument("--target_csv", required=True, help="Ruta del archivo CSV a etiquetar.")
    parser.add_argument("--labels_dir", required=True, help="Ruta de la carpeta con archivos CSV de referencia.")
    parser.add_argument("--config_path", required=True, help="Ruta del archivo JSON con configuraciones de etiquetado.")
    parser.add_argument("--output_file", required=True, help="Ruta del archivo CSV de salida.")
    parser.add_argument("--dataset_type", required=True, help="Tipo de dataset (por ejemplo, 'Bot-IoT').")
    args = parser.parse_args()

    # Inicializar variables para el proceso
    dataset_config_dictionary = None
    label_file_path = None
    tagged_df = None
    df_for_tagging = None
    target_df = None

    # Cargar configuración específica para el dataset
    try:
        logger.info("Cargando configuración...")
        dataset_config_dictionary = load_config(args.config_path, args.dataset_type)
    except Exception as exp:
        logger.error(f"Error durante la carga de la configuración: {exp}")
        print(f"Error durante la carga de la configuración: {exp}")

    # Determinar archivo fuente de etiquetado
    try:
        logger.info("Determinando tipo de etiquetado...")
        label_file_path = determine_labeling_file(args.target_csv, args.labels_dir, dataset_config_dictionary)
    except Exception as exp:
        logger.error(f"Error durante el proceso de seleccion: {exp}")
        print(f"Error durante el proceso de sleccion: {exp}")

    print(f"Archivo de etiquetado: {label_file_path}")

    # Cargar archivo CSV de etiquetado correspondiente
    if label_file_path:
        try:
            logger.info("Cargando archivo CSV de etiquetado...")
            logger.info(f"Archivo de etiquetado: {label_file_path}")
            df_for_tagging = load_csv(label_file_path)
        except Exception as exp:
            logger.error(f"Error durante la carga del archivo CSV de etiquetado: {exp}")
            print(f"Error durante la carga del archivo CSV de etiquetado: {exp}")

    # Cargar archivo CSV a etiquetar
    try:
        logger.info("Cargando archivo CSV a etiquetar...")
        target_df = load_csv(args.target_csv)
    except Exception as exp:
        logger.error(f"Error durante la carga del archivo CSV a etiquetar: {exp}")
        print(f"Error durante la carga del archivo CSV a etiquetar: {exp}")

    # Procesar el etiquetado
    if df_for_tagging is not None and target_df is not None:
        try:
            logger.info("Iniciando proceso de etiquetado...")
            tagged_df = match_columns_optimized(target_df, df_for_tagging, args.dataset_type ,dataset_config_dictionary)
        except Exception as exp:
            logger.error(f"Error durante el proceso de etiquetado: {exp}")
            print(f"Error durante el proceso de etiquetado: {exp}")


    # Guardar el DataFrame etiquetado
    if tagged_df is not None:
        try:
            logger.info("Guardando archivo etiquetado...")
            save_csv(tagged_df, args.output_file)
        except Exception as exp:
            logger.error(f"Error durante el guardado del archivo etiquetado: {exp}")
            print(f"Error durante el guardado del archivo etiquetado: {exp}")


if __name__ == "__main__":
    main()
