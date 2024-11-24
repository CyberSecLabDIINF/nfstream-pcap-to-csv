import argparse
import os

from modules.logger import setup_logger
from modules.config_loader import load_config
from modules.file_handler import save_csv,load_csv,load_json
from modules.label_determiner import process_tagging
from modules.matcher import match_columns

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

    # Determinar tipo de etiquetado
    try:
        logger.info("Determinando tipo de etiquetado...")
        label_file_path = process_tagging(args.target_csv, args.labels_dir)
    except Exception as exp:
        logger.error(f"Error durante el proceso de etiquetado: {exp}")
        print(f"Error durante el proceso de etiquetado: {exp}")

    # Cargar configuración específica para el dataset
    try:
        logger.info("Cargando configuración...")
        json_config = load_config(args.config_path, args.dataset_type)
    except Exception as exp:
        logger.error(f"Error durante la carga de la configuración: {exp}")
        print(f"Error durante la carga de la configuración: {exp}")

    # Cargar archivo CSV de etiquetado correspondiente
    try:
        logger.info("Cargando archivo CSV de etiquetado...")
        df_tagged = load_csv(label_file_path)
    except Exception as exp:
        logger.error(f"Error durante la carga del archivo CSV de etiquetado: {exp}")
        print(f"Error durante la carga del archivo CSV de etiquetado: {exp}")

    # Cargar archivo CSV a etiquetar
    try:
        logger.info("Cargando archivo CSV a etiquetar...")
        df_target = load_csv(args.target_csv)
    except Exception as exp:
        logger.error(f"Error durante la carga del archivo CSV a etiquetar: {exp}")
        print(f"Error durante la carga del archivo CSV a etiquetar: {exp}")

    # Procesar el etiquetado
    try:
        logger.info("Iniciando proceso de etiquetado...")
        df_tagged = match_columns(df_target, df_tagged, json_config)
    except Exception as exp:
        logger.error(f"Error durante el proceso de etiquetado: {exp}")
        print(f"Error durante el proceso de etiquetado: {exp}")

    # Guardar el DataFrame etiquetado
    try:
        logger.info("Guardando archivo etiquetado...")
        save_csv(df_tagged, args.output_file)
    except Exception as exp:
        logger.error(f"Error durante el guardado del archivo etiquetado: {exp}")
        print(f"Error durante el guardado del archivo etiquetado: {exp}")

    logger.info(f"Archivo etiquetado guardado exitosamente en: {args.output_file}")

if __name__ == "__main__":
    main()
