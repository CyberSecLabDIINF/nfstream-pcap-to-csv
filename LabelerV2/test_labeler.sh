#!/bin/bash

# Definir variables para las rutas
DATA_FILE="data/target_csvs/IoT_Keylogging__00002_20180619140719.csv"
LABELS_DIR="data/labels/Bot-IoT"
OUTPUT_FILE="data/output/algo.csv"
CONFIG_FILE="labeler_config.json"
DATASET_TYPE="Bot-IoT"

# Ejecutar el programa principal
python3 main.py --target_csv "$DATA_FILE" --labels_dir "$LABELS_DIR" --config_path "$CONFIG_FILE" --output_file "$OUTPUT_FILE" --dataset_type "$DATASET_TYPE"