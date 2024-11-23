#!/bin/bash

# Definir variables para las rutas
DATA_FILE="data/samples/IoT_Keylogging__00002_20180619140719.csv"
LABELS_DIR="data/labels/"
OUTPUT_FILE="data/output/labeled_output.csv"
CONFIG_FILE="config/labeling_config.json"

# Ejecutar el programa principal
python3 main.py \
    --data-file "$DATA_FILE" \
    --labels-dir "$LABELS_DIR" \
    --output-file "$OUTPUT_FILE" \
    --config-file "$CONFIG_FILE" \
    --debug
