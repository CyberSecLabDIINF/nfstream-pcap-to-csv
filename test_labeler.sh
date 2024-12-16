#!/bin/bash

# Definir variables para las rutas
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # Directorio donde se encuentra el script
LABELS_DIR="$SCRIPT_DIR/CSVs/Labeling/"                         # Carpeta de archivos etiquetados
CONFIG_FILE="$SCRIPT_DIR/LabelerV2/labeler_config.json"         # Archivo de configuración

# Valores para Data Exfiltration
DATA_FILE="$SCRIPT_DIR/CSVs/f/USACH/Ayudantias/Investigacion-Redes/PCAPs/Bot-IoT/Theft/Data_Exfiltration/IoT_Dataset_data_theft__00001_20180618110503.csv" # Archivo de datos
OUTPUT_FILE="$SCRIPT_DIR/CSVs/Labeling/Results/Prueba06/IoT_Dataset_data_theft__00001_20180618110503_labeled.csv" # Archivo de salida
DATASET_TYPE="Bot-IoT"                                          # Tipo de conjunto de datos

# Valores para Keylogging
#DATA_FILE="$SCRIPT_DIR/CSVs/f/USACH/Ayudantias/Investigacion-Redes/PCAPs/Bot-IoT/Theft/Keylogging/IoT_Keylogging__00001_20180619135903.csv" # Archivo de datos
#OUTPUT_FILE="$SCRIPT_DIR/CSVs/Labeling/Results/Prueba06/IoT_Keylogging__00001_20180619135903_labeled.csv" # Archivo de salida

# Valores Dos_HTTP
#DATA_FILE="$SCRIPT_DIR/CSVs/f/USACH/Ayudantias/Investigacion-Redes/PCAPs/Bot-IoT/DoS/DoS_HTTP/IoT_Dataset_HTTP_DoS__00001_20180604150105.csv" # Archivo de datos
#OUTPUT_FILE="$SCRIPT_DIR/CSVs/Labeling/Results/Prueba06/IIoT_Dataset_HTTP_DoS__00001_20180604150105_labeled.csv" # Archivo de salida

# Activar el entorno virtual
source "$SCRIPT_DIR/.venv/Scripts/activate"

# Ejecutar el programa principal
python3 LabelerV2/main.py \
    --target_csv "$DATA_FILE" \
    --labels_dir "$LABELS_DIR" \
    --config_path "$CONFIG_FILE" \
    --output_file "$OUTPUT_FILE" \
    --dataset_type "$DATASET_TYPE"

# Desactivar el entorno virtual
deactivate