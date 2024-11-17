#!/bin/bash

# Uso individual: bash automation.sh -p (path to PCAPs folder) -d

# Definir variables de ruta configurables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # Directorio donde se encuentra el script
VENV_DIR="$SCRIPT_DIR/.venv"                                    # Ruta al entorno virtual
PCAPS_FOLDER_DEFAULT="$SCRIPT_DIR/PCAPs"                         # Carpeta predeterminada de archivos PCAP
CSVS_FOLDER_DEFAULT="$SCRIPT_DIR/CSVs"                          # Carpeta predeterminada para archivos CSV
LABELING_FOLDER_DEFAULT="$SCRIPT_DIR/CSVs/Labeling/Bot-IoT"     # Carpeta predeterminada para archivos etiquetados

# Function to show help
show_help() {
    echo "Usage: $0 [option...]"
    echo
    echo "   -h, --help             show help"
    echo "   -d, --dependencies     install dependencies"
    echo "   -p, --path             path to the folder with the pcap files"
    echo
    exit 1
}

# Function to show error messages
show_error() {
    echo "Error: $1"
    exit 1
}

# Function to activate virtual environment
activate_virtual_environment() {
    echo "Activating the virtual environment"
    # Verificar si el directorio del entorno virtual existe
    if [ ! -d "$VENV_DIR" ]; then
        show_error "Virtual environment directory not found"
    fi
    # Activar el entorno virtual
    source "$VENV_DIR/Scripts/activate" || show_error "Failed to activate virtual environment"
    # Verificar si el entorno virtual fue activado
    if [ -n "$VIRTUAL_ENV" ]; then
        echo "Virtual environment activated: $VIRTUAL_ENV"
    else
        show_error "Failed to activate virtual environment"
    fi
}

# Function to install dependencies
install_dependencies() {
    echo "Installing python dependencies"
    # Crear el entorno virtual si no existe
    if [ ! -d "$VENV_DIR" ]; then
        python3 -m venv "$VENV_DIR" || show_error "Failed to create virtual environment"
    fi
    # Activar el entorno virtual y proceder con la instalación
    activate_virtual_environment
    pip3 install -r "$SCRIPT_DIR/requirements.txt" || show_error "Failed to install dependencies"
    # Desactivar el entorno virtual después de la instalación
    deactivate
}

# Function to process PCAP files
process_files() {
    # Encuentra recursivamente todos los archivos .pcap dentro de la carpeta de origen
    find "$1" -type f -name "*.pcap" | while read -r file; do
        action "$file" "$1"
    done
}

# Action to be performed on each file
action() {
    local file="$1"                    # Archivo actual a procesar
    local pcaps_folder="$2"             # Carpeta donde se encuentran los archivos PCAP
    echo "Processing file: $file"

    # Eliminar el prefijo /mnt de la ruta del archivo para la ruta de destino
    relative_path="${file#*/mnt/}"
    relative_dir=$(dirname "$relative_path")
    name=$(basename "$file" .pcap)

    # Construir la ruta de directorio objetivo omitiendo /mnt
    target_dir="$csvs_folder/$relative_dir"
    mkdir -p "$target_dir" || show_error "Failed to create directory structure in CSVs"

    # Procesar el archivo y generar el CSV en la subcarpeta apropiada
    python3 "$SCRIPT_DIR/nfs-preprocesser.py" -i "$file" -o "$target_dir/$name.csv"
    echo "File processed: $target_dir/$name.csv"

    # Etiquetar el archivo PCAP procesado
    echo "Labeling file: $file"
    base_name=$(basename "$file" .pcap)
    labeled_file="${target_dir}/${base_name}_labeled.csv"
    python3 "$SCRIPT_DIR/single_labeler.py" --data-file "$target_dir/$name.csv" --labels-dir "$LABELING_FOLDER_DEFAULT" --output-file "$labeled_file" --debug
}

# -------------------------------------- SCRIPT --------------------------------------

# Verificar si el número de argumentos es cero
if [ $# -eq 0 ]; then
    show_help
fi

# Valores por defecto
pcaps_folder="$PCAPS_FOLDER_DEFAULT"
csvs_folder="$CSVS_FOLDER_DEFAULT"
install_deps=false

# Parsear los argumentos de la línea de comandos
while getopts ":hdp:" opt; do
    case ${opt} in
        h ) show_help ;;                  # Mostrar ayuda
        d ) install_deps=true ;;          # Activar instalación de dependencias
        p ) pcaps_folder=$OPTARG ;;       # Especificar carpeta de archivos PCAP
        \? ) show_help ;;                 # Mostrar ayuda para opciones inválidas
    esac
done

# Verificar si se deben instalar las dependencias
if [ "$install_deps" = true ]; then
    install_dependencies
fi

# Verificar si existe la carpeta de archivos PCAP
if [ ! -d "$pcaps_folder" ]; then
    show_error "PCAPs folder not found"
fi

# Verificar si existe la carpeta de archivos CSV, si no, crearla
if [ ! -d "$csvs_folder" ]; then
    mkdir "$csvs_folder" || show_error "Failed to create CSVs folder"
fi

# Activar el entorno virtual
activate_virtual_environment

# Procesar cada archivo en la carpeta de archivos PCAP
process_files "$pcaps_folder"

# Desactivar el entorno virtual
deactivate

# Fin del script
echo "Done"
exit 0