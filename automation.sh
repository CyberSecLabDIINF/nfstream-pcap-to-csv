#!/bin/bash

# Uso individual: bash automation.sh -p (path to PCAPs folder) -d

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
    # Temporary save the current directory
    current_dir=$(pwd)
    # Cd into the directory of the script if not already in it
    if [ "$(basename "$(pwd)")" != "NFSTREAM_pre-processing" ]; then
        cd "$(dirname "$0")" || show_error "Failed to change directory"
    fi
    # Check if the virtual environment directory is found
    if [ ! -d "venv" ]; then
        show_error "Virtual environment directory not found"
    fi
    # Activate the virtual environment
    source venv/bin/activate || show_error "Failed to activate virtual environment"
    # Check if the virtual environment is activated
    if [ -n "$VIRTUAL_ENV" ]; then
        echo "Virtual environment activated: $VIRTUAL_ENV"
    else
        show_error "Failed to activate virtual environment"
    fi
    # Cd back to the original directory
    cd "$current_dir" || show_error "Failed to change directory"
}

# Function to install dependencies
install_dependencies() {
    echo "Installing python dependencies"
    # Temporary save the current directory
    current_dir=$(pwd)
    # Cd into the directory of the script if not already in it
    if [ "$(basename "$(pwd)")" != "NFSTREAM_pre-processing" ]; then
        cd "$(dirname "$0")" || show_error "Failed to change directory"
    fi
    # Create a virtual environment
    python3 -m venv venv || show_error "Failed to create virtual environment"
    # Check if the virtual environment directory is found
    if [ ! -d "venv" ]; then
        show_error "Virtual environment directory not found"
    fi
    # Activate the virtual environment
    activate_virtual_environment
    # If in the virtual environment, install the dependencies
    if [ -n "$VIRTUAL_ENV" ]; then
        pip3 install -r requirements.txt || show_error "Failed to install dependencies"
    else
        show_error "Failed to activate virtual environment"
    fi
    # Deactivate the virtual environment
    deactivate
    # Cd back to the original directory
    cd "$current_dir" || show_error "Failed to change directory"
}

# 
process_files() {
    # Encuentra recursivamente todos los archivos .pcap dentro de la carpeta de origen
    find "$1" -type f -name "*.pcap" | while read -r file; do
        action "$file" "$1"
    done
}

# Action to be performed on each file
action() {
    file=$1
    pcaps_folder=$2
    echo "Processing file: $file"
    
    # Extrae la ruta relativa completa a partir de pcaps_folder
    relative_path="${file#$pcaps_folder/}"
    relative_dir=$(dirname "$relative_path")
    name=$(basename "$file" .pcap)

    # Construye la estructura completa en CSVs, comenzando con el directorio final de pcaps_folder
    target_dir="$csvs_folder/$(basename "$pcaps_folder")/$relative_dir"
    mkdir -p "$target_dir" || show_error "Failed to create directory structure in CSVs"

    # Procesa el archivo y genera el CSV en la estructura de subcarpetas
    if [ "$(basename "$(pwd)")" = "NFSTREAM_pre-processing" ]; then
        python3 nfs-preprocesser.py -i "$file" -o "$target_dir/$name.csv"
    else
        python3 investigation-utilities/NFSTREAM_pre-processing/nfs-preprocesser.py -i "$file" -o "$target_dir/$name.csv"
    fi
}

# -------------------------------------- SCRIPT --------------------------------------

# Check if the number of arguments is zero
if [ $# -eq 0 ]; then
    show_help
fi

# Default values
pcaps_folder="PCAPs"
csvs_folder="/mnt/s/Programacion/Investigacion-Redes/investigation-utilities/NFSTREAM_pre-processing/CSVs"
install_deps=false
file_extension=".pcap"

# Parse the command line arguments
while getopts ":hdp:" opt; do
    case ${opt} in
        h )
            show_help
            ;;
        d )
            install_deps=true
            ;;
        p )
            pcaps_folder=$OPTARG
            ;;
        \? )
            show_help
            ;;
    esac
done

# Check if the dependencies need to be installed
if [ "$install_deps" = true ]; then
    install_dependencies
fi

# Check if the PCAPs folder exists
if [ ! -d "$pcaps_folder" ]; then
    show_error "PCAPs folder not found"
fi

# Check if the CSVs folder exists
if [ ! -d "$csvs_folder" ]; then
    mkdir "$csvs_folder" || show_error "Failed to create CSVs folder"
fi

# Activate the virtual environment
activate_virtual_environment

# Process each file in the PCAPs folder
process_files "$pcaps_folder"

# Deactivate the virtual environment
deactivate

# End of script
echo "Done"
exit 0