# -*- coding: utf-8 -*-
# Pre processing
import nfstream
import os
import argparse
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de argparse
parser = argparse.ArgumentParser(description="Procesamiento de archivos PCAP a CSV usando NFStreamer")
parser.add_argument("-i", "--input", type=str, required=True, help="Ruta del archivo PCAP de entrada")
parser.add_argument("-o", "--output", type=str, required=True, help="Ruta del archivo CSV de salida")

args = parser.parse_args()

pcap_path = args.input
output_csv_path = args.output

# Verificar si el archivo PCAP existe
if not os.path.exists(pcap_path):
    logging.error(f"El archivo PCAP no existe: {pcap_path}")
else:
    try:
        stream_reader = nfstream.NFStreamer(source=pcap_path, statistical_analysis=True)
        stream_reader.to_csv(output_csv_path)
        logging.info(f"Conexiones exportadas a {output_csv_path}")
    except FileNotFoundError as fnf_error:
        logging.error(f"Archivo no encontrado: {fnf_error}")
    except PermissionError as perm_error:
        logging.error(f"Permiso denegado: {perm_error}")
    except Exception as e:
        logging.error(f"Error procesando el archivo PCAP: {e}")