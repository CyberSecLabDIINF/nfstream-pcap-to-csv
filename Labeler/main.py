import argparse
from pathlib import Path
from config.config import ProcessingConfig
from processor.csv_processor import CSVProcessor
from utils.logger import setup_logging

def main():
    # Configuración de argumentos
    parser = argparse.ArgumentParser(description="Etiquetador modular de archivos CSV")
    parser.add_argument("--data-file", required=True, help="Archivo de datos a procesar (CSV)")
    parser.add_argument("--labels-dir", required=True, help="Directorio de etiquetas")
    parser.add_argument("--output-file", required=True, help="Archivo de salida procesado (CSV)")
    parser.add_argument("--config-file", required=True, help="Archivo de configuración JSON")
    parser.add_argument("--debug", action="store_true", help="Activa el modo debug")
    args = parser.parse_args()

    # Crear configuración
    config = ProcessingConfig(
        data_path=Path(args.data_file).resolve(),
        labels_dir=Path(args.labels_dir).resolve(),
        output_path=Path(args.output_file).resolve(),
        config_path=Path(args.config_file).resolve(),
        debug=args.debug
    )

    # Inicializar logger
    logger = setup_logging(config.debug)

    # Agregar logs de diagnóstico
    logger.debug("Configuración inicial:")
    logger.debug(f"data_path: {config.data_path}")
    logger.debug(f"labels_dir: {config.labels_dir}")
    logger.debug(f"output_path: {config.output_path}")
    logger.debug(f"config_path: {config.config_path}")
    logger.debug(f"debug: {config.debug}")

    # Crear y ejecutar el procesador
    processor = CSVProcessor(config, logger)
    if processor.initialize():
        success = processor.process()
        if not success:
            logger.error("El procesamiento falló")
            return 1
        logger.info("Procesamiento completado exitosamente")
        return 0
    else:
        logger.error("La inicialización falló")
        return 1

if __name__ == "__main__":
    exit(main())