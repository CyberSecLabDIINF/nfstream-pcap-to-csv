# Bot-IoT PCAP Preprocessing and Labeling

Este proyecto automatiza el procesamiento y etiquetado de archivos PCAP de Bot-IoT, convirtiéndolos a formato CSV para su análisis. Utiliza `NFStreamer` para extraer conexiones y generar archivos CSV estructurados.

## Instalación (asumiendo linux o WSL)

1. Clona este repositorio e ingresa a la carpeta del preprocesador:

   ```bash
   git clone https://github.com/CyberSecLabDIINF/investigation-utilities.git
   cd investigation-utilities/NFSTREAM_pre-processing
   ```

2. Verifica que tengas instalado python, pip y virtualenv:

   ```bash
   sudo apt install python3
   sudo apt install python3-pip
   sudo apt install python3-virtualenv
   ```

## Uso

El proyecto incluye dos scripts principales:

1. **nfs-preprocesser.py**: Un script de Python para convertir archivos PCAP a CSV usando `NFStreamer`.

   **Ejemplo de uso**:

   ```bash
   python nfs-preprocesser.py -i <ruta_del_archivo_PCAP> -o <ruta_de_archivo_CSV_resultante>
   ```

2. **automation.sh**: Un script de Bash para automatizar el procesamiento de múltiples archivos PCAP.

   **Opciones disponibles**:

   - `-d`: Instala las dependencias
   - `-p`: Especifica la ruta de la carpeta con los archivos PCAP

   **Ejemplo de uso**:

   ```bash
   bash automation.sh -p <ruta_de_la_carpeta_PCAPs> -d
   ```

## Estructura de Archivos

- `nfs-preprocesser.py`: Script de Python para procesar archivos PCAP individualmente.
- `automation.sh`: Script de Bash que procesa de forma recursiva todos los archivos PCAP en una carpeta especificada.
- `requirements.txt`: Archivo de dependencias para instalar paquetes necesarios.

## Ejemplo de Ejecución

1. Para procesar un archivo PCAP específico:

   ```bash
   python nfs-preprocesser.py -i example.pcap -o output.csv
   ```

2. Para procesar múltiples archivos PCAP en un directorio:
   ```bash
   bash automation.sh -p /ruta/a/pcaps -d
   ```

## Log y Manejo de Errores

El procesamiento de archivos genera logs detallados en la terminal. En caso de errores, el sistema notifica el tipo de error específico (por ejemplo, `Archivo no encontrado` o `Permiso denegado`).

## Créditos

Estos scripts fue desarrollado por [Andrés Zelaya](https://github.com/Opsord) para facilitar el análisis de tráfico de red en el contexto de Bot-IoT.
