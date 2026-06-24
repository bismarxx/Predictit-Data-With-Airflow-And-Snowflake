# Entorno Local de Apache Airflow con MinIO y Snowflake

Este proyecto proporciona un entorno de desarrollo local completo utilizando Docker Compose. Despliega Apache Airflow (configurado con CeleryExecutor), una instancia local de MinIO (almacenamiento compatible con S3) y preconfigura automáticamente las conexiones necesarias para MinIO y Snowflake.

## 🚀 Requisitos Previos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes en tu sistema:
* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/) (versión compatible con formato '3.8')

## 📦 Servicios Incluidos

El archivo `docker-compose.yml` levanta la siguiente arquitectura:

* **PostgreSQL (`postgres`)**: Base de datos que almacena los metadatos de Airflow.
* **Redis (`redis`)**: Broker de mensajería utilizado por Celery para encolar las tareas.
* **Airflow Webserver (`airflow-webserver`)**: Interfaz gráfica de usuario (UI) de Airflow.
* **Airflow Scheduler (`airflow-scheduler`)**: Demonio encargado de planificar y disparar la ejecución de los DAGs.
* **Airflow Worker (`airflow-worker`)**: Nodos de procesamiento que ejecutan las tareas mediante Celery.
* **Airflow Init (`airflow-init`)**: Contenedor efímero que inicializa la base de datos de Airflow, crea un usuario administrador y configura las conexiones de Snowflake y MinIO.
* **MinIO (`minio`)**: Servidor local de almacenamiento de objetos compatible con Amazon S3.
* **MinIO Bucket Creator (`minio-create-bucket`)**: Script de inicialización que crea automáticamente el bucket `predictit-raw` al levantar los servicios.

## 🛠️ Instalación y Uso

### 1. Preparar la estructura de directorios
Airflow necesita ciertas carpetas mapeadas en tu host para leer los DAGs, plugins y guardar logs. Ejecuta los siguientes comandos en la raíz del proyecto para crearlas:

```bash
mkdir -p ./dags ./logs ./plugins ./scripts
