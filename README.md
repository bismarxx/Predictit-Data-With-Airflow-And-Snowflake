# PredictIt Data Pipeline

## Airflow + MinIO + Snowflake + Power BI

Pipeline de datos de extremo a extremo para extraer información desde la API pública de PredictIt, almacenarla en MinIO (S3-compatible), procesarla mediante Apache Airflow y cargarla en Snowflake para análisis y visualización con Power BI.

---

## Objetivo

Construir una arquitectura moderna de ingeniería de datos que:

* Extraiga datos desde la API de PredictIt.
* Almacene datos crudos en MinIO.
* Orqueste procesos con Apache Airflow.
* Cargue datos estructurados en Snowflake.
* Permita análisis y dashboards en Power BI.
* Funcione completamente en Docker.

---

## Arquitectura

```text id="a1k9x2"
PredictIt API
      │
      ▼
 Apache Airflow
      │
      ▼
    MinIO
 (Raw JSON)
      │
      ▼
  Snowflake
(Data Warehouse)
      │
      ▼
   Power BI
```

---

## Tecnologías Utilizadas

| Componente         | Tecnología     |
| ------------------ | -------------- |
| Orquestación       | Apache Airflow |
| Contenedores       | Docker         |
| Almacenamiento Raw | MinIO          |
| Data Warehouse     | Snowflake      |
| Visualización      | Power BI       |
| Lenguaje           | Python         |
| Procesamiento      | Pandas         |
| API                | Requests       |

---

## Estructura del Proyecto

```text id="p9q3lm"
predictit-pipeline/
├── dags/
│   └── predictit_dag.py
├── plugins/
├── scripts/
│   ├── predictit_ingestion.py
│   └── snowflake_loader.py
├── logs/
├── docker-compose.yml
├── requirements.txt
├── .env
└── README.md
```

---

## Requisitos Previos

* Docker Desktop
* WSL2 (Windows)
* Cuenta Snowflake
* Power BI Desktop
* Git (opcional)

---

## Configuración

### 1. Clonar el repositorio

```bash id="c8k1ld"
git clone https://github.com/tu_usuario/predictit-pipeline.git
cd predictit-pipeline
```

---

### 2. Configurar variables de entorno

```env id="v7d2pq"
AIRFLOW_UID=50000
```

---

### 3. Instalar dependencias

```txt id="m1x8qw"
boto3>=1.26.0
requests>=2.28.0
apache-airflow-providers-amazon
snowflake-connector-python>=3.0.0
pandas>=1.5.0
```

---

### 4. Levantar servicios

```bash id="t4z9ab"
docker-compose up -d
```

Verificar:

```bash id="k2p8rs"
docker-compose ps
```

---

## Accesos

### Airflow

```text id="air1"
http://localhost:8080
Usuario: admin
Contraseña: admin
```

### MinIO

```text id="min1"
http://localhost:9001
Usuario: minioadmin
Contraseña: minioadmin
```

---

## Flujo del Pipeline

### 1. Extracción

Se consume la API pública:

```http id="api1"
GET https://www.predictit.org/api/marketdata/all/
```

---

### 2. Almacenamiento Raw (MinIO)

```text id="raw1"
predictit-raw/
└── raw/
    └── predict_markets_YYYYMMDD_HHMMSS.json
```

---

### 3. Carga a Snowflake

Tabla destino:

```sql id="sf1"
PREDICTIT_DB.RAW.STG_PREDICTIT_MARKETS
```

---

### 4. Visualización en Power BI

Power BI se conecta directamente a Snowflake para:

* Dashboards de volumen de mercados
* Tendencias de apuestas políticas
* Análisis histórico
* KPIs de contratos

---

## Configuración Snowflake

```sql id="sf2"
CREATE DATABASE IF NOT EXISTS PREDICTIT_DB;

CREATE SCHEMA IF NOT EXISTS PREDICTIT_DB.RAW;

CREATE OR REPLACE TABLE PREDICTIT_DB.RAW.STG_PREDICTIT_MARKETS (
    MARKET_ID INTEGER,
    MARKET_NAME VARCHAR(500),
    SHORT_NAME VARCHAR(200),
    URL VARCHAR(500),
    TIMESTAMP VARCHAR(50)
);
```

---

## DAG de Airflow

Nombre:

```text id="dag1"
predictit_pipeline_minio_snowflake
```

Frecuencia:

```python id="dag2"
schedule_interval=timedelta(hours=6)
```

---

## Ejecución

1. Abrir Airflow
2. Activar DAG
3. Ejecutar manualmente o esperar trigger automático
4. Revisar logs

---

## Validación Snowflake

```sql id="val1"
SELECT COUNT(*) 
FROM PREDICTIT_DB.RAW.STG_PREDICTIT_MARKETS;
```

```sql id="val2"
SELECT * 
FROM PREDICTIT_DB.RAW.STG_PREDICTIT_MARKETS
LIMIT 10;
```

---

## Validación MinIO

* Abrir: `http://localhost:9001`
* Bucket: `predictit-raw`
* Revisar archivos JSON generados

---

## Problemas comunes

### Error módulos scripts

Verificar PYTHONPATH en Docker.

### Error credenciales Snowflake

Revisar usuario, password y account.

### Tabla vacía

Revisar logs del task `load_to_snowflake`.

---

## Mejoras futuras

* Incremental load
* dbt para modelado
* Data Quality checks
* CI/CD con GitHub Actions
* Monitoreo con Grafana
* Alertas automáticas

---

## Resultado final

* Pipeline automatizado de datos
* Datos almacenados en MinIO
* Datos estructurados en Snowflake
* Dashboards en Power BI
* Arquitectura lista para portafolio Data Engineer

---

## Autor

Proyecto de ingeniería de datos para práctica profesional.

Stack:

* Airflow
* MinIO
* Snowflake
* Python
* Power BI
* Docker

---

## Licencia

MIT
