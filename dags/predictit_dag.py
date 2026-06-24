from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from scripts.predictit_ingestion import extract_predictit_data
from scripts.snowflake_loader import load_to_snowflake

default_args = {
    'owner': 'tu_nombre',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='predictit_pipeline_minio_snowflake',
    default_args=default_args,
    description='PredictIt API -> MinIO -> Snowflake (sin AWS S3)',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['predictit', 'minio', 'snowflake', 'local'],
    max_active_runs=1,
) as dag:

    tarea_extract = PythonOperator(
        task_id='extract_predictit_to_minio',
        python_callable=extract_predictit_data,
        provide_context=True,
    )

    tarea_load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    tarea_ready = DummyOperator(
        task_id='pipeline_complete',
    )

    tarea_extract >> tarea_load >> tarea_ready