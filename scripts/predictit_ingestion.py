import json
import logging
import requests
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

def extract_predictit_data(**context):
    url = "https://www.predictit.org/api/marketdata/all/"
    bucket_name = "predictit-raw"
    
    try:
        logger.info(f"Extrayendo datos de {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        json_data = response.json()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"predict_markets_{timestamp}.json"
        json_str = json.dumps(json_data, ensure_ascii=False, indent=2)
        
        s3_hook = S3Hook(aws_conn_id='minio')
        s3_hook.load_string(
            string_data=json_str,
            key=f"raw/{file_name}",
            bucket_name=bucket_name,
            replace=True
        )
        
        s3_path = f"s3://{bucket_name}/raw/{file_name}"
        context['task_instance'].xcom_push(key='s3_path', value=s3_path)
        logger.info(f"Archivo subido a MinIO: {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"Error en la ingesta: {e}")
        raise
